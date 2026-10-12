#
# Copyright 2026 The flink-gcp authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Bind real actor/controller/session objects to externally supplied approvals."""

import copy

import pytest
import requests
from flink_tier3.bigquery import actors, plan
from flink_tier3.common import Failure, timestamp
from flink_tier3.environment import Environment
from flink_tier3.model import APPROVAL_VERSION, validate_approval
from flink_tier3.policy import BIGQUERY, ENVIRONMENT, MIB, SMOKE
from flink_tier3.records import Records
from flink_tier3.runner import Runner
from flink_tier3.supervisor import Supervisor
from google.auth.transport.requests import AuthorizedSession
from google.oauth2.credentials import Credentials

from ..test_lifecycle import env as env  # noqa: PLC0414
from .test_plan import inputs as inputs  # noqa: PLC0414
from .test_plan import renderer as renderer  # noqa: PLC0414
from .test_plan import trial as trial  # noqa: PLC0414

TOKEN = "e" * 32


class Wire:
    """Every request and close an actor session would make, none of them sent."""

    def __init__(self):
        self.calls = []
        self.closed = []


@pytest.fixture
def wire(monkeypatch):
    value = Wire()

    def request(_session, method, url, *_args, **_kwargs):
        value.calls.append((method, url))
        raise AssertionError("an actor factory made a request")

    monkeypatch.setattr(requests.Session, "request", request)
    monkeypatch.setattr(
        requests.Session,
        "close",
        lambda session: (
            value.closed.append(type(session).__name__)
            if isinstance(session, AuthorizedSession)
            else None
        ),
    )
    return value


@pytest.fixture
def approval(env, inputs, renderer):
    """The BigQuery approval dispatch assembles from the rendered proposal."""
    value = copy.deepcopy(env[2])
    inputs.update(run_id=value["run_id"], nonce=value["nonce"])
    proposed = plan.prepare(**inputs)["proposal"]
    value.update(
        version=APPROVAL_VERSION,
        scenario="bigquery-recovery",
        started_at=proposed["started_at"],
        expires_at=proposed["expires_at"],
        cleanup_at=proposed["cleanup_at"],
        application_sha256=proposed["application_sha256"],
        upgrade_application_sha256=proposed["upgrade_application_sha256"],
        bigquery_trial=copy.deepcopy(inputs["trial"]),
    )
    value["namespaces"][BIGQUERY] = value["namespaces"].pop(SMOKE)
    value["images"].pop("smoke")
    value["images"].update(proposed["images"])
    validate_approval(value)
    return value


@pytest.fixture
def prepared(approval, env, inputs):
    kube, store, _, clock = env
    approval["namespaces"][BIGQUERY]["uid"] = BIGQUERY + "-uid"
    clock.now = timestamp(approval["started_at"])
    environment = Environment(kube, store, approval, clock, clock.sleep, actor="runner")
    return environment, plan.prepare(**inputs)


def test_runner_and_supervisor_manifests_bind_distinct_actors(prepared, wire):
    env, rendered = prepared
    with actors.runner(
        env,
        rendered["application"],
        runner_token=TOKEN,
        credentials=Credentials("runner"),
    ) as runner:
        assert isinstance(runner, Runner)
        assert runner.bigquery.binding["evidence_bytes"] == 10 * MIB
        assert runner.bigquery.binding["query_until"] == env.schedule.cleanup_at
        assert runner.bigquery.controller.api.deadline == env.schedule.cleanup_end
        assert runner.bigquery.controller.api.http.credentials.token == "runner"
        # The actor session, which returns a redirect unread.
        assert runner.bigquery.controller.api.http.get_redirect_target(None) is None
        assert env.refresh().bigquery is None
    assert wire.closed == ["AuthorizedSession"]
    other = copy.copy(env)
    other.actor = "supervisor"
    other.records = Records(env.store, env.approval, env.clock)
    with actors.supervisor(
        other,
        rendered["application"],
        rendered["upgrade_application"],
        credentials=Credentials("supervisor"),
    ) as supervisor:
        assert isinstance(supervisor, Supervisor)
        # Everything its own approval fixes, and no token yet: the runner has
        # not written a binding, and the supervisor may learn it only from one.
        assert supervisor.bigquery.binding == {
            **runner.bigquery.binding,
            "runner_token": None,
        }
        assert supervisor.bigquery.env is other
        assert supervisor.bigquery.controller.api.http.credentials.token == (
            "supervisor"
        )
        assert supervisor.cleanup.quiesce() is True
        assert other.refresh().bigquery is None
    assert wire.closed == ["AuthorizedSession"] * 2
    # Construction sends nothing: the first service call is the actor's own.
    assert not wire.calls


@pytest.mark.parametrize("change", ["application", "role", "owner"])
def test_supervisor_refuses_binding_drift(prepared, wire, change):
    env, rendered = prepared
    env.actor = "supervisor"
    app, upgrade = rendered["application"], rendered["upgrade_application"]
    if change == "application":
        app["changed"] = True
    elif change == "role":
        env.actor = "runner"
    elif change == "owner":
        _, generation = env.store.read(ENVIRONMENT)
        env.store.write(ENVIRONMENT, {"nonce": "replacement"}, generation)
    with (
        pytest.raises((Failure, ValueError)),
        actors.supervisor(
            env,
            app,
            upgrade,
            credentials=Credentials("supervisor"),
        ),
    ):
        pytest.fail("Admitted binding drift")
    assert not wire.calls


def test_a_spent_startup_budget_refuses_the_runner(prepared, wire):
    env, rendered = prepared
    env.clock.now = env.schedule.started + 600
    with (
        pytest.raises(Failure, match="startup deadline expired"),
        actors.runner(
            env,
            rendered["application"],
            runner_token=TOKEN,
            credentials=Credentials("runner"),
        ),
    ):
        pytest.fail("Admitted an expired construction")
    assert env.refresh().bigquery is None
    assert wire.closed == ["AuthorizedSession"]


def test_context_exception_closes_session_without_automatic_cleanup(prepared, wire):
    env, rendered = prepared
    with (
        pytest.raises(RuntimeError, match="caller failure"),
        actors.runner(
            env,
            rendered["application"],
            runner_token=TOKEN,
            credentials=Credentials("runner"),
        ),
    ):
        raise RuntimeError("caller failure")
    assert wire.closed == ["AuthorizedSession"] and not wire.calls
    assert env.refresh().bigquery is None


def test_a_lost_ownership_prevents_the_runner(prepared, wire):
    env, rendered = prepared
    _, generation = env.store.read(ENVIRONMENT)
    env.store.write(ENVIRONMENT, {"nonce": "replacement"}, generation)
    with (
        pytest.raises(Failure, match="owner"),
        actors.runner(
            env,
            rendered["application"],
            runner_token=TOKEN,
            credentials=Credentials("runner"),
        ),
    ):
        pytest.fail("Admitted after ownership loss")
    assert wire.closed == ["AuthorizedSession"]
