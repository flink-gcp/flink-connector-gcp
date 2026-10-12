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
"""Bind the Pub/Sub actors to externally supplied approvals."""

import copy
from types import SimpleNamespace

import pytest
from flink_tier3.common import Failure, timestamp
from flink_tier3.environment import Environment
from flink_tier3.lifecycle import bind
from flink_tier3.model import Approval
from flink_tier3.policy import ENVIRONMENT, PUBSUB, SMOKE
from flink_tier3.pubsub import actors, plan
from flink_tier3.pubsub.guard import PubSubGuard, admission_deadline
from flink_tier3.runner import Runner
from flink_tier3.supervisor import Supervisor
from google.oauth2.credentials import Credentials

from ..bigquery.test_actors import wire as wire  # noqa: PLC0414
from ..test_lifecycle import env as env  # noqa: PLC0414
from .test_plan import inputs as inputs  # noqa: PLC0414
from .test_plan import renderer as renderer  # noqa: PLC0414
from .test_plan import trial as trial  # noqa: PLC0414

TOKEN = "e" * 32


@pytest.fixture
def approval(env, inputs, renderer):
    """A Pub/Sub approval over the cluster fixture, as dispatch assembles it."""
    value = copy.deepcopy(env[2])
    inputs.update(run_id=value["run_id"], nonce=value["nonce"])
    proposed = plan.prepare(**inputs)["proposal"]
    value.update(
        scenario="pubsub-recovery",
        started_at=proposed["started_at"],
        expires_at=proposed["expires_at"],
        cleanup_at=proposed["cleanup_at"],
        application_sha256=proposed["application_sha256"],
        upgrade_application_sha256=proposed["recovery_application_sha256"],
        pubsub_trial=copy.deepcopy(inputs["trial"]),
    )
    value["namespaces"][PUBSUB] = value["namespaces"].pop(SMOKE)
    value["images"].pop("smoke")
    value["images"].update(
        supervisor=proposed["images"]["supervisor"],
        application=inputs["application_image"],
    )
    return value


@pytest.fixture
def prepared(approval, env, inputs):
    kube, store, _, clock = env
    # The cluster fixture's application namespace, as Pub/Sub's.
    quota = kube.get("ResourceQuota", SMOKE, "tier3-idle")
    quota["metadata"].update(namespace=PUBSUB, uid=PUBSUB + "-quota")
    kube.put(quota)
    approval["namespaces"][PUBSUB].update(
        uid=PUBSUB + "-uid", quota_uid=PUBSUB + "-quota"
    )
    clock.now = timestamp(approval["started_at"])
    environment = Environment(kube, store, approval, clock, clock.sleep, actor="runner")
    # The rendering dispatch checked, with the approval bound into its delivery.
    rendered = plan.prepare(**inputs)
    bind(Approval.from_dict(approval), rendered["delivery"], int(clock.now))
    return environment, rendered


def supervising(env):
    other = copy.copy(env)
    other.actor = "supervisor"
    return other


def construct_supervisor(env, upgrade):
    return actors.supervisor(env, upgrade, credentials=Credentials("supervisor"))


def test_runner_and_supervisor_bind_distinct_actors(prepared, wire):
    env, rendered = prepared
    deadline = env.schedule.started + 900
    with actors.runner(
        env, runner_token=TOKEN, credentials=Credentials("runner")
    ) as runner:
        assert isinstance(runner, Runner)
        handoff = runner.pubsub
        assert handoff.token == TOKEN
        guard = handoff.controller.before_operation
        assert isinstance(guard, PubSubGuard) and guard.deadline == deadline
        assert handoff.settled == handoff.controller.http.settled
        assert handoff.controller.http.http.credentials.token == "runner"
        # The actor session, which returns a redirect unread.
        assert handoff.controller.http.http.get_redirect_target(None) is None
        trial = env.approval.pubsub_trial
        assert handoff.records_per_subscription == trial["records_per_subscription"]
        assert env.refresh().pubsub is None
    assert wire.closed == ["AuthorizedSession"]
    other = supervising(env)
    tokens = set()
    for _ in range(2):
        wire.closed.clear()
        with construct_supervisor(
            other, rendered["recovery_application"]
        ) as supervisor:
            assert isinstance(supervisor, Supervisor)
            observer = supervisor.cleanup.pubsub
            assert observer.env is other
            assert observer.controller.before_operation.deadline == deadline
            assert observer.controller.http.http.credentials.token == "supervisor"
            # Built here, from this run: the namespace holds no writer.
            assert supervisor.cleanup.quiesce() is True
            tokens.add(observer.token)
        assert wire.closed == ["AuthorizedSession"]
    # Each supervisor process binds its own token.
    assert len(tokens) == 2 and TOKEN not in tokens
    assert env.refresh().pubsub is None
    # Construction sends nothing: the first service call is the actor's own.
    assert not wire.calls


@pytest.mark.parametrize("change", ["role", "owner"])
def test_supervisor_refuses_binding_drift(prepared, wire, change):
    env, rendered = prepared
    env = supervising(env)
    if change == "role":
        env.actor = "runner"
    else:
        _, generation = env.store.read(ENVIRONMENT)
        env.store.write(ENVIRONMENT, {"nonce": "replacement"}, generation)
    with (
        pytest.raises(Failure),
        construct_supervisor(env, rendered["recovery_application"]),
    ):
        pytest.fail("Admitted binding drift")
    assert not wire.calls


def test_the_admission_deadline_never_passes_cleanup(prepared):
    env, _ = prepared
    assert admission_deadline(env) == env.schedule.started + 900
    short = SimpleNamespace(schedule=SimpleNamespace(started=0, cleanup_at=600))
    assert admission_deadline(short) == 600


def construct_runner(env):
    return actors.runner(env, runner_token=TOKEN, credentials=Credentials("runner"))


def test_a_lost_ownership_prevents_the_runner(prepared, wire):
    env, _ = prepared
    _, generation = env.store.read(ENVIRONMENT)
    env.store.write(ENVIRONMENT, {"nonce": "replacement"}, generation)
    with pytest.raises(Failure, match="owner"), construct_runner(env):
        pytest.fail("Admitted after ownership loss")
    assert wire.closed == ["AuthorizedSession"]


def test_a_stop_prevents_the_runner(prepared, wire):
    env, _ = prepared
    env.records.request_stop()
    with (
        pytest.raises(Failure, match="admission has been stopped"),
        construct_runner(env),
    ):
        pytest.fail("Admitted after a stop")
    assert wire.closed == ["AuthorizedSession"]


def test_a_passed_admission_deadline_refuses_the_runner(prepared, wire):
    env, _ = prepared
    env.clock.now = admission_deadline(env)
    with (
        pytest.raises(Failure, match="admission deadline expired"),
        construct_runner(env),
    ):
        pytest.fail("Admitted an expired construction")
    assert wire.closed == ["AuthorizedSession"]


def test_a_context_exception_closes_the_session_without_cleanup(prepared, wire):
    env, _ = prepared
    with (
        pytest.raises(RuntimeError, match="caller failure"),
        construct_runner(env),
    ):
        raise RuntimeError("caller failure")
    assert wire.closed == ["AuthorizedSession"] and not wire.calls
    assert env.refresh().pubsub is None
