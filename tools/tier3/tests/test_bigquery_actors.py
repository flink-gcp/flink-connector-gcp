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
from dataclasses import replace

import pytest
from flink_tier3 import bigquery_actors as actors
from flink_tier3 import bigquery_auth as auth
from flink_tier3 import bigquery_bundle as bundles
from flink_tier3.common import Failure, timestamp
from flink_tier3.environment import Environment
from flink_tier3.policy import BIGQUERY, ENVIRONMENT, MIB
from flink_tier3.records import Records
from flink_tier3.runner import Runner
from flink_tier3.supervisor import Supervisor
from google.oauth2.credentials import Credentials
from test_bigquery_auth import identity
from test_bigquery_auth import wire as wire  # noqa: PLC0414
from test_bigquery_bundle import approval as approval  # noqa: PLC0414
from test_bigquery_plan import inputs as inputs  # noqa: PLC0414
from test_bigquery_plan import renderer as renderer  # noqa: PLC0414
from test_bigquery_plan import trial as trial  # noqa: PLC0414
from test_tier3_lifecycle import env as env  # noqa: PLC0414

TOKEN = "e" * 32


@pytest.fixture
def prepared(approval, env):
    kube, store, _, clock = env
    approval["namespaces"][BIGQUERY]["uid"] = BIGQUERY + "-uid"
    clock.now = timestamp(approval["started_at"])
    environment = Environment(kube, store, approval, clock, clock.sleep, actor="runner")
    bundle = bundles.prepare(approval, prepared_at=approval["started_at"])
    return environment, bundle


def test_runner_bundle_and_supervisor_manifests_bind_distinct_authenticated_actors(
    prepared, wire
):
    env, bundle = prepared
    wire.responses = [identity()]
    with actors.runner(
        env, bundle, runner_token=TOKEN, credentials=Credentials("runner")
    ) as runner:
        assert isinstance(runner, Runner)
        assert runner.bigquery.binding["evidence_bytes"] == 10 * MIB
        assert runner.bigquery.binding["query_until"] == env.schedule.cleanup_at
        assert runner.bigquery.controller.api.deadline == env.schedule.cleanup_end
        assert runner.bigquery.controller.api.http.principal.endswith(
            "tier3-runner@flink-gcp.iam.gserviceaccount.com"
        )
        assert env.refresh().bigquery is None
        assert len(wire.calls) == 1
    assert wire.closed
    other = copy.copy(env)
    other.actor = "supervisor"
    other.records = Records(env.store, env.approval, env.clock)
    wire.closed = False
    wire.calls.clear()
    wire.responses = [identity("supervisor")]
    with actors.supervisor(
        other,
        bundle["application"],
        bundle["upgrade_application"],
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
        assert supervisor.cleanup.quiesce() is True
        assert other.refresh().bigquery is None
        # The supervisor authenticates as its own principal; without this the
        # whole suite stays green when its authentication is removed.
        assert len(wire.calls) == 1
        assert wire.calls[0][1] == auth.USERINFO
    assert wire.closed


@pytest.mark.parametrize("field", ["application", "approval", "delivery"])
def test_runner_rejects_tampered_bundle_before_authentication(prepared, wire, field):
    env, bundle = prepared
    bundle[field]["unexpected"] = True
    with (
        pytest.raises(Failure),
        actors.runner(
            env, bundle, runner_token=TOKEN, credentials=Credentials("runner")
        ),
    ):
        pytest.fail("Admitted altered bundle")
    assert not wire.calls
    assert env.refresh().bigquery is None


@pytest.mark.parametrize(
    "change", ["source", "upgrade", "application", "role", "owner"]
)
def test_supervisor_refuses_binding_drift_before_authentication(prepared, wire, change):
    env, bundle = prepared
    env.actor = "supervisor"
    app, upgrade = bundle["application"], bundle["upgrade_application"]
    if change == "source":
        env.approval = replace(env.approval, delivery_sha256="0" * 64)
    elif change == "upgrade":
        upgrade["changed"] = True
    elif change == "application":
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


def test_supervisor_cannot_be_constructed_inside_the_cleanup_window(prepared, wire):
    """Cleanup runs in a context opened earlier; a restart cannot reopen one."""
    env, bundle = prepared
    env.actor = "supervisor"
    env.clock.now = env.schedule.cleanup_at
    with (
        pytest.raises(Failure, match="admission window"),
        actors.supervisor(
            env,
            bundle["application"],
            bundle["upgrade_application"],
            credentials=Credentials("supervisor"),
        ),
    ):
        pytest.fail("Constructed a supervisor after admission closed")
    assert not wire.calls


def test_supervisor_source_check_is_the_pin_it_can_reproduce(prepared, wire):
    """The supervisor runs from a mounted subset of the installed package.

    Its digest is the delivery pin, so checking `runtime_sha256` there would
    refuse every supervisor: the mount is a strict subset of what that digest
    covers. The runner keeps `runtime_sha256`, and the bundle it verifies
    compares both pins anyway.
    """
    env, bundle = prepared
    env.actor = "supervisor"
    env.approval = replace(env.approval, runtime_sha256="0" * 64)
    wire.responses = [identity("supervisor")]
    with actors.supervisor(
        env,
        bundle["application"],
        bundle["upgrade_application"],
        credentials=Credentials("supervisor"),
    ) as supervisor:
        assert isinstance(supervisor, Supervisor)
        assert len(wire.calls) == 1


def test_wrong_google_identity_prevents_runner_construction(prepared, wire):
    env, bundle = prepared
    wire.responses = [identity("supervisor")]
    with (
        pytest.raises(Failure, match="identity differs"),
        actors.runner(
            env, bundle, runner_token=TOKEN, credentials=Credentials("runner")
        ),
    ):
        pytest.fail("Admitted wrong Google identity")
    assert env.refresh().bigquery is None
    assert wire.closed


def test_authentication_crossing_startup_deadline_prevents_admission(prepared, wire):
    env, bundle = prepared
    wire.responses = [identity()]
    wire.hook = lambda _: setattr(env.clock, "now", env.schedule.started + 600)
    with (
        pytest.raises(Failure, match="construction deadline"),
        actors.runner(
            env, bundle, runner_token=TOKEN, credentials=Credentials("runner")
        ),
    ):
        pytest.fail("Admitted after startup")
    assert env.refresh().bigquery is None
    assert wire.closed


def test_spent_startup_budget_refuses_before_any_credential_call(prepared, wire):
    """Admission is checked first, so an expired startup budget costs no token."""
    env, bundle = prepared
    wire.responses = [identity()]
    env.clock.now = env.schedule.started + 600
    with (
        pytest.raises(Failure, match="startup deadline expired"),
        actors.runner(
            env, bundle, runner_token=TOKEN, credentials=Credentials("runner")
        ),
    ):
        pytest.fail("Admitted an expired construction")
    assert not wire.calls
    assert env.refresh().bigquery is None
    assert wire.closed


def test_context_exception_closes_session_without_automatic_cleanup(prepared, wire):
    env, bundle = prepared
    wire.responses = [identity()]
    with (
        pytest.raises(RuntimeError, match="caller failure"),
        actors.runner(
            env, bundle, runner_token=TOKEN, credentials=Credentials("runner")
        ),
    ):
        raise RuntimeError("caller failure")
    assert wire.closed and len(wire.calls) == 1
    assert env.refresh().bigquery is None


def test_ownership_lost_during_authentication_prevents_actor_return(prepared, wire):
    env, bundle = prepared
    wire.responses = [identity()]

    def replace_owner(_):
        _, generation = env.store.read(ENVIRONMENT)
        env.store.write(ENVIRONMENT, {"nonce": "replacement"}, generation)

    wire.hook = replace_owner
    with (
        pytest.raises(Failure, match="owner"),
        actors.runner(
            env, bundle, runner_token=TOKEN, credentials=Credentials("runner")
        ),
    ):
        pytest.fail("Admitted after ownership loss")
    assert wire.closed and len(wire.calls) == 1
