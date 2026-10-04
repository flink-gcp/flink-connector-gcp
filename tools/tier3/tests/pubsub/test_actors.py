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
"""Bind the authenticated Pub/Sub actors to externally supplied approvals."""

import copy
from dataclasses import replace
from types import SimpleNamespace

import pytest
from flink_tier3.actor_auth import PRINCIPALS, USERINFO
from flink_tier3.common import Failure, digest, timestamp
from flink_tier3.environment import Environment
from flink_tier3.policy import ENVIRONMENT, PUBSUB, SMOKE
from flink_tier3.pubsub import actors
from flink_tier3.pubsub import bundle as bundles
from flink_tier3.pubsub.guard import PubSubGuard, admission_deadline
from flink_tier3.runner import Runner
from flink_tier3.supervisor import Supervisor
from google.oauth2.credentials import Credentials

from ..bigquery.test_auth import identity
from ..bigquery.test_auth import wire as wire  # noqa: PLC0414
from ..test_lifecycle import env as env  # noqa: PLC0414
from .test_bundle import approval as approval  # noqa: PLC0414
from .test_plan import inputs as inputs  # noqa: PLC0414
from .test_plan import renderer as renderer  # noqa: PLC0414
from .test_plan import trial as trial  # noqa: PLC0414

TOKEN = "e" * 32


@pytest.fixture
def prepared(approval, env):
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
    bundle = bundles.prepare(approval, prepared_at=approval["started_at"])
    return environment, bundle


def supervising(env):
    other = copy.copy(env)
    other.actor = "supervisor"
    return other


def construct_supervisor(env, application, upgrade):
    return actors.supervisor(
        env, application, upgrade, credentials=Credentials("supervisor")
    )


def test_runner_and_supervisor_bind_distinct_authenticated_actors(prepared, wire):
    env, bundle = prepared
    deadline = env.schedule.started + 900
    wire.responses = [identity()]
    with actors.runner(
        env, bundle, runner_token=TOKEN, credentials=Credentials("runner")
    ) as runner:
        assert isinstance(runner, Runner)
        handoff = runner.pubsub
        assert handoff.token == TOKEN
        guard = handoff.controller.before_operation
        assert isinstance(guard, PubSubGuard) and guard.deadline == deadline
        assert handoff.settled == handoff.controller.http.settled
        assert handoff.controller.http.principal == PRINCIPALS["runner"]
        assert handoff.traffic.limits == env.approval.pubsub_traffic_limits
        assert env.refresh().pubsub is None
        assert [call[1] for call in wire.calls] == [USERINFO]
    assert wire.closed
    other = supervising(env)
    tokens = set()
    for _ in range(2):
        wire.closed = False
        wire.calls.clear()
        wire.responses = [identity("supervisor")]
        with construct_supervisor(
            other, bundle["application"], bundle["recovery_application"]
        ) as supervisor:
            assert isinstance(supervisor, Supervisor)
            observer = supervisor.cleanup.pubsub
            assert observer.env is other
            assert observer.controller.before_operation.deadline == deadline
            assert observer.controller.http.principal == PRINCIPALS["supervisor"]
            # Built here, from this run: the namespace holds no writer.
            assert supervisor.cleanup.quiesce() is True
            tokens.add(observer.token)
            assert [call[1] for call in wire.calls] == [USERINFO]
        assert wire.closed
    # Each supervisor process binds its own token.
    assert len(tokens) == 2 and TOKEN not in tokens
    assert env.refresh().pubsub is None


@pytest.mark.parametrize("field", ["application", "approval", "delivery"])
def test_runner_rejects_a_tampered_bundle_before_authentication(prepared, wire, field):
    env, bundle = prepared
    bundle[field]["unexpected"] = True
    with (
        pytest.raises(Failure),
        actors.runner(
            env, bundle, runner_token=TOKEN, credentials=Credentials("runner")
        ),
    ):
        pytest.fail("Admitted an altered bundle")
    assert not wire.calls
    assert env.refresh().pubsub is None


def repin(env, which, manifest):
    """Re-pin a manifest by digest, as an approval built around it would."""
    key = {
        "application": "application_sha256",
        "recovery": "upgrade_application_sha256",
    }[which]
    env.approval = replace(env.approval, **{key: digest(manifest)})


@pytest.mark.parametrize(
    "change",
    [
        "source",
        "application",
        "recovery",
        "application-args",
        "recovery-args",
        "recovery-parallelism",
        "role",
        "owner",
    ],
)
def test_supervisor_refuses_binding_drift_before_authentication(prepared, wire, change):
    env, bundle = prepared
    env = supervising(env)
    application = bundle["application"]
    recovery = bundle["recovery_application"]
    if change == "source":
        env.approval = replace(env.approval, delivery_sha256="0" * 64)
    elif change == "application":
        application["changed"] = True
    elif change == "recovery":
        recovery["changed"] = True
    elif change == "application-args":
        # A digest pins a manifest; only the trial says what it must run.
        application["spec"]["job"]["args"][-1] = "--entry-point=table"
        repin(env, "application", application)
    elif change == "recovery-args":
        recovery["spec"]["job"]["args"][1] = "--phase=initial"
        repin(env, "recovery", recovery)
    elif change == "recovery-parallelism":
        recovery["spec"]["job"]["parallelism"] = 1
        repin(env, "recovery", recovery)
    elif change == "role":
        env.actor = "runner"
    elif change == "owner":
        _, generation = env.store.read(ENVIRONMENT)
        env.store.write(ENVIRONMENT, {"nonce": "replacement"}, generation)
    match = "approved trial" if change.endswith(("args", "parallelism")) else None
    with (
        pytest.raises((Failure, ValueError), match=match),
        construct_supervisor(env, application, recovery),
    ):
        pytest.fail("Admitted binding drift")
    assert not wire.calls


def test_the_supervisor_checks_the_delivery_pin_it_can_reproduce(prepared, wire):
    env, bundle = prepared
    env = supervising(env)
    env.approval = replace(env.approval, runtime_sha256="0" * 64)
    wire.responses = [identity("supervisor")]
    with construct_supervisor(
        env, bundle["application"], bundle["recovery_application"]
    ) as supervisor:
        assert isinstance(supervisor, Supervisor)


def test_a_wrong_google_identity_prevents_runner_construction(prepared, wire):
    env, bundle = prepared
    wire.responses = [identity("supervisor")]
    with (
        pytest.raises(Failure, match="identity differs"),
        actors.runner(
            env, bundle, runner_token=TOKEN, credentials=Credentials("runner")
        ),
    ):
        pytest.fail("Admitted the wrong Google identity")
    assert wire.closed


def test_authentication_crossing_the_admission_deadline_prevents_admission(
    prepared, wire
):
    env, bundle = prepared
    wire.responses = [identity()]
    wire.hook = lambda _: setattr(env.clock, "now", env.schedule.started + 900)
    with (
        pytest.raises(Failure, match="construction deadline"),
        actors.runner(
            env, bundle, runner_token=TOKEN, credentials=Credentials("runner")
        ),
    ):
        pytest.fail("Admitted after the admission deadline")
    assert wire.closed


def test_the_admission_deadline_never_passes_cleanup(prepared):
    env, _ = prepared
    assert admission_deadline(env) == env.schedule.started + 900
    short = SimpleNamespace(schedule=SimpleNamespace(started=0, cleanup_at=600))
    assert admission_deadline(short) == 600


def construct_runner(env, bundle):
    return actors.runner(
        env, bundle, runner_token=TOKEN, credentials=Credentials("runner")
    )


def test_ownership_lost_during_authentication_prevents_the_actor(prepared, wire):
    env, bundle = prepared
    wire.responses = [identity()]

    def replace_owner(_):
        _, generation = env.store.read(ENVIRONMENT)
        env.store.write(ENVIRONMENT, {"nonce": "replacement"}, generation)

    wire.hook = replace_owner
    with pytest.raises(Failure, match="owner"), construct_runner(env, bundle):
        pytest.fail("Admitted after ownership loss")
    assert wire.closed


def test_a_stop_during_authentication_prevents_the_runner(prepared, wire):
    env, bundle = prepared
    wire.responses = [identity()]
    wire.hook = lambda _: env.records.request_stop()
    with (
        pytest.raises(Failure, match="admission has been stopped"),
        construct_runner(env, bundle),
    ):
        pytest.fail("Admitted after a stop")
    assert wire.closed


def test_a_spent_admission_budget_refuses_before_any_credential_call(prepared, wire):
    env, bundle = prepared
    wire.responses = [identity()]
    env.clock.now = admission_deadline(env)
    with pytest.raises(Failure), construct_runner(env, bundle):
        pytest.fail("Admitted an expired construction")
    assert not wire.calls
    assert wire.closed


def test_a_context_exception_closes_the_session_without_cleanup(prepared, wire):
    env, bundle = prepared
    wire.responses = [identity()]
    with (
        pytest.raises(RuntimeError, match="caller failure"),
        construct_runner(env, bundle),
    ):
        raise RuntimeError("caller failure")
    assert wire.closed and len(wire.calls) == 1
    assert env.refresh().pubsub is None


def test_a_supervisor_cannot_be_constructed_inside_the_cleanup_window(prepared, wire):
    """Cleanup runs in a context opened earlier; a restart cannot reopen one."""
    env, bundle = prepared
    env = supervising(env)
    env.clock.now = env.schedule.cleanup_at
    with (
        pytest.raises(Failure, match="admission window"),
        construct_supervisor(
            env, bundle["application"], bundle["recovery_application"]
        ),
    ):
        pytest.fail("Constructed a supervisor after admission closed")
    assert not wire.calls


def test_supervisor_authentication_crossing_cleanup_prevents_the_actor(prepared, wire):
    env, bundle = prepared
    env = supervising(env)
    wire.responses = [identity("supervisor")]
    wire.hook = lambda _: setattr(env.clock, "now", env.schedule.cleanup_at)
    with (
        pytest.raises(Failure, match="construction deadline"),
        construct_supervisor(
            env, bundle["application"], bundle["recovery_application"]
        ),
    ):
        pytest.fail("Constructed a supervisor during cleanup")
    assert wire.closed
