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
"""Production record/resource composition with synthetic service transports."""

import copy
from types import SimpleNamespace

import pytest
import requests
from flink_tier3.common import Failure, TransportError, digest, json_bytes
from flink_tier3.model import Phase, RunRecord
from flink_tier3.pubsub.lifecycle import PubSubLifecycle, require_pubsub_clean
from flink_tier3.records import Records

from ..test_lifecycle import Store
from .test_resources import FakeIamHttp


class Environment:
    """Internal caller contract; this fixture does not admit a CLI scenario."""

    def __init__(self, application):
        self.approval = SimpleNamespace(
            scenario="pubsub-recovery",
            run_id="lifecycle-1361",
            nonce="a" * 32,
            application_sha256=digest(application),
            pubsub_trial={"records_per_subscription": 1000},
        )
        self.actor = "runner"
        self.store = Store()
        self.records = Records(self.store, self.approval, lambda: 1800000000)
        self.store.write(self.records.path, RunRecord(self.approval.nonce).to_dict())

    def refresh(self):
        return self.records.read()[0]


@pytest.fixture
def setup():
    app = {"metadata": {"name": "lifecycle-1361", "namespace": "tier3-pubsub"}}
    env, http, guarded = Environment(app), FakeIamHttp(), []
    controller = PubSubLifecycle(env, http, lambda *args: guarded.append(args))
    controller.initialize()
    return controller, env, http, guarded


def restart(setup):
    _, env, http, guarded = setup
    env.records = Records(env.store, env.approval, lambda: 1800000000)
    return PubSubLifecycle(env, http, lambda *args: guarded.append(args))


def stop(env):
    env.records.request_stop()


def test_old_record_and_absent_lifecycle_preserve_settlement():
    data = RunRecord("a" * 32).to_dict()
    del data["pubsub"]
    record = RunRecord.from_dict(data)
    assert record.pubsub is None
    require_pubsub_clean(record)


def test_guard_is_mandatory(setup):
    _, env, http, _ = setup
    with pytest.raises(Failure, match="guard"):
        PubSubLifecycle(env, http, None)


def test_preparation_preserves_service_settings_and_explicit_policies(setup):
    controller, env, http, guarded = setup

    def before(method, _name):
        state = env.refresh().pubsub
        assert state["stage"] == "preparing"
        if method in ("PUT", "POST"):
            assert state["creation_intent"] is True
            assert (
                env.store.read(controller.plan.manifest_path)[0]
                == controller.plan.manifest()
            )

    http.before = before
    controller.prepare()
    saved = env.refresh().pubsub
    assert saved["stage"] == "prepared"
    assert {r["name"]: r for r in saved["resources"]} == http.resources
    assert {p["name"]: p["policy"] for p in saved["policies"]} == http.policies
    assert len(saved["resources"]) == len(saved["policies"]) == 6
    # The guard sees every service request, and control I/O is not one.
    assert not any(g[0] == "control" for g in guarded)
    for method, name, _ in http.calls:
        assert any(g[1:] == (method, name) for g in guarded)
    with pytest.raises(Failure, match="cannot be resumed"):
        restart(setup).prepare()
    assert len([c for c in http.calls if c[0] == "PUT"]) == 6


@pytest.mark.parametrize("actor", ["supervisor", "workload", "recovery"])
def test_preparation_is_runner_only(setup, actor):
    controller, env, http, _ = setup
    env.actor = actor
    for operation in (controller.initialize, controller.prepare):
        with pytest.raises(Failure, match="submitting runner"):
            operation()
    assert not http.calls


def test_cas_conflict_allows_only_one_preparation_attempt(setup):
    controller, env, http, _ = setup
    env.store.before_write = lambda: restart(setup).prepare()
    with pytest.raises(Failure, match="cannot be resumed"):
        controller.prepare()
    assert env.store.conflicts == 1
    assert env.refresh().pubsub["stage"] == "prepared"
    assert len([c for c in http.calls if c[0] == "PUT"]) == 6


def test_stop_racing_claim_prevents_service_io(setup):
    controller, env, http, _ = setup
    env.store.before_write = lambda: stop(env)
    with pytest.raises(Failure, match="admission has stopped"):
        controller.prepare()
    assert env.refresh().pubsub["stage"] == "initialized"
    assert not http.calls


@pytest.mark.parametrize("event", ["stop_requested", "evidence_failed", "phase"])
def test_a_stop_during_preparation_keeps_it_from_being_marked_prepared(setup, event):
    """Preparation checks the stop between its steps, not before each request."""
    controller, env, http, _ = setup

    def after(method, _name):
        if method == "PUT" and len(http.resources) == 1:
            env.records._change(
                lambda r: setattr(
                    r, event, Phase.CLEANING if event == "phase" else True
                )
            )

    http.after = after
    with pytest.raises(Failure, match="stopped"):
        controller.prepare()
    assert env.refresh().pubsub["stage"] == "preparing"
    # The step under way finishes; the next one, installing grants, is refused.
    assert len(http.resources) == 6 and not http.policies
    http.after = lambda *args: None
    env.actor = "supervisor"
    assert restart(setup).cleanup(lambda: True)
    assert not http.resources


@pytest.mark.parametrize("method", ["PUT", "POST"])
def test_lost_mutation_response_is_not_resumed_but_supervisor_can_clean(setup, method):
    controller, env, http, _ = setup

    def lost(current, _name):
        if current == method:
            raise requests.Timeout("Accepted write; response lost")

    http.after = lost
    with pytest.raises(TransportError):
        controller.prepare()
    with pytest.raises(Failure, match="cannot be resumed"):
        restart(setup).prepare()
    http.after = lambda *args: None
    env.actor = "supervisor"
    assert restart(setup).cleanup(lambda: True)
    assert not http.resources
    assert env.refresh().pubsub["stage"] == "cleaned"
    assert env.store.read(env.records.path)[0] is not None
    assert (
        env.store.read(controller.plan.manifest_path)[0] == controller.plan.manifest()
    )
    assert not env.refresh().idle and not env.refresh().success


def test_preflight_collision_is_not_adopted_or_deleted(setup):
    controller, env, http, _ = setup
    foreign = controller.plan.topics()[0]
    http.resources[foreign["name"]] = foreign
    with pytest.raises(Failure, match="refusing adoption"):
        controller.prepare()
    assert env.refresh().pubsub["creation_intent"] is False
    assert controller.cleanup(lambda: True)
    assert http.resources == {foreign["name"]: foreign}
    assert not any(c[0] == "DELETE" for c in http.calls)


@pytest.mark.parametrize("result", [False, None, 1])
def test_cleanup_requires_true_external_barrier_after_durable_stop(setup, result):
    controller, env, http, _ = setup
    controller.prepare()
    http.calls.clear()

    def quiesce():
        assert env.refresh().stop_requested
        assert env.refresh().pubsub["stage"] == "cleaning"
        return result

    with pytest.raises(Failure, match="not quiescent"):
        controller.cleanup(quiesce)
    assert not http.calls
    assert env.refresh().pubsub["stage"] == "cleaning"


def test_cleanup_retries_ambiguous_delete_and_rechecks_absence(setup):
    controller, env, http, _ = setup
    controller.prepare()

    def lost(method, _name):
        if method == "DELETE":
            raise requests.Timeout("Delete response lost")

    http.after = lost
    with pytest.raises(TransportError):
        controller.cleanup(lambda: True)
    assert env.refresh().pubsub["stage"] == "cleaning"
    assert len(http.resources) == 5
    http.after = lambda *args: None
    assert restart(setup).cleanup(lambda: True)
    assert not http.resources
    http.calls.clear()
    assert restart(setup).cleanup(lambda: True)
    assert len(http.calls) == 6 and all(c[0] == "GET" for c in http.calls)
    require_pubsub_clean(env.refresh())


@pytest.mark.parametrize("change", ["missing", "foreign", "float-version"])
def test_missing_or_foreign_manifest_retains_active_control(setup, change):
    controller, env, http, _ = setup
    controller.prepare()
    path = controller.plan.manifest_path
    value, generation = env.store.read(path)
    if change == "missing":
        env.store.delete(path, generation)
    else:
        value["nonce" if change == "foreign" else "version"] = (
            "b" * 32 if change == "foreign" else 2.0
        )
        env.store.write(path, value, generation)
    http.calls.clear()
    with pytest.raises(Failure):
        controller.cleanup(lambda: True)
    assert not any(c[0] != "GET" for c in http.calls)
    with pytest.raises(Failure, match="cleanup is incomplete"):
        require_pubsub_clean(env.refresh())
    assert env.store.read(env.records.path)[0] is not None


def test_uninitialized_control_refuses_all_controller_operations(setup):
    controller, env, http, _ = setup
    env.records._change(lambda r: setattr(r, "pubsub", None))
    for operation in (controller.prepare, lambda: controller.cleanup(lambda: True)):
        with pytest.raises(Failure, match="not initialized"):
            operation()
    assert not http.calls


def test_observation_limit_leaves_owned_partial_work_cleanable(setup, monkeypatch):
    import flink_tier3.pubsub.lifecycle as module

    controller, env, http, _ = setup
    monkeypatch.setattr(
        module, "MAX_CONTROL_BYTES", len(json_bytes(env.refresh().pubsub)) + 50
    )
    with pytest.raises(Failure, match="256 KiB"):
        controller.prepare()
    assert len(http.resources) == 6
    assert env.refresh().pubsub["resources"] is None
    assert env.refresh().pubsub["stage"] == "preparing"
    assert controller.cleanup(lambda: True)
    assert not http.resources


def test_a_refusing_guard_can_stop_policy_write_and_cleanup_io(setup):
    controller, env, http, _ = setup

    def refuse(phase, method, _name):
        if method in ("POST", "DELETE"):
            raise Failure("Budget exhausted")

    controller = PubSubLifecycle(env, http, refuse)
    with pytest.raises(Failure, match="Budget exhausted"):
        controller.prepare()
    assert not http.policies
    with pytest.raises(Failure, match="Budget exhausted"):
        controller.cleanup(lambda: True)
    assert len(http.resources) == 6
    assert env.refresh().pubsub["stage"] == "cleaning"


def test_a_refused_first_request_leaves_nothing_to_clean(setup):
    _, env, http, _ = setup

    def refuse(*_args):
        raise Failure("Request refused")

    controller = PubSubLifecycle(env, http, refuse)
    with pytest.raises(Failure, match="Request refused"):
        controller.prepare()
    state = env.refresh().pubsub
    assert (state["stage"], state["creation_intent"]) == ("preparing", False)
    assert not http.calls
    assert restart(setup).cleanup(lambda: True)
    assert not http.calls


def test_lost_control_claim_response_refuses_restart_before_any_service_call(setup):
    controller, env, http, _ = setup
    original = env.store.write

    def accepted(name, value, generation="0", **kwargs):
        result = original(name, value, generation, **kwargs)
        if name == env.records.path and value["pubsub"]["stage"] == "preparing":
            raise TransportError("Accepted claim; response lost")
        return result

    env.store.write = accepted
    with pytest.raises(TransportError, match="Accepted claim"):
        controller.prepare()
    assert env.refresh().pubsub["stage"] == "preparing"
    env.store.write = original
    with pytest.raises(Failure, match="cannot be resumed"):
        restart(setup).prepare()
    assert not http.calls
    assert restart(setup).cleanup(lambda: True)
    assert not http.calls


@pytest.mark.parametrize("actor", ["workload", "recovery", "unknown"])
def test_cleanup_refuses_non_lifecycle_actor_before_control_or_service_io(setup, actor):
    controller, env, http, guarded = setup
    before = copy.deepcopy(env.store.data)
    guarded.clear()
    env.actor = actor
    with pytest.raises(Failure, match="requires a lifecycle actor"):
        controller.cleanup(lambda: True)
    assert env.store.data == before
    assert not http.calls and not guarded


def test_cleanup_requires_callable_barrier_before_stop(setup):
    controller, env, http, _ = setup
    with pytest.raises(Failure, match="quiescence barrier"):
        controller.cleanup(None)
    assert not env.refresh().stop_requested
    assert not http.calls


def test_lost_creation_intent_response_can_settle_by_absence_without_manifest(setup):
    controller, env, http, _ = setup
    original = env.store.write

    def accepted(name, value, generation="0", **kwargs):
        result = original(name, value, generation, **kwargs)
        if name == env.records.path and value["pubsub"]["creation_intent"]:
            raise TransportError("Accepted creation intent; response lost")
        return result

    env.store.write = accepted
    with pytest.raises(TransportError, match="Accepted creation intent"):
        controller.prepare()
    env.store.write = original
    assert env.refresh().pubsub["creation_intent"]
    assert env.store.read(controller.plan.manifest_path)[0] is None
    assert not http.resources
    http.calls.clear()
    env.actor = "supervisor"
    assert restart(setup).cleanup(lambda: True)
    assert len(http.calls) == 6 and all(c[0] == "GET" for c in http.calls)
    assert env.store.read(controller.plan.manifest_path)[0] is None
    require_pubsub_clean(env.refresh())


@pytest.mark.parametrize("fault", ["present", "transport", "budget"])
def test_missing_manifest_absence_check_refuses_uncertainty_without_deleting(
    setup, fault
):
    controller, env, http, _ = setup
    env.records._change(
        lambda r: r.pubsub.update(stage="preparing", creation_intent=True)
    )
    last = controller.plan.topics()[-1]
    if fault == "present":
        http.resources[last["name"]] = last
    elif fault == "transport":

        def lost(_method, name):
            if name == last["name"]:
                raise requests.Timeout("Absence unknown")

        http.before = lost
    else:

        def refuse(_phase, _method, name):
            if name == last["name"]:
                raise Failure("Absence budget exhausted")

        controller = PubSubLifecycle(env, http, refuse)
    with pytest.raises(Failure):
        controller.cleanup(lambda: True)
    assert all(c[0] == "GET" for c in http.calls)
    assert len(http.calls) == (5 if fault == "budget" else 6)
    assert env.refresh().pubsub["stage"] == "cleaning"
    assert env.store.read(env.records.path)[0] is not None


def test_failed_recheck_recloses_settlement_after_previous_cleanup(setup):
    controller, env, http, _ = setup
    controller.prepare()
    controller.cleanup(lambda: True)
    require_pubsub_clean(env.refresh())

    def refuse(phase, _method, name):
        if phase == "cleanup" and name.startswith("projects/"):
            raise Failure("Recheck budget exhausted")

    controller = PubSubLifecycle(env, http, refuse)
    with pytest.raises(Failure, match="Recheck budget"):
        controller.cleanup(lambda: True)
    with pytest.raises(Failure, match="cleanup is incomplete"):
        require_pubsub_clean(env.refresh())
    assert restart(setup).cleanup(lambda: True)
    require_pubsub_clean(env.refresh())
