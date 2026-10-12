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
"""Serialized Pub/Sub trial binding and shared cleanup without cloud access."""

import copy

import pytest
from flink_tier3.cleanup import Cleanup, verify_idle
from flink_tier3.common import Failure, digest
from flink_tier3.environment import Environment
from flink_tier3.model import Approval, Phase, check_window, validate_approval
from flink_tier3.policy import PUBSUB
from flink_tier3.pubsub import plan
from flink_tier3.pubsub.handoff import PubSubHandoff
from flink_tier3.pubsub.lifecycle import PubSubLifecycle
from flink_tier3.runner import Runner
from flink_tier3.supervisor import Supervisor

from ..test_lifecycle import env as env  # noqa: PLC0414
from ..test_lifecycle import obj, rt
from .test_handoff import Service
from .test_plan import trial as trial  # noqa: PLC0414


@pytest.fixture
def prepared(env, trial):
    kube, store, value, clock = env
    saved = value["namespaces"].pop(rt.SMOKE)
    quota = kube.get("ResourceQuota", rt.SMOKE, "tier3-idle")
    quota["metadata"].update(namespace=PUBSUB, uid=PUBSUB + "-quota")
    kube.put(quota)
    value["namespaces"][PUBSUB] = {
        **saved,
        "uid": PUBSUB + "-uid",
        "quota_uid": PUBSUB + "-quota",
    }
    application = obj("FlinkDeployment", value["run_id"], PUBSUB)
    application["metadata"]["annotations"] = {rt.NONCE: value["nonce"]}
    application["spec"] = {
        "image": rt.GAR + "pubsub-recovery@sha256:" + "e" * 64,
        "job": {
            "args": ["--run-id=" + value["run_id"], "--records-per-subscription=1000"]
        },
    }
    value.update(
        version=6,
        scenario="pubsub-recovery",
        pubsub_trial=copy.deepcopy(trial),
        application_sha256=digest(application),
        upgrade_application_sha256="f" * 64,
    )
    value["images"].pop("smoke")
    value["images"]["application"] = application["spec"]["image"]
    environment = Environment(kube, store, value, clock, clock.sleep, actor="runner")
    return environment, application


@pytest.mark.parametrize("kind", plan.TRIALS)
@pytest.mark.parametrize("records", [3, 101, 10000])
@pytest.mark.parametrize("entry_point", plan.ENTRY_POINTS)
def test_trial_roundtrip_and_derived_plan(prepared, kind, records, entry_point):
    environment, _ = prepared
    value = environment.approval.to_dict()
    value["pubsub_trial"].update(
        trial=kind, records_per_subscription=records, entry_point=entry_point
    )
    validate_approval(value)
    approval = Approval.from_dict(value)
    assert Approval.from_dict(approval.to_dict()) == approval
    assert approval.pubsub_trial["entry_point"] == entry_point
    value["pubsub_trial"]["records_per_subscription"] = 4
    assert approval.pubsub_trial["records_per_subscription"] == records
    resources = approval.pubsub_plan.manifest()
    assert resources["run_id"] == approval.run_id
    assert resources["nonce"] == approval.nonce
    assert len(resources["topics"]) == len(resources["subscriptions"]) == 3
    assert approval.application_namespace == PUBSUB
    assert approval.schedule.active_seconds(approval.schedule.started) == 3420


@pytest.mark.parametrize(
    "key,value",
    [
        ("upgrade_application_sha256", ""),
        ("cells", [{}]),
        ("application_sha256", ""),
    ],
)
def test_incompatible_approval_refused(prepared, key, value):
    data = prepared[0].approval.to_dict()
    data[key] = value
    with pytest.raises(Failure):
        validate_approval(data)


@pytest.mark.parametrize(
    "key,delta",
    [
        ("started_at", 0.5),
        ("expires_at", 1),
        ("cleanup_at", 1),
    ],
)
def test_window_is_exact(prepared, key, delta):
    data = prepared[0].approval.to_dict()
    data[key] = rt.utc(rt.timestamp(data[key]) + delta)
    with pytest.raises(Failure, match="window differs"):
        validate_approval(data)


@pytest.mark.parametrize("target", ["namespace", "image", "extra-image", "expired"])
def test_approval_identity_and_admission_window(prepared, target):
    environment, _ = prepared
    data = environment.approval.to_dict()
    if target == "namespace":
        data["namespaces"][rt.SMOKE] = data["namespaces"].pop(PUBSUB)
    elif target == "image":
        data["images"]["application"] = rt.GAR + "smoke@sha256:" + "e" * 64
    elif target == "extra-image":
        data["images"]["smoke"] = rt.GAR + "smoke@sha256:" + "e" * 64
    else:
        validate_approval(data)
        with pytest.raises(Failure, match="admission window"):
            check_window(environment.approval, environment.schedule.cleanup_at)
        return
    with pytest.raises(Failure):
        validate_approval(data)


def test_smoke_roundtrip_omits_the_pubsub_trial(env):
    approval = Approval.from_dict(env[2])
    assert "pubsub_trial" not in approval.to_dict()
    assert Approval.from_dict(approval.to_dict()) == approval


def test_the_handoff_takes_its_input_domain_from_the_approval(prepared):
    environment, _ = prepared
    controller = PubSubLifecycle(
        environment, None, lambda *a: pytest.fail("Unexpected I/O")
    )
    handoff = PubSubHandoff(controller, actor_token="b" * 32)
    assert handoff.records_per_subscription == 1000


def test_application_replacement_and_control_quotas(prepared):
    environment, _ = prepared
    cleanup = Cleanup(environment)
    for namespace, count, ephemeral in (
        (PUBSUB, 4, 4 * 1024**3),
        (rt.SYSTEM, 3, 2 * 1024**3 + 128 * 1024**2),
    ):
        cleanup.quota(namespace, "run")
        hard = environment.kube.get("ResourceQuota", namespace, "tier3-idle")["spec"][
            "hard"
        ]
        assert hard["pods"] == str(count)
        assert hard["persistentvolumeclaims"] == "0"
        for category in ("requests", "limits"):
            assert rt.quantity(hard[category + ".cpu"]) == count
            assert rt.quantity(hard[category + ".memory"]) == count * 2 * 1024**3
            assert rt.quantity(hard[category + ".ephemeral-storage"]) == ephemeral
        cleanup.quota(namespace, None)
    verify_idle(environment)


def test_entrypoints_refuse_without_their_handoff(prepared):
    environment, application = prepared
    with pytest.raises(Failure, match="guarded handoff"):
        Runner(environment).start(None, None, application)
    with pytest.raises(Failure, match="requires its handoff"):
        Supervisor(environment).supervise("unused-pod")


def settled(prepared, service_intent):
    environment, _ = prepared
    runner = Runner(environment)
    if not service_intent:
        runner.cleanup.run("stopped before creation")
        return environment, runner
    service = Service()
    supervisor = Environment(
        environment.kube,
        environment.store,
        environment.approval,
        environment.clock,
        environment.sleep,
        actor="supervisor",
    )

    def actor(env, token):
        controller = PubSubLifecycle(env, service, lambda *a: None)
        return PubSubHandoff(controller, actor_token=token)

    sender, observer = actor(environment, "b" * 32), actor(supervisor, "c" * 32)
    sender.initialize()
    sender.prepare()
    environment.records.set_phase(Phase.READY)
    observer.join()
    sender.release()
    Cleanup(supervisor, pubsub=observer, quiesce=lambda: True).run("synthetic stop")
    assert environment.refresh().pubsub["stage"] == "cleaned"
    return environment, runner


# A retry keeps the receipt it finds, checking only that it is this run's and
# idle: it was written from the record before the record was deleted.
@pytest.mark.parametrize("change", ["none", "nonce", "idle"])
@pytest.mark.parametrize("service_intent", [False, True])
def test_receipt_retry_with_refreshed_plans(
    prepared, monkeypatch, change, service_intent
):
    environment, runner = settled(prepared, service_intent)
    plans = {
        "nonce": environment.approval.nonce,
        "roots": ["flink-gcp", "tier3-bootstrap", "tier3-operator"],
        "empty": True,
        "at": "2026-09-21T01:00:00Z",
    }
    original_delete = environment.store.delete

    def fail_delete(name, *a, **kw):
        if name == environment.records.path:
            raise Failure("control deletion unavailable")
        return original_delete(name, *a, **kw)

    monkeypatch.setattr(environment.store, "delete", fail_delete)
    with pytest.raises(Failure, match="control deletion unavailable"):
        runner.finalize(plans)
    path = f"runs/{environment.approval.run_id}/result.json"
    receipt, generation = environment.store.read(path)
    assert receipt["scenario"] == "pubsub-recovery"
    assert receipt["pubsub_trial"] == environment.approval.pubsub_trial
    assert receipt["success"] is False
    if change == "nonce":
        receipt["nonce"] = "0" * 32
    elif change == "idle":
        receipt["idle"] = False
    environment.store.write(path, receipt, generation)
    monkeypatch.setattr(environment.store, "delete", original_delete)
    plans = {**plans, "at": "2026-09-21T01:07:00Z"}
    if change == "none":
        assert runner.finalize(plans) is False
        assert environment.store.read(environment.records.path)[0] is None
        assert environment.store.read(rt.ENVIRONMENT)[0] is None
        assert environment.store.read(path)[0]["plans"]["at"] == "2026-09-21T01:00:00Z"
    else:
        with pytest.raises(Failure, match="Final receipt conflicts"):
            runner.finalize(plans)
        assert environment.store.read(environment.records.path)[0] is not None
        assert environment.store.read(rt.ENVIRONMENT)[0] is not None


def workload_pod(environment, name, role, parent):
    namespace = PUBSUB if role == "application" else rt.SYSTEM
    pod = obj("Pod", name, namespace)
    pod["metadata"]["ownerReferences"] = [{"uid": parent["metadata"]["uid"]}]
    shape = rt.POD_RESOURCES["smoke" if role == "application" else role]
    pod["spec"] = {
        "containers": [
            {
                "name": name,
                "image": environment.approval.images[role],
                "resources": {
                    category: copy.deepcopy(shape)
                    for category in ("requests", "limits")
                },
            }
        ]
    }
    if role == "application":
        pod["spec"]["nodeSelector"] = {"cloud.google.com/gke-spot": "true"}
    if role == "supervisor":
        pod["spec"]["affinity"] = {
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [
                        {
                            "matchExpressions": [
                                {
                                    "key": "cloud.google.com/gke-spot",
                                    "operator": "NotIn",
                                    "values": ["true"],
                                }
                            ]
                        }
                    ]
                }
            }
        }
    return environment.kube.put(pod)


@pytest.fixture
def seven_pods(prepared):
    environment, application = prepared
    runner = Runner(environment)
    runner.create_application(application)
    job = environment.kube.put(obj("Job", "supervisor", rt.SYSTEM))
    environment.remember("supervisor", job)
    operator = environment.kube.get("Deployment", rt.SYSTEM, rt.OPERATOR)
    for name in ("operator", "operator-replacement"):
        workload_pod(environment, name, "operator", operator)
    workload_pod(environment, "supervisor", "supervisor", job)
    for name in ("jm", "tm-0", "tm-1", "tm-replacement"):
        workload_pod(environment, name, "application", environment.root("application"))
    return environment, runner.cleanup


def test_audit_accepts_both_replacements(seven_pods):
    _environment, cleanup = seven_pods
    assert len(cleanup.audit()[1]) == 7


@pytest.mark.parametrize("change", ["image", "resources", "unowned"])
def test_audit_refuses_unapproved_pod(seven_pods, change):
    environment, cleanup = seven_pods
    pod = environment.kube.get("Pod", PUBSUB, "tm-0")
    if change == "image":
        pod["spec"]["containers"][0]["image"] = "foreign:latest"
    elif change == "resources":
        pod["spec"]["containers"][0]["resources"]["limits"]["memory"] = "4Gi"
    else:
        pod["metadata"]["ownerReferences"] = [{"uid": "unknown"}]
    environment.kube.put(pod)
    with pytest.raises(
        Failure, match="Pod image|Effective Pod resources|Unexpected object"
    ):
        cleanup.audit()


def test_concurrent_control_change_retains_control_and_lock(prepared):
    environment, _ = prepared
    runner = Runner(environment)
    runner.cleanup.run("stopped before creation")
    # The receipt write is the first write finalization makes.
    environment.store.before_write = lambda: environment.records.mark_evidence_failed(
        {"operation": "synthetic", "cause": "concurrent"}
    )
    with pytest.raises(rt.ApiError) as error:
        runner.finalize(
            {
                "nonce": environment.approval.nonce,
                "roots": ["flink-gcp", "tier3-bootstrap", "tier3-operator"],
                "empty": True,
            }
        )
    # The record is deleted only at the generation the receipt was derived from.
    assert error.value.status == 412
    assert environment.store.read(environment.records.path)[0] is not None
    assert environment.store.read(rt.ENVIRONMENT)[0] is not None


def lose_lock_release(environment, runner, monkeypatch):
    """Finalize with the lock release failing, after control was deleted."""
    control = environment.refresh().to_dict()
    plans = {
        "nonce": environment.approval.nonce,
        "roots": ["flink-gcp", "tier3-bootstrap", "tier3-operator"],
        "empty": True,
        "at": "2026-09-21T01:00:00Z",
    }
    original_delete = environment.store.delete

    def fail_lock(name, *args, **kwargs):
        if name == rt.ENVIRONMENT:
            raise Failure("lock release unavailable")
        return original_delete(name, *args, **kwargs)

    monkeypatch.setattr(environment.store, "delete", fail_lock)
    with pytest.raises(Failure, match="lock release unavailable"):
        runner.finalize(plans)
    monkeypatch.setattr(environment.store, "delete", original_delete)
    assert environment.store.read(environment.records.path)[0] is None
    assert environment.store.read(rt.ENVIRONMENT)[0] is not None
    return control, plans


@pytest.mark.parametrize("service_intent", [False, True])
def test_recovery_restores_a_cleaned_record_and_keeps_the_receipt(
    prepared, monkeypatch, service_intent
):
    environment, runner = settled(prepared, service_intent)
    _control, plans = lose_lock_release(environment, runner, monkeypatch)
    path = f"runs/{environment.approval.run_id}/result.json"
    receipt = environment.store.read(path)[0]
    recovery = Runner(environment)
    recovery.restore_control()
    restored = environment.refresh()
    assert restored.phase == Phase.CLEANED and restored.idle
    recovery.settle(request_stop=True)
    assert (
        Runner(environment).finalize({**plans, "at": "2026-09-21T01:07:00Z"}) is False
    )
    assert environment.store.read(environment.records.path)[0] is None
    assert environment.store.read(rt.ENVIRONMENT)[0] is None
    assert environment.store.read(path)[0] == receipt
    assert receipt["plans"]["at"] == "2026-09-21T01:00:00Z"


def test_recovery_evidence_failure_over_a_pubsub_receipt_still_finalizes(
    prepared, monkeypatch
):
    environment, runner = settled(prepared, True)
    _control, plans = lose_lock_release(environment, runner, monkeypatch)
    Runner(environment).restore_control()
    # A Pub/Sub receipt never records success, so there is no verdict for
    # recovery's own evidence failure to lower: it is recorded, not raised.
    environment.store.fail_evidence = True
    Runner(environment).settle(request_stop=True)
    environment.store.fail_evidence = False
    assert environment.refresh().evidence_failed
    # Finalization runs in a new process, without this one's local failure.
    environment.evidence_failure = None
    assert (
        Runner(environment).finalize({**plans, "at": "2026-09-21T01:07:00Z"}) is False
    )
    assert environment.store.read(environment.records.path)[0] is None
    assert environment.store.read(rt.ENVIRONMENT)[0] is None


@pytest.mark.parametrize("change", ["nonce", "idle"])
def test_recovery_refuses_a_receipt_from_another_run_or_not_idle(
    prepared, monkeypatch, change
):
    environment, runner = settled(prepared, True)
    _control, plans = lose_lock_release(environment, runner, monkeypatch)
    path = f"runs/{environment.approval.run_id}/result.json"
    receipt, generation = environment.store.read(path)
    if change == "nonce":
        receipt["nonce"] = "0" * 32
    else:
        receipt["idle"] = False
    environment.store.write(path, receipt, generation)
    recovery = Runner(environment)
    recovery.restore_control()
    recovery.settle(request_stop=True)
    with pytest.raises(Failure, match="Final receipt conflicts"):
        Runner(environment).finalize({**plans, "at": "2026-09-21T01:07:00Z"})
    assert environment.store.read(environment.records.path)[0] is not None
    assert environment.store.read(rt.ENVIRONMENT)[0] is not None


@pytest.mark.parametrize("reappeared", ["before", "during"])
def test_recovery_restoration_never_replaces_a_record_that_reappeared(
    prepared, monkeypatch, reappeared
):
    environment, runner = settled(prepared, True)
    lose_lock_release(environment, runner, monkeypatch)
    other = {"nonce": environment.approval.nonce, "phase": "cleaning"}
    if reappeared == "before":
        environment.store.write(environment.records.path, other)
    else:
        environment.store.before_write = lambda: environment.store.write(
            environment.records.path, other
        )
    with pytest.raises(rt.ApiError) as error:
        Runner(environment).restore_control()
    assert error.value.status == 412
    assert environment.store.read(environment.records.path)[0] == other
    assert environment.store.read(rt.ENVIRONMENT)[0] is not None


@pytest.mark.parametrize(
    "key,value", [("nonce", "foreign"), ("roots", ["foreign"]), ("empty", False)]
)
def test_refreshed_time_does_not_admit_invalid_current_plans(
    prepared, monkeypatch, key, value
):
    environment, runner = settled(prepared, False)
    plans = {
        "nonce": environment.approval.nonce,
        "roots": ["flink-gcp", "tier3-bootstrap", "tier3-operator"],
        "empty": True,
        "at": "2026-09-21T01:00:00Z",
    }
    original = environment.store.delete

    def fail_control(name, *args, **kwargs):
        if name == environment.records.path:
            raise Failure("control deletion unavailable")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(environment.store, "delete", fail_control)
    with pytest.raises(Failure, match="control deletion unavailable"):
        runner.finalize(plans)
    monkeypatch.setattr(environment.store, "delete", original)
    with pytest.raises(Failure, match="All three refreshed empty plans are required"):
        runner.finalize({**plans, "at": "2026-09-21T01:07:00Z", key: value})
    assert environment.store.read(environment.records.path)[0] is not None
    assert environment.store.read(rt.ENVIRONMENT)[0] is not None
