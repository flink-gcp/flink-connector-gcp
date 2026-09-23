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
from dataclasses import asdict, replace

import pytest
from flink_tier3 import pubsub_plan as plan
from flink_tier3.cleanup import Cleanup, verify_idle
from flink_tier3.common import Failure, digest
from flink_tier3.environment import Environment
from flink_tier3.model import Approval, Phase
from flink_tier3.policy import PUBSUB, PUBSUB_CEILINGS, PUBSUB_STATE, RECOVERY
from flink_tier3.pubsub_handoff import PubSubHandoff
from flink_tier3.pubsub_lifecycle import PubSubLifecycle
from flink_tier3.pubsub_traffic import PubSubTraffic
from flink_tier3.runner import Runner
from flink_tier3.supervisor import Supervisor
from test_pubsub_handoff import Service
from test_pubsub_plan import trial as trial  # noqa: PLC0414
from test_tier3_lifecycle import env as env  # noqa: PLC0414
from test_tier3_lifecycle import obj, rt


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
        version=5,
        scenario="pubsub-recovery",
        pubsub_trial=copy.deepcopy(trial),
        ceilings=dict(PUBSUB_CEILINGS),
        application_sha256=digest(application),
        upgrade_application_sha256="f" * 64,
    )
    value["images"].pop("smoke")
    value["images"]["application"] = application["spec"]["image"]
    environment = Environment(kube, store, value, clock, clock.sleep, actor="runner")
    return environment, application


@pytest.mark.parametrize("kind", plan.TRIALS)
@pytest.mark.parametrize("records", [2, 101, 10000])
def test_trial_roundtrip_and_derived_plan(prepared, kind, records):
    environment, _ = prepared
    value = environment.approval.to_dict()
    value["pubsub_trial"].update(trial=kind, records_per_subscription=records)
    approval = Approval.from_dict(value, environment.clock())
    assert Approval.from_dict(approval.to_dict()) == approval
    value["pubsub_trial"]["traffic_limits"]["publish_calls"] = 1
    assert approval.pubsub_trial["traffic_limits"]["publish_calls"] == 20000
    resources = approval.pubsub_plan.manifest()
    assert resources["run_id"] == approval.run_id
    assert resources["nonce"] == approval.nonce
    assert len(resources["topics"]) == len(resources["subscriptions"]) == 3
    assert asdict(approval.pubsub_traffic_limits) == {
        **approval.pubsub_trial["traffic_limits"],
        "admit_until": rt.timestamp(approval.cleanup_at),
    }
    assert approval.application_namespace == PUBSUB
    assert approval.schedule.active_seconds(approval.schedule.started) == 3420


@pytest.mark.parametrize(
    "key,value",
    [
        ("version", 4),
        ("version", 5.0),
        ("pubsub_trial", {}),
        ("upgrade_application_sha256", ""),
        ("recovery_policy", RECOVERY),
        ("bigquery_trial", {"version": 1}),
        ("cells", [{}]),
        ("runtime_sha256", "main"),
        ("application_sha256", ""),
        ("ceilings", rt.CEILINGS),
    ],
)
def test_incompatible_approval_refused(prepared, key, value):
    data = prepared[0].approval.to_dict()
    data[key] = value
    with pytest.raises(Failure):
        Approval.from_dict(data)


@pytest.mark.parametrize(
    "key,value",
    [
        ("trial", "combined"),
        ("records_per_subscription", True),
        ("records_per_subscription", 10001),
        ("traffic_limits", {}),
        ("total_request_limit", 29999),
        ("additional_cost_usd", "10.00"),
        ("unreviewed", 1),
    ],
)
def test_invalid_trial_refused(prepared, key, value):
    data = prepared[0].approval.to_dict()
    data["pubsub_trial"][key] = value
    with pytest.raises(Failure):
        Approval.from_dict(data)


@pytest.mark.parametrize(
    "counter,value",
    [
        ("input_messages", 1999),
        ("input_bytes", 1),
        ("publish_calls", 19),
        ("output_messages", 1999),
        ("pull_calls", 19),
        ("pubsub_requests", 59),
    ],
)
def test_serialized_trial_requires_one_feasible_pass(prepared, counter, value):
    data = prepared[0].approval.to_dict()
    data["pubsub_trial"]["traffic_limits"][counter] = value
    with pytest.raises(Failure, match="complete input/output pass"):
        Approval.from_dict(data)


@pytest.mark.parametrize(
    "key,value",
    [
        ("pods", 6),
        ("pods", 7.0),
        ("pvcs", False),
        ("state_bytes", 1),
        ("state_objects", 1),
        ("log_bytes", 1),
        ("evidence_bytes", 1),
        ("additional_cost_usd", "9.99"),
    ],
)
def test_fixed_ceiling_types_and_values(prepared, key, value):
    data = prepared[0].approval.to_dict()
    data["ceilings"][key] = value
    with pytest.raises(Failure, match="resource ceilings"):
        Approval.from_dict(data)


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
    with pytest.raises(Failure, match="one hour"):
        Approval.from_dict(data)


@pytest.mark.parametrize("target", ["namespace", "image", "extra-image", "expired"])
def test_approval_identity_and_admission_window(prepared, target):
    environment, _ = prepared
    data = environment.approval.to_dict()
    now = environment.clock()
    if target == "namespace":
        data["namespaces"][rt.SMOKE] = data["namespaces"].pop(PUBSUB)
    elif target == "image":
        data["images"]["application"] = rt.GAR + "smoke@sha256:" + "e" * 64
    elif target == "extra-image":
        data["images"]["smoke"] = rt.GAR + "smoke@sha256:" + "e" * 64
    else:
        now = environment.schedule.cleanup_at
    with pytest.raises(Failure):
        Approval.from_dict(data, now)


def test_smoke_roundtrip_cannot_carry_pubsub_trial(env, trial):
    value = env[2]
    approval = Approval.from_dict(value)
    assert "pubsub_trial" not in approval.to_dict()
    assert Approval.from_dict(approval.to_dict()) == approval
    value["pubsub_trial"] = trial
    with pytest.raises(Failure, match="Only Pub/Sub"):
        Approval.from_dict(value)


@pytest.mark.parametrize(
    "change", ["none", "limits", "deadline", "records", "invalid-approval"]
)
def test_helpers_bind_version_five_before_any_io(prepared, change):
    environment, application = prepared
    limits = environment.approval.pubsub_traffic_limits
    if change == "limits":
        limits = replace(limits, publish_calls=limits.publish_calls - 1)
    elif change == "deadline":
        limits = replace(limits, admit_until=limits.admit_until - 1)
    elif change == "records":
        application["spec"]["job"]["args"][-1] = "--records-per-subscription=999"
        environment.approval = replace(
            environment.approval, application_sha256=digest(application)
        )
    elif change == "invalid-approval":
        environment.approval = replace(environment.approval, pubsub_trial={})

    def construct():
        controller = PubSubLifecycle(
            environment, None, application, lambda *a: pytest.fail("Unexpected I/O")
        )
        return PubSubTraffic(controller, application, limits)

    if change == "none":
        assert construct().records == 1000
    else:
        with pytest.raises(Failure):
            construct()


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
    assert cleanup.ceilings["pods"] == 7
    verify_idle(environment)


def test_runnable_entrypoints_remain_disabled(prepared):
    environment, application = prepared
    with pytest.raises(Failure, match="Pub/Sub execution admission is not implemented"):
        Runner(environment).start(None, None, application)
    with pytest.raises(
        Failure, match="Pub/Sub recovery supervision is not implemented"
    ):
        Supervisor(environment).supervise("unused-pod")


def settled(prepared, service_intent):
    environment, application = prepared
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
        controller = PubSubLifecycle(env, service, application, lambda *a: None)
        return PubSubHandoff(
            PubSubTraffic(controller, application, env.approval.pubsub_traffic_limits),
            actor_token=token,
        )

    sender, observer = actor(environment, "b" * 32), actor(supervisor, "c" * 32)
    sender.initialize()
    sender.prepare()
    environment.records.set_phase(Phase.READY)
    observer.join()
    sender.release()
    Cleanup(supervisor, pubsub=observer, quiesce=lambda: True).run("synthetic stop")
    assert environment.refresh().pubsub["stage"] == "cleaned"
    return environment, runner


@pytest.mark.parametrize(
    "change", ["none", "trial", "scenario", "success", "plans", "service"]
)
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
    if change == "trial":
        receipt["pubsub_trial"]["trial"] = "jm-replacement"
    elif change == "scenario":
        receipt.pop("scenario")
    elif change == "success":
        receipt["success"] = True
    elif change == "plans":
        receipt["plans"]["roots"] = ["unapproved"]
    elif change == "service":
        receipt.setdefault("pubsub", {})["stage"] = "foreign"
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


def test_audit_accepts_both_replacements_and_rejects_eighth_pod(seven_pods):
    environment, cleanup = seven_pods
    assert len(cleanup.audit()[1]) == 7
    workload_pod(
        environment, "extra-tm", "application", environment.root("application")
    )
    with pytest.raises(Failure, match="Pod/PVC count"):
        cleanup.audit()


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


def test_state_byte_ceiling_uses_pubsub_bucket(seven_pods, monkeypatch):
    environment, cleanup = seven_pods
    original = environment.store.objects

    def oversized(prefix, bucket=rt.EVIDENCE, maximum=10000):
        if bucket == PUBSUB_STATE:
            return [
                {"name": prefix + "large", "size": str(1024**3 + 1), "generation": "1"}
            ]
        return original(prefix, bucket, maximum)

    monkeypatch.setattr(environment.store, "objects", oversized)
    with pytest.raises(Failure, match="State byte ceiling"):
        cleanup.audit()


def test_concurrent_control_change_retains_control_and_lock(prepared):
    environment, _ = prepared
    runner = Runner(environment)
    runner.cleanup.run("stopped before creation")
    environment.store.before_write = environment.records.mark_evidence_failed
    with pytest.raises(Failure, match="Run control changed"):
        runner.finalize(
            {
                "nonce": environment.approval.nonce,
                "roots": ["flink-gcp", "tier3-bootstrap", "tier3-operator"],
                "empty": True,
            }
        )
    assert environment.store.read(environment.records.path)[0] is not None
    assert environment.store.read(rt.ENVIRONMENT)[0] is not None


@pytest.mark.parametrize("service_intent", [False, True])
def test_retry_after_lock_failure_uses_reconstructed_control(
    prepared, monkeypatch, service_intent
):
    environment, runner = settled(prepared, service_intent)
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
    assert environment.store.read(environment.records.path)[0] is None
    assert environment.store.read(rt.ENVIRONMENT)[0] is not None
    # `lifecycle.recover` writes this placeholder record when control is missing.
    # It finalizes a run that never reached service intent, and cannot reproduce
    # a cleaned Pub/Sub portion, so a run that created one still needs the
    # verified snapshot from a recovery procedure this test does not provide.
    environment.store.write(
        environment.records.path,
        control
        if service_intent
        else {
            "nonce": environment.approval.nonce,
            "phase": "cleaned",
            "idle": True,
            "state_clean": False,
            "success": False,
        },
    )
    monkeypatch.setattr(environment.store, "delete", original_delete)
    assert (
        Runner(environment).finalize({**plans, "at": "2026-09-21T01:07:00Z"}) is False
    )
    assert environment.store.read(environment.records.path)[0] is None
    assert environment.store.read(rt.ENVIRONMENT)[0] is None


def test_minimal_recovery_record_cannot_finalize_a_cleaned_service(
    prepared, monkeypatch
):
    environment, runner = settled(prepared, True)
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
    assert environment.store.read(environment.records.path)[0] is None
    environment.store.write(
        environment.records.path,
        {
            "nonce": environment.approval.nonce,
            "phase": "cleaned",
            "idle": True,
            "state_clean": False,
            "success": False,
        },
    )
    monkeypatch.setattr(environment.store, "delete", original_delete)
    with pytest.raises(Failure, match="Final receipt conflicts"):
        Runner(environment).finalize({**plans, "at": "2026-09-21T01:07:00Z"})
    assert environment.store.read(environment.records.path)[0] is not None
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
