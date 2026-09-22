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
"""BigQuery approval, namespace and budget contracts without cloud execution."""

import copy
import json
from dataclasses import asdict, replace

import pytest
from flink_tier3 import bigquery_plan as plan
from flink_tier3 import runtime
from flink_tier3.bigquery_handoff import BigQueryHandoff
from flink_tier3.bigquery_lifecycle import BigQueryLifecycle
from flink_tier3.bundle import delivery_digest, source_digest
from flink_tier3.cleanup import Cleanup, verify_idle
from flink_tier3.common import INCONCLUSIVE, Failure, digest
from flink_tier3.environment import Environment
from flink_tier3.model import Approval, Phase
from flink_tier3.policy import (
    BIGQUERY,
    BIGQUERY_CEILINGS,
    BIGQUERY_STATE,
    RECOVERY,
)
from flink_tier3.runner import Runner
from flink_tier3.supervisor import Supervisor
from test_bigquery_lifecycle import Resources
from test_bigquery_plan import inputs as inputs  # noqa: PLC0414
from test_bigquery_plan import renderer as renderer  # noqa: PLC0414
from test_bigquery_plan import trial as trial  # noqa: PLC0414
from test_cloudtasks_session import session_approval
from test_tier3_lifecycle import env as env  # noqa: PLC0414
from test_tier3_lifecycle import obj, rt


@pytest.fixture
def prepared(env, inputs, renderer):
    kube, store, approval, clock = env
    inputs.update(run_id=approval["run_id"], nonce=approval["nonce"])
    bundle = plan.prepare(**inputs)
    proposal = bundle["proposal"]
    clock.now = rt.timestamp(inputs["started_at"])
    application = bundle["application"]
    application.update(apiVersion="flink.apache.org/v1beta1", kind="FlinkDeployment")
    upgrade = copy.deepcopy(application)
    upgrade["spec"]["job"]["args"][3] = "upgrade"
    upgrade["spec"]["job"]["args"][-1] = "true"
    saved = approval["namespaces"].pop(rt.SMOKE)
    quota = kube.get("ResourceQuota", rt.SMOKE, "tier3-idle")
    quota["metadata"].update(namespace=BIGQUERY, uid=BIGQUERY + "-quota")
    kube.put(quota)
    saved.update(uid=BIGQUERY + "-uid", quota_uid=BIGQUERY + "-quota")
    approval["namespaces"][BIGQUERY] = saved
    approval.update(
        version=4,
        scenario="bigquery-recovery",
        started_at=proposal["started_at"],
        expires_at=proposal["expires_at"],
        cleanup_at=proposal["cleanup_at"],
        ceilings={
            **BIGQUERY_CEILINGS,
            "additional_cost_usd": inputs["trial"]["additional_cost_usd"],
        },
        bigquery_trial=copy.deepcopy(inputs["trial"]),
        application_sha256=digest(application),
        upgrade_application_sha256=digest(upgrade),
    )
    approval["images"].pop("smoke")
    approval["images"]["application"] = inputs["application_image"]
    environment = Environment(kube, store, approval, clock, clock.sleep, actor="runner")
    return environment, application, upgrade, proposal


@pytest.mark.parametrize("mode", ["ALO", "EO"])
@pytest.mark.parametrize("destinations", [10, 50])
def test_approval_and_proposal_share_exact_service_plan(prepared, mode, destinations):
    environment, _, _, _ = prepared
    value = environment.approval.to_dict()
    value["bigquery_trial"].update(mode=mode, destinations=destinations)
    approval = Approval.from_dict(value, environment.clock())
    expected = plan.prepare(
        run_id=approval.run_id,
        nonce=approval.nonce,
        started_at=approval.started_at,
        expires_at=approval.expires_at,
        active_seconds=5220,
        revision=approval.sha,
        application_image=approval.images["application"],
        trial=approval.bigquery_trial,
    )["proposal"]
    assert asdict(approval.bigquery_plan) == expected["resources"]
    assert approval.application_namespace == BIGQUERY
    assert approval.schedule.cleanup_at - approval.schedule.started == 4500
    assert rt.timestamp(approval.cleanup_at) == approval.schedule.cleanup_at
    assert expected["cleanup_at"] == approval.cleanup_at
    assert approval.schedule.job_deadline - approval.schedule.started == 5220
    assert expected["limits"]["cleanup_seconds"] == BIGQUERY_CEILINGS["cleanup_seconds"]
    assert expected["limits"]["pods"] == BIGQUERY_CEILINGS["pods"]
    assert expected["limits"]["pvcs"] == BIGQUERY_CEILINGS["pvcs"]
    assert plan.ACTIVE_SECONDS == approval.schedule.active_seconds(
        approval.schedule.started
    )
    assert Approval.from_dict(approval.to_dict()) == approval
    value["bigquery_trial"]["query_slots"] = 1
    assert approval.bigquery_plan.query_slots == 12


@pytest.mark.parametrize(
    "key,value",
    [
        ("version", 4.0),
        ("version", 3),
        ("upgrade_application_sha256", ""),
        ("bigquery_trial", {}),
        ("recovery_policy", RECOVERY),
        ("campaign", "foreign"),
        ("flink_version", "1.20.4"),
        ("expires_at", "2026-09-21T01:00:00Z"),
        ("cleanup_at", "2026-09-21T01:20:00Z"),
        ("started_at", "2026-09-21T00:00:00.5Z"),
    ],
)
def test_inconsistent_approval_is_rejected(prepared, key, value):
    environment, *_ = prepared
    approval = environment.approval.to_dict()
    approval[key] = value
    with pytest.raises(Failure):
        Approval.from_dict(approval, environment.clock())
    assert environment.kube.calls == []


@pytest.mark.parametrize(
    "key,value",
    [
        ("mode", "FILE_LOADS"),
        ("destinations", 11),
        ("repetition", 4),
        ("query_slots", 13),
        ("maximum_bytes_billed", 4 * 1024**3 + 1),
        ("query_timeout_ms", 60001),
        ("additional_cost_usd", "1.00"),
        ("additional_cost_usd", "10.01"),
        ("records", 1),
    ],
)
def test_unreviewed_trial_is_rejected(prepared, key, value):
    environment, *_ = prepared
    approval = environment.approval.to_dict()
    approval["bigquery_trial"][key] = value
    with pytest.raises(Failure, match="approved BigQuery trial"):
        Approval.from_dict(approval)


@pytest.mark.parametrize(
    "key,value",
    [
        ("pods", 7),
        ("pods", 6.0),
        ("pvcs", False),
        ("state_bytes", 2**31),
        ("additional_cost_usd", "10.00"),
        ("evidence_bytes", 2**30),
    ],
)
def test_ceiling_drift_is_rejected(prepared, key, value):
    approval = prepared[0].approval.to_dict()
    approval["ceilings"][key] = value
    with pytest.raises(Failure, match="resource and cost ceilings"):
        Approval.from_dict(approval)


def test_wrong_namespace_image_and_runtime_binding_are_rejected(prepared):
    original = prepared[0].approval.to_dict()
    for edit in (
        lambda a: a["namespaces"].update({rt.SMOKE: a["namespaces"].pop(BIGQUERY)}),
        lambda a: a["images"].update(application=rt.GAR + "smoke@sha256:" + "e" * 64),
        lambda a: a.update(runtime_sha256="bad"),
    ):
        value = copy.deepcopy(original)
        edit(value)
        with pytest.raises(Failure):
            Approval.from_dict(value)


def test_old_approval_cannot_smuggle_bigquery_fields(env, trial):
    env[2]["bigquery_trial"] = trial
    with pytest.raises(Failure, match="Only BigQuery"):
        Approval.from_dict(env[2])


def test_admission_window_and_pricing_review_are_enforced(prepared):
    environment, *_ = prepared
    value = environment.approval.to_dict()
    for now in (environment.schedule.started - 31, environment.schedule.cleanup_at):
        with pytest.raises(Failure, match="admission window"):
            Approval.from_dict(value, now)
    value.update(
        started_at="2026-11-21T00:00:00Z",
        expires_at="2026-11-21T01:30:00Z",
        cleanup_at="2026-11-21T01:15:00Z",
    )
    with pytest.raises(Failure, match="pricing review"):
        Approval.from_dict(value)


def test_three_application_pods_and_the_control_pods_fit_their_quotas(prepared):
    environment, *_ = prepared
    cleanup = Cleanup(environment)
    cleanup.quota(BIGQUERY, "run")
    cleanup.quota(rt.SYSTEM, "run")
    # Two control Pods run; `tier3-system`'s third is the slot a replacement
    # occupies while the Pod it replaces terminates.
    for namespace, count in ((BIGQUERY, 3), (rt.SYSTEM, 3)):
        hard = environment.kube.get("ResourceQuota", namespace, "tier3-idle")["spec"][
            "hard"
        ]
        assert hard["pods"] == str(count)
        assert hard["persistentvolumeclaims"] == "0"
        assert rt.quantity(hard["requests.cpu"]) == count
        assert rt.quantity(hard["limits.memory"]) == count * 2 * 1024**3
    assert (
        environment.kube.get("ResourceQuota", rt.SMOKE, "tier3-idle")["spec"]["hard"][
            "pods"
        ]
        == "0"
    )
    cleanup.quota(BIGQUERY, None)
    cleanup.quota(rt.SYSTEM, None)
    verify_idle(environment)


def test_creation_and_uncertain_adoption_use_bigquery_namespace(prepared):
    environment, application, *_ = prepared
    runner = Runner(environment)
    environment.kube.fail_create_response = True
    with pytest.raises(Failure, match="Lost response"):
        runner.create_application(application)
    assert environment.root("application")["metadata"]["namespace"] == BIGQUERY
    assert (
        environment.kube.get("FlinkDeployment", rt.SMOKE, environment.approval.run_id)
        is None
    )
    other = copy.deepcopy(application)
    other["metadata"]["annotations"][rt.NONCE] = "b" * 32
    environment.kube.put(other)
    with pytest.raises(Failure, match="nonce differs"):
        runner.adopt_application(application)


def test_state_cleanup_uses_only_bigquery_bucket_and_run_prefix(prepared):
    environment, *_ = prepared
    prefix = f"runs/{environment.approval.run_id}/"
    targets = [
        (prefix + "checkpoint", BIGQUERY_STATE),
        (prefix + "checkpoint", rt.STATE),
        (prefix + "checkpoint", rt.EVIDENCE),
        ("runs/other/checkpoint", BIGQUERY_STATE),
    ]
    for path, bucket in targets:
        environment.store.write(path, {"saved": True}, bucket=bucket)
    cleanup = Cleanup(environment)
    assert [(o["name"], b) for o, b in cleanup.remaining_state()] == [targets[0]]
    cleanup.clean_state()
    assert environment.store.read(*targets[0])[0] is None
    for path, bucket in targets[1:]:
        assert environment.store.read(path, bucket)[0] == {"saved": True}


@pytest.mark.parametrize(
    "field,value",
    [
        ("query_slots", 1),
        ("maximum_bytes_billed", 1024**3),
        ("query_timeout_ms", 1),
        ("expires_ms", 1790000000000),
    ],
)
def test_resource_controller_cannot_substitute_plan(prepared, field, value):
    environment, application, *_ = prepared
    altered = replace(environment.approval.bigquery_plan, **{field: value})
    api = Resources(altered)
    with pytest.raises(Failure, match="resources differ"):
        BigQueryLifecycle(environment, api, application)
    assert api.calls == []


# The verdict the exercise left behind, including none at all and a record
# that is not one: `recovery` is durable JSON that nothing types on the way in,
# and this runs inside the finalizer that releases the environment lock.
@pytest.mark.parametrize(
    "stored",
    [
        {"stage": "complete"},
        {"stage": "complete", "verdict": INCONCLUSIVE},
        {"stage": "complete", "verdict": rt.USABLE},
        ["complete"],
        "complete",
    ],
)
def test_real_environment_and_handoff_complete_owned_cleanup(prepared, stored):
    environment, application, *_ = prepared
    runner = Runner(environment)
    api = Resources(environment.approval.bigquery_plan)
    handoff = BigQueryHandoff(
        BigQueryLifecycle(environment, api, application),
        runner_token="b" * 32,
        evidence_bytes=2 * rt.MIB,
        query_until=environment.schedule.cleanup_at,
    )
    handoff.initialize()
    handoff.provision()
    runner.cleanup.quota(BIGQUERY, "run")
    runner.create_application(application)
    environment.records.set_phase(Phase.READY)
    environment.records.set_phase(Phase.RUNNING, success=True)
    control, generation = environment.records.read()
    control.recovery = stored
    environment.store.write(environment.records.path, control.to_dict(), generation)
    handoff.release()
    supervisor = Environment(
        environment.kube,
        environment.store,
        environment.approval,
        environment.clock,
        environment.sleep,
    )
    observer = BigQueryHandoff(
        BigQueryLifecycle(supervisor, api, application),
        **{
            k: handoff.binding[k]
            for k in ("runner_token", "evidence_bytes", "query_until")
        },
    )
    Cleanup(supervisor, bigquery=observer, quiesce=lambda: True).run(
        "synthetic cleanup"
    )
    assert not api.tables
    assert supervisor.refresh().phase == Phase.CLEANED
    assert supervisor.refresh().bigquery["cleaned"]
    verify_idle(supervisor)
    # A completion flag from the generic executor must not authorize this trial.
    control, generation = environment.records.read()
    control.success = True
    control.evidence_failed = False
    environment.store.write(environment.records.path, control.to_dict(), generation)
    assert not environment.evidence_failed
    runner.finalize(
        {
            "nonce": environment.approval.nonce,
            "roots": ["flink-gcp", "tier3-bootstrap", "tier3-operator"],
            "empty": True,
        }
    )
    receipt, _ = environment.store.read(
        f"runs/{environment.approval.run_id}/result.json"
    )
    assert receipt["idle"]
    # A completion flag alone is not the claim; the exercise's verdict is, and
    # a record that is not a record reads as not usable rather than raising.
    usable = isinstance(stored, dict) and stored.get("verdict") == rt.USABLE
    assert receipt["success"] is usable
    assert receipt["scenario"] == "bigquery-recovery"
    assert receipt["bigquery_trial"] == environment.approval.bigquery_trial
    assert receipt["recovery"] == stored

    assert receipt["bigquery"]["cleaned"]
    assert environment.store.read(environment.records.path)[0] is None
    assert environment.store.read(rt.ENVIRONMENT)[0] is None


def test_unimplemented_execution_refuses_before_mutation(prepared, tmp_path):
    environment, application, *_ = prepared
    before = copy.deepcopy(environment.store.data)
    with pytest.raises(Failure, match="admission is not implemented"):
        Runner(environment).start({}, {}, application)
    with pytest.raises(Failure, match="supervision is not implemented"):
        Supervisor(environment).supervise("unused")
    assert environment.kube.calls == []
    assert environment.store.data == before
    approval = environment.approval.to_dict()
    approval["runtime_sha256"] = source_digest()
    approval["delivery_sha256"] = delivery_digest()
    (tmp_path / "approval.json").write_text(json.dumps(approval))
    (tmp_path / "application.json").write_text(json.dumps(application))
    with pytest.raises(Failure, match="supervision is not implemented"):
        runtime.supervisor_main(tmp_path)


@pytest.mark.parametrize("application", [rt.SMOKE, rt.CLOUDTASKS, BIGQUERY])
def test_live_inventory_scope_matches_approval(application):
    calls = []
    kube = rt.Kubernetes("https://example.invalid", None)
    kube.items = lambda kind, namespace: calls.append((kind, namespace)) or []
    assert kube.inventory(application) == []
    expected = {rt.SMOKE, rt.CLOUDTASKS, rt.SYSTEM}
    if application == BIGQUERY:
        expected.add(BIGQUERY)
    assert {namespace for _, namespace in calls} == expected
    calls.clear()
    assert kube.inventory() == []
    assert {namespace for _, namespace in calls} == {rt.SMOKE, rt.CLOUDTASKS, rt.SYSTEM}


@pytest.mark.parametrize("version", [1, 2, 3])
def test_legacy_recovery_keeps_its_original_inventory_baseline(env, version):
    kube, store, approval, clock = env
    if version == 2:
        approval.update(
            version=2,
            scenario="generic-recovery",
            recovery_policy=copy.deepcopy(RECOVERY),
            upgrade_application_sha256="f" * 64,
        )
    elif version == 3:
        session_approval(env)
    existing = obj("ConfigMap", "kube-root-ca.crt", BIGQUERY)
    kube.put(existing)
    environment = Environment(kube, store, approval, clock, clock.sleep)
    Cleanup(environment).audit()
    verify_idle(environment)
    assert kube.get("ConfigMap", BIGQUERY, "kube-root-ca.crt") == existing


def test_bigquery_inventory_still_refuses_unowned_namespace_objects(prepared):
    environment, *_ = prepared
    environment.kube.put(obj("ConfigMap", "foreign", BIGQUERY))
    with pytest.raises(Failure, match="outside the baseline"):
        Cleanup(environment).audit()
    with pytest.raises(Failure, match="non-baseline"):
        verify_idle(environment)


def workload_pod(environment, name, role, parent):
    namespace = BIGQUERY if role == "application" else rt.SYSTEM
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


def five_pods(environment, application):
    runner = Runner(environment)
    runner.create_application(application)
    job = environment.kube.put(obj("Job", "supervisor", rt.SYSTEM))
    environment.remember("supervisor", job)
    workload_pod(
        environment,
        "operator",
        "operator",
        environment.kube.get("Deployment", rt.SYSTEM, rt.OPERATOR),
    )
    workload_pod(environment, "supervisor", "supervisor", job)
    for name in ("jm", "tm-0", "tm-1"):
        workload_pod(environment, name, "application", environment.root("application"))
    return runner.cleanup


def test_inventory_accepts_the_budgeted_pods_and_rejects_one_more(prepared):
    environment, application, *_ = prepared
    cleanup = five_pods(environment, application)
    assert len(cleanup.audit()[1]) == 5
    # The sixth is the slot a replacement occupies while the Pod it replaces
    # terminates; without it the Deployment's own recovery is refused.
    workload_pod(
        environment, "extra-tm", "application", environment.root("application")
    )
    assert len(cleanup.audit()[1]) == 6
    workload_pod(
        environment, "seventh-tm", "application", environment.root("application")
    )
    with pytest.raises(Failure, match="Pod/PVC count"):
        cleanup.audit()


@pytest.mark.parametrize("change", ["image", "resources", "unowned"])
def test_inventory_rejects_unapproved_bigquery_pod(prepared, change):
    environment, application, *_ = prepared
    cleanup = five_pods(environment, application)
    pod = environment.kube.get("Pod", BIGQUERY, "tm-0")
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


def test_state_byte_ceiling_is_checked_in_bigquery_bucket(prepared, monkeypatch):
    environment, application, *_ = prepared
    cleanup = five_pods(environment, application)
    objects = environment.store.objects

    def oversized(prefix, bucket=rt.EVIDENCE, maximum=10000):
        if bucket == BIGQUERY_STATE:
            return [
                {
                    "name": prefix + "large",
                    "size": str(BIGQUERY_CEILINGS["state_bytes"] + 1),
                    "generation": "1",
                }
            ]
        return objects(prefix, bucket, maximum)

    monkeypatch.setattr(environment.store, "objects", oversized)
    with pytest.raises(Failure, match="State byte ceiling"):
        cleanup.audit()


@pytest.mark.parametrize(
    "change", ["none", "scenario", "bigquery_trial", "success", "plans"]
)
def test_final_receipt_retry_without_service_intent(prepared, monkeypatch, change):
    environment, *_ = prepared
    runner = Runner(environment)
    runner.cleanup.run("stopped before creation")
    plans = {
        "nonce": environment.approval.nonce,
        "roots": ["flink-gcp", "tier3-bootstrap", "tier3-operator"],
        "empty": True,
        "at": "2026-09-21T01:30:00Z",
    }
    original_delete = environment.store.delete

    def lose_control_delete(name, *args, **kwargs):
        if name == environment.records.path:
            raise Failure("control deletion unavailable")
        return original_delete(name, *args, **kwargs)

    monkeypatch.setattr(environment.store, "delete", lose_control_delete)
    with pytest.raises(Failure, match="control deletion unavailable"):
        runner.finalize(plans)
    path = f"runs/{environment.approval.run_id}/result.json"
    receipt, generation = environment.store.read(path)
    assert "bigquery" not in receipt
    assert receipt["scenario"] == "bigquery-recovery"
    assert receipt["bigquery_trial"] == environment.approval.bigquery_trial
    assert not receipt["success"]
    if change == "scenario":
        receipt.pop("scenario")
    elif change == "bigquery_trial":
        receipt["bigquery_trial"]["repetition"] = 3
    elif change == "success":
        receipt["success"] = True
    elif change == "plans":
        receipt["plans"]["roots"] = ["unapproved"]
    environment.store.write(path, receipt, generation)
    monkeypatch.setattr(environment.store, "delete", original_delete)
    plans = {**plans, "at": "2026-09-21T01:37:31Z"}
    if change == "none":
        assert runner.finalize(plans) is False
        assert environment.store.read(path)[0] == receipt
        assert environment.store.read(environment.records.path)[0] is None
        assert environment.store.read(rt.ENVIRONMENT)[0] is None
    else:
        with pytest.raises(Failure, match="Final receipt conflicts"):
            runner.finalize(plans)
        assert environment.store.read(environment.records.path)[0] is not None
        assert environment.store.read(rt.ENVIRONMENT)[0] is not None


def test_finalization_without_service_intent_keeps_concurrent_control(prepared):
    environment, *_ = prepared
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
    control, _ = environment.store.read(environment.records.path)
    assert control["evidence_failed"]
    assert environment.store.read(rt.ENVIRONMENT)[0] is not None


def test_final_receipt_retry_after_control_deleted_before_lock_release(
    prepared, monkeypatch
):
    environment, *_ = prepared
    runner = Runner(environment)
    runner.cleanup.run("stopped before creation")
    plans = {
        "nonce": environment.approval.nonce,
        "roots": ["flink-gcp", "tier3-bootstrap", "tier3-operator"],
        "empty": True,
        "at": "2026-09-21T01:30:00Z",
    }
    original_delete = environment.store.delete

    def lose_lock_release(name, *args, **kwargs):
        if name == rt.ENVIRONMENT:
            raise Failure("lock release unavailable")
        return original_delete(name, *args, **kwargs)

    monkeypatch.setattr(environment.store, "delete", lose_lock_release)
    with pytest.raises(Failure, match="lock release unavailable"):
        runner.finalize(plans)
    assert environment.store.read(environment.records.path)[0] is None
    assert environment.store.read(rt.ENVIRONMENT)[0] is not None
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
    plans = {**plans, "at": "2026-09-21T01:37:31Z"}
    assert Runner(environment).finalize(plans) is False
    assert environment.store.read(environment.records.path)[0] is None
    assert environment.store.read(rt.ENVIRONMENT)[0] is None
    receipt, _ = environment.store.read(
        f"runs/{environment.approval.run_id}/result.json"
    )
    assert receipt["plans"]["at"] == "2026-09-21T01:30:00Z"


def test_the_four_allowances_partition_the_approved_evidence_ceiling():
    """Four figures in one place, summing to the ceiling they divide."""
    from flink_tier3.policy import BIGQUERY_CEILINGS, MIB

    parts = (
        "receipt_bytes_supervisor",
        "receipt_bytes_runner",
        "query_bytes",
        "artifact_bytes",
    )
    assert (
        sum(BIGQUERY_CEILINGS[part] for part in parts)
        == (BIGQUERY_CEILINGS["evidence_bytes"])
    )
    # The handoff and the receipts read those entries rather than literals.
    assert BIGQUERY_CEILINGS["query_bytes"] == 10 * MIB
    assert BIGQUERY_CEILINGS["artifact_bytes"] == 2 * MIB
