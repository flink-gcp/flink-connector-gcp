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
from flink_tier3 import runtime
from flink_tier3.bigquery import plan
from flink_tier3.bigquery.handoff import BigQueryHandoff
from flink_tier3.bigquery.lifecycle import BigQueryLifecycle
from flink_tier3.cleanup import Cleanup, verify_idle
from flink_tier3.common import INCONCLUSIVE, Failure, digest
from flink_tier3.environment import Environment
from flink_tier3.model import (
    APPROVAL_VERSION,
    Approval,
    Phase,
    check_window,
    validate_approval,
)
from flink_tier3.policy import BIGQUERY, BIGQUERY_STATE, CLEANUP_SECONDS
from flink_tier3.runner import Runner
from flink_tier3.supervisor import Supervisor

from ..test_lifecycle import env as env  # noqa: PLC0414
from ..test_lifecycle import obj, rt
from .test_lifecycle import Resources
from .test_plan import inputs as inputs  # noqa: PLC0414
from .test_plan import renderer as renderer  # noqa: PLC0414
from .test_plan import trial as trial  # noqa: PLC0414


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
    upgrade["spec"]["job"]["args"] = bundle["upgrade_application"]["spec"]["job"][
        "args"
    ]
    saved = approval["namespaces"].pop(rt.SMOKE)
    quota = kube.get("ResourceQuota", rt.SMOKE, "tier3-idle")
    quota["metadata"].update(namespace=BIGQUERY, uid=BIGQUERY + "-quota")
    kube.put(quota)
    saved.update(uid=BIGQUERY + "-uid", quota_uid=BIGQUERY + "-quota")
    approval["namespaces"][BIGQUERY] = saved
    approval.update(
        version=APPROVAL_VERSION,
        scenario="bigquery-recovery",
        started_at=proposal["started_at"],
        expires_at=proposal["expires_at"],
        cleanup_at=proposal["cleanup_at"],
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
    approval = Approval.from_dict(value)
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
    assert expected["limits"]["cleanup_seconds"] == CLEANUP_SECONDS
    assert plan.ACTIVE_SECONDS == approval.schedule.active_seconds(
        approval.schedule.started
    )
    assert Approval.from_dict(approval.to_dict()) == approval
    value["bigquery_trial"]["mode"] = "EO" if mode == "ALO" else "ALO"
    assert approval.bigquery_plan.trial.mode == mode


@pytest.mark.parametrize(
    "key,value",
    [
        ("upgrade_application_sha256", ""),
        # A retired field is one dispatch no longer writes.
        ("recovery_policy", {}),
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
    validate_approval(approval)
    approval[key] = value
    with pytest.raises(Failure):
        validate_approval(approval)
    assert environment.kube.calls == []


@pytest.mark.parametrize(
    "key,value",
    [
        ("mode", "STORAGE_WRITE"),
        ("destinations", 11),
        ("version", 1),
        ("query_slots", 1),
        ("additional_cost_usd", "10.00"),
        ("records", 1),
    ],
)
def test_unreviewed_trial_is_rejected(prepared, key, value):
    environment, *_ = prepared
    approval = environment.approval.to_dict()
    approval["bigquery_trial"][key] = value
    # The plan is derived from the trial, so no actor can bind an unreviewed one.
    with pytest.raises(ValueError, match="Trial must select"):
        _ = Approval.from_dict(approval).bigquery_plan


def test_wrong_namespace_and_image_are_rejected(prepared):
    original = prepared[0].approval.to_dict()
    validate_approval(original)
    for edit in (
        lambda a: a["namespaces"].update({rt.SMOKE: a["namespaces"].pop(BIGQUERY)}),
        lambda a: a["images"].update(application=rt.GAR + "smoke@sha256:" + "e" * 64),
    ):
        value = copy.deepcopy(original)
        edit(value)
        with pytest.raises(Failure):
            validate_approval(value)


def test_admission_window_is_enforced_and_the_rates_age_is_not(prepared):
    environment, *_ = prepared
    value = environment.approval.to_dict()
    for now in (environment.schedule.started - 31, environment.schedule.cleanup_at):
        with pytest.raises(Failure, match="admission window"):
            check_window(Approval.from_dict(value), now)
    # Spend is approved from the estimate before dispatch; a run long after
    # the rates were reviewed is admitted like any other.
    value.update(
        started_at="2027-01-21T00:00:00Z",
        expires_at="2027-01-21T01:30:00Z",
        cleanup_at="2027-01-21T01:15:00Z",
    )
    check_window(Approval.from_dict(value), rt.timestamp(value["started_at"]))


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
    # Flink's GCS writer stages an upload under .inprogress/<bucket>/<object>/.
    staged = f".inprogress/{BIGQUERY_STATE}/{prefix}chk-1/_metadata/upload"
    targets = [
        (prefix + "checkpoint", BIGQUERY_STATE),
        (staged, BIGQUERY_STATE),
        (prefix + "checkpoint", rt.STATE),
        (prefix + "checkpoint", rt.EVIDENCE),
        ("runs/other/checkpoint", BIGQUERY_STATE),
        (f".inprogress/{BIGQUERY_STATE}/runs/other/upload", BIGQUERY_STATE),
    ]
    for path, bucket in targets:
        environment.store.write(path, {"saved": True}, bucket=bucket)
    cleanup = Cleanup(environment)
    assert [(o["name"], b) for o, b in cleanup.remaining_state()] == targets[:2]
    cleanup.clean_state()
    for path, bucket in targets[:2]:
        assert environment.store.read(path, bucket)[0] is None
    for path, bucket in targets[2:]:
        assert environment.store.read(path, bucket)[0] == {"saved": True}


def test_staged_uploads_share_the_run_state_object_ceiling(prepared):
    environment, *_ = prepared
    prefix = f"runs/{environment.approval.run_id}/"
    for name in ("a", "b"):
        environment.store.write(prefix + name, {}, bucket=BIGQUERY_STATE)
        environment.store.write(
            f".inprogress/{BIGQUERY_STATE}/{prefix}{name}", {}, bucket=BIGQUERY_STATE
        )
    cleanup = Cleanup(environment)
    assert len(cleanup.remaining_state(maximum=4)) == 4
    with pytest.raises(Failure, match="ceiling"):
        cleanup.remaining_state(maximum=3)


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
    environment, runner, api, supervisor = service_cleaned(prepared, stored)
    assert not api.tables
    assert supervisor.refresh().phase == Phase.CLEANED
    assert supervisor.refresh().bigquery["cleaned"]
    verify_idle(supervisor)
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


def service_cleaned(prepared, stored):
    """A run whose BigQuery portion is cleaned, with `stored` as its verdict."""
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
    # A completion flag from the generic executor must not authorize this trial.
    control, generation = environment.records.read()
    control.success = True
    control.evidence_failed = False
    environment.store.write(environment.records.path, control.to_dict(), generation)
    assert not environment.evidence_failed
    return environment, runner, api, supervisor


def test_an_actor_without_its_authenticated_handoff_refuses_before_mutation(
    prepared,
):
    """Enabled, but only through the actor factories: a bare actor still refuses."""
    environment, application, *_ = prepared
    before = copy.deepcopy(environment.store.data)
    with pytest.raises(Failure, match="requires its authenticated handoff"):
        Runner(environment).start({}, {}, application)
    with pytest.raises(Failure, match="requires its authenticated exercise"):
        Supervisor(environment).supervise("unused")
    assert environment.kube.calls == []
    assert environment.store.data == before


def mount(directory, environment, application, upgrade, **approval_changes):
    approval = environment.approval.to_dict()
    approval.update(approval_changes)
    (directory / "approval.json").write_text(json.dumps(approval))
    (directory / "application.json").write_text(json.dumps(application))
    if upgrade is not None:
        (directory / "upgrade-application.json").write_text(json.dumps(upgrade))


def test_a_valid_bigquery_delivery_is_admitted_by_the_entrypoint(prepared, tmp_path):
    """The positive case the refusal tests could not show.

    A BigQuery application carries its own arguments, and the generic recovery
    check refused every one; this delivery is exactly what dispatch renders.
    """
    environment, application, upgrade, _ = prepared
    mount(tmp_path, environment, application, upgrade)
    _, approved, mounted_application, mounted_upgrade = runtime.verify_delivery(
        tmp_path
    )
    assert approved.scenario == "bigquery-recovery"
    assert mounted_application == application
    assert mounted_upgrade == upgrade
    # Off-cluster, the next thing it does is read its service account: the
    # delivery is not what stops it.
    with pytest.raises(FileNotFoundError, match="serviceaccount"):
        runtime.supervisor_main(tmp_path)


@pytest.mark.parametrize(
    "case, match",
    [
        ("missing-upgrade", "missing upgrade-application.json"),
        ("changed-upgrade", "upgrade differs from approval"),
        ("changed-application", "application differs from approval"),
    ],
)
def test_the_supervisor_entrypoint_refuses_a_delivery_its_approval_does_not_fix(
    prepared, tmp_path, case, match
):
    """The unconditional refusal became a real check, and every arm still refuses.

    Each arm refuses before the entrypoint reads its service account or builds
    a Kubernetes client, so an off-cluster run of this test is itself the proof
    that no mutation could have been reached.
    """
    environment, application, upgrade, _ = prepared
    changes = {}
    if case == "missing-upgrade":
        upgrade = None
    elif case == "changed-upgrade":
        upgrade = dict(upgrade, changed=True)
    elif case == "changed-application":
        application = dict(application, changed=True)
    mount(tmp_path, environment, application, upgrade, **changes)
    with pytest.raises(Failure, match=match):
        runtime.supervisor_main(tmp_path)


def test_live_inventory_lists_every_namespace():
    calls = []
    kube = rt.Kubernetes("https://example.invalid", None)
    kube.items = lambda kind, namespace: calls.append((kind, namespace)) or []
    assert kube.inventory() == []
    assert {namespace for _, namespace in calls} == set(rt.NAMESPACES)
    assert BIGQUERY in rt.NAMESPACES


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


def test_inventory_accepts_the_budgeted_pods_and_a_replacement(prepared):
    environment, application, *_ = prepared
    cleanup = five_pods(environment, application)
    assert len(cleanup.audit()[1]) == 5
    # The sixth is the slot a replacement occupies while the Pod it replaces
    # terminates; without it the Deployment's own recovery is refused.
    workload_pod(
        environment, "extra-tm", "application", environment.root("application")
    )
    assert len(cleanup.audit()[1]) == 6


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


@pytest.mark.parametrize("change", ["none", "nonce", "idle"])
def test_final_receipt_retry_without_service_intent(prepared, monkeypatch, change):
    environment, *_ = prepared
    runner = Runner(environment)
    runner.cleanup.run("stopped before creation")
    plans = lose_control_delete(environment, runner, monkeypatch)
    path = f"runs/{environment.approval.run_id}/result.json"
    receipt, generation = environment.store.read(path)
    assert "bigquery" not in receipt
    assert receipt["scenario"] == "bigquery-recovery"
    assert receipt["bigquery_trial"] == environment.approval.bigquery_trial
    assert not receipt["success"]
    # A retry keeps the receipt it finds, unless it is not this run's idle one.
    if change == "nonce":
        receipt["nonce"] = "0" * 32
    elif change == "idle":
        receipt["idle"] = False
    environment.store.write(path, receipt, generation)
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
    environment.store.before_write = lambda: environment.records.mark_evidence_failed(
        {"operation": "concurrent", "cause": "synthetic"}
    )
    # The control deletion is conditional on the generation the receipt read.
    with pytest.raises(rt.ApiError) as error:
        runner.finalize(
            {
                "nonce": environment.approval.nonce,
                "roots": ["flink-gcp", "tier3-bootstrap", "tier3-operator"],
                "empty": True,
            }
        )
    assert error.value.status == 412
    control, _ = environment.store.read(environment.records.path)
    assert control["evidence_failed"]
    assert environment.store.read(rt.ENVIRONMENT)[0] is not None


def test_final_receipt_retry_after_control_deleted_before_lock_release(
    prepared, monkeypatch
):
    environment, *_ = prepared
    runner = Runner(environment)
    runner.cleanup.run("stopped before creation")
    plans = lose_lock_release(environment, runner, monkeypatch)
    recovery = Runner(environment)
    recovery.restore_control()
    recovery.settle(request_stop=True)
    plans = {**plans, "at": "2026-09-21T01:37:31Z"}
    assert Runner(environment).finalize(plans) is False
    assert environment.store.read(environment.records.path)[0] is None
    assert environment.store.read(rt.ENVIRONMENT)[0] is None
    receipt, _ = environment.store.read(
        f"runs/{environment.approval.run_id}/result.json"
    )
    assert receipt["plans"]["at"] == "2026-09-21T01:30:00Z"


def lose_lock_release(environment, runner, monkeypatch):
    """Finalize with the lock release failing, after control was deleted."""
    plans = {
        "nonce": environment.approval.nonce,
        "roots": ["flink-gcp", "tier3-bootstrap", "tier3-operator"],
        "empty": True,
        "at": "2026-09-21T01:30:00Z",
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
    return plans


@pytest.mark.parametrize("verdict", [rt.USABLE, INCONCLUSIVE])
def test_recovery_keeps_a_cleaned_bigquery_receipt(prepared, monkeypatch, verdict):
    stored = {"stage": "complete", "verdict": verdict}
    environment, runner, *_ = service_cleaned(prepared, stored)
    control = environment.refresh().to_dict()
    plans = lose_lock_release(environment, runner, monkeypatch)
    recovery = Runner(environment)
    # The restored record is the minimal one; the receipt carries the verdict.
    recovery.restore_control()
    recovery.settle(request_stop=True)
    assert Runner(environment).finalize({**plans, "at": "2026-09-21T01:37:31Z"}) is (
        verdict == rt.USABLE
    )
    assert environment.store.read(environment.records.path)[0] is None
    assert environment.store.read(rt.ENVIRONMENT)[0] is None
    receipt, _ = environment.store.read(
        f"runs/{environment.approval.run_id}/result.json"
    )
    assert receipt["plans"]["at"] == "2026-09-21T01:30:00Z"
    assert receipt["bigquery"] == control["bigquery"]


def lose_control_delete(environment, runner, monkeypatch):
    """Finalize with the control deletion failing, after the receipt was written."""
    plans = {
        "nonce": environment.approval.nonce,
        "roots": ["flink-gcp", "tier3-bootstrap", "tier3-operator"],
        "empty": True,
        "at": "2026-09-21T01:30:00Z",
    }
    original_delete = environment.store.delete

    def fail_control(name, *args, **kwargs):
        if name == environment.records.path:
            raise Failure("control deletion unavailable")
        return original_delete(name, *args, **kwargs)

    monkeypatch.setattr(environment.store, "delete", fail_control)
    with pytest.raises(Failure, match="control deletion unavailable"):
        runner.finalize(plans)
    monkeypatch.setattr(environment.store, "delete", original_delete)
    assert environment.store.read(environment.records.path)[0] is not None
    assert environment.store.read(rt.ENVIRONMENT)[0] is not None
    return plans


def recovery_runner(environment):
    """A runner in a new recovery process, which starts with no local failure."""
    return Runner(
        Environment(
            environment.kube,
            environment.store,
            environment.approval,
            environment.clock,
            environment.sleep,
        )
    )


def fail_event(store, monkeypatch, event):
    """Fail every evidence write of `event`; returns the restoring call."""
    write = store.write

    def failing(name, data, *args, **kwargs):
        if isinstance(data, dict) and data.get("event") == event:
            raise Failure("Evidence unavailable")
        return write(name, data, *args, **kwargs)

    monkeypatch.setattr(store, "write", failing)
    return lambda: monkeypatch.setattr(store, "write", write)


def stranded_receipt(prepared, monkeypatch, lost, verdict=rt.USABLE):
    """A run whose receipt exists while finalization retained the lock.

    `lost` names the write finalization lost: `control`, so the record
    survives, or `lock`, after which recovery restores the deleted record.
    """
    stored = {"stage": "complete", "verdict": verdict}
    environment, runner, *_ = service_cleaned(prepared, stored)
    if lost == "control":
        plans = lose_control_delete(environment, runner, monkeypatch)
    else:
        plans = lose_lock_release(environment, runner, monkeypatch)
        recovery_runner(environment).restore_control()
    receipt, _ = environment.store.read(
        f"runs/{environment.approval.run_id}/result.json"
    )
    assert receipt["success"] is (verdict == rt.USABLE)
    return environment, {**plans, "at": "2026-09-21T01:37:31Z"}


def assert_retained(environment):
    assert environment.store.read(environment.records.path)[0] is not None
    assert environment.store.read(rt.ENVIRONMENT)[0] is not None


def assert_released(environment):
    assert environment.store.read(environment.records.path)[0] is None
    assert environment.store.read(rt.ENVIRONMENT)[0] is None


@pytest.mark.parametrize("lost", ["control", "lock"])
@pytest.mark.parametrize("event", ["cleanup-start", "cleanup-ready", "idle"])
def test_recovery_evidence_failure_keeps_a_success_verdict(
    prepared, monkeypatch, lost, event
):
    environment, plans = stranded_receipt(prepared, monkeypatch, lost)
    restore = fail_event(environment.store, monkeypatch, event)
    first = recovery_runner(environment)
    first.settle(request_stop=True)
    restore()
    assert first.env.evidence_failed
    assert environment.refresh().evidence_failed
    # The receipt predates this recovery's failure, and finalization keeps it.
    assert recovery_runner(environment).finalize(plans) is True
    assert_released(environment)


@pytest.mark.parametrize("lost", ["control", "lock"])
def test_recovery_state_cleanup_failure_keeps_a_success_verdict(
    prepared, monkeypatch, lost
):
    environment, plans = stranded_receipt(prepared, monkeypatch, lost)
    first = recovery_runner(environment)
    calls = []

    def unavailable(cell_ids=None):
        calls.append(cell_ids)
        raise OSError("state listing unavailable")

    monkeypatch.setattr(first.cleanup, "clean_state", unavailable)
    # Nothing remains, so the settlement's own remaining-state check passes.
    first.settle(request_stop=True)
    assert calls == [None]
    assert not environment.refresh().state_clean
    assert recovery_runner(environment).finalize(plans) is True
    assert_released(environment)


@pytest.mark.parametrize("lost", ["control", "lock"])
def test_recovery_evidence_failure_under_an_unsuccessful_receipt_still_finalizes(
    prepared, monkeypatch, lost
):
    environment, plans = stranded_receipt(prepared, monkeypatch, lost, INCONCLUSIVE)
    # No success to keep: the failure is recorded as before, here by the final
    # settled write alone, and the receipt derived from it is the stored one.
    restore = fail_event(environment.store, monkeypatch, "idle")
    recovery_runner(environment).settle(request_stop=True)
    restore()
    assert environment.refresh().evidence_failed
    assert recovery_runner(environment).finalize(plans) is False
    assert_released(environment)


@pytest.mark.parametrize("lost", ["control", "lock"])
def test_recovery_refuses_remaining_state_without_lowering_the_verdict(
    prepared, monkeypatch, lost
):
    environment, plans = stranded_receipt(prepared, monkeypatch, lost)
    state = f"runs/{environment.approval.run_id}/checkpoint"
    environment.store.write(state, {"saved": True}, bucket=BIGQUERY_STATE)
    original_delete = environment.store.delete

    def keep_state(name, generation, bucket=rt.EVIDENCE):
        if bucket == BIGQUERY_STATE:
            raise OSError("state deletion unavailable")
        return original_delete(name, generation, bucket)

    monkeypatch.setattr(environment.store, "delete", keep_state)
    with pytest.raises(Failure, match="Run state remains"):
        recovery_runner(environment).settle(request_stop=True)
    with pytest.raises(Failure, match="Run state reappeared"):
        recovery_runner(environment).finalize(plans)
    assert_retained(environment)
    # Once the state can be deleted, the receipt's verdict finalizes.
    monkeypatch.setattr(environment.store, "delete", original_delete)
    recovery_runner(environment).settle(request_stop=True)
    assert recovery_runner(environment).finalize(plans) is True
    assert_released(environment)


@pytest.mark.parametrize("lost", ["control", "lock"])
def test_recovery_refuses_an_unexpected_object(prepared, monkeypatch, lost):
    environment, plans = stranded_receipt(prepared, monkeypatch, lost)
    environment.kube.put(obj("ConfigMap", "stray", BIGQUERY))
    with pytest.raises(Failure, match="non-baseline object remains"):
        recovery_runner(environment).settle(request_stop=True)
    with pytest.raises(Failure, match="non-baseline object remains"):
        recovery_runner(environment).finalize(plans)
    assert_retained(environment)


def test_recovery_does_not_keep_a_foreign_success_receipt(prepared, monkeypatch):
    environment, plans = stranded_receipt(prepared, monkeypatch, "control")
    path = f"runs/{environment.approval.run_id}/result.json"
    receipt, generation = environment.store.read(path)
    receipt["nonce"] = "0" * 32
    environment.store.write(path, receipt, generation)
    # Finalization refuses this receipt anyway; settlement records its own
    # failure rather than send the operator into a retry that cannot pass.
    restore = fail_event(environment.store, monkeypatch, "idle")
    recovery_runner(environment).settle(request_stop=True)
    restore()
    assert environment.refresh().evidence_failed
    with pytest.raises(Failure, match="Final receipt conflicts"):
        recovery_runner(environment).finalize(plans)
    assert_retained(environment)


def test_recovery_without_a_receipt_writes_the_minimal_record(prepared):
    environment, *_ = prepared
    control, generation = environment.store.read(environment.records.path)
    assert control["phase"] == "approved"
    environment.store.delete(environment.records.path, generation)
    Runner(environment).restore_control()
    assert (
        environment.store.read(environment.records.path)[0]
        == (
            rt.RunRecord(environment.approval.nonce, phase=rt.Phase.CLEANED, idle=True)
        ).to_dict()
    )


def test_recovery_workflow_releases_a_lock_stranded_after_control_deletion(
    prepared, monkeypatch, tmp_path
):
    from types import SimpleNamespace

    from flink_tier3 import lifecycle as cli

    stored = {"stage": "complete", "verdict": rt.USABLE}
    environment, runner, *_ = service_cleaned(prepared, stored)
    approval = environment.approval.to_dict()
    environment.store.write(
        f"runs/{environment.approval.run_id}/approval.json", approval
    )
    plans = lose_lock_release(environment, runner, monkeypatch)
    owner = approval["lock_owner"]
    monkeypatch.setenv("GITHUB_RUN_ID", "456")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")
    monkeypatch.delenv("GITHUB_OUTPUT", raising=False)
    monkeypatch.setattr(cli, "external", lambda _path, **kwargs: environment.kube)
    monkeypatch.setattr(
        cli.wf,
        "github_run",
        lambda _id: {
            "repository": {"full_name": rt.REPOSITORY},
            "path": ".github/workflows/tier3-run.yaml",
            "event": "workflow_dispatch",
            "head_sha": owner["sha"],
            "run_attempt": 1,
            "head_branch": "main",
            "status": "completed",
        },
    )
    preflights = []
    monkeypatch.setattr(
        cli.bootstrap,
        "Cluster",
        lambda path: SimpleNamespace(preflight=lambda: preflights.append(path)),
    )
    real = rt.Environment
    monkeypatch.setattr(
        rt,
        "Environment",
        lambda kube, store, approval, **kwargs: real(
            kube, store, approval, environment.clock, environment.sleep, **kwargs
        ),
    )
    args = SimpleNamespace(
        source_id=owner["github_run_id"],
        directory=tmp_path,
        kubeconfig=tmp_path / "kubeconfig",
    )
    cli.recover(args, environment.store)
    assert preflights == [args.kubeconfig]
    restored = environment.store.read(environment.records.path)[0]
    # Settlement ran over the restored record and rechecked state cleanup.
    assert restored["state_clean"]
    assert environment.store.read(rt.ENVIRONMENT)[0] == owner
    (tmp_path / "plans.json").write_text(
        json.dumps({**plans, "at": "2026-09-21T01:37:31Z"})
    )
    for root in cli.wf.ROOTS:
        (tmp_path / (root + "-plan.log")).write_text("No changes.")
    cli.finish(args, environment.store)
    assert environment.store.read(environment.records.path)[0] is None
    assert environment.store.read(rt.ENVIRONMENT)[0] is None
    receipt, _ = environment.store.read(
        f"runs/{environment.approval.run_id}/result.json"
    )
    assert receipt["success"] is True
    assert receipt["plans"]["at"] == "2026-09-21T01:30:00Z"
