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
"""Reject unusable offline proposals before rendering or cloud access."""

import copy
import json
from decimal import Decimal

import pytest
from flink_tier3 import render as command
from flink_tier3.common import Failure
from flink_tier3.model import validate_approval
from flink_tier3.pubsub import plan


@pytest.fixture
def trial():
    return {
        "version": 7,
        "trial": "rescale-out",
        "records_per_subscription": 1000,
        "restore_deduplication": False,
    }


@pytest.fixture
def inputs(trial, monkeypatch):
    monkeypatch.setattr(
        plan, "render", lambda *a, **k: pytest.fail("Invalid input reached CUE")
    )
    return {
        "run_id": "proposal-1361",
        "nonce": "a" * 32,
        "started_at": "2026-09-21T00:00:00Z",
        "expires_at": "2026-09-21T01:30:00Z",
        "active_seconds": 5220,
        "revision": "b" * 40,
        "application_image": plan.GAR + "pubsub-recovery@sha256:" + "c" * 64,
        "trial": trial,
    }


@pytest.fixture
def renderer(monkeypatch):
    """A render that answers with the manifests the real CUE produces."""
    calls = []

    def pod(role):
        return {
            "containers": [
                {
                    "resources": {
                        category: dict(plan.POD_RESOURCES[role])
                        for category in ("requests", "limits")
                    }
                }
            ]
        }

    def application(run_id, nonce, expiry, options, parallelism, phase, replicas=None):
        replicas = parallelism if replicas is None else replicas
        spec = {
            "image": options["application_image"],
            "podTemplate": {"spec": pod("smoke")},
            "job": {
                "parallelism": parallelism,
                "args": [
                    f"--run-id={run_id}",
                    f"--phase={phase}",
                    f"--records-per-subscription={options['pubsub_records']}",
                    f"--parallelism={parallelism}",
                    f"--require-restored={str(phase == 'upgrade').lower()}",
                    "--restore-deduplication="
                    + options["pubsub_restore_deduplication"],
                    "--record-delay-millis="
                    + str(options["pubsub_record_delay_millis"]),
                ],
            },
        }
        for manager, count in (("jobManager", 1), ("taskManager", replicas)):
            spec[manager] = {
                "replicas": count,
                "resource": {"cpu": 1, "memory": "2Gi"},
                "podTemplate": {"spec": pod("smoke")},
            }
        return {
            "apiVersion": "flink.apache.org/v1beta1",
            "kind": "FlinkDeployment",
            "metadata": {
                "name": run_id,
                "namespace": "tier3-pubsub",
                "annotations": {
                    "flink-gcp.io/approval": nonce,
                    "flink-gcp.io/expires-at": expiry,
                },
            },
            "spec": spec,
        }

    def render(run_id, nonce, expiry, seconds, **options):
        calls.append((run_id, nonce, expiry, seconds, options))
        kind = options["pubsub_trial"]
        initial = application(
            run_id,
            nonce,
            expiry,
            options,
            1 if kind == "rescale-out" else 2,
            "initial",
        )
        recovery = application(
            run_id,
            nonce,
            expiry,
            options,
            1 if kind == "rescale-in" else 2,
            "initial" if kind.endswith("-replacement") else "upgrade",
        )
        supervisor = pod("supervisor")
        supervisor["containers"][0]["image"] = (
            plan.GAR + "lifecycle-tools@sha256:" + "9" * 64
        )
        delivery = {
            "config": {
                "data": {
                    "approval.json": "{}",
                    "application.json": json.dumps(initial),
                    "upgrade-application.json": json.dumps(recovery),
                }
            },
            "supervisor": {
                "spec": {
                    "parallelism": 1,
                    "completions": 1,
                    "activeDeadlineSeconds": seconds,
                    "template": {"spec": supervisor},
                }
            },
        }
        probe = pod("supervisor")
        # Placed as the supervisor is: the rendered Pod excludes Spot.
        probe["affinity"] = {
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
        probe["containers"][0].update(
            image=supervisor["containers"][0]["image"],
            command=["python3", "-I", "-c", options["pubsub_probe_source"]],
            args=[options["pubsub_probe"]],
        )
        delivery["probe"] = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": run_id + "-access-probe",
                "namespace": "tier3-pubsub",
                "annotations": {"flink-gcp.io/approval": nonce},
            },
            "spec": {**probe, "serviceAccountName": "pubsub", "restartPolicy": "Never"},
        }
        return copy.deepcopy(initial), copy.deepcopy(recovery), delivery

    monkeypatch.setattr(plan, "render", render)
    return calls


@pytest.mark.parametrize("kind", plan.TRIALS)
def test_the_fake_render_satisfies_the_proposal_checks(inputs, renderer, kind):
    """The dispatch and actor tests stand on it, so it must pass as the CUE does."""
    inputs["trial"]["trial"] = kind
    proposal = plan.prepare(**inputs)["proposal"]
    assert proposal["approved"] is False
    assert renderer[0][4]["pubsub_trial"] == kind


@pytest.mark.parametrize(
    "kind, initial, recovery",
    [
        ("jm-replacement", (2, "initial"), (2, "initial")),
        ("tm-replacement", (2, "initial"), (2, "initial")),
        ("rescale-out", (1, "initial"), (2, "upgrade")),
        ("rescale-in", (2, "initial"), (1, "upgrade")),
        # The sequence's recovery manifest is its savepoint upgrade at two.
        ("sequence", (2, "initial"), (2, "upgrade")),
    ],
)
@pytest.mark.parametrize("deduplication", [False, True])
def test_the_trial_fixes_each_manifests_job(
    trial, kind, initial, recovery, deduplication
):
    trial.update(
        trial=kind,
        records_per_subscription=40,
        restore_deduplication=deduplication,
    )
    jobs = plan.manifest_jobs("run-1430", trial)
    assert jobs == tuple(
        (
            parallelism,
            [
                "--run-id=run-1430",
                f"--phase={phase}",
                "--records-per-subscription=40",
                f"--parallelism={parallelism}",
                f"--require-restored={str(phase == 'upgrade').lower()}",
                f"--restore-deduplication={str(deduplication).lower()}",
                # Only the sequence paces its input.
                f"--record-delay-millis={100 if kind == 'sequence' else 0}",
            ],
        )
        for parallelism, phase in (initial, recovery)
    )


@pytest.mark.parametrize("kind", plan.TRIALS)
def test_rendered_manifests_run_the_trial_they_were_rendered_for(
    inputs, renderer, kind
):
    inputs["trial"]["trial"] = kind
    rendered = plan.prepare(**inputs)
    plan.require_trial_jobs(
        inputs["run_id"],
        inputs["trial"],
        rendered["application"],
        rendered["recovery_application"],
    )


def _tamper(job, change):
    if change == "parallelism":
        job["parallelism"] = 3 - job["parallelism"]
    elif change == "parallelism-bool":
        job["parallelism"] = job["parallelism"] == 1
    elif change == "dropped":
        job["args"].pop()
    elif change == "extra":
        job["args"].append("--phase=upgrade")
    elif change == "reordered":
        job["args"].reverse()
    elif change == "missing":
        del job["args"]
    else:
        index = next(i for i, a in enumerate(job["args"]) if a.startswith(change))
        flag, value = job["args"][index].split("=", 1)
        flipped = {
            "--run-id": lambda: "other-run",
            "--phase": lambda: "initial" if value == "upgrade" else "upgrade",
            "--records-per-subscription": lambda: "999",
            "--parallelism": lambda: str(3 - int(value)),
            "--require-restored": lambda: "false" if value == "true" else "true",
        }[flag]()
        job["args"][index] = flag + "=" + flipped


@pytest.mark.parametrize("which", ["application", "recovery_application"])
@pytest.mark.parametrize(
    "change",
    [
        "--run-id",
        "--phase",
        "--records-per-subscription",
        "--parallelism",
        "--require-restored",
        "parallelism",
        "parallelism-bool",
        "dropped",
        "extra",
        "reordered",
        "missing",
    ],
)
def test_pinned_manifests_must_run_exactly_the_trial(inputs, renderer, which, change):
    """A digest pins a manifest; only this says the manifest runs the trial."""
    rendered = plan.prepare(**inputs)
    _tamper(rendered[which]["spec"]["job"], change)
    with pytest.raises(Failure, match="differ from the approved trial"):
        plan.require_trial_jobs(
            inputs["run_id"],
            inputs["trial"],
            rendered["application"],
            rendered["recovery_application"],
        )


@pytest.mark.parametrize("manifest", [None, [], {}, {"spec": []}, {"spec": {}}])
def test_malformed_manifests_are_refused_rather_than_raising(inputs, manifest):
    with pytest.raises(Failure, match="differ from the approved trial"):
        plan.require_trial_jobs(inputs["run_id"], inputs["trial"], manifest, manifest)


@pytest.mark.parametrize(
    "field,value",
    [
        ("version", True),
        ("version", 1),
        ("version", 2),
        ("trial", "combined"),
        # The AdaptiveScheduler kinds of #2054 are gone (#2240).
        ("trial", "reactive-scale-out"),
        ("trial", "adaptive-scale-in"),
        # Version 7 removed the entry point with the Table relay (#2240).
        ("version", 6),
        ("entry_point", "datastream"),
        ("trial", None),
        ("records_per_subscription", 1),
        ("records_per_subscription", 2),
        ("records_per_subscription", 10001),
        ("records_per_subscription", 2.0),
        ("records_per_subscription", True),
        ("version", 3),
        ("version", 4),
        # Version 5 removed the runtime traffic limits.
        ("traffic_limits", {}),
        ("total_request_limit", 100000),
        # Spend is approved from the estimate before dispatch, not proposed here.
        ("additional_cost_usd", "10.00"),
    ],
)
def test_invalid_trial_refused_before_render(inputs, field, value):
    inputs["trial"][field] = value
    with pytest.raises(Failure):
        plan.prepare(**inputs)


def test_the_planned_pass_publishes_each_cohort_in_one_batch(trial):
    planned = plan.input_plan("proposal-1361", trial)
    # Three cohorts of 333, 333 and 334, one publication each per input.
    assert planned["publish_calls"] == 6
    assert (planned["publish_batch_size"], planned["pull_batch_size"]) == (1000, 100)


@pytest.mark.parametrize(
    "field,value",
    [
        ("run_id", "../foreign"),
        ("nonce", "a" * 31),
        ("revision", "main"),
        ("application_image", plan.GAR + "pubsub-recovery:latest"),
        ("application_image", plan.GAR + "smoke@sha256:" + "c" * 64),
        ("expires_at", "2026-09-21T01:00:01Z"),
        ("started_at", "2026-09-21T00:00:00.500Z"),
        ("active_seconds", 5220.0),
        ("active_seconds", 5400),
    ],
)
def test_invalid_identity_or_window_refused(inputs, field, value):
    inputs[field] = value
    with pytest.raises(Failure):
        plan.prepare(**inputs)


def test_missing_and_unknown_fields(trial):
    for candidate in (
        {**trial, "approved": True},
        {k: v for k, v in trial.items() if k != "version"},
    ):
        with pytest.raises(Failure):
            plan.validate_trial(candidate)
    with pytest.raises(Failure):
        validate_approval({"scenario": "pubsub-recovery", "version": 4})


def test_cli_routes_pubsub_without_authentication(monkeypatch, capsys):
    calls = []

    def prepare(**kwargs):
        calls.append(kwargs)
        return {"proposal": {"approved": False}}

    monkeypatch.setattr(plan, "prepare", prepare)
    arguments = [
        "--scenario",
        "pubsub-recovery",
        "--run-id",
        "example",
        "--nonce",
        "a" * 32,
        "--started-at",
        "2026-09-21T00:00:00Z",
        "--expires-at",
        "2026-09-21T01:30:00Z",
        "--active-seconds",
        "5220",
        "--revision",
        "b" * 40,
        "--application-image",
        "synthetic",
        "--pubsub-trial",
        "sequence",
    ]
    command.main(arguments)
    assert calls[0]["trial"] == plan.trial("sequence")
    assert json.loads(capsys.readouterr().out)["proposal"]["approved"] is False
    for extra in (["--expression", "application"],):
        with pytest.raises(SystemExit):
            command.main(arguments + extra)
    assert len(calls) == 1


@pytest.mark.parametrize(
    "kind, slots",
    [
        ("jm-replacement", 7),
        ("rescale-out", 7),
        ("sequence", 7),
    ],
)
def test_the_estimate_charges_the_pod_budget_and_the_reserve(kind, slots):
    # Pods of 1 vCPU, 2 GiB and 1 GiB for the 90 minutes, at 0.141 an hour
    # each, and the reserve.
    assert plan.estimate(kind) == slots * Decimal("0.141") * Decimal("1.5") + Decimal(
        "1.00"
    )


def test_the_estimate_charges_each_slot_at_its_own_shape(monkeypatch):
    # The policy's Flink and Operator shapes are equal; a distinct one shows
    # which shape each slot is charged at.
    shapes = dict(plan.POD_RESOURCES)
    shapes["operator"] = {"cpu": "2", "memory": "2Gi", "ephemeral-storage": "1Gi"}
    monkeypatch.setattr(plan, "POD_RESOURCES", shapes)
    # Three control slots at 0.10 more an hour each, for the 90 minutes.
    assert plan.estimate("jm-replacement") == (
        Decimal("0.987") + 3 * Decimal("0.10")
    ) * Decimal("1.5") + Decimal("1.00")


def test_the_proposal_carries_the_estimate_and_its_basis(inputs, renderer):
    assert plan.prepare(**inputs)["proposal"]["cost"] == {
        "kind": "planning-estimate",
        "usd": "2.4805",
        "reviewed_at": "2026-10-10T00:00:00Z",
        "other_reserve_usd": "1.00",
    }


@pytest.mark.parametrize("kind", plan.TRIALS)
def test_a_kind_names_its_whole_trial(kind):
    """A dispatch chooses only the kind; the plan fixes everything else."""
    trial = plan.trial(kind)
    plan.validate_trial(trial)
    sequence = kind == "sequence"
    assert trial == {
        "version": plan.VERSION,
        "trial": kind,
        "records_per_subscription": 4800 if sequence else 1000,
        "restore_deduplication": sequence,
    }
    # The one-pass feasibility check dispatch reaches only after the cluster.
    plan.input_plan("feasibility", trial)


def test_an_unknown_kind_names_no_trial():
    for kind in ("none", "reactive-tm-kill", "", None):
        with pytest.raises(Failure, match="Unknown Pub/Sub trial"):
            plan.trial(kind)


def test_the_sequence_input_is_three_cohorts_of_whole_batches():
    planned = plan.input_plan("proposal-2240", plan.trial("sequence"))
    assert planned["messages"] == 9600
    # 1,600 a cohort on each input: two publications of 1,000 and 600.
    assert planned["publish_calls"] == 12
