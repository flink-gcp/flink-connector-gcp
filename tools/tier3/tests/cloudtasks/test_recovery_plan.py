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
"""Offline Cloud Tasks proposal contracts; cloud access is never invoked."""

import copy
import hashlib
import json
from decimal import Decimal

import pytest
from flink_tier3 import render as command
from flink_tier3.cloudtasks import plan
from flink_tier3.cloudtasks.trial import Trial
from flink_tier3.common import Failure, digest

RUN_ID = "ct-proposal"


@pytest.fixture
def inputs():
    return {
        "run_id": RUN_ID,
        "nonce": "a" * 32,
        "started_at": "2026-10-13T00:00:00Z",
        "expires_at": "2026-10-13T01:30:00Z",
        "active_seconds": 5220,
        "revision": "b" * 40,
        "application_image": plan.GAR + "cloudtasks-recovery@sha256:" + "c" * 64,
        "trial": dict(plan.TRIAL),
    }


@pytest.fixture
def renderer(monkeypatch):
    """What `lifecycle/cloudtasks.cue` renders, as the CUE suite pins it."""
    calls = []

    def render(run_id, nonce, expiry, seconds, **options):
        calls.append((run_id, nonce, expiry, seconds, options))
        trial = Trial(
            run_id,
            options["cloudtasks_records"],
            options["cloudtasks_records_per_second"],
        )
        resources = {
            category: dict(plan.POD_RESOURCES["smoke"])
            for category in ("requests", "limits")
        }
        initial = {
            "metadata": {
                "name": run_id,
                "namespace": "tier3-cloudtasks",
                "annotations": {
                    "flink-gcp.io/approval": nonce,
                    "flink-gcp.io/expires-at": expiry,
                },
            },
            "spec": {
                "image": options["application_image"],
                "jobManager": {"replicas": 1, "resource": {"cpu": 1, "memory": "2Gi"}},
                "taskManager": {"replicas": 2, "resource": {"cpu": 1, "memory": "2Gi"}},
                "job": {"args": trial.arguments("initial")},
            },
        }
        for manager in ("jobManager", "taskManager"):
            initial["spec"][manager]["podTemplate"] = {
                "spec": {"containers": [{"resources": copy.deepcopy(resources)}]}
            }
        upgrade = copy.deepcopy(initial)
        upgrade["spec"]["job"]["args"] = trial.arguments("upgrade")
        delivery = {
            "config": {
                "data": {
                    "approval.json": "{}",
                    "application.json": json.dumps(initial),
                    "upgrade-application.json": json.dumps(upgrade),
                }
            },
            "supervisor": {
                "spec": {
                    "template": {
                        "spec": {
                            "containers": [
                                {
                                    "resources": {
                                        category: dict(plan.POD_RESOURCES["supervisor"])
                                        for category in ("requests", "limits")
                                    },
                                    "image": plan.GAR
                                    + "lifecycle-tools@sha256:"
                                    + "d" * 64,
                                }
                            ]
                        }
                    }
                }
            },
        }
        return initial, upgrade, delivery

    monkeypatch.setattr(plan, "render", render)
    return calls


def test_a_task_name_is_the_sha256_of_the_run_and_sequence():
    """The application's taskIdExtractor key, which the sink hashes."""
    trial = Trial(RUN_ID, 3, 1)
    assert trial.task_id(0) == hashlib.sha256(b"ct-proposal/0").hexdigest()
    assert trial.task_ids() == {
        hashlib.sha256(f"ct-proposal/{n}".encode()).hexdigest(): n for n in range(3)
    }
    assert trial.queue == "ct1362-ct-proposal"
    assert trial.queue_path == (
        "projects/flink-gcp/locations/us-central1/queues/ct1362-ct-proposal"
    )
    for sequence in (-1, 3, True, 1.0):
        with pytest.raises(ValueError, match="outside"):
            trial.task_id(sequence)


@pytest.mark.parametrize(
    "records,rate",
    [(2, 1), (1800, 1), (180000, 100), (90000, 50)],
)
def test_the_trial_accepts_the_application_and_cue_bounds(records, rate):
    assert Trial(RUN_ID, records, rate).records == records


@pytest.mark.parametrize(
    "run_id,records,rate",
    [
        (RUN_ID, 1, 1),
        (RUN_ID, 1801, 1),
        (RUN_ID, 180001, 100),
        (RUN_ID, 360001, 100),
        (RUN_ID, 100, 0),
        (RUN_ID, 100, 101),
        (RUN_ID, 100.0, 1),
        (RUN_ID, True, 1),
        ("../bad", 100, 1),
        ("x" * 41, 100, 1),
    ],
)
def test_the_trial_refuses_inputs_the_application_or_cue_would(run_id, records, rate):
    with pytest.raises(ValueError):
        Trial(run_id, records, rate)


def test_the_arguments_are_the_ones_the_cue_package_renders():
    trial = Trial(RUN_ID, 180000, 100)
    assert trial.arguments("initial") == [
        "--run-id",
        RUN_ID,
        "--queue",
        "ct1362-" + RUN_ID,
        "--phase",
        "initial",
        "--records",
        "180000",
        "--records-per-second",
        "100",
        "--require-restored",
        "false",
    ]
    upgrade = trial.arguments("upgrade")
    assert [
        i for i, (a, b) in enumerate(zip(trial.arguments("initial"), upgrade)) if a != b
    ] == [5, 11]


@pytest.mark.parametrize(
    "value",
    [
        {"records": 180000.0, "records_per_second": 100},
        {"records": 180000, "records_per_second": True},
        {"records": 90000, "records_per_second": 50},
        {"records": 180000, "records_per_second": 100, "extra": 1},
        {"records": 180000},
    ],
)
def test_only_the_one_trial_is_dispatchable(value):
    with pytest.raises(ValueError, match="180,000 records at 100"):
        plan.validate_trial(value)
    plan.validate_trial({"records": 180000, "records_per_second": 100})


def test_the_estimate_prices_the_pods_and_every_task_operation():
    assert plan.operations() == 180000 + 6000 + 360 + 10
    operations = Decimal(plan.operations()) / 1_000_000 * Decimal("0.40")
    assert plan.estimate() - operations - plan.RESERVE_USD > 0
    assert plan.estimate().quantize(Decimal("0.01")) == Decimal("2.13")


def test_the_proposal_binds_identity_input_queue_and_estimate(inputs, renderer):
    bundle = plan.prepare(**inputs)
    proposal = bundle["proposal"]
    assert proposal["kind"] == "cloudtasks-trial-proposal"
    assert proposal["approved"] is False
    assert proposal["trial"] == {
        "run_id": RUN_ID,
        "records": 180000,
        "records_per_second": 100,
    }
    assert proposal["queue"].endswith("/queues/ct1362-" + RUN_ID)
    assert proposal["application_sha256"] == digest(bundle["application"])
    assert proposal["upgrade_application_sha256"] == digest(
        bundle["upgrade_application"]
    )
    assert proposal["supervisor_sha256"] == digest(bundle["delivery"]["supervisor"])
    assert proposal["cleanup_at"] == "2026-10-13T01:15:00Z"
    # Informational, as BigQuery's are: nothing at run time enforces a limit.
    assert proposal["limits"] == {
        "seconds": 5400,
        "cleanup_seconds": 900,
        "records": 180000,
    }
    assert proposal["cost"]["usd"] == str(plan.estimate())
    assert proposal["cost"]["operations"] == plan.operations()
    assert json.loads(bundle["delivery"]["config"]["data"]["proposal.json"]) == proposal
    [(_, _, _, _, options)] = renderer
    assert options["scenario"] == "cloudtasks-recovery"
    assert options["cloudtasks_records"] == 180000
    assert options["cloudtasks_records_per_second"] == 100


@pytest.mark.parametrize(
    "field,value",
    [
        ("run_id", "../bad"),
        ("revision", "b" * 39),
        ("application_image", plan.GAR + "bigquery-recovery@sha256:" + "c" * 64),
        ("started_at", "2026-10-13T00:00:01Z"),
        ("active_seconds", 5221),
        ("trial", {"records": 90000, "records_per_second": 50}),
    ],
)
def test_invalid_inputs_are_refused_before_render(inputs, renderer, field, value):
    inputs[field] = value
    with pytest.raises((ValueError, Failure)):
        plan.prepare(**inputs)
    assert renderer == []


@pytest.mark.parametrize(
    "change,match",
    [
        ("namespace", "proposed identity"),
        ("image", "proposed identity"),
        ("initial", "proposed input"),
        ("memory", "resource budget"),
        ("supervisor", "resource budget"),
        ("sidecar", "unpriced container"),
        ("upgrade", "more than phase"),
        ("replicas", "Pod budget"),
        ("approval", "unapproved manifests"),
    ],
)
def test_renderer_drift_does_not_produce_a_proposal(
    inputs, renderer, monkeypatch, change, match
):
    render = plan.render

    def altered(*args, **kwargs):
        initial, upgrade, delivery = render(*args, **kwargs)
        if change == "namespace":
            initial["metadata"]["namespace"] = "tier3-bigquery"
        elif change == "initial":
            initial["spec"]["job"]["args"][3] = "ct1362-other"
        elif change == "upgrade":
            upgrade["spec"]["image"] = "other"
        elif change == "replicas":
            initial["spec"]["taskManager"]["replicas"] = 3
        elif change == "image":
            initial["spec"]["image"] = inputs["application_image"].replace("c", "e")
        elif change == "memory":
            container = initial["spec"]["taskManager"]["podTemplate"]["spec"][
                "containers"
            ][0]
            container["resources"]["limits"]["memory"] = "4Gi"
        elif change == "supervisor":
            container = delivery["supervisor"]["spec"]["template"]["spec"][
                "containers"
            ][0]
            container["resources"]["requests"]["cpu"] = "2"
        elif change == "sidecar":
            initial["spec"]["podTemplate"] = {"spec": {"containers": [{}]}}
        else:
            delivery["config"]["data"]["approval.json"] = '{"x": 1}'
        return initial, upgrade, delivery

    monkeypatch.setattr(plan, "render", altered)
    with pytest.raises(Failure, match=match):
        plan.prepare(**inputs)


def test_the_render_command_takes_no_trial_input(inputs, renderer, capsys):
    argv = [
        "--run-id",
        RUN_ID,
        "--nonce",
        "a" * 32,
        "--expires-at",
        inputs["expires_at"],
        "--active-seconds",
        "5220",
        "--scenario",
        "cloudtasks-recovery",
        "--started-at",
        inputs["started_at"],
        "--revision",
        "b" * 40,
        "--application-image",
        inputs["application_image"],
    ]
    command.main(argv)
    assert json.loads(capsys.readouterr().out)["proposal"]["cost"]["usd"] == str(
        plan.estimate()
    )
    for extra in (["--trial", "eo-10"], ["--trial-file", "x.json"]):
        with pytest.raises(SystemExit):
            command.main(argv + extra)
    with pytest.raises(SystemExit):
        command.main(argv[: argv.index("--revision")])
