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
from flink_tier3.policy import CLOUDTASKS_OBSERVATIONS, POLL

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
        "trial": plan.trial("jm-replacement"),
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
            options["cloudtasks_trial"],
        )
        size = trial.sink_parallelism("initial")
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
                "taskManager": {
                    "replicas": size,
                    "resource": {"cpu": 1, "memory": "2Gi"},
                },
                "job": {"parallelism": size, "args": trial.arguments("initial")},
            },
        }
        for manager in ("jobManager", "taskManager"):
            initial["spec"][manager]["podTemplate"] = {
                "spec": {"containers": [{"resources": copy.deepcopy(resources)}]}
            }
        upgrade = copy.deepcopy(initial)
        upgrade["spec"]["job"]["args"] = trial.arguments("upgrade")
        upgrade["spec"]["job"]["parallelism"] = trial.sink_parallelism("upgrade")
        upgrade["spec"]["taskManager"]["replicas"] = trial.sink_parallelism("upgrade")
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


@pytest.mark.parametrize(
    "kind,failover,rescale,sizes",
    [
        ("jm-replacement", "jobmanager", None, (2, 2)),
        ("tm-replacement", "taskmanager", None, (2, 2)),
        ("rescale-out", "jobmanager", "out", (1, 2)),
        ("rescale-in", "jobmanager", "in", (2, 1)),
    ],
)
def test_each_kind_names_its_fault_and_its_sizes(kind, failover, rescale, sizes):
    trial = Trial(RUN_ID, 180000, 100, kind)
    assert (trial.failover, trial.rescale) == (failover, rescale)
    assert (
        trial.sink_parallelism("initial"),
        trial.sink_parallelism("upgrade"),
    ) == sizes
    with pytest.raises(ValueError, match="phase"):
        trial.sink_parallelism("resume")


def test_an_approval_without_a_kind_is_the_first_trial():
    """ct-1362-a1's approval names no kind; its evidence reads as what it ran."""
    assert Trial(RUN_ID, 180000, 100).kind == "jm-replacement"


@pytest.mark.parametrize("kind", ["upgrade-failover", "RESCALE-IN", None, 1])
def test_the_trial_refuses_an_unknown_kind(kind):
    with pytest.raises(ValueError, match="kind"):
        Trial(RUN_ID, 180000, 100, kind)


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
        "--sink-parallelism",
        "2",
    ]
    upgrade = trial.arguments("upgrade")
    assert [
        i for i, (a, b) in enumerate(zip(trial.arguments("initial"), upgrade)) if a != b
    ] == [5, 11]
    rescale = Trial(RUN_ID, 180000, 100, "rescale-in")
    assert rescale.arguments("initial")[-2:] == ["--sink-parallelism", "2"]
    assert rescale.arguments("upgrade")[-2:] == ["--sink-parallelism", "1"]


@pytest.mark.parametrize(
    "value",
    [
        {"kind": "jm-replacement", "records": 180000.0, "records_per_second": 100},
        {"kind": "jm-replacement", "records": 180000, "records_per_second": True},
        {"kind": "rescale-in", "records": 90000, "records_per_second": 50},
        {
            "kind": "tm-replacement",
            "records": 180000,
            "records_per_second": 100,
            "extra": 1,
        },
        {"kind": "rescale-out", "records": 180000},
        # The first trial's approval names no kind; a new dispatch must.
        {"records": 180000, "records_per_second": 100},
        {"kind": "upgrade-failover", "records": 180000, "records_per_second": 100},
        ["jm-replacement"],
    ],
)
def test_only_the_four_trials_are_dispatchable(value):
    with pytest.raises(ValueError, match="180,000 records at 100"):
        plan.validate_trial(value)


@pytest.mark.parametrize("kind", plan.KINDS)
def test_each_trial_is_its_kind_and_the_fixed_input(kind):
    plan.validate_trial(plan.trial(kind))
    assert plan.trial(kind) == {
        "kind": kind,
        "records": 180000,
        "records_per_second": 100,
    }


def test_an_unknown_kind_has_no_trial():
    with pytest.raises(ValueError, match="Unknown Cloud Tasks trial"):
        plan.trial("upgrade-failover")


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
        "kind": "jm-replacement",
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
    assert options["cloudtasks_trial"] == "jm-replacement"


@pytest.mark.parametrize(
    "kind,sizes",
    [
        ("jm-replacement", (2, 2)),
        ("tm-replacement", (2, 2)),
        ("rescale-out", (1, 2)),
        ("rescale-in", (2, 1)),
    ],
)
def test_each_trial_renders_its_sizes_and_the_same_estimate(
    inputs, renderer, kind, sizes
):
    inputs["trial"] = plan.trial(kind)
    bundle = plan.prepare(**inputs)
    for manifest, size in zip(
        (bundle["application"], bundle["upgrade_application"]), sizes, strict=True
    ):
        assert manifest["spec"]["job"]["parallelism"] == size
        assert manifest["spec"]["taskManager"]["replicas"] == size
    assert bundle["proposal"]["trial"]["kind"] == kind
    assert bundle["proposal"]["cost"]["usd"] == str(plan.estimate())
    [(_, _, _, _, options)] = renderer
    assert options["cloudtasks_trial"] == kind


@pytest.mark.parametrize(
    "field,value",
    [
        ("run_id", "../bad"),
        ("revision", "b" * 39),
        ("application_image", plan.GAR + "bigquery-recovery@sha256:" + "c" * 64),
        ("started_at", "2026-10-13T00:00:01Z"),
        ("active_seconds", 5221),
        (
            "trial",
            {"kind": "jm-replacement", "records": 90000, "records_per_second": 50},
        ),
        ("trial", {"kind": "other", "records": 180000, "records_per_second": 100}),
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
        ("parallelism", "proposed parallelism"),
        ("upgrade-size", "proposed parallelism"),
        ("approval", "unapproved manifests"),
        # Where the trial's size is one, only the type tells True from it: dict
        # equality treats them as equal.
        ("rescale-true", "proposed parallelism"),
    ],
)
def test_renderer_drift_does_not_produce_a_proposal(
    inputs, renderer, monkeypatch, change, match
):
    render = plan.render
    if change == "rescale-true":
        inputs["trial"] = plan.trial("rescale-in")

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
        elif change == "parallelism":
            initial["spec"]["job"]["parallelism"] = True
        elif change == "rescale-true":
            upgrade["spec"]["job"]["parallelism"] = True
        elif change == "upgrade-size":
            # A rescale the trial did not approve.
            upgrade["spec"]["job"]["parallelism"] = 1
            upgrade["spec"]["taskManager"]["replicas"] = 1
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


def test_the_render_command_takes_its_trial_by_kind(inputs, renderer, capsys):
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
        "--cloudtasks-trial",
        "rescale-in",
    ]
    command.main(argv)
    proposal = json.loads(capsys.readouterr().out)["proposal"]
    assert proposal["cost"]["usd"] == str(plan.estimate())
    assert proposal["trial"]["kind"] == "rescale-in"
    for extra in (
        ["--trial", "eo-10"],
        ["--trial-file", "x.json"],
        ["--cloudtasks-trial", "upgrade-failover"],
    ):
        with pytest.raises(SystemExit):
            command.main(argv + extra)
    for missing in ("--revision", "--cloudtasks-trial"):
        with pytest.raises(SystemExit):
            command.main(argv[: argv.index(missing)])


def test_only_the_cloudtasks_scenario_takes_a_cloudtasks_trial(capsys):
    with pytest.raises(SystemExit):
        command.main(
            [
                "--run-id",
                RUN_ID,
                "--nonce",
                "a" * 32,
                "--expires-at",
                "2026-10-13T01:30:00Z",
                "--active-seconds",
                "5220",
                "--cloudtasks-trial",
                "rescale-in",
            ]
        )
    assert "--cloudtasks-trial applies only" in capsys.readouterr().err


def test_the_input_outlasts_every_window_and_both_recoveries():
    """The windows end inside the input, however long each recovery takes.

    The input pauses while the job recovers, so a recovery's length moves the
    windows and the input's end alike. What separates them is fixed: the
    baseline and its warm-up, the two later windows, and for the upgrade and
    each proof the checkpoint and the poll that see it.
    """
    timing = CLOUDTASKS_OBSERVATIONS
    lag = plan.CHECKPOINT_SECONDS + POLL
    windows = (
        timing["warmup_seconds"]
        + timing["baseline_seconds"]
        + timing["post_upgrade_seconds"]
        + timing["post_recovery_seconds"]
        + 3 * lag
    )
    trial = plan.trial("jm-replacement")
    input_seconds = trial["records"] / trial["records_per_second"]
    # About eighteen minutes, a margin of twelve.
    assert windows == 1095
    assert input_seconds - windows >= 600
