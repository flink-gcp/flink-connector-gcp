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

import hashlib
import json

import pytest
from flink_tier3 import proposal
from flink_tier3 import render as command
from flink_tier3.cloudtasks import plan
from flink_tier3.cloudtasks.trial import Trial
from flink_tier3.common import Failure, digest
from flink_tier3.policy import GAR

from ..dispatched import RUN_ID
from ..dispatched import inputs as inputs  # noqa: PLC0414
from ..dispatched import renderer as renderer  # noqa: PLC0414


def test_a_task_name_is_the_sha256_of_the_run_and_sequence():
    """The application's taskIdExtractor key, which the sink hashes."""
    trial = Trial(RUN_ID, 3, 1, "sequence")
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
    assert Trial(RUN_ID, records, rate, "sequence").records == records


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
        Trial(run_id, records, rate, "sequence")


def test_the_sequence_starts_and_upgrades_at_two():
    """The fault sequence patches its rescales itself (ADR-0204)."""
    trial = Trial(RUN_ID, 180000, 100, "sequence")
    assert (trial.parallelism("initial"), trial.parallelism("upgrade")) == (2, 2)
    with pytest.raises(ValueError, match="phase"):
        trial.parallelism("resume")


def test_an_approval_without_a_kind_is_refused():
    """ct-1362-a1's approval names none; it is analysed at the revision that ran it."""
    with pytest.raises(TypeError, match="kind"):
        Trial(RUN_ID, 180000, 100)


# Unknown kinds, and the single-fault kinds no trial dispatches any more.
@pytest.mark.parametrize(
    "kind",
    ["upgrade-failover", "RESCALE-IN", None, 1, "jm-replacement", "rescale-in"],
)
def test_the_trial_refuses_an_unknown_kind(kind):
    with pytest.raises(ValueError, match="kind"):
        Trial(RUN_ID, 180000, 100, kind)


def test_the_arguments_are_the_ones_the_cue_package_renders():
    trial = Trial(RUN_ID, 180000, 100, "sequence")
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
        "--parallelism",
        "2",
    ]
    upgrade = trial.arguments("upgrade")
    assert [
        i for i, (a, b) in enumerate(zip(trial.arguments("initial"), upgrade)) if a != b
    ] == [5, 11]


@pytest.mark.parametrize(
    "value",
    [
        {"kind": "sequence", "records": 3600.0, "records_per_second": 10},
        {"kind": "sequence", "records": 3600, "records_per_second": True},
        {"kind": "sequence", "records": 3600, "records_per_second": 10, "extra": 1},
        {"kind": "sequence", "records": 3600},
        # The first trial's approval names no kind; a new dispatch must.
        {"records": 3600, "records_per_second": 10},
        {"kind": "upgrade-failover", "records": 3600, "records_per_second": 10},
        ["sequence"],
        # The single-fault trials, which no plan dispatches any more.
        {"kind": "sequence", "records": 180000, "records_per_second": 100},
        {"kind": "jm-replacement", "records": 180000, "records_per_second": 100},
        {"kind": "jm-replacement", "records": 3600, "records_per_second": 10},
    ],
)
def test_only_the_sequence_trial_is_dispatchable(value):
    with pytest.raises(ValueError, match="with its fixed input"):
        plan.validate_trial(value)


def test_the_sequence_trial_is_six_minutes_of_input_at_two_subtasks():
    """The fault sequence (ADR-0204) starts and upgrades at two, as a
    JobManager replacement does, and patches its rescales itself."""
    trial = plan.trial("sequence")
    plan.validate_trial(trial)
    assert trial == {"kind": "sequence", "records": 3600, "records_per_second": 10}
    identity = Trial(RUN_ID, **trial)
    assert (identity.parallelism("initial"), identity.parallelism("upgrade")) == (2, 2)
    assert identity.arguments("upgrade")[-2:] == ["--parallelism", "2"]


@pytest.mark.parametrize("kind", ["upgrade-failover", "jm-replacement", "rescale-in"])
def test_an_unknown_kind_has_no_trial(kind):
    with pytest.raises(ValueError, match="Unknown Cloud Tasks trial"):
        plan.trial(kind)


def test_the_proposal_binds_identity_input_and_queue(inputs, renderer):
    bundle = plan.prepare(**inputs)
    proposal = bundle["proposal"]
    assert proposal["kind"] == "cloudtasks-trial-proposal"
    assert proposal["approved"] is False
    assert proposal["trial"] == {
        "run_id": RUN_ID,
        "records": 3600,
        "records_per_second": 10,
        "kind": "sequence",
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
        "records": 3600,
    }
    assert "cost" not in proposal
    assert json.loads(bundle["delivery"]["config"]["data"]["proposal.json"]) == proposal
    [(_, _, _, _, options)] = renderer
    assert options["scenario"] == "cloudtasks-recovery"
    assert options["cloudtasks_records"] == 3600
    assert options["cloudtasks_records_per_second"] == 10
    assert "cloudtasks_trial" not in options


@pytest.mark.parametrize(
    "field,value",
    [
        ("run_id", "../bad"),
        ("revision", "b" * 39),
        ("application_image", GAR + "bigquery-recovery@sha256:" + "c" * 64),
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
    ],
)
def test_renderer_drift_does_not_produce_a_proposal(
    inputs, renderer, monkeypatch, change, match
):
    render = proposal.render

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

    monkeypatch.setattr(proposal, "render", altered)
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
        "sequence",
    ]
    command.main(argv)
    proposal = json.loads(capsys.readouterr().out)["proposal"]
    assert proposal["trial"]["kind"] == "sequence"
    for extra in (
        ["--bigquery-trial", "eo-10-sequence"],
        ["--pubsub-trial", "sequence"],
        ["--cloudtasks-trial", "upgrade-failover"],
        ["--cloudtasks-trial", "rescale-in"],
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
                "sequence",
            ]
        )
    assert "--cloudtasks-trial applies only" in capsys.readouterr().err
