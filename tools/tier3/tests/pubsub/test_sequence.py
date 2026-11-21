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
"""The Pub/Sub fault sequence (ADR-0204) over the simulated relay."""

import copy

import pytest
from flink_tier3.common import INCONCLUSIVE, USABLE, Failure
from flink_tier3.exercise import SEQUENCE, UNCONFIRMED
from flink_tier3.pubsub import exercise as pubsub_exercise
from flink_tier3.pubsub import plan
from flink_tier3.pubsub.messages import input_path, message_prefix, publications
from flink_tier3.pubsub.oracle import input_ids

from ..test_lifecycle import env as env  # noqa: PLC0414
from ..test_lifecycle import rt
from .test_actors import approval as approval  # noqa: PLC0414
from .test_actors import prepared as prepared  # noqa: PLC0414
from .test_admission import admitting as admitting  # noqa: PLC0414
from .test_analyze import assess, export
from .test_exercise import World
from .test_plan import inputs as inputs  # noqa: PLC0414
from .test_plan import renderer as renderer  # noqa: PLC0414

# Small enough to simulate, paced so that it outlasts the faults.
RECORDS = 60


@pytest.fixture
def trial():
    return {**plan.trial("sequence"), "records_per_subscription": RECORDS}


def kinds(control):
    return [fault["kind"] for fault in control.recovery["outcomes"]["faults"]]


def test_the_sequence_trial_builds_the_sequence_exercise(admitting):
    built = admitting.supervisor.exercise
    assert type(built) is pubsub_exercise.PubSubSequenceExercise
    environment = admitting.environment
    # The approval is frozen; the trial dict it holds is not.
    environment.approval.pubsub_trial["trial"] = "jm-replacement"
    exercise = pubsub_exercise.pubsub_exercise(
        environment, built.upgrade, built.handoff
    )
    assert type(exercise) is pubsub_exercise.PubSubExercise


def test_a_sequence_runs_every_fault_and_is_judged_by_its_output(
    admitting, monkeypatch, tmp_path
):
    world = World(admitting, monkeypatch, pace=1)
    control = world.run()
    assert control.recovery["stage"] == "complete", control.reason
    assert control.pubsub["stage"] == "cleaned"
    assert kinds(control) == list(SEQUENCE)
    assert all(
        fault["running_again_at"] for fault in control.recovery["outcomes"]["faults"]
    )
    assert control.recovery["completion"] == "all-observed"
    assert control.recovery["verdict"] == USABLE, control.recovery["reasons"]
    oracle = control.recovery["oracle"]
    assert oracle["rejected"] is None and oracle["missing_inputs"] == 0
    assert oracle["logical_inputs"] == 2 * RECORDS
    assert control.recovery["inputs"] == {"unpublished": 0, "unreceipted": 0}
    assert control.recovery["retried_pulls"] == []
    # The runner published every cohort, each once the one before was out.
    cohorts = control.pubsub["cohorts"]
    assert all(cohort["published_at"] for cohort in cohorts.values())
    assert len(cohorts) == 3
    # Three savepoint patches, two of them rescales, and two deletions.
    patches = [event for event in world.relay.events if event[0] == "patch"]
    deletions = [event[1] for event in world.relay.events if event[0] == "delete"]
    assert len(patches) == 3
    assert deletions == ["jobmanager", "taskmanager"]
    assert world.relay.parallelism == 2
    # Nothing samples or windows the run.
    assert control.recovery["coverage"] == {}
    # The exported evidence recomputes to the same verdict.
    assessed = assess(export(world, control, tmp_path))
    assert assessed["problems"] == []
    assert assessed["verdict"] == assessed["stored"] == USABLE
    assert assessed["oracle"] == oracle
    assert assessed["faults"] == list(SEQUENCE)


def test_a_record_still_missing_after_the_redelivery_horizon_is_lost(
    admitting, monkeypatch, tmp_path
):
    world = World(admitting, monkeypatch, pace=1)
    lost = []

    def lose():
        # One input the subscription delivered never reaches the relay again.
        if not lost and world.relay.pending[0]:
            lost.append(world.relay.pending[0].pop())

    world.relay.hooks.append(lose)
    control = world.run()
    assert control.recovery["stage"] == "complete", control.reason
    assert control.recovery["completion"] == "redelivery-horizon"
    assert control.recovery["verdict"] == INCONCLUSIVE
    assert control.recovery["reasons"] == ["oracle-incomplete"]
    assert control.recovery["oracle"]["missing_inputs"] == 1
    # Not before the horizon had passed since the latest restore and the
    # latest progress, which the input still flowing would have moved.
    restored = world.relay.restored["restore_timestamp"] / 1000
    progressed = max(
        at
        for lines in world.relay.logs.values()
        for at, line in lines
        if "pubsub-progress" in line
    )
    assert progressed > restored
    assert (
        rt.timestamp(control.recovery["at"])
        >= progressed + pubsub_exercise.REDELIVERY_SECONDS
    )
    assessed = assess(export(world, control, tmp_path))
    assert assessed["problems"] == []
    assert assessed["verdict"] == assessed["stored"] == INCONCLUSIVE


def test_an_input_through_before_the_faults_leaves_the_run_unconfirmed(
    admitting, monkeypatch
):
    # Unpaced, the relay reads every cohort as soon as it is published.
    world = World(admitting, monkeypatch)
    control = world.run()
    assert control.recovery["stage"] != "complete"
    assert UNCONFIRMED in control.reason


def test_an_output_under_an_unpublished_input_id_is_a_wrong_value(
    admitting, monkeypatch
):
    world = World(admitting, monkeypatch, pace=1)
    forged = []

    def forge():
        # One input reaches the relay under an ID no receipt holds.
        if not forged and world.relay.pending[1]:
            world.relay.pending[1][0]["id"] = "forged"
            forged.append(True)

    world.relay.hooks.append(forge)
    control = world.run()
    assert control.recovery["stage"] == "complete", control.reason
    assert control.recovery["inputs"]["unpublished"] == 1
    assert control.recovery["inputs"]["unreceipted"] == 0
    assert control.recovery["reasons"] == ["input-ids-unpublished"]


def test_a_pull_the_service_refuses_with_a_429_is_tried_again(
    admitting, monkeypatch, tmp_path
):
    world = World(admitting, monkeypatch, pace=1)
    world.relay.pull_failures = 1
    control = world.run()
    assert control.recovery["stage"] == "complete", control.reason
    assert control.recovery["verdict"] == USABLE, control.recovery["reasons"]
    [batch] = control.recovery["retried_pulls"]
    assert world.evidence("pubsub-pull-retried")[0]["payload"]["batch"] == batch
    # The batch holds an intent and nothing else, which the analysis does
    # not count as lost.
    assessed = assess(export(world, control, tmp_path))
    assert assessed["problems"] == []
    assert assessed["verdict"] == USABLE
    assert assessed["oracle"] == control.recovery["oracle"]


def test_a_failed_acknowledgement_still_stops_the_run(admitting, monkeypatch, tmp_path):
    """Its pull was saved, and whether the service took the acknowledgement is unknown."""
    world = World(admitting, monkeypatch, pace=1)

    def fail_once():
        if world.relay.output and not hasattr(world, "failed"):
            world.failed = world.relay.ack_failures = 1

    world.relay.hooks.append(fail_once)
    control = world.run()
    assert control.recovery["stage"] != "complete"
    assert not world.evidence("pubsub-pull-retried")
    # The stopped run's evidence reads without a lost batch.
    assessed = assess(export(world, control, tmp_path))
    assert not any(p.startswith("missing-output-batches") for p in assessed["problems"])


def test_the_sequence_patches_the_relay_s_own_parallelism_argument(admitting):
    exercise = admitting.supervisor.exercise
    spec = exercise.sequence_spec(1)
    assert (spec["job"]["parallelism"], spec["taskManager"]["replicas"]) == (1, 1)
    assert "--parallelism=1" in spec["job"]["args"]
    assert "--parallelism=2" not in spec["job"]["args"]
    exercise.upgrade = copy.deepcopy(exercise.upgrade)
    exercise.upgrade["spec"]["job"]["args"].remove("--parallelism=2")
    with pytest.raises(Failure, match="does not name its parallelism once"):
        exercise.sequence_spec(1)


def pod(uid):
    return {"metadata": {"uid": uid, "name": uid}}


def line(exercise, **fields):
    values = {
        "run_id": exercise.env.approval.run_id,
        "phase": "initial",
        "attempt": "a1",
        "restored": "false",
        "origin": "0",
        "processed": "1",
        **fields,
    }
    return f"{rt.utc(exercise.env.clock())} INFO event=pubsub-progress " + " ".join(
        f"{key}={value}" for key, value in values.items()
    )


@pytest.mark.parametrize(
    "fields, match",
    [
        ({"run_id": "other-run"}, "another run or phase"),
        ({"phase": "resume"}, "another run or phase"),
        # Upgraded progress before the first fault, or not restored.
        ({"phase": "upgrade", "restored": "true"}, "before its approved operation"),
    ],
)
def test_progress_from_another_run_or_too_early_an_upgrade_stops_the_run(
    admitting, fields, match
):
    exercise = admitting.supervisor.exercise
    with pytest.raises(Failure, match=match):
        exercise.progress(pod("tm-1"), line(exercise, **fields))


def test_one_attempt_announced_by_two_pods_stops_the_run(admitting):
    exercise = admitting.supervisor.exercise
    exercise.progress(pod("tm-1"), line(exercise))
    assert exercise.samples and exercise.attempts["a1"]["pod_uid"] == "tm-1"
    with pytest.raises(Failure, match="announced by two Pods"):
        exercise.progress(pod("tm-2"), line(exercise, processed="2"))


def test_the_parser_reads_the_line_the_relay_logs():
    # RecoveryContractTest pins the same example against the relay's format.
    match = pubsub_exercise.PROGRESS.search(
        "2026-09-21T00:00:00Z INFO event=pubsub-progress run_id=run-1 "
        "phase=upgrade attempt=a1 restored=true origin=7 processed=100"
    )
    assert match.groups() == ("run-1", "upgrade", "a1", "true", "7", "100")


@pytest.mark.parametrize(
    "shift, step, fields, match",
    [
        (-1e9, 0, {}, "predates this run"),
        (60, 0, {}, "dated in the future"),
        # After the first fault an upgraded attempt has always restored.
        (0, 1, {"phase": "upgrade", "restored": "false"}, "before its approved"),
    ],
)
def test_progress_out_of_time_or_unrestored_after_a_fault_stops_the_run(
    admitting, shift, step, fields, match
):
    exercise = admitting.supervisor.exercise
    exercise.step = step
    text = line(exercise, **fields)
    at = rt.utc(exercise.env.clock() + shift)
    with pytest.raises(Failure, match=match):
        exercise.progress(pod("tm-1"), at + text[text.index(" ") :])


def sample(at, attempt="a1", phase="upgrade"):
    return {"at": at, "phase": phase, "processed": 1, "attempt": attempt}


def test_running_again_needs_progress_from_an_attempt_the_fault_started(admitting):
    """A TaskManager's loss leaves the other region's attempt running.

    That attempt may log its first progress only after the restore, so what
    tells the two apart is when each announced itself on initialization.
    """
    exercise = admitting.supervisor.exercise
    exercise.awaiting = {"kind": "taskmanager", "at": 100}
    exercise.restored_at = 150
    exercise.announced = {"survivor": 90, "new": 140}
    exercise.samples = {1: sample(200, "survivor")}
    assert not exercise.ran_after_restore()
    # Progress of an attempt whose announcement was never read is not counted.
    exercise.samples[2] = sample(200, "unannounced")
    assert not exercise.ran_after_restore()
    exercise.samples[3] = sample(140, "new")
    assert not exercise.ran_after_restore()
    exercise.samples[4] = sample(200, "new")
    assert exercise.ran_after_restore()


def test_an_attempt_announces_itself_when_it_initializes(admitting):
    exercise = admitting.supervisor.exercise
    run_id = exercise.env.approval.run_id
    exercise.progress(
        pod("tm-1"),
        f"{rt.utc(exercise.env.clock())} INFO event=pubsub-attempt run_id={run_id} "
        "phase=initial attempt=a1 restored=false origin=0",
    )
    assert exercise.announced == {"a1": exercise.env.clock()}
    # Another run's announcement is not this run's attempt.
    exercise.progress(
        pod("tm-1"),
        f"{rt.utc(exercise.env.clock())} INFO event=pubsub-attempt run_id=other "
        "phase=initial attempt=a2 restored=false origin=0",
    )
    assert "a2" not in exercise.announced


def app(generation=1, state="RUNNING"):
    return {
        "metadata": {"generation": generation},
        "status": {
            "jobStatus": {"state": state},
            "observedGeneration": generation,
            "reconciliationStatus": {"state": "DEPLOYED"},
        },
    }


def running(exercise, taskmanagers=2):
    return [
        {
            "metadata": {
                "uid": f"{component}-{i}",
                "name": f"{component}-{i}",
                "namespace": exercise.namespace,
                "labels": {"component": component},
            },
            "status": {"phase": "Running"},
        }
        for component, count in (("jobmanager", 1), ("taskmanager", taskmanagers))
        for i in range(count)
    ]


def checkpoint(exercise, triggered):
    millis = int(triggered * 1000)
    return {
        "latest": {
            "completed": {
                "id": 9,
                "status": "COMPLETED",
                "is_savepoint": False,
                "trigger_timestamp": millis,
                "latest_ack_timestamp": millis + 1,
                "external_path": exercise.prefix("checkpoints") + "chk-9",
            }
        }
    }


def everything(exercise):
    return [
        {
            "input_index": index,
            "sequence": sequence,
            "input_message_id": f"{index}-{sequence}",
            "attempt": "a1",
            "at": 0,
        }
        for index in range(2)
        for sequence in range(exercise.records)
    ]


@pytest.fixture
def settled(admitting, monkeypatch):
    """The exercise after its last fault, recording what it would persist."""
    exercise = admitting.supervisor.exercise
    exercise.step, exercise.awaiting = len(SEQUENCE), None
    written = []
    monkeypatch.setattr(exercise, "persist", lambda stage, **d: written.append(d))
    monkeypatch.setattr(exercise, "published", dict)
    monkeypatch.setattr(exercise.collector, "drain", lambda pulls: 0)
    return exercise, written


@pytest.mark.parametrize(
    "restored, progressed",
    [(100, 200), (300, 200)],
    ids=["progress-later", "restore-later"],
)
def test_the_redelivery_horizon_counts_from_the_later_mark(
    settled, restored, progressed
):
    exercise, written = settled
    exercise.restored_at = restored
    exercise.samples = {1: sample(progressed)}
    horizon = max(restored, progressed) + pubsub_exercise.REDELIVERY_SECONDS
    # A checkpoint triggered before the horizon does not settle the run.
    assert not exercise.settle(app(), checkpoint(exercise, horizon), [])
    assert not written
    assert exercise.settle(app(), checkpoint(exercise, horizon + 1), [])
    [record] = written
    assert record["completion"] == "redelivery-horizon"
    assert record["reasons"] == ["oracle-incomplete"]


def test_a_final_round_cut_short_by_a_refused_pull_waits_for_the_next_poll(
    settled, monkeypatch
):
    exercise, written = settled
    exercise.restored_at, exercise.samples = 100, {}
    monkeypatch.setattr(
        exercise.collector, "drain", lambda pulls: exercise.collector.retried.append(1)
    )
    assert not exercise.settle(app(), checkpoint(exercise, 1000), [])
    assert not written


def test_a_complete_output_settles_without_waiting(settled):
    exercise, written = settled
    exercise.collector.observations = everything(exercise)
    assert exercise.settle(app(), {}, [])
    assert written[0]["completion"] == "all-observed"


def test_an_input_through_before_a_patch_is_sent_leaves_the_run_unconfirmed(settled):
    exercise, _ = settled
    exercise.step = 4
    exercise.awaiting = {"kind": "rescale-in", "at": 100, "generation": None}
    exercise.collector.observations = everything(exercise)
    with pytest.raises(Failure, match=UNCONFIRMED):
        exercise.settle(app(), {}, running(exercise))


@pytest.mark.parametrize("through", [False, True], ids=["idle", "through"])
def test_a_restored_job_with_nothing_to_process_leaves_the_run_unconfirmed(
    settled, monkeypatch, through
):
    exercise, _ = settled
    exercise.step = 2
    exercise.awaiting = {"kind": "jobmanager", "at": 100, "attempts": set()}
    exercise.restored_at = 150
    if through:
        exercise.collector.observations = everything(exercise)
    else:
        monkeypatch.setattr(
            exercise.env,
            "clock",
            lambda: 150 + pubsub_exercise.REDELIVERY_SECONDS - 1,
        )
        assert not exercise.settle(app(), {}, running(exercise))
        monkeypatch.setattr(
            exercise.env, "clock", lambda: 150 + pubsub_exercise.REDELIVERY_SECONDS
        )
    with pytest.raises(Failure, match=UNCONFIRMED):
        exercise.settle(app(), {}, running(exercise))
    # Not while it still lacks a TaskManager.
    exercise.collector.observations = []
    monkeypatch.setattr(exercise.env, "clock", lambda: 10**10)
    assert not exercise.settle(app(), {}, running(exercise, taskmanagers=1))


class Handoff:
    def __init__(self, cohorts):
        self.state, self.requests = cohorts, []

    def cohorts(self):
        return self.state

    def request_cohort(self, name, *, deadline):
        self.requests.append(name)


def test_a_cohort_is_asked_for_only_once_the_one_before_is_out(admitting):
    exercise = admitting.supervisor.exercise
    first, replay, last = pubsub_exercise.COHORTS
    exercise.handoff = Handoff({first: {"published_at": None}})
    exercise.request_input()
    assert exercise.handoff.requests == []
    exercise.handoff.state[first]["published_at"] = 1
    exercise.request_input()
    exercise.request_input()
    assert exercise.handoff.requests == [replay]
    exercise.handoff.state[replay] = {"published_at": 2}
    exercise.request_input()
    exercise.request_input()
    assert exercise.handoff.requests == [replay, last]


def test_a_missing_or_short_receipt_leaves_its_inputs_unreceipted(admitting):
    exercise = admitting.supervisor.exercise
    approval = exercise.env.approval
    prefix = message_prefix(approval.run_id, approval.nonce)
    receipts = list(publications(exercise.records))
    for number, (index, start, count) in enumerate(receipts):
        ids = [f"{index}-{start + offset}" for offset in range(count)]
        if number == 1:
            ids = ids[:-1]
        if number != 2:
            exercise.env.store.write(
                input_path(prefix, index, start, count) + "/response.json",
                {"messageIds": ids},
            )
    published = exercise.published()
    observed = [
        {
            "input_index": index,
            "sequence": sequence,
            "input_message_id": f"{index}-{sequence}",
        }
        for index, start, count in receipts
        for sequence in range(start, start + count)
    ]
    short, absent = receipts[1][2], receipts[2][2]
    assert input_ids(observed, published) == {
        "unpublished": 0,
        "unreceipted": short + absent,
    }
