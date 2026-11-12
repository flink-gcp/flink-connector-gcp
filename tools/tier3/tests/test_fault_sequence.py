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
"""One run that injects every fault and waits out everything else (ADR-0204)."""

import copy
from typing import ClassVar

import pytest
from flink_tier3 import exercise as exercise_module
from flink_tier3.common import UNPLANNED_EVENT, ApiError, Failure, TransportError
from flink_tier3.exercise import (
    RECORD_SECONDS,
    SEQUENCE_PARALLELISM,
    TIME_LIMIT_SECONDS,
    FaultSequence,
)

from .test_exercise import LINEAGE, Exercise, World, preempted
from .test_exercise import env as env  # noqa: PLC0414
from .test_exercise import inputs as inputs  # noqa: PLC0414
from .test_exercise import prepared as prepared  # noqa: PLC0414
from .test_exercise import renderer as renderer  # noqa: PLC0414
from .test_lifecycle import rt


class Sequenced(FaultSequence, Exercise):
    timing: ClassVar[dict] = {**Exercise.timing, "visibility_seconds": 600}


class SequenceWorld(World):
    """The shared world, with an Operator that moves a generation per patch.

    A fault's recovery is a restore the job's statistics report, after which
    the job logs progress again; the stopped job may log before that.
    """

    def __init__(self, prepared, monkeypatch):
        super().__init__(prepared, monkeypatch, exercise=Sequenced)
        args = self.upgrade["spec"]["job"]["args"]
        if "--parallelism" not in args:
            args += ["--parallelism", "2"]
        self.exercise.upgrade = self.upgrade
        self.emitted = []
        self.patch_errors = []
        # The state the job the Operator names reports on its own REST API,
        # read through the reader the loop attaches each poll.
        self.job_state = "RUNNING"
        self.service = {"metadata": {"name": "rest", "uid": "svc-uid"}}
        self.readers = []

        def reader(env, service, job_id):
            assert env is self.env
            self.readers.append(job_id)
            return type(
                "Rest", (), {"job_id": job_id, "service": service, "job": self.job}
            )()

        monkeypatch.setattr(exercise_module, "FlinkRest", reader)
        self.gone = set()
        emit, root = self.env.emit, self.env.root
        monkeypatch.setattr(
            self.env,
            "root",
            lambda key: copy.deepcopy(self.app) if key == "application" else root(key),
        )

        def record(event, payload):
            self.emitted.append((event, payload))
            emit(event, payload)

        monkeypatch.setattr(self.env, "emit", record)

    def job(self, path):
        assert path == ""
        if isinstance(self.job_state, Exception):
            raise self.job_state
        return {"state": self.job_state}

    def patch(self, app, operations, subresource="", before_repeat=None):
        assert app["metadata"]["uid"] == "app-uid" and not subresource
        if self.patch_errors:
            error = self.patch_errors.pop(0)
            if isinstance(error, tuple):
                error, landed = error
                if landed:
                    self.apply(operations)
            raise error
        return self.apply(operations)

    def apply(self, operations):
        self.patches.append(operations)
        for operation in operations:
            _, _, parent, field = operation["path"].split("/")
            self.app["spec"][parent][field] = operation["value"]
        self.app["metadata"]["generation"] += 1
        return copy.deepcopy(self.app)

    def delete(self, pod, force=False):
        if pod["metadata"]["name"] in self.gone:
            return False
        return super().delete(pod, force)

    def deploy(self):
        """The Operator reconciles the last patch."""
        self.app["status"].update(
            observedGeneration=self.app["metadata"]["generation"],
            reconciliationStatus={"state": "DEPLOYED"},
        )

    def restore(self):
        """The job restores its state, as every fault makes it."""
        self.advance(5)
        self.restored = {
            "id": 1,
            "is_savepoint": False,
            "restore_timestamp": int(self.env.clock() * 1000),
        }
        self.restores += 1

    def taskmanagers(self, count):
        for name in [n for n in self.pods if n.startswith("tm-")]:
            del self.pods[name]
        self.next_tm = getattr(self, "next_tm", 10)
        for _ in range(count):
            self.next_tm += 1
            self.pod(f"tm-{self.next_tm}", "taskmanager")

    def flowing(self):
        """The job logs progress on a TaskManager that reports the input."""
        tm = next(n for n in self.pods if n.startswith("tm-"))
        self.advance(5)
        self.progress(tm, "upgrade")

    def recover(self, kind):
        """The job runs again after `kind`, at the sequence's parallelism."""
        self.advance(20)
        if kind == "jobmanager":
            for name in [n for n in self.pods if n.startswith("jm-")]:
                del self.pods[name]
            self.pod(f"jm-{self.env.clock()}", "jobmanager")
        if kind == "taskmanager" or kind.startswith("rescale"):
            self.taskmanagers(SEQUENCE_PARALLELISM[kind])
        self.deploy()
        self.restore()
        self.flowing()
        return self.poll()

    def poll(self):
        self.exercise.scheduling(copy.deepcopy(list(self.pods.values())))
        self.exercise.attach_rest(
            self.service, self.app["status"]["jobStatus"].get("jobId", "1" * 32)
        )
        return self.observe()

    def start(self):
        """Initial input and a checkpoint after it: the first fault may land."""
        self.advance(10)
        self.progress("tm-1", "initial")
        self.advance(5)
        self.checkpoint(1)
        self.poll()

    def state(self, value):
        self.app["status"]["jobStatus"]["state"] = value


@pytest.fixture
def world(prepared, monkeypatch):
    return SequenceWorld(prepared, monkeypatch)


def awaited(world):
    return world.exercise.awaiting and world.exercise.awaiting["kind"]


def through(world, last):
    """Start, and recover from every fault up to `last`."""
    world.start()
    for kind in ("upgrade", "jobmanager", "taskmanager", "rescale-in", "rescale-out"):
        assert awaited(world) == kind
        world.recover(kind)
        if kind == last:
            return


def test_one_run_injects_every_fault_in_order_and_finishes(world):
    through(world, "rescale-out")
    faults = world.exercise.outcomes["faults"]
    assert [fault["kind"] for fault in faults] == [
        "upgrade",
        "jobmanager",
        "taskmanager",
        "rescale-in",
        "rescale-out",
    ]
    assert all("running_again_at" in fault for fault in faults)
    # The upgrade, then a rescale to one and back to two, each one patch.
    assert [ops[2]["value"] for ops in world.patches] == [2, 1, 2]
    assert [
        ops[0]["value"][ops[0]["value"].index("--parallelism") + 1]
        for ops in world.patches
    ] == ["2", "1", "2"]
    assert world.deleted == [f["pod"]["name"] for f in faults if "pod" in f]
    assert awaited(world) is None
    world.advance(5)
    world.processed = world.exercise.records - 100
    world.flowing()
    world.state("FINISHED")
    world.job_state = "FINISHED"
    assert world.poll() is True
    assert world.exercise.stage == "complete"


def test_no_fault_lands_before_a_checkpoint_follows_flowing_input(world):
    world.advance(10)
    world.progress("tm-1", "initial")
    world.poll()
    assert world.exercise.step == 0
    world.advance(5)
    world.checkpoint(1)
    world.poll()
    assert awaited(world) == "upgrade"


def test_the_first_fault_waits_for_the_scenarios_preparation(world, monkeypatch):
    monkeypatch.setattr(world.exercise, "ready_for_sequence", lambda: False)
    world.start()
    assert world.exercise.step == 0
    monkeypatch.setattr(world.exercise, "ready_for_sequence", lambda: True)
    world.poll()
    assert awaited(world) == "upgrade"


def test_a_patch_waits_until_the_operator_deployed_its_generation(world):
    world.start()
    # Still the last generation's DEPLOYED, with the new job's progress.
    world.app["status"].update(
        observedGeneration=world.app["metadata"]["generation"] - 1,
        reconciliationStatus={"state": "DEPLOYED"},
    )
    world.restore()
    world.flowing()
    world.poll()
    assert awaited(world) == "upgrade"
    world.deploy()
    world.poll()
    assert awaited(world) == "jobmanager"


def test_progress_the_stopped_job_logged_after_the_fault_is_not_running_again(world):
    """The deleted TaskManager's last lines precede the job's restore."""
    through(world, "jobmanager")
    assert awaited(world) == "taskmanager"
    deleted = world.exercise.outcomes["faults"][-1]["pod"]["name"]
    world.advance(1)
    world.progress(deleted, "upgrade")
    world.taskmanagers(2)
    world.poll()
    assert awaited(world) == "taskmanager"
    world.restore()
    world.poll()
    # Restored, but nothing logged since.
    assert awaited(world) == "taskmanager"
    world.flowing()
    world.poll()
    assert awaited(world) == "rescale-in"


def test_running_again_needs_the_taskmanagers_the_parallelism_needs(world):
    through(world, "rescale-in")
    assert awaited(world) == "rescale-out"
    world.advance(20)
    world.deploy()
    world.restore()
    world.flowing()
    world.poll()
    # Reconciled and flowing, but one TaskManager where the rescale wants two.
    assert awaited(world) == "rescale-out"
    world.taskmanagers(2)
    world.poll()
    assert awaited(world) is None


def test_the_stopped_jobs_finished_is_waited_out_during_a_savepoint_fault(world):
    """As ADR-0165 saw: FINISHED while the next job starts, even once deployed."""
    world.start()
    world.state("FINISHED")
    world.poll()
    world.deploy()
    assert world.poll() is False
    assert awaited(world) == "upgrade"
    world.state("RUNNING")
    world.restore()
    world.flowing()
    world.poll()
    assert awaited(world) == "jobmanager"


def test_a_job_that_finishes_before_a_poll_saw_it_run_again_completes(world):
    through(world, "rescale-in")
    world.advance(20)
    world.deploy()
    world.taskmanagers(2)
    world.restore()
    world.processed = world.exercise.records - 100
    world.flowing()
    world.state("FINISHED")
    world.job_state = "FINISHED"
    assert world.poll() is True
    assert "running_again_at" in world.exercise.outcomes["faults"][-1]


def test_input_finishing_before_the_last_fault_leaves_the_run_unconfirmed(world):
    """No progress can follow, so the remaining faults could never be seen."""
    through(world, "upgrade")
    world.advance(20)
    world.restore()
    world.processed = world.exercise.records - 100
    world.flowing()
    # The fault's recovery is seen, but the input is done: no further fault lands.
    world.poll()
    assert world.exercise.awaiting is None and world.exercise.step == 2
    world.state("FINISHED")
    with pytest.raises(Failure, match="unconfirmed"):
        world.poll()


def test_input_finished_before_a_fault_landed_is_unconfirmed_once_finished(world):
    """The fault restores completed input, which logs nothing more."""
    through(world, "upgrade")
    assert awaited(world) == "jobmanager"
    world.processed = world.exercise.records - 100
    world.flowing()
    assert world.poll() is False
    world.state("FINISHED")
    with pytest.raises(Failure, match="unconfirmed"):
        world.poll()


def test_a_completion_before_a_replay_logs_is_waited_out(world):
    """The stopped attempt logged the last record; the restore has not replayed yet."""
    through(world, "upgrade")
    world.advance(20)
    world.processed = world.exercise.records - 100
    world.flowing()
    world.advance(5)
    world.restore()
    # Restored and running, before the replay's first record.
    assert world.poll() is False
    assert awaited(world) == "jobmanager"
    world.processed = world.exercise.records - 300
    world.flowing()
    world.poll()
    assert awaited(world) == "taskmanager"


def test_a_savepoint_faults_stale_finished_is_settled_by_the_jobs_own_state(world):
    through(world, "rescale-in")
    assert awaited(world) == "rescale-out"
    world.processed = world.exercise.records - 100
    world.flowing()
    world.state("FINISHED")
    world.deploy()
    world.restore()
    world.job_state = TransportError("GET job: reset")
    assert world.poll() is False
    world.job_state = "RUNNING"
    assert world.poll() is False
    world.job_state = "FINISHED"
    with pytest.raises(Failure, match="unconfirmed"):
        world.poll()


def test_a_completion_before_a_restore_is_not_the_input_done(world):
    """The stopped attempt logged the last record; the restore went back before it."""
    through(world, "upgrade")
    world.advance(20)
    world.processed = world.exercise.records - 100
    world.flowing()
    world.advance(5)
    world.restore()
    # The JobManager's replacement replays from an earlier checkpoint.
    world.processed = world.exercise.records - 300
    world.flowing()
    world.poll()
    assert not world.exercise.input_complete()
    assert awaited(world) == "taskmanager"


def test_the_last_savepoint_faults_stale_finished_waits_for_the_job_after_recovery(
    world,
):
    """The input is done and the recovery seen, but the copies still run."""
    through(world, "rescale-in")
    world.advance(20)
    world.deploy()
    world.taskmanagers(2)
    world.restore()
    world.processed = world.exercise.records - 100
    world.flowing()
    world.state("FINISHED")
    assert world.poll() is False
    assert world.exercise.awaiting is None
    world.job_state = "FINISHED"
    assert world.poll() is True


def test_the_rest_reader_follows_the_loops_service_and_job(world):
    """Attached each poll whether or not the exercise measures."""
    world.start()
    assert world.readers == ["1" * 32]
    world.app["status"]["jobStatus"]["jobId"] = "2" * 32
    world.poll()
    assert world.readers == ["1" * 32, "2" * 32]
    world.exercise.attach_rest(None, "2" * 32)
    assert world.exercise.rest is None
    world.exercise.awaiting = None
    world.exercise.outcomes["faults"] = [{"kind": "upgrade"}]
    assert world.exercise.finished() is False


def test_a_restore_that_does_not_replay_the_input_keeps_its_completion(world):
    """Another region's restore, or a finished input's, logs no progress."""
    through(world, "rescale-in")
    world.advance(20)
    world.deploy()
    world.taskmanagers(2)
    world.restore()
    world.processed = world.exercise.records - 100
    world.flowing()
    world.poll()
    assert world.exercise.input_complete()
    world.advance(30)
    world.restore()
    world.poll()
    assert world.exercise.input_complete()
    world.state("FINISHED")
    world.job_state = "FINISHED"
    assert world.poll() is True


def test_input_done_while_a_patch_is_resent_is_unconfirmed(world):
    world.patch_errors.append((TransportError("PATCH: reset"), False))
    world.start()
    assert awaited(world) == "upgrade" and world.exercise.awaiting["generation"] is None
    world.processed = world.exercise.records - 100
    world.flowing()
    # The patch is not sent again on completed input.
    world.poll()
    assert world.patches == []
    world.state("FINISHED")
    with pytest.raises(Failure, match="unconfirmed"):
        world.poll()


def test_a_stale_finished_after_the_next_job_ran_is_waited_out(world):
    """The next job restored and logs, but its input is not done."""
    world.start()
    world.deploy()
    world.restore()
    world.flowing()
    world.state("FINISHED")
    assert world.poll() is False
    assert awaited(world) == "upgrade"
    world.state("RUNNING")
    world.poll()
    assert awaited(world) == "jobmanager"


@pytest.mark.parametrize("status", [422, 409])
def test_a_patch_that_conflicts_is_sent_again(world, status):
    """The Operator writing the application meanwhile is no reason to stop."""
    world.patch_errors.append(ApiError(status, "PATCH", "flinkdeployments"))
    world.start()
    assert awaited(world) == "upgrade" and not world.patches
    assert ("patch-conflict", {"kind": "upgrade", "status": status}) in world.emitted
    world.poll()
    assert len(world.patches) == 1
    world.recover("upgrade")
    assert awaited(world) == "jobmanager"


@pytest.mark.parametrize("landed", [True, False])
def test_a_patch_whose_answer_was_lost_counts_only_once_applied(world, landed):
    world.patch_errors.append((TransportError("PATCH: reset"), landed))
    world.start()
    assert len(world.patches) == (1 if landed else 0)
    if not landed:
        assert any(event == "patch-unconfirmed" for event, _ in world.emitted)
        world.poll()
        assert len(world.patches) == 1
    world.recover("upgrade")
    assert awaited(world) == "jobmanager"


def test_a_deletion_whose_answer_was_lost_is_sent_again_while_the_pod_runs(
    world, monkeypatch
):
    through(world, "upgrade")
    target = next(n for n in world.pods if n.startswith("jm-"))
    world.exercise.step -= 1
    world.exercise.awaiting = None
    world.exercise.outcomes["faults"].pop()
    world.deleted.clear()
    delete, get = world.env.kube.delete, world.env.kube.get

    def lost(pod, force=False):
        monkeypatch.setattr(world.env.kube, "delete", delete)
        raise TransportError("DELETE: reset")

    monkeypatch.setattr(world.env.kube, "delete", lost)
    monkeypatch.setattr(
        world.env.kube,
        "get",
        lambda kind, namespace, name: (
            copy.deepcopy(world.pods.get(name))
            if kind == "Pod"
            else get(kind, namespace, name)
        ),
    )
    world.poll()
    assert awaited(world) is None and world.exercise.step == 1
    world.poll()
    assert awaited(world) == "jobmanager" and world.deleted == [target]


def test_a_pod_gone_before_its_deletion_is_injected_again(world):
    through(world, "upgrade")
    target = next(n for n in world.pods if n.startswith("jm-"))
    # The JobManager was gone by the time its deletion was sent.
    world.exercise.step -= 1
    world.exercise.awaiting = None
    world.exercise.outcomes["faults"].pop()
    world.deleted.clear()
    world.gone.add(target)
    world.poll()
    assert awaited(world) is None and world.exercise.step == 1
    assert any(event == "fault-target-gone" for event, _ in world.emitted)
    world.gone.clear()
    world.poll()
    assert awaited(world) == "jobmanager" and world.deleted == [target]


@pytest.mark.parametrize(
    "change",
    [
        {"state": "FAILED"},
        {"state": "CANCELED"},
        {"state": "SUSPENDED"},
        {"reconciliation": "ROLLED_BACK"},
        {"deleted": True},
    ],
)
def test_what_stops_a_run(world, change):
    through(world, "upgrade")
    assert awaited(world) == "jobmanager"
    if "state" in change:
        world.state(change["state"])
    if "reconciliation" in change:
        world.app["status"]["reconciliationStatus"] = {
            "state": change["reconciliation"]
        }
    if "deleted" in change:
        world.app["metadata"]["deletionTimestamp"] = rt.utc(world.env.clock())
    with pytest.raises(Failure):
        world.poll()


@pytest.mark.parametrize("state", ["SUSPENDED", "CANCELED"])
def test_a_savepoint_fault_waits_out_the_jobs_suspension(world, state):
    world.start()
    world.state(state)
    assert world.poll() is False
    world.state("RUNNING")
    world.recover("upgrade")
    assert awaited(world) == "jobmanager"


def test_a_platform_interruption_is_recorded_once_and_waited_out(world):
    world.start()
    preempted(world, "tm-2")
    world.poll()
    world.poll()
    assert world.exercise.outcomes["unplanned"][0] == "Unplanned workload interruption"
    assert [p["cause"] for e, p in world.emitted if e == UNPLANNED_EVENT].count(
        "Unplanned workload interruption"
    ) == 1
    world.recover("upgrade")
    assert awaited(world) == "jobmanager"


def test_a_pod_replaced_between_two_polls_is_recorded(world):
    through(world, "rescale-out")
    del world.pods[next(n for n in world.pods if n.startswith("tm-"))]
    world.pod("tm-99", "taskmanager")
    world.poll()
    assert "Unplanned workload replacement" in world.exercise.outcomes["unplanned"]


def test_pods_only_leaving_are_no_replacement(world):
    through(world, "rescale-out")
    del world.pods[next(n for n in world.pods if n.startswith("tm-"))]
    world.poll()
    assert "unplanned" not in world.exercise.outcomes


def test_each_restore_is_recorded_once_whatever_its_count(world):
    world.start()
    for count in (1, 2, 5):
        world.restore()
        world.restores = count
        world.poll()
        world.poll()
    assert [e for e, _ in world.emitted].count("job-restore") == 3


def test_a_restored_initial_line_is_not_an_unplanned_start(world):
    """A platform restart before the upgrade restores the initial phase."""
    world.advance(10)
    world.exercise.progress(
        world.pods["tm-1"],
        f"{rt.utc(world.env.clock())} event=cloudtasks-progress "
        f"run_id={world.env.approval.run_id} phase=initial lineage={LINEAGE} "
        "restored=true processed=100 sequence=99",
    )
    assert len(world.exercise.samples) == 1


def test_a_fresh_start_before_the_first_fault_replaces_the_lineage(world):
    """A restart before any checkpoint reads the input again under a new lineage."""
    world.advance(10)
    world.progress("tm-1", "initial")
    world.advance(10)
    world.exercise.progress(
        world.pods["tm-1"],
        f"{rt.utc(world.env.clock())} event=cloudtasks-progress "
        f"run_id={world.env.approval.run_id} phase=initial "
        "lineage=223e4567-e89b-12d3-a456-426614174000 "
        "restored=false processed=100 sequence=99",
    )
    assert {s["lineage"] for s in world.exercise.samples.values()} == {
        "223e4567-e89b-12d3-a456-426614174000"
    }
    assert any(event == "fresh-start" for event, _ in world.emitted)


def test_the_lineage_is_fixed_by_the_first_fault(world):
    world.start()
    assert world.exercise.lineage == LINEAGE
    world.advance(10)
    with pytest.raises(Failure, match="Input lineage changed after the first fault"):
        world.exercise.progress(
            world.pods["tm-1"],
            f"{rt.utc(world.env.clock())} event=cloudtasks-progress "
            f"run_id={world.env.approval.run_id} phase=upgrade "
            "lineage=223e4567-e89b-12d3-a456-426614174000 "
            "restored=true processed=200 sequence=199",
        )


def test_a_poll_without_checkpoint_statistics_is_waited_out(world):
    world.start()
    assert world.exercise.rest_may_vanish()
    assert world.exercise.tolerate_rest(TransportError("GET proxy: reset"))
    world.exercise.restarts({})
    pods = copy.deepcopy(list(world.pods.values()))
    assert world.exercise.observe(copy.deepcopy(world.app), {}, pods) is False


def test_the_run_and_its_oracle_reads_end_within_the_time_limit(world):
    started = world.env.schedule.started
    assert world.exercise.deadline == min(
        started + TIME_LIMIT_SECONDS, world.env.schedule.cleanup_at
    )
    world.env.clock.now = world.exercise.deadline - 120
    assert world.exercise.oracle_window() == world.exercise.deadline - RECORD_SECONDS
    world.env.clock.now = world.exercise.deadline
    with pytest.raises(Failure, match="time limit"):
        world.poll()
