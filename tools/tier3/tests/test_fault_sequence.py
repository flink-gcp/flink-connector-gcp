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

import pytest
from flink_tier3 import exercise as exercise_module
from flink_tier3.bigquery.exercise import BigQuerySequenceExercise
from flink_tier3.bigtable.exercise import BigtableSequenceExercise
from flink_tier3.cloudtasks.exercise import CloudTasksSequenceExercise
from flink_tier3.common import UNPLANNED_EVENT, ApiError, Failure, TransportError
from flink_tier3.exercise import (
    RECORD_SECONDS,
    SEQUENCE_PARALLELISM,
    SHUTDOWN_MESSAGE,
    TIME_LIMIT_SECONDS,
    UNCONFIRMED,
    FaultSequence,
)
from flink_tier3.firestore.exercise import FirestoreSequenceExercise
from flink_tier3.metrics import FlinkRest
from flink_tier3.policy import MIB
from flink_tier3.pubsub.exercise import PubSubSequenceExercise
from flink_tier3.spanner.exercise import SpannerSequenceExercise

from .sequence_world import LINEAGE, Exercise, World, preempted
from .sequence_world import env as env  # noqa: PLC0414
from .sequence_world import inputs as inputs  # noqa: PLC0414
from .sequence_world import prepared as prepared  # noqa: PLC0414
from .sequence_world import renderer as renderer  # noqa: PLC0414
from .test_lifecycle import rt


class Sequenced(FaultSequence, Exercise):
    pass


class SequenceWorld(World):
    """The sequence's world, whose Operator moves a generation per patch.

    A fault's recovery is a restore the job's statistics report, after which
    the job logs progress again; the stopped job may log before that.
    """

    def __init__(self, prepared, monkeypatch):
        super().__init__(prepared, monkeypatch, Sequenced)
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

    def delete(self, pod):
        if pod["metadata"]["name"] in self.gone:
            return False
        return super().delete(pod)

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

    def flowing(self, tm=None):
        """The job logs progress on `tm`, else the first TaskManager."""
        tm = tm or next(n for n in self.pods if n.startswith("tm-"))
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


@pytest.mark.parametrize("reads", [True, False], ids=["reads", "does-not-read"])
def test_a_poll_whose_checkpoint_read_failed_completes_only_without_a_restore_read(
    world, reads
):
    """A verdict that reads the latest restore waits for statistics that show it.

    Any other completes at once: BigQuery's FILE_LOADS check refuses a load
    still running when the job finished, which a later poll would miss.
    """
    world.exercise.reads_final_restore = reads
    through(world, "rescale-out")
    world.advance(5)
    world.processed = world.exercise.records - 100
    world.flowing()
    world.advance(5)
    world.restore()
    world.state("FINISHED")
    world.job_state = "FINISHED"
    world.exercise.restarts({})
    pods = copy.deepcopy(list(world.pods.values()))
    completed = world.exercise.observe(copy.deepcopy(world.app), {}, pods)
    assert completed is not reads
    if reads:
        assert world.exercise.stage != "complete"
        assert world.poll() is True
        assert world.exercise.restored_at == world.restored["restore_timestamp"] / 1000
        assert world.exercise.restored_after_input()


def test_a_finish_before_a_read_showed_the_restore_is_unconfirmed(world):
    """The job ran again and finished between polls, and this poll's read failed.

    The run does not wait for the next read, so a run whose BigQuery
    FILE_LOADS load outlived the job cannot reach a verdict on a later poll.
    """
    through(world, "rescale-in")
    world.advance(20)
    world.deploy()
    world.taskmanagers(2)
    world.restore()
    world.processed = world.exercise.records - 100
    world.flowing()
    world.state("FINISHED")
    world.job_state = "FINISHED"
    world.exercise.restarts({})
    pods = copy.deepcopy(list(world.pods.values()))
    with pytest.raises(Failure, match=UNCONFIRMED):
        world.exercise.observe(copy.deepcopy(world.app), {}, pods)


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

    def lost(pod):
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


def node_event(world, reason, node, at, message):
    """An event on a Node, as Kubernetes records it in `default`."""
    name = f"{reason}-{node}-{at}"
    world.env.kube.put(
        {
            "kind": "Event",
            "apiVersion": "v1",
            "metadata": {
                "namespace": "default",
                "name": name,
                "uid": name + "-uid",
                "resourceVersion": "1",
            },
            "involvedObject": {"kind": "Node", "name": node},
            "reason": reason,
            "message": message,
            "lastTimestamp": at,
        }
    )


def shutdown(world, seconds, node="spot-node", message=SHUTDOWN_MESSAGE):
    """The kubelet's `Shutdown`, ``seconds`` after the poll that last saw the Pod."""
    node_event(world, "Shutdown", node, rt.utc(world.env.clock() + seconds), message)


def scale_down(world, seconds, node="spot-node"):
    node_event(
        world,
        "ScaleDown",
        node,
        rt.utc(world.env.clock() + seconds),
        "marked the node as toBeDeleted/unschedulable",
    )


def place(world, pod, node, bound=-3600):
    """Binds a Pod to a node ``bound`` seconds from now; it was created three hours ago."""
    now = world.env.clock()
    world.pods[pod]["spec"] = {"nodeName": node}
    world.pods[pod]["metadata"]["creationTimestamp"] = rt.utc(now - 10800)
    world.pods[pod]["status"]["conditions"] = [
        {
            "type": "PodScheduled",
            "status": "True",
            "lastTransitionTime": rt.utc(now + bound),
        }
    ]


def lose_tm2(world, bound=-3600):
    """The poll that last sees a TaskManager on its node, then Flink's replacement of it.

    After the last fault, so the Pods running are the stable set, and the
    replacement is neither only Pods leaving nor only Pods arriving.
    """
    through(world, "rescale-out")
    assert awaited(world) is None
    lost = next(name for name in world.pods if name.startswith("tm-"))
    place(world, lost, "spot-node", bound)
    world.exercise.scheduling(copy.deepcopy(list(world.pods.values())))
    del world.pods[lost]
    world.pod("tm-99", "taskmanager")


def recorded(world):
    """What one poll's scheduling read recorded as the run's unplanned causes."""
    world.exercise.scheduling(copy.deepcopy(list(world.pods.values())))
    return world.exercise.outcomes.get("unplanned")


INTERRUPTION = ["Unplanned workload interruption"]
REPLACEMENT = ["Unplanned workload replacement"]


def test_a_pod_replaced_after_its_node_shut_down_is_an_interruption(world):
    lose_tm2(world)
    shutdown(world, 16)
    emitted = len(world.emitted)
    # Flink has already replaced the Pod, so only the node's event shows the
    # reclamation.
    assert recorded(world) == INTERRUPTION
    assert [
        (event, [item["involvedObject"]["name"] for item in payload])
        for event, payload in world.emitted[emitted:]
        if event == "scheduling-events"
    ] == [("scheduling-events", ["spot-node"])]


def test_a_shutdown_a_long_poll_after_the_pod_is_still_an_interruption(world):
    lose_tm2(world)
    shutdown(world, 75)
    assert recorded(world) == INTERRUPTION


@pytest.mark.parametrize(
    ("node", "marked"),
    [
        # Another node's removal.
        ("other-node", 70),
        # A removal marked more than ten minutes before the shutdown.
        ("spot-node", 75 - 601),
        # A mark recorded after the shutdown.
        ("spot-node", 80),
    ],
)
def test_a_scale_down_that_did_not_remove_the_node_vetoes_nothing(world, node, marked):
    lose_tm2(world)
    scale_down(world, marked, node=node)
    shutdown(world, 75)
    assert recorded(world) == INTERRUPTION


@pytest.mark.parametrize(
    ("node", "seconds", "scaled_down", "message"),
    [
        # An earlier life of the node name: after the Pod's creation, before its binding.
        ("spot-node", -7200, False, SHUTDOWN_MESSAGE),
        # The autoscaler removing the node, just after marking it.
        ("spot-node", 75, True, SHUTDOWN_MESSAGE),
        # Another node, which held none of the run's Pods.
        ("other-node", 16, False, SHUTDOWN_MESSAGE),
        # The kubelet cancelling a shutdown, under the same reason.
        (
            "spot-node",
            16,
            False,
            "Shutdown manager detected shutdown cancellation event",
        ),
    ],
)
def test_a_shutdown_that_did_not_stop_the_pod_is_no_interruption(
    world, node, seconds, scaled_down, message
):
    lose_tm2(world)
    if scaled_down:
        scale_down(world, seconds - 5, node=node)
    shutdown(world, seconds, node=node, message=message)
    assert recorded(world) == REPLACEMENT


def test_a_mark_from_before_the_binding_still_vetoes(world):
    # An in-flight binding can follow the autoscaler's mark, so the mark still
    # counts, and the cause, if any, is the replacement's, not the platform's.
    lose_tm2(world, bound=-300)
    scale_down(world, -400)
    shutdown(world, 75)
    assert recorded(world) == REPLACEMENT


def test_a_mark_published_while_the_shutdowns_are_read_still_vetoes(world, monkeypatch):
    # The autoscaler marks the node just before it shuts it down, so a mark can
    # land between two reads; the shutdowns are read first for that reason.
    lose_tm2(world)
    read = world.env.kube.node_events

    def node_events(reason):
        events = read(reason)
        if reason == "Shutdown":
            scale_down(world, 70)
            shutdown(world, 75)
            events = read(reason)
        return events

    monkeypatch.setattr(world.env.kube, "node_events", node_events)
    assert recorded(world) == REPLACEMENT


def test_a_node_shutdown_with_no_pod_lost_is_no_interruption(world):
    through(world, "rescale-out")
    assert awaited(world) is None
    held = next(name for name in world.pods if name.startswith("tm-"))
    place(world, held, "spot-node")
    world.exercise.scheduling(copy.deepcopy(list(world.pods.values())))
    shutdown(world, 16)
    assert recorded(world) is None
    # The same shutdown is an interruption once the Pod it held is replaced.
    del world.pods[held]
    world.pod("tm-99", "taskmanager")
    assert recorded(world) == INTERRUPTION


@pytest.mark.parametrize(
    "status",
    [
        # A kubelet eviction under resource pressure.
        {"phase": "Failed", "reason": "Evicted"},
        # The kubelet's own termination, which only the disruption marks.
        {
            "phase": "Running",
            "conditions": [
                {
                    "type": "DisruptionTarget",
                    "status": "True",
                    "reason": "TerminationByKubelet",
                }
            ],
        },
    ],
    ids=["evicted", "disruption-target"],
)
def test_an_eviction_is_recorded_and_waited_out(world, status):
    """An eviction may be the job's own limits, so it is recorded, never a stop."""
    world.start()
    world.pods["tm-2"]["status"] = status
    world.poll()
    assert world.exercise.outcomes["unplanned"] == ["Unplanned workload eviction"]
    world.pods["tm-2"]["status"] = {"phase": "Running"}
    world.recover("upgrade")
    assert awaited(world) == "jobmanager"


def test_a_restarted_container_is_recorded_and_waited_out(world):
    world.start()
    world.pods["tm-2"]["status"]["containerStatuses"] = [{"restartCount": 1}]
    world.poll()
    assert "Unplanned container restart" in world.exercise.outcomes["unplanned"]
    world.recover("upgrade")
    assert awaited(world) == "jobmanager"


def test_the_taskmanager_fault_waits_for_the_reporting_taskmanager_to_run(world):
    """Only the TaskManager the latest input came from is the fault's target."""
    through(world, "upgrade")
    assert awaited(world) == "jobmanager"
    reporter = next(n for n in world.pods if n.startswith("tm-"))
    world.pods[reporter]["metadata"]["deletionTimestamp"] = rt.utc(world.env.clock())
    world.pod("tm-extra", "taskmanager")
    world.recover("jobmanager")
    # The job runs again, but its latest input came from a TaskManager on its
    # way out, so nothing is deleted yet.
    assert awaited(world) is None and world.exercise.step == 2
    del world.pods[reporter]["metadata"]["deletionTimestamp"]
    world.poll()
    assert awaited(world) == "taskmanager"
    assert world.deleted[-1] == reporter


def test_the_taskmanager_fault_waits_while_the_reporting_taskmanager_is_pending(world):
    """A Pod that is not `Running` is not the fault's target yet."""
    through(world, "upgrade")
    reporter = next(n for n in world.pods if n.startswith("tm-"))
    world.pods[reporter]["status"]["phase"] = "Pending"
    world.pod("tm-extra", "taskmanager")
    world.recover("jobmanager")
    assert awaited(world) is None and world.exercise.step == 2
    world.pods[reporter]["status"]["phase"] = "Running"
    world.poll()
    assert awaited(world) == "taskmanager"
    assert world.deleted[-1] == reporter


def test_running_again_needs_the_taskmanagers_to_be_running(world):
    """A pending TaskManager does not count toward the parallelism."""
    through(world, "rescale-in")
    assert awaited(world) == "rescale-out"
    world.advance(20)
    world.deploy()
    world.taskmanagers(2)
    pending = [n for n in world.pods if n.startswith("tm-")][-1]
    world.pods[pending]["status"]["phase"] = "Pending"
    world.restore()
    world.flowing()
    world.poll()
    assert awaited(world) == "rescale-out"
    world.pods[pending]["status"]["phase"] = "Running"
    world.poll()
    assert awaited(world) is None


def test_the_taskmanager_fault_deletes_the_latest_reporter(world):
    """Of two TaskManagers that logged progress, the later one is the target."""
    through(world, "upgrade")
    assert awaited(world) == "jobmanager"
    world.advance(20)
    for name in [n for n in world.pods if n.startswith("jm-")]:
        del world.pods[name]
    world.pod("jm-2", "jobmanager")
    world.deploy()
    world.restore()
    world.flowing("tm-1")
    world.flowing("tm-2")
    world.poll()
    assert awaited(world) == "taskmanager"
    assert world.exercise.outcomes["faults"][-1]["pod"]["name"] == "tm-2"
    assert world.deleted[-1] == "tm-2"


def test_the_rest_reader_reads_the_job_through_its_services_proxy(
    prepared, monkeypatch
):
    environment = prepared[0]
    calls = []

    def request(method, path, *, limit):
        calls.append((method, path, limit))
        return {"state": "RUNNING"}

    monkeypatch.setattr(environment.kube, "request", request)
    rest = FlinkRest(environment, {"metadata": {"name": "run-rest"}}, "a" * 32)
    assert rest.job("/checkpoints") == {"state": "RUNNING"}
    service = environment.kube.path(
        "Service", environment.approval.application_namespace, "run-rest:8081"
    )
    assert calls == [("GET", service + "/proxy/jobs/" + "a" * 32 + "/checkpoints", MIB)]


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
    assert world.exercise.tolerate_rest(TransportError("GET proxy: reset"))
    world.exercise.restarts({})
    pods = copy.deepcopy(list(world.pods.values()))
    assert world.exercise.observe(copy.deepcopy(world.app), {}, pods) is False


@pytest.mark.parametrize(
    "status,tolerated",
    [
        (404, True),
        (408, True),
        (429, True),
        (500, True),
        (503, True),
        (401, False),
        (403, False),
    ],
)
def test_which_answers_of_a_rest_read_are_waited_out(world, status, tolerated):
    """A missing endpoint and the transient statuses; nothing else."""
    world.start()
    error = ApiError(status, "GET", "checkpoints")
    assert world.exercise.tolerate_rest(error) is tolerated


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


@pytest.mark.parametrize(
    "exercise,reads",
    [
        (SpannerSequenceExercise, True),
        (BigtableSequenceExercise, True),
        (FirestoreSequenceExercise, False),
        (BigQuerySequenceExercise, False),
        (CloudTasksSequenceExercise, False),
        (PubSubSequenceExercise, False),
    ],
)
def test_only_the_change_stream_verdicts_wait_for_a_restore_read(exercise, reads):
    """BigQuery's FILE_LOADS check must run on the poll that saw the job finish."""
    assert exercise.reads_final_restore is reads
