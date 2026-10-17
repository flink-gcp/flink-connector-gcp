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
"""The shared recovery exercise's fault kinds, driven poll by poll.

The BigQuery and Cloud Tasks suites run the default upgrade and JobManager
failover through their whole supervisor loops; these drive the base exercise
directly, over a real environment and record, with the job's answers scripted
for each poll, so each fault kind's proof and its refusals are held here.
"""

import copy

import pytest
from flink_tier3.common import TRANSIENT_STATUSES, ApiError, Failure, TransportError
from flink_tier3.exercise import RecoveryExercise
from flink_tier3.policy import CLOUDTASKS, CLOUDTASKS_OBSERVATIONS, CLOUDTASKS_STATE

from .cloudtasks.test_recovery_plan import inputs as inputs  # noqa: PLC0414
from .cloudtasks.test_recovery_plan import renderer as renderer  # noqa: PLC0414
from .cloudtasks.test_recovery_runtime import prepared as prepared  # noqa: PLC0414
from .test_lifecycle import env as env  # noqa: PLC0414
from .test_lifecycle import rt

LINEAGE = "123e4567-e89b-12d3-a456-426614174000"


# A scenario that samples nothing: no windows, and the failover begins once
# the upgrade is proved.
WINDOWS = (
    "warmup_seconds",
    "baseline_seconds",
    "post_upgrade_seconds",
    "post_recovery_seconds",
    "upgrade_wait_seconds",
)
IMMEDIATE = {
    key: value for key, value in CLOUDTASKS_OBSERVATIONS.items() if key not in WINDOWS
}
HOLD = 300
HELD = {**IMMEDIATE, "post_upgrade_seconds": HOLD}


class Exercise(RecoveryExercise):
    namespace = CLOUDTASKS
    state_bucket = CLOUDTASKS_STATE
    timing = IMMEDIATE
    progress_event = "cloudtasks-progress"
    expected_pods = 3
    records = 1000


class HeldExercise(Exercise):
    timing = HELD


class World:
    """One application, its Pods and its checkpoint history, poll by poll."""

    def __init__(
        self,
        prepared,
        monkeypatch,
        *,
        failover="jobmanager",
        rescale=None,
        exercise=Exercise,
    ):
        self.env, application, upgrade, _ = prepared
        self.env.actor = "supervisor"
        self.env.records.set_phase(rt.Phase.READY)
        self.env.records.set_phase(rt.Phase.RUNNING)
        self.upgrade = copy.deepcopy(upgrade)
        if rescale:
            self.upgrade["spec"]["taskManager"]["replicas"] = (
                3 if rescale == "out" else 1
            )
        self.app = copy.deepcopy(application)
        self.app["metadata"].update(uid="app-uid", resourceVersion="1", generation=1)
        self.app["status"] = {"jobStatus": {"state": "RUNNING", "jobId": "1" * 32}}
        self.pods = {}
        for name, component in (
            ("jm-1", "jobmanager"),
            ("tm-1", "taskmanager"),
            ("tm-2", "taskmanager"),
        ):
            self.pod(name, component)
        self.patches, self.deleted = [], []
        self.patch_error = None
        monkeypatch.setattr(self.env.kube, "patch", self.patch)
        monkeypatch.setattr(self.env.kube, "delete", self.delete)
        self.restored, self.completed = {}, {}
        # The restores the job's current JobMaster has counted.
        self.restores = 0
        self.processed = 0
        self.exercise = exercise(
            self.env, self.upgrade, failover=failover, rescale=rescale
        )

    def pod(self, name, component):
        self.pods[name] = {
            "metadata": {
                "namespace": CLOUDTASKS,
                "name": name,
                "uid": name + "-uid",
                "labels": {"component": component},
            },
            "status": {"phase": "Running"},
        }

    def patch(self, app, operations, subresource=""):
        assert app["metadata"]["uid"] == "app-uid" and not subresource
        if self.patch_error:
            raise self.patch_error
        self.patches.append(operations)
        for operation in operations:
            _, _, parent, field = operation["path"].split("/")
            self.app["spec"][parent][field] = operation["value"]
        self.app["metadata"].update(generation=2, resourceVersion="2")
        return copy.deepcopy(self.app)

    def delete(self, pod, force=False):
        assert not force
        self.deleted.append(pod["metadata"]["name"])
        return True

    def advance(self, seconds):
        self.env.clock.now += seconds

    def progress(self, pod, phase):
        self.processed += 100
        line = (
            f"{rt.utc(self.env.clock())} event=cloudtasks-progress "
            f"run_id={self.env.approval.run_id} phase={phase} lineage={LINEAGE} "
            f"restored={str(phase == 'upgrade').lower()} "
            f"processed={self.processed} sequence={self.processed - 1}"
        )
        self.exercise.progress(self.pods[pod], line)

    def checkpoint(self, number, subtasks=3):
        now = self.env.clock()
        self.completed = {
            "id": number,
            "status": "COMPLETED",
            "is_savepoint": False,
            "trigger_timestamp": now * 1000,
            "latest_ack_timestamp": (now + 1) * 1000,
            "num_subtasks": subtasks,
            "external_path": self.exercise.prefix("checkpoints") + f"chk-{number}",
        }

    def observe(self):
        """One poll: the loop hands its checkpoint statistics on, then observes."""
        rest = {
            "counts": {"restored": self.restores},
            "latest": {"completed": self.completed, "restored": self.restored},
        }
        self.exercise.restarts(rest)
        return self.exercise.observe(
            copy.deepcopy(self.app), rest, copy.deepcopy(list(self.pods.values()))
        )

    def baseline(self, subtasks=3):
        """Initial input from tm-1 and a checkpoint after it; the upgrade starts."""
        self.advance(10)
        self.progress("tm-1", "initial")
        self.advance(5)
        self.checkpoint(1, subtasks)
        self.observe()

    def upgraded(self, subtasks=3, progress_from=("tm-1",)):
        """The Operator's upgrade: a new JobManager, the savepoint restored."""
        boundary = self.exercise.boundary
        self.advance(40)
        del self.pods["jm-1"]
        self.pod("jm-2", "jobmanager")
        self.app["status"] = {
            "observedGeneration": 2,
            "reconciliationStatus": {"state": "DEPLOYED"},
            "jobStatus": {
                "state": "RUNNING",
                "jobId": "2" * 32,
                "savepointInfo": {
                    "lastSavepoint": {
                        "location": self.exercise.prefix("savepoints") + "sp-1",
                        "timeStamp": (boundary + 5) * 1000,
                        "triggerType": "UPGRADE",
                    }
                },
            },
        }
        self.restored = {
            "id": 2,
            "is_savepoint": True,
            "restore_timestamp": int(self.env.clock() * 1000),
            "external_path": self.exercise.prefix("savepoints") + "sp-1",
        }
        self.restores = 1
        for pod in progress_from:
            self.advance(5)
            self.progress(pod, "upgrade")
        self.advance(5)
        self.checkpoint(3, subtasks)
        self.observe()

    def failed_over(self, restored=3, progress_from="tm-2"):
        """The job back from its retained checkpoint, input advancing again."""
        self.advance(40)
        self.restored = {
            "id": restored,
            "is_savepoint": False,
            "restore_timestamp": int(self.env.clock() * 1000),
            "external_path": self.exercise.prefix("checkpoints") + f"chk-{restored}",
        }
        # A TaskManager's loss keeps the JobManager, which counts on.
        kept = self.exercise.failover == "taskmanager"
        self.restores = self.restores + 1 if kept else 1
        self.advance(5)
        self.progress(progress_from, "upgrade")
        self.advance(5)
        self.checkpoint(4)
        return self.observe()


@pytest.fixture
def world(prepared, monkeypatch, request):
    return World(prepared, monkeypatch, **getattr(request, "param", {}))


def test_the_default_trial_upgrades_the_arguments_and_deletes_the_jobmanager(world):
    world.baseline()
    assert world.exercise.stage == "upgrade"
    # Only the arguments: the default trial patches what it always has.
    assert world.patches == [
        [
            {
                "op": "replace",
                "path": "/spec/job/args",
                "value": world.upgrade["spec"]["job"]["args"],
            }
        ]
    ]
    world.upgraded()
    assert world.exercise.stage == "failover"
    assert world.deleted == ["jm-2"]
    del world.pods["jm-2"]
    world.pod("jm-3", "jobmanager")
    world.failed_over()
    assert world.exercise.stage == "finishing"
    outcomes = world.exercise.outcomes
    assert set(outcomes["upgrade"]) == {"restored", "checkpoint", "progress", "jm_uid"}
    assert set(outcomes["failover"]) == {"restored", "checkpoint", "progress", "jm_uid"}
    assert outcomes["failover"]["jm_uid"] == "jm-3-uid"


@pytest.mark.parametrize("world", [{"failover": "taskmanager"}], indirect=True)
def test_a_taskmanager_replacement_deletes_the_one_reporting_progress(world):
    world.baseline()
    world.upgraded(progress_from=("tm-1", "tm-2"))
    # The TaskManager whose task reported the latest input, not the first one.
    assert world.deleted == ["tm-2"]
    record = world.env.refresh().recovery
    assert record["stage"] == "failover"
    assert record["before"]["tm_uid"] == "tm-2-uid"
    assert record["before"]["tm"] == "tm-2"
    assert record["before"]["jm_uid"] == "jm-2-uid"
    # The deleted Pod still terminating: recovery is not proved yet.
    world.pods["tm-2"]["metadata"]["deletionTimestamp"] = "2026-10-13T00:01:00Z"
    world.failed_over(progress_from="tm-1")
    assert world.exercise.stage == "failover"
    del world.pods["tm-2"]
    world.pod("tm-3", "taskmanager")
    world.advance(5)
    world.checkpoint(5)
    world.observe()
    assert world.exercise.stage == "finishing"
    proof = world.exercise.outcomes["failover"]
    # The same JobManager kept the job, and the loss is named in the proof.
    assert proof["jm_uid"] == "jm-2-uid"
    assert proof["replaced"] == {"uid": "tm-2-uid", "name": "tm-2"}
    assert proof["restored"]["id"] == 3
    # Recovery is proved, so the Pods running now are the stable set.
    assert world.exercise.stable_pods == {"jm-2-uid", "tm-1-uid", "tm-3-uid"}


@pytest.mark.parametrize("world", [{"failover": "taskmanager"}], indirect=True)
@pytest.mark.parametrize("reporter", ["terminating", "jobmanager"])
def test_only_a_running_taskmanager_is_a_target(world, reporter):
    world.baseline()
    if reporter == "terminating":
        world.pods["tm-1"]["metadata"]["deletionTimestamp"] = "2026-10-13T00:01:00Z"
        world.upgraded()
    else:
        # A reporter that is not a TaskManager, as the selection reads labels.
        world.upgraded(progress_from=("jm-2",))
    assert world.exercise.stage == "upgrade"
    assert world.deleted == []


@pytest.mark.parametrize("world", [{"failover": "taskmanager"}], indirect=True)
def test_a_taskmanager_replacement_waits_for_its_target_to_run(world):
    world.baseline()
    world.pods["tm-1"]["status"]["phase"] = "Pending"
    world.upgraded()
    # Upgrade proved, but no running TaskManager reported the latest input.
    assert world.exercise.stage == "upgrade"
    assert world.deleted == []
    world.pods["tm-1"]["status"]["phase"] = "Running"
    world.advance(15)
    world.observe()
    assert world.deleted == ["tm-1"]
    assert world.exercise.stage == "failover"


@pytest.mark.parametrize("world", [{"failover": "taskmanager"}], indirect=True)
def test_a_target_that_never_runs_ends_at_the_upgrade_deadline(world):
    world.baseline()
    world.pods["tm-1"]["status"]["phase"] = "Pending"
    world.upgraded()
    world.advance(CLOUDTASKS_OBSERVATIONS["recovery_seconds"])
    with pytest.raises(Failure, match="deadline expired: upgrade"):
        world.observe()
    assert world.deleted == []


@pytest.mark.parametrize("world", [{"failover": "taskmanager"}], indirect=True)
def test_a_jobmanager_replaced_during_a_taskmanager_replacement_fails(world):
    world.baseline()
    world.upgraded()
    del world.pods["tm-1"], world.pods["jm-2"]
    world.pod("jm-3", "jobmanager")
    with pytest.raises(Failure, match="JobManager was replaced"):
        world.failed_over()


@pytest.mark.parametrize("world", [{"failover": "taskmanager"}], indirect=True)
@pytest.mark.parametrize(
    "change",
    [
        {"is_savepoint": True},
        {"id": 2},
        {"external_path": "gs://elsewhere/chk-3"},
        {"job_id": "3" * 32},
    ],
)
def test_a_taskmanager_replacement_must_restore_a_retained_checkpoint(world, change):
    world.baseline()
    world.upgraded()
    del world.pods["tm-1"]
    change = dict(change)
    if "job_id" in change:
        world.app["status"]["jobStatus"]["jobId"] = change.pop("job_id")
    world.advance(40)
    world.restored = {
        "id": 3,
        "is_savepoint": False,
        "restore_timestamp": int(world.env.clock() * 1000),
        "external_path": world.exercise.prefix("checkpoints") + "chk-3",
        **change,
    }
    world.restores = 2
    world.advance(5)
    world.progress("tm-2", "upgrade")
    world.advance(5)
    world.checkpoint(4)
    with pytest.raises(Failure, match="TaskManager replacement did not recover"):
        world.observe()


@pytest.mark.parametrize("world", [{"failover": "taskmanager"}], indirect=True)
def test_a_lost_taskmanager_delete_response_must_show_it_going(world):
    world.baseline()

    def lost(pod, force=False):
        raise ApiError(503, "DELETE", pod["metadata"]["name"])

    world.env.kube.delete = lost
    # The Pod is still there, unmarked: the deletion is not known to have landed.
    world.env.kube.put({**world.pods["tm-1"], "kind": "Pod"})
    with pytest.raises(Failure, match="TM delete response lost"):
        world.upgraded()


@pytest.mark.parametrize(
    "world,subtasks",
    [({"rescale": "out"}, 4), ({"rescale": "in"}, 2)],
    indirect=["world"],
)
def test_a_rescale_patches_its_sizes_and_proves_the_new_parallelism(world, subtasks):
    world.baseline()
    replicas = world.upgrade["spec"]["taskManager"]["replicas"]
    assert world.patches == [
        [
            {
                "op": "replace",
                "path": "/spec/job/args",
                "value": world.upgrade["spec"]["job"]["args"],
            },
            {"op": "replace", "path": "/spec/taskManager/replicas", "value": replicas},
        ]
    ]
    world.upgraded(subtasks=subtasks)
    assert world.exercise.stage == "failover"
    assert world.exercise.outcomes["upgrade"]["rescale"] == {
        "direction": world.exercise.rescale,
        "subtasks_before": 3,
        "subtasks_after": subtasks,
    }


@pytest.mark.parametrize("world", [{"rescale": "out"}], indirect=True)
def test_a_rescale_adds_a_size_the_initial_manifest_leaves_to_flink(world):
    world.upgrade["spec"]["job"]["parallelism"] = 3
    world.app["spec"]["job"].pop("parallelism", None)
    world.baseline()
    assert {"op": "add", "path": "/spec/job/parallelism", "value": 3} in (
        world.patches[0]
    )


@pytest.mark.parametrize(
    "world,subtasks",
    [({"rescale": "out"}, 3), ({"rescale": "out"}, 2), ({"rescale": "in"}, 4)],
    indirect=["world"],
)
def test_a_rescale_that_did_not_move_the_subtasks_as_approved_fails(world, subtasks):
    world.baseline()
    with pytest.raises(Failure, match="did not move the checkpointed subtasks"):
        world.upgraded(subtasks=subtasks)


@pytest.mark.parametrize("world", [{"rescale": "in"}], indirect=True)
@pytest.mark.parametrize("before,after", [(3, None), (None, 2), ("3", 2)])
def test_a_rescale_needs_both_checkpoints_subtask_counts(world, before, after):
    world.baseline(subtasks=before)
    with pytest.raises(Failure, match="do not report their subtasks"):
        world.upgraded(subtasks=after)


def test_an_upgrade_that_rescales_without_approval_is_not_applied(world):
    world.upgrade["spec"]["taskManager"]["replicas"] = 3
    with pytest.raises(Failure, match="without a rescale"):
        world.baseline()
    assert world.patches == []
    # Refused before the transition is recorded, so no upgrade is pending.
    assert world.env.refresh().recovery["stage"] == "baseline"


@pytest.mark.parametrize("world", [{"rescale": "in"}], indirect=True)
@pytest.mark.parametrize(
    "want,have", [(0, 2), (True, 2), ("2", 2), (None, 2), (1, "2")]
)
def test_a_rescale_size_must_be_a_positive_count(world, want, have):
    world.upgrade["spec"]["taskManager"]["replicas"] = want
    world.app["spec"]["taskManager"]["replicas"] = have
    with pytest.raises(Failure, match="positive count"):
        world.baseline()
    assert world.patches == []


@pytest.mark.parametrize("world", [{"rescale": "in"}], indirect=True)
def test_a_rescale_does_not_add_a_section_the_application_lacks(world):
    world.upgrade["spec"]["taskManager"]["replicas"] = 1
    del world.app["spec"]["taskManager"]
    with pytest.raises(Failure, match="section the application lacks"):
        world.baseline()
    assert world.patches == []
    assert world.env.refresh().recovery["stage"] == "baseline"


@pytest.mark.parametrize(
    "kind",
    [{"failover": "taskmanagers"}, {"failover": None}, {"rescale": "sideways"}],
)
def test_the_fault_kind_must_be_one_the_exercise_runs(prepared, kind):
    environment, _, upgrade, _ = prepared
    with pytest.raises(Failure, match="approved fault kind"):
        Exercise(environment, upgrade, **kind)


@pytest.fixture
def held(prepared, monkeypatch, request):
    return World(
        prepared,
        monkeypatch,
        exercise=HeldExercise,
        **getattr(request, "param", {}),
    )


def hold(world):
    """The baseline and the upgrade, proved and held for its window."""
    world.baseline()
    world.upgraded()
    assert world.exercise.stage == "upgraded"
    assert world.deleted == []


def test_a_held_upgrade_measures_a_window_before_the_failover(held):
    hold(held)
    proved_at = held.env.clock()
    record = held.env.refresh().recovery
    assert record["stage"] == "upgraded"
    assert record["until"] == rt.utc(proved_at + HOLD)
    assert set(record["outcomes"]["upgrade"]) == {
        "restored",
        "checkpoint",
        "progress",
        "jm_uid",
    }
    # The upgraded job is a steady state: its Pods are the stable set.
    assert held.exercise.stable_pods == {"jm-2-uid", "tm-1-uid", "tm-2-uid"}
    assert not held.exercise.recovering
    held.advance(HOLD - 1)
    held.progress("tm-1", "upgrade")
    held.checkpoint(4)
    held.observe()
    assert held.exercise.stage == "upgraded"
    assert held.deleted == []
    held.advance(1)
    held.progress("tm-1", "upgrade")
    held.checkpoint(5)
    held.observe()
    assert held.exercise.stage == "failover"
    assert held.deleted == ["jm-2"]
    before = held.env.refresh().recovery["before"]
    # The failover must not restore behind the window's latest checkpoint.
    assert before["checkpoint"]["id"] == 5
    assert before["processed"] == held.processed
    del held.pods["jm-2"]
    held.pod("jm-3", "jobmanager")
    held.failed_over(restored=5)
    assert held.exercise.stage == "finishing"
    # The upgrade's proof is the one taken when it was proved.
    assert held.exercise.outcomes["upgrade"]["checkpoint"]["id"] == 3


def test_a_failover_behind_the_held_windows_checkpoint_fails(held):
    hold(held)
    held.advance(HOLD)
    held.progress("tm-1", "upgrade")
    held.checkpoint(5)
    held.observe()
    assert held.exercise.stage == "failover"
    del held.pods["jm-2"]
    held.pod("jm-3", "jobmanager")
    with pytest.raises(Failure, match="did not recover the checkpointed job"):
        held.failed_over(restored=3)


def test_a_held_window_waits_for_a_checkpoint_taken_inside_it(held):
    hold(held)
    held.advance(HOLD)
    held.progress("tm-1", "upgrade")
    # The latest completed checkpoint is still the one the proof read.
    held.observe()
    assert held.exercise.stage == "upgraded"
    held.advance(CLOUDTASKS_OBSERVATIONS["recovery_seconds"])
    with pytest.raises(Failure, match="deadline expired: upgraded"):
        held.observe()
    assert held.deleted == []


def test_a_pod_replaced_inside_the_held_window_is_unplanned(held):
    hold(held)
    pods = [copy.deepcopy(pod) for pod in held.pods.values()]
    held.exercise.scheduling(pods)
    del held.pods["tm-2"]
    held.pod("tm-3", "taskmanager")
    with pytest.raises(Failure, match="Unplanned workload replacement"):
        held.exercise.scheduling(copy.deepcopy(list(held.pods.values())))


def test_a_rest_error_inside_the_held_window_is_not_a_recoverys(held):
    hold(held)
    assert not held.exercise.tolerate_rest(ApiError(503, "GET", "checkpoints"))


def test_a_job_that_finishes_inside_the_held_window_fails(held):
    hold(held)
    held.app["status"]["jobStatus"]["state"] = "FINISHED"
    with pytest.raises(Failure, match="without both recovery proofs"):
        held.observe()


@pytest.mark.parametrize("held", [{"failover": "taskmanager"}], indirect=True)
def test_a_held_upgrade_deletes_the_taskmanager_reporting_after_the_window(held):
    hold(held)
    held.advance(HOLD)
    held.progress("tm-2", "upgrade")
    held.checkpoint(5)
    held.observe()
    assert held.deleted == ["tm-2"]
    assert held.env.refresh().recovery["before"]["tm"] == "tm-2"


def test_a_scenario_without_windows_completes_when_its_input_does(world):
    """No post-recovery window to outlast: the finished input completes it."""
    world.baseline()
    world.upgraded()
    del world.pods["jm-2"]
    world.pod("jm-3", "jobmanager")
    world.failed_over()
    assert world.exercise.stage == "finishing"
    assert world.exercise.post_until is None
    world.processed = Exercise.records - 100
    world.advance(5)
    world.progress("tm-2", "upgrade")
    world.app["status"]["jobStatus"]["state"] = "FINISHED"
    assert world.observe() is True
    assert world.exercise.stage == "complete"


def test_a_sampling_scenario_must_name_its_windows(prepared):
    environment, _, upgrade, _ = prepared

    class Sampling(Exercise):
        measurement_event = "sampling-measurement"
        baseline_event = "sampling-baseline-window"

    with pytest.raises(Failure, match="windows' events and timing"):
        Sampling(environment, upgrade)

    class Unnamed(Sampling):
        timing = CLOUDTASKS_OBSERVATIONS
        baseline_event = None

    with pytest.raises(Failure, match="windows' events and timing"):
        Unnamed(environment, upgrade)


def restarted(world, count):
    """The job restores its latest checkpoint again, on the Pods it ran on."""
    world.advance(20)
    world.restored = {
        **world.restored,
        "id": world.completed["id"],
        "is_savepoint": False,
        "restore_timestamp": int(world.env.clock() * 1000),
    }
    world.restores = count


class Statistics:
    """A REST reader answering only the job's checkpoint statistics.

    With a `count`, the job has just restored again; without one, it has
    not restored at all.
    """

    def __init__(self, world, count=None):
        self.world, self.count = world, count
        self.error = None

    def job(self, path):
        assert path == "/checkpoints"
        if self.error:
            raise self.error
        if self.count is None:
            return {"counts": {"restored": 0}, "latest": {"restored": None}}
        restored = {
            "id": self.world.completed["id"],
            "is_savepoint": False,
            "restore_timestamp": int(self.world.env.clock() * 1000),
        }
        return {"counts": {"restored": self.count}, "latest": {"restored": restored}}


def test_each_planned_recovery_accounts_for_its_one_restore(world):
    """A new JobMaster counts its restore from one; later polls see it again."""
    world.baseline()
    world.upgraded()
    assert world.exercise.last_restore["stage"] == "upgrade"
    del world.pods["jm-2"]
    world.pod("jm-3", "jobmanager")
    world.failed_over()
    assert world.exercise.stage == "finishing"
    assert world.exercise.last_restore["stage"] == "failover"
    world.advance(30)
    world.observe()
    assert world.exercise.last_restore["count"] == 1


@pytest.mark.parametrize("world", [{"failover": "taskmanager"}], indirect=True)
@pytest.mark.parametrize("count", [1, 3])
def test_a_taskmanager_replacement_adds_one_restore_to_the_jobmaster(world, count):
    """The kept JobMaster counted the upgrade's restore; three is one unseen."""
    world.baseline()
    world.upgraded()
    assert world.deleted == ["tm-1"]
    restarted(world, count)
    with pytest.raises(Failure, match="Unplanned job restart"):
        world.observe()


def test_a_restore_outside_a_recovery_is_unplanned(world, monkeypatch):
    """A restart on the same Pods, before the upgrade, leaves Kubernetes as it was."""
    world.advance(10)
    world.progress("tm-1", "initial")
    world.checkpoint(1)
    restarted(world, 1)
    emitted = []
    monkeypatch.setattr(
        world.env, "emit", lambda event, payload: emitted.append((event, payload))
    )
    with pytest.raises(Failure, match="Unplanned job restart"):
        world.observe()
    assert emitted == [
        (
            "unplanned-restore",
            {
                "stage": "baseline",
                "restored": world.restored,
                "count": 1,
                "expected": None,
            },
        )
    ]
    assert world.patches == []


@pytest.mark.parametrize("count", [1, 2])
def test_a_restore_inside_the_held_window_is_unplanned(held, count):
    """As in `ct1778-rescale-out-a6`, or a new JobMaster counting from one."""
    hold(held)
    held.advance(5)
    held.checkpoint(5)
    restarted(held, count)
    with pytest.raises(Failure, match="Unplanned job restart"):
        held.observe()


@pytest.mark.parametrize("world", [{"failover": "taskmanager"}], indirect=True)
def test_a_second_restore_inside_one_recovery_is_unplanned(world):
    """The kept JobMaster's count rises by one for each, so only the stage tells."""
    world.baseline()
    world.upgraded()
    # The deleted Pod still terminating: the first restore proves nothing yet.
    world.pods["tm-1"]["metadata"]["deletionTimestamp"] = "2026-10-13T00:01:00Z"
    world.failed_over()
    assert world.exercise.stage == "failover"
    assert world.exercise.last_restore["count"] == 2
    restarted(world, 3)
    with pytest.raises(Failure, match="Unplanned job restart"):
        world.observe()


def test_a_restart_just_before_the_upgrade_is_read_again(world):
    """A new JobMaster would count from one, so the old one is read before the patch."""
    world.exercise.rest = Statistics(world, 1)
    with pytest.raises(Failure, match="Unplanned job restart"):
        world.baseline()
    assert world.patches == []
    assert world.env.refresh().recovery["stage"] == "baseline"


def test_a_restart_just_before_the_jobmanager_deletion_is_read_again(held):
    held.exercise.rest = Statistics(held)
    hold(held)
    held.exercise.rest.count = 2
    held.advance(HOLD)
    held.progress("tm-1", "upgrade")
    held.checkpoint(5)
    with pytest.raises(Failure, match="Unplanned job restart"):
        held.observe()
    assert held.deleted == []


@pytest.mark.parametrize(
    "error, deleted",
    [
        *(
            (ApiError(status, "GET", "checkpoints"), ["jm-2"])
            for status in TRANSIENT_STATUSES
        ),
        (TransportError("GET checkpoints: reset"), ["jm-2"]),
        (ApiError(403, "GET", "checkpoints"), []),
        (ApiError(404, "GET", "checkpoints"), []),
    ],
)
def test_a_transient_error_on_the_read_before_the_deletion_lets_it_go_ahead(
    held, error, deleted
):
    """The poll's own read stands; only a transient refusal is passed over."""
    held.exercise.rest = Statistics(held)
    hold(held)
    held.exercise.rest.error = error
    held.advance(HOLD)
    held.progress("tm-1", "upgrade")
    held.checkpoint(5)
    if deleted:
        held.observe()
    else:
        with pytest.raises(ApiError):
            held.observe()
    assert held.deleted == deleted


@pytest.mark.parametrize("at", ["1", 1.5e12])
def test_a_restore_without_an_integer_time_is_refused(world, at):
    world.baseline()
    world.restored = {"id": 1, "restore_timestamp": at}
    with pytest.raises(Failure, match="no restore time"):
        world.observe()


def test_a_restore_without_its_count_is_refused(world):
    world.baseline()
    restarted(world, None)
    with pytest.raises(Failure, match="no restore count"):
        world.observe()
