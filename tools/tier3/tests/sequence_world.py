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
"""One application, its Pods and its checkpoint history, for the sequence tests.

The fault-sequence suite drives the shared exercise poll by poll over a real
environment and record, with the job's answers scripted for each poll; this
is the world it runs in and the fixtures it takes.
"""

import copy

from flink_tier3.exercise import RecoveryExercise
from flink_tier3.policy import CLOUDTASKS, CLOUDTASKS_OBSERVATIONS, CLOUDTASKS_STATE

from .dispatched import inputs as inputs  # noqa: PLC0414
from .dispatched import prepared as prepared  # noqa: PLC0414
from .dispatched import renderer as renderer  # noqa: PLC0414
from .test_lifecycle import env as env  # noqa: PLC0414
from .test_lifecycle import rt

LINEAGE = "123e4567-e89b-12d3-a456-426614174000"


class Exercise(RecoveryExercise):
    namespace = CLOUDTASKS
    state_bucket = CLOUDTASKS_STATE
    timing = CLOUDTASKS_OBSERVATIONS
    progress_event = "cloudtasks-progress"
    records = 1000


class World:
    """One application, its Pods and its checkpoint history, poll by poll.

    A subclass answers the application's patches with `patch`, and names the
    `exercise` the polls drive.
    """

    def __init__(self, prepared, monkeypatch, exercise):
        self.env, application, upgrade, _ = prepared
        self.env.actor = "supervisor"
        self.env.records.set_phase(rt.Phase.READY)
        self.env.records.set_phase(rt.Phase.RUNNING)
        self.upgrade = copy.deepcopy(upgrade)
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
        monkeypatch.setattr(self.env.kube, "patch", self.patch)
        monkeypatch.setattr(self.env.kube, "delete", self.delete)
        self.restored, self.completed = {}, {}
        # The restores the job's current JobMaster has counted.
        self.restores = 0
        self.processed = 0
        self.exercise = exercise(self.env, self.upgrade)

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

    def delete(self, pod):
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

    def checkpoint(self, number):
        now = self.env.clock()
        self.completed = {
            "id": number,
            "status": "COMPLETED",
            "is_savepoint": False,
            "trigger_timestamp": now * 1000,
            "latest_ack_timestamp": (now + 1) * 1000,
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


def preempted(world, name):
    """The scheduler's preemption of a Pod, which the platform marks on it."""
    world.pods[name]["status"]["conditions"] = [
        {
            "type": "DisruptionTarget",
            "status": "True",
            "reason": "PreemptionByScheduler",
        }
    ]
