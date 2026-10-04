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
"""Writer listings and the retrying quiescence poll the service barriers share."""

from .cloudtasks.session import transient
from .common import Failure
from .policy import POLL

# Kinds that can carry or restore a writer. A Pod runs one; the controllers
# above it put one back, which is why their absence is required too rather
# than inferred from an empty Pod list at one instant.
# Controllers first, Pods last: a Pod created between the two listings by a
# controller that is itself already listed is still caught, while the reverse
# order would miss a create-then-collect that straddles them. This is not the
# same set `verify_idle` refuses after deletion, deliberately in both
# directions: it adds Deployment and ReplicaSet, which restore a writer but
# which the idle check tolerates from the Operator, and it omits
# PersistentVolumeClaim, which the idle check refuses but which writes
# nothing.
# Consecutive unreadable attempts tolerated inside one call. The cleanup
# window cannot bound this: `Cleanup.run` computes its own deadline through
# `cleanup_window(now)` and `env.schedule.cleanup_end` is the static expiry
# the run usually reaches before cleanup starts, so reading it here would
# spend the retry before the first one. One more than the two consecutive
# transient reads the supervisor already tolerates elsewhere.
UNREAD_RETRIES = 3

WRITER_KINDS = (
    "FlinkDeployment",
    "FlinkSessionJob",
    "FlinkBlueGreenDeployment",
    "Deployment",
    "StatefulSet",
    "ReplicaSet",
    "CronJob",
    "Job",
    "Pod",
)


def namespace_writers(env):
    """Every object in the application namespace that could run a writer now."""
    namespace = env.approval.application_namespace
    return [
        obj
        for kind in WRITER_KINDS
        for obj in env.kube.items(kind, namespace)
        if obj["metadata"]["namespace"] == namespace
    ]


def poll_writers(env, writers, service):
    """The quiescence callback around one fresh listing of remaining writers."""

    def quiesce():
        # Never answer "unknown". The caller asks twice per poll and the second
        # asker turns anything but True into a failure that ends the cleanup
        # pass, so a read that says nothing about the cluster is retried here
        # rather than reported. Exhausting the retries still raises: a cluster
        # this supervisor cannot read is not one it can call quiescent.
        for _ in range(UNREAD_RETRIES):
            try:
                remaining = writers()
                break
            except Failure as error:
                if not transient(error):
                    raise
                env.emit("quiescence-unread", {"cause": type(error).__name__})
                env.sleep(POLL)
        else:
            raise Failure(service + " quiescence could not be read")
        if remaining:
            env.emit(
                "quiescence-pending",
                {
                    "remaining": sorted(
                        f"{obj['kind']}/{obj['metadata']['name']}" for obj in remaining
                    )[:20]
                },
            )
            return False
        return True

    return quiesce
