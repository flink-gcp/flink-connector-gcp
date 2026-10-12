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
"""Construct the Cloud Tasks actors the production entrypoints run."""

from contextlib import contextmanager

from ..common import Failure
from ..google import actor_session
from ..quiesce import namespace_writers, poll_writers
from ..runner import Runner
from ..supervisor import Supervisor
from .lifecycle import CloudTasksLifecycle
from .queue import Queue


def _controller(env, application, http, role):
    if env.actor != role or env.approval.scenario != "cloudtasks-recovery":
        raise Failure("Cloud Tasks actor environment has the wrong role or scenario")
    env.assert_owner()
    queue = Queue(
        http, env.approval.cloudtasks_plan, env.schedule.cleanup_end, env.clock
    )
    return CloudTasksLifecycle(env, queue, application)


def barrier(env):
    """Build the quiescence callback for this run's Cloud Tasks supervisor.

    What it proves, from a fresh listing on every call: no object that could
    run or restore a writer remains in the application namespace, whoever
    owns it. The workload's task-creation grant is project-wide, and any Pod
    running as the namespace's service account holds it, so the whole
    namespace is what must be empty, as for Pub/Sub. Deletes on this path are
    graceful, so a Pod leaving the API means its containers terminated.

    What it does not prove: that no create a terminated Pod sent is still
    executing at the service. A create that lands after the queue's deletion
    is refused, because the queue is gone and the workload cannot create one.
    """
    if env.actor != "supervisor" or env.approval.scenario != "cloudtasks-recovery":
        raise Failure("The Cloud Tasks quiescence barrier belongs to its supervisor")
    return poll_writers(env, lambda: namespace_writers(env), "Cloud Tasks")


@contextmanager
def runner(env, application, *, credentials=None):
    """Bind the runner to the environment and application dispatch approved.

    Keep this context open through start and settlement.
    """
    with actor_session(credentials) as http:
        actor = Runner(env, cloudtasks=_controller(env, application, http, "runner"))
        actor.admission_open()
        yield actor


@contextmanager
def supervisor(env, application, upgrade, *, credentials=None):
    """Bind the supervisor to the delivery `verify_delivery` checked."""
    quiesce = barrier(env)
    with actor_session(credentials) as http:
        controller = _controller(env, application, http, "supervisor")
        yield Supervisor(env, upgrade, cloudtasks=controller, quiesce=quiesce)
