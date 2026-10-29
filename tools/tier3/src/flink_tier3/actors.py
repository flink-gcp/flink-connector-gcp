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
"""Construct the actors of a scenario whose runner creates one resource."""

from contextlib import contextmanager

from .bigtable.instance import Instance
from .bigtable.lifecycle import BigtableLifecycle
from .cloudtasks.lifecycle import CloudTasksLifecycle
from .cloudtasks.queue import Queue
from .common import Failure
from .google import actor_session
from .quiesce import namespace_writers, poll_writers
from .resource import RESOURCE_SCENARIOS
from .runner import Runner
from .spanner.instance import Instance as SpannerInstance
from .spanner.lifecycle import SpannerLifecycle
from .supervisor import Supervisor

# Each scenario's REST client and lifecycle controller.
CONTROLLERS = {
    "cloudtasks-recovery": (Queue, CloudTasksLifecycle),
    "bigtable-recovery": (Instance, BigtableLifecycle),
    "spanner-recovery": (SpannerInstance, SpannerLifecycle),
}


def _service(env):
    """The service the approved scenario names in messages, or "Resource"."""
    scenario = RESOURCE_SCENARIOS.get(env.approval.scenario)
    return scenario.service if scenario else "Resource"


def _controller(env, application, http, role):
    if env.actor != role or env.approval.scenario not in CONTROLLERS:
        raise Failure(
            _service(env) + " actor environment has the wrong role or scenario"
        )
    env.assert_owner()
    client, lifecycle = CONTROLLERS[env.approval.scenario]
    plan = getattr(
        env.approval, RESOURCE_SCENARIOS[env.approval.scenario].field + "_plan"
    )
    return lifecycle(
        env, client(http, plan, env.schedule.cleanup_end, env.clock), application
    )


def barrier(env):
    """Build the quiescence callback for this run's resource supervisor.

    What it proves, from a fresh listing on every call: no object that could
    run or restore a writer remains in the application namespace, whoever
    owns it. The workload's grants reach every resource of its kind, Cloud
    Tasks queues project-wide, Bigtable's `bt1316-` instances and Spanner's
    `sp1360-` instances, and any Pod running as the namespace's service
    account holds them, so the whole namespace is what must be empty, as for
    Pub/Sub. Deletes on this path are graceful, so a Pod leaving the API
    means its containers terminated.

    What it does not prove: that no write a terminated Pod sent is still
    executing at the service. One that lands after the resource's deletion is
    refused, because the resource is gone and the workload cannot create one.
    """
    if env.actor != "supervisor" or env.approval.scenario not in CONTROLLERS:
        raise Failure(
            f"The {_service(env)} quiescence barrier belongs to its supervisor"
        )
    return poll_writers(env, lambda: namespace_writers(env), _service(env))


@contextmanager
def runner(env, application, *, credentials=None):
    """Bind the runner to the environment and application dispatch approved.

    Keep this context open through start and settlement.
    """
    with actor_session(credentials) as http:
        actor = Runner(env, resource=_controller(env, application, http, "runner"))
        actor.admission_open()
        yield actor


@contextmanager
def supervisor(env, application, upgrade, *, credentials=None):
    """Bind the supervisor to the delivery `verify_delivery` checked."""
    quiesce = barrier(env)
    with actor_session(credentials) as http:
        controller = _controller(env, application, http, "supervisor")
        yield Supervisor(env, upgrade, resource=controller, quiesce=quiesce)
