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
"""Construct the Pub/Sub actors the production entrypoints run."""

import uuid
from contextlib import contextmanager

from ..common import Failure
from ..google import actor_session
from ..runner import Runner
from ..supervisor import Supervisor
from .guard import PubSubGuard
from .handoff import PubSubHandoff
from .lifecycle import PubSubLifecycle
from .quiesce import barrier
from .session import PubSubSession


def _handoff(env, token, http, role):
    if env.actor != role or env.approval.scenario != "pubsub-recovery":
        raise Failure("Pub/Sub actor environment has the wrong role or approval")
    env.assert_owner()
    controller = PubSubLifecycle(env, http, PubSubGuard(env))
    return PubSubHandoff(controller, actor_token=token, settled=http.settled)


@contextmanager
def runner(env, *, runner_token, credentials=None):
    """Bind the runner to the environment dispatch approved.

    Keep this context open through start and settlement. The caller retains the
    original process token and owns dispatch authorization and the quiescence barrier.
    """
    with PubSubSession(actor_session(credentials)) as http:
        handoff = _handoff(env, runner_token, http, "runner")
        actor = Runner(env, pubsub=handoff)
        actor.admission_open()
        yield actor


@contextmanager
def supervisor(env, upgrade, *, credentials=None):
    """Bind the supervisor to the delivery `verify_delivery` checked.

    The quiescence barrier is built here from this run's own identity rather
    than accepted from the caller, as for BigQuery. Each supervisor process
    takes a fresh token, which joining binds.
    """
    quiesce = barrier(env)
    with PubSubSession(actor_session(credentials)) as http:
        handoff = _handoff(env, uuid.uuid4().hex, http, "supervisor")
        yield Supervisor(env, upgrade, pubsub=handoff, quiesce=quiesce)
