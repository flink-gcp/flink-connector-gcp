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
"""Construct the authenticated BigQuery actors the production entrypoints run."""

from contextlib import contextmanager

from ..actor_auth import authenticate
from ..common import Failure
from ..policy import BIGQUERY_OBSERVATIONS, QUERY_EVIDENCE_BYTES
from ..runner import Runner
from ..supervisor import Supervisor
from .auth import BigQuerySession
from .handoff import BigQueryHandoff
from .lifecycle import BigQueryLifecycle
from .quiesce import barrier
from .resources import BigQueryResources


def _handoff(env, application, runner_token, http, role):
    if env.actor != role or env.approval.scenario != "bigquery-recovery":
        raise Failure("BigQuery actor environment has the wrong role or scenario")
    env.assert_owner()
    resources = BigQueryResources(
        http, env.approval.bigquery_plan, env.schedule.cleanup_end, env.clock
    )
    return BigQueryHandoff(
        BigQueryLifecycle(env, resources, application),
        runner_token=runner_token,
        evidence_bytes=QUERY_EVIDENCE_BYTES,
        query_until=env.schedule.cleanup_at,
    )


@contextmanager
def runner(env, application, *, runner_token, credentials=None):
    """Bind the runner to the environment and application dispatch approved.

    Keep this context open through start and settlement. The caller retains the
    original process token and owns dispatch authorization and the quiescence barrier.
    """
    with BigQuerySession("runner", credentials) as http:
        handoff = _handoff(env, application, runner_token, http, "runner")
        actor = Runner(env, bigquery=handoff)
        actor.admission_open()
        authenticate(
            env,
            http,
            min(
                env.schedule.started + BIGQUERY_OBSERVATIONS["startup_seconds"],
                env.schedule.cleanup_at,
            ),
        )
        actor.admission_open()
        yield actor


@contextmanager
def supervisor(env, application, upgrade, *, credentials=None):
    """Bind the supervisor to the delivery `verify_delivery` checked.

    This does not authenticate the Kubernetes/storage collaborators. The
    quiescence barrier is built here from this run's own identity rather than
    accepted from the caller: a `callable` check cannot tell a proof from a
    constant, and `lambda: True` satisfied every check this module used to make.

    It takes no runner token. The supervisor starts before the runner writes
    the binding, and the handoff adopts the token on its first read.
    """
    quiesce = barrier(env)
    with BigQuerySession("supervisor", credentials) as http:
        handoff = _handoff(env, application, None, http, "supervisor")
        actor = Supervisor(env, upgrade, bigquery=handoff, quiesce=quiesce)
        authenticate(env, http, env.schedule.cleanup_at)
        yield actor
