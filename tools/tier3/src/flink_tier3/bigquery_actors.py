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
"""Construct authenticated internal actors without enabling production admission."""

from contextlib import contextmanager

from . import bigquery_bundle
from .bigquery_auth import BigQuerySession
from .bigquery_handoff import BigQueryHandoff
from .bigquery_lifecycle import BigQueryLifecycle
from .bigquery_resources import BigQueryResources
from .bundle import delivery_digest, source_digest
from .common import Failure, digest
from .model import validate_approval
from .policy import BIGQUERY_OBSERVATIONS, HTTP_TIMEOUT, MIB
from .runner import Runner
from .supervisor import Supervisor


def _handoff(env, application, runner_token, http, role):
    if env.actor != role or env.approval.scenario != "bigquery-recovery":
        raise Failure("BigQuery actor environment has the wrong role or scenario")
    validate_approval(env.approval.to_dict(), env.clock())
    # The runner runs from a complete installation; the supervisor runs from a
    # mounted subset, whose digest is the pin the approval carries for it.
    if role == "supervisor":
        installed, approved = delivery_digest(), env.approval.delivery_sha256
    else:
        installed, approved = source_digest(), env.approval.runtime_sha256
    if installed != approved:
        raise Failure("BigQuery actor source differs from approval")
    env.assert_owner()
    resources = BigQueryResources(
        http, env.approval.bigquery_plan, env.schedule.cleanup_end, env.clock
    )
    return BigQueryHandoff(
        BigQueryLifecycle(env, resources, application),
        runner_token=runner_token,
        evidence_bytes=10 * MIB,
        query_until=env.schedule.cleanup_at,
    )


def _authenticate(env, http, deadline):
    http.authenticate(timeout=min(HTTP_TIMEOUT, deadline - env.clock()))
    if env.clock() >= deadline:
        raise Failure("BigQuery actor construction deadline expired")
    env.assert_owner()


@contextmanager
def runner(env, bundle, *, runner_token, credentials=None):
    """Verify against the external environment approval, then bind its runner.

    Keep this context open through start and settlement. The caller retains the
    original process token and owns dispatch authorization and the external fence.
    """
    verified = bigquery_bundle.validate(bundle, env.approval)
    with BigQuerySession("runner", credentials) as http:
        handoff = _handoff(env, verified["application"], runner_token, http, "runner")
        actor = Runner(env, bigquery=handoff)
        actor.admission_open()
        _authenticate(
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
def supervisor(env, application, upgrade, *, runner_token, quiesce, credentials=None):
    """Bind mounted manifests/source to the caller's independent approval.

    This does not re-render CUE inside the runtime image or authenticate the
    Kubernetes/storage collaborators. The external writer fence is mandatory.
    """
    if not callable(quiesce):
        raise Failure("BigQuery supervisor requires an external quiescence callback")
    if digest(upgrade) != env.approval.upgrade_application_sha256:
        raise Failure("BigQuery supervisor upgrade differs from approval")
    with BigQuerySession("supervisor", credentials) as http:
        handoff = _handoff(env, application, runner_token, http, "supervisor")
        actor = Supervisor(env, upgrade, bigquery=handoff, quiesce=quiesce)
        _authenticate(env, http, env.schedule.cleanup_at)
        yield actor
