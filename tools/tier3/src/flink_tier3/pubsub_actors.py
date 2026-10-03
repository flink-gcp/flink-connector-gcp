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
"""Construct the authenticated Pub/Sub actors the production entrypoints run."""

import uuid
from contextlib import contextmanager

from . import pubsub_bundle
from .actor_auth import authenticate, require_installed_source
from .common import Failure, digest
from .model import validate_approval
from .pubsub_auth import PubSubSession
from .pubsub_guard import PubSubGuard
from .pubsub_handoff import PubSubHandoff
from .pubsub_lifecycle import PubSubLifecycle
from .pubsub_plan import require_trial_jobs
from .pubsub_quiesce import barrier
from .pubsub_traffic import PubSubTraffic
from .runner import Runner
from .supervisor import Supervisor


def _handoff(env, application, token, http, role):
    approval = env.approval
    if (
        env.actor != role
        or approval.scenario != "pubsub-recovery"
        or approval.version != 5
    ):
        raise Failure("Pub/Sub actor environment has the wrong role or approval")
    validate_approval(approval.to_dict(), env.clock())
    require_installed_source(env, role, "Pub/Sub")
    env.assert_owner()
    controller = PubSubLifecycle(env, http, application, PubSubGuard(env))
    return PubSubHandoff(
        PubSubTraffic(controller, application, approval.pubsub_traffic_limits),
        actor_token=token,
        settled=http.settled,
    )


@contextmanager
def runner(env, bundle, *, runner_token, credentials=None):
    """Verify against the external environment approval, then bind its runner.

    The bundle re-renders against the caller's approval, which checks both
    manifests' digests and that they run the approved trial. Keep this context
    open through start and settlement. The caller retains the original process
    token and owns dispatch authorization and the external fence.
    """
    verified = pubsub_bundle.validate(bundle, env.approval)
    with PubSubSession("runner", credentials) as http:
        handoff = _handoff(env, verified["application"], runner_token, http, "runner")
        actor = Runner(env, pubsub=handoff)
        actor.admission_open()
        authenticate(env, http, handoff.controller.before_operation.deadline)
        actor.admission_open()
        yield actor


@contextmanager
def supervisor(env, application, upgrade, *, credentials=None):
    """Bind mounted manifests and source to the caller's independent approval.

    Nothing re-renders the mounted manifests, so their digests are checked
    here, and so is that both run exactly the job the approved trial derives:
    a digest pins a manifest but does not say what it runs. The quiescence
    barrier is built here from this run's own identity rather than accepted
    from the caller, as for BigQuery. Each supervisor process takes a fresh
    token: joining binds it, and a replacement cannot act on a former
    process's authority.
    """
    approval = env.approval
    quiesce = barrier(env)
    if (
        digest(application) != approval.application_sha256
        or digest(upgrade) != approval.upgrade_application_sha256
    ):
        raise Failure("Pub/Sub supervisor manifests differ from approval")
    require_trial_jobs(approval.run_id, approval.pubsub_trial, application, upgrade)
    with PubSubSession("supervisor", credentials) as http:
        handoff = _handoff(env, application, uuid.uuid4().hex, http, "supervisor")
        actor = Supervisor(env, upgrade, pubsub=handoff, quiesce=quiesce)
        authenticate(env, http, env.schedule.cleanup_at)
        yield actor
