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
"""Offline approval-bound Pub/Sub delivery; this does not authenticate admission."""

from .. import approval_bundle
from ..common import Failure, digest
from . import plan as pubsub_plan


def prepare(approval, *, prepared_at):
    """Build from a separately supplied version 5 approval, without service access."""
    approved, when = approval_bundle.check_approval(
        approval, prepared_at, "pubsub-recovery", "Pub/Sub"
    )
    approval_bundle.check_revision(approved.sha)
    bundle = pubsub_plan.prepare(
        run_id=approved.run_id,
        nonce=approved.nonce,
        started_at=approved.started_at,
        expires_at=approved.expires_at,
        active_seconds=pubsub_plan.ACTIVE_SECONDS,
        revision=approved.sha,
        application_image=approved.images["application"],
        trial=approved.pubsub_trial,
    )
    # The proposal names the second manifest for its role in the trial; the
    # approval keeps the name every service scenario shares.
    if (
        digest(bundle["application"]) != approved.application_sha256
        or digest(bundle["recovery_application"]) != approved.upgrade_application_sha256
        or bundle["proposal"]["images"]["supervisor"] != approved.images["supervisor"]
        or bundle["proposal"]["runtime_sha256"] != approved.runtime_sha256
        or bundle["proposal"]["delivery_sha256"] != approved.delivery_sha256
    ):
        raise Failure(
            "Rendered bundle differs from the approved manifests, images or source"
        )
    approval_bundle.check_revision(approved.sha)
    return approval_bundle.bind("pubsub-approval-bundle", approved, bundle, when)


def validate(bundle, approval):
    """Re-render against the caller's approval; embedded approval is not authority."""
    return approval_bundle.validate(prepare, bundle, approval)
