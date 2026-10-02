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
"""The approval-bound Pub/Sub delivery and its tamper checks, without admission."""

import copy
import json

import pytest
from flink_tier3 import approval_bundle
from flink_tier3 import pubsub_bundle as bundles
from flink_tier3 import pubsub_plan as plan
from flink_tier3.bundle import delivery_digest, source_digest
from flink_tier3.common import Failure
from flink_tier3.model import Approval
from flink_tier3.policy import PUBSUB, PUBSUB_CEILINGS, SMOKE
from test_pubsub_plan import inputs as inputs  # noqa: PLC0414
from test_pubsub_plan import renderer as renderer  # noqa: PLC0414
from test_pubsub_plan import trial as trial  # noqa: PLC0414
from test_tier3_lifecycle import env as env  # noqa: PLC0414


@pytest.fixture
def approval(env, inputs, renderer, monkeypatch):
    monkeypatch.setattr(approval_bundle, "check_revision", lambda revision: None)
    value = copy.deepcopy(env[2])
    inputs.update(run_id=value["run_id"], nonce=value["nonce"])
    rendered = plan.prepare(**inputs)
    proposed = rendered["proposal"]
    value.update(
        version=5,
        scenario="pubsub-recovery",
        started_at=proposed["started_at"],
        expires_at=proposed["expires_at"],
        cleanup_at=proposed["cleanup_at"],
        runtime_sha256=source_digest(),
        delivery_sha256=delivery_digest(),
        application_sha256=proposed["application_sha256"],
        upgrade_application_sha256=proposed["recovery_application_sha256"],
        ceilings=dict(PUBSUB_CEILINGS),
        pubsub_trial=copy.deepcopy(inputs["trial"]),
    )
    value["namespaces"][PUBSUB] = value["namespaces"].pop(SMOKE)
    value["images"].pop("smoke")
    value["images"].update(
        supervisor=proposed["images"]["supervisor"],
        application=inputs["application_image"],
    )
    return value


@pytest.mark.parametrize(
    "prepared_at,seconds",
    [("2026-09-21T00:00:00Z", 3420), ("2026-09-21T00:10:00Z", 2820)],
)
def test_delivery_binds_approval_and_shortens_job_budget(
    approval, prepared_at, seconds
):
    before = copy.deepcopy(approval)
    value = bundles.prepare(approval, prepared_at=prepared_at)
    assert value["kind"] == "pubsub-approval-bundle"
    assert value["admission_enabled"] is False
    assert value["proposal"]["approved"] is False
    data = value["delivery"]["config"]["data"]
    assert json.loads(data["approval.json"]) == Approval.from_dict(approval).to_dict()
    assert json.loads(data["application.json"]) == value["application"]
    assert (
        json.loads(data["upgrade-application.json"]) == (value["recovery_application"])
    )
    assert value["delivery"]["supervisor"]["spec"]["activeDeadlineSeconds"] == seconds
    assert bundles.validate(value, approval) == value
    assert approval == before


@pytest.mark.parametrize(
    "key",
    [
        "runtime_sha256",
        "delivery_sha256",
        "application_sha256",
        "upgrade_application_sha256",
        "supervisor",
    ],
)
def test_mismatched_approval_refuses_delivery(approval, key):
    if key == "supervisor":
        approval["images"][key] = approval["images"][key][:-64] + "0" * 64
    else:
        approval[key] = "0" * 64
    with pytest.raises(Failure, match="differs"):
        bundles.prepare(approval, prepared_at=approval["started_at"])


def test_the_approved_trial_is_the_one_rendered(approval, renderer):
    """A different trial renders different manifests, which the digests refuse."""
    bundles.prepare(approval, prepared_at=approval["started_at"])
    options = renderer[-1][4]
    assert options["pubsub_trial"] == approval["pubsub_trial"]["trial"]
    assert (
        options["pubsub_records"]
        == (approval["pubsub_trial"]["records_per_subscription"])
    )
    assert options["pubsub_entry_point"] == approval["pubsub_trial"]["entry_point"]
    approval["pubsub_trial"]["trial"] = "rescale-in"
    with pytest.raises(Failure, match="differs"):
        bundles.prepare(approval, prepared_at=approval["started_at"])


@pytest.mark.parametrize(
    "at",
    [
        "2026-09-20T23:59:59Z",
        "2026-09-21T00:00:00.500Z",
        "2026-09-21T00:45:00Z",
        "2026-09-21T01:00:00Z",
    ],
)
def test_preparation_stays_in_the_approved_window(approval, at):
    with pytest.raises(Failure):
        bundles.prepare(approval, prepared_at=at)


def test_other_approval_and_malformed_bundle_are_rejected(env, approval):
    with pytest.raises(Failure, match="Pub/Sub"):
        bundles.prepare(env[2], prepared_at=env[2]["started_at"])
    for value in (None, [], {}):
        with pytest.raises(Failure):
            bundles.validate(value, approval)
    with pytest.raises(Failure, match="JSON object"):
        bundles.prepare([], prepared_at=approval["started_at"])


def test_an_internally_consistent_bundle_cannot_supply_its_own_approval(approval):
    other = copy.deepcopy(approval)
    # A field the rendering does not read, so only the approval tells them apart.
    other["actor"] = "another-actor"
    value = bundles.prepare(other, prepared_at=other["started_at"])
    assert bundles.validate(value, other) == value
    with pytest.raises(Failure, match="separately supplied approval"):
        bundles.validate(value, approval)


@pytest.mark.parametrize("changed_after_render", [False, True])
def test_checkout_drift_refuses_preparation(
    approval, monkeypatch, changed_after_render
):
    checks = []

    def check(revision):
        assert revision == approval["sha"]
        checks.append(revision)
        if len(checks) > int(changed_after_render):
            raise Failure("Checkout changed")

    monkeypatch.setattr(approval_bundle, "check_revision", check)
    with pytest.raises(Failure, match="Checkout changed"):
        bundles.prepare(approval, prepared_at=approval["started_at"])


def test_configmap_size_includes_embedded_approval(approval, monkeypatch):
    original = plan.prepare

    def at_limit(*args, **kwargs):
        proposed = original(*args, **kwargs)
        data = proposed["delivery"]["config"]["data"]
        data["padding"] = ""
        used = sum(
            len(key.encode()) + len(value.encode()) for key, value in data.items()
        )
        data["padding"] = "x" * (1024**2 - used)
        return proposed

    monkeypatch.setattr(plan, "prepare", at_limit)
    with pytest.raises(Failure, match="ConfigMap exceeds"):
        bundles.prepare(approval, prepared_at=approval["started_at"])
