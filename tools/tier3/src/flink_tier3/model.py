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
"""Tier-3 lifecycle model."""

from __future__ import annotations

import copy
import re
from dataclasses import asdict, dataclass, field, fields, replace
from decimal import Decimal
from enum import Enum
from typing import ClassVar

from .common import Failure, quantity, timestamp
from .policy import (
    BIGQUERY,
    BIGTABLE,
    CLEANUP_SECONDS,
    CLOUDTASKS,
    COST_RATES,
    GAR,
    POD_RESOURCES,
    PUBSUB,
    RUN_ID,
    SHA,
    SMOKE,
    SYSTEM,
    WINDOWS,
)


def estimated_cost(seconds):
    resources = {
        key: sum(
            quantity(values[key]) * (2 if role == "smoke" else 1)
            for role, values in POD_RESOURCES.items()
        )
        for key in COST_RATES
    }
    compute = (
        sum(
            resources[key] / (1024**3 if key != "cpu" else 1) * rate
            for key, rate in COST_RATES.items()
        )
        * Decimal(str(seconds))
        / 3600
    )
    # Reserve covers bounded GCS storage, operations and telemetry transfer;
    # the already-running cluster's standing fee is outside incremental cost.
    return compute + Decimal("0.25")


def _hourly(shapes):
    resources = {
        key: sum(quantity(values[key]) for values in shapes) for key in COST_RATES
    }
    return sum(
        resources[key] / (1024**3 if key != "cpu" else 1) * rate
        for key, rate in COST_RATES.items()
    )


# The approval schema every scenario writes. Nothing gates on it: recovery
# reads a held run's approval as written.
APPROVAL_VERSION = 6
SCENARIOS = tuple(WINDOWS)
# Each service scenario's application namespace; a smoke run's is SMOKE.
APPLICATION_NAMESPACES = {
    "pubsub-recovery": PUBSUB,
    "bigquery-recovery": BIGQUERY,
    "cloudtasks-recovery": CLOUDTASKS,
    "bigtable-recovery": BIGTABLE,
}


def validate_approval(approval):
    """Check the approval dispatch assembled, once, before it takes the lock.

    Nothing checks the schema again: admission checks only the window
    (`check_window`), and recovery reads a held run's approval as written, so
    a later policy change cannot strand that run's recovery.
    """
    required = {
        "version",
        "run_id",
        "nonce",
        "sha",
        "started_at",
        "expires_at",
        "cleanup_at",
        "namespaces",
        "images",
        "operator_uid",
        "baseline_uids",
        "lock_owner",
        "application_sha256",
    }
    if not isinstance(approval, dict) or not required <= approval.keys():
        raise Failure("Incomplete run approval")
    if (
        approval["lock_owner"].get("nonce") != approval["nonce"]
        or approval["lock_owner"].get("rig_sha", approval["lock_owner"].get("sha"))
        != approval["sha"]
        or approval["lock_owner"].get("run_id") != approval["run_id"]
    ):
        raise Failure("Approval and lock identity differ")
    if approval["operator_uid"] not in approval["baseline_uids"]:
        raise Failure("Operator must belong to the observed foundation")
    scenario = approval.get("scenario", "smoke")
    if scenario not in SCENARIOS:
        raise Failure("Unknown approved scenario")
    if not approval.keys() <= {item.name for item in fields(Approval)}:
        raise Failure("Approval carries fields outside its schema")
    if not RUN_ID.fullmatch(approval.get("run_id", "")):
        raise Failure("Invalid run approval identity")
    service = scenario != "smoke"
    upgrade = approval.get("upgrade_application_sha256", "")
    if service and not re.fullmatch(r"[0-9a-f]{64}", upgrade):
        raise Failure("Service approval must pin its own upgrade manifest")
    if not service and upgrade:
        raise Failure("Smoke approval cannot authorize recovery operations")
    if not SHA.fullmatch(approval.get("sha", "")) or not re.fullmatch(
        r"[0-9a-f]{32}", approval.get("nonce", "")
    ):
        raise Failure("Approval must pin a revision and nonce")
    start, end = timestamp(approval["started_at"]), timestamp(approval["expires_at"])
    if (
        start != int(start)
        or end - start != WINDOWS[scenario]
        or timestamp(approval["cleanup_at"]) != end - CLEANUP_SECONDS
    ):
        raise Failure("Approval window differs from its scenario's policy")
    application = APPLICATION_NAMESPACES.get(scenario, SMOKE)
    if set(approval["namespaces"]) != {application, SYSTEM}:
        raise Failure("Approval must name its application and control namespaces")
    for saved in approval["namespaces"].values():
        if (
            not saved.get("uid")
            or not saved.get("quota_uid")
            or saved["hard"].get("pods") != "0"
            or saved["hard"].get("persistentvolumeclaims") != "0"
        ):
            raise Failure(
                "Approval requires observed zero Pod/PVC quotas and namespace UIDs"
            )
    roles = {"operator": "operator", "supervisor": "lifecycle-tools"}
    if service:
        roles["application"] = scenario
    else:
        roles["smoke"] = "smoke"
    if set(approval["images"]) != set(roles):
        raise Failure("Approval must name exactly the runtime image roles")
    for key, package in roles.items():
        if not re.fullmatch(
            re.escape(GAR + package) + r"@sha256:[0-9a-f]{64}", approval["images"][key]
        ):
            raise Failure("Every runtime image must be an approved GAR digest")
    if not re.fullmatch(r"[0-9a-f]{64}", approval.get("application_sha256", "")):
        raise Failure("Approval must pin its application bytes")


def check_window(approval, now):
    """Refuse an admission step outside the approval's admission window."""
    if now < timestamp(approval.started_at) - 30 or now >= timestamp(
        approval.cleanup_at
    ):
        raise Failure("Approval is outside its admission window")


@dataclass(frozen=True)
class Schedule:
    """Absolute run deadlines and the serial recovery budget."""

    started: float
    cleanup_at: float
    expires_at: float
    job_deadline: float
    cleanup_end: float
    force_at: float
    runner_wait_until: float
    post_cleanup_grace: int = 180
    state_cleanup_seconds: int = 120
    operator_grace: int = 60
    plan_budget_seconds: ClassVar[int] = 540
    setup_and_receipt_seconds: ClassVar[int] = 300

    @classmethod
    def for_window(cls, started, end):
        return cls(
            started, end - CLEANUP_SECONDS, end, end - 180, end, end - 300, end - 180
        )

    @classmethod
    def from_approval(cls, approval):
        return cls.for_window(
            timestamp(approval.started_at), timestamp(approval.expires_at)
        )

    def active_seconds(self, now):
        seconds = int(self.job_deadline - now)
        if seconds <= 0:
            raise Failure("Supervisor Job deadline has elapsed")
        return seconds

    def cleanup_window(self, now):
        end = max(self.expires_at, now + self.post_cleanup_grace)
        return replace(self, cleanup_end=end, force_at=min(now + 600, end - 300))

    def readiness_until(self, now):
        return min(now + 600, self.cleanup_at)

    def settle_until(self, now):
        return max(now + self.operator_grace, self.runner_wait_until)

    @property
    def serial_budget(self):
        return (
            self.runner_wait_until
            - self.started
            + self.post_cleanup_grace
            + self.state_cleanup_seconds
            + 2 * self.operator_grace
            + 2 * self.post_cleanup_grace
            + self.plan_budget_seconds
            + self.setup_and_receipt_seconds
        )


@dataclass(frozen=True)
class Approval:
    version: int
    run_id: str
    nonce: str
    sha: str
    started_at: str
    expires_at: str
    cleanup_at: str
    namespaces: dict
    images: dict
    operator_uid: str
    baseline_uids: list
    lock_owner: dict
    application_sha256: str
    actor: str = ""
    scenario: str = "smoke"
    upgrade_application_sha256: str = ""
    bigquery_trial: dict = field(default_factory=dict)
    pubsub_trial: dict = field(default_factory=dict)
    cloudtasks_trial: dict = field(default_factory=dict)
    bigtable_trial: dict = field(default_factory=dict)

    @classmethod
    def from_dict(cls, value):
        """Read an approval as written, without checking it against policy."""
        try:
            kept = {k: v for k, v in value.items() if k not in RETIRED_APPROVAL_FIELDS}
            return cls(**copy.deepcopy(kept))
        except (TypeError, AttributeError) as error:
            raise Failure("Invalid run approval") from error

    @property
    def schedule(self):
        return Schedule.from_approval(self)

    @property
    def application_namespace(self):
        return APPLICATION_NAMESPACES.get(self.scenario, SMOKE)

    @property
    def bigquery_plan(self):
        """Derive the exact service plan from the trial inputs and expiry."""
        from .bigquery.plan import resource_plan

        if self.scenario != "bigquery-recovery":
            raise Failure("Approval does not describe a BigQuery trial")
        return resource_plan(
            self.run_id, self.nonce, self.expires_at, self.bigquery_trial
        )

    @property
    def pubsub_plan(self):
        """Derive service identities from a Pub/Sub approval."""
        from .pubsub.resources import ResourcePlan

        if self.scenario != "pubsub-recovery":
            raise Failure("Approval does not describe a Pub/Sub trial")
        return ResourcePlan(self.run_id, self.nonce)

    @property
    def cloudtasks_plan(self):
        """Derive the trial's input and queue from a Cloud Tasks approval."""
        from .cloudtasks.trial import Trial

        if self.scenario != "cloudtasks-recovery":
            raise Failure("Approval does not describe a Cloud Tasks trial")
        return Trial(self.run_id, **self.cloudtasks_trial)

    @property
    def bigtable_plan(self):
        """Derive the trial's input and instance from a Bigtable approval."""
        from .bigtable.trial import Trial

        if self.scenario != "bigtable-recovery":
            raise Failure("Approval does not describe a Bigtable trial")
        return Trial(self.run_id, **self.bigtable_trial)

    def to_dict(self):
        value = asdict(self)
        if self.scenario != "pubsub-recovery":
            value.pop("pubsub_trial")
        if self.scenario != "bigquery-recovery":
            value.pop("bigquery_trial")
        if self.scenario != "cloudtasks-recovery":
            value.pop("cloudtasks_trial")
        if self.scenario != "bigtable-recovery":
            value.pop("bigtable_trial")
        return value


class Phase(str, Enum):
    APPROVED = "approved"
    READY = "ready"
    RUNNING = "running"
    CLEANING = "cleaning"
    CLEANED = "cleaned"


TRANSITIONS = {
    Phase.APPROVED: {Phase.READY, Phase.CLEANING},
    Phase.READY: {Phase.RUNNING, Phase.CLEANING},
    Phase.RUNNING: {Phase.CLEANING},
    Phase.CLEANING: {Phase.CLEANED},
    Phase.CLEANED: set(),
}


# Fields removed from the control record. The Cloud Tasks session (#1723) added
# the first seven to every record, empty for any other scenario. A record written
# before a removal still carries them, and recovery of a run that spans the
# change must be able to read it.
RETIRED_RECORD_FIELDS = frozenset(
    {
        "queue_intent",
        "queue",
        "cells",
        "cell_intent",
        "operations",
        "exports",
        "evidence_bytes",
        # The supervisor claim a replacement Pod could take over (#1725).
        "supervisor_pod",
        "supervisor_claimed_at",
        "supervision_started",
    }
)

# Fields an approval written before #1725 carries and nothing reads now.
RETIRED_APPROVAL_FIELDS = frozenset(
    {"ceilings", "runtime_sha256", "delivery_sha256", "recovery_policy"}
)


@dataclass
class RunRecord:
    nonce: str
    phase: Phase = Phase.APPROVED
    roots: dict = field(default_factory=dict)
    observed: dict = field(default_factory=dict)
    stop_requested: bool = False
    application_intent: bool = False
    config_intent: dict | None = None
    supervisor_intent: dict | None = None
    # The workload access probe Pod a Pub/Sub admission creates.
    probe_intent: dict | None = None
    heartbeat: str | None = None
    lineage: str | None = None
    checkpoint_observed: bool = False
    success: bool = False
    state_clean: bool = False
    evidence_failed: bool = False
    # The first evidence failure's operation and cause, when one occurred.
    evidence_failure: dict | None = None
    idle: bool = False
    reason: str = ""
    final_log_attempted: bool = False
    recovery: dict | None = None
    bigquery: dict | None = None
    pubsub: dict | None = None
    cloudtasks: dict | None = None
    bigtable: dict | None = None

    @classmethod
    def from_dict(cls, value):
        try:
            kept = {k: v for k, v in value.items() if k not in RETIRED_RECORD_FIELDS}
            return cls(**{**kept, "phase": Phase(value["phase"])})
        except (TypeError, KeyError, ValueError, AttributeError) as error:
            raise Failure("Invalid run control record") from error

    def to_dict(self):
        return {**asdict(self), "phase": self.phase.value}

    def fail_evidence(self, failure):
        """Set the evidence flag with its cause; the first recorded cause stays."""
        if failure is None:
            return
        self.evidence_failed = True
        if self.evidence_failure is None:
            self.evidence_failure = copy.deepcopy(failure)

    def set_phase(self, phase):
        if phase != self.phase and phase not in TRANSITIONS[self.phase]:
            raise Failure(
                f"Invalid run phase transition: {self.phase.value} to {phase.value}"
            )
        self.phase = phase
