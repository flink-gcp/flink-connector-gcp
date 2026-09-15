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
from dataclasses import asdict, dataclass, field, replace
from decimal import Decimal
from enum import Enum
from typing import ClassVar

from .common import Failure, quantity, timestamp
from .policy import (
    CEILINGS,
    COST_RATES,
    GAR,
    POD_RESOURCES,
    PRICING_REVIEWED,
    RUN_ID,
    SHA,
    SMOKE,
    SYSTEM,
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


def validate_approval(approval, now=None):
    required = {
        "version",
        "run_id",
        "nonce",
        "sha",
        "started_at",
        "expires_at",
        "cleanup_at",
        "ceilings",
        "namespaces",
        "images",
        "operator_uid",
        "baseline_uids",
        "lock_owner",
        "runtime_sha256",
        "application_sha256",
    }
    if not isinstance(approval, dict) or not required <= approval.keys():
        raise Failure("Incomplete run approval")
    if (
        approval["lock_owner"].get("nonce") != approval["nonce"]
        or approval["lock_owner"].get("sha") != approval["sha"]
        or approval["lock_owner"].get("run_id") != approval["run_id"]
    ):
        raise Failure("Approval and lock identity differ")
    if approval["operator_uid"] not in approval["baseline_uids"]:
        raise Failure("Operator must belong to the observed foundation")
    if approval.get("version") != 1 or not RUN_ID.fullmatch(approval.get("run_id", "")):
        raise Failure("Invalid run approval identity")
    if not SHA.fullmatch(approval.get("sha", "")) or not re.fullmatch(
        r"[0-9a-f]{32}", approval.get("nonce", "")
    ):
        raise Failure("Approval must pin a revision and nonce")
    if approval.get("ceilings") != CEILINGS:
        raise Failure("Approval does not match the fixed resource/cost ceilings")
    start, end = timestamp(approval["started_at"]), timestamp(approval["expires_at"])
    if not 900 < end - start <= 3600 or timestamp(approval["cleanup_at"]) != end - 900:
        raise Failure(
            "Approval must reserve 15 minutes of the one-hour window for cleanup"
        )
    if estimated_cost(end - start) > Decimal(CEILINGS["additional_cost_usd"]):
        raise Failure("Estimated incremental cost exceeds approval")
    if now is not None and now > timestamp(PRICING_REVIEWED) + 30 * 86400:
        raise Failure(
            "Pricing review is older than 30 days; refresh rates before admission"
        )
    if now is not None and (
        now < start - 30 or now >= timestamp(approval["cleanup_at"])
    ):
        raise Failure("Approval is outside its admission window")
    if set(approval["namespaces"]) != {SMOKE, SYSTEM}:
        raise Failure("Approval must name both Tier-3 namespaces")
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
    for key, package in (
        ("smoke", "smoke"),
        ("operator", "operator"),
        ("supervisor", "lifecycle-tools"),
    ):
        if not re.fullmatch(
            re.escape(GAR + package) + r"@sha256:[0-9a-f]{64}", approval["images"][key]
        ):
            raise Failure("Every runtime image must be an approved GAR digest")
    if not re.fullmatch(
        r"[0-9a-f]{64}", approval.get("runtime_sha256", "")
    ) or not re.fullmatch(r"[0-9a-f]{64}", approval.get("application_sha256", "")):
        raise Failure("Approval must pin its runtime and application bytes")


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
        return cls(started, end - 900, end, end - 180, end, end - 300, end - 180)

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
    ceilings: dict
    namespaces: dict
    images: dict
    operator_uid: str
    baseline_uids: list
    lock_owner: dict
    runtime_sha256: str
    application_sha256: str
    actor: str = ""

    @classmethod
    def from_dict(cls, value, now=None):
        validate_approval(value, now)
        return cls(**copy.deepcopy(value))

    @property
    def schedule(self):
        return Schedule.from_approval(self)

    def to_dict(self):
        return asdict(self)


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
    heartbeat: str | None = None
    lineage: str | None = None
    checkpoint_observed: bool = False
    success: bool = False
    state_clean: bool = False
    evidence_failed: bool = False
    idle: bool = False
    reason: str = ""
    final_log_attempted: bool = False

    @classmethod
    def from_dict(cls, value):
        try:
            return cls(**{**value, "phase": Phase(value["phase"])})
        except (TypeError, KeyError, ValueError) as error:
            raise Failure("Invalid run control record") from error

    def to_dict(self):
        return {**asdict(self), "phase": self.phase.value}

    def set_phase(self, phase):
        if phase != self.phase and phase not in TRANSITIONS[self.phase]:
            raise Failure(
                f"Invalid run phase transition: {self.phase.value} to {phase.value}"
            )
        self.phase = phase
