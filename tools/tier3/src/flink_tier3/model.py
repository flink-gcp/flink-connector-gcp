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
import math
import re
from dataclasses import asdict, dataclass, field, replace
from decimal import Decimal
from enum import Enum
from fractions import Fraction
from typing import ClassVar

from .common import Failure, quantity, timestamp
from .policy import (
    CEILINGS,
    CELL_ID,
    CLOUDTASKS,
    CLOUDTASKS_CEILINGS,
    CLOUDTASKS_POD_RESOURCES,
    CLOUDTASKS_POLICY,
    COST_RATES,
    FLINK_LINES,
    GAR,
    POD_RESOURCES,
    PRICING_REVIEWED,
    PROJECT,
    RECOVERY,
    REGION,
    RUN_ID,
    SHA,
    SMOKE,
    SYSTEM,
)

ARMS = (
    "UNNAMED",
    "NAMED_HASH",
    "NAMED_RANDOM_CONTROL",
    "STAGED_HASH",
    "STAGED_RANDOM",
)
CELL_FIELDS = (
    "id",
    "arm",
    "body_bytes",
    "parallelism",
    "concurrency",
    "checkpoint_seconds",
    "channel_pool_size",
    "distribution",
    "offered_rate",
    "warmup_seconds",
    "observation_seconds",
    "record_limit",
    "attempt_limit",
    "control_delay_millis",
    "emit_attempts",
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


def taskmanager_class(parallelism):
    return "p" + str(parallelism)


def largest_parallelism(cells):
    return max((int(cell["parallelism"]) for cell in cells), default=1)


def session_shapes(cells, shapes=CLOUDTASKS_POD_RESOURCES):
    """JobManager and TaskManager shapes a session's largest cell needs."""
    return (
        shapes["jobmanager"],
        shapes["taskmanager"][taskmanager_class(largest_parallelism(cells))],
    )


def estimated_session_cost(seconds, cells):
    """Compute, task-operation and reserve cost of one Cloud Tasks session."""
    shapes = [
        POD_RESOURCES["supervisor"],
        POD_RESOURCES["operator"],
        *session_shapes(cells),
    ]
    compute = _hourly(shapes) * Decimal(str(seconds)) / 3600
    creations = sum(cell_creations(cell) for cell in cells)
    operations = (
        Decimal(creations)
        * Decimal(CLOUDTASKS_POLICY["task_operation_usd_per_million"])
        / 10**6
    )
    return compute + operations + Decimal("0.25")


def cell_creations(cell):
    """Planning bound on task creations: every subtask exhausts its attempt
    allowance in every incarnation one JobMaster's restart strategy permits.

    A JobManager failover resets that restart budget, so the cell deadline and
    the paused queue, not this number, cap what a broken cell can spend.
    """
    return (
        int(cell["attempt_limit"])
        * int(cell["parallelism"])
        * (1 + CLOUDTASKS_POLICY["restart_attempts"])
    )


def creator_share(cell, records):
    """Records the busiest creator maps: all of them at parallelism 1, nine
    tenths under the skewed distribution, an even share otherwise."""
    if int(cell["parallelism"]) == 1:
        return records
    if cell["distribution"] == "skew":
        return math.ceil(Fraction(records) * Fraction(9, 10))
    return math.ceil(Fraction(records, int(cell["parallelism"])))


def cell_records(cell):
    return math.ceil(
        int(cell["offered_rate"])
        * (
            int(cell["warmup_seconds"])
            + int(cell["observation_seconds"])
            + 2 * int(cell["checkpoint_seconds"])
            + 1
        )
    )


def cell_nominal_seconds(cell):
    return (
        int(cell["warmup_seconds"])
        + int(cell["observation_seconds"])
        + 2 * int(cell["checkpoint_seconds"])
        + 1
    )


def cell_budget_seconds(cell):
    return (
        CLOUDTASKS_POLICY["cell_startup_seconds"]
        + cell_nominal_seconds(cell)
        + CLOUDTASKS_POLICY["cell_teardown_seconds"]
    )


def session_plan(cells):
    """Serial seconds the approved cells need at their nominal pace."""
    return sum(cell_budget_seconds(cell) for cell in cells)


def validate_cell(cell, manifest=True):
    """Check one cell against the measurement application's input domains."""
    expected = set(CELL_FIELDS) | ({"manifest_sha256"} if manifest else set())
    if not isinstance(cell, dict) or set(cell) != expected:
        raise Failure("Cell fields differ from the measurement input contract")
    if not isinstance(cell["id"], str) or not CELL_ID.fullmatch(cell["id"]):
        raise Failure("Invalid cell ID")
    for key in CELL_FIELDS:
        if key in ("id", "arm", "distribution", "emit_attempts"):
            continue
        if isinstance(cell[key], bool) or not isinstance(cell[key], int):
            raise Failure(f"Cell {key} must be an integer")
    domains = {
        "arm": ARMS,
        "body_bytes": (1024, 65536),
        "parallelism": (1, 4, 16),
        "concurrency": (1, 4, 16),
        "checkpoint_seconds": (1, 10, 60),
        "channel_pool_size": (1, 4, 8),
        "distribution": ("even", "skew"),
        "control_delay_millis": (0, 100),
        "emit_attempts": (True, False),
    }
    for key, allowed in domains.items():
        if cell[key] not in allowed or type(cell[key]) is not type(allowed[0]):
            raise Failure(f"Cell {key} is outside its enumerated domain")
    if not 1 <= cell["offered_rate"] <= 10000:
        raise Failure("Cell offered_rate must be 1..10000")
    if not 1 <= cell["warmup_seconds"] <= 120:
        raise Failure("Cell warmup_seconds must be 1..120")
    if not 1 <= cell["observation_seconds"] <= 600:
        raise Failure("Cell observation_seconds must be 1..600")
    records = cell_records(cell)
    if not records <= cell["record_limit"] <= 10000000:
        raise Failure("Cell record_limit must cover the window and stay <= 10000000")
    share = creator_share(cell, records)
    if not share <= cell["attempt_limit"] <= 3 * records:
        raise Failure(
            "Cell attempt_limit must cover one creator's record share and stay <= 3 * records"
        )
    if cell["control_delay_millis"] and (
        cell["arm"] != "UNNAMED"
        or cell["parallelism"] != 1
        or cell["concurrency"] != 1
        or not cell["emit_attempts"]
    ):
        raise Failure(
            "Delay control requires UNNAMED, parallelism/concurrency 1 and CSV"
        )
    if manifest and not re.fullmatch(r"[0-9a-f]{64}", str(cell["manifest_sha256"])):
        raise Failure("Cell must pin its rendered manifest")


def validate_cells(cells, manifest=True):
    """Validate a session's cell list and return its derived expectations."""
    if not isinstance(cells, list) or not cells:
        raise Failure("A session needs at least one cell")
    if len(cells) > CLOUDTASKS_CEILINGS["cells"]:
        raise Failure("Session exceeds the cell ceiling")
    for cell in cells:
        validate_cell(cell, manifest)
    ids = [cell["id"] for cell in cells]
    if len(set(ids)) != len(ids):
        raise Failure("Session cell IDs must be unique")
    creations = sum(cell_creations(cell) for cell in cells)
    if creations > CLOUDTASKS_CEILINGS["task_creations"]:
        raise Failure("Session exceeds the task creation ceiling")
    return {
        "cells": len(cells),
        "task_creations": creations,
        "records": sum(cell_records(cell) for cell in cells),
        "plan_seconds": session_plan(cells),
        "largest_parallelism": largest_parallelism(cells),
    }


def queue_name(run_id):
    return (
        f"projects/{PROJECT}/locations/{REGION}/queues/"
        + CLOUDTASKS_POLICY["queue_prefix"]
        + run_id
    )


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
    scenario = approval.get("scenario", "smoke")
    if scenario not in ("smoke", "generic-recovery", "cloudtasks"):
        raise Failure("Unknown approved scenario")
    recovery = scenario == "generic-recovery"
    cloudtasks = scenario == "cloudtasks"
    versions = {"smoke": 1, "generic-recovery": 2, "cloudtasks": 3}
    if approval.get("version") != versions[scenario] or not RUN_ID.fullmatch(
        approval.get("run_id", "")
    ):
        raise Failure("Invalid run approval identity")
    if recovery:
        if approval.get("recovery_policy") != RECOVERY or not re.fullmatch(
            r"[0-9a-f]{64}", approval.get("upgrade_application_sha256", "")
        ):
            raise Failure("Recovery approval must pin its policy and upgrade manifest")
    elif approval.get("recovery_policy") or approval.get("upgrade_application_sha256"):
        raise Failure("Ordinary smoke approval cannot authorize recovery operations")
    if not SHA.fullmatch(approval.get("sha", "")) or not re.fullmatch(
        r"[0-9a-f]{32}", approval.get("nonce", "")
    ):
        raise Failure("Approval must pin a revision and nonce")
    if approval.get("ceilings") != CEILINGS:
        raise Failure("Approval does not match the fixed resource/cost ceilings")
    start, end = timestamp(approval["started_at"]), timestamp(approval["expires_at"])
    if cloudtasks:
        _validate_session(approval, start, end)
    else:
        for key in (
            "campaign",
            "flink_version",
            "queue",
            "target",
            "cells",
            "cloudtasks_ceilings",
            "cloudtasks_pod_resources",
        ):
            if approval.get(key):
                raise Failure("Smoke approvals cannot carry Cloud Tasks session fields")
        if (
            not 900 < end - start <= 3600
            or timestamp(approval["cleanup_at"]) != end - 900
        ):
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
    application = CLOUDTASKS if cloudtasks else SMOKE
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
    if cloudtasks:
        roles["application"] = FLINK_LINES[approval["flink_version"]][0]
    else:
        roles["smoke"] = "smoke"
    if set(approval["images"]) != set(roles):
        raise Failure("Approval must name exactly the runtime image roles")
    for key, package in roles.items():
        if not re.fullmatch(
            re.escape(GAR + package) + r"@sha256:[0-9a-f]{64}", approval["images"][key]
        ):
            raise Failure("Every runtime image must be an approved GAR digest")
    if not re.fullmatch(
        r"[0-9a-f]{64}", approval.get("runtime_sha256", "")
    ) or not re.fullmatch(r"[0-9a-f]{64}", approval.get("application_sha256", "")):
        raise Failure("Approval must pin its runtime and application bytes")


def _validate_session(approval, start, end):
    for key in ("campaign", "flink_version", "queue", "target", "cells"):
        if key not in approval:
            raise Failure("Incomplete Cloud Tasks session approval")
    if approval.get("recovery_policy") or approval.get("upgrade_application_sha256"):
        raise Failure("A Cloud Tasks session cannot authorize recovery operations")
    if not RUN_ID.fullmatch(str(approval["campaign"])):
        raise Failure("Invalid campaign identity")
    if approval["flink_version"] not in FLINK_LINES:
        raise Failure("Unsupported Flink line for the measurement application")
    if approval["queue"] != queue_name(approval["run_id"]):
        raise Failure("Approved queue must be the run's own ct1246 queue")
    if approval["target"] != CLOUDTASKS_POLICY["target"]:
        raise Failure("Approved target differs from the reviewed policy")
    if (
        approval.get("cloudtasks_ceilings") != CLOUDTASKS_CEILINGS
        or approval.get("cloudtasks_pod_resources") != CLOUDTASKS_POD_RESOURCES
    ):
        raise Failure("Approval does not match the fixed session ceilings and shapes")
    plan = validate_cells(approval["cells"])
    ceilings = CLOUDTASKS_CEILINGS
    cleanup = ceilings["cleanup_seconds"]
    window = end - start
    if (
        window > ceilings["session_seconds"]
        or window < plan["plan_seconds"] + cleanup
        or window > plan["plan_seconds"] + cleanup + 600
        or timestamp(approval["cleanup_at"]) != end - cleanup
    ):
        raise Failure(
            "Session window must cover the cell plan plus cleanup without over-reservation"
        )
    if estimated_session_cost(window, approval["cells"]) > Decimal(
        ceilings["additional_cost_usd"]
    ):
        raise Failure("Estimated session cost exceeds approval")


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

    def cell_deadline(self, now, cell):
        """Latest moment a cell may still run before the session must tear down.

        A slower arm consumes its finite input more slowly than the offered
        rate; four nominal windows cover the constrained-support ratio.
        """
        return min(
            now
            + CLOUDTASKS_POLICY["cell_startup_seconds"]
            + 4 * cell_nominal_seconds(cell)
            + CLOUDTASKS_POLICY["cell_teardown_seconds"],
            self.cleanup_at,
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
    scenario: str = "smoke"
    recovery_policy: dict = field(default_factory=dict)
    upgrade_application_sha256: str = ""
    campaign: str = ""
    flink_version: str = ""
    queue: str = ""
    target: str = ""
    cells: list = field(default_factory=list)
    cloudtasks_ceilings: dict = field(default_factory=dict)
    cloudtasks_pod_resources: dict = field(default_factory=dict)

    @classmethod
    def from_dict(cls, value, now=None):
        validate_approval(value, now)
        return cls(**copy.deepcopy(value))

    @property
    def schedule(self):
        return Schedule.from_approval(self)

    @property
    def application_namespace(self):
        return CLOUDTASKS if self.scenario == "cloudtasks" else SMOKE

    @property
    def cell_ids(self):
        return [cell["id"] for cell in self.cells]

    def cell(self, cell_id):
        for cell in self.cells:
            if cell["id"] == cell_id:
                return cell
        raise Failure("Cell is not part of this approval")

    def to_dict(self):
        value = asdict(self)
        if self.scenario != "cloudtasks":
            for key in (
                "campaign",
                "flink_version",
                "queue",
                "target",
                "cells",
                "cloudtasks_ceilings",
                "cloudtasks_pod_resources",
            ):
                value.pop(key)
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
    recovery: dict | None = None
    queue_intent: bool = False
    queue: dict | None = None
    cells: dict = field(default_factory=dict)
    cell_intent: dict | None = None
    operations: dict = field(default_factory=dict)
    exports: dict = field(default_factory=dict)
    evidence_bytes: int = 0
    bigquery: dict | None = None

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
