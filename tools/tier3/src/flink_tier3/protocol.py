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
"""The preregistered #1246 measurement protocol and its derived quantities."""

from __future__ import annotations

import hashlib
import math
import tomllib
from fractions import Fraction
from importlib.resources import files

from .common import Failure
from .model import ARMS, cell_records, creator_share
from .policy import CELL_ID

PROTOCOL_FILE = "protocol_1246.toml"
# The private draft this file was converted from, field for field.
DRAFT_SHA256 = "32122ff01527f4e17a1eac7377c0f11430d71fabba2bce65b4c011afb5b07354"
SHAPES = {
    "s1": (1, 1, 1, "even"),
    "s2": (4, 4, 10, "even"),
    "s3": (16, 16, 60, "even"),
    "s4": (1, 16, 60, "even"),
    "s5": (16, 1, 1, "even"),
    "s6": (4, 4, 10, "skew"),
    "s7": (16, 16, 60, "skew"),
}
LINES = ("1.20.4", "2.2.1")
BODIES = (1024, 65536)
REPETITIONS = (1, 2, 3)
MAX_RECORDS = 10000000


def window(checkpoint_seconds):
    """Warm-up and observation seconds: max(60, 2cp) and max(180, 6cp)."""
    return max(60, 2 * checkpoint_seconds), max(180, 6 * checkpoint_seconds)


def protocol_sha256(path=None):
    path = files("flink_tier3").joinpath(PROTOCOL_FILE) if path is None else path
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load(path=None):
    """Load and validate the pinned protocol; every cell must satisfy the rules."""
    path = files("flink_tier3").joinpath(PROTOCOL_FILE) if path is None else path
    try:
        protocol = tomllib.loads(path.read_text())
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise Failure("Unreadable measurement protocol") from error
    if (
        protocol.get("status") != "PREPARATION_ONLY_NOT_EXECUTION_AUTHORIZATION"
        or protocol.get("version") != 1
        or protocol.get("order_seed") != 1246
    ):
        raise Failure("Measurement protocol identity differs from the preregistration")
    search = protocol.get("capacity_search", {})
    thresholds = protocol.get("support_thresholds", {})
    if (
        search.get("growth_factor") != 2
        or search.get("relative_bracket_width") != 0.1
        or search.get("admitted_rate_fraction") != 0.95
        or thresholds.get("general")
        != {"throughput_ratio_min": 0.7, "p95_ratio_max": 2}
        or thresholds.get("constrained")
        != {"throughput_ratio_min": 0.25, "p95_ratio_max": 4}
        or thresholds.get("throughput_range_over_mean_max") != 0.1
    ):
        raise Failure("Measurement protocol rules differ from the preregistration")
    cells = protocol.get("cells")
    if not isinstance(cells, list) or len(cells) != 420:
        raise Failure("Measurement protocol must hold the 420 preregistered cells")
    seen = set()
    for cell in cells:
        validate_protocol_cell(cell)
        if cell["cell_id"] in seen:
            raise Failure("Measurement protocol cell IDs must be unique")
        seen.add(cell["cell_id"])
    return protocol


def validate_protocol_cell(cell):
    expected = {
        "cell_id",
        "flink",
        "shape",
        "parallelism",
        "concurrency",
        "checkpoint_seconds",
        "distribution",
        "body_bytes",
        "repetition",
        "arm",
        "channel_pool_size",
        "warmup_seconds",
        "observation_seconds",
    }
    if not isinstance(cell, dict) or set(cell) != expected:
        raise Failure(
            "Measurement protocol cell fields differ from the preregistration"
        )
    if not isinstance(cell["cell_id"], str) or not CELL_ID.fullmatch(cell["cell_id"]):
        raise Failure("Invalid measurement protocol cell ID")
    shape = SHAPES.get(cell["shape"])
    if shape is None or shape != (
        cell["parallelism"],
        cell["concurrency"],
        cell["checkpoint_seconds"],
        cell["distribution"],
    ):
        raise Failure("Measurement protocol cell does not match its shape")
    if (
        cell["flink"] not in LINES
        or cell["body_bytes"] not in BODIES
        or cell["repetition"] not in REPETITIONS
        or cell["arm"] not in ARMS
        or cell["channel_pool_size"] != 1
    ):
        raise Failure("Measurement protocol cell is outside the preregistered domains")
    warm, observation = window(cell["checkpoint_seconds"])
    if cell["warmup_seconds"] != warm or cell["observation_seconds"] != observation:
        raise Failure("Measurement protocol cell window differs from the rule")


def session_cell(cell, offered_rate, suffix="", control_delay_millis=0, emit=True):
    """Instantiate a protocol cell as a session cell at a frozen offered rate."""
    if not isinstance(offered_rate, int) or not 1 <= offered_rate <= 10000:
        raise Failure("Offered rate must be an integer from 1 through 10000")
    result = {
        "id": derived_id(cell["cell_id"], suffix),
        "arm": cell["arm"],
        "body_bytes": cell["body_bytes"],
        "parallelism": cell["parallelism"],
        "concurrency": cell["concurrency"],
        "checkpoint_seconds": cell["checkpoint_seconds"],
        "channel_pool_size": cell["channel_pool_size"],
        "distribution": cell["distribution"],
        "offered_rate": offered_rate,
        "warmup_seconds": cell["warmup_seconds"],
        "observation_seconds": cell["observation_seconds"],
        "control_delay_millis": control_delay_millis,
        "emit_attempts": emit,
    }
    records = cell_records(result)
    if records > MAX_RECORDS:
        raise Failure(
            "Offered rate exceeds the application's record limit for this window"
        )
    result["record_limit"] = records
    result["attempt_limit"] = attempt_limit(result, records)
    return result


def attempt_limit(cell, records):
    """Per-creator attempts: 1.5x the busiest creator's records, at most 3x."""
    share = creator_share(cell, records)
    return min(3 * records, max(share, math.ceil(Fraction(share) * Fraction(3, 2))))


def derived_id(cell_id, suffix):
    """A probe or repeat ID that stays inside the application's label grammar."""
    derived = cell_id + suffix
    if not CELL_ID.fullmatch(derived):
        raise Failure("Derived cell ID is outside the label grammar")
    return derived


def next_probe(state):
    """Pure capacity-search step: double until rejected, then bisect to <=10%.

    ``state`` holds ``accepted`` (highest rate that met the rule, or None),
    ``rejected`` (lowest rate that failed, or None) and ``initial``.
    Returns ``(status, rate)`` with status ``searching``, ``bracketed`` or
    ``frozen``; ``rate`` is the next offered rate, or the frozen rate.
    """
    accepted, rejected = state.get("accepted"), state.get("rejected")
    if accepted is None and rejected is None:
        return "searching", int(state["initial"])
    if rejected is None:
        return "searching", min(10000, accepted * 2)
    if accepted is None:
        if rejected <= 1:
            return "frozen", 0
        return "searching", max(1, rejected // 2)
    if (
        Fraction(rejected - accepted, rejected) <= Fraction(1, 10)
        or rejected - accepted <= 1
    ):
        return "frozen", accepted
    return "bracketed", (accepted + rejected) // 2
