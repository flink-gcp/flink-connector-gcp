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
"""The preregistered rule's arithmetic: rows, repetitions, comparisons and groups."""

from fractions import Fraction
from types import SimpleNamespace

import pytest
from flink_tier3.cloudtasks import groups, protocol

from ..test_lifecycle import Store
from .test_evidence import CELL_A, INC, PROC, RUN, WALL, StoreSource, Synthetic


def repetition(index, throughput, p95=100, offered_rate=10):
    return {
        "cell_id": f"c{index}",
        "status": "usable",
        "throughput": Fraction(throughput),
        "offered_rate": offered_rate,
        "repetition": index,
        "repeat": False,
        "rows": {"p95_nanos": p95},
    }


def arm(*throughputs, p95=100):
    return groups.arm_statistics(
        [repetition(i, t, p95) for i, t in enumerate(throughputs, start=1)]
    )


def test_range_exactly_ten_percent_is_usable_and_above_is_inconclusive():
    base = arm(95, 100, 105)
    assert base["range_over_mean"] == Fraction(1, 10)
    assert groups.compare(base, arm(100, 100, 100))["label"] == "general"
    wide = arm(Fraction("94.95"), 100, Fraction("105.05"))
    assert wide["range_over_mean"] == Fraction(101, 1000)
    result = groups.compare(wide, arm(100, 100, 100))
    assert result["label"] == "inconclusive" and result["reasons"] == ["range-baseline"]
    result = groups.compare(arm(100, 100, 100), wide)
    assert result["label"] == "inconclusive" and result["reasons"] == [
        "range-candidate"
    ]


@pytest.mark.parametrize(
    ("throughput", "p95", "label"),
    [
        (70, 200, "general"),
        (Fraction("69.999"), 200, "constrained"),
        (70, 201, "constrained"),
        (25, 400, "constrained"),
        (Fraction("24.999"), 400, "decline"),
        (25, 401, "decline"),
    ],
)
def test_ratio_thresholds_pass_exactly(throughput, p95, label):
    base = arm(100, 100, 100)
    result = groups.compare(base, arm(throughput, throughput, throughput, p95=p95))
    assert result["label"] == label
    assert result["throughput_ratio"] == Fraction(throughput, 100)
    assert result["p95_ratio"] == Fraction(p95, 100)


def test_fewer_than_three_usable_repetitions_is_inconclusive():
    arms = {name: arm(100, 100, 100) for name in groups.ARMS}
    arms["NAMED_HASH"] = arm(100, 100)
    verdict = groups.group_verdict(arms)
    assert verdict["short_arms"] == ["NAMED_HASH"]
    assert verdict["verdict"] == "inconclusive"
    assert verdict["comparisons"]["STAGED_HASH"]["reasons"] == ["arm-short:NAMED_HASH"]
    # The incremental cost is still reported from what is there.
    assert verdict["incremental_cost"]["STAGED_HASH"]["baseline_usable"] == 2
    assert verdict["incremental_cost"]["STAGED_HASH"]["throughput_ratio"] == 1


def test_p95_is_the_mean_of_repetitions_never_pooled():
    statistics = groups.arm_statistics(
        [repetition(1, 100, 100), repetition(2, 100, 400), repetition(3, 100, 100)]
    )
    assert statistics["mean_p95_nanos"] == Fraction(200)


def test_a_repeat_replaces_only_a_failed_repetition():
    original = repetition(1, 100)
    repeat = {**repetition(1, 50), "cell_id": "c1-x2", "repeat": True}
    assert groups.arm_statistics([original, repeat])["cells"] == ["c1"]
    failed = {**original, "status": "inconclusive"}
    assert groups.arm_statistics([failed, repeat])["cells"] == ["c1-x2"]


def test_only_the_contributing_repetitions_set_the_offered_rates():
    """An earlier run of the same cell that did not count offers no rate."""
    usable = repetition(1, 100, offered_rate=10)
    unexported = {**repetition(1, 100, offered_rate=20), "status": "unexported"}
    assert groups.arm_statistics([unexported, usable])["offered_rates"] == [10]


# --- rows -----------------------------------------------------------------------


NAMED = {**CELL_A, "id": "named-dedup", "arm": "NAMED_HASH"}


def rows_of(cell, lines, span=None):
    """``analyze_rows`` over one part of ``lines`` for ``cell``."""
    synthetic = Synthetic(cell)
    synthetic.part(INC, 1, lines)
    store = Store()
    synthetic.install(store)
    run = SimpleNamespace(source=StoreSource(store), run_id=RUN)
    return groups.analyze_rows(run, cell, [INC], span)


def window(first, last):
    """The span whose edges are the origin instants of two sequences."""
    return {
        "start": Fraction(WALL + first * 100, 1000),
        "end": Fraction(WALL + last * 100, 1000),
    }


def test_a_deduplicated_create_counts_once_for_a_named_arm_only():
    named = Synthetic(NAMED)
    named_rows = rows_of(NAMED, [named.row(0, status="ALREADY_EXISTS"), named.row(1)])
    assert named_rows["distinct_ok"] == 2
    unnamed = Synthetic(CELL_A)
    unnamed_rows = rows_of(
        CELL_A, [unnamed.row(0, status="ALREADY_EXISTS"), unnamed.row(1)]
    )
    # The baseline has no answer that explains an ALREADY_EXISTS, so it does
    # not count as a created record.
    assert unnamed_rows["distinct_ok"] == 1


def test_rows_of_another_run_cell_or_arm_are_excluded():
    synthetic = Synthetic(CELL_A)
    result = rows_of(
        CELL_A,
        [
            synthetic.row(0),
            synthetic.row(1, cell=NAMED),
            synthetic.row(2, run_id="other-run"),
        ],
        window(0, 2),
    )
    assert result["rows"] == 3 and result["foreign_rows"] == 2
    assert result["distinct_ok"] == 1 and result["p95_sample"] == 1


def test_the_window_includes_the_rows_on_its_edges():
    synthetic = Synthetic(CELL_A)
    result = rows_of(CELL_A, [synthetic.row(i) for i in range(8)], window(2, 5))
    assert result["distinct_ok"] == 8
    assert result["distinct_ok_in_window"] == 4
    assert result["p95_sample"] == 4


def test_a_latency_measured_across_processes_is_excluded_from_the_sample():
    synthetic = Synthetic(CELL_A)
    result = rows_of(
        CELL_A,
        [synthetic.row(0), synthetic.row(1, origin_process=PROC[:-1] + "9")],
        window(0, 1),
    )
    assert result["latency_excluded"] == 1 and result["p95_sample"] == 1


def test_p95_is_the_nearest_rank_value():
    """The ceil(0.95 n)-th smallest: the 19th of twenty, not the twentieth."""
    synthetic = Synthetic(CELL_A)
    lines = [synthetic.row(i, completed_offset=(i + 1) * 1_000_000) for i in range(20)]
    result = rows_of(CELL_A, lines, window(0, 19))
    assert result["p95_sample"] == 20
    assert result["p95_nanos"] == 19_000_000


# --- groups -----------------------------------------------------------------------


def executed(pinned, **changes):
    """The record ``vmanalyze`` builds for a usable run of ``pinned``."""
    cell = protocol.session_cell(pinned, 10)
    return {
        "cell_id": pinned["cell_id"],
        "protocol_cell_id": pinned["cell_id"],
        "campaign": "vm",
        "status": "usable",
        "reasons": [],
        "throughput": Fraction(10),
        "offered_rate": 10,
        "repeat": False,
        "rows": {"p95_nanos": 100},
        "flink": pinned["flink"],
        "body_bytes_executed": cell["body_bytes"],
        **{
            name: cell[name]
            for name in (
                "arm",
                "parallelism",
                "concurrency",
                "checkpoint_seconds",
                "distribution",
                "channel_pool_size",
                "warmup_seconds",
                "observation_seconds",
            )
        },
        **changes,
    }


PINNED = protocol.load()["cells"][0]


def test_a_run_under_the_pinned_conditions_joins_its_group():
    record = executed(PINNED)
    [group], unmatched = groups.build_groups(
        [record], groups.protocol_index(protocol.load())
    )
    assert unmatched == [] and record["status"] == "usable" and not record["reasons"]
    assert group["cells"] == [PINNED["cell_id"]]
    assert (group["flink"], group["shape"], group["body_bytes"]) == (
        PINNED["flink"],
        PINNED["shape"],
        PINNED["body_bytes"],
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("arm", "UNNAMED"),
        ("flink", "2.2.1"),
        ("body_bytes_executed", 65536),
        ("parallelism", 4),
        ("concurrency", 4),
        ("checkpoint_seconds", 10),
        ("distribution", "skew"),
        ("channel_pool_size", 4),
        ("warmup_seconds", 30),
        ("observation_seconds", 90),
    ],
)
def test_every_pinned_condition_is_compared_with_what_ran(field, value):
    assert executed(PINNED)[field] != value
    record = executed(PINNED, **{field: value})
    groups.build_groups([record], groups.protocol_index(protocol.load()))
    named = "body_bytes" if field == "body_bytes_executed" else field
    assert record["status"] == "inconclusive"
    assert record["reasons"] == ["conditions-differ-from-protocol:" + named]
