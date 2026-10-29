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
"""The Spanner exercise's late seed rows, its table reads and the verdict they decide."""

import dataclasses
from types import SimpleNamespace

import pytest
from flink_tier3.common import TransportError, timestamp, utc
from flink_tier3.exercise import RecoveryExercise
from flink_tier3.spanner import instance
from flink_tier3.spanner.exercise import ORACLE_STAGE, SpannerExercise
from flink_tier3.spanner.instance import Instance
from flink_tier3.spanner.trial import LATE_ROWS, Trial, row_key
from flink_tier3.spanner.verdict import OBSERVED, SAMPLED_STAGES

from ..cloudtasks.test_recovery_plan import inputs as inputs  # noqa: PLC0414
from ..cloudtasks.test_recovery_plan import renderer as renderer  # noqa: PLC0414
from ..cloudtasks.test_recovery_runtime import prepared as prepared  # noqa: PLC0414
from ..test_lifecycle import env as env  # noqa: PLC0414
from ..test_lifecycle import rt
from .test_tables import READ_AT, Database

# The JobManager's line for #1877's measured plan, in the enumerator's words.
PLAN_LINE = (
    "2026-10-28 14:43:09,123 INFO  io.github.flink.gcp.connector.spanner.source.batch"
    ".enumerator.SpannerBatchReadSplitEnumerator [] - Spanner planned 7 partition(s)"
    " for the read of table seed columns [k, n] of projects/flink-gcp/instances/"
    "sp1360-x/databases/recovery at read timestamp 2026-10-28T14:43:09.123456000Z"
    " and parallelism 2."
)


def tables(trial, late=True):
    """Every table as a usable run leaves it."""
    seed = {**trial.seed_keys(), **(trial.late_keys() if late else {})}
    return {
        "input": [(row_key(n), n) for n in range(trial.records)],
        "relay": [
            (row_key(n), f"initial/u/{n}", 1000 + n) for n in range(trial.records)
        ],
        "seed": sorted(seed.items()),
        "batch_out": [
            (row_key(n), f"initial/u/{n}", n) for n in range(trial.seed_rows)
        ],
    }


@pytest.fixture
def baseline(prepared, monkeypatch):
    """A Spanner exercise in its baseline, over a five-record input and six seed rows."""
    environment, _, upgrade, _ = prepared
    environment.actor = "supervisor"
    end = utc(environment.clock() + 3600)
    small = Trial(environment.approval.run_id, 5, 1, end, 6, 1)
    environment.approval = dataclasses.replace(
        environment.approval, scenario="spanner-recovery"
    )
    monkeypatch.setattr(
        type(environment.approval), "spanner_plan", property(lambda self: small)
    )
    database = Database(tables(small, late=False))
    controller = SimpleNamespace(
        env=environment,
        trial=small,
        listing=lambda until: Instance(database, small, until, environment.clock),
    )
    environment.records.set_phase(rt.Phase.READY)
    environment.records.set_phase(rt.Phase.RUNNING)
    value = SpannerExercise(environment, upgrade, controller)
    value.persist("baseline")
    return value, database


@pytest.fixture
def exercise(baseline):
    """The exercise once both recoveries are proved and the input finished."""
    value, database = baseline
    environment = value.env
    database.tables = {name: sorted(rows) for name, rows in tables(value.trial).items()}
    value.outcomes.update(
        upgrade={},
        failover={},
        late_seed={"commit_timestamp": utc(environment.clock()), "rows": LATE_ROWS},
    )
    value.coverage = {
        window: {
            "attempts": 1,
            "observed": sorted(OBSERVED),
            "maxima": {"watermark": index},
        }
        for index, window in enumerate(SAMPLED_STAGES)
    }
    # Progress counts only after the exercise's boundary, the window's start.
    environment.sleep(10)
    value.samples[("tm", 0)] = {
        "at": environment.clock(),
        "phase": "upgrade",
        "processed": 5,
        "lineage": "x",
        "sequence": 4,
        "pod_uid": "tm",
    }
    value.persist("complete")
    assert value.stage == ORACLE_STAGE
    return value, database


def test_every_table_is_read_to_its_last_page_at_one_snapshot_and_judged(
    exercise, monkeypatch
):
    monkeypatch.setattr(instance, "PAGE_ROWS", 2)
    value, database = exercise
    value.verify_tables()
    record = value.env.refresh().recovery
    assert (record["stage"], record["verdict"], record["reasons"]) == (
        "complete",
        "usable",
        [],
    )
    oracles = record["outcomes"]["oracles"]
    assert {c: oracles[c]["matched"] for c in ("sink", "change_stream", "batch")} == {
        "sink": 5,
        "change_stream": 5,
        "batch": 6,
    }
    assert oracles["batch"]["seed"]["matched"] == value.trial.seed_rows + LATE_ROWS
    # Pages of two continue after the last key read, until a short page.
    read = [query[2] for query in database.queries if "FROM batch_out" in query[0]]
    assert [params and params["k"] for params in read] == [
        None,
        "1/0000001",
        "3/0000003",
        "5/0000005",
    ]
    bounds = [bound for _, bound, _ in database.queries]
    assert bounds[0] == {"strong": True, "returnReadTimestamp": True}
    assert set(map(str, bounds[1:])) == {str({"readTimestamp": READ_AT})}
    assert oracles["read_timestamp"] == READ_AT


def test_a_transient_query_failure_is_repeated(exercise):
    value, database = exercise
    database.errors = [TransportError("timeout")]
    value.verify_tables()
    assert value.env.refresh().recovery["verdict"] == "usable"


def test_an_input_finished_too_close_to_the_change_streams_end_is_unjudged(exercise):
    value, _ = exercise
    end = timestamp(value.trial.change_stream_end)
    for sample in value.samples.values():
        sample["at"] = end - 60
    value.env.clock = lambda: end
    value.verify_tables()
    record = value.env.refresh().recovery
    assert record["reasons"] == ["change_stream-oracle-unjudged"]
    assert record["outcomes"]["oracles"]["change_stream"]["verdict"] == "pass"


def test_late_rows_wait_for_a_copied_row_and_the_upgrade_for_their_record(
    baseline, monkeypatch
):
    """Rows written before the plan would be copied; an upgrade before the
    rows leaves the batch oracle nothing to judge."""
    value, database = baseline
    upgrades = []
    monkeypatch.setattr(
        RecoveryExercise, "begin_upgrade", lambda self, *args: upgrades.append(args)
    )
    copies, database.tables["batch_out"] = database.tables["batch_out"], []
    value.begin_upgrade({}, {}, [], [])
    assert (database.commits, upgrades) == ([], [])
    database.tables["batch_out"] = copies
    database.errors = [TransportError("timeout")]
    value.begin_upgrade({}, {}, [], [])
    assert upgrades == [] and "late_seed" not in value.outcomes
    # An aborted commit, answered 409, is repeated at the next poll.
    database.aborts = 1
    value.begin_upgrade({}, {}, [], [])
    assert upgrades == [] and "late_seed" not in value.outcomes
    assert database.aborts == 0
    value.begin_upgrade({}, {}, [], [])
    [write] = database.commits
    assert dict(write["values"]) == {
        key: str(n) for key, n in value.trial.late_keys().items()
    }
    recorded = value.env.refresh().recovery["outcomes"]["late_seed"]
    assert recorded == {
        "commit_timestamp": "2026-10-28T00:30:00.000001Z",
        "rows": LATE_ROWS,
    }
    assert len(upgrades) == 1


def test_the_batch_plan_is_read_from_the_jobmanager_log(baseline):
    value, _ = baseline
    pod = {"metadata": {"uid": "jm", "labels": {"component": "jobmanager"}}}
    value.progress(pod, PLAN_LINE + "\n")
    # Overlapping log reads return the line again, which is the same plan.
    value.progress(pod, PLAN_LINE + "\n")
    assert value.outcomes["batch_plan"] == {
        "partitions": 7,
        "read_timestamp": "2026-10-28T14:43:09.123456000Z",
        "plans": 1,
    }
