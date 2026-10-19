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
"""The Bigtable exercise's table reads, page by page, and the verdict they decide."""

import dataclasses
from types import SimpleNamespace

import pytest
from flink_tier3.bigtable.exercise import ORACLE_STAGE, BigtableExercise
from flink_tier3.bigtable.trial import Trial
from flink_tier3.bigtable.verdict import OBSERVED, SAMPLED_STAGES
from flink_tier3.common import TransportError, timestamp, utc

from ..cloudtasks.test_recovery_plan import inputs as inputs  # noqa: PLC0414
from ..cloudtasks.test_recovery_plan import renderer as renderer  # noqa: PLC0414
from ..cloudtasks.test_recovery_runtime import prepared as prepared  # noqa: PLC0414
from ..test_lifecycle import env as env  # noqa: PLC0414
from ..test_lifecycle import rt
from .test_tables import eo_row, relay_row


class Tables:
    """Each table's rows, read two at a time after a key, as ReadRows pages."""

    def __init__(self, env, trial, rows):
        self.env, self.trial, self.rows = env, trial, rows
        self.reads, self.errors = [], []

    def listing(self, until):
        return SimpleNamespace(read_rows=self.read_rows)

    def read_rows(self, table, after):
        self.reads.append((table, after))
        if self.errors:
            raise self.errors.pop(0)
        rows = [row for row in self.rows[table] if after is None or row[0] > after]
        page = sorted(rows)[:2]
        # The token is the last key; an empty page ends the table.
        return page, page[-1][0] if page else None


@pytest.fixture
def exercise(prepared, monkeypatch):
    """A Bigtable exercise that has proved both recoveries over a five-record input."""
    environment, _, upgrade, _ = prepared
    environment.actor = "supervisor"
    end = utc(environment.clock() + 3600)
    small = Trial(environment.approval.run_id, 5, 1, end)
    environment.approval = dataclasses.replace(
        environment.approval, scenario="bigtable-recovery"
    )
    monkeypatch.setattr(
        type(environment.approval), "bigtable_plan", property(lambda self: small)
    )
    rows = {
        "eo": [eo_row(n) for n in range(5)],
        "alo": [eo_row(n, markers=0) for n in range(5)],
        "relay": [relay_row(n) for n in range(5)],
    }
    tables = Tables(environment, small, rows)
    environment.records.set_phase(rt.Phase.READY)
    environment.records.set_phase(rt.Phase.RUNNING)
    value = BigtableExercise(environment, upgrade, tables)
    value.outcomes.update(upgrade={}, failover={})
    value.coverage = {
        window: {"attempts": 1, "observed": sorted(OBSERVED)}
        for window in SAMPLED_STAGES
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
    value.persist("baseline")
    value.persist("complete")
    assert value.stage == ORACLE_STAGE
    return value, tables


def test_every_table_is_read_to_its_last_page_and_judged(exercise):
    value, tables = exercise
    value.verify_tables()
    record = value.env.refresh().recovery
    assert (record["stage"], record["verdict"], record["reasons"]) == (
        "complete",
        "usable",
        [],
    )
    oracles = record["outcomes"]["oracles"]
    assert {c: oracles[c]["matched"] for c in ("eo", "alo", "relay")} == {
        "eo": 5,
        "alo": 5,
        "relay": 5,
    }
    # Pages of two continue after the last key read, until an empty page.
    assert [after for table, after in tables.reads if table == "eo"] == [
        None,
        "1/0000001",
        "3/0000003",
        "4/0000004",
    ]


def test_a_transient_read_failure_is_repeated(exercise):
    value, tables = exercise
    tables.errors = [TransportError("timeout")]
    value.verify_tables()
    assert value.env.refresh().recovery["verdict"] == "usable"
    assert tables.reads[:2] == [("eo", None), ("eo", None)]


def test_an_input_finished_too_close_to_the_change_streams_end_is_unjudged(exercise):
    value, _ = exercise
    end = timestamp(value.trial.change_stream_end)
    for sample in value.samples.values():
        sample["at"] = end - 60
    value.env.clock = lambda: end
    value.verify_tables()
    record = value.env.refresh().recovery
    assert record["reasons"] == ["relay-oracle-unjudged"]
    assert record["outcomes"]["oracles"]["relay"]["verdict"] == "pass"
