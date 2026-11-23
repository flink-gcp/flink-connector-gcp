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
from flink_tier3.bigtable.exercise import (
    COMMIT_SECONDS,
    ORACLE_STAGE,
    BigtableSequenceExercise,
)
from flink_tier3.bigtable.trial import Trial
from flink_tier3.common import TransportError, timestamp, utc
from flink_tier3.exercise import TIME_LIMIT_SECONDS
from flink_tier3.supervisor import RESOURCE_EXERCISES

from ..dispatched import inputs as inputs  # noqa: PLC0414
from ..dispatched import prepared as prepared  # noqa: PLC0414
from ..dispatched import renderer as renderer  # noqa: PLC0414
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
    """The sequence exercise at its oracle stage, its five-record input finished."""
    value, tables = built(prepared, monkeypatch)
    value.env.sleep(10)
    finish(value, value.env.clock())
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


def built(prepared, monkeypatch):
    """The exercise the supervisor builds for a five-record sequence trial."""
    environment, _, upgrade, _ = prepared
    environment.actor = "supervisor"
    # The sequence's end, well inside the run's time limit.
    end = utc(environment.clock() + 1800)
    small = Trial(environment.approval.run_id, 5, 1, end, kind="sequence")
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
    value = RESOURCE_EXERCISES[environment.approval.scenario](
        environment, upgrade, tables
    )
    value.persist("baseline")
    return value, tables


@pytest.fixture
def sequence(prepared, monkeypatch):
    """The fault-sequence exercise over the same five-record input."""
    return built(prepared, monkeypatch)


def test_the_sequence_trial_builds_the_sequence_exercise(sequence):
    value, _ = sequence
    assert isinstance(value, BigtableSequenceExercise)
    assert value.deadline == value.env.schedule.started + TIME_LIMIT_SECONDS


def finish(value, at):
    """The input finished at `at`, and the run at its oracle stage."""
    value.samples[("tm", 0)] = {
        "at": at,
        "phase": "upgrade",
        "processed": 5,
        "lineage": "x",
        "sequence": 4,
        "pod_uid": "tm",
    }
    value.persist("complete")


def test_a_sequence_run_is_judged_by_its_data_alone(sequence):
    """The three oracles decide."""
    value, _ = sequence
    end = timestamp(value.trial.change_stream_end)
    finished = end - COMMIT_SECONDS - 60
    finish(value, finished)
    # The job cannot finish, and the tables are not read, before the end.
    value.env.clock = lambda: end
    value.verify_tables()
    record = value.env.refresh().recovery
    assert (record["verdict"], record["reasons"]) == ("usable", [])
    oracles = record["outcomes"]["oracles"]
    assert oracles["input_finished_at"] == utc(finished)
    assert oracles["relay"]["judged"] is True


def test_the_relay_is_judged_from_the_latest_progress_line(sequence):
    """A restore after the input finished may commit rows only then."""
    value, _ = sequence
    end = timestamp(value.trial.change_stream_end)
    finish(value, end - COMMIT_SECONDS - 60)
    # The input finished again after a restore, too close to the end.
    value.samples[("tm", 1)] = {
        **value.samples[("tm", 0)],
        "at": end - 60,
        "sequence": 5,
    }
    value.env.clock = lambda: end
    value.verify_tables()
    record = value.env.refresh().recovery
    assert record["reasons"] == ["relay-oracle-unjudged"]


@pytest.mark.parametrize(
    "restore,judged",
    [(-1, True), (1, False), (COMMIT_SECONDS + 840, False)],
    ids=["before-the-line", "just-after-the-line", "near-the-end"],
)
def test_a_restore_after_the_latest_progress_line_leaves_the_relay_unjudged(
    sequence, restore, judged
):
    """Its recorded time precedes the commits its state carried, so it bounds nothing."""
    value, _ = sequence
    end = timestamp(value.trial.change_stream_end)
    line = end - COMMIT_SECONDS - 900
    finish(value, line)
    value.restored_at = line + restore
    value.env.clock = lambda: end
    value.verify_tables()
    record = value.env.refresh().recovery
    oracles = record["outcomes"]["oracles"]
    assert oracles["input_finished_at"] == utc(line)
    assert oracles["restored_after_input"] is not judged
    assert oracles["relay"]["judged"] is judged
    assert record["reasons"] == ([] if judged else ["relay-oracle-unjudged"])
