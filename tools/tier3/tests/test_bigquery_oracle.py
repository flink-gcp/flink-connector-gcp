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
"""Run the generated SQL on synthetic tables and assess its actual aggregate rows."""

import copy
import json
import sqlite3

import pytest
from flink_tier3 import bigquery as bq
from flink_tier3 import cli


class Tables:
    def __init__(self, trial):
        self.trial = trial
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        for destination in range(trial.destinations):
            self.db.execute(
                f"CREATE TABLE `{trial.table(destination)}` "
                "(run_id TEXT, sequence INTEGER, destination INTEGER)"
            )
        for sequence in range(trial.records):
            destination = sequence % trial.destinations
            self.add(destination, trial.run_id, sequence, destination)

    def add(self, physical, run_id, sequence, destination):
        self.db.execute(
            f"INSERT INTO `{self.trial.table(physical)}` VALUES (?, ?, ?)",
            (run_id, sequence, destination),
        )

    def result(self):
        # Execute the generated query unchanged. SQLite exercises relational/NULL
        # semantics here; it is not proof of BigQuery execution or service visibility.
        return [dict(row) for row in self.db.execute(bq.query(self.trial))]

    def report(self):
        return bq.assess(self.trial, self.result())


@pytest.fixture
def tables():
    value = Tables(bq.Trial("oracle-1312", "EO", 10, 23))
    yield value
    value.db.close()


@pytest.mark.parametrize("mode", ["ALO", "EO"])
@pytest.mark.parametrize("destinations", [10, 50])
def test_complete_nondivisible_input_and_string_encoded_counts(mode, destinations):
    trial = bq.Trial("oracle-1312", mode, destinations, 2 * destinations + 3)
    tables = Tables(trial)
    try:
        rows = tables.result()
        assert len(rows) == destinations
        assert rows[0]["distinct_sequences"] == 3
        assert rows[-1]["distinct_sequences"] == 2
        exported = [
            {
                key: str(value) if isinstance(value, int) else value
                for key, value in row.items()
            }
            for row in reversed(rows)
        ]
        report = bq.assess(trial, exported)
        assert report["verdict"] == "pass"
        assert report["distinct_sequences"] == trial.records
        assert (
            report["missing_sequences"]
            == report["invalid_rows"]
            == report["duplicate_rows"]
            == 0
        )
        assert [row["destination"] for row in report["tables"]] == list(
            range(destinations)
        )
    finally:
        tables.db.close()


@pytest.mark.parametrize("mode,verdict", [("ALO", "pass"), ("EO", "fail")])
def test_duplicates_have_mode_specific_verdict(mode, verdict):
    tables = Tables(bq.Trial("oracle-1312", mode, 10, 20))
    try:
        tables.add(0, tables.trial.run_id, 0, 0)
        report = tables.report()
        assert report["verdict"] == verdict
        assert report["duplicate_rows"] == 1
        assert report["missing_sequences"] == report["invalid_rows"] == 0
    finally:
        tables.db.close()


def test_one_missing_and_one_duplicate_cannot_cancel(tables):
    trial = tables.trial
    tables.db.execute(f"DELETE FROM `{trial.table(0)}` WHERE sequence = 10")
    tables.add(0, trial.run_id, 0, 0)
    report = tables.report()
    assert report["total_rows"] == trial.records
    assert report["missing_sequences"] == report["duplicate_rows"] == 1
    assert report["verdict"] == "fail"


@pytest.mark.parametrize(
    "physical,run_id,sequence,destination",
    [
        (0, "other-run", 0, 0),
        (0, None, 0, 0),
        (0, "oracle-1312", None, 0),
        (0, "oracle-1312", -10, 0),
        (0, "oracle-1312", 30, 0),
        (0, "oracle-1312", 0, None),
        (0, "oracle-1312", 0, 1),
        (1, "oracle-1312", 0, 0),
        (1, "oracle-1312", 0, 1),
    ],
)
def test_invalid_rows_are_counted_instead_of_filtered_away(
    tables, physical, run_id, sequence, destination
):
    tables.add(physical, run_id, sequence, destination)
    report = tables.report()
    assert report["total_rows"] == tables.trial.records + 1
    assert report["invalid_rows"] == 1
    assert report["missing_sequences"] == report["duplicate_rows"] == 0
    assert report["verdict"] == "fail"


def test_empty_tables_still_produce_aggregates(tables):
    for destination in range(tables.trial.destinations):
        tables.db.execute(f"DELETE FROM `{tables.trial.table(destination)}`")
    report = tables.report()
    assert report["verdict"] == "fail"
    assert report["missing_sequences"] == tables.trial.records
    assert (
        report["total_rows"] == report["duplicate_rows"] == report["invalid_rows"] == 0
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("run_id", "x' OR TRUE --"),
        ("run_id", "-bad"),
        ("run_id", "a" * 41),
        ("run_id", None),
        ("mode", "FILE_LOADS"),
        ("mode", []),
        ("destinations", True),
        ("destinations", 9),
        ("records", True),
        ("records", 19),
        ("records", 2 * 1024**3 // 1024 + 1),
    ],
)
def test_invalid_trial_never_produces_sql(field, value):
    values = {"run_id": "oracle-1312", "mode": "EO", "destinations": 10, "records": 20}
    values[field] = value
    with pytest.raises(ValueError):
        bq.Trial(**values)


@pytest.mark.parametrize("mode,maximum", [("ALO", 32768), ("EO", 2097152)])
def test_application_byte_boundary_and_table_contract(mode, maximum):
    trial = bq.Trial("a-b", mode, 50, maximum)
    assert trial.table(49) == "flink-gcp.flink_gcp_tier3_bigquery.bq_a_b_d49"
    assert sum(trial.expected(i) for i in range(50)) == maximum
    with pytest.raises(ValueError, match="2 GiB"):
        bq.Trial("a-b", mode, 50, maximum + 1)


@pytest.mark.parametrize(
    "bad", [None, True, 1.0, -1, "01", "1.0", "-1", "1e3", " 1", 1 << 63]
)
def test_invalid_count_encoding_is_rejected(tables, bad):
    rows = tables.result()
    rows[0]["total_rows"] = bad
    with pytest.raises(ValueError, match="INT64"):
        bq.assess(tables.trial, rows)


@pytest.mark.parametrize(
    "change",
    [
        "missing",
        "extra",
        "duplicate",
        "destination",
        "run",
        "mode",
        "records",
        "fanout",
        "field",
        "negative-order",
        "impossible-distinct",
        "zero-distinct",
    ],
)
def test_malformed_or_misbound_aggregate_is_not_a_data_verdict(tables, change):
    rows = tables.result()
    if change == "missing":
        rows.pop()
    elif change == "extra":
        rows.append(copy.deepcopy(rows[0]))
    elif change == "duplicate":
        rows[-1] = copy.deepcopy(rows[0])
    elif change == "destination":
        rows[0]["destination"] = 10
    elif change == "run":
        rows[0]["run_id"] = "another"
    elif change == "mode":
        rows[0]["mode"] = "ALO"
    elif change == "records":
        rows[0]["expected_records"] = 24
    elif change == "fanout":
        rows[0]["destinations"] = 50
    elif change == "field":
        rows[0]["unexpected"] = 1
    elif change == "negative-order":
        rows[0]["valid_rows"] = rows[0]["total_rows"] + 1
    elif change == "zero-distinct":
        rows[0]["distinct_sequences"] = 0
    else:
        for field in ("total_rows", "valid_rows", "distinct_sequences"):
            rows[0][field] = tables.trial.records
    with pytest.raises(ValueError):
        bq.assess(tables.trial, rows)


@pytest.mark.parametrize(
    "payload",
    [
        b'{"x":1,"x":2}',
        b"[] trailing",
        b"\xff",
        b"[" * 2000,
        b" " * (bq.MAX_RESULT_BYTES + 1),
    ],
)
def test_bounded_json_reader_rejects_ambiguous_or_invalid_input(tmp_path, payload):
    path = tmp_path / "result.json"
    path.write_bytes(payload)
    with pytest.raises(ValueError):
        bq.read_result(path)


def test_offline_cli_needs_no_repository_and_returns_data_failure_code(
    tables, tmp_path, monkeypatch, capsys
):
    monkeypatch.chdir(tmp_path)
    args = [
        "--run-id",
        "oracle-1312",
        "--mode",
        "EO",
        "--destinations",
        "10",
        "--records",
        "23",
    ]
    assert cli.main(["bigquery", "query", *args]) == 0
    assert capsys.readouterr().out == bq.query(tables.trial)
    path = tmp_path / "result.json"
    path.write_text(json.dumps(tables.result()))
    assert cli.main(["bigquery", "assess", *args, "--result", str(path)]) == 0
    assert json.loads(capsys.readouterr().out)["verdict"] == "pass"
    tables.add(0, "oracle-1312", 0, 0)
    path.write_text(json.dumps(tables.result()))
    assert cli.main(["bigquery", "assess", *args, "--result", str(path)]) == 1
    assert json.loads(capsys.readouterr().out)["duplicate_rows"] == 1
    path.write_text("[]")
    with pytest.raises(SystemExit) as error:
        cli.main(["bigquery", "assess", *args, "--result", str(path)])
    assert error.value.code == 2
