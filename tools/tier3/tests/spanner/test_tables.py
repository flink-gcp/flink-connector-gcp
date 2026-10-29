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
"""The single-use queries the oracles page with, and the three oracles."""

import re

import pytest
from flink_tier3.common import Failure
from flink_tier3.spanner import instance
from flink_tier3.spanner.instance import Instance
from flink_tier3.spanner.oracle import BatchCopy, ChangeStream, Seed, Sink, micros
from flink_tier3.spanner.trial import Trial, row_key

from ..bigquery.test_resources import NOW, Response

TRIAL = Trial("sp-oracle", 20, 10, "2026-10-28T01:05:00Z", 30, 1)
SESSION = TRIAL.database_path + "/sessions/s"
READ_AT = "2026-10-28T01:20:00.123456Z"
END = micros(TRIAL.change_stream_end)
# The primary key each table's pages must be ordered by.
ORDER = {"input": "k", "seed": "k", "relay": "k, delivery", "batch_out": "k, delivery"}


class Database:
    """The tables as Spanner answers the single-use queries over them.

    Rows are tuples in column order; a page is the rows after the query's
    key parameters, in key order when the query orders by the table's key and
    in no order without it, up to its LIMIT, with every value a string as JSON
    carries INT64. An answer without rows omits them, as proto3 JSON omits an
    empty repeated field. The late rows' commit adds them to `seed`, unless
    one of `aborts` refuses it with 409 first.
    """

    def __init__(self, tables):
        self.tables = {name: sorted(rows) for name, rows in tables.items()}
        self.queries, self.errors, self.commits = [], [], []
        self.aborts = 0

    def request(self, method, url, json=None, **_):
        path = url.removeprefix("https://spanner.googleapis.com/v1/")
        if path.endswith("/sessions"):
            return Response({"name": path + "/s"})
        if self.errors:
            raise self.errors.pop(0)
        if path.endswith(":commit"):
            if self.aborts:
                self.aborts -= 1
                return Response({}, 409)
            [write] = [m["insertOrUpdate"] for m in json["mutations"]]
            self.commits.append(write)
            self.tables[write["table"]] = sorted(
                {
                    *self.tables[write["table"]],
                    *((k, int(n)) for k, n in write["values"]),
                }
            )
            return Response({"commitTimestamp": "2026-10-28T00:30:00.000001Z"})
        assert path.endswith("/sessions/s:executeSql"), path
        bound = json["transaction"]["singleUse"]["readOnly"]
        self.queries.append((json["sql"], bound, json.get("params")))
        table = re.search(r"FROM (\w+)", json["sql"])[1]
        if json["sql"].startswith("SELECT 1 "):
            rows = [["1"]] if self.tables[table] else []
        else:
            params = json.get("params")
            if params is None:
                # The first page has no lower bound.
                assert " WHERE " not in json["sql"], json["sql"]
                after = ()
            else:
                assert json["paramTypes"] == {
                    name: {"code": "STRING"} for name in params
                }
                # Only a query that resumes inside a key reads its second column.
                width = 2 if "(k = @k AND delivery > @delivery)" in json["sql"] else 1
                after = tuple(params.values())[:width]
            limit = int(re.search(r"LIMIT (\d+)", json["sql"])[1])
            order = re.search(r" ORDER BY (.+) LIMIT ", json["sql"])
            if order:
                assert order[1] == ORDER[table], json["sql"]
            stored = self.tables[table] if order else self.tables[table][::-1]
            rows = [
                [str(value) for value in row]
                for row in stored
                if not after or row[: len(after)] > after
            ][:limit]
        metadata = {"rowType": {"fields": []}}
        if bound.get("returnReadTimestamp"):
            metadata["transaction"] = {"readTimestamp": READ_AT}
        answer = {"metadata": metadata}
        if rows:
            answer["rows"] = rows
        return Response(answer)


def test_pages_read_one_snapshot_and_resume_inside_a_key(monkeypatch):
    """A page that ends among a key's deliveries must not skip the rest of them."""
    monkeypatch.setattr(instance, "PAGE_ROWS", 2)
    rows = [("0/0000000", f"initial/u/{i}", 5) for i in range(3)]
    database = Database({"relay": rows})
    api = Instance(database, TRIAL, NOW + 600, lambda: NOW)
    read, after, snapshot = [], None, None
    while True:
        page, after, snapshot = api.read_page(SESSION, "relay", after, snapshot)
        read += page
        if after is None:
            break
    assert read == rows
    bounds = [bound for _, bound, _ in database.queries]
    assert bounds == [
        {"strong": True, "returnReadTimestamp": True},
        {"readTimestamp": READ_AT},
    ]
    assert database.queries[1][2] == {"k": "0/0000000", "delivery": "initial/u/1"}


@pytest.mark.parametrize(
    ("table", "rows"),
    [
        ("input", [("", 0), ("0/0000000", 0)]),
        ("relay", [("", "", 5), ("0/0000000", "initial/u/0", 5)]),
    ],
)
def test_the_first_page_reads_an_empty_key(table, rows):
    """An empty key sorts first, so a first page bounded by it would miss a foreign row."""
    database = Database({table: rows})
    api = Instance(database, TRIAL, NOW + 600, lambda: NOW)
    page, after, _ = api.read_page(SESSION, table)
    assert page == rows
    assert after is None


def test_a_strong_page_without_its_read_timestamp_is_refused():
    class Silent(Database):
        def request(self, *args, **kwargs):
            response = super().request(*args, **kwargs)
            response.data = b'{"metadata": {}, "rows": []}'
            return response

    api = Instance(Silent({"input": []}), TRIAL, NOW + 600, lambda: NOW)
    with pytest.raises(Failure, match="no read timestamp"):
        api.read_page(SESSION, "input")


def test_a_row_that_is_not_its_columns_is_refused():
    class Short(Database):
        def request(self, *args, **kwargs):
            response = super().request(*args, **kwargs)
            response.data = b'{"metadata": {}, "rows": [["0/0000000"]]}'
            return response

    api = Instance(Short({"input": []}), TRIAL, NOW + 600, lambda: NOW)
    with pytest.raises(Failure, match="malformed"):
        api.read_page(SESSION, "input", None, READ_AT)


def relay_rows(n, deliveries=1, commits=1, at=None):
    """`deliveries` relayed rows of key `n`, over `commits` distinct commits."""
    commit = END - 10**6 if at is None else at
    return [
        (row_key(n), f"initial/u/{n}-{i}", commit + i % commits)
        for i in range(deliveries)
    ]


def relayed():
    """Every input key relayed once."""
    relay = ChangeStream(TRIAL)
    relay.add([row for n in range(TRIAL.records) for row in relay_rows(n)])
    return relay


def test_the_sink_oracle_passes_each_key_once_with_its_sequence():
    relay = relayed()
    relay.add(relay_rows(4, deliveries=2, commits=2)[1:])
    check = Sink(TRIAL)
    check.add([(row_key(n), n) for n in range(TRIAL.records)])
    report = check.report(relay)
    assert report["verdict"] == "pass"
    # A second commit of a key is a second execution, which is reported.
    assert report["executions"]["extra_executions"] == 1


@pytest.mark.parametrize(
    ("deviation", "kind"),
    [
        (lambda rows: rows[1:], "missing"),
        (lambda rows: [*rows[:1], (row_key(1), 7), *rows[2:]], "wrong_value"),
        (lambda rows: [*rows, ("zz", 0)], "foreign"),
    ],
)
def test_the_sink_oracle_fails_on_each_deviation(deviation, kind):
    check = Sink(TRIAL)
    check.add(deviation([(row_key(n), n) for n in range(TRIAL.records)]))
    report = check.report(relayed())
    assert (report["verdict"], report[kind]) == ("fail", 1)


def test_the_change_stream_oracle_counts_replays_without_failing():
    check = relayed()
    # Delivered twice from one commit: a replay; twice from two: two executions.
    check.add(relay_rows(3, deliveries=2, commits=1)[1:])
    check.add(relay_rows(4, deliveries=2, commits=2)[1:])
    check.add([(row_key(5), "upgrade/v/0", END - 10**6 + 1)])
    report = check.report(True)
    assert (report["verdict"], report["replays"], report["replayed_keys"]) == (
        "pass",
        1,
        1,
    )
    assert report["deliveries_by_phase"] == {"initial": 22, "upgrade": 1}


@pytest.mark.parametrize(
    ("rows", "kind"),
    [
        ([row for n in range(1, TRIAL.records) for row in relay_rows(n)], "missing"),
        ([*relay_rows(2, at=END + 1)], "after_end"),
        ([("zz", "initial/u/0", 1)], "foreign"),
    ],
)
def test_the_change_stream_oracle_fails_on_each_deviation(rows, kind):
    check = ChangeStream(TRIAL)
    if kind == "missing":
        check.add(rows)
    else:
        check.add([row for n in range(TRIAL.records) for row in relay_rows(n)])
        check.add(rows)
    report = check.report(True)
    assert (report["verdict"], report[kind]) == ("fail", 1)


def seed_rows(trial, late=True):
    keys = {**trial.seed_keys(), **(trial.late_keys() if late else {})}
    return sorted(keys.items())


def seeded():
    seed = Seed(TRIAL)
    seed.add(seed_rows(TRIAL))
    return seed.report()


def copied(n, deliveries=1, value=None):
    return [
        (row_key(n), f"initial/u/{n}-{i}", n if value is None else value)
        for i in range(deliveries)
    ]


def copy():
    """Every seed row copied once."""
    return [row for n in range(TRIAL.seed_rows) for row in copied(n)]


def test_the_batch_oracle_counts_replays_by_split_without_failing():
    check = BatchCopy(TRIAL)
    check.add(copy())
    check.add(copied(4, 3)[1:])
    check.add(copied(21, 2)[1:])
    report = check.report(seeded())
    assert report["verdict"] == "pass"
    assert (report["extra_deliveries"], report["replays_by_prefix"]) == (
        3,
        {"4": 2, "5": 1},
    )


@pytest.mark.parametrize(
    ("deviation", "kind"),
    [
        (lambda rows: rows[1:], "missing"),
        # A late row copied was read after the plan's snapshot.
        (
            lambda rows: [*rows, (row_key(TRIAL.seed_rows), "u/w/0", TRIAL.seed_rows)],
            "late",
        ),
        (lambda rows: [*rows, ("zz", "u/w/0", 0)], "foreign"),
        # A copy delivered twice with two values.
        (lambda rows: [*rows, copied(1, value=99)[0]], "wrong_value"),
    ],
)
def test_the_batch_oracle_fails_on_each_deviation(deviation, kind):
    check = BatchCopy(TRIAL)
    check.add(deviation(copy()))
    report = check.report(seeded())
    assert (report["verdict"], report[kind]) == ("fail", 1)


@pytest.mark.parametrize(
    "rows",
    [
        seed_rows(TRIAL, late=False),
        [(key, 7) if key == row_key(3) else (key, n) for key, n in seed_rows(TRIAL)],
        [*seed_rows(TRIAL), ("zz", 0)],
    ],
)
def test_the_batch_oracle_fails_unless_the_seed_holds_its_rows_and_the_late_ones(rows):
    """Without the late rows, one absent from the copy proves nothing."""
    seed = Seed(TRIAL)
    seed.add(rows)
    check = BatchCopy(TRIAL)
    check.add(copy())
    assert check.report(seed.report())["verdict"] == "fail"
