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
"""The queries the oracles page with in each mode, and the oracles."""

import json

import pytest
from flink_tier3.firestore import database
from flink_tier3.firestore.database import Databases
from flink_tier3.firestore.oracle import BatchCopy, Seed, Sink
from flink_tier3.firestore.trial import MODES, Trial, record_key

from ..bigquery.test_resources import NOW, Response

TRIAL = Trial("fs-oracle", 20, 10, 30, 1)
READ_AT = "2026-10-28T01:20:00.123456Z"
COMMITTED = "2026-10-28T00:30:00.000001Z"
FIRESTORE = "https://firestore.googleapis.com/v1/"
DATASTORE = "https://datastore.googleapis.com/v1/projects/flink-gcp:"


class Firestore:
    """Both APIs over one run's two databases, as their REST references describe.

    A database created reads back `creating` seconds later, and one deleted
    reads back `deleting` more times before it is gone; a DELETE of a
    database that does not exist answers 404. A commit of more than 500
    writes, or to a database that does not exist, is refused, and so is a
    Datastore API call without the routing header that names its database,
    with the service's 400 and message. A
    Native-mode query answers an array, every element with the read time,
    and resumes after a document's full name; a Datastore-mode one answers a
    batch with an end cursor and whether more results follow. Each
    collection holds its documents by key as (fields, update time).
    """

    def __init__(self, clock, trial=TRIAL):
        self.clock, self.trial = clock, trial
        self.databases, self.ready_at = {}, {}
        self.data = {mode: {} for mode in MODES}
        self.calls, self.errors, self.writes, self.queries = [], {}, [], []
        self.creating, self.deleting, self.vanishing = 30, 0, {}

    def request(self, method, url, json=None, params=None, headers=None, **_):
        if url.startswith(DATASTORE):
            database = self.trial.database("datastore")
            routing = f"project_id=flink-gcp&database_id={database}"
            if (headers or {}).get("x-goog-request-params") != routing:
                message = (
                    "Missing routing header. Please fill in the request header"
                    " with format x-goog-request-params:" + routing + "."
                )
                return Response({"error": {"code": 400, "message": message}}, 400)
        operation, mode = self._route(method, url, params, json)
        self.calls.append((operation, mode))
        if (operation, mode) in self.errors:
            error = self.errors.pop((operation, mode))
            if isinstance(error, Exception):
                raise error
            return Response({}, error)
        return getattr(self, operation)(mode, json)

    def _route(self, method, url, params, body):
        if url.startswith(DATASTORE):
            assert body["databaseId"] == self.trial.database("datastore")
            return url.removeprefix(DATASTORE), "datastore"
        path = url.removeprefix(FIRESTORE)
        if path == "projects/flink-gcp/databases":
            assert method == "POST"
            modes = {self.trial.database(mode): mode for mode in MODES}
            return "create", modes[params["databaseId"]]
        for mode in MODES:
            if path == self.trial.database_path(mode):
                return {"GET": "get", "DELETE": "delete"}[method], mode
        return path.rsplit(":", 1)[1], "native"

    def _visible(self, mode):
        return mode in self.databases and self.clock() >= self.ready_at[mode]

    def get(self, mode, _):
        if mode in self.vanishing:
            if self.vanishing[mode] == 0:
                del self.databases[mode], self.vanishing[mode]
            else:
                self.vanishing[mode] -= 1
        if not self._visible(mode):
            return Response({}, 404)
        return Response(self.databases[mode])

    def create(self, mode, body):
        if mode in self.databases:
            return Response({}, 409)
        assert body == {"type": database.TYPES[mode], **database.SETTINGS}
        self.databases[mode] = {
            "name": self.trial.database_path(mode),
            "type": body["type"],
            **database.SETTINGS,
        }
        self.ready_at[mode] = self.clock() + self.creating
        return Response({"name": "operations/create"})

    def delete(self, mode, _):
        if mode not in self.databases:
            return Response({}, 404)
        # A deletion already running accepts the repeat and changes nothing.
        if mode not in self.vanishing:
            self.data[mode] = {}
            if self.deleting:
                self.vanishing[mode] = self.deleting
            else:
                del self.databases[mode]
        return Response({"name": "operations/delete"})

    def commit(self, mode, body):
        if not self._visible(mode):
            return Response({}, 404)
        if mode == "native":
            writes = [
                (w["update"]["name"].split("/")[-2:], w["update"]["fields"])
                for w in body["writes"]
            ]
        else:
            assert body["mode"] == "NON_TRANSACTIONAL"
            writes = []
            for mutation in body["mutations"]:
                key = mutation["upsert"]["key"]
                assert key["partitionId"]["databaseId"] == body["databaseId"]
                [element] = key["path"]
                writes.append(
                    (
                        (element["kind"], element["name"]),
                        mutation["upsert"]["properties"],
                    )
                )
        if len(writes) > 500:
            return Response({}, 400)
        self.writes.append((mode, len(writes)))
        for (collection, key), fields in writes:
            self.data[mode].setdefault(collection, {})[key] = (fields, COMMITTED)
        if mode == "native":
            return Response({"commitTime": COMMITTED})
        # The real service sets no commit time on a non-transactional commit.
        return Response(
            {
                "mutationResults": [{"version": "1", "updateTime": COMMITTED}]
                * len(writes)
            }
        )

    def _page(self, mode, collection, after, limit, read_time):
        self.queries.append((mode, collection, after, read_time))
        stored = self.data[mode].get(collection, {})
        keys = sorted(key for key in stored if after is None or key > after)[:limit]
        return [(key, *stored[key]) for key in keys]

    def runQuery(self, mode, body):
        if mode == "native":
            query = body["structuredQuery"]
            [source] = query["from"]
            documents = self.trial.database_path(mode) + "/documents"
            start = query.get("startAt")
            after = None
            if start:
                assert start["before"] is False
                after = start["values"][0]["referenceValue"].rsplit("/", 1)[1]
            page = self._page(
                mode,
                source["collectionId"],
                after,
                query["limit"],
                body.get("readTime"),
            )
            answers = [
                {
                    "document": {
                        "name": f"{documents}/{source['collectionId']}/{key}",
                        "fields": fields,
                        "updateTime": updated,
                    },
                    "readTime": READ_AT,
                }
                for key, fields, updated in page
            ]
            return Response(answers or [{"readTime": READ_AT}])
        query = body["query"]
        [kind] = query["kind"]
        cursor = query.get("startCursor")
        page = self._page(
            mode,
            kind["name"],
            cursor and cursor.removeprefix("after:"),
            query["limit"],
            body.get("readOptions", {}).get("readTime"),
        )
        results = [
            {
                "entity": {
                    "key": {"path": [{"kind": kind["name"], "name": key}]},
                    "properties": fields,
                },
                "updateTime": updated,
            }
            for key, fields, updated in page
        ]
        return Response(
            {
                "batch": {
                    "entityResults": results,
                    "endCursor": "after:" + page[-1][0] if page else cursor or "",
                    "moreResults": (
                        "MORE_RESULTS_AFTER_LIMIT"
                        if len(page) == query["limit"]
                        else "NO_MORE_RESULTS"
                    ),
                    "readTime": READ_AT,
                }
            }
        )


def fields(**values):
    return {
        name: {"integerValue": str(value)}
        if isinstance(value, int)
        else {"stringValue": value}
        for name, value in values.items()
    }


def stored(trial, late=True, at=COMMITTED):
    """Every collection as a usable run leaves it, in both databases."""
    seed = {**trial.seed_keys(), **(trial.late_keys() if late else {})}
    collections = {
        "input": {record_key(n): (fields(n=n), at) for n in range(trial.records)},
        "seed": {key: (fields(n=n), at) for key, n in seed.items()},
        "batch_out": {
            f"{record_key(n)}@initial:u:{n}": (
                fields(k=record_key(n), delivery=f"initial:u:{n}", n=n),
                at,
            )
            for n in range(trial.seed_records)
        },
    }
    return {mode: {c: dict(d) for c, d in collections.items()} for mode in MODES}


@pytest.mark.parametrize("mode", MODES)
def test_pages_read_one_read_time_and_resume_after_the_last_key(mode, monkeypatch):
    monkeypatch.setattr(database, "PAGE", 4)
    service = Firestore(lambda: NOW)
    service.databases = {m: {} for m in MODES}
    service.ready_at = {m: 0 for m in MODES}
    service.data = stored(TRIAL)
    api = Databases(service, TRIAL, NOW + 600, lambda: NOW)
    read, after, read_time = [], None, None
    while True:
        page, after, read_time = api.read_page(mode, "batch_out", after, read_time)
        read += page
        if after is None:
            break
    assert [row[:3] for row in read] == [
        (record_key(n), f"initial:u:{n}", n)
        for n in sorted(range(TRIAL.seed_records), key=record_key)
    ]
    # The first page is read now, and every later one at its read time.
    times = [query[3] for query in service.queries]
    assert times[0] is None and set(times[1:]) == {READ_AT}
    assert read_time == READ_AT


def copies(trial, extra=()):
    """Every seed record copied once, and `extra` more copies."""
    rows = [
        (record_key(n), f"initial:u:{n}", n, COMMITTED)
        for n in range(trial.seed_records)
    ]
    return rows + list(extra)


def seeded(trial=TRIAL, late=True):
    seed = Seed(trial)
    keys = {**trial.seed_keys(), **(trial.late_keys() if late else {})}
    seed.add(sorted(keys.items()))
    return seed.report()


@pytest.mark.parametrize(
    ("deviation", "kind"),
    [
        (lambda rows: rows[1:], "missing"),
        (lambda rows: [*rows[:1], (record_key(1), 7), *rows[2:]], "wrong_value"),
        (lambda rows: [*rows, ("zz", 0)], "foreign"),
    ],
)
def test_the_sink_oracle_fails_on_each_deviation(deviation, kind):
    check = Sink(TRIAL)
    check.add(deviation([(record_key(n), n) for n in range(TRIAL.records)]))
    report = check.report()
    assert (report["verdict"], report[kind]) == ("fail", 1)


def test_the_batch_oracle_counts_extra_copies_and_the_reads_span():
    later = "2026-10-28T00:40:00Z"
    check = BatchCopy(TRIAL)
    check.add(copies(TRIAL, [(record_key(4), "upgrade:v:0", 4, later)]))
    report = check.report(seeded(), "2026-10-28T00:20:00.000000001Z")
    assert report["verdict"] == "pass"
    assert (report["replayed_keys"], report["extra_deliveries"]) == (1, 1)
    assert report["deliveries_by_phase"] == {"initial": 30, "upgrade": 1}
    assert report["last_copy_at"] == later
    assert (report["read_seconds"], report["read_seconds_from"]) == (1200, "read_time")
    # Without the plan's read time the span starts at the first copy.
    unplanned = BatchCopy(TRIAL)
    unplanned.add(copies(TRIAL, [(record_key(4), "upgrade:v:0", 4, later)]))
    report = unplanned.report(seeded())
    assert report["read_seconds"] == 600


@pytest.mark.parametrize(
    ("extra", "kind"),
    [
        # A late record copied was read after the plan's read time.
        ([(record_key(TRIAL.seed_records), "u:w:0", TRIAL.seed_records, None)], "late"),
        ([("zz", "u:w:0", 0, None)], "foreign"),
        # A copy delivered twice with two values, or without its sequence.
        ([(record_key(1), "u:w:0", 99, None)], "wrong_value"),
        ([(record_key(0), "u:w:0", None, None)], "wrong_value"),
    ],
)
def test_the_batch_oracle_fails_on_each_deviation(extra, kind):
    check = BatchCopy(TRIAL)
    check.add(copies(TRIAL, extra))
    report = check.report(seeded())
    assert (report["verdict"], report[kind]) == ("fail", 1)


@pytest.mark.parametrize("mode", MODES)
def test_a_copy_without_its_key_field_is_foreign_whatever_it_is_named(mode):
    """A copy names its record only by `k`, never by its document or entity key."""
    service = Firestore(lambda: NOW)
    service.databases = {m: {} for m in MODES}
    service.ready_at = {m: 0 for m in MODES}
    service.data = stored(TRIAL)
    batch = service.data[mode]["batch_out"]
    del batch[f"{record_key(0)}@initial:u:0"]
    batch[record_key(0)] = (fields(delivery="initial:u:0", n=0), COMMITTED)
    api = Databases(service, TRIAL, NOW + 600, lambda: NOW)
    rows, after = [], None
    while True:
        page, after, _ = api.read_page(mode, "batch_out", after)
        rows += page
        if after is None:
            break
    check = BatchCopy(TRIAL)
    check.add(rows)
    report = check.report(seeded())
    assert (report["verdict"], report["foreign"], report["missing"]) == ("fail", 1, 1)


def test_the_batch_oracle_fails_unless_the_seed_holds_the_late_records():
    """Without the late records, one absent from the copy proves nothing."""
    check = BatchCopy(TRIAL)
    check.add(copies(TRIAL))
    assert check.report(seeded(late=False))["verdict"] == "fail"


@pytest.mark.parametrize(
    ("path", "name"),
    [
        ([{"kind": "input", "id": "0"}], "#key:input:0"),
        (
            [{"kind": "Parent", "name": "p"}, {"kind": "input", "name": "0-0000000"}],
            "#key:Parent:p/input:0-0000000",
        ),
    ],
)
def test_an_entity_that_is_not_a_named_root_is_foreign_to_every_record(path, name):
    """An id key, or a descendant whose own name is a record key, matches none."""
    service = Firestore(lambda: NOW)
    service.databases = {m: {} for m in MODES}
    service.ready_at = {m: 0 for m in MODES}
    service.data = stored(TRIAL)
    run_query = service.runQuery

    def with_id(mode, body):
        response = run_query(mode, body)
        if mode == "datastore":
            payload = json.loads(response.data)
            payload["batch"]["entityResults"].append(
                {
                    "entity": {
                        "key": {"path": path},
                        "properties": fields(n=0),
                    }
                }
            )
            response = Response(payload)
        return response

    service.runQuery = with_id
    api = Databases(service, TRIAL, NOW + 600, lambda: NOW)
    rows, after, _ = api.read_page("datastore", "input")
    assert after is None and rows[-1] == (name, 0)
    check = Sink(TRIAL)
    check.add(rows)
    report = check.report()
    assert (report["verdict"], report["foreign"]) == ("fail", 1)


def test_a_deletion_reads_back_until_it_has_run_and_an_absent_one_answers_404():
    service = Firestore(lambda: NOW)
    service.databases = {m: {} for m in MODES}
    service.ready_at = {m: 0 for m in MODES}
    service.deleting = 1
    api = Databases(service, TRIAL, NOW + 600, lambda: NOW)
    api.delete_database("native")
    assert api.database("native") == {}
    assert api.database("native") is None
    # The client takes the 404 of a database already gone as its absence.
    api.delete_database("native")
    assert service.calls[-1] == ("delete", "native")
