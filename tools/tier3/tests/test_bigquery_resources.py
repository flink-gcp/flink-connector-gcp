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
"""Synthetic REST contracts, including ambiguous writes and unsafe cleanup."""

import copy
import json
from dataclasses import replace

import pytest
import requests
from flink_tier3.bigquery import Trial, query
from flink_tier3.bigquery_resources import BASE, BigQueryResources, ResourcePlan
from flink_tier3.common import ApiError, Failure, TransportError

NOW = 1700000000


class Response:
    def __init__(self, value, status=200):
        self.status_code = status
        self.data = value if isinstance(value, bytes) else json.dumps(value).encode()
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.closed = True

    def iter_content(self, size):
        for start in range(0, len(self.data), size):
            yield self.data[start : start + size]


class Http:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, copy.deepcopy(kwargs)))
        value = self.responses.pop(0)
        if isinstance(value, Exception):
            raise value
        return value if isinstance(value, Response) else Response(value)


@pytest.fixture
def plan():
    return ResourcePlan(
        Trial("resources-1312", "EO", 10, 23),
        "a" * 32,
        (NOW + 3600) * 1000,
        3,
        1024**3,
        60000,
    )


def client(plan, *responses, deadline=NOW + 120, clock=lambda: NOW):
    http = Http(*responses)
    return BigQueryResources(http, plan, deadline, clock), http


def table(plan, destination=0):
    return {
        **plan.table_body(destination),
        "type": "TABLE",
        "creationTime": str(NOW * 1000),
        "numRows": "0",
        "etag": "server-etag",
    }


def job(plan, slot=0, state="DONE"):
    value = plan.job_body(slot)
    value["configuration"]["jobType"] = "QUERY"
    value["configuration"]["query"]["destinationTable"] = {
        "projectId": "flink-gcp",
        "datasetId": "_anonymous",
        "tableId": "result",
    }
    return {
        **value,
        "status": {"state": state},
        "statistics": {"query": {"totalBytesBilled": "10485760", "cacheHit": False}},
    }


def page(plan, start=0, end=None, token=None):
    fields = [
        "run_id",
        "mode",
        "expected_records",
        "destinations",
        "destination",
        "total_rows",
        "valid_rows",
        "distinct_sequences",
    ]
    rows = []
    for destination in range(start, plan.trial.destinations if end is None else end):
        count = str(plan.trial.expected(destination))
        values = [
            plan.trial.run_id,
            plan.trial.mode,
            str(plan.trial.records),
            str(plan.trial.destinations),
            str(destination),
            count,
            count,
            count,
        ]
        rows.append({"f": [{"v": value} for value in values]})
    result = {
        "jobReference": plan.job_body(0)["jobReference"],
        "jobComplete": True,
        "totalRows": str(plan.trial.destinations),
        "schema": {
            "fields": [
                {
                    "name": name,
                    "type": "STRING" if name in ("run_id", "mode") else "INTEGER",
                    "mode": "NULLABLE",
                }
                for name in fields
            ]
        },
        "rows": rows,
    }
    if token is not None:
        result["pageToken"] = token
    return result


@pytest.mark.parametrize("mode", ["ALO", "EO"])
@pytest.mark.parametrize("destinations", [10, 50])
def test_request_contract_for_every_destination(plan, mode, destinations):
    plan = replace(
        plan, trial=Trial("resources-1312", mode, destinations, destinations * 2 + 3)
    )
    responses = []
    for destination in range(destinations):
        responses.extend([Response({}, 404), table(plan, destination)])
    api, http = client(plan, *responses)
    for destination in range(destinations):
        assert api.ensure_table(destination)["creationTime"] == str(NOW * 1000)
        get, create = http.calls[destination * 2 : destination * 2 + 2]
        assert get[0:2] == (
            "GET",
            f"{BASE}/datasets/flink_gcp_tier3_bigquery/tables/bq_resources_1312_d{destination}",
        )
        assert create[0:2] == (
            "POST",
            f"{BASE}/datasets/flink_gcp_tier3_bigquery/tables",
        )
        body = create[2]["json"]
        assert body["schema"]["fields"] == [
            {"name": "run_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "sequence", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "destination", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "payload", "type": "BYTES", "mode": "NULLABLE"},
        ]
        assert body["labels"]["tier3_nonce"] == "a" * 32
        assert body["labels"]["tier3_run"] == "resources-1312"
        assert body["expirationTime"] == "1700003600000"


@pytest.mark.parametrize("kind", ["table", "query"])
def test_lost_write_response_reconciles_same_persisted_intent(plan, kind):
    resource = table(plan) if kind == "table" else job(plan)
    api, http = client(
        plan, Response({}, 404), requests.Timeout("lost response"), resource
    )
    operation = api.ensure_table if kind == "table" else api.submit_query
    with pytest.raises(TransportError):
        operation(0)
    # Simulate a restarted actor with the same persisted plan, not an in-memory cache.
    restarted = BigQueryResources(http, replace(plan), NOW + 120, lambda: NOW)
    result = (restarted.ensure_table if kind == "table" else restarted.submit_query)(0)
    assert result == resource
    assert [c[0] for c in http.calls] == ["GET", "POST", "GET"]


@pytest.mark.parametrize("kind", ["table", "query"])
@pytest.mark.parametrize("owned", [True, False])
def test_create_conflict_only_reconciles_matching_intent(plan, kind, owned):
    value = table(plan) if kind == "table" else job(plan)
    labels = value["labels"] if kind == "table" else value["configuration"]["labels"]
    if not owned:
        labels["tier3_nonce"] = "b" * 32
    api, http = client(plan, Response({}, 404), Response({}, 409), value)
    operation = api.ensure_table if kind == "table" else api.submit_query
    if owned:
        assert operation(0) == value
    else:
        with pytest.raises(Failure, match="intent"):
            operation(0)
    assert [c[0] for c in http.calls] == ["GET", "POST", "GET"]


@pytest.mark.parametrize(
    "field,value",
    [
        ("labels", {}),
        ("schema", {"fields": []}),
        ("tableReference", {}),
        ("expirationTime", "1700007200000"),
        ("type", "VIEW"),
        ("creationTime", None),
        ("creationTime", "0"),
        ("creationTime", "1700003600000"),
        ("timePartitioning", {}),
        ("cloneDefinition", {}),
    ],
)
def test_refuse_existing_unowned_or_changed_table_without_writes(plan, field, value):
    observed = table(plan)
    observed[field] = value
    api, http = client(plan, observed)
    with pytest.raises(Failure):
        api.ensure_table(0)
    assert [c[0] for c in http.calls] == ["GET"]


def test_delete_requires_receipt_and_same_creation_identity(plan):
    original = table(plan)
    replacement = {**original, "creationTime": str(NOW * 1000 + 1)}
    api, http = client(plan, replacement)
    with pytest.raises(Failure, match="replaced"):
        api.delete_table(0, original)
    assert [c[0] for c in http.calls] == ["GET"]
    with pytest.raises(Failure, match="intent"):
        api.delete_table(0, {})
    assert len(http.calls) == 1


@pytest.mark.parametrize("status", [200, 404])
def test_delete_owned_table_and_idempotent_absence(plan, status):
    api, http = client(plan, table(plan), Response({}, status), Response({}, 404))
    api.delete_table(0, table(plan))
    api.delete_table(0, table(plan))
    assert [c[0] for c in http.calls] == ["GET", "DELETE", "GET"]
    assert http.calls[0][1] == http.calls[1][1] == http.calls[2][1]


def test_query_budget_and_retry_identity(plan):
    api, http = client(plan, Response({}, 404), job(plan, 2))
    api.submit_query(2)
    body = http.calls[1][2]["json"]
    assert body["jobReference"] == {
        "projectId": "flink-gcp",
        "location": "us-central1",
        "jobId": "bq_resources-1312_" + "a" * 32 + "_2",
    }
    assert body["configuration"]["query"] == {
        "query": query(plan.trial),
        "useLegacySql": False,
        "useQueryCache": False,
        "maximumBytesBilled": "1073741824",
    }
    assert body["configuration"]["jobTimeoutMs"] == "60000"
    assert http.calls[0][2]["params"] == {"location": "us-central1"}
    for slot in (-1, 3, True, "0"):
        with pytest.raises(ValueError):
            api.submit_query(slot)
    assert len(http.calls) == 2
    changed = replace(plan, maximum_bytes_billed=2 * 1024**3)
    assert changed.job_body(2)["jobReference"] == body["jobReference"]
    changed_api, _ = client(changed, job(plan, 2))
    with pytest.raises(Failure, match="intent"):
        changed_api.submit_query(2)


@pytest.mark.parametrize(
    "path,value",
    [
        (("jobReference", "location"), "US"),
        (("jobReference", "jobId"), "foreign"),
        (("configuration", "labels"), {}),
        (("configuration", "jobTimeoutMs"), "0"),
        (("configuration", "dryRun"), True),
        (("configuration", "query", "query"), "SELECT 1"),
        (("configuration", "query", "maximumBytesBilled"), "2147483648"),
        (("configuration", "query", "useLegacySql"), True),
        (("configuration", "query", "useQueryCache"), True),
    ],
)
def test_foreign_or_changed_query_cannot_be_adopted_read_or_cancelled(
    plan, path, value
):
    observed = job(plan, state="RUNNING")
    target = observed
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    for operation in ("submit_query", "cancel_query", "results"):
        api, http = client(plan, observed)
        with pytest.raises(Failure, match="intent"):
            getattr(api, operation)(0)
        assert [c[0] for c in http.calls] == ["GET"]


def test_cancel_is_request_not_completion(plan):
    running = job(plan, state="RUNNING")
    api, http = client(plan, running, {"job": running}, job(plan), Response({}, 404))
    assert api.cancel_query(0)["status"]["state"] == "RUNNING"
    assert api.cancel_query(0)["status"]["state"] == "DONE"
    assert api.cancel_query(0) is None
    assert [c[0] for c in http.calls] == ["GET", "POST", "GET", "GET"]
    assert http.calls[1][1].endswith("/cancel")
    assert http.calls[1][2]["params"] == {"location": "us-central1"}


@pytest.mark.parametrize("mode", ["ALO", "EO"])
@pytest.mark.parametrize("destinations", [10, 50])
def test_collect_all_pages_preserves_job_and_runs_oracle(plan, mode, destinations):
    plan = replace(
        plan, trial=Trial("resources-1312", mode, destinations, 2 * destinations + 3)
    )
    original_job = job(plan)
    api, http = client(
        plan, original_job, page(plan, 0, 1, "a+/="), page(plan, 1, destinations)
    )
    result = api.results(0)
    assert result["job"] == original_job
    assert result["report"]["verdict"] == "pass"
    assert len(result["rows"]) == destinations
    assert http.calls[1][1].endswith("/queries/bq_resources-1312_" + "a" * 32 + "_0")
    assert http.calls[1][2]["params"] == {
        "location": "us-central1",
        "maxResults": destinations,
        "timeoutMs": 0,
    }
    assert http.calls[2][2]["params"]["pageToken"] == "a+/="
    assert [c[0] for c in http.calls] == ["GET"] * 3


def test_result_schema_controls_flattening_and_mismatch_remains_data_verdict(plan):
    value = page(plan)
    value["rows"][0]["f"][5]["v"] = "4"  # One invalid row, not malformed evidence.
    value["schema"]["fields"].reverse()
    for row in value["rows"]:
        row["f"].reverse()
    api, _ = client(plan, job(plan), value)
    result = api.results(0)
    assert result["report"]["verdict"] == "fail"
    assert result["rows"][0]["total_rows"] == "4"


@pytest.mark.parametrize("change", ["running", "failed", "no-billing", "over-budget"])
def test_result_requires_success_and_billing_evidence_before_page_read(plan, change):
    value = job(plan)
    if change == "running":
        value["status"]["state"] = "RUNNING"
    elif change == "failed":
        value["status"]["errorResult"] = {"reason": "invalidQuery"}
    else:
        value["statistics"]["query"]["totalBytesBilled"] = (
            None if change == "no-billing" else str(plan.maximum_bytes_billed + 1)
        )
    api, http = client(plan, value)
    with pytest.raises(Failure):
        api.results(0)
    assert len(http.calls) == 1


@pytest.mark.parametrize(
    "change",
    [
        "foreign",
        "incomplete",
        "error",
        "total",
        "schema",
        "duplicate-field",
        "type",
        "repeated-field",
        "short-row",
        "null",
        "missing-row",
        "extra-row",
        "duplicate-row",
        "oversized-row",
    ],
)
def test_malformed_or_incomplete_results_never_produce_verdict(plan, change):
    value = page(plan)
    if change == "foreign":
        value["jobReference"]["jobId"] = "foreign"
    elif change == "incomplete":
        value["jobComplete"] = False
    elif change == "error":
        value["errors"] = [{"reason": "backendError"}]
    elif change == "total":
        value["totalRows"] = "9"
    elif change == "schema":
        value["schema"]["fields"].pop()
    elif change == "duplicate-field":
        value["schema"]["fields"][1] = value["schema"]["fields"][0]
    elif change == "type":
        value["schema"]["fields"][2]["type"] = "FLOAT"
    elif change == "repeated-field":
        value["schema"]["fields"][0]["mode"] = "REPEATED"
    elif change == "short-row":
        value["rows"][0]["f"].pop()
    elif change == "null":
        value["rows"][0]["f"][0]["v"] = None
    elif change == "missing-row":
        value["rows"].pop()
    elif change == "extra-row":
        value["rows"].append(value["rows"][0])
    elif change == "duplicate-row":
        value["rows"][1] = value["rows"][0]
    else:
        value["rows"][0]["f"][0]["v"] = "x" * 65536
    api, _ = client(plan, job(plan), value)
    with pytest.raises(
        Failure, match="result bound" if change == "oversized-row" else None
    ):
        api.results(0)


def test_pagination_cycles_and_page_count_are_bounded(plan):
    api, http = client(
        plan, job(plan), page(plan, 0, 0, "same"), page(plan, 0, 0, "same")
    )
    with pytest.raises(Failure, match="repeated"):
        api.results(0)
    assert len(http.calls) == 3
    pages = [page(plan, 0, 0, str(i)) for i in range(11)]
    api, http = client(plan, job(plan), *pages)
    with pytest.raises(Failure, match="page ceiling"):
        api.results(0)
    assert len(http.calls) == 12


@pytest.mark.parametrize(
    "field,value",
    [
        ("schema", None),
        ("schema", {"fields": [None]}),
        ("rows", None),
        ("rows", [None]),
        ("rows", [{"f": [None] * 8}]),
        ("pageToken", 0),
        ("pageToken", []),
    ],
)
def test_invalid_page_shapes_fail_closed(plan, field, value):
    result = page(plan)
    result[field] = value
    api, _ = client(plan, job(plan), result)
    with pytest.raises(Failure):
        api.results(0)


def test_deadline_stops_pagination_before_another_http_request(plan):
    now = [NOW]

    class ExpiringPage(Response):
        def __exit__(self, *args):
            super().__exit__(*args)
            now[0] += 120

    api, http = client(
        plan, job(plan), ExpiringPage(page(plan, 0, 1, "more")), clock=lambda: now[0]
    )
    with pytest.raises(Failure, match="deadline"):
        api.results(0)
    assert len(http.calls) == 2


def test_transport_is_bounded_and_does_not_follow_redirects_or_retry(plan):
    api, http = client(plan, Response({}, 503), deadline=NOW + 2)
    with pytest.raises(ApiError) as error:
        api.table(0)
    assert error.value.status == 503
    assert len(http.calls) == 1
    assert http.calls[0][2]["timeout"] == 2
    assert http.calls[0][2]["allow_redirects"] is False
    assert http.calls[0][2]["stream"] is True
    expired, http = client(plan, deadline=NOW)
    with pytest.raises(Failure, match="deadline"):
        expired.submit_query(0)
    assert not http.calls


@pytest.mark.parametrize("raw", [b"[]", b"not-json", b"\xff"])
def test_invalid_response_closed(plan, raw):
    response = Response(raw)
    api, _ = client(plan, response)
    with pytest.raises(Failure):
        api.table(0)
    assert response.closed


def test_oversized_valid_response_rejected_and_closed(plan):
    # Otherwise valid owned metadata must not pass if the transport guard is lost.
    response = Response({**table(plan), "description": "x" * (1024**2)})
    api, _ = client(plan, response)
    with pytest.raises(Failure, match="response exceeds 1 MiB"):
        api.table(0)
    assert response.closed


@pytest.mark.parametrize(
    "field,value",
    [
        ("nonce", "../foreign"),
        ("nonce", "A" * 32),
        ("expires_ms", True),
        ("query_slots", 0),
        ("maximum_bytes_billed", -1),
        ("query_timeout_ms", 0),
        ("query_slots", (1 << 63) - 1),
    ],
)
def test_invalid_intent_rejected(plan, field, value):
    with pytest.raises(ValueError):
        replace(plan, **{field: value})


@pytest.mark.parametrize("seconds", [0, -1, 86401])
def test_expiration_bounds_before_any_request(plan, seconds):
    api, http = client(replace(plan, expires_ms=(NOW + seconds) * 1000))
    with pytest.raises(Failure, match="expiration"):
        api.ensure_table(0)
    assert not http.calls
