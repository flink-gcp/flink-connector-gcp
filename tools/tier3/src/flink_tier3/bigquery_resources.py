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
"""BigQuery REST operations for a persisted trial intent, without admission."""

from __future__ import annotations

import json
import math
import re
import time
from dataclasses import asdict, dataclass

import google.auth.exceptions
import requests

from .bigquery import (
    DATASET,
    FIELDS,
    MAX_RESULT_BYTES,
    PROJECT,
    Trial,
    assess,
    integer,
    query,
)
from .common import ApiError, Failure, TransportError, digest, json_bytes
from .policy import HTTP_TIMEOUT, REGION

BASE = f"https://bigquery.googleapis.com/bigquery/v2/projects/{PROJECT}"
MAX_RESPONSE_BYTES = 1024 * 1024
TABLE_FIELDS = (
    ("run_id", "STRING"),
    ("sequence", "INTEGER"),
    ("destination", "INTEGER"),
    ("payload", "BYTES"),
)


def positive(value, name):
    if type(value) is not int or not 0 < value <= (1 << 63) - 1:
        raise ValueError(f"{name} must be a positive INT64")


@dataclass(frozen=True)
class ResourcePlan:
    """Caller-persisted intent; values must come from the approved run budget.

    Persist before the first write and reuse unchanged after restart. The caller
    owns authorization, exclusive lifecycle access and query-slot reservation.
    """

    trial: Trial
    nonce: str
    expires_ms: int
    query_slots: int
    maximum_bytes_billed: int
    query_timeout_ms: int

    def __post_init__(self):
        if not isinstance(self.trial, Trial):
            raise TypeError("trial must be a validated Trial")
        if not isinstance(self.nonce, str) or not re.fullmatch(
            r"[0-9a-f]{32}", self.nonce
        ):
            raise ValueError("nonce must be 32 lowercase hexadecimal characters")
        for name in (
            "expires_ms",
            "query_slots",
            "maximum_bytes_billed",
            "query_timeout_ms",
        ):
            positive(getattr(self, name), name)
        positive(
            self.query_slots * self.maximum_bytes_billed, "total query byte budget"
        )

    def labels(self):
        return {
            "tier3_run": self.trial.run_id,
            "tier3_nonce": self.nonce,
            "tier3_trial": digest(asdict(self.trial))[:63],
        }

    def table_body(self, destination):
        if (
            type(destination) is not int
            or not 0 <= destination < self.trial.destinations
        ):
            raise ValueError("destination is outside the trial")
        return {
            "tableReference": {
                "projectId": PROJECT,
                "datasetId": DATASET,
                "tableId": self.trial.table(destination).split(".")[-1],
            },
            "schema": {
                "fields": [
                    {"name": name, "type": kind, "mode": "NULLABLE"}
                    for name, kind in TABLE_FIELDS
                ]
            },
            "labels": self.labels(),
            "expirationTime": str(self.expires_ms),
        }

    def job_body(self, slot):
        if type(slot) is not int or not 0 <= slot < self.query_slots:
            raise ValueError("query slot is outside the approved budget")
        return {
            "jobReference": {
                "projectId": PROJECT,
                "location": REGION,
                "jobId": f"bq_{self.trial.run_id}_{self.nonce}_{slot}",
            },
            "configuration": {
                "labels": self.labels(),
                "jobTimeoutMs": str(self.query_timeout_ms),
                "query": {
                    "query": query(self.trial),
                    "useLegacySql": False,
                    "useQueryCache": False,
                    "maximumBytesBilled": str(self.maximum_bytes_billed),
                },
            },
        }


class BigQueryResources:
    """No automatic retries or polling; failed writes retain their exact identity.

    Inject a Google authorized session with retries disabled. The absolute UTC
    deadline bounds starting calls and result pagination, not server execution.
    """

    def __init__(self, http, plan, deadline, clock=time.time):
        if not isinstance(plan, ResourcePlan):
            raise TypeError("plan must be a ResourcePlan")
        if type(deadline) not in (int, float) or not math.isfinite(deadline):
            raise ValueError("deadline must be a finite UTC timestamp")
        self.http, self.plan, self.deadline, self.clock = http, plan, deadline, clock

    def with_deadline(self, deadline):
        """Share the transport with an operation deadline that cannot extend this one."""
        if type(deadline) not in (int, float) or not math.isfinite(deadline):
            raise ValueError("deadline must be a finite UTC timestamp")
        return BigQueryResources(
            self.http, self.plan, min(self.deadline, deadline), self.clock
        )

    def _remaining(self):
        remaining = self.deadline - self.clock()
        if remaining <= 0:
            raise Failure("BigQuery operation deadline exceeded")
        return min(HTTP_TIMEOUT, remaining)

    def _call(self, method, path, body=None, params=None, absent=False):
        timeout = self._remaining()
        try:
            with self.http.request(
                method,
                BASE + path,
                json=body,
                params=params,
                timeout=timeout,
                allow_redirects=False,
                stream=True,
            ) as response:
                self._remaining()
                if absent and response.status_code == 404:
                    return None
                if not 200 <= response.status_code < 300:
                    raise ApiError(response.status_code, method, path)
                data = bytearray()
                for chunk in response.iter_content(8192):
                    self._remaining()
                    data.extend(chunk)
                    if len(data) > MAX_RESPONSE_BYTES:
                        raise Failure("BigQuery response exceeds 1 MiB")
                self._remaining()
                result = json.loads(data) if data else {}
                if not isinstance(result, dict):
                    raise Failure("BigQuery response must be an object")
                return result
        except (
            requests.exceptions.RequestException,
            google.auth.exceptions.GoogleAuthError,
        ) as error:
            raise TransportError(
                f"{method} BigQuery request failed: {type(error).__name__}"
            ) from error
        except (ValueError, UnicodeError) as error:
            raise Failure("Invalid BigQuery JSON response") from error

    def _table_path(self, destination):
        table = self.plan.table_body(destination)["tableReference"]["tableId"]
        return f"/datasets/{DATASET}/tables/{table}"

    def _verify_table(self, destination, table):
        expected = self.plan.table_body(destination)
        if not isinstance(table, dict) or any(
            table.get(k) != v for k, v in expected.items()
        ):
            raise Failure(
                "BigQuery table differs from persisted ownership/schema intent"
            )
        if table.get("type") != "TABLE" or any(
            key in table
            for key in (
                "view",
                "materializedView",
                "externalDataConfiguration",
                "snapshotDefinition",
                "cloneDefinition",
                "timePartitioning",
                "rangePartitioning",
                "clustering",
            )
        ):
            raise Failure("Expected an ordinary unpartitioned BigQuery table")
        try:
            created = integer(table.get("creationTime"), "creationTime")
        except ValueError as error:
            raise Failure("Missing BigQuery table creation identity") from error
        if not 0 < created < self.plan.expires_ms:
            raise Failure("Invalid BigQuery table creation identity")
        return table

    def table(self, destination):
        value = self._call("GET", self._table_path(destination), absent=True)
        return None if value is None else self._verify_table(destination, value)

    def ensure_table(self, destination):
        """Create or reconcile this intent; never adopt an unrelated name collision.

        This does not prove emptiness or authorize workload admission. Metadata
        row counts can lag streaming writes; the executor owes a quiescent check.
        """
        if not 0 < self.plan.expires_ms - self.clock() * 1000 <= 86400000:
            raise Failure("Table expiration must be within the next 24 hours")
        current = self.table(destination)
        if current is not None:
            return current
        try:
            created = self._call(
                "POST", f"/datasets/{DATASET}/tables", self.plan.table_body(destination)
            )
            return self._verify_table(destination, created)
        except ApiError as error:
            if error.status != 409:
                raise
            current = self.table(destination)
            if current is None:
                raise Failure("Conflicting BigQuery table is not readable") from error
            return current

    def delete_table(self, destination, receipt):
        """Delete a verified incarnation under caller-held exclusive access.

        tables.delete has no documented generation precondition. The caller must
        stop all creators/writers first and exclude replacement through deletion.
        """
        self._verify_table(destination, receipt)
        current = self.table(destination)
        if current is None:
            return
        if current["creationTime"] != receipt["creationTime"]:
            raise Failure("BigQuery table was replaced after its receipt")
        self._call("DELETE", self._table_path(destination), absent=True)

    def _verify_job(self, slot, job):
        expected = self.plan.job_body(slot)
        config = job.get("configuration") if isinstance(job, dict) else None
        if not isinstance(config, dict) or not isinstance(config.get("query"), dict):
            raise Failure("BigQuery job has no query configuration")
        status = job.get("status")
        if not isinstance(status, dict) or status.get("state") not in (
            "PENDING",
            "RUNNING",
            "DONE",
        ):
            raise Failure("BigQuery job has no usable status")
        requested = expected["configuration"]
        if (
            job.get("jobReference") != expected["jobReference"]
            or config.get("labels") != requested["labels"]
            or config.get("jobTimeoutMs") != requested["jobTimeoutMs"]
            or config.get("dryRun", False) is not False
            or any(k in config for k in ("load", "copy", "extract"))
            or any(config["query"].get(k) != v for k, v in requested["query"].items())
        ):
            raise Failure("BigQuery job differs from persisted query intent")
        return job

    def _job_path(self, slot, collection="jobs"):
        return f"/{collection}/{self.plan.job_body(slot)['jobReference']['jobId']}"

    def job(self, slot):
        value = self._call(
            "GET", self._job_path(slot), params={"location": REGION}, absent=True
        )
        return None if value is None else self._verify_job(slot, value)

    def submit_query(self, slot):
        """Submit an already reserved slot, or recover its unchanged job ID."""
        current = self.job(slot)
        if current is not None:
            return current
        try:
            return self._verify_job(
                slot, self._call("POST", "/jobs", self.plan.job_body(slot))
            )
        except ApiError as error:
            if error.status != 409:
                raise
            current = self.job(slot)
            if current is None:
                raise Failure("Conflicting BigQuery job is not readable") from error
            return current

    def cancel_query(self, slot):
        """Request cancellation of an owned job; the caller must confirm DONE."""
        current = self.job(slot)
        if current is None or current.get("status", {}).get("state") == "DONE":
            return current
        result = self._call(
            "POST", self._job_path(slot) + "/cancel", params={"location": REGION}
        )
        return self._verify_job(slot, result.get("job", {}))

    def results(self, slot):
        """Collect a successful job's complete bounded aggregate and statistics."""
        job = self.job(slot)
        if job is None or job.get("status", {}).get("state") != "DONE":
            raise Failure("BigQuery query has not completed")
        if job["status"].get("errorResult"):
            raise Failure("BigQuery query failed")
        statistics = job.get("statistics")
        query_statistics = (
            statistics.get("query") if isinstance(statistics, dict) else None
        )
        if not isinstance(query_statistics, dict):
            raise Failure("BigQuery query billing evidence is missing")
        try:
            billed = integer(
                query_statistics.get("totalBytesBilled"), "totalBytesBilled"
            )
        except ValueError as error:
            raise Failure("BigQuery query billing evidence is missing") from error
        if billed > self.plan.maximum_bytes_billed:
            raise Failure("BigQuery query exceeded its byte budget")
        rows, tokens = [], set()
        token = None
        for _ in range(self.plan.trial.destinations + 1):
            params = {
                "location": REGION,
                "maxResults": self.plan.trial.destinations,
                "timeoutMs": 0,
            }
            if token is not None:
                params["pageToken"] = token
            page = self._call("GET", self._job_path(slot, "queries"), params=params)
            if (
                page.get("jobReference") != job["jobReference"]
                or page.get("jobComplete") is not True
                or page.get("errors")
                or page.get("totalRows") != str(self.plan.trial.destinations)
            ):
                raise Failure("Incomplete or mismatched BigQuery result page")
            schema = page.get("schema")
            fields = schema.get("fields") if isinstance(schema, dict) else None
            if not isinstance(fields, list) or any(
                not isinstance(f, dict) for f in fields
            ):
                raise Failure("Unexpected BigQuery aggregate schema")
            names = [f.get("name") for f in fields]
            if (
                len(names) != len(FIELDS)
                or any(not isinstance(name, str) for name in names)
                or set(names) != FIELDS
                or any(
                    f.get("type")
                    != ("STRING" if f["name"] in ("run_id", "mode") else "INTEGER")
                    or f.get("mode", "NULLABLE") not in ("NULLABLE", "REQUIRED")
                    for f in fields
                )
            ):
                raise Failure("Unexpected BigQuery aggregate schema")
            page_rows = page.get("rows", [])
            if not isinstance(page_rows, list):
                raise Failure("Malformed BigQuery aggregate rows")
            for row in page_rows:
                cells = row.get("f") if isinstance(row, dict) else None
                if (
                    not isinstance(cells, list)
                    or len(cells) != len(names)
                    or any(not isinstance(c, dict) or set(c) != {"v"} for c in cells)
                ):
                    raise Failure("Malformed BigQuery aggregate row")
                rows.append(dict(zip(names, (c["v"] for c in cells), strict=True)))
            if (
                len(rows) > self.plan.trial.destinations
                or len(json_bytes(rows)) > MAX_RESULT_BYTES
            ):
                raise Failure("BigQuery aggregate exceeds its result bound")
            token = page.get("pageToken")
            if token is None or token == "":
                break
            if not isinstance(token, str) or token in tokens:
                raise Failure("Invalid or repeated BigQuery page token")
            tokens.add(token)
        else:
            raise Failure("BigQuery result page ceiling exceeded")
        try:
            report = assess(self.plan.trial, rows)
        except ValueError as error:
            raise Failure("Invalid BigQuery aggregate") from error
        return {"job": job, "rows": rows, "report": report}
