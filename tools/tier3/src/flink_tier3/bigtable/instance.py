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
"""Bigtable REST operations on one run's instance, without admission."""

from __future__ import annotations

import base64

from ..common import Failure, TransportError
from ..policy import MIB, PROJECT
from ..rest import TrialService
from .trial import (
    APP_PROFILE,
    AT_LEAST_ONCE_TABLE,
    CLUSTER,
    EXACTLY_ONCE_TABLE,
    MARKER_FAMILY,
    RELAY_FAMILY,
    RELAY_TABLE,
    SUM_FAMILY,
    ZONE,
    Trial,
)

ADMIN = "bigtableadmin.googleapis.com/v2/"
DATA = "bigtable.googleapis.com/v2/"
# A ReadRows page: at most this many rows, which keeps one response of the
# trial's widest rows far below the response ceiling. A page may hold fewer
# while more follow, so reading ends at an empty page.
PAGE_ROWS = 5000
MAX_RESPONSE_BYTES = 16 * MIB


def _key(text):
    return base64.b64encode(text.encode()).decode()


# Int64 SUM, as the client library's Type.int64Sum() encodes it.
_SUM = {
    "valueType": {
        "aggregateType": {
            "inputType": {"int64Type": {"encoding": {"bigEndianBytes": {}}}},
            "sum": {},
        }
    }
}
# The application's tables (kubernetes/apps/bigtable README): `eo` keeps the
# staged sink's markers without a GC rule and a day of change stream, and its
# splits give the source at least four partitions.
TABLES = {
    EXACTLY_ONCE_TABLE: {
        "columnFamilies": {SUM_FAMILY: _SUM, MARKER_FAMILY: {}},
        "changeStreamConfig": {"retentionPeriod": "86400s"},
    },
    AT_LEAST_ONCE_TABLE: {"columnFamilies": {SUM_FAMILY: _SUM}},
    RELAY_TABLE: {"columnFamilies": {RELAY_FAMILY: {}}},
}
SPLITS = {EXACTLY_ONCE_TABLE: ["4", "8", "c"]}
# Single-cluster routing with single-row transactions, which the staged sink
# requires and the change-stream source's partition ledger uses.
ROUTING = {"clusterId": CLUSTER, "allowTransactionalWrites": True}


class Instance(TrialService):
    """No automatic retries; the caller owns admission, intent and ownership.

    Inject a Google authorized session with retries disabled. The absolute UTC
    deadline bounds starting each call and reading its response.
    """

    label = "Bigtable"
    base = "https://"
    max_response_bytes = MAX_RESPONSE_BYTES
    trial_type = Trial

    @staticmethod
    def resource_name(trial):
        return trial.instance_path

    def get(self):
        """The instance, or None when it does not exist."""
        return self._call("GET", ADMIN + self.name, absent=True)

    def create(self):
        """Start creating the run's one-node SSD instance; returns the operation."""
        return self._call(
            "POST",
            ADMIN + f"projects/{PROJECT}/instances",
            {
                "instanceId": self.trial.instance,
                "instance": {
                    "displayName": "Tier-3 Bigtable recovery",
                    "type": "PRODUCTION",
                },
                "clusters": {
                    CLUSTER: {
                        "location": f"projects/{PROJECT}/locations/{ZONE}",
                        "serveNodes": 1,
                        "defaultStorageType": "SSD",
                    }
                },
            },
        )

    def create_table(self, table):
        body = {"tableId": table, "table": TABLES[table]}
        if table in SPLITS:
            body["initialSplits"] = [{"key": _key(key)} for key in SPLITS[table]]
        return self._call("POST", ADMIN + self.name + "/tables", body)

    def table(self, table):
        """A table's schema and change-stream configuration, or None."""
        return self._call(
            "GET",
            ADMIN + self.trial.table_path(table),
            params={"view": "SCHEMA_VIEW"},
            absent=True,
        )

    def create_app_profile(self):
        return self._call(
            "POST",
            ADMIN + self.name + "/appProfiles",
            {"singleClusterRouting": ROUTING},
            params={"appProfileId": APP_PROFILE},
        )

    def app_profile(self):
        return self._call(
            "GET", ADMIN + f"{self.name}/appProfiles/{APP_PROFILE}", absent=True
        )

    def disable_change_stream(self):
        """Clear `eo`'s change stream; an absent table has none."""
        self._call(
            "PATCH",
            ADMIN + self.trial.table_path(EXACTLY_ONCE_TABLE),
            {},
            params={"updateMask": "changeStreamConfig"},
            absent=True,
        )

    def delete(self):
        """Delete the instance; an absent one is already gone."""
        self._call("DELETE", ADMIN + self.name, absent=True)

    def read_rows(self, table, after=None):
        """One page of a table's rows after the key `after`, in key order.

        `after` is the page token the previous page returned: its last row
        key as the service encoded it, so a key that is not UTF-8 still
        resumes where it ended. Returns each row's key and cells, decoded, and
        the next page's token, or None after an empty page.
        """
        body = {"rowsLimit": str(PAGE_ROWS), "appProfileId": APP_PROFILE}
        if after is not None:
            body["rows"] = {"rowRanges": [{"startKeyOpen": after}]}
        responses = self._call(
            "POST",
            DATA + self.trial.table_path(table) + ":readRows",
            body,
            shape=list,
        )
        try:
            rows, last = merge(responses)
        except (ValueError, TypeError, KeyError) as error:
            raise Failure("Bigtable ReadRows response is malformed") from error
        return rows, last


def _text(value):
    # A foreign row is reported, so a key that is not UTF-8 must still decode.
    return base64.b64decode(value, validate=True).decode(errors="backslashreplace")


def merge(responses):
    """Rows from ReadRows responses, by the protocol's chunk rules.

    Returns each row as ``(key, cells)``, with every cell ``(family,
    qualifier, timestamp, value)``, and the last row's key as the service
    encoded it, or None for no row. Keys and qualifiers decode as UTF-8, which
    the application's are, and anything else decodes with escapes. A chunk
    carries its row's key, family and qualifier only when they change, a cell
    split over chunks carries `valueSize` on all but its last, `resetRow`
    discards the row so far, and a field at its default is absent.

    A streamed call that fails after its first response ends the array with
    an `error` element, which is raised as a transport failure, so the read is
    repeated rather than taken for the table's last page.
    """
    rows, last, raw, key, cells, cell = [], None, None, None, [], None
    family = qualifier = None
    for response in responses:
        if not isinstance(response, dict) or not isinstance(
            response.get("chunks", []), list
        ):
            raise Failure("Bigtable ReadRows response is malformed")
        if "error" in response:
            raise TransportError("Bigtable ReadRows stream failed after it began")
        for chunk in response.get("chunks", []):
            if not isinstance(chunk, dict):
                raise Failure("Bigtable ReadRows chunk is malformed")
            if chunk.get("resetRow"):
                raw = key = cell = family = qualifier = None
                cells = []
                continue
            if "rowKey" in chunk:
                raw, key = chunk["rowKey"], _text(chunk["rowKey"])
            if "familyName" in chunk:
                family = chunk["familyName"]
            if "qualifier" in chunk:
                qualifier = _text(chunk["qualifier"])
            value = base64.b64decode(chunk.get("value", ""), validate=True)
            if cell is None:
                cell = [family, qualifier, int(chunk.get("timestampMicros", 0)), b""]
            cell[3] += value
            if not chunk.get("valueSize"):
                cells.append(tuple(cell))
                cell = None
            if chunk.get("commitRow"):
                if key is None or cell is not None:
                    raise Failure("Bigtable ReadRows committed an incomplete row")
                rows.append((key, cells))
                last, raw = raw, None
                key = family = qualifier = None
                cells = []
    if key is not None or cells or cell is not None:
        raise Failure("Bigtable ReadRows ended inside a row")
    return rows, last
