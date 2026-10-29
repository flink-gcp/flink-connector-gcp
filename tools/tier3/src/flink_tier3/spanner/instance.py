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
"""Spanner REST operations on one run's instance, without admission."""

from __future__ import annotations

from ..common import ApiError, Failure, transient
from ..policy import MIB, PROJECT
from ..rest import TrialService
from .trial import (
    BATCH_TABLE,
    CHANGE_STREAM,
    DATABASE,
    INPUT_TABLE,
    RELAY_TABLE,
    SEED_TABLE,
    Trial,
    row_key,
)

API = "spanner.googleapis.com/v1/"
CONFIG = f"projects/{PROJECT}/instanceConfigs/regional-us-central1"
PROCESSING_UNITS = 100
# The application's database (kubernetes/apps/spanner README). CreateDatabase
# runs the extra statements atomically with the creation, so the database
# never exists without them; `seed` is the batch read's table, which the
# runner fills, and the retention covers the 90-minute window, because a
# restore resumes the batch read at the snapshot it planned at the start.
TABLES = (INPUT_TABLE, RELAY_TABLE, SEED_TABLE, BATCH_TABLE)
RETENTION = "2h"
STATEMENTS = (
    (
        f"CREATE TABLE {INPUT_TABLE} (k STRING(MAX) NOT NULL, n INT64 NOT NULL)"
        " PRIMARY KEY (k)"
    ),
    (
        f"CREATE TABLE {RELAY_TABLE} (k STRING(MAX) NOT NULL,"
        " delivery STRING(MAX) NOT NULL, commit_micros INT64 NOT NULL)"
        " PRIMARY KEY (k, delivery)"
    ),
    (
        f"CREATE TABLE {SEED_TABLE} (k STRING(MAX) NOT NULL, n INT64 NOT NULL)"
        " PRIMARY KEY (k)"
    ),
    (
        f"CREATE TABLE {BATCH_TABLE} (k STRING(MAX) NOT NULL,"
        " delivery STRING(MAX) NOT NULL, n INT64 NOT NULL) PRIMARY KEY (k, delivery)"
    ),
    f"CREATE CHANGE STREAM {CHANGE_STREAM} FOR {INPUT_TABLE}",
    (
        f"ALTER DATABASE `{DATABASE}`"
        f" SET OPTIONS (version_retention_period = '{RETENTION}')"
    ),
)


def retryable_commit(error):
    """Whether a seed commit may be repeated after `error`.

    A single-use read-write commit can still abort, which REST answers with
    409, and a failure that says nothing may have committed or not. Every seed
    commit is an insert-or-update of fixed values, so a repeat writes nothing
    new either way.
    """
    return transient(error) or (isinstance(error, ApiError) and error.status == 409)


# An oracle page: at most this many rows, one single-use query each. A page of
# the widest rows, a relay delivery's, is under a megabyte however the service
# prints it, and the ceiling leaves it room.
PAGE_ROWS = 5000
MAX_RESPONSE_BYTES = 4 * MIB
# Each table the oracles read: its columns, the first `KEYS[table]` of which
# are its primary key, and which of them are INT64, which JSON carries as
# decimal strings.
COLUMNS = {
    INPUT_TABLE: ("k", "n"),
    RELAY_TABLE: ("k", "delivery", "commit_micros"),
    SEED_TABLE: ("k", "n"),
    BATCH_TABLE: ("k", "delivery", "n"),
}
KEYS = {INPUT_TABLE: 1, RELAY_TABLE: 2, SEED_TABLE: 1, BATCH_TABLE: 2}
INTEGERS = frozenset(("n", "commit_micros"))
STRONG = {"strong": True, "returnReadTimestamp": True}


def page_query(table, first=False):
    """The query of one page of `table`, in key order.

    The first page has no lower bound, so even an empty key is read. A later
    page resumes after a primary key, and a key of two columns resumes inside
    the first one's rows, so a page that ends among a key's deliveries does
    not skip the rest of them.
    """
    columns = COLUMNS[table]
    after = (
        "k > @k" if KEYS[table] == 1 else "k > @k OR (k = @k AND delivery > @delivery)"
    )
    where = "" if first else f" WHERE {after}"
    order = ", ".join(columns[: KEYS[table]])
    return (
        f"SELECT {', '.join(columns)} FROM {table}{where}"
        f" ORDER BY {order} LIMIT {PAGE_ROWS}"
    )


def _row(columns, row):
    """One result row as a tuple, its INT64 columns decoded."""
    if not isinstance(row, list) or not all(isinstance(value, str) for value in row):
        raise TypeError("a row is a list of strings")
    return tuple(
        int(value) if column in INTEGERS else value
        for column, value in zip(columns, row, strict=True)
    )


class Instance(TrialService):
    """No automatic retries; the caller owns admission, intent and ownership.

    Inject a Google authorized session with retries disabled. The absolute UTC
    deadline bounds starting each call and reading its response. Every data
    call runs in a multiplexed session, which nothing deletes or reads: the
    roles hold neither permission, and deleting the instance ends it.
    """

    label = "Spanner"
    base = "https://"
    max_response_bytes = MAX_RESPONSE_BYTES
    trial_type = Trial

    @staticmethod
    def resource_name(trial):
        return trial.instance_path

    def get(self):
        """The instance, or None when it does not exist."""
        return self._call("GET", API + self.name, absent=True)

    def create(self):
        """Start creating the run's 100-processing-unit instance; returns the operation."""
        return self._call(
            "POST",
            API + f"projects/{PROJECT}/instances",
            {
                "instanceId": self.trial.instance,
                "instance": {
                    "config": CONFIG,
                    "displayName": self.trial.display_name,
                    "processingUnits": PROCESSING_UNITS,
                    "edition": "STANDARD",
                },
            },
        )

    def create_database(self):
        return self._call(
            "POST",
            API + self.name + "/databases",
            {
                "createStatement": f"CREATE DATABASE `{DATABASE}`",
                "extraStatements": list(STATEMENTS),
                "databaseDialect": "GOOGLE_STANDARD_SQL",
            },
        )

    def database(self):
        """The database, or None when it does not exist."""
        return self._call("GET", API + self.trial.database_path, absent=True)

    def ddl(self):
        """The database's DDL statements, as the service prints them, or None."""
        response = self._call(
            "GET", API + self.trial.database_path + "/ddl", absent=True
        )
        return None if response is None else response.get("statements", [])

    def session(self):
        """Create a multiplexed session in the database; returns its name."""
        response = self._call(
            "POST",
            API + self.trial.database_path + "/sessions",
            {"session": {"multiplexed": True}},
        )
        name = response.get("name")
        if not isinstance(name, str) or not name.startswith(
            self.trial.database_path + "/sessions/"
        ):
            raise Failure("Spanner session response names another database")
        return name

    def write_seed(self, session, numbers):
        """Write the seed rows numbered `numbers` in one single-use commit.

        An insert-or-update of the same values, so a commit repeated after a
        response that never arrived writes nothing new.
        """
        return self._call(
            "POST",
            API + session + ":commit",
            {
                "singleUseTransaction": {"readWrite": {}},
                "mutations": [
                    {
                        "insertOrUpdate": {
                            "table": SEED_TABLE,
                            "columns": ["k", "n"],
                            # INT64 travels as a decimal string.
                            "values": [[row_key(n), str(n)] for n in numbers],
                        }
                    }
                ],
            },
        )

    def query(self, session, sql, read_only, params=None):
        """One single-use read-only query: its rows and its metadata.

        `read_only` is the timestamp bound; only the single-use form is used,
        which the supervisor's role allows without the read API or a
        multi-use snapshot. Every parameter is a STRING.
        """
        body = {"sql": sql, "transaction": {"singleUse": {"readOnly": read_only}}}
        if params:
            body["params"] = params
            body["paramTypes"] = {name: {"code": "STRING"} for name in params}
        response = self._call("POST", API + session + ":executeSql", body)
        rows = response.get("rows", [])
        metadata = response.get("metadata")
        if not isinstance(rows, list) or not isinstance(metadata, dict):
            raise Failure("Spanner query response is malformed")
        return rows, metadata

    def copied(self, session):
        """Whether `batch_out` holds a row, read strongly now."""
        rows, _ = self.query(
            session, f"SELECT 1 FROM {BATCH_TABLE} LIMIT 1", {"strong": True}
        )
        return bool(rows)

    def read_page(self, session, table, after=None, read_timestamp=None):
        """One page of `table` after the primary key `after`, in key order.

        Without `read_timestamp` the page is read strongly and the service's
        read timestamp is returned for every later page to read at, so all
        the tables are read at one snapshot. Returns the rows decoded, the key
        to resume after, or None after a short page, and the read timestamp.
        """
        columns = COLUMNS[table]
        width = KEYS[table]
        params = (
            None if after is None else dict(zip(columns[:width], after, strict=True))
        )
        bound = STRONG if read_timestamp is None else {"readTimestamp": read_timestamp}
        query = page_query(table, first=after is None)
        rows, metadata = self.query(session, query, bound, params)
        if read_timestamp is None:
            read_timestamp = (metadata.get("transaction") or {}).get("readTimestamp")
            if not isinstance(read_timestamp, str):
                raise Failure("Spanner strong query returned no read timestamp")
        try:
            decoded = [_row(columns, row) for row in rows]
        except (TypeError, ValueError) as error:
            raise Failure("Spanner query response is malformed") from error
        resume = decoded[-1][:width] if len(decoded) >= PAGE_ROWS else None
        return decoded, resume, read_timestamp

    def add_split_point(self, key):
        """Split `seed` before `key`; one key per request is all the service takes."""
        return self._call(
            "POST",
            API + self.trial.database_path + ":addSplitPoints",
            {
                "splitPoints": [{"table": SEED_TABLE, "keys": [{"keyParts": [key]}]}],
                "initiator": "tier3",
            },
        )

    def delete(self):
        """Delete the instance; an absent one is already gone."""
        self._call("DELETE", API + self.name, absent=True)
