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
"""Persist the run's instance intent, its readback and its cleanup."""

from __future__ import annotations

import re

from ..common import ApiError, Failure, transient
from ..resource import InstanceLifecycle
from .instance import RETENTION, TABLES, retryable_commit
from .trial import CHANGE_STREAM

# Seed rows per commit: two mutations each, 20,000 of the 80,000 a commit
# takes; one took about 3 seconds in #1877's measurement.
SEED_COMMIT_ROWS = 10000
# The seed table's split points, which gave its batch read seven partitions
# rather than one at once in #1877's measurement. At 100 processing units the
# service adds one split point a minute and refuses a request over that whole
# with 429; the measurement spaced its requests 65 seconds apart, and none was
# refused.
SPLIT_POINTS = ("4", "8", "c")
SPLIT_POINT_SECONDS = 65
_TABLE = re.compile(r"CREATE TABLE `?(\w+)")
_CHANGE_STREAM = re.compile(r"CREATE CHANGE STREAM `?(\w+)")


class SpannerLifecycle(InstanceLifecycle):
    """The run's instance, from intent to deletion, in the run's control record.

    The runner creates the instance, waits for it to be ready, creates the
    database with its tables, change stream and retention, reads them back,
    writes the seed rows and splits the seed table, all before the
    application exists. Its role cannot read rows, so the receipt records the
    seed rows and split points the service accepted rather than a readback.
    """

    scenario = "spanner-recovery"

    def _populate(self, api, deadline):
        """Create, read back, seed and split the database in the ready instance."""
        self._ready(api.get, deadline)
        self._created(api.create_database, api.database, deadline)
        self._ready(api.database, deadline, "Spanner database")
        receipt = self._readback(api, deadline)
        # A repeat after a lost response leaves a second multiplexed session,
        # which the instance's deletion ends with the first.
        session = self._settled(api.session, deadline)
        rows = self.trial.seed_rows
        for start in range(0, rows, SEED_COMMIT_ROWS):
            numbers = range(start, min(start + SEED_COMMIT_ROWS, rows))
            # An aborted commit, answered 409, is repeated like a silent one.
            self._settled(
                lambda numbers=numbers: api.write_seed(session, numbers),
                deadline,
                retryable_commit,
            )
        for index, key in enumerate(SPLIT_POINTS):
            if index:
                self._wait(SPLIT_POINT_SECONDS, deadline)
            # A 429 within the minute is retried like any other answer that
            # says nothing about the database.
            self._settled(lambda key=key: api.add_split_point(key), deadline)
        return {**receipt, "seed_rows": rows, "split_points": list(SPLIT_POINTS)}

    def _wait(self, seconds, deadline):
        """Wait `seconds`, checking admission at every settle poll."""
        until = self.env.clock() + seconds
        while self.env.clock() < until:
            self.env.admission_open()
            if not self._pause(deadline):
                raise Failure("Spanner split points were not added in time")

    def _readback(self, api, deadline):
        """The names of the database's tables and change stream, and its retention.

        The statements are the service's to print in its own shape, so only
        the names they create are compared, as Bigtable's family names are;
        the retention is what a restored batch read cannot run without.
        """
        database = self._settled(api.database, deadline) or {}
        statements = [
            statement
            for statement in self._settled(api.ddl, deadline) or []
            if isinstance(statement, str)
        ]
        tables = sorted(m[1] for s in statements if (m := _TABLE.match(s)))
        streams = sorted(m[1] for s in statements if (m := _CHANGE_STREAM.match(s)))
        retention = database.get("versionRetentionPeriod")
        if (
            tables != sorted(TABLES)
            or streams != [CHANGE_STREAM]
            or retention != RETENTION
        ):
            self.env.emit(
                "spanner-database-deviation",
                {"database": database, "statements": statements},
            )
            raise Failure("Spanner database differs from its configuration")
        return {
            "database": {
                "tables": tables,
                "change_streams": streams,
                "versionRetentionPeriod": retention,
            }
        }

    def delete(self, deadline):
        """Delete the instance, which takes its database and split points with it.

        #1877's measurement deleted an instance with live split points at the
        first call. A deletion refused with 400 or 409, and a call that said
        nothing, are left to the caller's next pass.
        """
        api = self.api.with_deadline(deadline)
        try:
            if api.get() is None:
                return True
            api.delete()
            return api.get() is None
        except Failure as error:
            refused = isinstance(error, ApiError) and error.status in (400, 409)
            if not (refused or transient(error)):
                raise
            self.env.emit("spanner-delete-pending", {"cause": str(error)})
            return False
