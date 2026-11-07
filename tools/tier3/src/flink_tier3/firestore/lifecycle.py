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
"""Persist the run's database intent, its readback and its cleanup."""

from __future__ import annotations

from ..common import ApiError, Failure, transient
from ..resource import InstanceLifecycle
from .database import COMMIT_WRITES, LOCATION, READBACK, TYPES
from .trial import MODES


class FirestoreLifecycle(InstanceLifecycle):
    """The run's two databases, from intent to deletion, in the run's control record.

    The runner creates the Native-mode database, then the Datastore-mode one
    under the same call marker, reads each back until it exists, and writes
    each seed, all before the application exists. Neither role may read a
    create's long-running operation, so a database is waited for by reading
    it, as the shared lifecycle waits for an instance. The receipt records
    the databases read back and the seed records the service accepted.
    """

    scenario = "firestore-recovery"

    def _populate(self, api, deadline):
        """Create the second database, read both back and write both seeds."""
        self._created(
            lambda: api.create("datastore"), lambda: api.database("datastore"), deadline
        )
        receipt = {mode: self._readback(api, mode, deadline) for mode in MODES}
        records = self.trial.seed_records
        for mode in MODES:
            for start in range(0, records, COMMIT_WRITES):
                numbers = range(start, min(start + COMMIT_WRITES, records))
                self._settled(
                    lambda mode=mode, numbers=numbers: api.write_seed(mode, numbers),
                    deadline,
                )
        return {"databases": receipt, "seed_records": records}

    def _readback(self, api, mode, deadline):
        """One database once it exists: its type and location must be the run's."""
        while (database := self._settled(lambda: api.database(mode), deadline)) is None:
            if not self._pause(deadline):
                raise Failure("Firestore database did not become readable")
        if (
            database.get("type") != TYPES[mode]
            or database.get("locationId") != LOCATION
        ):
            self.env.emit("firestore-database-deviation", {"database": database})
            raise Failure("Firestore database differs from its configuration")
        return {field: database.get(field) for field in READBACK}

    def delete(self, deadline):
        """Delete both databases; whether neither reads back any more.

        A deletion runs on as a long-running operation, so a database that
        still reads back is left to the caller's next pass, as is a deletion
        refused with 400 or 409 and a call that said nothing.
        """
        api = self.api.with_deadline(deadline)
        try:
            existing = api.get() or {}
            for mode in existing:
                api.delete_database(mode)
            return not existing or api.get() is None
        except Failure as error:
            refused = isinstance(error, ApiError) and error.status in (400, 409)
            if not (refused or transient(error)):
                raise
            self.env.emit("firestore-delete-pending", {"cause": str(error)})
            return False
