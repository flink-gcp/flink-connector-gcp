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

from concurrent.futures import ThreadPoolExecutor

from ..common import ApiError, Failure, transient
from ..resource import InstanceLifecycle
from ..rest import Unsent
from ..spanner.instance import retryable_commit
from .database import COMMIT_WRITES, READBACK
from .trial import MODES


class FirestoreLifecycle(InstanceLifecycle):
    """The run's two databases, from intent to deletion, in the run's control record.

    The runner creates the Native-mode database, then at once the
    Datastore-mode one under the same call marker, reads each back until it
    exists, and writes the two seeds side by side, all before the application
    exists: each database takes its own ramp of writes, so the two together
    should take about as long as one (#2135). Neither role may read a create's
    long-running operation, so a database is waited for by reading
    it, as the shared lifecycle waits for an instance, and the marker stays
    until both databases read back: a create whose outcome is unknown could
    still land behind cleanup. The receipt records the databases read back
    and the seed records the service accepted.
    """

    scenario = "firestore-recovery"

    def _landed(self, api, deadline):
        """Create the Datastore-mode database, then read both back.

        A second create whose first attempt was never sent, or was refused by
        the service with a client error, cannot land, so it is left to `_populate` to raise:
        the shared provision then clears the marker, and cleanup deletes the
        Native-mode database alone. Any other failure keeps the marker.
        """
        self.refused = None
        attempts = []

        def create():
            attempts.append(None)
            try:
                api.create("datastore")
            except (Unsent, ApiError) as error:
                # Only the create's own refusal on its first attempt proves
                # that none can land: a retry follows an attempt whose outcome
                # is unknown, and a failed readback says nothing of the create.
                client = isinstance(error, Unsent) or (
                    400 <= error.status < 500 and error.status not in (408, 409, 429)
                )
                if not client or len(attempts) > 1:
                    raise
                self.refused = error

        self._created(create, lambda: api.database("datastore"), deadline)
        self.readback = {"native": self._readback(api, "native", deadline)}
        if self.refused is not None:
            return
        self.readback["datastore"] = self._readback(api, "datastore", deadline)
        self.env.emit("firestore-databases-read-back", {})

    def _populate(self, api, deadline):
        """Write both seeds into the databases read back, the two together.

        Only the commits run on the pool: admission, the pause, the clock and
        the evidence stay on this thread, so admission is read once before
        every pair of commits. A commit that aborted or said nothing is sent
        again while its partner waits at the same chunk.
        """
        if self.refused is not None:
            raise self.refused
        records = self.trial.seed_records
        with ThreadPoolExecutor(max_workers=len(MODES)) as pool:
            for start in range(0, records, COMMIT_WRITES):
                numbers = range(start, min(start + COMMIT_WRITES, records))
                owed = list(MODES)
                while owed:
                    self.env.admission_open()
                    sent = {m: pool.submit(api.write_seed, m, numbers) for m in owed}
                    for mode, commit in sent.items():
                        try:
                            commit.result()
                        except Failure as error:
                            if (
                                not retryable_commit(error)
                                or self.env.clock() >= deadline
                            ):
                                raise
                            continue
                        owed.remove(mode)
                    if owed:
                        self._pause(deadline)
        return {"databases": self.readback, "seed_records": records}

    def _readback(self, api, mode, deadline):
        """One database once it exists, in the fields the receipt keeps."""
        while (database := self._settled(lambda: api.database(mode), deadline)) is None:
            if not self._pause(deadline):
                raise Failure("Firestore database did not become readable")
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
