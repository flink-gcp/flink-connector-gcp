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
"""Firestore REST operations on one run's two databases, without admission.

The Native-mode database is read and written through the Firestore API and
the Datastore-mode one through the Datastore API, which names the database in
each request body. The lifecycle image carries neither client library, so both
are plain JSON calls.
"""

from __future__ import annotations

from ..common import Failure
from ..policy import MIB, PROJECT
from ..rest import TrialService
from .trial import BATCH, MODES, SEED, Trial, record_key

API = "firestore.googleapis.com/v1/"
DATASTORE_API = "datastore.googleapis.com/v1/"
LOCATION = "us-central1"
TYPES = {"native": "FIRESTORE_NATIVE", "datastore": "DATASTORE_MODE"}
# What the runner creates: a Standard-edition database of each mode, without
# point-in-time recovery or delete protection, so the supervisor can delete it.
SETTINGS = {
    "locationId": LOCATION,
    "databaseEdition": "STANDARD",
    "pointInTimeRecoveryEnablement": "POINT_IN_TIME_RECOVERY_DISABLED",
    "deleteProtectionState": "DELETE_PROTECTION_DISABLED",
}
# The database fields the receipt keeps; the type and the location are the
# ones the readback requires.
READBACK = ("type", *SETTINGS)
# The writes one commit takes in either mode.
COMMIT_WRITES = 500
# An oracle page: at most this many documents or entities, each well under a
# kilobyte as the service prints it.
PAGE = 2000
MAX_RESPONSE_BYTES = 8 * MIB
# The Datastore API's answers that more results follow the batch.
MORE = frozenset(
    ("NOT_FINISHED", "MORE_RESULTS_AFTER_LIMIT", "MORE_RESULTS_AFTER_CURSOR")
)


def _value(fields, name):
    """An integer or string field of a document or entity, or None."""
    value = fields.get(name) if isinstance(fields, dict) else None
    if not isinstance(value, dict):
        return None
    if "integerValue" in value:
        try:
            return int(value["integerValue"])
        except (TypeError, ValueError):
            return None
    text = value.get("stringValue")
    return text if isinstance(text, str) else None


def _integer(n):
    # INT64 travels as a decimal string in both APIs.
    return {"integerValue": str(n)}


class Databases(TrialService):
    """No automatic retries; the caller owns admission, intent and ownership.

    Inject a Google authorized session with retries disabled. The absolute UTC
    deadline bounds starting each call and reading its response. Every write
    sets fixed values without a precondition, so one repeated after a
    response that never arrived writes nothing new.
    """

    label = "Firestore"
    base = "https://"
    max_response_bytes = MAX_RESPONSE_BYTES
    trial_type = Trial

    @staticmethod
    def resource_name(trial):
        return [trial.database_path(mode) for mode in MODES]

    def database(self, mode):
        """One mode's database, or None when it does not exist."""
        return self._call("GET", API + self.trial.database_path(mode), absent=True)

    def get(self):
        """The databases that exist, by mode, or None when neither does."""
        found = {}
        for mode in MODES:
            database = self.database(mode)
            if database is not None:
                found[mode] = database
        return found or None

    def create(self, mode="native"):
        """Start creating one mode's database; returns the operation."""
        return self._call(
            "POST",
            API + f"projects/{PROJECT}/databases",
            {"type": TYPES[mode], **SETTINGS},
            params={"databaseId": self.trial.database(mode)},
        )

    def delete_database(self, mode):
        """Start deleting one mode's database; an absent one is already gone."""
        self._call("DELETE", API + self.trial.database_path(mode), absent=True)

    def commit(self, mode, collection, numbers):
        """Set the records numbered `numbers` in `collection`; returns the commit time."""
        if mode == "native":
            documents = self.trial.database_path(mode) + "/documents"
            response = self._call(
                "POST",
                API + documents + ":commit",
                {
                    "writes": [
                        {
                            "update": {
                                "name": f"{documents}/{collection}/{record_key(n)}",
                                "fields": {"n": _integer(n)},
                            }
                        }
                        for n in numbers
                    ]
                },
            )
        else:
            database = self.trial.database(mode)
            partition = {"projectId": PROJECT, "databaseId": database}
            response = self._call(
                "POST",
                DATASTORE_API + f"projects/{PROJECT}:commit",
                {
                    "databaseId": database,
                    "mode": "NON_TRANSACTIONAL",
                    "mutations": [
                        {
                            "upsert": {
                                "key": {
                                    "partitionId": partition,
                                    "path": [
                                        {"kind": collection, "name": record_key(n)}
                                    ],
                                },
                                "properties": {"n": _integer(n)},
                            }
                        }
                        for n in numbers
                    ],
                },
            )
        committed = response.get("commitTime")
        if not isinstance(committed, str):
            raise Failure("Firestore commit returned no commit time")
        return committed

    def write_seed(self, mode, numbers):
        """Write the seed records numbered `numbers` in one commit."""
        return self.commit(mode, SEED, numbers)

    def copied(self, mode):
        """Whether one mode's `batch_out` holds a copy, read now."""
        found, _, _ = self._query(mode, BATCH, 1)
        return bool(found)

    def read_page(self, mode, collection, after=None, read_time=None):
        """One page of `collection` after the cursor `after`, in key order.

        Without `read_time` the page is read now and the service's read time
        is returned for every later page to read at, so all of a database's
        collections are read at one time. Returns the rows decoded, the cursor
        to resume after or None at the end, and the read time. A row is the
        key and `n`, and a copy's row the fields `k`, `delivery` and `n` with
        the copy's update time.
        """
        found, after, read_time = self._query(mode, collection, PAGE, after, read_time)
        rows = []
        for key, fields, updated in found:
            if collection == BATCH:
                rows.append(
                    (
                        _value(fields, "k") or key,
                        _value(fields, "delivery"),
                        _value(fields, "n"),
                        updated,
                    )
                )
            else:
                rows.append((key, _value(fields, "n")))
        return rows, after, read_time

    def _query(self, mode, collection, limit, after=None, read_time=None):
        """The documents or entities found, the resume cursor and the read time.

        Each one found is its key, its fields and its update time.
        """
        try:
            if mode == "native":
                found, after, read = self._native(collection, limit, after, read_time)
            else:
                found, after, read = self._datastore(
                    collection, limit, after, read_time
                )
        except (AttributeError, KeyError, IndexError, TypeError) as error:
            raise Failure("Firestore query response is malformed") from error
        if not isinstance(read, str):
            raise Failure("Firestore query returned no read time")
        return found, after, read

    def _native(self, collection, limit, after, read_time):
        documents = self.trial.database_path("native") + "/documents"
        query = {
            "from": [{"collectionId": collection}],
            "orderBy": [{"field": {"fieldPath": "__name__"}, "direction": "ASCENDING"}],
            "limit": limit,
        }
        if after is not None:
            query["startAt"] = {"values": [{"referenceValue": after}], "before": False}
        body = {"structuredQuery": query}
        if read_time is not None:
            body["readTime"] = read_time
        # The method streams its answers, which REST returns as one array.
        answers = self._call("POST", API + documents + ":runQuery", body, shape=list)
        found = []
        for answer in answers:
            read_time = read_time or answer.get("readTime")
            document = answer.get("document")
            if document is not None:
                found.append(
                    (
                        document["name"].rsplit("/", 1)[1],
                        document.get("fields", {}),
                        document.get("updateTime"),
                    )
                )
        resume = (
            f"{documents}/{collection}/{found[-1][0]}" if len(found) >= limit else None
        )
        return found, resume, read_time

    def _datastore(self, collection, limit, after, read_time):
        database = self.trial.database("datastore")
        query = {
            "kind": [{"name": collection}],
            "order": [{"property": {"name": "__key__"}, "direction": "ASCENDING"}],
            "limit": limit,
        }
        if after is not None:
            query["startCursor"] = after
        body = {
            "databaseId": database,
            "partitionId": {"projectId": PROJECT, "databaseId": database},
            "query": query,
        }
        if read_time is not None:
            body["readOptions"] = {"readTime": read_time}
        batch = self._call(
            "POST", DATASTORE_API + f"projects/{PROJECT}:runQuery", body
        )["batch"]
        found = [
            (
                result["entity"]["key"]["path"][-1]["name"],
                result["entity"].get("properties", {}),
                result.get("updateTime"),
            )
            for result in batch.get("entityResults", [])
        ]
        resume = batch["endCursor"] if batch.get("moreResults") in MORE else None
        return found, resume, read_time or batch.get("readTime")
