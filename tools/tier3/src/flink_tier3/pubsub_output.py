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
"""The supervisor's reading of the relay's independent output subscription."""

import base64
import binascii
import re
import uuid

from .common import Failure
from .pubsub_messages import MAX_BATCH, _encode, cohort_ranges

PHASES = ("initial", "upgrade")
DECIMAL = re.compile(r"0|[1-9][0-9]{0,4}")


def decode(text):
    """Canonical unpadded base64url as UTF-8, or None."""
    if not isinstance(text, str) or not re.fullmatch(r"[A-Za-z0-9_-]*", text):
        return None
    try:
        value = base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))
        decoded = value.decode("utf-8")
    except (ValueError, binascii.Error, UnicodeDecodeError):
        return None
    return decoded if _encode(value) == text else None


def canonical_uuid(text):
    try:
        return str(uuid.UUID(text)) == text
    except ValueError:
        return False


def parse(line, run_id, records):
    """One collected TSV line as the relay's observation, or None.

    The application writes nine fields: version, run, input index, sequence,
    the input message ID, the processing attempt, a fresh observation ID,
    phase and whether the attempt restored state. Anything else, including a
    payload of another run, is not this relay's output; the collector keeps
    its line in the evidence for the oracle to refuse.
    """
    parts = line.split("\t")
    if len(parts) != 2:
        return None
    message_id, payload = (decode(part) for part in parts)
    if not message_id or payload is None:
        return None
    fields = payload.split("|")
    if len(fields) != 9:
        return None
    input_message_id = decode(fields[4])
    if (
        fields[0] != "v1"
        or fields[1] != run_id
        or fields[2] not in ("0", "1")
        or not DECIMAL.fullmatch(fields[3])
        or int(fields[3]) >= records
        or not input_message_id
        or not canonical_uuid(fields[5])
        or not canonical_uuid(fields[6])
        or fields[7] not in PHASES
        or fields[8] not in ("true", "false")
    ):
        return None
    return {
        "output_message_id": message_id,
        "input_index": int(fields[2]),
        "sequence": int(fields[3]),
        "input_message_id": input_message_id,
        "attempt": fields[5],
        "observation": fields[6],
        "phase": fields[7],
        "restored": fields[8] == "true",
    }


class OutputCollector:
    """Pull the output subscription through the supervisor's handoff.

    Every pull goes through the shared reservations and leaves the message
    helper's evidence. The parsed observations are kept in this process for
    the exercise; they are not the evidence, and a replacement supervisor,
    which never collects, does not rebuild them. Batch IDs carry a random
    prefix per collector, because the evidence they name is create-only, so
    two collectors collide only if their prefixes do.
    """

    def __init__(self, handoff):
        self.handoff, self.env = handoff, handoff.env
        self.run_id = self.env.approval.run_id
        self.records = handoff.traffic.records
        self.prefix = "out-" + uuid.uuid4().hex[:8]
        self.batches = 0
        self.observations = []
        # Lines that are not this relay's output, by batch.
        self.foreign = []
        # Every collected line in collection order, repeats included, which
        # is the oracle's input.
        self.lines = []

    def pull(self):
        """Pull one batch; return how many messages it received."""
        self.batches += 1
        batch = f"{self.prefix}-{self.batches:06d}"
        result = self.handoff.collect(batch)
        at = self.env.clock()
        lines = result["tsv"].splitlines()
        if len(lines) != result["count"]:
            raise Failure("Collected Pub/Sub observations differ from their count")
        self.lines.extend(lines)
        for line in lines:
            parsed = parse(line, self.run_id, self.records)
            if parsed is None:
                self.foreign.append({"batch": batch, "line": line})
            else:
                self.observations.append({**parsed, "batch": batch, "at": at})
        return result["count"]

    def drain(self, max_pulls):
        """Pull until a batch comes back short or ``max_pulls`` are spent.

        A short or empty pull does not prove the subscription empty; it only
        ends this round.
        """
        if type(max_pulls) is not int or max_pulls < 1:
            raise ValueError("A drain needs at least one pull")
        received = 0
        for _ in range(max_pulls):
            count = self.pull()
            received += count
            if count < MAX_BATCH:
                break
        return received

    def processed(self, input_index, start, count, *, where=None):
        """The sequences of one input range seen in output, optionally filtered."""
        return {
            item["sequence"]
            for item in self.observations
            if item["input_index"] == input_index
            and start <= item["sequence"] < start + count
            and (where is None or where(item))
        }

    def complete(self, name, *, where=None):
        """Whether every sequence of a named cohort was seen on both inputs."""
        cohort = cohort_ranges(self.records)[name]
        return all(
            len(self.processed(index, cohort["start"], cohort["count"], where=where))
            == cohort["count"]
            for index in range(2)
        )
