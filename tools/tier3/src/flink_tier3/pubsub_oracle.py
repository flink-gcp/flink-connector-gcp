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
"""The relay's offline output oracle, ported for the supervisor.

``PubSubRecoveryReport.java`` decides completeness and duplicate populations
over collected output, and the emulator integration test uses it; the
supervisor's image carries no JVM, so the verdict the receipt waits on runs
this port instead. Both decide the same cases from the relay's test
resources, so a rule changed on one side only fails the other's run of them.
Where a rejection's message is the report's own, the port uses it, so a case
can name the rejection it expects; where the JDK writes it, both only refuse.
"""

from .common import Failure
from .pubsub_output import PHASES, canonical_uuid, decode

# The report's bounds, in the UTF-16 units Java's String.length() counts.
MAX_LINES = 200000
MAX_ID = 256
MAX_PAYLOAD = 2048
MAX_LINE = 4096
COUNTERS = (
    "logical_inputs",
    "input_publication_duplicates",
    "repeated_input_processing",
    "output_publication_duplicates",
    "repeated_output_delivery",
)


def _units(text):
    return len(text.encode("utf-16-le")) // 2


def _consistent(values, key, value):
    if values.setdefault(key, value) != value:
        raise Failure("One evidence identity names conflicting records")


class Report:
    """Completeness and duplicate accounting over one run's collected output."""

    def __init__(self, run_id, records):
        self.run_id, self.records = run_id, records
        self.output_messages = {}
        self.observations = {}
        self.attempts = {}
        self.input_messages = {}
        self.logical_inputs = set()
        self.lines = 0

    def _input(self, logical):
        """RecoveryPayload.parseInput: this run's canonical input identity."""
        version, run_id, index, sequence = logical.split("|")
        if version != "v1" or run_id != self.run_id:
            raise Failure("Input must belong to this version and run")
        try:
            index, sequence = int(index), int(sequence)
        except ValueError:
            raise Failure("Input identity must use canonical decimal fields") from None
        if index not in (0, 1):
            raise Failure("Input index must be 0 or 1")
        if not 0 <= sequence < self.records:
            raise Failure("Sequence is outside this run's logical input domain")
        if logical != f"v1|{self.run_id}|{index}|{sequence}":
            raise Failure("Input identity must use canonical decimal fields")
        return index

    def accept(self, output_message_id, payload):
        """Account for one collected output message, or refuse the evidence."""
        self.lines += 1
        if (
            self.lines > MAX_LINES
            or not output_message_id
            or _units(output_message_id) > MAX_ID
            or _units(payload) > MAX_PAYLOAD
        ):
            raise Failure("Evidence exceeds its bound or lacks an output message ID")
        fields = payload.split("|")
        if (
            len(fields) != 9
            or fields[7] not in PHASES
            or fields[8] not in ("true", "false")
            or (fields[7] == "upgrade" and fields[8] != "true")
        ):
            raise Failure("Invalid output observation")
        logical = "|".join(fields[:4])
        index = self._input(logical)
        input_message_id = decode(fields[4])
        if input_message_id is None:
            raise Failure("Noncanonical UTF-8/base64 field")
        if not input_message_id:
            raise Failure("Input message ID is missing")
        if not (canonical_uuid(fields[5]) and canonical_uuid(fields[6])):
            raise Failure("Attempt and observation IDs must be UUIDs")
        _consistent(self.output_messages, output_message_id, payload)
        _consistent(self.observations, fields[6], payload)
        _consistent(self.attempts, fields[5], fields[7] + "|" + fields[8])
        _consistent(self.input_messages, f"{index}|{fields[4]}", logical)
        self.logical_inputs.add(logical)

    def accept_line(self, line):
        """One collected TSV line: base64url message ID, a tab, base64url payload."""
        if _units(line) > MAX_LINE:
            raise Failure("Evidence line exceeds 4096 characters")
        parts = line.split("\t")
        if len(parts) != 2:
            raise Failure(
                "Expected bounded base64url ID and payload separated by a tab"
            )
        decoded = [decode(part) for part in parts]
        if None in decoded:
            raise Failure("Noncanonical UTF-8/base64 field")
        self.accept(*decoded)

    def counters(self):
        """The five populations over the evidence accepted so far."""
        logical = len(self.logical_inputs)
        inputs = len(self.input_messages)
        observations = len(self.observations)
        outputs = len(self.output_messages)
        return dict(
            zip(
                COUNTERS,
                (
                    logical,
                    inputs - logical,
                    observations - inputs,
                    outputs - observations,
                    self.lines - outputs,
                ),
                strict=True,
            )
        )

    def complete(self):
        """The counters, once every logical input of both subscriptions appeared."""
        expected = 2 * self.records
        if len(self.logical_inputs) != expected:
            raise Failure(
                f"Missing logical inputs: expected={expected} "
                f"observed={len(self.logical_inputs)}"
            )
        return self.counters()


def reconcile(lines, run_id, records):
    """The oracle's account of collected TSV lines, as a record field.

    A rejection names the first line refused and carries no counters, because
    the report stops there and what it counted before is not the evidence's
    account. An accepted run reports how many logical inputs are missing
    rather than refusing, so that a verdict can say which of the two failed.
    """
    report = Report(run_id, records)
    for number, line in enumerate(lines, 1):
        try:
            report.accept_line(line)
        except Failure as error:
            return {
                "lines": len(lines),
                "rejected": str(error),
                "rejected_line": number,
            }
    return {
        "lines": len(lines),
        "rejected": None,
        "missing_inputs": 2 * records - len(report.logical_inputs),
        **report.counters(),
    }
