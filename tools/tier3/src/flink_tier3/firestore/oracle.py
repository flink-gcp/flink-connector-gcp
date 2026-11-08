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
"""Judge each collection against the run's input and seed, one database at a time.

The checks are the Spanner trial's, which take a collection's rows page by
page and report counts and the first few offending keys. Each mode's
collections are checked on their own, the same way: the input sink writes one
document or entity per record, and the batch copy one per delivery, which a
key's copies count.
"""

from ..common import Failure, timestamp, utc
from ..spanner import oracle as spanner
from ..spanner.oracle import Check, Seed

__all__ = ("BatchCopy", "Seed", "Sink")


class Sink(Check):
    """`input`: every record's key holds its sequence, and nothing else is there.

    Only the final state is read: neither database has a change feed in this
    graph, so how often the sink executed a write is not observable.
    """

    def __init__(self, trial):
        super().__init__(trial.record_keys())
        self.counts.update(wrong_value=0)

    def row(self, key, n):
        if n != self.expected[key]:
            self._note("wrong_value", key)

    def report(self):
        return super().report(("foreign", "wrong_value"))


class BatchCopy(spanner.BatchCopy):
    """`batch_out`: every seed record copied with its value, and no late record.

    The copy reads `seed` at the read time it planned, before the late
    records were written, so a late key copied is a read after that time. A
    key's copies beyond one are deliveries emitted after a restored
    checkpoint, which are counted and not failed. The copies' update times
    give the read's span, which the service bounds: it refuses a read time
    older than about an hour.
    """

    def __init__(self, trial):
        super().__init__(trial)
        self.phases = {}
        self.first, self.last = None, None

    def row(self, key, delivery, n, updated):
        # A copy without its sequence is a wrong value, never a missing one.
        super().row(key, delivery, -1 if n is None else n)
        # A delivery is named `<phase>:<task instance>:<position>`.
        phase = str(delivery).split(":", 1)[0]
        self.phases[phase] = self.phases.get(phase, 0) + 1
        try:
            at = timestamp(updated)
        except Failure:
            return
        self.first = at if self.first is None else min(self.first, at)
        self.last = at if self.last is None else max(self.last, at)

    def report(self, seed, read_time=None):
        """The verdict over `seed`'s report, with the read's span.

        `read_time` is the one the read planned, when the JobManager's plan
        line was read; the span then runs from it, and otherwise from the
        first copy, to the last copy.
        """
        result = super().report(seed)
        # The Spanner split points' prefixes divide nothing here.
        result.pop("replays_by_prefix")
        start, since = self.first, "first_copy"
        if read_time is not None:
            try:
                start, since = timestamp(read_time), "read_time"
            except Failure:
                pass
        result.update(
            deliveries_by_phase=dict(sorted(self.phases.items())),
            first_copy_at=None if self.first is None else utc(self.first),
            last_copy_at=None if self.last is None else utc(self.last),
            read_seconds=(
                None
                if start is None or self.last is None
                else round(self.last - start, 3)
            ),
            read_seconds_from=since,
        )
        return result
