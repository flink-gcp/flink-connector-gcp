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
"""Judge each table's rows against the run's input, one component at a time.

Each check takes a table's rows in key order, page by page, as ReadRows
returns them, and reports counts and the first few offending row keys. The
sinks write one row per input record, so a row is an input identity.
"""

from .trial import MARKER_FAMILY, RELAY_FAMILY, SUM_FAMILY, SUM_QUALIFIER

# The row keys a report names for each kind of deviation, at most.
EXAMPLES = 20


def _sum(cells):
    """The row's `agg:count` SUM, or None unless it is one 8-byte cell."""
    sums = [cell for cell in cells if cell[0] == SUM_FAMILY]
    if len(sums) != 1 or sums[0][1] != SUM_QUALIFIER or len(sums[0][3]) != 8:
        return None
    return int.from_bytes(sums[0][3], "big", signed=True)


class _Check:
    """Counts and examples shared by the three checks."""

    families = frozenset()

    def __init__(self, trial):
        self.expected = trial.row_keys()
        self.matched = set()
        self.counts = {"rows": 0, "foreign": 0, "foreign_families": 0}
        self.examples = {}

    def _note(self, kind, key):
        self.counts[kind] = self.counts.get(kind, 0) + 1
        examples = self.examples.setdefault(kind, [])
        if len(examples) < EXAMPLES:
            examples.append(key)

    def add(self, rows):
        for key, cells in rows:
            self.counts["rows"] += 1
            if key not in self.expected:
                self._note("foreign", key)
                continue
            self.matched.add(key)
            if any(cell[0] not in self.families for cell in cells):
                self._note("foreign_families", key)
            self.row(key, cells)

    def row(self, key, cells):
        raise NotImplementedError

    def report(self, failing, wanted=None):
        wanted = self.expected.keys() if wanted is None else wanted
        missing = [key for key in wanted if key not in self.matched]
        result = {
            **self.counts,
            "matched": len(self.matched),
            "missing": len(missing),
            "examples": {**self.examples, "missing": sorted(missing)[:EXAMPLES]},
        }
        result["verdict"] = (
            "fail" if missing or any(result.get(kind) for kind in failing) else "pass"
        )
        return result


class ExactlyOnce(_Check):
    """`eo`: each row's SUM is its one contribution, beside exactly one marker."""

    families = frozenset((SUM_FAMILY, MARKER_FAMILY))

    def __init__(self, trial):
        super().__init__(trial)
        # The identities whose contribution the sink applied, which the relay
        # must have delivered.
        self.applied = set()

    def row(self, key, cells):
        total = _sum(cells)
        if total is not None and total >= 1:
            self.applied.add(key)
        if total != 1:
            self._note("wrong_sum", key)
        if sum(cell[0] == MARKER_FAMILY for cell in cells) != 1:
            self._note("wrong_markers", key)

    def report(self):
        return super().report(
            ("foreign", "foreign_families", "wrong_sum", "wrong_markers")
        )


class AtLeastOnce(_Check):
    """`alo`: every identity present; contributions beyond one are counted."""

    families = frozenset((SUM_FAMILY,))

    def __init__(self, trial):
        super().__init__(trial)
        self.counts.update(extra_contributions=0, rows_with_extra=0)

    def row(self, key, cells):
        total = _sum(cells)
        if total is None or total < 1:
            self._note("wrong_sum", key)
        elif total > 1:
            self.counts["extra_contributions"] += total - 1
            self.counts["rows_with_extra"] += 1

    def report(self):
        return super().report(("foreign", "foreign_families", "wrong_sum"))


class Relay(_Check):
    """`relay`: every identity `eo` applied was relayed; replays are counted.

    Each delivery is a cell of its own, so a row's cells count its
    deliveries, and a row of an identity `eo` did not apply is reported as
    unapplied. No delivery count above one fails the check: the source does
    not claim exactly-once delivery.
    """

    families = frozenset((RELAY_FAMILY,))

    def __init__(self, trial):
        super().__init__(trial)
        self.counts.update(deliveries=0, replayed_rows=0, extra_deliveries=0)

    def row(self, key, cells):
        deliveries = sum(cell[0] == RELAY_FAMILY for cell in cells)
        self.counts["deliveries"] += deliveries
        if deliveries > 1:
            self.counts["replayed_rows"] += 1
            self.counts["extra_deliveries"] += deliveries - 1

    def report(self, applied, judged):
        result = super().report(("foreign", "foreign_families"), applied)
        unapplied = sorted(self.matched - set(applied))
        result.update(
            unapplied=len(unapplied),
            judged=judged,
            verdict=result["verdict"] if not unapplied else "fail",
        )
        result["examples"]["unapplied"] = unapplied[:EXAMPLES]
        return result
