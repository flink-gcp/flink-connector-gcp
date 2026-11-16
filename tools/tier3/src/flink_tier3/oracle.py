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
"""The checks the Spanner and Firestore scenarios judge their tables by.

Each check takes a table's rows in key order, page by page, as the
scenario's reads return them, and reports counts and the first few offending
keys. A scenario's own checks build on these.
"""

# The keys a report names for each kind of deviation, at most.
EXAMPLES = 20


class Check:
    """Counts and examples shared by the checks."""

    def __init__(self, expected):
        self.expected = expected
        self.matched = set()
        self.counts = {"rows": 0, "foreign": 0}
        self.examples = {}

    def _note(self, kind, key):
        self.counts[kind] = self.counts.get(kind, 0) + 1
        examples = self.examples.setdefault(kind, [])
        if len(examples) < EXAMPLES:
            examples.append(key)

    def add(self, rows):
        for key, *values in rows:
            self.counts["rows"] += 1
            if key not in self.expected:
                self.other(key)
                continue
            self.matched.add(key)
            self.row(key, *values)

    def other(self, key):
        self._note("foreign", key)

    def row(self, key, *values):
        raise NotImplementedError

    def report(self, failing):
        missing = [key for key in self.expected if key not in self.matched]
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


class Seed(Check):
    """`seed`: the runner's rows and the supervisor's late ones, each its own value.

    The table's key is the row's, so with none missing and none foreign it
    holds exactly the seed and the late rows.
    """

    def __init__(self, trial):
        super().__init__({**trial.seed_keys(), **trial.late_keys()})
        self.counts.update(wrong_value=0)

    def row(self, key, n):
        if n != self.expected[key]:
            self._note("wrong_value", key)

    def report(self):
        return super().report(("foreign", "wrong_value"))


class BatchCopy(Check):
    """`batch_out`: every seed row copied with its value, and no late row.

    The copy reads `seed` at the snapshot it planned, before the late rows
    were written, so a late key copied is a read after that snapshot. A key's
    rows count its deliveries; those beyond one are replays, which are
    counted and not failed.
    """

    def __init__(self, trial):
        super().__init__(trial.seed_keys())
        self.late = set(trial.late_keys())
        self.deliveries, self.low, self.high = {}, {}, {}
        self.counts.update(wrong_value=0, late=0)

    def other(self, key):
        self._note("late" if key in self.late else "foreign", key)

    def row(self, key, delivery, n):
        self.deliveries[key] = self.deliveries.get(key, 0) + 1
        self.low[key] = min(self.low.get(key, n), n)
        self.high[key] = max(self.high.get(key, n), n)

    def extra(self):
        """Each replayed key's deliveries beyond its first."""
        return {k: n - 1 for k, n in self.deliveries.items() if n > 1}

    def report(self, seed):
        """The verdict over `seed`'s report.

        Without the late rows in `seed`, one absent from the copy proves
        nothing, so the copy fails unless `seed` passed.
        """
        for key in sorted(self.low):
            if not self.low[key] == self.high[key] == self.expected[key]:
                self._note("wrong_value", key)
        result = super().report(("foreign", "late", "wrong_value"))
        extra = self.extra()
        result.update(
            seed=seed,
            replayed_keys=len(extra),
            extra_deliveries=sum(extra.values()),
        )
        if seed["verdict"] != "pass":
            result["verdict"] = "fail"
        return result
