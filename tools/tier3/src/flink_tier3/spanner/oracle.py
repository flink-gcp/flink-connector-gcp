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
"""Judge each table's rows against the run's input and seed, one component at a time.

Each check takes a table's rows in key order, page by page, as the single-use
queries return them, and reports counts and the first few offending keys. The
sink writes one row per input record, and the relay and the batch copy one
row per delivery, which a key's rows count.
"""

from datetime import UTC, datetime, timedelta

from ..common import Failure

# The keys a report names for each kind of deviation, at most.
EXAMPLES = 20
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_MICROSECOND = timedelta(microseconds=1)


def micros(value):
    """An RFC 3339 UTC instant in whole microseconds, as Spanner times commits."""
    if not isinstance(value, str) or not value.endswith("Z"):
        raise Failure("Spanner timestamp is not a UTC instant")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise Failure("Spanner timestamp is not a UTC instant") from error
    return (parsed - _EPOCH) // _MICROSECOND


class _Check:
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


class Sink(_Check):
    """`input`: every record's key holds its sequence, and nothing else is there."""

    def __init__(self, trial):
        super().__init__(trial.row_keys())
        self.counts.update(wrong_value=0)

    def row(self, key, n):
        if n != self.expected[key]:
            self._note("wrong_value", key)

    def report(self, relay):
        """The verdict, with the sink's executions counted from `relay`'s commits.

        Each write the sink executes commits under a time of its own, and
        produces a change record even when it repeats identical values, so a
        key's distinct relayed commit times count its executions. A commit
        the relay missed is not among them, so the count is a lower bound.
        """
        result = super().report(("foreign", "wrong_value"))
        histogram = {}
        for commits in relay.commits.values():
            histogram[len(commits)] = histogram.get(len(commits), 0) + 1
        result["executions"] = {
            "bound": "lower",
            "histogram": {str(n): histogram[n] for n in sorted(histogram)},
            "extra_executions": sum((n - 1) * keys for n, keys in histogram.items()),
        }
        return result


class ChangeStream(_Check):
    """`relay`: every input key was delivered, and no commit after the end.

    A key's rows are its deliveries and their distinct commit times its
    commits, so deliveries beyond its commits are replays, which are counted
    and not failed: the source does not claim exactly-once delivery. The check
    is by key, so a second commit of a key, lost while another commit of the
    key was relayed, is not detected.
    """

    def __init__(self, trial):
        super().__init__(trial.row_keys())
        self.end = micros(trial.change_stream_end)
        self.commits, self.deliveries, self.phases = {}, {}, {}
        self.counts.update(deliveries=0, after_end=0)

    def row(self, key, delivery, commit_micros):
        self.counts["deliveries"] += 1
        self.deliveries[key] = self.deliveries.get(key, 0) + 1
        self.commits.setdefault(key, set()).add(commit_micros)
        # A delivery is named `<phase>/<task instance>/<position>`.
        phase = delivery.split("/", 1)[0]
        self.phases[phase] = self.phases.get(phase, 0) + 1

    def report(self, judged):
        for key in sorted(self.commits):
            if max(self.commits[key]) > self.end:
                self._note("after_end", key)
        result = super().report(("foreign", "after_end"))
        extra = {
            key: count - len(self.commits[key])
            for key, count in self.deliveries.items()
            if count > len(self.commits[key])
        }
        result.update(
            judged=judged,
            replays=sum(extra.values()),
            replayed_keys=len(extra),
            deliveries_by_phase=dict(sorted(self.phases.items())),
        )
        return result


class Seed(_Check):
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


class BatchCopy(_Check):
    """`batch_out`: every seed row copied with its value, and no late row.

    The copy reads `seed` at the snapshot it planned, before the late rows
    were written, so a late key copied is a read after that snapshot. A key's
    rows count its deliveries; those beyond one are replays, which are
    counted and not failed, by the key's first character, the hexadecimal
    digit the split points divide the table by.
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

    def report(self, seed):
        """The verdict over `seed`'s report.

        Without the late rows in `seed`, one absent from the copy proves
        nothing, so the copy fails unless `seed` passed.
        """
        for key in sorted(self.low):
            if not self.low[key] == self.high[key] == self.expected[key]:
                self._note("wrong_value", key)
        result = super().report(("foreign", "late", "wrong_value"))
        extra = {k: n - 1 for k, n in self.deliveries.items() if n > 1}
        by_prefix = {}
        for key, count in extra.items():
            by_prefix[key[0]] = by_prefix.get(key[0], 0) + count
        result.update(
            seed=seed,
            replayed_keys=len(extra),
            extra_deliveries=sum(extra.values()),
            replays_by_prefix=dict(sorted(by_prefix.items())),
        )
        if seed["verdict"] != "pass":
            result["verdict"] = "fail"
        return result
