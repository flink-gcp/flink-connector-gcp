<!--
Copyright 2026 laughingman7743

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# ADR-0068: A `Duration` budget is bounded at the setter by what a nanosecond clock can express

- Status: Accepted
- Date: 2026-08-08
- Issues: [#334], [#321], [#333]
- Modules: base (`BoundedShutdown`), pubsub, bigquery
- Current behavior: each knob's row in `docs/content/docs/reference/{pubsub,bigquery}.md`

## Context

A `Duration` is unbounded for practical purposes (±292 billion years); a `long` of nanoseconds is
not (±292 years). Every budget this repository spends is converted with `Duration.toNanos()`
somewhere, and that conversion throws `ArithmeticException` — never at the setter that accepted
the value, always later, on a TaskManager, in code the user is not standing in front of. The
worst landing site is a teardown: an exception out of `close()` reaches Flink's teardown path, not
a caller's `try`.

It is not a value anyone types by accident. What makes it worth closing is that three of the four
knobs below invite it: each is documented as taking a very long `Duration` to mean "effectively
unbounded" or "never evict". [#321] bounded `PubSubDeadLetterQueue`'s two budgets for exactly that
reason and [#333] bounded `publishProgressTimeout`; both left [#334] behind, and the survey done
for [#334] found the rule had already been missed twice more.

The tree measured 2026-08-08 at `origin/main` `07a11a3`:

| Budget | Conversion | Where an over-long value threw | Bound before this ADR |
|---|---|---|---|
| `PubSubPublisherOptions.shutdownTimeout` | `BoundedShutdown.start()` | TaskManager, writer teardown | none |
| `PubSubSubscriberOptions.firstCheckpointTimeout` | `MissingCheckpointDetector`'s constructor | TaskManager, reader construction | non-negative only |
| `DefaultStreamOptions.destinationIdleTimeout` | `BigQueryDefaultStreamWriter`'s constructor | TaskManager, writer construction | positive only |
| `PubSubSubscriberOptions.shutdownTimeout` | `toMillis()`, not `toNanos()` | nowhere in practice — see below | positive only |

The fourth is the one [#334] named and the one with no crash: the reader's wait is a
`CountDownLatch`, whose `await(long, TimeUnit)` saturates rather than throwing, and `toMillis()`
overruns only past ~292 **million** years.

## Decision

**A `Duration` a connector will convert to nanoseconds is rejected, at the setter that accepts it,
past `Duration.ofNanos(Long.MAX_VALUE)`.** All four knobs above now carry it, the fourth included:
it is the same knob name as the sink's, documented the same way, and one answer for both beats a
divergence a user would have to discover.

**`BoundedShutdown`'s constructor checks the same bound, as well as the setters.** The setter is
where the failure belongs — it is the client, and the value is still in the user's hand — but a
setter cannot be a class invariant. `BoundedShutdown` is shared, `@Internal`, and reached by two
connectors today; a third consumer building a budget in code passes no setter at all, and the
precondition it would otherwise inherit silently is the one this ADR exists to close. It is a
`checkArgument` on the `Duration` and not a conversion, so `start()` keeps its single
`toNanos()`: the check states the precondition without moving the arithmetic.

**The deadline arithmetic at that ceiling is left exactly as it was, and each site now says why.**
`System.nanoTime() + budget.toNanos()` does overflow at the maximum, which is what made this look
like a second defect — and it is benign: the subtraction that reads the deadline wraps a second
time, and the two cancel to the true remainder (Evidence below). Both sites keep the deadline
form, with a comment and a test naming the change that *would* break it — a `Math.addExact` or a
clamp added later to "harden" the stamp, which would turn the documented way of saying
"effectively unbounded" into an exception or into the setting that waits least.

**What a SQL user is shown**, measured 2026-08-08 through `FactoryMocks.createTableSink` with
`sink.shutdown-timeout = '400000 d'` (a throwaway probe, deleted after the run):

```text
ValidationException: Unable to create a sink for writing table 'default.default.t1'.
  caused by IllegalArgumentException: shutdownTimeout must be at most PT2562047H47M16.854775807S
```

The actionable sentence reaches the user in the cause, and `shutdownTimeout` maps onto
`sink.shutdown-timeout` readably — which is what settles the "restate it in DDL keys" question
below, and it is also what showed the raw `Duration` to be unreadable on its own.

## Alternatives declined

- **Saturating instead of rejecting** (`Long.MAX_VALUE` nanoseconds is 292 years; nothing
  distinguishes that from what the caller meant). The cheapest answer and arguably the most
  generous reading, declined because it silently accepts a value nobody meant to type: a
  `Duration.ofDays(400_000)` in a job graph is a mistake worth reporting, and the setter is where
  reporting it costs the user nothing.
- **Checking only in `BoundedShutdown`'s constructor** ([#334]'s candidate 2), which covers every
  consumer of that class at once. Declined as the whole answer: it moves the failure from the
  setter to the object that consumes it — still a TaskManager — and it covers exactly one of the
  four conversions, leaving the two constructor-side ones and the `toMillis()` one untouched.
- **Checking only at the setters** ([#334]'s candidate 1), which is what the issue proposed. Kept
  as half the answer rather than all of it, for the class-invariant reason above.
- **A shared `checkExpressibleInNanos` helper in `flink-connector-gcp-base`**, collapsing the
  three module-local ceiling constants into one definition. Declined for now against the base
  module's own bar (ADR-0036: a type moves in once it has multiple consumers *and* the move is
  argued): a new `@Internal` type crossing a module boundary to hold one `checkArgument` is a
  larger decision than the defect, and the two existing precedents ([#321], [#333]) each spell the
  check out locally. Within the Pub/Sub source the sharing that already exists is reused —
  `OptionChecks.checkExpressibleInNanos` serves both of its knobs.
- **Restating the bound in the Table API mappers.** ADR-0007's rule is that a check whose message
  names Java setters needs restating in DDL keys. Declined on the measurement above rather than on
  the "unreachable in practice" reasoning [#321] and [#333] used: the message does reach the SQL
  user in the cause, and `shutdownTimeout` is legible as `sink.shutdown-timeout` — unlike
  ADR-0007's `retryTotalTimeout`, which appears nowhere in a `WITH` clause and is why that rule
  exists. What the measurement *did* change is the message itself, above.

**The rejection message names the year count, not only the `Duration`.** `Duration.toString()`
renders the ceiling as `PT2562047H47M16.854775807S`; nobody reads "292 years" out of an hour count
with a fractional second on it. Every ceiling message in the repository therefore ends `(about 292
years)` — the eight sites include the two [#321] and [#333] had already shipped, so one knob's
message does not read differently from the next — and tests pin the year count so that removing it
fails rather than quietly making the message unreadable. This was found by measuring what a SQL
user sees, which is the one place the message is *all* they get (Evidence).

## Evidence

**The deadline overflow is benign, and this was measured after a rewrite had already been written
to fix it.** The plan for this change called the overflow a second defect — the largest accepted
budget would be "accepted and then spent instantly" — and replaced both deadline computations with
an elapsed-subtracted-from-budget form. The mutation batch disproved it: restoring the deadline
form in either site left every test green. A JVM probe (macOS, JDK 17, 2026-08-08, one run) says
why:

```text
nanoTime=1868160800788875
deadline (overflowed)=-9221503876053986934
remaining=9223372036854775474      // 106751 days
elapsed-form remaining=9223372036854775474
equal? true
```

The true remainder is representable by construction — at most the budget, and once the budget is
spent no more negative than the time elapsed since the stamp, so `|r| ≤ Long.MAX_VALUE` on both
sides of zero — and two's-complement subtraction returns it whatever the intermediate wrapped to.
(The negative half matters: both call sites read the remainder after it has expired, and clamp
with `Math.max(…, 0)` rather than relying on the sign never appearing.) The
rewrite was reverted and the two tests kept, re-aimed at the mutation that would actually break
the boundary. The general form, worth keeping because it reads as a bug in review: **a `nanoTime`
deadline that overflows is still correct, and "fixing" it with `Math.addExact` is what introduces
the failure.**

## Consequences

- Every affected knob's javadoc `@param` now states the ceiling, and BigQuery's "to never evict,
  set a very large duration" — in the javadoc, `reference/bigquery.md` and the DataStream page —
  names how large is large enough. That sentence is the reason this knob mattered: it is an
  instruction to do the thing that used to fail the job as it started.
- What the check does **not** cover, deliberately: `flink-connector-gcp-test-utils`' own
  `toNanos()` call sites (`Awaits`, `Drains`, `PubSubSplitReaders`, `PubSubTestClients`), whose
  budgets are literals in test code that no user value reaches.
- The two tests named `theLargestExpressibleBudget…` pin the boundary against a future "hardening"
  of the deadline stamp, not against today's code — the Evidence above is why they cannot be
  aimed at today's code.

[#321]: https://github.com/laughingman7743/flink-connector-gcp/issues/321
[#333]: https://github.com/laughingman7743/flink-connector-gcp/issues/333
[#334]: https://github.com/laughingman7743/flink-connector-gcp/issues/334
