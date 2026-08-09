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
- Date: 2026-08-08; revised by [#381] (2026-08-08), [#413] (2026-08-09)
- Issues: [#334], [#321], [#333], [#381], [#413]
- Modules: base (`base.options`, `BoundedShutdown`), pubsub, cloudtasks, bigtable, bigquery
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
- **A shared helper in `flink-connector-gcp-base`, at [#334]'s scope.** Declined there and taken
  by [#381]; the Decision above is what stands. The reason it waited is worth keeping: at that
  point the ceiling was one `checkArgument` in a bug fix on a shipped path, and a new `@Internal`
  type crossing a module boundary is a decision about the base module's surface rather than a
  consequence of the fix. What changed the balance was measuring the duplication — six files, and
  a message whose readable half a copy drops.
- **Restating the bound in the Table API mappers.** ADR-0007's rule is that a check whose message
  names Java setters needs restating in DDL keys. Declined on the measurement above rather than on
  the "unreachable in practice" reasoning [#321] and [#333] used: the message does reach the SQL
  user in the cause, and `shutdownTimeout` is legible as `sink.shutdown-timeout` — unlike
  ADR-0007's `retryTotalTimeout`, which appears nowhere in a `WITH` clause and is why that rule
  exists. What the measurement *did* change is the message itself, above.

**[#381]: the rule has one implementation, `base.options.OptionChecks`.** A rule enforced by
copying four lines is a rule the next knob obeys only if its author remembers it, and the part a
copy silently drops is the message's readable half. Both checks a `Duration` option setter runs
moved there, and both clear the base module's multiple-consumer bar (ADR-0036) on their own: nine
ceiling call sites across base, pubsub and bigquery; thirty-one positivity call sites across
pubsub and bigquery.

**[#413]: the same rule one unit down — a `Duration` spent in milliseconds is bounded below, at the
setter, at one millisecond.** `OptionChecks.checkAtLeastOneMilli` is the third check, and it
arrived the way the other two did: as five private copies (pubsub, cloudtasks, bigtable, and
BigQuery's buffered-stream and FILE_LOADS options) in two message shapes and two mechanics. What
the copies dropped this time was not the message's readable half but its *coverage*. Grepping for
the conversion rather than for either wording — the discipline [#381] arrived at — found eleven
setters that convert a user's value and had no floor at all:

| Knob | What spends it in milliseconds | What a sub-millisecond value did |
|---|---|---|
| `BufferedStreamOptions` and `DefaultStreamOptions` `recovery{Initial,Max}Backoff` | `RetrySchedule`, whose constructor requires `initialBackoffMs > 0` | accepted at the setter, `IllegalArgumentException` on a TaskManager as the writer or committer was built |
| `DefaultStreamOptions.retry{InitialDelay,MaxDelay}` | gax's `ExponentialRetryAlgorithm`, which reads `toMillis()` | a retry loop with no backoff |
| `DefaultStreamOptions.maxRetryDuration` | the SDK's `ConnectionWorker`, which reads `toMillis()` and treats **zero as unlimited** | the ceiling inverted — a knob set to give up almost at once retries forever |
| `DefaultStreamOptions.flushInterval` | `registerTimer(now + toMillis())` | a timer re-armed in the past, spinning on the mailbox thread |
| `TableCreateOptions.timePartitioningExpiration` | the setter itself, into the request's `expirationMs` | a zero expiration nobody asked for |
| `PubSubSubscriberOptions.awaitAckConfirmation` | `future.get(toMillis(), MILLISECONDS)` | a confirmation timing out before it can arrive |
| `PubSubSubscriberOptions.shutdownTimeout` | `latch.await(toMillis(), MILLISECONDS)` | a shutdown that waits for nothing |

The two vendor readings were measured from the resolved sources, not inferred: gax 2.82.0's
`ExponentialRetryAlgorithm.createNextAttempt` (`long newRetryDelay =
settings.getInitialRetryDelayDuration().toMillis()`), and google-cloud-bigquerystorage 3.30.0's
`ConnectionWorker` (`maxRetryDuration.toMillis() == 0f || …`, the disjunct that makes zero mean
"no ceiling" — and the SDK says so itself, on `StreamWriter.Builder.setMaxRetryDuration`: *"You can
allow unlimited retry by setting the value to be 0."*). Without them the retry knobs read as a
`Duration`-typed SDK API with no truncation in it, and the sweep would have skipped four setters —
the `maxRetryDuration` one inverting the user's intent rather than merely ignoring it.

Three conversions were surveyed and deliberately left unfloored, which is what makes this a rule
rather than a sweep: `FileLoadsOptions.minCheckpointInterval` only feeds a warning threshold,
`PubSubSubscriberOptions.firstCheckpointTimeout` is already floored by `Math.max(1, …)` where it is
spent, and the sink's `PubSubPublisherOptions.shutdownTimeout` is spent in nanoseconds and
truncates nothing.

**The check follows the conversion, not the knob name**, which is the one question this ADR now
answers two ways and is worth stating rather than leaving to be re-derived. The *ceiling* was given
to the source's `shutdownTimeout` **because** the sink has a knob of that name — one answer for one
name. The *floor* is withheld from the sink's for the opposite reason. The difference is that the
ceiling is uniform and costs nothing, while the floor's message asserts something — "it is applied
at millisecond granularity" — that is true at every call site it stands on and would be false at
the sink's. A message that states its own reason is what keeps the asymmetry checkable.

**A value the SDK itself defines is not the floor's to refuse — `checkAtLeastOneMilliOrZero`.**
A knob this project *forwards* to a vendor setting stays settable as the vendor defines it, and
four vendor settings this project forwards give `Duration.ZERO` a meaning, each read from the
resolved sources rather than assumed:

| Vendor setting | What zero means there | Our knobs |
|---|---|---|
| `StreamWriter.Builder.setMaxRetryDuration` (bigquerystorage 3.30.0) | *"You can allow unlimited retry by setting the value to be 0"*; `ConnectionWorker` skips the elapsed-time comparison | `maxRetryDuration` ×2 |
| `RetrySettings.totalTimeout` (gax 2.82.0) | *"the logic will instead use the number of attempts to determine retries"* — and it is gax's own retry default | `retryTotalTimeout` |
| `RetrySettings.initialRetryDelay` / `maxRetryDelay` (gax 2.82.0) | gax's own retry default; a zero cap clamps every delay to none | `retryInitialDelay` / `retryMaxDelay` ×3 classes |
| `RetrySettings.initialRpcTimeout` / `maxRpcTimeout` (gax 2.82.0) | *"allows the RPC to continue indefinitely"* | `retryInitialRpcTimeout` / `retryMaxRpcTimeout` |
| `Subscriber.Builder.setMaxAckExtensionPeriod` (google-cloud-pubsub 1.152.0) | *"A zero duration effectively disables auto deadline extensions"* | `maxAckExtensionPeriod` — **non-negative only, no floor**: see below |

**The floor and the exemption are the same argument, not a compromise between them.** Every one of
these values is read by the vendor with `toMillis()`, so a *positive* sub-millisecond value would
arrive as zero and silently become the sentinel — which is how a retry ceiling set to give up
almost at once became unlimited retry. Refusing it is what keeps zero meaning only what the user
typed, and it is why five Pub/Sub sink knobs gain a floor here that they never had: `checkPositive`
let 500 µs through, and gax turned it into "run indefinitely".

Whether zero is legal is therefore a property of the SDK on the other side, never a loosening for
convenience: a `Duration` this project *spends itself* — a `RetrySchedule` backoff, a
processing-time flush interval, a bounded `await` — keeps the plain floor, because nothing on the
other side gives its zero a meaning.

**And a forwarded knob the vendor does not truncate takes neither check**, which is
`maxAckExtensionPeriod` and was caught by round two rather than by reasoning: the first draft gave
it the zero-tolerant floor on the assumption that "the SDK reads it in milliseconds too", and
`MessageDispatcher` spends it as `now().plus(maxAckExtensionPeriod)` — an `Instant` at nanosecond
resolution, with no `toMillis()` anywhere (google-cloud-pubsub 1.152.0, line 500). A sub-millisecond
value there is a very short budget, not a zero in disguise, so the floor had nothing to prevent and
its message — "it is applied at millisecond granularity" — would have asserted something false.
The knob takes a non-negative check written at its own setter instead. The general form: **the
message is the test.** A floor whose stated reason is untrue at a call site is a floor that does
not belong there, and checking the reason is what tells the two apart.

**Where a setter carries both, the ceiling runs before the floor.** Only one does today — the
Pub/Sub source's `shutdownTimeout` — and the order is not stylistic: the floor converts with
`toMillis()`, which past about 292 million years throws an `ArithmeticException` naming no knob at
all, so a floor placed first would answer the most absurd budget with the least useful message and
undo what this ADR bought. The ceiling rejects everything that could overflow the conversion, which
is why running it first costs nothing.

**The message carries the reason and the value, and the mechanics fold positivity in.** The two
copied shapes were `"x must be at least 1 millisecond (it is applied at millisecond granularity)"`
and `"x must be at least 1 ms: <value>"`; the survivor is their union, on [#381]'s finding that the
value-carrying form wins. Zero and negative durations fail the floor with its own message rather
than with positivity's: a user told only "must be positive" sets 500 µs next and is rejected a
second time, so folding costs one round trip less. And a duration past about 292 million years
still throws `ArithmeticException` out of `toMillis()` — accidental in all five copies, kept
deliberately and pinned by a test, because the exception type is odd but the landing site is the
setter, which is the whole point.

- The ceiling constant had stood in **six files**, with its message written out in eight.
- Positivity had **two implementations and three message shapes for one check** — `"x must be
  positive"`, `"x must be positive: <value>"` and `"x must be positive, but was <value>"`. That is
  what a rule with no single implementation decays into, and the third shape was found only by
  grepping for the check rather than for either known wording.
- **The value-carrying form won.** A rejection naming only the knob leaves a builder chain that
  sets several durations ambiguous, and the value costs nothing to include. Nineteen Pub/Sub
  messages gained it; BigQuery's eleven already had it and are unchanged.
- The blocker the old split itself named is removed by the same move:
  `pubsub.source.OptionChecks`' javadoc explained that the sink kept a private copy because
  sharing "would need a public type to cross the package boundary" — an `@Internal` base type is
  that, and is smaller than a public Pub/Sub one.

Numeric (`int`/`long`) positivity checks stayed inline: the helper is `Duration`-typed, and
Bigtable and Cloud Tasks have no `Duration` positivity check to unify.

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
- The floor tightens eleven shipped setters: a sub-millisecond value they used to accept is now
  refused. Nothing in the tree set one — every such value in the test sources was already a
  rejection assertion — and the reference pages carry defaults, not floors, so no documented
  configuration changes. Three connectors' rejection messages gain the offending value and
  BigQuery's two gain the parenthetical, which is a user-visible wording change taken while the
  artifacts are unpublished, as [#381]'s was.
- **What a SQL user is shown**, measured 2026-08-09 through `FactoryMocks.createTableSink` with
  `sink.default-stream.recovery.initial-backoff = '500 micros'` (a throwaway probe, deleted after
  the run), as the ceiling's was:

  ```text
  ValidationException: Unable to create a sink for writing table 'default.default.t1'.
    caused by IllegalArgumentException: recoveryInitialBackoff must be at least 1 millisecond
    (it is applied at millisecond granularity): PT0.0005S
  ```

  So the floor is **not** restated in DDL keys, on the same measurement the ceiling was: the
  actionable sentence reaches the user in the cause, and `recoveryInitialBackoff` is legible as
  `…recovery.initial-backoff`, unlike ADR-0007's `retryTotalTimeout`. The probe also settled that
  the floor is reachable from SQL at all — Flink's duration parser rejects `us` but accepts
  `micros`, `µs` and `nanos`, so a sub-millisecond value is expressible in a `WITH` clause. The
  one knob whose builder name `TableCreateOptionsMapper` already judges unactionable in DDL,
  `timePartitioningExpiration`, is covered by accident rather than by exception: its floor message
  names `expiration`, which is its option key's own last segment.

[#321]: https://github.com/laughingman7743/flink-connector-gcp/issues/321
[#333]: https://github.com/laughingman7743/flink-connector-gcp/issues/333
[#334]: https://github.com/laughingman7743/flink-connector-gcp/issues/334
[#381]: https://github.com/laughingman7743/flink-connector-gcp/issues/381
[#413]: https://github.com/laughingman7743/flink-connector-gcp/issues/413
