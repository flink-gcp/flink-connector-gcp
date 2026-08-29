<!--
Copyright 2026 The flink-gcp authors

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

# ADR-0007: The publisher teardown is two-phase, and its bound is real

- Status: Accepted
- Date: 2026-08-06 ([#265]; [#310], [#312], [#311] extended it the same day); revised 2026-08-29 ([#1132])
- Issues: [#265], [#310], [#311], [#312], [#1132]
- Modules: pubsub (the mechanism, `BoundedShutdown`, lives in `base.lifecycle` — [#312])
- Current behavior: `docs/content/docs/connectors/datastream/pubsub.md` § Publisher lifecycle

## Context

The SDK defect, read from `google-cloud-pubsub` 1.152.0 rather than assumed: `Publisher.publish`
increments `messagesWaiter` per accepted message, and the failure callback cancels the messages
still accumulating in a failed ordering key's **un-flushed** `MessagesBatch` and removes the
batch, while decrementing only by the *in-flight* batch's size — so those increments are never
returned, `pendingCount` stays above zero forever, and `Waiter.waitComplete()` (uninterruptible,
wakes only on an exact zero) parks `Publisher.shutdown()` for good. Our 30 s bound sat on the
*next* line, `awaitTermination`, which was never reached; both `TopicPublisher.close()`'s
javadoc and the docs page already promised a bound, so this was a contract violated rather than
a feature missing. The window is not exotic — a keyed publish failing with more of that key
still batched is what [#78] and [#215] exist for, and `close()` on the failure path runs without
a preceding flush.

**The defect is not the only thing that needs the bound, and the second reason is the bigger
one**: with `enableMessageOrdering` the SDK replaces the publisher's retry settings with
`maxAttempts = Integer.MAX_VALUE` and an effectively infinite total timeout (`Publisher.java`
1.152.0, and its own `TODO` says this is per publisher, so unkeyed messages get it too), so
during an outage the in-flight publishes retry forever and `waitComplete()` never drains — no
defect required. An ordered sink therefore needs this bound whatever the SDK version, which is
why nothing here is written as a workaround and why [#309]'s rewording is a rewording rather
than a removal.

## Decision

The sink runs the whole SDK teardown on a separate daemon thread (`BoundedShutdown`) and gives
up at the deadline, releasing the channel either way; the close is two-phase — every publisher
asked to shut down before any is waited on, so the waits overlap and a close costs one
`shutdownTimeout` however many topics the writer wrote to.

Decisions not to re-litigate about the teardown itself:

- **A separate thread is the only lever**: the wait ignores interruption, `Publisher` has no
  forcible variant, and `Waiter` is package-private. It is a **daemon** thread (one that never
  returns must not keep a JVM alive) — the repository's first main-code thread — and a plain
  thread rather than an `ExecutorService`, which buys nothing since `shutdownNow()` cannot
  interrupt that wait either: its thread would leak identically, and the executor would then
  need a bounded teardown of its own.
- **The deadline is recorded by `start()`, not by `close()`**, which is what makes the writer's
  overlapped teardown cost one timeout rather than one per topic; `start()` is idempotent and
  deliberately does not restart the clock. Pinned by
  `theBudgetRunsFromTheShutdownCallRatherThanFromTheClose`, the only test that fails if the
  deadline moves into `close()`.
- **`awaitTermination` runs on that thread too, not on the task thread after a successful
  join** — measured on gax 2.82.0, whose `BackgroundResourceAggregation.awaitTermination` passes
  the *full* duration to each resource in turn (its own source carries the `TODO subtract time
  already used up from previous resources`), and a publisher nests several: its executor, then
  the stub's transport channel and watchdog. The first shape of this fix awaited on the task
  thread and so cost a *multiple* of the timeout while claiming to cost one; the self-review
  caught it.
- **Anything either step throws is captured and rethrown by `close()` with its own type** — on a
  bare thread it would reach only Flink's JVM-wide handler, losing a teardown failure the
  pre-[#265] inline call reported and, under `cluster.uncaught-exception-handling: FAIL`,
  exiting the whole TaskManager instead of failing one task.
- **The two steps are functional values, not a `Publisher`**, because `Publisher` cannot be
  subclassed — non-final, but its only constructor is private ([#324]), the same mechanism that
  makes Bigtable's `BigtableDataClient` unfakeable. That is the only seam a test can drive, the
  same argument `PubSubDeadLetterQueue`'s `publisherShutdown`/`channelShutdown` fields make.
- **The give-up warning does not attribute itself to [#265]**: the budget is shared, and a
  healthy teardown an earlier publisher left no time for reaches the same branch. **Both
  warnings report the time actually waited, not the configured budget**, for that same reason —
  a publisher after one that hung gets none of the budget, and "did not finish within 30 s"
  having waited nothing reads as "raise the timeout" when the answer is elsewhere.
- **The thread is named after the task thread as well as the topic** (Flink's
  `SplitFetcherManager` convention): a writer is per subtask, and without it every subtask
  writing one topic leaves identically-named threads for an operator to tell apart.
- **`close()` restores the interrupt flag** before propagating an `InterruptedException`: `join`
  clears it, `Closers.closeAll` collects and carries on, so without the restore the rest of the
  writer's teardown stops honouring the cancellation. A failure captured *after* `close()` gave
  up is logged rather than dropped — nothing would otherwise read the field, and a thread
  outliving its job meets a closed user classloader.

**[#310]: `retryTotalTimeout` and `retryMaxAttempts` are rejected beside
`enableMessageOrdering(true)`**, rather than documented as ignored — the ordering override above
means they would reach nothing. Only an explicitly set knob is a conflict (both are `@Nullable`,
so "unset" is distinguishable and the SDK's own defaults are exactly what ordering is expected
to override); the other six retry knobs still apply. Rejecting was chosen over documenting
because documenting alone is **unpinnable**: `Publisher` exposes `getBatchingSettings()` and
nothing for retry settings — the values are folded into the stub's callables — so no reflective
assertion can hold a documented claim. The check lives in the options class (both knobs are its
own) and names **only the knob that was actually set**. `PublisherOptionsMapper` restates it in
DDL keys; the first draft's claim that the builder's message "reaches SQL unchanged" was wrong —
measured on `flink-table-common` 2.2.1, `FactoryUtil.createDynamicTableSink` wraps *anything*
the factory throws in a `ValidationException` whose own message names only the table, so the
actionable sentence lands in the cause and would name `retryTotalTimeout(...)`, which appears
nowhere in a `WITH` clause. The rule that generalises (the counter-example being
`PubSubSourceBuilder`'s `parallelPullCount` × `PER_KEY` check, which throws from
`getScanRuntimeProvider`, outside the factory's `try`): **a check that fires inside
`createDynamicTable{Source,Sink}` is wrapped, and a check whose message names Java setters needs
restating in option keys; one whose message needs no translation does not.** (For single-value
checks this judgment was later replaced by a mechanical rename at the mapper seam — [ADR-0133];
the restating rule remains the cross-field checks' rule, with this record's restatement as the
example.)
Deliberately **no runtime re-check** in `DefaultPublisherFactory`: `PubSubWriter` carries the
Bigtable-style "deserialization does not run the builder" guard for `maxInFlightMessages`
because that invariant is *relied on* (a non-positive cap parks the task forever), and this one
is not — a deserialized violating instance behaves exactly as before, since the SDK overwrites
the settings either way. The check is advisory, and that is the whole of it.

**[#311]: the residue is visible as `publisherShutdownsAbandoned`**, a **counter** reading
`PubSubShutdownResidue.PUBLISHER_SHUTDOWNS_ABANDONED`. Four things not to re-derive:

- The **storage** is a `LongAdder` at the **module root** that this connector owns and passes
  into every `BoundedShutdown` it builds — the base class holds no count, so a future adopter
  gets its own field here rather than a second meaning for this one. **[#329] is that adopter**:
  the dead-letter queue's teardowns moved to a second adder, reported as
  `deadLetterPublisherShutdownsAbandoned`, because that queue registers on the *host* sink's
  metric group and a Pub/Sub sink has already registered this name there (ADR-0009). This count
  is therefore the **sink's publishers'** alone. It has to outlive the task:
  measured, with a MiniCluster probe whose reporter ran at 10 ms (default 10 s) and never once
  saw a close-time counter above zero over four runs, while a counter incremented during the run
  read its full value; the writer's metric group is unregistered as its task is cleaned up, in
  the same instant its `close()` runs.
- **On a session cluster or in application mode the scope is the class loader**: a job's own jar
  isolates it per job, the SQL uber-jar in `lib/` shares one count across every job on the
  TaskManager. Nothing is corrupted either way — it becomes a TaskManager property, which is the
  honest scope for threads that are in the JVM whoever left them, and the docs say so, including
  that a `PubSub → BigQuery` job dead-lettering to Pub/Sub contributes to a dead-letter count
  every other pipeline on that loader reports too.
- The **instrument does not follow from the storage**, which the first draft got wrong by
  registering a gauge: a cumulative count of events is a counter by the base module's naming
  rule, and a caller-supplied `Counter` reading the connector's adder registers the same number
  — so the name is `publisherShutdownsAbandoned`, not `abandonedPublisherShutdowns`. Registered
  in the `PubSubWriterMetrics` **constructor**, not `bindWriterState`, since it reads no
  writer state.
- **What it counts is closes that overran their budget, not stranded threads**: after give-up
  the background thread exits as soon as the client's shutdown returns, so an overrun of one
  second leaves nothing behind and still increments — the docs say so, having first claimed the
  opposite.

**[#1132]: running-task publisher eviction changes where the bound may be spent and observed.**
A capacity or idle eviction can run a bounded teardown while the writer metric group is registered,
so an overrun there is visible from the current attempt; only final-close overruns retain the
original "next attempt observes it" shape.
The pinned SDK defect also means that connector state becoming drained does not prove the SDK's
shutdown counter can reach zero.
Nor can the connector classify this safely from the publish future: `Publisher.publish()` releases
its batch lock before incrementing the shutdown waiter and returning, so a concurrent root-failure
callback can cancel an accepted buffered publish before the caller observes that future.
An already-complete cancellation can therefore be either that unsafe in-batch race or a safe
rejection for a key that was already paused.
Running-task eviction consequently tests the lifecycle outcome instead of guessing its cause.
It closes a connector-clean publisher with the ordinary bounded teardown and opens no replacement
until that finishes.
If the shutdown thread is still alive when the budget expires, or the client's termination wait
reports resources still alive, the eviction fails the task with an actionable error.
One publisher may then be abandoned in that attempt, but destination churn cannot repeatedly
replace it and accumulate resources behind `maxActivePublishers`.

What [#311] asked for and did **not** get, with reasons: **setting the stranded thread's context
classloader** does not work — the thread's stack holds the `BoundedShutdown` instance, hence its
class, hence the user classloader, so the retention survives whatever the TCCL is; the narrower
benefit (avoiding `IllegalStateException: Trying to access closed classloader` from a reflective
lookup on that thread) cuts both ways, since a `ServiceLoader` lookup for a provider in the job
jar would then find nothing. A cap on stranded threads was originally declined because it would
turn a close-time outage into a job failure and the counter was sufficient for that close-only
design. [#1132] adds running-task eviction, where repeatedly abandoning publishers would defeat the
active-resource bound; the targeted failure above applies only when an actual running-task release
overruns, not to final close.

**[#312]: `PubSubDeadLetterQueue` uses the same teardown**, which [#265] had deliberately left
alone on the grounds that its `envelope(...)` sets no ordering key so the leaking cancel branch
is unreachable there. That reasoning was sound and insufficient: `waitComplete()` still blocks
until every in-flight dead letter resolves under the ordinary 600 s retry budget, and the wait
sat on the task thread one entry after the bounded sink leg in the same `Closers.closeAll` list
— so a sink with a DLQ presented `shutdownTimeout` as its close's budget while spending an
unbounded leg on top of it. Three decisions from that move: **the class went to
`base.lifecycle`** (two consumers is the base module's bar, the package already exists, and ~30
lines of subtle concurrency duplicated is worse than the ~10 duplicated lines of emulator setup
this module already accepts); **the channel parameter became a nullable `Runnable release`, not
an `AutoCloseable`** — it runs in a `finally`, where anything it threw would replace the failure
being propagated; the sink passes `channel::shutdownNow` and the DLQ passes `null`, its channel
being the next entry in its own `closeAll` list; and **the DLQ's hardcoded 30 s became
`PubSubDeadLetterQueue.Builder.shutdownTimeout(Duration)`** — once the docs promise a budget
covering the whole close, the half a user cannot reach is the half they cannot fix. It is a
second budget spent after the sink's, and both user-facing documents say to keep the **sum**
under `task.cancellation.timeout`. (`check-option-docs`'s `SOURCE_GLOBS` — `*Options.java` /
`*SinkBuilder.java` / `*SourceBuilder.java` — does not match this builder's file, so from the day
the class landed neither direction of that check read it. [#328] reached it by naming the file in
the module's `sources`, and both budgets now carry a `reference/pubsub.md` row.)

## Consequences

What remains, logged rather than hidden: a publisher whose shutdown never returns leaves that
thread and the client's executors until the JVM exits.
At final close that remains a logged residue.
During running-task eviction the same overrun is visible while the attempt is running and fails the
task before any replacement publisher opens, so one attempt cannot accumulate the residue through
destination churn.
`shutdownTimeout` became a `PubSubPublisherOptions` knob (30 s, matching what was hardcoded) for
symmetry with `PubSubSubscriberOptions.shutdownTimeout`. [#321] then bounded the DLQ's **flush**, a
separate budget (ADR-0009).

[#78]: https://github.com/flink-gcp/flink-connector-gcp/issues/78
[#215]: https://github.com/flink-gcp/flink-connector-gcp/issues/215
[#265]: https://github.com/flink-gcp/flink-connector-gcp/issues/265
[#309]: https://github.com/flink-gcp/flink-connector-gcp/issues/309
[#310]: https://github.com/flink-gcp/flink-connector-gcp/issues/310
[#311]: https://github.com/flink-gcp/flink-connector-gcp/issues/311
[#312]: https://github.com/flink-gcp/flink-connector-gcp/issues/312
[#321]: https://github.com/flink-gcp/flink-connector-gcp/issues/321
[#324]: https://github.com/flink-gcp/flink-connector-gcp/issues/324
[#328]: https://github.com/flink-gcp/flink-connector-gcp/issues/328
[#329]: https://github.com/flink-gcp/flink-connector-gcp/issues/329
[#1132]: https://github.com/flink-gcp/flink-connector-gcp/issues/1132
[ADR-0133]: 0133-a-table-option-value-the-builder-rejects-is-renamed-to-its-option-key.md
