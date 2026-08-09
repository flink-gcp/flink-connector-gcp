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

# ADR-0012: The subscriber teardown absorbs, discriminates, and releases in order

- Status: Accepted
- Date: 2026-08-05 ([#297]) through 2026-08-07 ([#348], [#349], [#350], [#351], the [#348]
  cluster); revised by [#358] (2026-08-09)
- Issues: [#297], [#325], [#348], [#349], [#350], [#351], [#358]
- Modules: pubsub (source); the cross-connector re-report contract is ADR-0003
- Current behavior: `docs/content/docs/connectors/datastream/pubsub.md` (source lifecycle,
  including the four-outcome teardown message table, and the reader metrics)

## Context

The FLIP-27 source's `PubSubSplitReader` owns one `PubSubNotifyingPullSubscriber` per split,
each wrapping an SDK `Subscriber` whose lifecycle operations are injected as functional values
(`SubscriberStart`, a `Runnable` stop, a `TerminationWait`) because `Subscriber` cannot be
subclassed — non-final, private constructor — and every path only a misbehaving client reaches
was untested before: the absorb, the timeout, and the failed-start release. `SubscriberStart`
takes a `Consumer<Throwable>` rather than an SDK `ApiService.Listener`, so no vendor type
reaches the seam; the production constructor cannot delegate through `this(...)` because the
receiver it hands the factory is `this::receiveMessage`, so the two constructors assign the same
fields and share only `startOrRelease`.

## Decision

- **`close()` puts the shutdowns and the closes in one list through `Closers.closeAll`, never a
  loop followed by a call** ([#297]). `shutdown()` declares no checked exception, so an
  unchecked one from the first subscriber used to skip every later nack *and* skip the
  `closeAll` wholesale — leaving even the already-shut-down subscribers open, holding messages
  Pub/Sub only redelivers once their acknowledgement deadline expires. The single list keeps the
  ordering (every shutdown before any close, so the waits overlap) because `closeAll` runs
  entries in order, and it makes the ordering survive a failure — pinned by asserting the
  recorded call order in the failing case too.
- **`awaitTerminated()` absorbs what the client raises because the client re-reports a failure
  this subscriber has already delivered** ([#325]; the mechanism and the eight SPIs measured not
  to share it are ADR-0003) — not only because the shutdown is best-effort, which was the whole
  of the stated reason until [#325] measured it, and is the weaker half.
- **The absorb tells a re-report from a first report, and only the first of those is a repeat of
  anything** ([#351]). The catch was `TimeoutException | RuntimeException` with one message, and
  [#325]'s own javadoc named the case it was wrong about: `Subscriber.doStop()` runs
  `runShutdown()` on a thread of its own under `catch (Exception e) { notifyFailed(e); }`, so a
  client healthy when `shutdown()` ran can fail **during** the teardown, set `permanentError`
  with nothing left to read it, and arrive at the same `IllegalStateException`. The
  discrimination is **a flag set where the failure is handed to a caller**
  (`permanentErrorReported`, written by `throwIfFailed`), tested together with the cause's
  identity. **Do not replace it with a snapshot of `permanentError` taken before `shutdown()`**
  — that looks equivalent and is wrong twice over, which this shipped as a draft and the first
  review round caught: on the reader's own close path the snapshot is taken *after*
  `stopAsync()` (every subscriber's `shutdown()` runs before any `close()`), so a failure the
  teardown itself produced lands in it and is reported as a repeat of something nobody read; and
  "recorded" never meant "consumed" anyway — a stream dying after the last `pullMessages` is
  recorded and read by nothing. The flag needs no ordering argument at all. What the identity
  half rests on is measured: Guava's `notifyFailed` is a no-op on an already-`FAILED` service,
  so the cause reaching the catch is the one recorded **first**. **Four outcomes, four
  messages** — the fourth being the failed-start release path, which has its own absorb rather
  than sharing this one, because both of its messages are false there (nothing was reported to a
  reader, and `shutdown()` never ran so nothing was nacked).
- **A throwing `nackSplit` must not skip the stop, and a throwing `shutdown()` must not skip the
  wait** ([#350]) — `Closers.closeAll` in both `shutdown()` and `close()`, [#297]'s rule one and
  two levels further in. `closed` is set before the nack, so the old shape left `shutdown()`'s
  idempotence guard claiming a client had been asked to stop that had not, and `close()` then
  spent the entire budget waiting on it. **Argue this as robustness, not as a bug closed**:
  measured on 1.152.0, the production `AckHandle` cannot throw at all — both flavours end in
  `SettableApiFuture.set`, which returns a `boolean` — so what the list buys is over an
  `@Internal` SPI whose implementations need not all be ours. `stopQuietly()` is deliberately
  **not** given the same list, and the reason is a property rather than scope-drawing: there, a
  stop that threw has started no shutdown for the wait to wait out, so skipping it is the right
  outcome. `removeSplit` has **no production path** (`SplitsRemoval` reaches a reader only
  through `SourceReaderBase`'s `eofRecordEvaluator` branch, which `PubSubSourceReader` supplies
  none for, and the enumerator never removes a split) — the reason `close()` holds the invariant
  is that it should hold it whoever calls it. That method reports a failure before it closes for
  the same reason: a split removed while paused carries one nothing has read, and removal,
  unlike teardown, happens while the job carries on.
- **A paused split is still watched** ([#348]). `NotifyingPullSubscriber.checkFailure()`,
  evaluated from `PubSubSplitReader.fetch()` beside `MissingCheckpointDetector.check()` — not
  inside `drainInto`, the tempting one-line site: the two guards belong together because both
  exist for a state whose whole symptom is the *absence* of records, which no record-driven
  check can see, and a method called *drain* should not throw for a reason that is not about
  draining. `pullMessages` was the only reader of `permanentError`, and a paused split is
  skipped entirely, so a subscriber that died while watermark alignment held its split was
  reported by nothing and the job ran green with one subscription dead — the empty-*source*
  shape of the trap ADR-0001's `recordsSkipped` and BigQuery's eager schema derivation both
  exist for. The check must read the recorded failure and never the message count, since a
  paused split is *supposed* to produce none.
- **The failed-start release is a no-op, and there is nothing left for it to release** ([#349],
  correcting the inference [#325] drew). [#325]'s own half stands — Guava's `stopAsync()` is
  guarded by `state().compareTo(RUNNING) <= 0` and `FAILED` sorts last, so once the service has
  failed the call enters nothing. What was wrong was what followed from it: **the channel and
  executors it cannot reach have already been released, by the SDK itself.**
  `Subscriber.startStreamingConnections()` adds to every connection a listener whose
  `failed(...)` runs `runShutdown()` — `stopAllStreamingConnections`,
  `shutdownBackgroundResources`, `subscriberStub.shutdownNow()` — **before** it calls
  `notifyFailed`, so a connection that fails to start, or a stream that dies on a missing IAM
  grant, releases everything on the SDK's own path. `doStart()`'s other failure,
  `GrpcSubscriberStub.create` throwing `IOException`, strands nothing either: it happens before
  the stub exists. **Owning the channel and executor (`setChannelProvider` /
  `setExecutorProvider`) was declined for it**: that means taking over channel sizing, which the
  SDK derives from `parallelPullCount`, to cover the one path out of four the SDK does not cover
  itself. The release call is kept for the states where `stopAsync()` is genuinely not a no-op —
  a failure registering the listener leaves the service `NEW`, one racing a start leaves it
  `STARTING` — and because it costs a no-op otherwise.
- **`BoundedShutdown` is deliberately not adopted here**, though the base module names this
  teardown as its nearest future adopter: [#265]'s problem was `Publisher.shutdown()` blocking
  the **task thread** uninterruptibly and without bound, and here the task thread's wait is
  already bounded — `stopAsync()` returns at once and `awaitTerminated` takes the budget — so
  the class would buy a thread and a residue counter for a bound that exists. **State the reason
  that way and not as "there is no unbounded wait"**, because there is one and `BoundedShutdown`
  could not take it either: `Subscriber.doStop()` spawns a bare `new Thread(...)` running
  `runShutdown()` under `SubscriberShutdownSettings.getTimeout()`, whose default is
  `Duration.ofSeconds(-1)`, no timeout (measured on 1.152.0, [#325]). A bare `new Thread`
  **inherits** its creator's daemon flag, so on Flink's task thread it is non-daemon — a
  property of who calls it, not of the SDK setting one. That thread is the SDK's own, so all a
  bounded wait could do is what `awaitTerminated` already does: give up and warn.
- **Two of the four outcomes are counted, and which two follows from what an operator would do
  about each** ([#358], paying the evidence this ADR's Consequences deferred). An expired wait is
  `subscriberShutdownsAbandoned` — spelled the way the sink's `publisherShutdownsAbandoned` is,
  because it means the same thing; a failure the teardown was the only report of is
  `subscriberFailuresUnreported`. The sub-decisions, none of them mechanical:
  - **Not one counter.** A single `subscriberShutdownsUnclean` would bury the incident under the
    tuning signal — an expired wait says `shutdownTimeout` is too low for the deployment, while
    the unreported failure is the case [#351] exists to isolate and the only report of it there
    is. They are read on different days and acted on differently, and a sum answers neither.
  - **Not four.** The re-report and the failed-start release are each a footnote to a *louder*
    report of the same incident — a job failure the reader is already raising, and the
    `IOException` `startOrRelease` is propagating as `stopQuietly()` runs — so a series for either
    would increment only when something more visible already had. What the failed-start release
    would otherwise report, stranded resources, [#349] measured the SDK to release itself on three
    of that path's four routes.
  - **Counting the failed-start release's timeout into `subscriberShutdownsAbandoned` was
    declined too**, though by that name it would belong: `stopQuietly()` catches
    `TimeoutException | RuntimeException` in one branch deliberately, and splitting it to count
    half would trade the paragraph above for a branch whose two arms say the same sentence.
  - **The name says the property, not the origin.** `subscriberShutdownFailures` was the obvious
    spelling and is wrong at the edge: the branch also catches a streaming failure that landed
    after the reader's last `pullMessages`, which is not a shutdown failure but *is* one nothing
    consumed. `unreportedSubscriberFailures` reads better in English, and loses on a property of
    this repository's inventories that is worth stating because it decided a name: **a counter
    leads with the thing counted, a gauge leads with the state** — `messagesAcked`, `topicsCreated`,
    `filesStaged`, `tasksDeduplicated` against `pendingAcks`, `parkedSplits`, `inFlightMutations`,
    `outstandingDeadLetters` — so a counter spelled state-first reads as a gauge. Counted across the
    four connectors' inventories 2026-08-09: 18 of 18 counters lead with the thing counted, 17 of 18
    gauges with the state. The exception is `deadLetterFlushMillis`, a duration with no state word
    available to lead with — its own sibling `longestDeadLetterFlushMillis` has one and uses it.
  - The storage is what this ADR already predicted — two more `PubSubShutdownResidue` adders, read
    through the existing `ResidueCounter` so the *instrument* is a counter while the
    *scope* is the class loader's — and the scope decision is the sink's, unchanged: aggregate by
    de-duplicating within a TaskManager and then summing. It is stated on the source page by
    reference rather than restated, which is the shape the dead-letter section already uses.
    **What the counter counts is subscriber teardowns, not reader closes**: a reader owns one per
    split, and parking a paused split (ADR-0066) closes one on its own, so a park can increment it
    with no reader closing. That last case is also the limit of the storage argument, and saying so
    keeps it honest: a park runs on a job that is still going, so *its* increments would be scraped
    from an ordinary per-subtask counter. The teardown-time increments are the ones that would be
    lost, and a metric name has one storage.

## Evidence

- **A subscriber's `awaitTerminated()` runs at most once per instance**, which is what makes the two
  counters counts of teardowns rather than of attempts to tear down: `removeSplit` takes the split
  out of the reader's map before closing it, `PubSubSplitReader.close()` skips a parked split, and
  `shutdown()`'s idempotence guard does not cover the wait — so the one path that could double-count
  is a second `close()` on the same instance, which no caller has.
- The counters need no new test seam: every one of the four outcomes already had a unit test from
  [#325], [#349] and [#351], and each now asserts the residues — including the two that must stay at
  zero, which is what makes the non-counts pinned rather than incidental.
- [#349] was **measured both ways**: from the 1.152.0 sources, and empirically by
  `PubSubSubscriberFailureReleaseITCase` — executors handed to a subscriber whose streaming pull
  fails permanently come back shut down, over repeated attempts. That class tests the *vendor*
  on purpose, because a javadoc here asserts the vendor's behaviour, and the executor is its
  observable because a `FixedTransportChannelProvider` would take ownership away from the SDK
  and so measure us instead of it. What remains uncovered is narrow and was never the claim: a
  throw in the **synchronous** part of `startStreamingConnections` before any connection
  listener exists.
- [#348]'s premise ("alignment pauses splits routinely") was measured rather than argued:
  `PubSubSourceWatermarkAlignmentITCase` (2026-08-07, emulator, one run: 13 of 200 ahead records
  consumed against 200 of 200 behind). Two things about it are load-bearing: the **throttle** (a
  MiniCluster drains a few hundred buffered messages in less time than one watermark interval,
  so without it the ahead split finishes before the machinery under test has run once), and the
  **stability** assertion, sampled until the count stops moving — `ahead < BATCH` alone is
  satisfied by a split that is merely lagging, and at `maxRecordsPerFetch(1)` the two splits
  advance in lockstep, so with alignment doing nothing the count at that instant is 199 or 200,
  and 199 passes. The second test discriminates as written: with the `fetch()` call removed the
  job stayed **running** for the full budget (measured 2026-08-07, one run, then 120 s) rather
  than failing. Deleting a subscription under a running job makes the emulator fail that
  streaming pull permanently — a rare case of the emulator being usable for a failure path.
- **What the reader controls is the report, not the job's death**: a `SourceOperator` in
  `WAITING_FOR_ALIGNMENT` returns `NOTHING_AVAILABLE` without calling `pollNext()` and waits on
  the alignment future rather than the reader's availability, so `SourceReaderBase.checkErrors`
  is not reached and a subtask held back by alignment surfaces the failure only when its group
  releases it (read from `flink-runtime`'s `SourceOperator`). A delay rather than a loss — that
  subtask emits nothing further, so it does not hold the group's minimum back — and a property
  of every FLIP-27 source, not of this one. The ITCase does not meet it because parallelism 1
  makes this subtask the group minimum; a multi-subtask reproduction would be measuring Flink.

## Consequences

- A genuine streaming failure landing after the last `pullMessages` is classified as the
  *unconsumed* case, which is correct by the property being tested: `pullMessages` will not be
  called again, so nothing consumes it either. What that case costs is **promptness, not
  messages** — `runShutdown()` begins with `stopAllStreamingConnections`, the thing that flushes
  the nacks `nackSplit` just enqueued, so a failure before that flush leaves them to wait out
  the acknowledgement deadline ([#118]'s property).
- **The two counters carry the scope their storage forces**, which is the cost of reporting them at
  all: a count incremented during `close()` is never scraped — the reader's metric group is
  unregistered in the same instant, measured for [#311] — so the value a subtask reports is its
  class loader's total rather than its own, and on a session cluster running the SQL uber-jar from
  `lib/` it is the TaskManager's across every job. A reader that has seen nothing still reports
  what earlier attempts left, which is the point, and a resubmitted job reports zero while stranded
  clients remain, which is the trap. Summing the raw series across subtasks multiplies one JVM's
  count.
- **Two of the four outcomes stay reported by a log line alone**, so a deployment that wants them
  needs a log pipeline that keeps `WARN`. The bet is that neither ever occurs without the louder
  report beside it; a case where one does — a re-report on a job that somehow survives, say — would
  be evidence to revisit this, not a gap to fill by symmetry.

[#118]: https://github.com/laughingman7743/flink-connector-gcp/issues/118
[#265]: https://github.com/laughingman7743/flink-connector-gcp/issues/265
[#297]: https://github.com/laughingman7743/flink-connector-gcp/issues/297
[#311]: https://github.com/laughingman7743/flink-connector-gcp/issues/311
[#325]: https://github.com/laughingman7743/flink-connector-gcp/issues/325
[#348]: https://github.com/laughingman7743/flink-connector-gcp/issues/348
[#349]: https://github.com/laughingman7743/flink-connector-gcp/issues/349
[#350]: https://github.com/laughingman7743/flink-connector-gcp/issues/350
[#351]: https://github.com/laughingman7743/flink-connector-gcp/issues/351
[#358]: https://github.com/laughingman7743/flink-connector-gcp/issues/358
