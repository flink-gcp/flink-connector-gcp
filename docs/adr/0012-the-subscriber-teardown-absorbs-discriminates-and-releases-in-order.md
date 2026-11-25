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

# ADR-0012: The subscriber teardown absorbs, discriminates, and releases in order

- Status: Accepted
- Date: 2026-08-05 ([#297]) through 2026-08-07 ([#348], [#349], [#350], [#351], the [#348]
  cluster); revised by [#358] (2026-08-09); seam refined by [#2263] (2026-11-23); failed stub
  creation corrected by [#2291] (2026-11-23); budget clock refined by [#2302] (2026-11-23);
  failed-start deliveries released by [#2307] (2026-11-23); throwing nacks qualified by [#2326]
  (2026-11-25)
- Issues: [#297], [#325], [#348], [#349], [#350], [#351], [#358], [#2263], [#2291], [#2302],
  [#2307], [#2324], [#2326]
- Modules: pubsub (source); the cross-connector re-report contract is ADR-0003
- Current behavior: `docs/content/docs/connectors/datastream/pubsub.md` (source lifecycle,
  including the four-outcome teardown message table, and the reader metrics)

## Context

The FLIP-27 source's `PubSubSplitReader` owns one `StreamingPullSubscriber` (named
`PubSubNotifyingPullSubscriber` until [#755]) per split,
each wrapping an SDK `Subscriber` whose lifecycle operations are injected as functional values
(`SubscriberStart`, a `Runnable` stop, a `TerminationWait`) because `Subscriber` cannot be
subclassed — non-final, private constructor — and every path only a misbehaving client reaches
was untested before: the absorb, the timeout, and the failed-start release. `SubscriberStart`
takes a `Consumer<Throwable>` rather than an SDK `ApiService.Listener`, so no vendor type
reaches the seam; the production constructor cannot delegate through `this(...)` because the
receiver it hands the factory is `this::receiveMessage`, so the two constructors assign the same
fields and share only `startOrRelease`.

Since [#2263] the client is one connector-owned interface, `SubscriberClient` (start, stop,
bounded wait, and a close that releases its transport), which `DefaultSubscriberFactory` returns
and a test scripts; its `start` still takes a `Consumer<Throwable>`. Both constructors end in one
private constructor that opens the client with `this::receiveMessage`, the production one through
the factory and the seam by returning the scripted client. The same change gave the failed-start
release something to release, which the decision on it below predates: once the factory has
returned a client, the subscriber owns that client's transport (ADR-0208), so the release closes
the client after `stopQuietly()`, and with an emulator that also shuts down the channel the
transport opened. This change leaves untouched what that decision says about the SDK's
`stopAsync()` being a no-op on a failed service. The other decisions are unchanged.

## Decision

- **`close()` puts the shutdowns and the closes in one list through `Closers.closeAll`, never a
  loop followed by a call** ([#297]). `shutdown()` declares no checked exception, so an
  unchecked one from the first subscriber used to skip every later nack *and* skip the
  `closeAll` wholesale — leaving even the already-shut-down subscribers open, holding messages
  Pub/Sub only redelivers once their acknowledgement deadline expires. The single list keeps the
  ordering (every shutdown before any close, so the waits overlap) because `closeAll` runs
  entries in order, and it makes the ordering survive a failure — pinned by asserting the
  recorded call order in the failing case too.
- **The budget's clock starts when `shutdown()` has asked the client to stop, and
  `awaitTerminated()` waits only for what is left of it** ([#2302]). The ordering above
  overlapped the waits only for clients that did stop: each `awaitTerminated()` handed the client
  the full `shutdownTimeout` from the moment its turn came, so N clients stuck in shutdown cost N
  budgets, and seven at the 5 s default outlast Flink's `source.reader.close.timeout`.
  `SubscriberTeardown.startClock()` stamps a deadline in a `finally` after the nack and
  `stopAsync()` — after them so the budget runs from the stop request, as the guide says, and in
  a `finally` so a step that throws still leaves the wait a deadline — and it never restarts the
  clock. The reader's deadlines therefore fall within the time its shutdowns take, which is the
  shape `BoundedShutdown.start()` gives the sink's publishers (ADR-0007), and the remainder is
  handed to the client in nanoseconds as there. With nothing left the client is still asked:
  measured on Guava 33.6.0 and api-common 2.73.0's `AbstractApiService`, `awaitTerminated(0, …)`
  returns for a terminated service and throws `TimeoutException` for one still stopping, so a
  client that stopped while an earlier wait expired is not reported as abandoned. One exception
  is read rather than measured: Guava's timed wait gives up without checking the state when it
  cannot take the service's lock at once, as its source notes, so a client terminating at that
  very instant can be reported as an expired wait. The budget is converted with `toNanos()`,
  which makes the option's nanosecond ceiling load-bearing rather than a matter of consistency;
  `StreamingPullSubscriber` re-checks the option's range before opening the client, so a forged
  options object fails before there is a transport to strand. The deadline overflows at that
  ceiling, and the subtraction that reads it wraps back, as ADR-0068 records for
  `BoundedShutdown`. What the shared budget costs is a count: a client still stopping when its
  turn comes after its deadline can now be counted in `subscriberShutdownsAbandoned`, because
  the SDK's wait then times out at once, even when it would have finished within a full budget
  counted from its turn; the counter's documented reading (raise `shutdownTimeout`) applies to it
  unchanged. The failed-start release keeps its own full budget in milliseconds, and with it the
  option's one-millisecond floor. Each subscriber's transport release still
  follows its own wait and is outside the budget, as the Pub/Sub guide states.
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
  so the cause reaching the catch is the one recorded **first**. **Four outcomes, four `WARN`
  messages** — the fourth being the failed-start release path, which has its own absorb rather
  than sharing this one, because both of its messages are false there: nothing was reported to a
  reader, and the failure being propagated is the start's, not one that arrived after the reader
  stopped pulling. Since [#2307] the fourth also logs at `DEBUG` when its wait only repeats the
  start failure. Until then this bullet also said that nothing was nacked because `shutdown()`
  never ran; the release now nacks what the client had delivered itself.
- **A throwing `nackSplit` must not skip the stop, and a throwing `shutdown()` must not skip the
  wait** ([#350]) — `Closers.closeAll` in both `shutdown()` and `close()`, [#297]'s rule one and
  two levels further in. `closed` is set before the nack, so the old shape left `shutdown()`'s
  idempotence guard claiming a client had been asked to stop that had not, and `close()` then
  spent the entire budget waiting on it. **Argue this as robustness, not as a bug closed**:
  measured on 1.152.0, both production `AckHandle` flavours end in `SettableApiFuture.set`, which
  returns a `boolean`, and no ordinary nack failure has been demonstrated for them. Guava runs a
  direct listener of that future inside `set` and lets an `Error` from it propagate, so a throwing
  nack is not shown to be impossible either ([#2326], read in `google-cloud-pubsub` 1.161.0 and
  Guava 33.7.2). What the list
  buys is mostly over an `@Internal` SPI whose implementations need not all be ours. One level in,
  `nackSplit` itself nacks every handle of the split before it throws the first failure, since
  the handles are already out of its maps and one it skipped would be left to the client
  library's shutdown or lease expiry ([#2326]). `stopQuietly()` is deliberately
  **not** given the same list, and the reason is a property rather than scope-drawing: there, a
  stop that threw has started no shutdown for the wait to wait out, so skipping it is the right
  outcome. `removeSplit` has **no production path** (`SplitsRemoval` reaches a reader only
  through `SourceReaderBase`'s `eofRecordEvaluator` branch, which `PubSubSourceReader` supplies
  none for, and the enumerator never removes a split) — the reason `close()` holds the invariant
  is that it should hold it whoever calls it. That method reports a failure before it closes for
  the same reason: a split removed while paused carries one nothing has read, and removal,
  unlike teardown, happens while the job carries on.
- **A paused split is still watched** ([#348]). `PullSubscriber.checkFailure()`,
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
- **The failed-start release's `stopAsync()` is a no-op, and on most routes nothing running is left
  for it to release** ([#349], correcting the inference [#325] drew; refined by [#2291]). Since
  [#2263] the release also closes the client's transport (see Context); what follows is about the
  SDK stop. [#325]'s own half stands — Guava's `stopAsync()` is guarded by
  `state().compareTo(RUNNING) <= 0` and `FAILED` sorts last, so once the service has failed the call
  enters nothing. What was wrong was what followed from it: **when a connection reports its failure,
  the executors it cannot reach, and in production the channel, are released by the SDK itself.**
  `Subscriber.startStreamingConnections()` adds to every connection a listener whose `failed(...)`
  runs `runShutdown()` — `stopAllStreamingConnections`, `shutdownBackgroundResources`,
  `subscriberStub.shutdownNow()` — **before** it calls `notifyFailed`, so a connection that fails to
  start, or a stream that dies on a missing IAM grant, releases everything on the SDK's own path.
  That order is the listener's, not the subscriber's: when a connection fails to start, the startup
  thread's own `awaitRunning()` throws and its `notifyFailed` can report `FAILED` while the
  listener, which runs on the system executor, is still in `runShutdown()`, so the release can
  finish after the failure is reported ([#2291]). It still finishes. `doStart()`'s other failure,
  `GrpcSubscriberStub.create` throwing, leaves the system executor unreleased ([#2291]; this said
  "strands nothing" until then). The stub never exists, yet `Subscriber.Builder.build()` has already
  created that executor and the constructor has registered it for `runShutdown()`, which nothing
  runs on this path. When the throw comes from `ClientContext.create`, on the credentials or on
  building the transport channel, the executor has started no thread: both steps run before anything
  is scheduled on it, and a `ScheduledThreadPoolExecutor` starts a worker only for its first task.
  On the measured credentials failure, what is left is an object that becomes unreachable with the
  failed `Subscriber`. Two narrower cases of this route were read, not measured. A throw after
  `ClientContext.create` returns, which in 1.160.0 can come from mTLS certificate-provider discovery
  while the operations stub's settings are built, leaves the stream watchdog scheduled on that
  executor, the same residue as the synchronous route below. And a channel pool whose later channel
  fails to build leaves its earlier channels open, holding the executor, because the pool builds
  them one at a time with no rollback. **Owning that executor (`setSystemExecutorProvider`) so the
  release could shut it down was declined.** On the measured route there is no thread to stop. On
  the routes that leave the watchdog running, the late stub failure above, the startup thread itself
  failing to start (`doStart()`'s `new Thread(...).start()` throwing, for example at the JVM's
  native-thread limit), and a throw in the synchronous part of `startStreamingConnections` (see
  Evidence), owning it means taking over the sizing the SDK derives from `parallelPullCount`
  (`max(6, 2 * parallelPullCount)` threads), to cover routes the SDK does not cover itself; in
  production the stub's channels would stay open either way, while with an emulator the connector
  owns the one fixed channel, which the SDK neither sizes nor closes, and the release closes it.
  Until [#2291] this decision named the channel and the message executor (`setChannelProvider` /
  `setExecutorProvider`) instead. That would not have covered those routes, because the watchdog
  runs on the system executor rather than the message executor, and since [#2263] the connector
  supplies the channel provider anyway, which in production the SDK still sizes through
  `withPoolSize`. The release call is kept for the states where `stopAsync()` is genuinely not a
  no-op — a failure registering the listener leaves the service `NEW`, one racing a start leaves it
  `STARTING` — and because it costs a no-op otherwise.
- **A failed start returns what the client had already delivered** ([#2307]). The release assumed
  until then that a client which failed to start had received nothing, and both its `WARN` and the
  Pub/Sub guide said so. That is false. A connection is `RUNNING` once it has started its stream,
  without waiting for the service to answer (`StreamingSubscriberConnection.doStart()` ends in
  `initialize(); notifyStarted()`), and it hands on whatever the stream delivers, so it can deliver
  while the subscriber is still `STARTING` and the startup thread is still waiting for a later
  connection; when that one fails, the caller's `awaitRunning()` throws with the earlier deliveries
  already admitted to the buffer, the reader-wide budget (which also counts the unregistered split
  among its starting holders) and the ack tracker. The release therefore does what `shutdown()`
  does, in the same order: it latches `closed` and empties the buffer under the monitor every
  callback admits under, returns the reservation, nacks the split, and only then stops and waits for
  the client and closes its transport, all in one `closeAllSuppressing` list so a step that throws
  neither skips the rest nor, short of a JVM-fatal error, replaces the start failure. The latch is
  the half that a one-time release would miss. A delivery can still arrive after the start has
  thrown, from a connection the SDK has not stopped yet: the startup thread holds the connection
  list's lock across its `awaitRunning()` loop, and the failure listener's `runShutdown()` needs the
  same lock to stop the healthy connections (read in google-cloud-pubsub 1.160.0). With `closed`
  set, such a delivery is nacked before it reserves anything. The budget gets one step of its own,
  `releaseFailedStart`, that returns the reservation and the starting share together under its
  monitor, so no peer sees one without the other. It drops the share even when nothing is returned,
  because `tryReserve` adds a starting holder before it checks the limits, and it leaves any
  registered entry alone: the split has none on either caller's path, since a fresh assignment never
  registered it and a park unregistered it before the reopen. A second route reaches the same
  release without a `FAILED` client, read in Guava 33.6.0 rather than measured: a delivery that
  crosses the budget during the start asks the starting client to stop, Guava then reports it as
  `STOPPING`, and `awaitRunning()` throws on that too. The connector's nack can follow the SDK's own
  `NACK_IMMEDIATELY` nack of the same delivery; the SDK's `AckHandler.forget()` returns early the
  second time, so flow control is not released twice, and at worst a second modify-deadline request
  is queued (read in google-cloud-pubsub 1.160.0). **The release's own wait is logged at `DEBUG`
  when it only repeats the start failure**: on a `FAILED` client the wait throws, as soon as it
  takes the service's lock, an `IllegalStateException` carrying `failureCause()`, the cause the
  start's exception also carries, so the two are told apart by that cause's identity, as the
  re-report case above is (read in Guava 33.6.0 and api-common 2.73.0, and measured on the
  credentials failure in Evidence). By the same reading, that `WARN` accompanied every such failed
  start before this change. An expired wait or any other cause keeps the `WARN`, now saying that the
  release nacked what the reader still held of the client's deliveries. Four alternatives were
  declined. Gating admission until the start returns would nack, on every successful start, whatever
  the early connections delivered, spending delivery attempts and flow-control capacity on a healthy
  start to cover a failing one; holding those deliveries aside instead needs the same bound and the
  same release under another name. Registering the split in the budget before the start would remove
  only the starting share, and a registration can promote a pending park to a failure for a client
  that then fails to open. Rolling back in the roster has nothing to roll back, because the
  constructor threw before handing it a subscriber. Correcting only the record would have left the
  late-delivery window open and held by no test; it was the smaller change, since both callers
  normally fail the job and discard the reader with its budget and tracker (Evidence has the Flink
  2.x window where the reader misses that failure, [#2324]).
- **`BoundedShutdown` is deliberately not adopted here**, though the base module names this
  teardown as its nearest future adopter: [#265]'s problem was `Publisher.shutdown()` blocking
  the **task thread** uninterruptibly and without bound, and here the task thread's wait is
  already bounded — `stopAsync()` returns at once and `awaitTerminated` takes the budget — so
  the class would buy a thread and a residue counter for a bound that exists. **State the reason
  that way and not as "there is no unbounded wait"**, because there is one and `BoundedShutdown`
  could not take it either: `Subscriber.doStop()` spawns a bare `new Thread(...)` running
  `runShutdown()` under `SubscriberShutdownSettings.getTimeout()`, whose default is
  `Duration.ofSeconds(-1)`, no timeout (measured on 1.152.0, [#325]; the
  `setSubscriberShutdownSettings` setter that installs it is `@BetaApi` —
  [ADR-0141](0141-a-surfaces-stability-tier-is-set-by-what-can-reshape-its-inputs-and-outputs.md)'s
  inventory records it). A bare `new Thread`
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
    would otherwise report, stranded resources, the SDK releases itself when a connection fails to
    start: read from the source, through the same connection listener that [#349] measured
    releasing the executors when a stream dies after a successful start. And [#2291] measured that
    a client context whose credentials fail leaves the subscriber's executor without a thread.
  - **Counting the failed-start release's timeout into `subscriberShutdownsAbandoned` was
    declined too**, though by that name it would belong: `stopQuietly()` catches
    `TimeoutException | RuntimeException` in one branch deliberately, and splitting it to count
    half would trade the paragraph above for a branch whose two arms say the same sentence. The
    branch has told one case apart since [#2307], a wait that repeats the start failure, and that
    decides the log level and nothing else.
  - **The name says the property, not the origin.** `subscriberShutdownFailures` was the obvious
    spelling and is wrong at the edge: the branch also catches a streaming failure that landed
    after the reader's last `pullMessages`, which is not a shutdown failure but *is* one nothing
    consumed. `unreportedSubscriberFailures` reads better in English, and loses on a property of
    this repository's inventories that is worth stating because it decided a name: **a counter
    leads with the thing counted, a gauge leads with the state** — `messagesAcked`, `topicsCreated`,
    `filesStaged`, `tasksDeduplicated` against `pendingAcks`, `parkedSplits`, `inFlightEntries`,
    `inFlightDeadLetters` — so a counter spelled state-first reads as a gauge. Counted across the
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
- [#349] was **read from the 1.152.0 sources and measured for one route**:
  `PubSubSubscriberFailureReleaseITCase` starts a subscriber successfully, lets its streaming pull
  fail permanently on a missing subscription, and observes that the message executors it handed over
  come back shut down, over repeated attempts. That class tests the *vendor* on purpose, because a
  javadoc here asserts the vendor's behaviour, and the executor is its observable because a
  `FixedTransportChannelProvider` would take ownership away from the SDK and so measure us instead
  of it. Its observable is narrower than the claim, and [#2291] corrected the earlier wording
  "measured both ways": it sees only `isShutdown()` on the message executors, so neither the order
  of `runShutdown()` against `notifyFailed` nor `subscriberStub.shutdownNow()` is seen, and a
  connection that fails to start, which never reaches `RUNNING`, is source reading. Within
  `startStreamingConnections()`, what remains uncovered by the SDK itself is narrow and was never
  the claim: a throw out of the **synchronous** part of `startStreamingConnections` itself, which
  reaches `notifyFailed` without any connection reporting a failure to the listener that runs
  `runShutdown()`.
- [#2291] was **measured through the production factory**: `StreamingPullSubscriberFailedStartTest`
  builds the client through `DefaultSubscriberFactory` with a credentials provider that throws
  `IOException`, starts it through `StreamingPullSubscriber` so the failed-start release runs, and
  then reads the SDK's system executor. It had not been shut down, given a task, or started a
  thread (google-cloud-pubsub 1.160.0, gax 2.90.0, 2026-11-23). One failed start suffices: each
  builds its own executor, and nothing carries over between starts for a second to accumulate. A
  mutant that hands the executor one task fails the test, so a BOM bump that schedules work
  before the failure point fails it too. The transport-channel route was read, not run: the
  stream watchdog, which `ClientContext.create` schedules on that executor, is built after
  `getTransportChannel()` returns, and the channel pool, statically sized for this client,
  schedules a refresh there only if one is enabled and only after every initial channel exists.
- [#2307] was **measured both through a scripted client and against the real SDK**. A client
  that, inside its start, hands one message to the receiver of the production constructor and
  then throws left the budget holding one message and its 13 bytes with the split among its
  starting holders, one pending acknowledgement in the ack tracker, and no `nackSplit`. The real
  SDK was driven through the production `DefaultSubscriberFactory` with an emulator endpoint
  pointing at a localhost fake `google.pubsub.v1.Subscriber` service, whose every `StreamingPull`
  stream answers one message and then fails with `PERMISSION_DENIED`; 100 starts per row
  (google-cloud-pubsub 1.160.0, 2026-11-23):

  | `parallelPullCount` | Failed starts | Deliveries left per failed start |
  |---|---|---|
  | 1, 4 | 0 | none |
  | 16 | 93–99 | 4–9 |
  | 64 | 95–98 | 21–43 |

  Every failure arrived from `STARTING`; a subscriber reaching `RUNNING` and failing before the
  caller's `awaitRunning()` returned, the route the issue first described, was never observed. In
  the fake the SDK's own shutdown nacked about as many messages as were left behind. The real-SDK
  run is probabilistic, so it is recorded here rather than run per build. What holds the release is
  unit tests through the production constructor with a real ack tracker: deliveries admitted inside
  the start are returned and each handle nacked once, a delivery during the stop and one after the
  constructor has thrown are nacked without reserving or crossing a one-message budget, and a failed
  reopen after a park returns what the replacement admitted while the slot stays parked. The
  starting share is shown to leave by the peer's cap, not by usage, which reads zero either way. The
  `DEBUG` repeat is measured on the real SDK too: `StreamingPullSubscriberFailedStartTest`'s failed
  start, a credentials failure through `DefaultSubscriberFactory`, logs exactly that line. Each of
  nine mutations (no release, no latch, no nack, the nack after the stop, the share kept in the
  budget or by a plain `release`, the repeat test answering always or never, and the repeat test
  without its null guard) fails at least one of those tests. Read rather than measured: the release
  acknowledges nothing, and a leftover delivery is acknowledged only as any delivery is, after the
  record emitter has handled a copy of its message id that the reader drained; and both callers of
  the constructor, `SubscriberRoster.addSplit` and the reopen of a parked split, throw inside a
  Flink `SplitFetcher` task. Flink records the failure and wakes the reader, which raises it at its
  next error check; a throwaway probe showed that on flink-connector-base 2.2.1 and 1.20.5. When the
  failure is recorded between the reader's error check and its queue poll, the empty poll consumes
  the wake-up: on 2.2.1 the probe's reader was still waiting after 3 s, because the fetcher thread
  waits on a latch before the exit hook that would wake it again ([#2324], a Flink defect); on
  1.20.5 that hook woke it. The release this change adds runs before the start failure is
  propagated, so it does not depend on the reader failing.
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

[#118]: https://github.com/flink-gcp/flink-connector-gcp/issues/118
[#265]: https://github.com/flink-gcp/flink-connector-gcp/issues/265
[#297]: https://github.com/flink-gcp/flink-connector-gcp/issues/297
[#311]: https://github.com/flink-gcp/flink-connector-gcp/issues/311
[#325]: https://github.com/flink-gcp/flink-connector-gcp/issues/325
[#348]: https://github.com/flink-gcp/flink-connector-gcp/issues/348
[#349]: https://github.com/flink-gcp/flink-connector-gcp/issues/349
[#350]: https://github.com/flink-gcp/flink-connector-gcp/issues/350
[#351]: https://github.com/flink-gcp/flink-connector-gcp/issues/351
[#358]: https://github.com/flink-gcp/flink-connector-gcp/issues/358
[#755]: https://github.com/flink-gcp/flink-connector-gcp/issues/755
[#2263]: https://github.com/flink-gcp/flink-connector-gcp/issues/2263
[#2291]: https://github.com/flink-gcp/flink-connector-gcp/issues/2291
[#2302]: https://github.com/flink-gcp/flink-connector-gcp/issues/2302
[#2307]: https://github.com/flink-gcp/flink-connector-gcp/issues/2307
[#2324]: https://github.com/flink-gcp/flink-connector-gcp/issues/2324
[#2326]: https://github.com/flink-gcp/flink-connector-gcp/issues/2326
