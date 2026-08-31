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

# ADR-0066: Pub/Sub subscriber buffers are bounded independently of SDK flow control

- Status: Accepted
- Date: 2026-08-08, refined 2026-08-09 ([#377], [#440]) and 2026-08-29 ([#1138])
- Issues: [#357], [#377], [#440], [#1138]
- Modules: pubsub (source)
- Current behavior: `docs/content/docs/connectors/datastream/pubsub.md` (§ Subscriber options,
  Metrics and Watermark alignment); `docs/content/docs/reference/pubsub.md` for the four knobs

## Context

ADR-0012 settled that a paused split is still *watched*; what bounds what it *holds* was left as
the client library's flow control, and [#356]'s second review round found that claim false past
`maxAckExtensionPeriod`. Filed as [#357], P1: the failure mode is a TaskManager filling up while
the job looks healthy, since alignment holding a split is alignment working as asked.

[#377] then showed that the same lapse grows a subscriber deque when downstream is completely
stalled, and that Flink's fetcher retains another four drains at its default queue capacity.
The observability response left the subscriber deque unbounded because a failure recorded on the
fetcher still needs `pollNext()` to reach the job.
[#1138] closes that remaining pre-1.0 OOM gap with a callback-visible response and a coordinator
event that does not depend on the task or fetcher polling.

## Decision

**When a paused split's buffer passes its bound, the reader stops that split's SDK client and
opens a fresh one when the split resumes.** The bound is two new `PubSubSubscriberOptions` knobs,
`pausedSplitBufferMaxMessages` and `pausedSplitBufferMaxBytes`, each defaulting to twice the
flow-control limit it shadows.

**Every source reader also owns one hard aggregate budget across all of its subscriber deques.**
`subscriberBufferMaxMessages` defaults to 10000 and `subscriberBufferMaxBytes` defaults to 64 MiB.
The callback reserves both dimensions before adding the delivery to `PubSubAckTracker` or the
deque, so the delivery that would cross either bound is NACKed and never retained.

- **The response is non-blocking and stops intake before it reports.** The first crossing asks
  every subscriber in that reader to stop asynchronously.
  Later in-flight callbacks are also rejected and NACKed while those clients terminate, but the
  stopped streams cannot sustain a redelivery loop.
- **An entirely alignment-paused reader parks instead of failing.** The callback marks a
  reader-wide park request, stops all subscribers, and wakes the fetcher.
  The existing park lifecycle then checks failures, NACKs accepted deliveries, closes the clients
  and reopens them when their splits resume.
  A resume that races the park promotes the response to coordinator failure, because its subscriber
  has already received the asynchronous stop request and cannot safely remain active.
- **Any unpaused split makes the coordinator fail the job.** The callback sends a
  `SubscriberBufferLimitExceededEvent` through `SourceReaderContext` after stopping intake.
  `PubSubSplitEnumerator.handleSourceEvent` throws an actionable exception, and Flink's
  `SourceCoordinator.runInEventLoop` turns that exception into `context.failJob(...)`.
  This path stays live when complete downstream backpressure has stopped both `pollNext()` and
  `PubSubSplitReader.fetch()`.
- **The defaults are independent of split cardinality.** They bound one reader's subscriber
  aggregate rather than multiplying a per-subscriber allowance by the number of assigned splits.
  A constrained 256 MiB child-JVM probe holds the default 10000 messages with 4 KiB payloads and
  rejects the next delivery.
- **Flink-owned fetcher retention remains a separate envelope and now has separate gauges.**
  `fetcherBufferedMessages` and `fetcherBufferedBytes` count batches after they leave subscriber
  deques and before `SourceReaderBase` takes them.
  The message-count ceiling is `(source.reader.element.queue.capacity + 2) × maxRecordsPerFetch ×
  assigned splits`; the connector cannot impose a byte cap on queues Flink owns.
- **Checkpoint completion remains the Ack boundary.** An admitted and emitted record follows the
  existing staged/pending/checkpoint lifecycle unchanged.
  Only a delivery rejected before admission is NACKed directly.

- **The default is twice the flow-control limit it shadows, and the factor is the lapse's own
  size.** The client releases a whole window of permits per expiry wave, so the first wave carries
  the buffer to `2 × limit + 1` (measured: a 50-message window stepped to 101). The bound is
  therefore still crossed by the lapse and by nothing smaller, with no number chosen. The
  three-rung fallback is paused-split knob → twice the flow-control knob → twice the SDK's own
  default, and that last rung is **read live** from
  `Subscriber.Builder.getDefaultFlowControlSettings()` rather than mirrored as a constant.
  `DefaultSubscriberFactory` does mirror `maxAckExtensionPeriod`'s default, and the reason it has
  to is that *that* SDK constant is package-private; these are `public static` and production code
  already reads them, so a copy here could drift while buying nothing.
- **The bound is deliberately *not* the flow-control limit itself, and the draft's argument that it
  could be was wrong.** That argument was "the deque is a subset of what the client holds
  outstanding, so it cannot exceed the limit under healthy operation" — which, with `exceededBy`
  strictly greater, left one message of headroom. Round two of self-review found three ways a
  healthy split exceeds it, each verified against `google-cloud-pubsub` 1.152.0 and gax 2.x:
  gax's `BlockingSemaphore.acquirePartial` clamps an oversized byte reservation to the limit and
  lets its permits go negative, so **one** message larger than `flowControlMaxOutstandingRequestBytes`
  already passes a bound set there; `MessageDispatcher.processBatch` reserves and *then* calls
  `addDeliveryInfoCount`, so on a dead-letter subscription every buffered message is a few dozen
  bytes larger than what was reserved for it; and a redelivery is buffered beside the copy it
  supersedes while `PubSubAckTracker.addPendingAck` nacks the superseded handle, releasing that
  delivery's permit. Each would have parked a healthy split — the failure mode the feature exists
  to avoid, arrived at from the other side.
- **Both dimensions, and either one crossing is enough.** Which limit binds depends on message
  size, so a single-dimension bound is unreachable in the other's case: at 1 KB messages flow
  control admits 1000 and a byte-only bound at 100 MB waits ~100 h of lapse; at 1 MB messages it
  admits 100 and a message-only bound at 1000 waits until 1 GB — past the heap it exists to
  protect.
- **The failure check runs before the park, and the park's own list checks again.** Parking runs
  `close()`, which absorbs what the client raises (ADR-0003), so a paused split that is both dead
  and overfull has to fail the job rather than be quietly stopped. Each parked split's
  `checkFailure` therefore heads its own entries in the release list below — its last chance, since
  after the park there is no client left to watch.
- **The close is inline on the fetcher thread, not deferred.** Under `PER_KEY` a deferred close
  could leave the old streaming-pull connection alive past the resume, and
  `DefaultSubscriberFactory` forces `parallelPullCount(1)` there precisely to keep one connection
  per ordered subscription; closing inline makes the old client terminated before the new one
  starts, or `shutdownTimeout` has elapsed and the reader has given up on it with a `WARN` —
  `awaitTerminated` absorbs its own timeout, so this is a bounded wait rather than a guarantee, and
  what a timeout costs under `PER_KEY` is a stall while the old leases expire, not reordering. The
  cost is bounded either way — at most `shutdownTimeout` (5 s) once per pause episode, and a parked
  split cannot grow again to re-park.
- **Every split parked in one fetch goes through one `Closers.closeAll` list, every shutdown
  before any close** — the shape and the reason of `close()` (#297), and not an optimisation but
  the same defect avoided. Alignment pauses a subtask's splits as a *group* and they cross the
  bound in the same wave, so parking them one at a time would spend `shutdownTimeout` per split,
  serially, on the fetcher thread: at six paused splits that is the 30 s
  `source.reader.close.timeout` budget, inside a single fetch, with `wakeUp()` unable to shorten
  it. Found in review after the per-split shape had shipped in the draft.
- **The reopen is eager, in `pauseOrResumeSplits`**, mirroring `addSplit`: same seam, same
  `IOException` → `RuntimeException` wrap, same thread. A drain should not throw for a reason that
  is not about draining (ADR-0012), and a reopen failure fails the job exactly as an assignment
  that cannot open its subscriber does.
- **`pauseOrResumeSplits` ends by raising the data-available signal.** A split paused while it
  still holds messages an earlier fetch did not drain is already over its bound with its signal
  spent, and pausing stops the client delivering — so the next fetch drains nothing and waits
  indefinitely, with every guard sitting after that wait. Growth and failure each raise their own
  signal, so this is the pause's own case and not a second belt for theirs.
- **Two metrics, one of each kind**: `parkedSplits` (gauge) is what an operator alerts on, and
  `splitsParked` (counter) is what survives a park and resume falling between two scrapes. Both
  live in `PubSubSourceReaderMetrics` rather than the split reader, because a fetcher may be
  rebuilt over a reader's life while the subtask has one gauge either way.

## Evidence

Measured 2026-08-08 by `PubSubPausedSplitBufferITCase` against `google-cloud-pubsub` 1.152.0 and
the Pub/Sub emulator, five runs at a 50-message flow-control window, `maxAckExtensionPeriod` 20 s,
ack deadline 10 s, 400-message backlog.

- **The premise.** The buffer sat at exactly the flow-control limit (50) at 5 s in all five runs,
  then stepped to 100–101 at 10.2–10.4 s and to 152–174 at 20.4–20.5 s: **about one flow-control
  window per wave, one wave per `maxAckExtensionPeriod` minus one deadline period**. Two waves is
  what the test observes — the "no ceiling" claim rests on the mechanism, not on the sample, and
  the emulator note below is a direct counter-observation to it. The mechanism read from the sources matches — `processReceivedMessages` stamps
  `totalExpiration` at *receipt*, `extendDeadlines` forgets a message it can no longer extend past
  the next deadline, and `forget()` calls `flowController.release(...)`. Note the wave lands one
  deadline period *before* `totalExpiration`, not after it.
- **The fix.** With the bound at 60, the park fired with 121 and 104 buffered over two runs,
  nacked all of it (so `pendingAcks` returned to 0), and the resumed split consumed again.
- **The bound is per fetch, so the overshoot is one delivery burst.** 104–121 against 60 is the
  wave arriving between two fetches, and it varies with where the fetch lands in it; it is bounded
  in turn by what the client will deliver at once, which is its flow-control window. Stated on the
  knob and the docs page rather than smoothed over.
- **Two emulator deviations, recorded because the rule is that an emulator is not an authority.**
  It never redelivered a message whose lease had expired (`messagesNacked` stayed 0 across 110 s
  with leases long gone), so the half of [#357] about redelivered copies being buffered *alongside*
  the originals is unverified here and would need the gated real-GCP suite. And at a 10-message
  window the growth stopped dead after a single wave (10 → 21, then flat for 110 s over two runs)
  where 50 repeats on schedule — which is why the committed test uses 50.

Measured 2026-08-09 for [#377] by `PubSubBackpressuredSplitBufferITCase` (emulator, six runs, same
constants, three arms drained concurrently), `PubSubBackpressuredSplitBufferRealGcpITCase` (real
Pub/Sub, two runs) and `PubSubBackpressuredReaderGuardTest`.

- **Pausing is not the condition; a stalled drain is.** An arm that was never paused and never
  fetched reproduced this ADR's own series (50, ~101 at 10.3 s, ~152 at 20.5 s). What the drain rate
  changes is everything after that: all three arms were *delivered* 151–195 messages over 40 s on
  one machine, and were left holding 151–179 (no drain), 112–154 (1/s) and **zero** (15/s, peaking
  at 46 in the first delivery). *How much* is delivered belongs to the runner, not to the connector:
  on an unrelated commit a CI runner delivered 318, 175 and 300 to the same three arms, the fast one
  alone given more than any local run's total. What survives across both is the holding, which is
  the finding ([#440]). The break-even follows from the permit accounting rather than from the sample — an
  acknowledgement only covers what was already drained, so the only drain-independent source of
  permits is expiry — and is `W / (maxAckExtensionPeriod − one lease extension)`: **about 0.28
  messages a second at the production defaults.** The subtracted term is `MessageDispatcher`'s own
  `messageDeadlineSeconds`, which starts at `Subscriber.MIN_STREAM_ACK_DEADLINE` (10 s) and is
  recomputed from a percentile of observed acknowledgement latency, bounded by
  `minDurationPerAckExtension`/`maxDurationPerAckExtension` — **not** the subscription's
  `ackDeadlineSeconds`, which the two coincide with only because the test sets both to 10 s. That
  it adapts is also why the observed refill varied between arms (3.9–8.2 messages a second where
  the formula says 5), so the break-even is an order of magnitude, not a constant. Any job making real progress is above it, which is what
  narrows the backpressure case to a downstream that has stopped altogether.
- **The redelivery channel is real, it dominates, and the emulator's silence hid it.** Against the
  service, an arm draining 1/s over 90 s was delivered 369 and 462 messages while holding 279 and
  371 — and **215 and 338 of those deliveries were supersedes**, a redelivered copy of a message the
  connector was still holding. So the majority of what a backpressured split is handed, and of what
  its buffer then carries, is churn rather than new data. Of its 90 drained messages only 74 were
  distinct. A redelivered copy is appended beside the one it supersedes while `addPendingAck` nacks
  the superseded handle, returning that delivery's permit, so the channel adds a buffered message at
  no permit cost — which is also why `messagesReceived` is not a delivery total the permit
  accounting bounds (a draft asserted such a ceiling and CI produced 327 against 250; a second one
  barred the fast arm's *drain* at half its requested rate, which is the same claim in another
  shape, and CI met that bar exactly at 300 against 300 — [#440]). Two further
  consequences: duplicates reach a *running* pipeline rather than only a restart, and the two-wave
  ceiling above is an emulator artifact — the service kept going. On the emulator the same
  measurement reads `messagesNacked` of exactly zero, which is that recorded deviation asserted
  rather than assumed.
- **Only a full stall blinds the guards, and there nothing could report anyway.** From a frozen
  loop, one poll frees one element-queue slot, lets one `put` through and runs `fetch()` once — the
  detector fired on that fetch in both runs. So ordinary backpressure delays a guard by one drain
  interval. Under a *full* stall the task thread is not calling `pollNext` either, and `pollNext` is
  the only path a fetcher's recorded failure has to the job (`SourceReaderBase` calls
  `checkErrors()` at two places, both under it), so evaluating the guards on another thread would
  buy no earlier report. That is why [#377] added observability and no second bound.
- **The buffer is not the reader's whole footprint.** A frozen loop at `maxRecordsPerFetch` 1000 and
  an element-queue capacity of 2 had pulled 4000 messages, emitted 1, and was holding **3999** in
  the element queue, the current fetch and the batch it could not hand over. `bufferUsage()` sees
  none of it, so the bound above is evaluated against a number that understates the reader by up to
  `(capacity + 2) × maxRecordsPerFetch × splits` — the queue, plus those last two, which is where
  the fourth batch in the measurement comes from, and each of the three holds one drain of every
  assigned split rather than of one.

Verified 2026-08-29 for [#1138] by `SubscriberBufferBudgetTest`,
`PubSubBackpressuredReaderGuardTest`, `SubscriberBufferMemoryBoundaryTest` and the slow emulator
`PubSubBackpressuredSplitBufferITCase`.

- **Concurrent callbacks stop at the exact aggregate boundary.** Sixty-four callback threads
  racing a 17-message budget admitted exactly 17, and count/byte boundary tests rejected the first
  crossing without changing retained usage.
- **The frozen-downstream series plateaus after acknowledgement extension expires.** The emulator
  arm never calls `fetch()`, runs for twice its 20-second `maxAckExtensionPeriod`, reaches the
  75-message hard cap and remains there for sampled observations after its subscriber is stopped.
  The concurrently drained control deliberately keeps its hard limit out of the way, continues
  beyond two flow-control windows, and ends below one window.
  Four local runs of that repaired control on 2026-08-31 received and drained between 167 and 182
  messages, recorded no NACKs and ended at zero buffered messages.
  Those observations do not bound final-buffer timing for every possible delivery wave; all five
  Weekly version/JDK rows are the acceptance check for that residual exposure.
  [Weekly run 33336508126] disproved the earlier same-cap control assumption: the control's recorded
  delivery count jumped from 117 to 289 at its 20.3-second step, with 75 callbacks retained and 97
  rejected at the 75-message cap while the control drained 192 messages and ended empty.
  The 250 ms sampler reported a peak of 46, so only the hard-limit event and rejection count
  captured the transient crossing.
  The log did not record callback message IDs, so it does not prove that all 172 callbacks were
  distinct; it does prove that a draining control can cross a 75-message cap.
  Average drain rate does not bound the callbacks retained during a delivery wave.
- **The standalone subscriber-buffer probe fits in a 256 MiB heap.** A child JVM under `-Xmx256m`
  retains 10000 distinct messages with 4 KiB payloads and their pending acknowledgement handles,
  then rejects message 10001 without exhausting the heap.
  This is a boundary regression, not a TaskManager sizing result: Flink, the SDK client, fetcher
  queues and user code all consume additional heap that the probe deliberately excludes.
- **Fetcher retention is separately visible.** The deterministic frozen-fetcher test still
  measures 3999 records outside the subscriber deque and now observes the same count through
  `fetcherBufferedMessages`, with a positive serialized-byte gauge.
- **The real-service contract has a gated, small-volume test.** Four messages under one ordering
  key and a five-attempt dead-letter policy cross a one-message cap.
  The test requires their complete ordered redelivery after the stop and requires the dead-letter
  observer to remain empty, covering the service behavior the emulator cannot supply.
  `PubSubBackpressuredSplitBufferRealGcpITCase` passed against real Pub/Sub on 2026-08-29: the
  callback crossed the cap, all four messages were then pulled in their original order with their
  ordering key intact, and the dead-letter observer remained empty for the 20-second observation
  window.

## Alternatives declined

- **Fail an alignment-paused reader at a threshold.** Smallest original change, keeps ADR-0012's
  watching trivially intact, and turns an eventual heap exhaustion into a diagnosable failure
  naming `withIdleness(...)`.
  Declined for the paused-only case because it kills a job alignment would not otherwise have
  killed, and a restart lands in the same state.
  The #1138 refinement does choose coordinator failure when any split is unpaused, because a total
  downstream stall has no safe resume event and no task-thread path capable of surfacing a local
  failure.
- **Document a sizing rule and leave the buffer unbounded.** Cheapest and reversible. Declined on
  the measurement above: the growth has no ceiling, and the deployments most exposed are the ones
  that followed the page's own advice to size `flowControlMaxOutstandingElementCount` to
  peak-rate × checkpoint interval.
- **Keep the client alive and NACK everything after the buffer is full.** Bounds memory and keeps
  [#348]'s watching.
  Declined because an immediate redelivery would loop continuously, spend a dead-letter policy's
  `maxDeliveryAttempts` on messages nobody is consuming and divert live data to the dead-letter
  topic.
  The chosen response NACKs the crossing delivery but stops all reader subscribers in the same
  callback response, so there is no live intake to sustain that loop.
- **Block in the receiver callback.** Ruled out by the design already recorded on
  `StreamingPullSubscriber` (then named `PubSubNotifyingPullSubscriber`): it stalls an ordering
  key's dispatch chain and holds a client-library thread.

## Consequences

- **[#348] does not hold while a split is parked.** A parked split has no client, so a subscription
  deleted or its access revoked *during* the pause is not noticed until the resume, where
  `subscriberOpener.open` throws. A failure recorded before the park is still reported, by the
  `checkFailure` at the head of the park's list. The delay is the remainder of the pause, which for
  the case this exists for is unbounded. The trade is explicit: today that detection is bought with
  an unbounded buffer, and a liveness probe would mean opening a client that immediately buffers
  again.
- **Re-emission on resume** is this split's records emitted since the last completed checkpoint —
  `nackSplit` drains pending, staged and checkpoint-bound alike. The per-split default normally takes
  roughly one `maxAckExtensionPeriod` of continuous pause to park, by which time a checkpoint often
  covers pre-pause output. The reader-wide hard bound is different: it can park every paused split as
  soon as an ordinary finite pause crosses the aggregate count or byte cap. A split that was draining
  until the pause can therefore be parked with its previous checkpoint still in flight even under
  the defaults, and its records are then re-emitted into a running pipeline. Lowering either bound
  makes that likelier rather than introducing it. This remains within at-least-once and is stated on
  the knobs and docs page rather than left to the reader.
- **`PER_KEY` replays, never reorders.** Pub/Sub resumes an ordered key at its earliest unacked
  message, so the reopened client delivers the same run in the same order — which rests on the old
  client being terminated first, and is why the close is inline.
- **Each park costs one delivery attempt against a dead-letter policy's `maxDeliveryAttempts` for
  every message the split holds in any state** — `nackSplit` drains pending, staged and
  checkpoint-bound alike, so it covers records the pipeline already emitted, not only buffered
  ones. One park per pause episode (a parked split holds nothing, so it cannot re-park until it has
  resumed and refilled), against a default `maxDeliveryAttempts` of 5. `messagesNacked` is where
  the attempt burn is visible. Under `PER_KEY` nothing else pulls those messages meanwhile.
- **`MissingCheckpointDetector` sees the outstanding count drop at a park**, so on a job that never
  checkpoints *and* whose splits are aligned-paused it fires later or not at all. It is the
  detector behaving as specified — the state it exists to catch is a reader *holding* messages with
  no checkpoint coming, and a parked reader holds none — but it is a narrower guard than before.
- **`pendingAcks` stops being the alerting signal** the docs named for this: it climbs and then
  falls at each park. `parkedSplits` replaces it there.
- The same lapse reaches a split held back by sustained downstream **backpressure**, where a
  reader-driven park cannot see it: `FetchTask` keeps its `lastRecords` until `elementsQueue.put`
  succeeds (capacity 2) and skips `fetch()` while it holds one, so no guard placed there runs.
  The callback-side hard budget now stops the subscribers without that loop and sends the failure
  through the source coordinator.
  `bufferedMessages` / `bufferedBytes` report the bounded subscriber aggregate, while
  `fetcherBufferedMessages` / `fetcherBufferedBytes` report the separate Flink-owned footprint.

[#348]: https://github.com/flink-gcp/flink-connector-gcp/issues/348
[#356]: https://github.com/flink-gcp/flink-connector-gcp/issues/356
[#357]: https://github.com/flink-gcp/flink-connector-gcp/issues/357
[#377]: https://github.com/flink-gcp/flink-connector-gcp/issues/377
[#440]: https://github.com/flink-gcp/flink-connector-gcp/issues/440
[#1138]: https://github.com/flink-gcp/flink-connector-gcp/issues/1138
[Weekly run 33336508126]: https://github.com/flink-gcp/flink-connector-gcp/actions/runs/33336508126
