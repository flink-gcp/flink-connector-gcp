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

# ADR-0066: A paused split's buffer is bounded by parking its subscriber

- Status: Accepted
- Date: 2026-08-08
- Issues: [#357]
- Modules: pubsub (source)
- Current behavior: `docs/content/docs/connectors/datastream/pubsub.md` (§ Watermark alignment);
  `docs/content/docs/reference/pubsub.md` for the two knobs

## Context

ADR-0012 settled that a paused split is still *watched*; what bounds what it *holds* was left as
the client library's flow control, and [#356]'s second review round found that claim false past
`maxAckExtensionPeriod`. Filed as [#357], P1: the failure mode is a TaskManager filling up while
the job looks healthy, since alignment holding a split is alignment working as asked.

## Decision

**When a paused split's buffer passes its bound, the reader stops that split's SDK client and
opens a fresh one when the split resumes.** The bound is two new `PubSubSubscriberOptions` knobs,
`pausedSplitBufferMaxMessages` and `pausedSplitBufferMaxBytes`, each defaulting to twice the
flow-control limit it shadows.

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

## Alternatives declined

- **Fail the job at a threshold.** Smallest change, keeps ADR-0012's watching trivially intact,
  and turns an eventual heap exhaustion into a diagnosable failure naming `withIdleness(...)`.
  Declined because it kills a job the pause would not otherwise have killed, and a restart lands
  in the same state — a crash loop until the watermark strategy is fixed.
- **Document a sizing rule and leave the buffer unbounded.** Cheapest and reversible. Declined on
  the measurement above: the growth has no ceiling, and the deployments most exposed are the ones
  that followed the page's own advice to size `flowControlMaxOutstandingElementCount` to
  peak-rate × checkpoint interval.
- **Nack what arrives once the buffer is full.** Bounds memory, keeps the client alive and so
  keeps [#348]'s watching. Declined because a nack is an immediate redelivery: the split would
  loop its backlog through delivery continuously, spending a dead-letter policy's
  `maxDeliveryAttempts` on messages nobody is consuming and diverting live data to the dead-letter
  topic.
- **Block in the receiver callback.** Ruled out by the design already recorded on
  `PubSubNotifyingPullSubscriber`: it stalls an ordering key's dispatch chain and holds a
  client-library thread.

## Consequences

- **[#348] does not hold while a split is parked.** A parked split has no client, so a subscription
  deleted or its access revoked *during* the pause is not noticed until the resume, where
  `subscriberOpener.open` throws. A failure recorded before the park is still reported, by the
  `checkFailure` at the head of the park's list. The delay is the remainder of the pause, which for
  the case this exists for is unbounded. The trade is explicit: today that detection is bought with
  an unbounded buffer, and a liveness probe would mean opening a client that immediately buffers
  again.
- **Re-emission on resume** is this split's records emitted since the last completed checkpoint —
  `nackSplit` drains pending, staged and checkpoint-bound alike. **At the default bound** it is
  normally empty: a park cannot happen before roughly one `maxAckExtensionPeriod` into the pause,
  and the split emits nothing while paused, so any checkpoint covering its pre-pause output has
  long completed; what is left is the case where checkpoints are not completing, which
  `pendingCheckpoints` already reports. **That argument is about the common case, not a
  guarantee** — a split that was draining until the moment it was paused can be parked on the next
  fetch, with the previous checkpoint still in flight, and its records are then re-emitted into a
  running pipeline. Lowering the bound makes that likelier rather than introducing it. Within
  at-least-once, and stated on the knob and the docs page rather than left to the reader.
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
- The same lapse reaches a split held back by sustained downstream **backpressure**, and a
  reader-driven park cannot see it: `FetchTask` blocks in `elementsQueue.put` (capacity 2), so
  `fetch()` is not entered and no guard placed there runs — which is equally true of today's
  paused-split failure check and `MissingCheckpointDetector`. Out of scope here and stated on the
  method rather than implied away.

[#348]: https://github.com/laughingman7743/flink-connector-gcp/issues/348
[#356]: https://github.com/laughingman7743/flink-connector-gcp/issues/356
[#357]: https://github.com/laughingman7743/flink-connector-gcp/issues/357
