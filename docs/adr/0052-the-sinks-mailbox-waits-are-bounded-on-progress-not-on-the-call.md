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

# ADR-0052: The Pub/Sub sink's mailbox waits are bounded on progress, not on the call

- Status: Accepted
- Date: 2026-08-07; revised by [#755] (2026-08-16)
- Issues: [#333], [#755]
- Modules: pubsub
- Current behavior: `docs/content/docs/connectors/datastream/pubsub.md` (the progress-timeout
  section); `docs/content/docs/reference/pubsub.md` for the knob

## Context

ADR-0009 left the sink's own `drainInFlight()` as the unbounded leg that dominates what a
checkpoint spends — unbounded outright under `enableMessageOrdering` — filed as [#333].

## Decision

`PubSubPublisherOptions.publishProgressTimeout`, 600 s, spent by the admission gate and the drain
alike — `InFlightTracker.awaitCapacity` and `drainToEmpty` since [#755] moved them there — restarted
by every completion the publisher reports.

- **The bound had to cover `write` too, and that reverses the obvious scope.** Which leg a
  stalled sink parks in depends only on whether the in-flight caps fill before the barrier
  arrives: from the task thread's own stack at 5000 rec/s with a 1 s interval, at the
  **shipped** cap of 1000 it was in the admission gate ← `PubSubWriter.write`, and only at a cap
  large enough for the barrier to win was it in the drain ← `flush` ←
  `prepareSnapshotPreBarrier`.
- **Progress-based**: a slow-but-answering topic restarts the budget and never spends it, so only
  a publisher that has stopped answering fails — which is what answers the issue's own
  counter-argument, that blocking on the sink's own writes is legitimate backpressure. It costs
  one thing, stated on the docs page rather than hidden: this does **not** bound what a checkpoint
  spends in total, since a repair adds a sequential drain per attempt (measured: 1 +
  `recoveryMaxAttempts`, following the knob 3→4 and 6→7; an isolation pass 1 per parked message,
  5→7 and 8→10; and the drains each pay a full publish latency, 4 × 250 ms = 1022/1032 ms,
  4 × 500 ms = 2020/2023 ms).
- **`MailboxExecutor` has no timed `yield()`** — only the blocking one and `tryYield()` — so the
  wait is `tryYield` plus a 1 ms park, reached only while nothing is arriving. The blocking
  `yield()` cannot be used at all here, since a stalled publisher sends no mail and the deadline
  would never be read. Two consequences of that swap, both found in review rather than by
  construction. **`tryYield()` does not look at the interrupt flag where `yield()` took the
  mailbox lock interruptibly**, so the loop reads `Thread.interrupted()` itself, at the top and
  on every pass; without it a cancellation arriving while mails keep coming would surface as the
  budget expiring, with the wrong exception and up to ten minutes late. And **the budget is read
  only once `tryYield` has come back empty**, a genuine trade taken deliberately: reading it
  first lets a wait expire while work it has not yet done would have ended it — a completion
  mail queued behind other work is a publish that *already succeeded* — so a busy task thread
  would fail the job blaming an unreachable topic. Reading it after means a mailbox saturated
  for the whole budget defers the check, so a stall behind continuous unrelated traffic is
  noticed late rather than never. Failing a healthy job is the worse of the two. The first draft
  had this the other way round and a test caught it:
  `workTheMailboxStillHasToDoIsNotCountedAgainstTheBudget` is the pin, and
  `unrelatedMailTrafficDelaysTheBudgetButDoesNotDefeatIt` is what says the cost is a delay and
  not a defeat (real traffic has gaps, and any gap is a reading).
- **Progress is stamped in the completion callback, on the SDK's thread, not when the mail
  runs** — the one field of the tracker *written* off the task thread, hence `volatile` (the two counters are read off it as well, by the metric reporter). What
  the budget asks is whether the publisher is still answering, and a mail enqueued but not yet
  dequeued already answers it. It is stamped in the constructor too, so it is always a real
  `nanoTime` reading: the zero default is not one, and the comparison against `waitStartNanos`
  is a subtraction, only meaningful between readings — on a JVM whose `nanoTime` origin is
  negative the zero default would have disabled the bound entirely.
- **The budget starts at the later of "this wait began" and "a publish last completed"**, or an
  idle stream — nothing published for longer than the budget — would fail on its next flush.
- **Blocking at the in-flight cap sends what the SDK is still batching, once.** A message counts
  against the caps from the moment the publisher accepts it, which is before it goes anywhere, so
  at the cap every in-flight message may still be inside a batch — and the writer, holding the
  task thread, cannot add the message that would trip the size threshold instead. Without the
  flush the batcher's own `batchDelayThreshold` is a term *inside* this budget, and a delay
  configured longer than it expires the wait on a reachable topic, blaming it for messages the
  writer never sent. Flushing is right here and nowhere else on the record path — at the cap nothing more can be admitted, so anything still buffered is exactly
  what the wait is waiting for. Once per wait rather than per pass, since the task thread is
  blocked and no publish can join a batch while it waits; the count is pinned by
  `aWriteAtTheInFlightCapGivesUpOnTheSameBudget`, whose wait makes hundreds of passes, and not by
  the flush's own test, whose wait ends on its first.

## Alternatives declined

- **A flush-only bound**, which is what [#333] asks for and what the plan for it assumed. Declined
  on the stack measurement above: at the shipped in-flight cap a stalled sink parks in `write`, so
  a bound on the checkpoint drain alone would never fire for it and the job would stay as green and
  as frozen as before.
- **A deadline on the call rather than on the absence of progress.** Declined because it cannot
  tell a slow topic from a stopped one, so it would fail jobs the SDK's retries were about to
  rescue — the issue's own counter-argument, and the reason a bound here was in question at all.
- **Matching ADR-0009's 60 s**, so that the failure lands inside Flink's checkpoint budget rather
  than racing it. Declined: with ordering the SDK retries forever, so a budget short enough to beat
  that clock is also short enough to turn a three-minute Pub/Sub incident — which 600 s rides out
  untouched, because the publishes simply succeed when it ends — into a restart loop, and to make
  ordering *more* fragile than not using it. The two knobs are not the same measurement either:
  `flushTimeout` is a deadline on one call, which a slow-but-working topic spends, while this one
  only advances when nothing completes. The signal that default was wanted for is now a log line.
- **A `build()` cross-check rejecting `batchDelayThreshold >= publishProgressTimeout`.** Declined:
  it rules out a legitimate pairing to work around a gap that flushing the batcher closes outright.
- **A per-wait flag for the stalled-wait warning.** Declined for a writer-wide field: a wait is not
  an incident, and the isolation pass drains once per parked message over a batch that runs to
  about twice `maxInFlightMessages`, so per-wait would put a thousand lines in the log for one
  `flush`.
- **A gated real-GCP class.** Declined on a property rather than a cost — see Consequences.

## Evidence

Measured 2026-08-07 against an unreachable endpoint, and every number decided something:

- **The unordered path already self-terminates**: a flush ends at the SDK's publish retry budget
  — 9987/9954 ms at a 10 s `retryTotalTimeout`, 19958/19993 ms at 20 s, **591062 ms at the
  shipped defaults** — and `UNAVAILABLE` classifies `FATAL`, so it fails the job by itself. The
  delays have to be pinned to observe this: gax draws its retry delay uniformly on `[0, delay)`
  and `shouldRetry` subtracts the draw from the remaining budget (`ExponentialRetryAlgorithm`,
  2.82.0), so a nominal budget is always observed *below* itself and a 10-vs-20 ratio does not
  discriminate without `retryInitialDelay = retryMaxDelay = 100 ms, multiplier 1.0`.
- **The ordered path has no self-terminating condition at all** — still waiting at 700 s, past
  the 591 s the unordered default had already died of, which is the only alternative hypothesis
  worth testing. So this knob is the only thing *inside the sink* that ends an ordered outage —
  outside it, Flink's checkpoint timeout still does at its default, which the next bullet measures,
  and stops doing the moment `tolerable-failed-checkpoints` is raised.
- **What Flink does without this knob**, since it is what makes [#333] a P0 rather than a
  nuisance: at defaults the *checkpoint* timeout ends the job (measured: terminal at 25791/25319
  ms for a 20 s timeout and 45562/45959 ms for 40 s — the ~20 s delta is what identifies the
  mechanism), so an ordered outage costs 600 s of a stalled job and then dies of
  `Checkpoint expired`, naming nothing about Pub/Sub. Raise
  `execution.checkpointing.tolerable-failed-checkpoints` above 0 and **nothing ends it**: the
  job stayed `RUNNING` with its record count frozen (11462 → 11462, 10658 → 10658 over a further
  60 s) while checkpoint after checkpoint expired. The control that pins that on ordering rather
  than on the harness is the identical job with ordering off, which dies of
  `UnavailableException` whatever the checkpoint settings are.
- Cancellation was measured and is **not** the failure mode: the mailbox wait is interruptible,
  so a stalled job cancelled reached terminal in 5036/5037 ms — exactly its `shutdownTimeout`,
  nowhere near `task.cancellation.timeout`. The discriminating half was run too (2 s
  cancellation timeout against a 60 s close): the watchdog does fire then, `Task did not exit
  gracefully within 2 + seconds.` on `Sink: Writer (1/1)#0`, so it was armed and simply is not
  what this trips.

## Consequences

**What the budget does not cover, stated because the wording invites the opposite reading.** It
bounds the two waits the writer makes through the tracker and nothing else on the task thread: a user
`DestinationResolver`, serializer, `FailureHandler` or `DeadLetterQueue` runs there too — a handler
inside the wait itself, via a mailbox mail — and the task thread cannot bound code it is executing.
The built-in `PubSubDeadLetterQueue` bounds itself (`flushTimeout`), so the exposure is a
user-supplied one; the docs page says so, and the `FailureHandler` contract is where a duration
obligation would belong if one is ever added. `topicAdmin.createTopic` is outside it as well.

**The default races Flink's own, and that was accepted rather than missed.** 600 s is also
`execution.checkpointing.timeout`'s default, so at stock settings the checkpoint timeout is what an
operator sees first — `Checkpoint expired`, naming nothing about Pub/Sub. ADR-0009 faced the same
coincidence for the dead-letter queue and resolved it the other way, choosing 60 s so the failure
lands *inside* the checkpoint budget. The difference is what the two budgets are measured against:
the queue's is a side channel with no natural scale, while this one is chosen to equal what the
unordered path already spends before the client gives up, so that ordering costs a job no
*additional* strictness. The case the knob exists for — `tolerable-failed-checkpoints` above 0,
where nothing else fails the job at all — is unaffected by the race. Operators who want the
Pub/Sub-named failure first are told to set it below their checkpoint timeout.

**A stalled wait warns at a tenth of the budget, and that is the answer to the default's race
rather than a smaller default.** The obvious counters need not report this state — a publish that never answers is never counted as
a failure, so `errorClass.*.errors` and `numRecordsSendErrors` can sit unchanged throughout, at
whatever the repairs before the stall left them; "stay at zero" would be too strong, since a stall
usually grows out of counted failures — so without a line the first thing anyone sees is the job dying, at the
shipped default ten minutes later and possibly of `Checkpoint expired` instead. A `WARN` naming Pub/Sub, the wait and the in-flight
count, at `publishProgressTimeout / 10` = 60 s by default and **at most once per that interval
across the tracker, of which the writer has one** — a field, not a per-wait flag, because a wait is not an incident: the isolation
pass drains once per parked message and a parked batch runs to ~2x `maxInFlightMessages`, so
per-wait would put a thousand lines in the log for one `flush`. The operator gets the early, Pub/Sub-named signal from the log rather than from a
failure, and keeps the tolerance a longer budget buys. `inFlightMessages` pegged at the cap with `numRecordsSend`
flat remains the metric-side signal; a "time since last completion" gauge over `lastCompletionNanos`
is the obvious next step if this proves hard to operate, and is not built here because nothing has
operated it yet.

**No gated real-GCP class, and the reason is a property rather than a cost**: the unbounded case
needs a *sustained retryable* failure, and every failure the real service can be made to give
(`INVALID_ARGUMENT`, `PERMISSION_DENIED`, `NOT_FOUND`) is non-retryable and resolves at once,
while `RESOURCE_EXHAUSTED` means deliberately exhausting billed quota. A refused connection is
`UNAVAILABLE`, on the retryable list the `Publisher` constructor installs, so it drives the
identical algorithm with the identical settings. What *would* earn a gated class is a fix that
abandons in-flight publishes; this one does not.

[#333]: https://github.com/flink-gcp/flink-connector-gcp/issues/333
[#755]: https://github.com/flink-gcp/flink-connector-gcp/issues/755
