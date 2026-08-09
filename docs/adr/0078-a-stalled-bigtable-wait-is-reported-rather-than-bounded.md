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

# ADR-0078: A stalled Bigtable wait is reported rather than bounded

- Status: Accepted
- Date: 2026-08-09
- Issues: [#431]
- Modules: bigtable (`sink.writer`)
- Current behavior: `docs/content/docs/connectors/datastream/bigtable.md` § Tuning

## Context / Evidence

`BigtableWriter`'s two mailbox waits — `awaitCapacity()` on the record path and `drainInFlight()` at
a checkpoint — ran `mailboxExecutor.yield()` with no bound and no report. [#431] was filed to port
ADR-0052's `publishProgressTimeout` from the Pub/Sub sink, and asked for a measurement first. The
measurement retired most of the reason for the port.

**The waits are already interruptible.** `MailboxExecutorImpl.yield()` delegates to
`TaskMailboxImpl.take(int)`, which takes the lock with `lockInterruptibly()` and waits on
`notEmpty.await(1, TimeUnit.SECONDS)` — a `Condition.await`, so interruptible; `Task.TaskCanceler`
interrupts the executing thread through `maybeInterruptOnCancel(...)` and repeats it periodically
(Flink 2.2.1 sources, read 2026-08-09). A wait therefore ends on cancellation and
`task.cancellation.timeout` is not breached, which is one of the two things [#431] was filed on.

**The futures terminate.** A probe against a real `google-cloud-bigtable` 2.80.0 client — one
mutation through a real bulk mutation batcher, timing the entry future, with the retry settings read
at run time (`totalTimeout=PT10M, maxRpcTimeout=PT1M, maxAttempts=0, jittered=true`):

| Stall shape | Terminated after | With |
|---|---|---|
| Black hole — TCP accepted, never answered | **601 111 ms** (10 min 1 s) | `DeadlineExceededException` |
| Connection refused — a misconfigured endpoint | **586 143 ms** (9 min 46 s) | `UnavailableException` |

So there is no unbounded wait to bound: the client gives up on its own.

**What survives is ten minutes of silence, ending in a race the operator can lose.** Flink's
defaults are `execution.checkpointing.timeout` = 10 min and
`execution.checkpointing.tolerable-failed-checkpoints` = 0, so one expired checkpoint fails the job
(`CheckpointingOptions`, 2.2.1). That puts two ten-minute clocks against each other, and which wins
decides whether the failure names this connector:

- **A batch first sent by the flush itself gives the checkpoint the earlier clock**: the checkpoint
  clock starts at trigger time and that batch's RPC clock starts after the barrier arrives, so the
  black-hole arm's 10 min 1 s lands *after* the checkpoint expires. The job then dies with
  `Checkpoint expired before completing`, naming nothing about Bigtable, a second before the writer
  was going to say exactly what was wrong.
- **A batch already in flight when the barrier arrived** — pushed earlier by the element threshold —
  started its clock first, so its failure can instead arrive before the checkpoint expires and does
  surface `A mutation of Bigtable table … failed`. The refused arm's 9 min 46 s wins by ~14 s even
  in the flush case.
- So which report an operator gets is decided by seconds and by where the stalled mutations
  happened to be, which is exactly what makes it not something to rely on.

Either way nothing is reported for about ten minutes, and no counter can report it: the state *is*
that nothing is resolving, so `numRecordsSend` stays flat and the error counters need not move at
all — a mutation that never answers is never counted as a failure.

## Decision

**The waits report a stall; they do not bound one.** No option, no default to choose, no new failure
path — the measurement closed all three. This adopts half of ADR-0052 and declines the other half,
on the grounds that the half it declines is answering a question Pub/Sub has and Bigtable does not:
there, ordering makes the SDK retry a publish without limit, so nothing inside the sink ends an
outage but the budget; here the client's own 10-minute total timeout does.

**The report is not separable from the loop, and that is what makes this more than a log line.**
`yield()` blocks until a mail arrives and a stalled client sends none, so a writer that keeps it can
never notice a stall at all. Both waits therefore run ADR-0052's shape — `tryYield()` plus a park —
through one helper, `awaitMutationProgress(waitStartNanos, what)`:

- **The interrupt flag is read there, first and on every pass.** `tryYield()` does not look at it
  where the blocking `yield()` took the mailbox lock interruptibly, and `LockSupport.parkNanos`
  returns on interrupt without clearing it. Without this read the rewrite would take away the one
  property the measurement above found *working*, and nothing else in the writer would notice.
- **The idle time is read only once `tryYield()` has come back empty.** A completion mail queued
  behind other work is a mutation the client already answered, so counting the time it waits its
  turn would report a stall on a healthy writer. ADR-0052 takes the same trade for the same reason
  and records that its first draft had it the other way round.
- **Measured against the later of "this wait began" and "the client last answered"**, or a writer
  whose stream went quiet for an hour would report its next wait as an hour-long stall.
- **The park is 1 ms, and the interval is set by mail latency rather than by the warning.** A park
  is time a completion mail sits unprocessed, so it is the throughput cost of leaving the blocking
  `yield()`; a threshold measured in tens of seconds makes a longer park look free, and it is not.

**Progress is stamped on the gax callback thread**, in `MutationCallback.onSuccess` *and*
`onFailure`, before either dispatches its mail — `lastCompletionNanos`, the one field of this writer
not confined to the task thread, hence `volatile`. What a wait asks is whether the *client* is still
answering, and a failure is an answer; a mail enqueued but not yet dequeued already answers it.
Stamping on the task thread instead would report a busy mailbox as a stalled client.

**The threshold is a constant, 60 s, derived rather than chosen**: a tenth of the ten-minute ceiling
measured above, which is the fraction Pub/Sub warns at within its own budget. It is not an option
because there is no budget for one to size, and because the sink exposes none of the client's retry
settings by policy (ADR-0041). It is injectable for tests alongside the recovery schedule and the
clock.

**The warning is rate-limited writer-wide**, by `lastStallWarnNanos`, back-dated at construction so
the first stall of a writer's life warns as promptly as the tenth. Not a per-wait flag: the
isolation pass drains once per parked mutation and a park runs to a whole `maxInFlightEntries`, so
one `flush()` can make a thousand waits. ADR-0052 declined the same shape for a weaker version of
the same reason.

**The message carries what the measurement bought**: the idle duration, which wait it is, the
in-flight entry count, the live destination count (ADR-0074), and the two facts an operator
cannot otherwise know — that the client gives up on its own at about ten minutes, and that Flink's
checkpoint timeout may fail the job first with a message naming nothing about Bigtable.

**`awaitCapacity()` also sends what the batchers are still accumulating, once per wait**, when
`tryYield()` first comes back empty. This is a second finding, folded in by the user's decision
rather than by the issue: a mutation counts against the in-flight caps from the moment the batcher
accepts it, which is before it goes anywhere, so at the cap some of what the wait is waiting for may
still sit in an accumulator — and the writer, holding the task thread, cannot add the mutation that
would trip the batcher's own element threshold instead. Without the send, gax's 1-second delay
threshold is a term inside every such wait. Once per wait rather than per pass, since the task
thread is blocked and nothing can join a batch while it waits. `drainInFlight()` does not do it: its
callers send immediately before.

## Consequences

- The waits no longer block; they poll at 1 ms. That is a real cost — a millisecond of latency per
  empty poll on a completion mail, and a wakeup per millisecond while waiting — accepted for the
  same reason ADR-0052 accepted it, and bounded by the fact that `tryYield()` returns `true` and
  parks nothing at all while completions are arriving.
- A stalled sink still fails only by someone else's clock. This change makes the failure
  *diagnosable*, not faster.
- The tests drive the writer's injected clock from a scheduled task rather than waiting in real
  time, so a minute-long stall costs a millisecond of wall clock — which `PubSubWriterProgressTimeoutTest`
  cannot do, having no clock seam. The clock double is `volatile` for the same reason the writer's
  stamp is.

## Alternatives considered

- **Porting `publishProgressTimeout` whole.** Declined above: it answers a question this connector
  does not have, and would add a public knob, a default to justify, and a new way to fail a job that
  the client is already ending.
- **A metric instead of a log line.** Declined: the state is that no counter moves. A gauge of the
  idle time would be scraped by a reporter that is not the thing an operator reads when a job stops
  checkpointing, and would not name the connector in the place they are looking.
- **Documenting the diagnosis instead of emitting anything** — a "if checkpoints stop completing,
  look at `inFlightEntries`" paragraph, at zero code. Declined: it helps only a reader who already
  suspects this connector, which is precisely what the measured failure mode denies them.

[#431]: https://github.com/laughingman7743/flink-connector-gcp/issues/431
