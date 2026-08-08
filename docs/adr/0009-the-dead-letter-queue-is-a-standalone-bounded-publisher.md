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

# ADR-0009: `PubSubDeadLetterQueue` is a standalone publisher with bounded flush and close

- Status: Accepted
- Date: 2026-08-02 ([#211]); flush bound added 2026-08-06 ([#321]); metrics added 2026-08-08
  ([#329])
- Issues: [#211] (the [#37] series), [#321], [#329]
- Modules: pubsub (driven by any connector's `FailureHandler`)
- Current behavior: the three datastream pages' dead-lettering sections, and `pubsub.md`'s
  "Dead-letter metrics"

## Decision

The repository's one shipped `DeadLetterQueue`, in a **top-level `pubsub.deadletter` package**
rather than under `sink` — it is not sink API, it is driven by *any* connector's
`FailureHandler`, so putting it under the Pub/Sub sink would misfile it (the [#119] layer test
is about a family layer inside `sink`, and this is not inside `sink` at all). It uses the SDK
`Publisher` **directly**, not `PublisherFactory`/`TopicPublisher`: those are sink internals
parameterised by `PubSubPublisherOptions`, and a DLQ has no publisher-tuning surface. The ~10
duplicated lines of emulator-channel setup are the accepted price and are not a defect to fix by
coupling the two. `TopicDestination` *is* reused, since inventing a second topic identity in one
module would be worse.

Decisions worth keeping from [#211]:

- The envelope's `dlq-error` is **truncated on a character boundary** to Pub/Sub's 1024-byte
  attribute-value limit and marked `...` — cutting UTF-8 bytes blindly leaves a partial
  character, which the service rejects, turning a dead letter into a job failure; the truncation
  is a `CharsetDecoder` with `IGNORE` rather than arithmetic on code point widths.
- The cause chain is deliberately **not** in the envelope (no bounded string form) and reaches
  the job log at DEBUG instead.
- `maxOutstandingMessages` **bounds what one checkpoint interval accumulates** (default 1000,
  `0` = write through per element, `-1` = unbounded, one predicate covering all three) because a
  systematic failure turns every record into a dead letter and the SDK publisher has no flow
  control by default — the issue text said buffer-until-flush, and that shape can OOM where the
  pre-[#37] behaviour merely failed the job.
- `envelope(...)` is a **pure static** taking the subtask index and the instant, which is what
  lets the attribute set be pinned exactly without a live publisher — `Publisher` cannot be
  subclassed (ADR-0007), so every seam here is arranged deliberately. The second seam is
  `close()`'s: its two steps are held as `@VisibleForTesting` `AutoCloseable` fields
  (`publisherShutdown`, `channelShutdown`) that `open()` assigns, so [#276]'s test can make the
  publisher's shutdown throw an `Error` and assert the channel is shut down anyway. **The
  not-open guard reads `publisherShutdown`, not `publisher`** — they are set and cleared
  together, so it means the same thing, and it lets the test drive `close()` without opening a
  real publisher and stranding a gax executor in the test JVM.
- The topic is never auto-created: a dead-letter destination created on the fly is one nothing
  is consuming.

**The flush is bounded on the wait side, by one deadline per wait rather than one per publish**
([#321]). `Builder.flushTimeout(Duration)`, 60 s, covering both waits — `flush()`, which runs at
every checkpoint barrier, and the `maxOutstandingMessages` drain — and the publisher hand-off
with them.

## Alternatives declined ([#321])

Wait-side rather than giving the queue's publisher its own `retryTotalTimeout`, the issue's
preferred candidate. **The decisive reason is that it bounds the wrong thing**: gax does
truncate each attempt's RPC timeout to the remaining total budget
(`ExponentialRetryAlgorithm.createNextAttempt`, 2.82.0), so the SDK's futures normally resolve
within it — but what has to be bounded is *our wait on the task thread*, the [#265]/[#312]
lesson, where the wait that never returned was the SDK's own. The supporting reason is that
[#321]'s acceptance criterion is a measurement, and that budget admits none: `Publisher` exposes
`getBatchingSettings()` and **nothing** for retry settings ([#310]'s fact from the other side).
Note what that argument does **not** establish — unpinnability alone does not disqualify a knob,
because [#310] shipped exactly that unpinnable `retryTotalTimeout` on the *sink's* publisher;
and `Publisher.Builder.MIN_TOTAL_TIMEOUT` (10 s, message-less `checkArgument`) forbids nothing
this knob wanted to express. **No unbounded opt-out**: `UNBOUNDED` is already taken here by
`maxOutstandingMessages`, `shutdownTimeout` rejects zero and negative alike, and an effectively
infinite budget stays expressible as a large `Duration` without being a mode.

## Consequences

- The number to hold against is `execution.checkpointing.timeout`, **600 s, exactly the SDK's
  default publish retry budget**, so before this the queue alone could spend a checkpoint's
  whole budget; a `flush()` failure is sync-phase, which Flink fails over on whatever
  `tolerable-failed-checkpoints` says.
- **Expiry throws and nothing is dropped** — the `DeadLetterQueue` contract's at-least-once
  wording needed no change: it already scopes the guarantee to failures that recur on replay and
  already says a throwing `flush()` fails the checkpoint. The futures are deliberately **not**
  cancelled, so a message the SDK still delivers is a duplicate the contract covers rather than
  a loss.
- **One deadline for the whole list, never one per future**: `maxOutstandingMessages` defaults
  to 1000, so a per-future budget would be a thousandfold multiple of the number it claims to be
  — [#265]'s teardown mistake in a new place.
- **A budget is per call, and a checkpoint interval may make several** — `flush()` runs at a
  barrier *and* at any sink-triggered flush (BigQuery's default-stream writer has a periodic
  one), and the offer-side drain fires once per bound-full, once per *element* under
  `WRITE_THROUGH`. So a slow-but-working topic spends several budgets in an interval with
  nothing expiring, and the "one budget per outage" claim holds only because the first expiry
  throws. **An expiry from the offer path fails the task mid-processing, not a checkpoint.**
  The rendered pages say all of this; sizing the knob against
  `execution.checkpointing.timeout` alone is the mistake the error message used to invite.
- The test seam is a third static beside `envelope(...)`, taking the publisher hand-off as a
  `Runnable` plus the futures, the topic and the budget — being handed the futures is the only
  way in, and that `Runnable` is what let the folded-in guard on `publishAllOutstanding()` be
  tested at all (it sat outside every try/catch in both callers, while `offer`'s `publish(...)`
  two lines earlier was wrapped). `flushTimeout()` is readable off the instance, which is why
  its serialization test needs no `open()` where `shutdownTimeout`'s does.
- The knobs are documented as a `## PubSubDeadLetterQueue.builder()` section of
  `reference/pubsub.md`, once, with the other reference pages linking to it — the class is
  Pub/Sub's even though one instance serves every connector. This reverses what [#321] settled:
  a row was declined then because every table on that page is `Option`-headed, so it would have
  failed `check-option-docs`'s staleness direction and needed an `[extra]` entry, and `[extra]`
  is for keys someone else declares. That objection was a symptom of the checker not reading
  this class at all — [#328] made the class reachable by naming it in the module's `sources`,
  after which the row is satisfied by the setter behind it and no allowlist entry is involved.
  The datastream pages' dead-lettering prose keeps what it is for and points at the values.

- **Both of this class's budgets reject a `Duration` too large to express in nanoseconds**,
  because the flush knob's own documentation offers a long one as the way to say "effectively
  unbounded" and `Duration.toNanos()` would otherwise throw on a TaskManager — at the first
  flush, or inside `BoundedShutdown.start()`. [#334] then made that the rule for every budget of
  this shape, `BoundedShutdown` included, and measured the flush deadline's own overflow at that
  ceiling as benign (ADR-0068); the sink's
  own `drainInFlight()` — the leg that dominates what a checkpoint spends, and unbounded
  outright under `enableMessageOrdering` until [#333] bounded it on progress (ADR-0052).

## The four metrics ([#329])

**Where the names live is settled by the options decision above**: the class is Pub/Sub's even
though one instance serves every connector, so they are declared in `PubSubMetricNames`. The
alternative — a shared holder in `base` for names several connectors have in common — is the shape
ADR-0038 records as built first and **withdrawn**, and the answer taken needs no change to
`check-metric-docs`. The group is not a choice at all: `FailureHandlerContext` carries the **host
sink writer's**, so a BigQuery job's dead-letter metrics appear beside BigQuery's own. That is why
each of the four carries `deadLetter` in its name.

- **`deadLettersPublished` counts confirmations, not hand-offs**, which reverses what [#329]
  proposed. The issue's premise — that a record routed to the queue "leaves that accounting
  entirely" — is **false, checked across all six writers**: every one increments
  `numRecordsSendErrors` immediately before calling its failure handler, so under
  `sendToDeadLetterQueue(...)` the offered count is already on that same group, and a hand-off
  counter here would be that series twice. What nothing reported is how much of it reached the
  topic, which is what [#329]'s own acceptance criterion asks for. Incremented one future at a
  time, after the `get`, so a partly resolved wait reports what it resolved.
- **`deadLetterFlushMillis` is a gauge of the last completed wait**, recorded in a `finally` so the
  expiry — the case worth having it for — is not the one it skips, and held `volatile` because a
  non-volatile `long` read from the reporter thread may tear (the sink's gauges expose `int` state,
  which cannot). A cumulative counter of waited time was declined: it cannot tell one wait that
  spent the whole budget from many short ones, and the budget is per wait.
- **`deadLetterPublisherShutdownsAbandoned` is a second residue adder**, reversing this record's
  earlier "a publisher too, so it counts into the same total the sink's do". One name cannot serve
  both: these register on the host's group, a Pub/Sub sink has already registered
  `publisherShutdownsAbandoned` there, and `AbstractMetricGroup.addMetric` (read in flink-runtime
  2.2.1) resolves that by keeping the metric registered first and logging `Name collision: … Metric
  will not be reported.` — which a healthy configuration would then log at every subtask. Splitting
  is what ADR-0007 already prescribes for a new owner ("a future adopter gets its own field here"),
  and it closes the gap `pubsub.md` used to state outright: a job with no Pub/Sub sink reported
  those teardowns in **no metric**, only as a `WARN` on `BoundedShutdown`. The collision is not a
  guess about one version — the drop branch and its "Metric will not be reported" message are in
  flink-runtime 1.20.4, 2.2.1 and 2.3.0 alike, which is the whole supported range. Declined
  instead: putting the four under a fixed `deadLetterQueue.` subgroup,
  which avoids the collision but is unrepresentable in `check-metric-docs`'s `[[subgroups]]` — that
  mechanism reads a *templated* middle segment out of the `base.metrics` registrars — so it would
  have cost the checker, its tests and its skill.
- The read-only `Counter` view over a residue adder became a top-level `AbandonedShutdownsCounter`
  taking the adder as an argument, since there are now two registrars; it was a private class
  inside `PubSubSinkWriterMetrics`.

[#37]: https://github.com/laughingman7743/flink-connector-gcp/issues/37
[#119]: https://github.com/laughingman7743/flink-connector-gcp/issues/119
[#211]: https://github.com/laughingman7743/flink-connector-gcp/issues/211
[#265]: https://github.com/laughingman7743/flink-connector-gcp/issues/265
[#276]: https://github.com/laughingman7743/flink-connector-gcp/issues/276
[#310]: https://github.com/laughingman7743/flink-connector-gcp/issues/310
[#312]: https://github.com/laughingman7743/flink-connector-gcp/issues/312
[#321]: https://github.com/laughingman7743/flink-connector-gcp/issues/321
[#328]: https://github.com/laughingman7743/flink-connector-gcp/issues/328
[#329]: https://github.com/laughingman7743/flink-connector-gcp/issues/329
[#333]: https://github.com/laughingman7743/flink-connector-gcp/issues/333
[#334]: https://github.com/laughingman7743/flink-connector-gcp/issues/334
