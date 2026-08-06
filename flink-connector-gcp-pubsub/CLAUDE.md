# CLAUDE.md — flink-connector-gcp-pubsub

Module-scoped guidance, loaded when Claude works in this module. Repository-wide rules
(build, workflow, version policy, licensing, package layout) stay in the root `CLAUDE.md`.

## Design decisions (do not silently revisit)

- **Pub/Sub**: base implementation is vendored from `GoogleCloudPlatform/pubsub`
  `flink-connector/` (decision record: issues #17 and #31); the Apache connector is only a
  design reference (table-factory plumbing, emulator harness). All packages are normalized to
  `io.github.flink.gcp.connector.pubsub.*`
- **Pub/Sub sink** (#18): Publisher-based flush-on-checkpoint stateless writer; FLIP-171
  `AsyncSinkBase` evaluated and rejected (SDK `Publisher` already batches; AsyncSink persists
  buffers into writer state). Mailbox-based backpressure with in-flight caps; writer-owned
  per-topic publishers (no JVM-wide cache); publish failures are capture-and-rethrow (the
  Apache connector's infinite republish is deliberately not adopted). Topic auto-creation (#19)
  is reactive — NOT_FOUND publishes are parked and republished after creating the topic via the
  `TopicAdmin` SPI (`sink.topics`, ALREADY_EXISTS = success), gated by `CreateDisposition`.
  Tuning (#20) lives in one `PubSubPublisherOptions` object (nested-options pattern; plain
  serializable values, no gax types on the public API; unset = SDK/sink default): batching,
  publish retries, `enableMessageOrdering`, the in-flight caps and the recovery backoff.
  In-flight bounds (#85, revising #20): the writer owns **both** caps — `maxInFlightMessages`
  (1000) and `maxInFlightBytes` (64 MiB per subtask) — and the two SDK `flowControl*` knobs #20
  exposed are **removed**, not deprecated. gax flow control could never be the byte bound an
  ordered sink needs: SDK 1.152.0 leaks a permit per publish cancelled on a paused key (so the
  builder rejected combining it with ordering — exactly where cascades pile up), and it blocks the
  task thread instead of yielding to the mailbox. Message count alone bounds no memory, since
  Pub/Sub allows 10 MiB per message. Three constraints not to re-litigate: the byte bound is an
  *additional* condition on `write`'s admission only, because the three drains (now
  `drainInFlight()`, named apart from `awaitCapacity()` for exactly this reason) must keep meaning
  "empty, and `checkAsyncError`" — #78/#110 made that load-bearing; admission is "below the cap",
  never "does this message fit", since `yield()` blocks until a mail arrives and no mail can
  arrive at zero in flight, so a fits-predicate would hang the task on an oversized message
  instead of backpressuring it; and a repair republishes its parked batch
  **exempt from both caps**, because yielding between a key's republishes reorders it. Parked
  messages are counted by neither cap (their failure mail released them). The two writer test
  classes carry `@Timeout(30)`: the fake mailbox blocks like the real one, so a broken predicate
  hangs rather than fails.
  Ordering×repair (revised in #78): cascade cancellations are parked alongside their NOT_FOUND
  root and every repair attempt calls `resumePublish` before republishing, but **per-key order is
  restored by sorting the parked batch on a publish sequence, never by observation order** — the
  "mailbox FIFO preserves per-key order" premise this was first built on is false, because the SDK
  cancels an ordering key's queued publishes from its own thread, so a cascade can be observed
  before the failure that caused it. Anything derived from that order is a race, including
  deciding whether to park a cascade by whether something is parked already: that was the #78
  flake, and it was *also* the only thing hiding a silent ordering violation, since the parked
  list was appended in observation order too. Consequences to keep: a cancellation is never a root
  cause, so one is parked unconditionally and a fatal root is caught by
  the pre-repair drain (`drainInFlight()` → `checkAsyncError`) rather than by classifying
  the cascade. **The disposition no longer gates parking** — #215 moved that guarantee onto
  `topicMissing`, so read the #215 bullet before restoring any `repairsTopics()` check on a
  parking branch. Emulator
  support (#21) is a builder option `emulatorEndpoint(host:port)` — plaintext + no credentials
  for publishers (each owning its channel) and the auto-creation admin, mirroring the Apache
  connector's `withHostAndPortForEmulator`; the emulator ITs (including a MiniCluster streaming
  test through the public builder) reuse the production factory/admin, no test-only factory.
  Decision record in the connector documentation page
- **Pub/Sub sink per-message failure policy** (#206, the #37 series): `failedMessageHandler(...)`
  takes the shared `FailureHandler<FailedMessage>` from `base.failure`, defaulting to `failJob()`
  — behaviourally today's capture-and-rethrow, which is why `PubSubWriterTest` and
  `PubSubWriterAutoCreationTest` were left untouched and are the regression guard. `FailedMessage`
  sits at the `sink` root (a one-class `sink.failure` fails the #119 layer test) and carries the
  **whole serialized `PubsubMessage`** as `getPayloadBytes()`, not just its data, so a dead-letter
  consumer recovers attributes and ordering key with `parseFrom`; `describeDestination()` is the
  `projects/p/topics/t` resource name the `FailedElement` javadoc prescribes, not
  `TopicDestination.toString()`'s `project/topic`. `PubSubErrorClassifier` (`sink.writer`) absorbs
  the writer's `isNotFound`/`isCancellation` and fixes the precedence
  `TOPIC_NOT_FOUND` → `CANCELLATION` → `MESSAGE_LEVEL` → `FATAL`, each walking the cause chain;
  the order is pinned by test, because a chain can carry both a cancellation and a status.
  **Exactly two failures are routed, and the boundary is the decision**: a record the serializer
  rejects, and a publish rejected `INVALID_ARGUMENT`. A record the serializer *skips* by returning
  null is in neither class (#230): it is not a failure, no publisher is even opened for it — the
  check sits ahead of `stateFor(...)` — and `recordsSkipped` is the only thing that reports it.
  `MetadataSerializationSchema` propagates a skip unchanged and calls neither extractor for it, so
  the writer's check stays the single decision point; `dataOnly(...)` and the table layer's
  `RowDataSerializationSchema` cannot skip at all, because Flink's `SerializationSchema` contract
  has no null in it and reading one as a skip would silently drop every record a format failed on.
  The root `CLAUDE.md` carries the whole contract. Not routed, in two directions and for two
  different reasons — outage-shaped failures (an unavailable service, an exhausted SDK retry
  budget) must never reach a dropping handler, or an incident bleeds the stream one message at a
  time rather than backpressuring; and configuration-shaped failures (a `DestinationResolver`
  returning null, an ordering key without `enableMessageOrdering`) fail *every* record alike, so
  dropping them would leave an empty topic under a green job — the same trap eager schema
  derivation closes on the BigQuery side. **That second argument does not reach as far as it
  sounds, and the docs now say so**: a serializer producing an *invalid message* for every record
  is rejected per message, so it is routed and droppable exactly like a genuine one-off — the
  classification reads a response status code and cannot tell a systematic rejection from a
  per-message one. Nothing in this PR's scope closes that; the answer is the #208 error metrics,
  and BigQuery carries the identical exposure. A `MESSAGE_LEVEL` handler failure is captured into
  `asyncError` rather than thrown, because it happens inside a mailbox mail; an unchecked one is
  wrapped naming the topic. Coverage is unit tests only: the
  emulator validates nothing, and what real Pub/Sub answers `INVALID_ARGUMENT` to would have to be
  measured before a gated IT could assert it
- **Ordering beside a dropping failure policy** (#215, lifting the `build()` precondition #206
  shipped): allowed, with the gap documented rather than mechanised. The SDK facts the design turns
  on — read from `google-cloud-pubsub` 1.152.0 sources, not assumed — are that
  `SequentialExecutorService.cancelQueuedTasks` adds the ordering key to `keysWithErrors`
  **unconditionally**, taking a bare `Throwable` it never inspects, so an `INVALID_ARGUMENT` poisons
  a key exactly as a `NOT_FOUND` does; that nothing auto-resumes (`keysWithErrors.remove` has one
  caller, the public `resumePublish`); and that a later `publish()` on a paused key **returns an
  already-failed future** carrying the shared static `CancellationException` rather than throwing —
  which is what makes the "leave it paused" design below work at all. A naive lift would have let
  one dropped keyed message kill its key for the writer's lifetime. Three changes, each with a
  reason not to re-litigate:
  **(a) parking a cascade no longer depends on the create disposition** — a cascade's root may be a
  dropped message, which `CREATE_NEVER` needs repaired too. The tempting narrower form (park only
  when a drop is recorded for that key) **is #78's bug rebuilt**: the drop mail and the cascade mail
  arrive in either order, so a cascade observed first would find nothing recorded and become
  `asyncError`. Unconditional parking also makes a root's error message win over a cascade's under
  `CREATE_NEVER`, which used to depend on mail order.
  **(b) the repair carries a reason** — `DestinationState.topicMissing`, set only where a
  `TOPIC_NOT_FOUND` is parked, so only that repair creates a topic. This is what preserves
  "`CREATE_NEVER` creates nothing" once (a) removed the disposition guard from the parking branches,
  and the invariant is now asserted directly rather than through "nothing is parked". Creation is
  decided **per attempt but performed at most once per repair**: a batch parked for another reason
  can turn out to need it (its republish being the first publish to meet the missing topic), while
  the retry loop itself is for metadata propagating over a topic that by then exists.
  **(c) a dropped keyed message registers its key** — `keysToResume`, drained by
  `resumeOrderingKeys` — and **the resume is deliberately not in `routeFailedMessage`**: `write()`
  tests `repairNeeded` *before* `awaitCapacity()`, and mails run inside it, so a key resumed from
  the failure mail could be published to by the rest of that same `write()` while its cascades were
  still parked, putting a newer message ahead of older ones. Left paused, that racing publish comes
  back cancelled, is parked, and is republished in sequence order with the rest. Without (c) the
  writer is still correct — the next message for the key fails and is repaired — but (c) is what
  makes `flush()`'s `while (repairNeeded)` loop mean *no checkpoint completes with a key paused*.
  The `return` after a throwing handler is **belt and braces, and measured as such**: a mutant
  deleting it survives, because `asyncError` gates every path into a repair anyway, and the test
  says so rather than claiming a discrimination it does not have. The fake publisher models the
  paused-key state itself since #277 — a failed keyed publish pauses its key, a publish on a paused
  key comes back cancelled without being published, and only `resumePublish` clears it — so the
  racing-publish reordering above is pinned by test
  (`aKeyPausedByADropStaysPausedUntilTheRepairResumesIt`) rather than verified only by reading the
  SDK source. The one finding from that reading that was not a writer decision — a `shutdown()`
  that never returns — is answered by the teardown bullet below (#265)
- **The publisher teardown is two-phase, and its bound is real** (#265). The SDK defect, read from
  `google-cloud-pubsub` 1.152.0 rather than assumed: `Publisher.publish` increments
  `messagesWaiter` per accepted message, and the failure callback cancels the messages still
  accumulating in a failed ordering key's **un-flushed** `MessagesBatch` and removes the batch,
  while decrementing only by the *in-flight* batch's size — so those increments are never
  returned, `pendingCount` stays above zero forever, and `Waiter.waitComplete()` (uninterruptible,
  wakes only on an exact zero) parks `Publisher.shutdown()` for good. Our 30 s bound sat on the
  *next* line, `awaitTermination`, which was never reached; both `TopicPublisher.close()`'s javadoc
  and the docs page already promised a bound, so this was a contract violated rather than a feature
  missing. The window is not exotic — a keyed publish failing with more of that key still batched
  is what #78 and #215 exist for, and `close()` on the failure path runs without a preceding flush.
  **The defect is not the only thing that needs the bound, and the second reason is the bigger
  one**: with `enableMessageOrdering` the SDK replaces the publisher's retry settings with
  `maxAttempts = Integer.MAX_VALUE` and an effectively infinite total timeout (`Publisher.java`
  1.152.0, and its own `TODO` says this is per publisher, so unkeyed messages get it too), so during
  an outage the in-flight publishes retry forever and `waitComplete()` never drains — no defect
  required. An ordered sink therefore needs this bound whatever the SDK version, which is why
  nothing here is written as a workaround and why #309's rewording is a rewording rather than a
  removal. That same override is what #310 settled from the other side: `retryTotalTimeout` and
  `retryMaxAttempts` are **rejected** by `PubSubPublisherOptions.build()` beside
  `enableMessageOrdering(true)`, rather than documented as ignored. Only an explicitly set knob is a
  conflict (both are `@Nullable`, so "unset" is a distinguishable state and the SDK's own defaults
  are exactly what ordering is expected to override), and the other six retry knobs still apply.
  Rejecting was chosen over documenting because documenting alone is **unpinnable**: `Publisher`
  exposes `getBatchingSettings()` and nothing for retry settings, and keeps no `retrySettings` field
  — the values are folded into the stub's callables — so the reflective assertion the issue asked
  for has no analogue of `configureAppliesSettingsToABuiltPublisher` to follow. The check lives in
  the options class rather than in `PubSubSinkBuilder` because both knobs are its own, and it names
  **only the knob that was actually set**, as `PubSubSourceBuilder`'s cross-checks do.
  **`PublisherOptionsMapper` restates it in DDL keys**, and the first draft's claim that the
  builder's message "reaches SQL unchanged" was simply wrong — measured on
  `flink-table-common` 2.2.1: `FactoryUtil.createDynamicTableSink` wraps *anything* the factory
  throws in a `ValidationException` whose own message is only "Unable to create a sink for writing
  table …", so the actionable sentence lands in the cause, and it would name
  `retryTotalTimeout(...)`, which appears nowhere in a `WITH` clause. `TopicCreateOptionsMapper`
  restates its builder's create-disposition check for exactly that reason, and this is the same
  shape. The counter-example that misled the first draft is `PubSubSourceBuilder`'s
  `parallelPullCount` × `PER_KEY` check, which *does* reach SQL unwrapped — because it throws from
  `getScanRuntimeProvider`, outside the factory's `try`. So the rule is not "builder checks reach
  SQL unchanged" but: **a check that fires inside `createDynamicTable{Source,Sink}` is wrapped, and
  a check whose message names Java setters needs restating in option keys; one whose message needs
  no translation does not.**
  Deliberately **no runtime re-check** in `DefaultPublisherFactory`: `PubSubWriter` carries the
  Bigtable-style "deserialization does not run the builder" guard for `maxInFlightMessages` because
  that invariant is *relied on* (a non-positive cap parks the task forever), and this one is not —
  a deserialized violating instance behaves exactly as it did before this check existed, since the
  SDK overwrites the settings either way. The check is advisory, and that is the whole of it.

  Six decisions not to re-litigate about the teardown itself (the run below has grown past six —
  count them before quoting the number). **A separate thread is the only lever**: the wait ignores
  interruption, `Publisher` has no forcible variant, and `Waiter` is package-private — so
  `DefaultPublisherFactory.BoundedShutdown` runs the SDK shutdown on a **daemon** thread (one that
  never returns must not keep a JVM alive) and gives up at the deadline. This is the repository's
  first main-code thread; an `ExecutorService` was the alternative and buys nothing, since
  `shutdownNow()` cannot interrupt that wait either — its thread would leak identically, and the
  executor would then need a bounded teardown of its own. **The deadline is recorded by
  `start()`, not by `close()`**, which is what makes the writer's overlapped teardown cost one
  timeout however many topics it owns rather than one per topic; `start()` is idempotent and
  deliberately does not restart the clock. Pinned by
  `theBudgetRunsFromTheShutdownCallRatherThanFromTheClose`, which is the only test that fails if
  the deadline moves into `close()`. **`awaitTermination` runs on that thread too, not on the task
  thread after a successful join** — measured on gax 2.82.0, whose
  `BackgroundResourceAggregation.awaitTermination` passes the *full* duration to each resource in
  turn (its own source carries the `TODO subtract time already used up from previous resources`),
  and a publisher nests several: its executor, then the stub's transport channel and watchdog. The
  first shape of this fix awaited on the task thread and so cost a *multiple* of the timeout while
  claiming to cost one; the self-review caught it. **Anything either step throws is captured and
  rethrown by `close()` with its own type** — on a bare thread it would reach only Flink's JVM-wide
  handler, losing a teardown failure the pre-#265 inline call reported and, under
  `cluster.uncaught-exception-handling: FAIL`, exiting the whole TaskManager instead of failing one
  task. **The two steps are functional values, not a `Publisher`**, because `Publisher` is final:
  that is the only seam a test can drive, the same argument `PubSubDeadLetterQueue`'s
  `publisherShutdown` / `channelShutdown` fields make. What remains, and is logged rather than
  hidden: a publisher whose shutdown never returns leaves that thread and the client's executors
  until the JVM exits — and the give-up warning deliberately does **not** attribute itself to #265,
  since the budget is shared and a healthy teardown an earlier publisher left no time for reaches
  the same branch. **Both warnings report the time actually waited, not the configured budget**, for
  that same reason: a publisher after one that hung gets none of the budget, and "did not finish
  within 30 s" having waited nothing reads as "raise the timeout" when the answer is elsewhere.
  **The thread is named after the task thread as well as the topic** (Flink's `SplitFetcherManager`
  convention), because a writer is per subtask and without it every subtask writing one topic leaves
  identically-named threads for an operator to tell apart. **`close()` restores the interrupt flag**
  before propagating an `InterruptedException`: `join` clears it, `Closers.closeAll` collects and
  carries on, so without the restore the rest of the writer's teardown stops honouring the
  cancellation. And a failure captured *after* `close()` gave up is logged rather than dropped —
  nothing would otherwise read the field, and a thread outliving its job meets a closed user
  classloader. What this deliberately does **not** do is bound the accumulation (#311) or the
  dead-letter queue's own inline unbounded shutdown one entry later in the same list (#312).
  `PubSubDeadLetterQueue` uses the SDK `Publisher` directly too and is deliberately **not**
  changed — its `envelope(...)` sets no ordering key, so the cancel branch that leaks is
  unreachable there. `shutdownTimeout` became a `PubSubPublisherOptions` knob (30 s, matching
  what was hardcoded) for symmetry with `PubSubSubscriberOptions.shutdownTimeout`; the DLQ's own
  constant stays, having no options object and no exposure
- **A `MESSAGE_LEVEL` verdict is confirmed solo before it is routed** (#264, closing #269 with
  it). Measured on real Pub/Sub 2026-08-06 (record on #264): a `Publish` carrying one invalid
  message is rejected **all-or-nothing**, the SDK sets the *same* `Throwable` instance on every
  co-batched future, and nothing in the status names the offender (`details=0`, no `BadRequest`)
  — so routing on the report would hand a whole batch to a dropping handler for one bad message.
  The writer therefore parks a non-solo `INVALID_ARGUMENT` (`DestinationState.isolationNeeded`,
  consumed per attempt like `topicMissing`) and the repair runs an **isolation pass**: each parked
  message goes out as its own single-message request (`publishTo(..., soloVerdict=true)` +
  `flushOutstanding` + `drainInFlight` per message), and only a message rejected solo reaches
  `routeFailedMessage`. Decisions not to re-litigate: **the pass resumes a key right after a drop
  pauses it** (`resumeRegisteredKeys`, between publishes) — this does not violate "resume never in
  `routeFailedMessage`", because the resume stays inside the repair, the key's remaining messages
  are held by the pass in sequence order, and drains only complete publishes — and without it one
  drop costs one budget attempt, #269 rebuilt inside the fix; **a batched report is not counted**
  by `publishFailure` (the #208 cascade-exclusion argument: one incident, not batch-size errors),
  so `errorClass.INVALID_ARGUMENT.errors` and `numRecordsSendErrors` count true rejections;
  **client-side limit validation was declined** as the fix (it covers only the limits we encode)
  and **fail-on-batched-rejection was declined** (it defeats the dropping policy). #269 resolved
  as fallout: a poisoned key drains in one attempt however long the run, budget semantics
  unchanged — what remains is the two-variant exhaustion message (`kept failing …` vs `could not
  drain its parked messages within the recovery budget …`, chosen by whether this repair handed
  messages to the handler), each variant pinned by test. An oversized message under default
  batching never shared a request (the SDK sends an element exceeding the byte threshold alone,
  measured), so the fan-out concerns under-threshold violations — attribute limits and the like.
  The measured behaviour is pinned end-to-end by `PubSubSinkRejectionRealGcpITCase` (#303) — the
  first sink-side gated class, extending the source-side `AbstractPubSubRealGcpITCase`
  cross-package, which is the settled answer to where a sink gated class lives — deliberately at
  the outcome level (survivors published, exactly the invalid message routed, flush green), since
  the outcome is what the fix guarantees whatever the service's rejection granularity
- **`PubSubDeadLetterQueue`** (#211, the #37 series): the repository's one shipped
  `DeadLetterQueue`, in a **top-level `pubsub.deadletter` package** rather than under `sink` —
  it is not sink API, it is driven by *any* connector's `FailureHandler`, so putting it under the
  Pub/Sub sink would misfile it (the #119 layer test is about a family layer inside `sink`, and
  this is not inside `sink` at all). It uses the SDK `Publisher` **directly**, not
  `PublisherFactory`/`TopicPublisher`: those are sink internals parameterised by
  `PubSubPublisherOptions`, and a DLQ has no publisher-tuning surface. The ~10 duplicated lines of
  emulator-channel setup are the accepted price and are not a defect to fix by coupling the two.
  `TopicDestination` *is* reused, since inventing a second topic identity in one module would be
  worse. Three decisions worth keeping: the envelope's `dlq-error` is **truncated on a character
  boundary** to Pub/Sub's 1024-byte attribute-value limit and marked `...` — cutting UTF-8 bytes
  blindly leaves a partial character, which the service rejects, turning a dead letter into a job
  failure, and the truncation is a `CharsetDecoder` with `IGNORE` rather than arithmetic on code
  point widths; the cause chain is deliberately **not** in the envelope (no bounded string form)
  and reaches the job log at DEBUG instead; and `maxOutstandingMessages` **bounds what one
  checkpoint interval accumulates** (default 1000, `0` = write through per element, `-1` =
  unbounded, one predicate covering all three) because a systematic failure turns every record
  into a dead letter and the SDK publisher has no flow control by default — the issue text said
  buffer-until-flush, and that shape can OOM where the pre-#37 behaviour merely failed the job.
  `envelope(...)` is a **pure static** taking the subtask index and the instant, which is what
  lets the attribute set be pinned exactly without a live publisher — `Publisher` is final, so
  every seam here has to be arranged deliberately. The second one is `close()`'s: its two steps
  are held as `@VisibleForTesting` `AutoCloseable` fields (`publisherShutdown`, `channelShutdown`)
  that `open()` assigns, rather than being called as private methods, so #276's test can make the
  publisher's shutdown throw an `Error` and assert the channel is shut down anyway. **The
  not-open guard reads `publisherShutdown`, not `publisher`** — they are set and cleared together,
  so it means the same thing, and it is what lets the test drive `close()` without opening a real
  publisher and stranding a gax executor in the test JVM. The topic is never auto-created: a
  dead-letter destination created on the fly is one nothing is consuming
- **Pub/Sub sink metrics** (#208, the #37 series): `PubSubSinkWriterMetrics` (`sink.writer`) on the
  `PubSubSourceReaderMetrics` model, but with **plain counters, not `ThreadSafeSimpleCounter`** —
  every increment happens on the task thread here, since completions arrive as mailbox mails,
  which is exactly what the source cannot say. Four decisions worth keeping. **`numRecordsSend` is
  counted inside `publishTo`, guarded by its `firstAttempt` parameter** — not at the `write()` call
  site, and not unguarded. Two properties have to hold at once, and only that placement gives both:
  a repair re-enters `publishTo` for every parked message, so an unguarded
  increment would count publish *attempts* rather than records; and counting at the call site would
  count a record whose `publisher.publish(...)` threw synchronously, which registers no callback and
  reached the client not at all. So the counter sits beside `inFlightMessages++`, after the publish
  was accepted, under the flag. The repo-wide decision behind counting once (with what it costs
  `numBytesSend`) is on #208 and in the base module's CLAUDE.md. **`parkedMessages` is a
  new plain `int` field** maintained by the sole `park(...)` helper rather than a sum over
  `states`, since the gauge is read from the reporter thread and walking those maps would race the
  task thread; `close()` zeroes it, because parked messages are dropped with the writer.
  **Error-class counters skip cascade cancellations**: under #78 a cancellation always trails a
  root failure that is itself counted, and it carries no status, so counting it would both multiply
  one incident by the key's queue length and bury real unclassifiable failures under
  `UNCLASSIFIED`. The traversal that finds the code is `PubSubErrorClassifier.statusCode` — beside
  `classify`, since this class owns the connector's cause-chain policy. **Per-destination counters
  are resolved once per `DestinationState`**, not per record, so the topic's resource name (the
  same `toTopicPath()` `describeDestination()` uses) is composed once. Measured, so it is not
  re-investigated: `google-cloud-pubsub` 1.152.0 exposes **no** programmatic metric accessor on
  `Publisher` — only `setEnableOpenTelemetryTracing`/`setOpenTelemetry`, and
  `OpenTelemetryPubsubTracer` emits spans, not meters — so the flink-connector-kafka-style
  passthrough of client-native metrics has no source to read here
- **Pub/Sub source** (#79, #80, #81): FLIP-27 streaming-pull source; split = (subscription, uid),
  ack on checkpoint completion, nack on close. **The reader checkpoints no splits** — the
  enumerator is the only owner of split assignment, recomputing the deterministic plan
  (`splitCount = PER_KEY ? |subs| : max(|subs|, parallelism)`) on every start — because
  `SourceOperator` unions reader-restored splits with the enumerator's plan, so a reader that
  snapshotted its splits would leave a rescaled restore with one subscription consumed by two
  subtasks, exactly what `PER_KEY` exists to prevent (the #79 self-review bug; pinned by
  `checkpointsNeverCarrySplits`, exercised end-to-end by `PubSubSourceRecoveryITCase`). Tuning
  lives in one `PubSubSubscriberOptions` object
  (nested-options pattern, same shape as `PubSubPublisherOptions`). Two decisions deviate from the
  #80 issue text and must not be silently re-litigated:
  (a) the **subscriber shutdown mode is not exposed** — `NACK_IMMEDIATELY` is fixed because
  `WAIT_FOR_PROCESSING` waits for acknowledgements that only arrive at checkpoint completion, which
  never happens during close; only `shutdownTimeout` is a knob (an SDK enum on the public API would
  also break the #47 SQL mapping);
  (a′) **`close()` puts the shutdowns and the closes in one list through `Closers.closeAll`**, never
  a loop followed by a call (#297). `shutdown()` declares no checked exception, so an unchecked one
  from the first subscriber used to skip every later nack *and* skip the `closeAll` wholesale —
  leaving even the already-shut-down subscribers open, holding messages Pub/Sub only redelivers once
  their acknowledgement deadline expires. Both comments in that method asserted the opposite. The
  single list keeps the ordering those comments argue for (every shutdown before any close, so the
  waits overlap) because `closeAll` runs entries in order, and it is what makes the ordering
  survive a failure — pinned by asserting the recorded call order in the failing case too, not just
  on the success path;
  (b) the "**fail when running without checkpointing**" guard **cannot read the configuration** —
  `SourceReaderContext.getConfiguration()` is the TaskManager configuration
  (`SourceOperatorFactory` passes `getTaskManagerInfo().getConfiguration()`), while
  `env.enableCheckpointing(...)` writes into the job configuration, so absence proves nothing and
  failing on it would break jobs that enable checkpointing programmatically while passing every
  MiniCluster test. Replaced by `MissingCheckpointDetector` (no checkpoint taken + messages
  outstanding + budget spent → fail), **evaluated from `PubSubSplitReader.fetch()`, not the record
  path** — once flow control fills, the client stops delivering and `pollNext` is never called
  again, so a record-driven check would go silent in exactly the state it exists to catch; the
  detector bounds the fetch park only while armed, so a healthy reader parks indefinitely as
  before. Its budget (#101) starts at the reader's **first split assignment**, not at
  `createReader` — a reader is created before the enumerator's startup check finishes, so a
  constructor-started budget would be partly spent before there is anything to checkpoint; an
  unstarted detector is **not armed**, so a reader assigned no split parks indefinitely; and it
  retires at the **first checkpoint barrier** — `SourceOperator.snapshotState` is called
  unconditionally, so a barrier carrying no data counts, which is what bounds the guard to
  measuring one interval, once. The detector's fields are deliberately plain, not volatile:
  `AddSplitsTask` runs on the fetcher thread, the same thread as `fetch()`, and the reasoning
  lives in the class javadoc so it is not re-litigated. The config-derived ack-extension check is
  a best-effort warning only.
  `parallelPullCount > 1` is rejected with `orderingMode(PER_KEY)` rather than silently forced to 1
  (the factory still force-sets 1 so the guarantee does not rest on the SDK default).
  The startup check (#81) verifies every subscription (`GetSubscription`) before any split is
  assigned and rejects: a missing subscription without create options, an unordered subscription
  under `orderingMode(PER_KEY)`, an exactly-once-delivery subscription, and
  `deserializationFailurePolicy(NACK)` on a subscription without a dead-letter policy — the NACK
  requirement is enforced twice, in the builder for auto-created subscriptions and in the
  enumerator preflight for existing ones. The check's failure messages name the missing
  permission or setting on purpose; that text is the entire value of those branches.
  Subscription auto-creation is authorized by the presence of per-subscription
  `SubscriptionCreateOptions` — no disposition enum, because a subscription without a topic
  binding is not a subscription (the Table bullet records how the two directions spell creation
  differently, and the source never creates a topic). `StartPosition` seeks **once, at the first
  start of a job, never on a restore**: the guard is `PubSubEnumeratorState.startPositionApplied`,
  and a checkpoint with the flag still false contains no reader holding a split, so re-applying
  after such a restore is safe; a redeploy without state seeks again.
  **The real-GCP gated suite (#82)** is the *only* coverage of: ordered dispatch (per-key
  callback serialization is gated on a streaming-pull response field the emulator never sets),
  dead-letter forwarding (performed by the Pub/Sub service agent under project-level grants in
  `opentofu/`, not by the job's credentials), seek on an ordering-enabled subscription, the
  create-option knobs persisting (the emulator stores but ignores them), nack-redelivery
  *promptness* (an observed-behaviour bound — the #118 settlement moved the claim here and left
  the emulator IT asserting non-loss only), and the subscription admin's permission-denied
  message texts (via impersonation of the zero-role `e2e-no-pubsub` account — the local-run
  self-grant is documented on the docs page and deliberately not in opentofu, keeping personal
  identifiers out of source). Gating is `@EnabledIfEnvironmentVariable` on
  `PUBSUB_IT_PROJECT` **on every concrete class, never the abstract base** —
  `scripts/e2e-gated-its.sh` greps the annotation literal and then expects a surefire report per
  matching file. `PubSubSubscriptionAdmin` carries a `@VisibleForTesting` `CredentialsProvider`
  constructor for exactly the impersonation tests; no production path uses it
- **The emulator never answers the client library's keepalive ping, so an idle streaming pull is
  torn down and reopened on a cycle** (measured 2026-08-03 on `google-cloud-pubsub` 1.152.0, four
  runs, while investigating #244). `StreamingSubscriberConnection` sends an empty
  `StreamingPullRequest` every 30 s and closes the stream when the last ping is unanswered for
  ≥15 s; against the emulator that is *every* ping, so an idle stream logs `No response from
  server for 20 seconds since last ping. Closing stream.` at ~50 s after open and then roughly
  every 20 s (the first cycle is longer because the stream's own opening response answers the
  ping sent at open). Three consequences worth keeping. The line is **routine on an idle emulator
  stream, not a fault** — it says only that the stream received nothing, which any subscription
  with no messages satisfies; healthy emulator ITs never show it because none of them idles that
  long, which is also why its appearance in a *failing* run is worth reading. Simultaneous idle
  streams fire **together, within milliseconds** (measured: two streams, lines 5 ms apart, at
  50045/50050 ms and 50025/50028 ms across two runs), so the *spacing* of the lines carries
  information the count does not — two lines tens of seconds apart are not two streams idling in
  parallel. And a stream reset this way still delivers normally: publishing after an idle window,
  messages arrived in 105 ms and 104 ms. That last measurement is the one that makes prolonged
  silence on a subscription with a backlog abnormal rather than expected. Real Pub/Sub answers the
  ping, so none of this reaches a production job — it is a property of the harness, in the
  tradition of every other emulator deviation recorded here
- **Pub/Sub Table API / SQL** (#47, split into #135–#138): the `table` layer is a *mapping* onto the
  DataStream builders, never a second implementation — one typed `ConfigOption` per builder setter,
  applied with `getOptional(...).ifPresent(...)` so "absent from the DDL" and "left at the
  connector's default" are the same state and no default is restated in a `ConfigOption`. A
  reflective test asserts the setter set and the option set match, which is what keeps that true
  once the key names are grouped (`sink.batching.*`, `sink.retry.*`) and no naming rule connects
  the two. **No `properties.*` passthrough**: Kafka's is a map its own client parses, Pub/Sub has
  none, and #20 already decided no gax type reaches the public API. Byte knobs are `memoryType()`,
  converted at the mapper boundary so `MemorySize` never reaches the connector API.
  **The four connector enums carry their DDL spelling in `toString()`** (`create-if-needed`,
  `per-key`, `nack`, `continue-from-subscription`) because `ConfigurationUtils.convertToEnum`
  matches on `toString()` case-insensitively and normalizes nothing — Flink's own
  `DeliveryGuarantee` has the same shape. Table-local `DescribedEnum` duplicates (Kafka's
  `ScanStartupMode`) were the alternative and were declined: four extra types and a conversion step
  for no gain. The one visible cost is `StartPosition.toString()`, which now reads
  `StartPosition{mode=latest}`.
  One factory class implements both directions (#136 adds `DynamicTableSourceFactory` to it), so
  `topic`/`subscription` are **not** in `requiredOptions()` — each is checked in the `create...`
  method that needs it, or a table used in only one direction would be forced to configure the
  other. Sink specifics: metadata is **not** forwarded to formats (no built-in format ships
  writable metadata, and Kafka does not forward either), so the physical prefix of a consumed row
  is exactly the table's physical columns and a reused `ProjectedRowData` hides the metadata suffix
  from the encoder; the row is written into the `PubsubMessage.Builder` directly rather than
  through the public `withAttributes`/`withOrderingKey` combinators, whose `Map<String, String>`
  extractor would allocate a map per record; a null attribute key or value **fails the write**
  rather than being dropped; `ChangelogMode.insertOnly()` because Pub/Sub cannot express a
  retraction; and an `ordering-key` column without `sink.message-ordering.enabled` is rejected in
  `applyWritableMetadata`, since the writer would otherwise fail on the first record. Credentials
  stay ADC-only (#139) and dynamic per-record topics stay out (#140) — both cut from #47
  deliberately.
  **Package layout**: `table` holds the `@PublicEvolving` options class and the factory, `table.sink`
  (and `table.source` from #136) the `@Internal` implementation — a deliberate departure from Kafka,
  which keeps its whole table layer flat. The root `CLAUDE.md` rule (public API at the package root,
  implementation beneath) decides it, and #136 is the in-prospect sibling the #119 test asks for, so
  this is #125's situation rather than Cloud Tasks'. **The factory is the only place a DDL option
  becomes a value** — `PubSubDynamicSink` takes resolved constructor arguments and has no
  configuration vocabulary at all, which is why `PublisherOptionsMapper` is `@Internal public`
  rather than package-private.
  **Source specifics** (#136): the SPI was widened to
  `deserialize(PubsubMessage, SubscriptionDestination, Collector<T>)` rather than dropping the
  `subscription` metadata column — nothing is published, so a signature change is the cheap option
  (see the repo-level stance), and `SubscriptionSplit` was already in `emitRecord`, so the call site
  was one line. That column carries the **resource name**, not the
  bare id — the argument is on the docs page; what belongs here is that a two-column
  short-id-plus-resource-name design was built and dropped as redundant, and that the column
  deliberately does **not** equal the `subscription` option, which is documented rather than fixed.
  **`DecodingFormat.applyReadableMetadata` throws by default** and no built-in format overrides it,
  so it must be guarded — and the guard is on the format *declaring* metadata, as Kafka's is, not on
  the planner having selected some: only that form can shrink the key set back, and the ability
  permits repeated calls. Calling it unconditionally breaks every table with any metadata column;
  caught by the acceptance IT, never by a unit test.
  **Per-key ordering is not reachable from SQL** (#143): the guarantee is per writer subtask, the
  DataStream answer is a `keyBy` before the sink, and SQL has no equivalent — `DISTRIBUTED BY` needs
  `SupportsBucketing`, which this sink does not implement. `sink.parallelism = 1` is the only correct
  configuration today; it is documented rather than enforced, because a distribution the user
  arranged upstream is legitimate and the sink cannot tell the difference.
  **Auto-creation and start position** (#137): three setters do not take a `ConfigOption`'s shape,
  and each resolution lives in a mapper under `table.source` rather than in the factory —
  `StartPositionMapper` and `SubscriptionCreateOptionsMapper`, joining `SubscriberOptionsMapper`.
  Start position is `scan.startup.mode` + `scan.startup.timestamp-millis`, **Kafka's spelling rather
  than the connector's own** ("start position") — weighed, and settled on what a migrating SQL user
  types without reading anything; the docs table's "Maps to" column carries the connection to
  `StartPosition`. It has no declared default, like every other option here: `PubSubSourceBuilder`
  already initialises `continueFromSubscription()`, so absent means default and the issue's "default
  `continue-from-subscription`" describes behaviour rather than a `ConfigOption` default.
  `StartPosition.of(Mode, Instant)` raises both pairing errors, so the mapper delegates; the one rule
  it owns is a **timestamp with no mode**, where `of` is never reached and the option would otherwise
  be read by nothing. Same reasoning gives "a `scan.auto-create.*` knob without
  `scan.auto-create.topic` is rejected, not ignored".
  **`expirationTtl` versus `neverExpire` has no builder backstop** — the issue assumed one, and the
  builder is in fact last-writer-wins, each setter clearing the other, which is right for a call
  sequence and meaningless for a `WITH` clause. So the table layer rejects the pair *only* here, and
  that check is load-bearing rather than a nicer message; the builder was deliberately left as it is.
  `never-expire = false` calls nothing, since the setter takes no argument and `false` is already the
  state.
  **Auto-creation requires exactly one subscription**, because settings are per destination and carry
  the topic binding: N options objects are inexpressible in a flat DDL namespace, and sharing one
  would duplicate every message with nothing reporting an error. The precondition is checked in the
  mapper and again in `PubSubDynamicSource`'s constructor, which is the code that indexes the list.
  A `scan.auto-create.topics` `mapType()` extension for N>1 is deferred to #152. The builder's
  own cross-checks (ordering under `PER_KEY`, a dead-letter policy under a policy that needs one)
  then reach SQL users unchanged, which `carriesTheCreationSettingsAndTheStartPositionIntoTheBuiltSource`
  and its sibling are what prove — the create options and the start position are otherwise invisible
  from outside the built `Source`, and a mutant that dropped the start position on the way to the
  builder survived every unit test until they read it back through
  `PubSubStreamingPullSource.getConfig()`.
  **The two directions spell resource creation differently on purpose, and this is where that was
  settled** (the question #136 left open, having counted `sink.create-disposition`, `sink.recovery.*`
  and `scan.auto-create.*` as three spellings of one feature). They are not one feature. A topic
  needs no configuration to exist, so the sink can gate creation with a `CreateDisposition` enum and
  a "create with defaults" is meaningful; a subscription without a topic binding is not a
  subscription, so the source has no disposition enum at all and **the presence of settings is the
  authorization**. `scan.create.*` was weighed and declined: sharing the word would put a uniform
  vocabulary over a difference the DataStream API makes deliberately, and this layer maps rather
  than invents. `scan.` itself is not a choice — it is Flink's read-side prefix, carried by every
  source option here and by `FactoryUtil.SOURCE_PARALLELISM` (`scan.parallelism`) — and with one
  factory serving both directions it is what tells a reader which half an option belongs to.
  **The stated expiry of that settlement was reached and resolved in #153**, which gave the sink
  creation settings (`TopicCreateOptions`: `messageRetention`, `kmsKeyName`, the storage policy).
  The re-opened naming question settled as: the *settings* vocabulary aligns —
  `sink.auto-create.*` beside `scan.auto-create.*`, one spelling where both sides carry one knob
  (`message-retention`) — while the *gates* stay different, because the gate reasoning above is
  about what a resource needs to exist and #153 changed nothing about that. Three sink-side facts
  not to re-derive: the settings are additive and never authorize (the disposition still does, and
  its default `CREATE_IF_NEEDED` means settings alone are meaningful — only an explicit
  `CREATE_NEVER` beside them is rejected, in the builder naming methods and in the mapper naming
  option keys); **one options object applies to every topic the sink creates**, dynamic
  destinations included, because unlike a subscription's topic binding nothing in the settings ties
  them to one topic — so there is no per-topic map to express; and `schemaSettings`, `labels` and
  `tags` were considered and declined (schema validates at publish time only, re-checking what this
  sink serialized, and is invisible to subscriptions beyond the `googclient_schema*` attributes —
  its payoff accrues to GCP-managed consumers like BigQuery export subscriptions, not to the Flink
  pipeline, and its evolution model, single-file Avro/proto definitions with a bounded revision
  range managed through topic updates, means support would not end at creation; labels/tags mirror
  the subscription side's omission) — all additive later. **Deliberately no follow-up issue for
  schema support** (decided with the user on #153): the declined record here and on the docs page
  is the anchor, and a future issue needs a real consumer-side use case, not a speculative
  placeholder. The emulator stores
  all four knobs verbatim and returns them on `GetTopic` — measured in #153 after a first
  measurement wrongly concluded the opposite off a one-line grep of a multi-line proto `toString`
  — but validates nothing and shows no effect, so the ITs assert the round trip and the
  *semantics* (real CMEK, residency, retention-driven replay) stay with the real-GCP suite (#82).
  **The source never creates a topic.** There is no `createTopic` in the `source` package, so
  `scan.auto-create.topic` names a topic that must already exist, while the sink's
  `create-if-needed` does create one — the same two words meaning opposite things across one DDL,
  which is why both user-facing documents now say so outright. A sink-created topic without
  creation settings still takes every `Topic` field's service default, message retention among
  them, so a backwards seek over it replays nothing that was already acknowledged unless
  `messageRetention` was set at creation
- **`flink-sql-connector-gcp-pubsub`, the uber-jar** (#138) — the repository's first shaded module,
  so what is decided here sets the shape every later `flink-sql-connector-gcp-*` will copy.
  **Everything bundled is relocated under `io.github.flink.gcp.connector.pubsub.shaded.`, with no
  exemption for `grpc-netty-shaded`.** The exemption is the tempting answer and was built first:
  that artifact carries native libraries whose names netty derives from its own package, and
  maven-shade does not rename native resources. It was rejected on a measurement rather than a
  preference — with `io.grpc.netty.shaded` left in place the jar cannot share a classpath with
  anything else carrying that package, failing with `ServiceConfigurationError: NettyChannelProvider
  not a subtype`, and the *first* thing to trigger that would be a second GCP SQL connector built
  the same way. The price is two path relocations renaming
  `META-INF/native/(lib)?io_grpc_netty_shaded_netty*` to the relocated prefix with dots as
  underscores; both forms are needed because Windows DLLs carry no `lib`. **That is the established
  form and it was checked, not assumed**: identical pairs are in googleapis/java-bigtable-hbase
  (citing netty#6995 and grpc-java#2485), Dataproc's gcs-connector, spark-bigquery, Beam's
  `GrpcVendoring`, and the uber-jars of both Google Flink connectors — while
  GoogleCloudDataproc/flink-bigquery-connector relocates without renaming and ships a jar whose
  tcnative and epoll can never load. `rawString` is **not** needed (maven-shade matches resource
  paths directly) and no surveyed project uses it. The replacement is constrained by netty's
  `calculateMangledPackagePrefix()`: the relocated name must remain a pure *prefix* of
  `io.netty.util.internal.NativeLibraryLoader` — Beam gets away with collapsing `io.grpc.netty
  .shaded` to its vendor root only because what remains still satisfies that — and an underscore in
  the prefix would have to be spelled `_1`, which is why the shaded prefix must not grow one.
  `PubSubSqlConnectorPackagingITCase` derives the expected string from the shaded prefix rather than
  repeating it, so config and assertion cannot drift. Untested residue, deliberately: whether the
  renamed libraries load through JNI is only exercised on Linux with epoll or tcnative, and a wrong
  rename degrades to NIO and JDK SSL *silently*. Still unrelocated are `org.conscrypt` (native
  libraries too, but a reflectively-loaded optional TLS provider gRPC does without) and four
  annotation-only packages, where a duplicate class is inert because nothing invokes it.
  Three build traps worth not rediscovering. **Declaring a Google artifact at `test` scope in the
  SQL module demotes it out of the bundle** — Maven's nearest-definition rule beats the transitive
  `compile` scope — which silently cut the jar down to guava plus a few annotation jars.
  `maven-dependency-plugin:analyze` is absent for the same reason: the scoping it would demand is
  the scoping that breaks the bundle, so the test harness uses classes that arrive transitively and
  declares none of them. **`artifactSet` is `*:*`, and the enumerated include list it replaced should not come back.**
  The list's justifications each died when measured: an unlisted new transitive does *not* fail the
  build, contrary to what #138 assumed — it is silently dropped from the jar, the worst available
  outcome — and "readable beside the NOTICE" ended when the NOTICE became generated. With the
  wildcard a new dependency is bundled automatically; what remains human is relocating a genuinely
  new package root (a real decision — conscrypt must *not* be, and commons-lang3 under
  `org.apache.commons` would arrive unrelocated because only `commons.codec` is mapped, measured),
  and the packaging tests fail with the artifact's name until it is made.
  `BundledDependenciesNoticeTest` diffs the NOTICE against the recorded runtime tree both ways. **`ApacheNoticeResourceTransformer` needs `organizationName` and `inceptionYear`,
  not just `projectName`**, or the aggregated NOTICE still reads "Copyright 2006-2026 The Apache
  Software Foundation". Relatedly, the root pom now sets `<organization>`: without it the ASF
  parent's remote-resources bundle stamped that same claim into *every* module jar this project
  builds.
  **The NOTICE's prose is hand-written; everything mechanical is generated and pinned.** The split
  is `NOTICE.template` (module root): human paragraphs plus one `{{Licence}}` placeholder per
  group, which `scripts/check-notice.py --update` fills from what license-maven-plugin resolved —
  so a wrong group, a duplicate bullet or a stale version is not a checkable mistake but an
  *inexpressible* one. `just update-notice <module>` regenerates; `just check-notice <module>`
  (CI) re-renders in memory and fails on any drift, offline. Licence *texts* come only from
  `scripts/licence-sources.toml`, each entry pinned by **sha256** with its provenance recorded:
  the artifact's own jar where one ships a text (threetenbp, javax.annotation-api — best
  provenance, version-exact), otherwise a curated URL whose ref matches the bundled version and
  whose note says why (protobuf's Java 4.33.x maps to core tag v33.x; gax and google-auth live in
  *archived* — hence frozen — repositories with no tag for these versions; POM-declared URLs are
  often HTML pages or bare templates, and the script rejects HTML outright). A fetch that stops
  hashing to its pin fails: upstream changed, a human reviews. This replaced an earlier state
  where five texts had been curl'd from repository heads chosen by hand — wrong provenance, and
  the reason the pin exists. **Curating a new entry follows a fixed fallback ladder** (also printed by the failure
  message): (1) a licence file inside the artifact's own jar; (2) the publisher's repository at
  the tag matching the bundled version; (3) the publisher's repository head only when it is
  frozen (archived) or no version tag exists, with the reason in the note; and there is no rung
  4 — a generic template is not the project's text, since the copyright line is part of a BSD or
  MIT licence, so an artifact with no pinnable publisher text is a reason to question the
  dependency, not to substitute one. The curation itself is judgment; everything after the pin
  is mechanical. Measured before any of this was built: the plugin's classification matched
  the hand grouping on **all 52 artifacts**, including the two that inherit `<licenses>` from a
  parent pom (guava, animal-sniffer), the dual-licensed `javax.annotation-api`, and re2j's
  non-SPDX "Go License". `licenseMerges` in the root pom's `pluginManagement` is what makes the
  vocabulary stable — this tree alone spells Apache-2.0 six ways — and it lives there so a
  sibling SQL module inherits one vocabulary rather than inventing a second.
  **What a sibling actually costs, measured against the BigQuery module's 113-artifact tree**: the
  plugin block and one execution in its pom, its own `NOTICE.template`, a CI step, and
  `licence-sources.toml` entries for its non-Apache artifacts (the file and its pins are shared, so
  overlapping dependencies cost nothing twice). No new `licenseMerges` — that list was extended
  here, once, to cover the spellings that tree adds (`Apache License V2.0`, `BSD 3-clause`,
  `MIT License`, `The MIT License`, `The BSD 2-Clause License`), and `failOnMissing` does not fire
  on it. The test trio is the other reusable half: `ShadedJar` is generic modulo two constants
  (artifact id, shaded prefix), `BundledDependenciesNoticeTest` is module-relative already, and
  the packaging IT's skeleton carries over — including the netty-native assertions, since the
  BigQuery tree bundles gRPC too. **Deliberately not extracted yet**: sharing needs a test-jar or
  a shared test module, and generalising on one consumer means guessing the parameterisation —
  extract when the second SQL module actually lands, shaped by what it needs (the call #27 made
  for the emulator harnesses, extracted once the third consumer existed).
  **`download-licenses` must not be used for the licence texts**: it names files after the *licence*,
  so protobuf, gax, google-auth and threetenbp collapse into one BSD-3-Clause file and the last
  download wins. Measured — it left ThreeTen's copyright line standing for Google's code, and the
  copyright holder is part of a BSD or MIT text.
  **Invoke the goal through a phase, never as a bare `license:add-third-party`** — a
  CLI goal invocation selects reactor modules but does not build them, so `-pl` cannot resolve the
  connector the module bundles, not even with `-am`, unless an earlier `install` happened to leave
  it in the local repository. That failed in CI and passed locally twice for exactly that reason.
  It costs nothing to bind: the goal reads POMs Maven has already resolved and fetches nothing —
  the network-using goal is `download-licenses`, which is the one not used here.
  `BundledDependenciesNoticeTest` overlaps the script's first check deliberately: the comparison is
  a Python script rather than a Maven plugin, so it is a CI step of its own, and the test is what
  makes the same drift fail inside `just verify`
