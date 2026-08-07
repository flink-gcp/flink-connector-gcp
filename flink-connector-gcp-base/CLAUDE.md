# CLAUDE.md — flink-connector-gcp-base

Design decisions for the shared main-code module (#61). Read before adding anything here.

- **Main-code shared infrastructure only.** This is the module #61 (retry) and #37 (DLQ/metrics)
  planned; test-support code stays in `flink-connector-gcp-test-utils`, whose CLAUDE.md records
  the mirror-image rule. Everything here is `@Internal` **except `base.failure`** — the public
  knobs live on each connector's own options objects, which map onto the internal types here.
  The exception is structural, not preferential: `base.failure` is a user-*implemented* SPI
  (#37), an interface users implement cannot be internal, and keeping it per-connector would
  mean three copies and no cross-connector `DeadLetterQueue` — the point of that issue. A second
  public package needs the same kind of argument. A type only moves in once it has multiple
  consumers (the same bar test-utils applies).
- **`base.failure` is the shared failure SPI** (#37, first consumer BigQuery via #205):
  `FailedElement` (read-only contract — connector id, destination string, nullable payload
  bytes, message, cause), `FailureHandler<F extends FailedElement>` (`handle` = drop-or-throw,
  default `open(FailureHandlerContext)`/`flush()`/`close()`; built-ins `failJob()` — the default
  everywhere — `logAndDrop()`, `sendToDeadLetterQueue`), `DeadLetterQueue` (`@Experimental`,
  `offer` + the same lifecycle, driven by the `sendToDeadLetterQueue` handler), and the
  `@Internal` `DefaultFailureHandlerContext` the sinks build from their `WriterInitContext`.
  Decisions not to re-litigate: **`flush()` runs from each writer's `flush(boolean)` after the
  write-path drain** — failures are discovered by the drain, so flushing first would checkpoint
  past unflushed dead letters; the guarantee is stated as **at-least-once for failures that
  recur on replay**, and exactly-once is deliberately not offered (it would require the
  dead-letter write to join the sink's own commit protocol); `open()` carries subtask index and
  metric group and nothing more — grow it only when a real consumer demands it. The generic
  parameter keeps `failedRowHandler(...)`-style setters typed per connector while
  `FailedElement` lets one `DeadLetterQueue` implementation serve every connector; the
  built-ins' unchecked casts are safe because handlers only consume elements, and the connector
  builders' setters take `FailureHandler<? super X>` so a cross-connector
  `FailureHandler<FailedElement>` is accepted without a cast. `getConnector()`
  values are lower-case module words (`bigquery`, `pubsub`, `cloudtasks`) and are API —
  dead-letter consumers key on them. `describeDestination()` is not `getDestination()` because a
  connector's concrete type keeps a typed `getDestination()` (BigQuery's returns
  `TableDestination`), and a same-signature `String` override would be an irreconcilable clash. **Which failures are row-level stays per-connector** (only
  data-shaped failures reach a handler), exactly as retryability classification stays
  per-connector under #61; the Pub/Sub adoption is #206 (its module CLAUDE.md records where that
  connector puts the boundary and why), Cloud Tasks' is #207. `protobuf-java`
  (BOM-managed) is here for `ByteString` on `FailedElement`.
- **`base.metrics` is the shared sink-metric helper package** (#208, first consumers Pub/Sub #208
  and Cloud Tasks #209), and unlike `base.failure` it is `@Internal` throughout — nothing here is
  implemented by a user, so the module's default rule applies without an exception. Two types.
  `ErrorClassCounters` registers `errorClass.CODE.errors`, `CODE` being a gax `StatusCode.Code`
  name or `UNCLASSIFIED`; child counters are created on first use, because registering ~17 rows per
  subtask for statuses a job never sees is what the laziness avoids, and **which throwable in a
  chain classifies a failure stays at the call site**, exactly as `StatusCodes.codeOf` leaves
  traversal there (Pub/Sub matches any element, Cloud Tasks the first classifiable one — the #61
  do-not-converge decision, extended to metrics). `DestinationMetrics` is the opt-in per-destination
  pair (`recordsSend`/`sendErrors`, `perDestinationMetrics` default false on every connector's
  options object): **Flink cannot unregister a metric**, so an unconditional subgroup per
  destination would grow the registry for the task's lifetime against an unbounded destination set,
  and the same fact is why **entries are never removed** — a destination whose writer state was
  evicted and rebuilt reuses its counters, since re-registering the name would be refused. It hands
  out a `Counters` handle rather than taking a destination name per record, so the name is composed
  once per destination and a disabled instance costs two null checks; call sites cache the handle
  beside their own per-destination state, or look it up per request where they keep none (Cloud
  Tasks; BigQuery's default-stream writer, which counts per *batch* and rebuilds that state on
  every repair). `Counters.recordsSent(long)` is `recordSent()`'s batching form and the same
  counter, added for BigQuery (#210), where one append carries n rows. Both types are
  **task-thread only** — plain
  `SimpleCounter`s, valid because every sink increment site in this repository runs on the task
  thread, unlike the Pub/Sub *source*, whose SDK callback threads forced `ThreadSafeSimpleCounter`.
  A connector counting from a callback thread must not reuse them as they stand. `flink-test-utils`
  is a *test*-scope dependency here for `MetricListener`, so the helpers are asserted through the
  names they register under rather than through the counter objects they hold.
- **`numRecordsSend` counts each record once, at the first hand-off, in every connector** (decided
  on #208, superseding the #37 design's "retries re-count"): a sink-owned retry — Pub/Sub's
  topic-creation republish, Cloud Tasks' park-and-redispatch, BigQuery's re-append — must not count
  the record again. The increment therefore goes **inside the send call, guarded by a first-attempt
  flag** (`PubSubWriter.publishTo`'s `firstAttempt`, `CloudTasksWriter.dispatch`'s `pending == null`)
  — not at the `write()` call site, which would count a record the client rejected synchronously,
  and not unguarded, which would count attempts. Bigtable already counts once because its retries are inside the SDK
  batcher, so the four connectors report one quantity and a dashboard comparing them is honest. What
  is given up is stated on the docs pages: `numBytesSend` is payload volume, not wire volume. Retry
  volume is read from the `errorClass.CODE.errors` counters instead, which is per status code and
  strictly more informative than a re-counted send.
- **Every connector declares its metric names in one `<Product>MetricNames` class at its module
  root** (#280), and that file is the connector's inventory: what it reports can be read there
  without opening a writer, a reader or the enumerator. Two things follow. A connector's metric
  names stay **inside that connector**, so implementing one needs nothing from `base` — a shared
  holder for the names several connectors happen to share was built first and **withdrawn**, because
  it split each connector's inventory across two modules to close one narrow drift, and the
  connector-local file is what a maintainer reads. And cross-connector consistency is checked by
  **diffing those four files**, which is the whole mechanism: a name meaning the same thing in two
  connectors should be spelled the same way, and nothing automated says so. What the class does not
  hold: Flink's standard names, which come from metric-group accessors rather than from a name, and
  the subgroup leaves `base.metrics` registers on a connector's behalf. The registering classes take
  every name from it — a `*Metrics` class declaring its own constant puts the inventory back in two
  places.
- **A metric this repository registers itself is a lowerCamelCase noun phrase, and its shape says
  which kind it is** (#280) — the convention every connector here follows, with **no exceptions in
  the tree**:
  - a **counter** names the *event* it counts, `<plural noun><past participle>`: `tablesCreated`,
    `topicsCreated`, `tasksDeduplicated`, `messagesReceived`/`Acked`/`Nacked`/`Dropped`,
    `recordsSkipped`, `loadJobsSubmitted`, `filesStaged`. A count of occurrences with no actor to
    name is a plain noun phrase instead — `appendRetries`, `schemaReconciliations`, `errors`;
  - a **gauge** names the *state* it reports, `<adjective or participle><plural noun>`:
    `openDestinations`, `inFlightBatches`/`Appends`/`Mutations`/`Messages`/`Tasks`/`Bytes`,
    `parkedMessages`, `parkedTasks`, `assignedSplits`, `unassignedReaders`, `pendingAcks`,
    `pendingCheckpoints`.

  Read it as a test, not decoration: a name in the wrong shape reports the wrong kind of quantity
  to whoever reads the dashboard. #280 found exactly two that did and renamed both — `stagedFiles`
  (a *counter* of finished staging files, which read as "how many are staging right now") and
  `checkpointsPendingAck` (a *gauge*, which read as a count of events) — so a later addition that
  does not fit the shape is a review finding rather than a precedent.
  **Flink prescribes nothing here**: FLIP-33 standardizes a *list of names* and explicitly leaves a
  connector's own names alone, so the convention is this repository's to keep. It is not invented,
  though — Flink's own connectors have the same shape, checked before this was written: Kafka's
  `commitsSucceeded`/`commitsFailed` (counters) beside `committedOffsets`/`currentOffsets`
  (gauges), Kinesis's `millisBehindLatest`, `averageRecordSizeBytes`, `loopFrequencyHz`, HBase's
  `lookupCacheHitRate`.
  The one deliberate departure is `DestinationMetrics`' subgroup leaves, `recordsSend` and
  `sendErrors`: they are not a pair by this rule, and are not meant to be — each is Flink's
  standard name with the `num` prefix dropped, so `destination.X.recordsSend` reads against
  `numRecordsSend`, which is the quantity it partitions.
- **A metric this repository registers itself never takes Flink's `num` prefix** (#280), which is
  the one part of the shape rule above that is mechanical enough to check. `num…` is Flink's own
  vocabulary — `MetricNames` spells 22 such names, and `SinkWriterMetricGroup` exposes four
  (`numRecordsOutErrors`, `numRecordsSendErrors`, `numRecordsSend`, `numBytesSend`), of which every
  writer here takes three — so a custom counter inside it costs two things. A reader cannot tell it
  from a Flink-defined one except by the docs table's `counter (Flink standard)` column; nothing in
  the source says which it is. And it can be silently dropped: `AbstractMetricGroup.addMetric`
  resolves a name collision by keeping the metric **already** registered and logging `Name
  collision: Group already contains a Metric with the name '…'. Metric will not be reported.`, and
  `InternalSinkWriterMetricGroup` registers all four in its **constructor** — so ours is always the
  later one, and so the one dropped. That second cost is what aims the rule at the sink writer group
  in particular, and it is also why `pendingAcks` and `pendingCheckpoints`
  (`PubSubSourceReaderMetrics`) are **not** counter-examples despite sitting in the family of
  Flink's `pendingRecords`/`pendingBytes`: those two are registered only when a connector calls
  `setPendingRecordsGauge`/`setPendingBytesGauge`, and this source calls neither (`pendingRecords`
  deliberately, for the reason that class's javadoc gives), so there is nothing there to collide
  with. `pending…` is also plain English for what a gauge reports, which the shape rule above
  requires; `num…` is not a word.
  The one counter that had taken the `num` prefix was `recordsSkipped` (#230, `numRecordsSkipped`
  until #280 renamed it), and the argument offered for keeping it is recorded here as **measured
  false** so it is not re-argued: `numRecordsSend`, `numRecordsSendErrors` and the skip counter do
  *not* partition every record the writer is handed, in five of the six writers. `numRecordsSend`
  counts records handed **to the client**, so a record the service then rejects is counted by it
  and by `numRecordsSendErrors` both. `FileLoadsWriter` alone partitions, and only because it makes
  no per-record request.
- **Every schedule jitters, at one shared ratio, and the ratio is never a knob** (#197). The
  maintainer's standing posture is exponential backoff *with* jitter, so
  `RetrySchedule.DEFAULT_JITTER_RATIO` is the only ratio in the repository — a connector passing
  a literal is a review finding, and passing `0` needs a recorded reason (nothing in main sources
  does today; the constructor still accepts it because tests want deterministic backoffs). One
  number rather than a per-site choice because the value is not load-bearing: the jitter is
  **mean-preserving** (factor in `[1 - r, 1 + r]`), so it costs the budget nothing in
  expectation and only has to be non-zero. That also disposes of the pre-#197 argument that a
  short budget cannot afford jitter — true of full jitter, false of this shape. Not exposed as a
  builder knob: it fails the workload-property test the `recovery*`/`retry*` knobs pass. The
  AWS-taxonomy variants (full, equal, decorrelated) are unadopted **in `RetrySchedule`** — #197's
  Question 2 — and one arrives only with the call site whose measurement justifies it. Two
  full-jitter waits do exist outside the type and are not counter-examples:
  `BigQueryDefaultStreamWriter.sleepJitter()` spreads subtasks across a metadata-update quota
  rather than backing off a retry, and gax jitters the SDK's own in-stream retries over
  `[0, delay)` beneath these schedules.
- **A connector's knobs are mapped onto a `RetrySchedule` by the options class that owns them**,
  as `CloudTasksWriterOptions.toRetrySchedule()` always did and as `DefaultStreamOptions`,
  `BufferedStreamOptions` and `PubSubPublisherOptions` now do (#197). Never in the consumer: one
  method then serves every consumer of the same knobs (the buffered writer and its committer),
  and the mapping becomes directly unit-testable — a ratio silently regressing to zero inside a
  writer constructor is otherwise unobservable, which is exactly the mutant #197's tests had to
  kill.
- **Retry loops stay in the connectors; only the schedule, the backoff sleep and status-code
  extraction are shared.** #61's plan sketched a `Retries.run(schedule, isRetryable, action)`
  executor, and it was evaluated against every loop and adopted nowhere (recorded on #61): all
  seven measured loops are not plain predicate-retry — success-via-exception in
  `BufferedStreamCommitter.flush`, repair side effects in `createStream`, a mid-loop schedule
  swap in `retryBatches`, condition-driven (not exception-driven) retry in
  `LoadJobOrchestrator`'s schema loop, unbounded completion polling in
  `BigQueryLoadJobRunner.awaitJob`, drain-based success in `PubSubWriter.repairDestination`, and
  no loop at all in Cloud Tasks' park-and-redispatch writer — and each carries site-specific
  messages and logging that tests pin. Do not add an unused executor; a future consumer with a
  genuinely plain loop is what would justify one.
- **`Retries.sleep` takes the interruption message as a parameter** because the five call sites
  it replaced each named what was being waited for ("…to retry appends to BigQuery", "…for
  BigQuery job <id>", …), and flattening them to one message would have discarded diagnostic
  context. New call sites follow suit: name the thing being waited for.
- **`StatusCodes.codeOf` inspects one throwable and never walks the cause chain.** Which element
  of a chain classifies a failure is per-connector policy (Pub/Sub matches any element,
  Cloud Tasks takes the first classifiable one), so the traversal stays at the call site with
  `ExceptionUtils.findThrowable`. Classification itself — which codes are transient, terminal,
  row-level — also stays per-connector (#61's decision); BigQuery's `AppendErrorClassifier`
  deliberately does not use this helper, since it targets `io.grpc.Status.Code` with
  gRPC-first precedence and feeds typed code sets, and converting it would churn the classifier
  for no dedup gain.
- **`EmulatorEndpoint` is the parsed form of every connector's `emulatorEndpoint(String)`, and
  the only form that travels past the setter** (#235). It shares `base.rpc` with `StatusCodes`
  rather than taking a package of its own — that package is the client seam in both directions,
  codes out of client exceptions and an address into client settings, and a one-class package
  would fail the #119 layer test. The five setters (four connectors plus `PubSubDeadLetterQueue`)
  parse at `build()` time, and the configs, factories and admins behind them carry the type, so a
  client can never be handed an endpoint nothing has checked; the Bigtable factory's inline parse,
  which was the only one anywhere, moved here. Public signatures stay `String`: the type is
  `@Internal` and must not leak into a `@PublicEvolving` one. Two parse decisions not to
  re-litigate — **whitespace is rejected, never trimmed** (a trimmed value is silently a different
  endpoint from the one configured, and the stray space is one of the typos #235 exists to catch),
  and **the host is split at the last colon and kept verbatim**, which is exactly what
  `DefaultMutationBatcherFactory` did before, so a bracketed IPv6 literal reaches the client
  unchanged and `getTarget()` reconstructs the input. One message covers every malformed value
  (`emulatorEndpoint must be host:port, was '<value>'`), which is why the old "must not be blank"
  is gone: a blank endpoint is not a separate kind of mistake.
- **`BoundedShutdown` is `base.lifecycle`'s one class** (#265 built it inside
  `DefaultPublisherFactory`; #312 moved it here): one client's shutdown and its termination wait,
  both on a daemon thread, under one deadline that the caller's single `join` is the whole of. It
  arrived when its second consumer did — the Pub/Sub sink's per-topic publishers and
  `PubSubDeadLetterQueue` — which is this module's multiple-consumers bar met exactly, and it joined
  an existing package rather than taking one of its own, for the reason `EmulatorEndpoint` joined
  `base.rpc`. Why it exists at all, and the six decisions inside it, are the Pub/Sub module's
  CLAUDE.md; **the class contract lives in its own javadoc** — the daemon thread, the
  release hook's nullable-`Runnable` shape, the idempotent `close()`, the one-thread precondition
  with its per-field threading account, and the caller-supplied `LongAdder`'s ownership argument
  (#311, with the 10 ms-reporter measurement) — which the aggregated API reference publishes, so
  none of it is restated here. What belongs here is the shape the move imposed and what the class
  cannot say about itself. It is **client-agnostic by construction**: two functional values rather
  than a client, a `String description` rather than a destination type, and no gax or gRPC import
  — so the module gained no dependency. Its warnings log under `base.lifecycle.BoundedShutdown`
  rather than the connector's package, which a log configuration scoped to `…connector.pubsub`
  stops matching; and its give-up message carries **no issue link**, deliberately — a shared class
  must not send one client's operator after another client's defect, and #265 is unreachable for
  the dead-letter queue's publisher. `timeout()` is the module's first `@VisibleForTesting public`
  method, and it is public only because a *sibling module's* tests read it; that is the price of
  promoting a test seam, and the next one here should cite this rather than widen by default.
  The Pub/Sub **source**'s subscriber teardown — the nearest future adopter — was measured on
  #325 and is **not** a candidate: `Subscriber.stopAsync()` returns at once and `awaitTerminated`
  already takes the budget, so the *task thread's* wait is bounded without this class (the SDK's
  own non-daemon shutdown thread is the SDK's, not a wait of ours, and this class could not take
  it either). A first draft held an `AtomicLong` here and documented the resulting bound instead
  of removing it; do not reintroduce it.
- **`base.lifecycle` is also two methods, one loop** (#229 then #276), and every `close()`-shaped call
  site in this repository goes through one of them — nothing calls `IOUtils.closeAll` any more, so
  its `scripts/flink-api-tiers.toml` entry is gone and `ExceptionUtils`' covers both users.
  `closeAll(closeables)` is for when **closing is the operation**;
  `closeAllSuppressing(failure, closeables...)` for when **something else already failed** — a
  writer creation that got part-way. **The contract lives in the `Closers` javadoc** — null
  handling, close-everything-before-reporting, first-failure-wins, the type-preserving rethrow,
  the JVM-fatal escalation with its stated bound, and the nested shape of multiple close failures
  — which the aggregated API reference publishes, so none of it is restated here.
  **The loop is written out rather than delegated to `IOUtils.closeAll`, and that is #276's whole
  decision** — reversing #229's "delegate rather than reimplement", which is worth stating because
  the reversal was argued, not drifted into: `IOUtils.closeAll` rethrows *from inside its loop*
  anything its `Class` argument does not cover, so `Exception.class` (what the one-argument
  varargs form passes) abandoned every later resource on an `Error` — the live bug at nine
  sites, with the failure handler last in six of the lists and, since #211, owning an SDK
  `Publisher` and a gRPC `ManagedChannel` — while `Throwable.class` wraps an `Error` as
  `new Exception(e)`, which `Task.preProcessException` then never halts on. Ten lines keep the
  type, which is what both `closeAll` tests and every site's `Error` test assert.
  For the same reason the six creation-guard call sites catch **`Throwable`, not `Exception`** —
  a client's first classload failing with `NoClassDefFoundError` repeats on every restart attempt
  — which precise rethrow makes compile without widening any `throws` clause. The module's
  multiple-consumers bar was cleared on arrival with six call sites — the five sinks #229 fixed
  plus `BigtableMutateRowsSink`, whose private `closeSuppressing` it replaced so the tree keeps
  one idiom — and #276 added nine more.
- **Dependencies are `flink-core` (provided) plus `gax`/`grpc-api`/`protobuf-java`
  (BOM-managed).** Unlike
  test-utils, consumers depend on this module at **compile** scope, so it is bundled into the
  `flink-sql-connector-gcp-*` uber-jars and must be relocated there like any other bundled
  package root (the Pub/Sub SQL module's CLAUDE.md records the shading rules); it is also on the
  justfile `binary-compat`/`e2e` install lists for the same reactor-resolution reason
  test-utils is (#181).
- No compat source roots (`src/main/java-flink1`/`java-flink2`): nothing here touches the
  1.x/2.x `Sink` API gap. `DefaultFailureHandlerContext.of(WriterInitContext)` is not a
  counter-example — the type and both methods it reads (`getTaskInfo()`, `metricGroup()`) exist
  identically in 1.20 and 2.x; the compile-breaking gap is only `createWriter(Sink.InitContext)`
  (the other delta the root file's version policy records, `getCheckpointId()`'s return type, is
  dodged by `getCheckpointIdOrEOI()` and touches nothing here).
