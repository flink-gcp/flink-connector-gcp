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
  beside their own per-destination state. Both types are **task-thread only** — plain
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
