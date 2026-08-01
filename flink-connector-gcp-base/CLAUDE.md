# CLAUDE.md — flink-connector-gcp-base

Design decisions for the shared main-code module (#61). Read before adding anything here.

- **Main-code shared infrastructure only.** This is the module #61 (retry) and #37 (DLQ/metrics)
  planned; test-support code stays in `flink-connector-gcp-test-utils`, whose CLAUDE.md records
  the mirror-image rule. Everything here is `@Internal` — the public knobs live on each
  connector's own options objects, which map onto the internal types here. A type only moves in
  once it has multiple consumers (the same bar test-utils applies).
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
- **Dependencies are `flink-core` (provided) plus `gax`/`grpc-api` (BOM-managed).** Unlike
  test-utils, consumers depend on this module at **compile** scope, so it is bundled into the
  `flink-sql-connector-gcp-*` uber-jars and must be relocated there like any other bundled
  package root (the Pub/Sub SQL module's CLAUDE.md records the shading rules); it is also on the
  justfile `binary-compat`/`e2e` install lists for the same reactor-resolution reason
  test-utils is (#181).
- No compat source roots (`src/main/java-flink1`/`java-flink2`): nothing here touches the
  1.x/2.x `Sink` API gap.
