# CLAUDE.md — flink-connector-gcp-base

Rules for the shared main-code module (#61). Read before adding anything here; each decision's
record — context, evidence, declined alternatives — is the named ADR under `docs/adr/`.

## Scope and dependencies

- **Main-code shared infrastructure only** — test-support code stays in
  `flink-connector-gcp-test-utils`, whose CLAUDE.md records the mirror-image rule. Everything
  here is `@Internal` **except `base.failure`** (a user-implemented SPI cannot be internal —
  `docs/adr/0036`); a second public package needs the same kind of argument. **A type only moves
  in once it has multiple consumers.**
- Dependencies are `flink-core` (provided) plus `gax`/`grpc-api`/`protobuf-java` (BOM-managed).
  Consumers depend on this module at **compile** scope, so it is bundled into the
  `flink-sql-connector-gcp-*` uber-jars and must be relocated there (`docs/adr/0015`), and it is
  on the justfile `binary-compat`/`e2e` install lists for the reactor-resolution reason
  test-utils is (#181).
- No compat source roots (`src/main/java-flink1`/`java-flink2`): nothing here touches the
  1.x/2.x `Sink` API gap. `DefaultFailureHandlerContext.of(WriterInitContext)` is not a
  counter-example — the type and both methods it reads exist identically in 1.20 and 2.x.

## `base.failure` (`docs/adr/0036`)

- Handler `flush()` runs after each writer's write-path drain; the guarantee is at-least-once
  for failures that recur on replay; `open()` grows only for a real consumer. Which failures are
  row-level stays per-connector. `getConnector()` values are lower-case module words and are
  API. The policy semantics every writer shares live in the `FailureHandler` javadoc.

## `base.metrics` and metric naming (`docs/adr/0037`, `0038`)

- `ErrorClassCounters` and `DestinationMetrics` are **task-thread only** (plain counters); a
  connector counting from a callback thread must not reuse them. Entries are never removed —
  Flink cannot unregister a metric.
- **`numRecordsSend` counts each record once, at the first hand-off**, inside the send call
  under a first-attempt flag (`docs/adr/0037`).
- Every connector declares its names in one `<Product>MetricNames` inventory at its module root;
  counters name events, gauges name states, and **no name takes Flink's `num` prefix**
  (`docs/adr/0038`; the mechanical half is `just check-metric-docs`).

## `base.retry` and `base.rpc` (`docs/adr/0039`)

- Retry loops stay in the connectors; do not add a `Retries.run` executor. Every schedule
  jitters at `RetrySchedule.DEFAULT_JITTER_RATIO` — a literal ratio is a review finding, and the
  ratio is never a knob. Knob-to-schedule mapping lives on the options class that owns the
  knobs (`toRetrySchedule()`), never in the consumer.
- `Retries.sleep` call sites name what is being waited for. `StatusCodes.codeOf` inspects one
  throwable and never walks the cause chain — traversal and classification stay per-connector.
- `EmulatorEndpoint` is the only form an endpoint travels in past the setter; whitespace is
  rejected, never trimmed, and the host splits at the last colon, kept verbatim. Public
  signatures stay `String`.

## `base.lifecycle` (`docs/adr/0040`, `docs/adr/0007`)

- **`BoundedShutdown`**: why it exists, and the decisions inside it, are `docs/adr/0007`; the
  class contract (daemon thread, nullable-`Runnable` release hook, idempotent `close()`,
  one-thread precondition, per-field threading, the caller-supplied `LongAdder`'s ownership
  argument) lives in its own javadoc, published by the API reference. What belongs here: it is
  client-agnostic by construction (two functional values, a `String description`, no gax or gRPC
  import — the module gained no dependency); its warnings log under
  `base.lifecycle.BoundedShutdown`, which a `…connector.pubsub`-scoped log configuration stops
  matching; its give-up message carries **no issue link**, deliberately — a shared class must
  not send one client's operator after another client's defect. `timeout()` is the module's
  first `@VisibleForTesting public` method, public only because a sibling module's tests read
  it; the next seam here should cite this rather than widen by default. The Pub/Sub source's
  subscriber teardown was measured (#325) and is **not** a candidate adopter
  (`docs/adr/0012`). A first draft held an `AtomicLong` here; do not reintroduce it.
- **`Closers`**: every `close()`-shaped call site goes through `closeAll` /
  `closeAllSuppressing`; the contract lives in the class javadoc, the written-out-loop decision
  and its `Error`-type reasoning in `docs/adr/0040`. A new call site owes the `Error` test
  beside the exception ones, and creation guards catch `Throwable`, not `Exception`.
