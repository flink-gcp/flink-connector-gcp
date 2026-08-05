# CLAUDE.md — flink-connector-gcp-test-utils

Design decisions for the shared test-utils module (#27). Read before adding anything here.

- **Test-support code only, forever.** Production code shared across connectors — retry
  (`RetrySchedule`, #61), DLQ/metrics (#37) — goes to the *main-code* shared module,
  `flink-connector-gcp-base` (created by #61; its own CLAUDE.md records the mirror-image rule).
  Utilities live in `src/main/java` here only so siblings can consume the plain jar at `test`
  scope (the `flink-test-utils` shape); that placement does not make them production code.
- **Every dependency is `provided`.** Consumers' test classpaths already carry what they exercise,
  and `provided` is non-transitive, so depending on this module adds no new resolution path to any
  artifact — which is what keeps the SQL uber-jar's shade-scope mediation undisturbed (a Google
  artifact reachable at `test` scope can win Maven's nearest-definition mediation and demote itself
  out of the shaded bundle; see the SQL module's CLAUDE.md).
- **No forced unification of emulator container fixtures.** The goccy BigQuery and aertje
  Cloud Tasks fixtures are hand-rolled, single-consumer, and structurally unlike the testcontainers
  `PubSubEmulatorContainer`; they stay in their modules (recorded on #27). Only what has multiple
  consumers moves here.
- **`StubWriterInitContext` answers what a sink reads and throws for everything else** (#206, its
  second consumer — it arrived with #205 in the BigQuery test tree and moved when Pub/Sub needed
  it). The unsupported methods are the point: a sink growing a new dependency on the context shows
  up as a failing test rather than as a silent null. Two things are deliberate. The metric group is
  held in a **field**, so a test can assert by identity that it reached whatever it was handed to;
  and the mailbox is a real `FakeMailboxExecutor`, because the Pub/Sub sink's production
  `createWriter` takes one — where BigQuery's stub had thrown. No compat source root is needed:
  `WriterInitContext` and every method overridden here exist identically in 1.20 and 2.x.
  That field was a **null-returning `Proxy` until #208**, and the replacement was forced rather than
  chosen: once a writer captures counters in its constructor, every test building one through
  `createWriter(context)` dies on a `NullPointerException`, so the proxy and the metrics half could
  not coexist. It is now a real `TestSinkWriterMetricGroup`, which keeps the identity property and
  adds read-back.
- **`TestSinkWriterMetricGroup` is the shared sink metric-group harness** (#208): a
  `ProxyMetricGroup` over a `MetricListener` group, with the FLIP-33 standard counters registered
  under their documented names rather than merely held. Everything is asserted **by registered
  name**, so a renamed or unregistered metric fails its test — which is why the two obvious
  alternatives are unusable: `UnregisteredMetricsGroup.createSinkWriterMetricGroup()` hands out a
  fresh `SimpleCounter` per call, leaving what the writer captured unreachable, and
  `InternalSinkWriterMetricGroup` has no `mock(...)` in either supported Flink line (measured on
  1.20.4 and 2.2.1: a package-private constructor and `wrap(OperatorMetricGroup)`, which a listener
  group cannot satisfy). It brings `flink-test-utils` into this module at `provided`, so **a module
  using the harness declares `flink-test-utils` at test scope itself** — provided being
  non-transitive is the property this pom rests on, not an oversight. Bigtable's private
  `RecordingSinkWriterMetricGroup` predated it and **was deleted by #237**, which brought that sink
  up to the series' standard: assert-by-registered-name is what its gauges and `errorClass`
  subgroups needed, and a counter-holding stub reaches neither. So this is the one sink
  metric-group harness in the tree, with no second pattern beside it.
- **An assertion names the metric with a string literal, never with the constant the class under
  test declares** (#280). The harnesses' whole claim is that a renamed metric fails its test, and a
  constant reference cannot deliver it: a `static final String` is **inlined into the test class at
  compile time**, so `counter(XMetrics.TABLES_CREATED)` compares the constant against itself and
  passes for any value, typos included. Measured — mutating `BigtableWriterMetrics.RECORDS_SKIPPED`'s
  value left `BigtableWriterMetricsTest` green, while the same mutant killed
  `PubSubWriterFailureHandlerTest`, which spells the name out. Fifteen production metric names were
  unpinned that way when the rule was written (thirteen in BigQuery, plus Bigtable's
  `recordsSkipped` and the source's `messagesReceived`, which no test named at all); all are
  literals now. The class-side constants stay — they name the registration site — but the test side
  spells the name, which also makes a test read like the docs table it corresponds to. The same
  inlining is a build trap worth knowing: a mutant run bakes the mutated string into `target/`, so a
  `just verify` without `clean` then fails on a string that is no longer anywhere in the tree.
- **`TestSinkCommitterMetricGroup` is its committer sibling** (#210, which #208 deferred it to for
  want of a consumer): the same `ProxyMetricGroup`-over-`MetricListener` shape for
  `SinkCommitterMetricGroup`, and the one type here admitted with a **single** consumer — the
  FILE_LOADS committer's `loadJobsSubmitted` is the only custom committer metric in the repository,
  so the multiple-consumer bar would keep it out forever, and the alternative is a private
  module-local copy of exactly what #237 then deleted on the Bigtable side. It registers the
  framework's five committer counters under the names a **reporter** sees — `totalCommittables`,
  `successfulCommittables`, `alreadyCommittedCommittables`, `failedCommittables`,
  `retriedCommittables`, read from `MetricNames` in flink-runtime 2.2.1 — which are *not* the
  `getNumCommittables*Counter` accessor names on the interface. #210's issue text called them
  `numCommittables*`, and a docs page written from that would have named metrics no reporter emits.
  The pending-committables gauge is captured rather than registered, as the writer harness captures
  `currentSendTime`.
- **Real-GCP gating annotations never move here.** `scripts/e2e-gated-its.sh` discovers the gated
  suite by grepping the `@EnabledIfEnvironmentVariable` literal on concrete classes under the
  connector modules and expects a surefire report per match — a meta-annotation or a base class in
  this module would make that grep silently return nothing. The same holds for the `@Tag("gated")`
  each of those classes carries beside it (#245): the pair is what makes the suite opt-in per
  command, `--check-tags` greps both literals, and hoisting either into a shared annotation here
  would defeat the check rather than tidy it.
- **The justfile install lists name this module.** The `binary-compat` and `e2e` recipes run
  goal-only / `-pl`-scoped Maven, which cannot resolve a reactor sibling from source (#181), so
  both install this module into `~/.m2` first. A rename or a new similarly-consumed module must
  update those recipes.
- **The module has tests since #244, and the bar for adding one is narrow**: a helper here is
  normally exercised by the sibling that consumes it, so a test in this module earns its place
  only when the consumer *cannot* reach the behaviour. `AwaitsTest` is the first — `Awaits`'s
  diagnosis runs only after an await has already timed out, so no green build executes it and a
  broken diagnosis would first be discovered by the CI failure it exists to explain. Anything
  covered incidentally by a consumer's ITs stays uncovered here. Consequences of the module now
  producing surefire reports: `scripts/surefire-fingerprint.sh` picks them up (its `find` had
  simply matched nothing before), so `binary-compat`'s same-tests diff covers them automatically.
  `e2e-gated-its.sh --assert-ran` still ignores the module, and for an unrelated reason — it has
  no gated class for the annotation grep to find; a gated class *without* a report is fatal there,
  not ignored.
- No compat source roots (`src/main/java-flink1`/`java-flink2`): nothing here implements `Sink`
  across the 1.x/2.x API gap. Adding a sink test-double that does would need the seam — prefer
  keeping such doubles in the module that needs them.
