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
  `RecordingSinkWriterMetricGroup` predates it and is **superseded**: #237 deletes it when it brings
  that sink up to the series' standard, so until then it is a leftover, not a second pattern to
  copy. A **committer sibling was deliberately not added**: #208 has no consumer for one, and it
  arrives with #210's FILE_LOADS committer counter under the multiple-consumer bar everything else
  here clears.
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
- This module has no tests of its own: it produces no surefire reports, which
  `scripts/surefire-fingerprint.sh` correctly ignores (its `find` simply matches nothing here).
  `e2e-gated-its.sh --assert-ran` ignores the module for a different reason — it has no gated
  class for the annotation grep to find; a gated class *without* a report is fatal there, not
  ignored. If the module ever gains tests, `binary-compat`'s same-tests diff picks them up
  automatically.
- No compat source roots (`src/main/java-flink1`/`java-flink2`): nothing here implements `Sink`
  across the 1.x/2.x API gap. Adding a sink test-double that does would need the seam — prefer
  keeping such doubles in the module that needs them.
