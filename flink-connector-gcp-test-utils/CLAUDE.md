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
  consumers moves here — and #290 is what that rule looks like when it fires. The BigQuery SQL
  uber-jar's smoke test needed the goccy container, so **its container half moved and nothing else
  did**: `testutils.bigquery.BigQueryEmulatorContainers` owns the image tag, the two ports, the
  wait strategy and a stock REST client, while `AbstractBigQueryEmulatorITCase` keeps `createTable`
  and `queryNames`, which take connector types. That line is not tidiness — the SQL module runs its
  tests against the *relocated* connector, so a helper naming a connector type could not compile
  there at all, and the shared half deals only in stock `com.google.*` and testcontainers types,
  the same constraint `PubSubTestClients` was built under. The Cloud Tasks fixture still has one
  consumer and still stays where it is.
- **`testutils.sql` holds the shaded-module test bases** (#290, discharging the #26 trigger — the
  trigger called them a trio; the extraction landed as four):
  `ShadedJar` plus the abstract `AbstractBundledDependenciesNoticeTest` and
  `AbstractSqlConnectorPackagingITCase` and `AbstractSqlConnectorSmokeITCase`, which each
  `flink-sql-connector-gcp-*` extends with thin concrete subclasses. **Abstract JUnit bases rather than assertion-free helpers**, decided
  with the user: it puts every assertion and every message in one place, at the price of
  `junit-jupiter` and `assertj-core` in this pom — the only two here that appear in no helper's
  signature and exist purely so the bases compile, and `provided` like everything else for the
  reason above. Naming an abstract base `Abstract*` is what keeps surefire from trying to run
  it; being in `src/main/java` of a different module is what keeps it off the consumer's test-scan
  entirely.
  Three things the extraction changed rather than moved, each because the second consumer needed
  it. The artifact-count floor is **per module** (a shared 40 is vacuous against a 111-artifact
  tree), and lives on that module's `UberJar` holder because two unrelated bases ask for it. The
  relocated `ManagedChannelProvider` SPI name is **derived from the shaded prefix**, as the netty
  native-library name already was, so config and assertion cannot drift. And the
  unrelocated-package allow-list is split into the base's shared half and the subclass's own.
  **That split is the intersection, not the union, and the difference was a real hole**: written as
  a union it carried `org/checkerframework/`, which only the BigQuery tree has, so a
  `libraries-bom` bump bringing checker-qual into the Pub/Sub tree would have shipped it
  unrelocated with the packaging test green. An allow-list entry only ever *permits*, so a vacuous
  one is invisible where the escapes are checked —
  `everyExemptionOnTheAllowListIsInTheJar` is the second direction, and it is what makes the
  intersection a property rather than a discipline.
  The smoke tests share `AbstractSqlConnectorSmokeITCase`, whose one test is the precondition the
  rest of each smoke class rests on: that the connector came out of the uber-jar. One assertion is
  worth a base class exactly when its failure makes every other assertion in the subclass
  meaningless-but-green.
  **`ShadedJar.of` rejects a prefix containing `_`**, which was a pom comment in both modules and
  enforced by nothing: netty's `calculateMangledPackagePrefix()` would require it spelled `_1` in
  the `META-INF/native` relocations and in every assertion derived from them.
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
- **`LogCapture` is the shared log-assertion harness** (#323), and it is **deliberately narrow: 7
  of the repository's 24 `LOG.warn`/`LOG.error` sites**, in three modules. The bar is that the log
  is the report — remove the assertion and the branch has nothing identifying the event left.
  Today: `FailureHandlers.LogAndDrop`, whose whole behaviour is the log; the Bigtable batcher's
  absorbed shutdown report (#238); `BoundedShutdown`'s two warnings, one with no observable at all
  and one whose `abandonedCount` says a teardown was abandoned but not which client (#265/#312);
  the two FILE_LOADS quota warnings, which the job builds straight through; and
  `LoadJobOrchestrator`'s live-schema warning, which was already log-asserted before #323 and was
  merely migrated onto the helper.
  - **The bar exists because the cost is real.** An assertion here couples a test to the *wording*
    of a message, so rewording a log line — a harmless, desirable thing — breaks it. Measured
    2026-08-06 over all 24 sites: 13 were already driven by an existing test, and for 9 of those
    the same test already asserts a counter, a returned value or an absorbed exception that
    identifies the event. Asserting the log there was tried and **reverted**: it doubled the
    coverage of nothing and left ten tests pinned to prose. Do not re-add them. What the sites
    above have in common is that no such assertion exists to fall back on.
  - Three modules still beats a per-case capture, which would put the four log4j2 traps below into
    three copies — the issue's own test for "the helper is the wrong shape" was two or three
    *sites*, and this is past it.
  - **The other 17 sites are unasserted on purpose, and nothing tracks them as a gap.** Five would
    need an injection point opened in production code to be reachable at all; #336 proposed exactly
    that and was **closed**, because changing a shipped class's structure to reach a log line is a
    larger version of the cost the nine reverted assertions were already judged not to be worth.
    #337 is open over one of the remaining sites' classes, but for an unrelated reason —
    `BigQueryLoadJobRunner` has no unit test of its own — so a log assertion there would be
    incidental and still has to clear the bar above.
  - **The backend is log4j2, and both mechanisms #323 proposed are unavailable.** `log4j-slf4j-impl`
    2.24.3 reaches every module transitively through `flink-test-utils`; logback is absent, so its
    `ListAppender` cannot apply, and an slf4j-level capture would mean swapping the binding. log4j's
    own `ListAppender` ships only in a `log4j-core` test-jar this build does not resolve, so the
    appender is hand-rolled. No log4j2 type appears in `LogCapture`'s signature, so a backend change
    is one file.
  - **`log4j-core` is deliberately not declared in the pom**, though `LogCapture` compiles against
    it. Nothing manages a log4j version here or in `flink-connector-parent`, so declaring it means
    pinning one by hand — and a pin that drifts from what Flink puts on the runtime classpath fails
    at runtime, where losing the transitive fails at compile time on the next build. The pom comment
    on `flink-test-utils` records the chain.
  - Four log4j2 mechanics the implementation works around, each of which would otherwise make a
    capture collect nothing while looking like a log that was never emitted. The javadoc carries
    them; the short form: the logger name is derived as **slf4j** derives it (`Class#getName`) and
    not as `LogManager.getLogger(Class)` does (`getCanonicalName`) — they differ for a nested class
    such as `LogAndDrop` and are not even in an ancestor relationship; the level is forced on the
    **`LoggerConfig`**, not the `Logger`, since any later `updateLoggers()` discards the latter; the
    level is only ever **widened**; and the appender name is unique per instance, because
    `LoggerConfig.removeAppender(name)` removes *every* control with that name.
  - **A test asserting a log was not emitted must sit beside one asserting a log was**, on the same
    logger. An empty capture is the expected result of both a working capture and a broken one, and
    only the positive case tells them apart.
  - **`LogCaptureTest` is discriminating by construction, and the module having no `log4j2` config
    is not what makes it so.** Every module now ships `log4j2-test.properties` (`rootLogger.level =
    WARN`, `logger.gcp.level = INFO`) — this one included, added by #323 because `base`, `bigtable`
    and `cloudtasks` had none and log4j2 fell back to `ERROR`, the #244 failure shape. So a WARN
    passes everywhere with no forcing at all, and the level tests capture at **DEBUG**, which the
    ambient config filters. `theAmbientConfigurationIsWhatTheseTestsAssume` asserts that
    precondition rather than assuming it, so changing either properties file fails there instead of
    quietly disarming the suite.
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
  covered incidentally by a consumer's ITs stays uncovered here. `LogCaptureTest` is the second
  and clears the same bar: every failure mode it pins is "the capture silently saw nothing", which
  in a consumer is indistinguishable from the log not being emitted — the assertion fails either
  way and names the wrong culprit. Consequences of the module now
  producing surefire reports: `scripts/surefire-fingerprint.sh` picks them up (its `find` had
  simply matched nothing before), so `binary-compat`'s same-tests diff covers them automatically.
  `e2e-gated-its.sh --assert-ran` still ignores the module, and for an unrelated reason — it has
  no gated class for the annotation grep to find; a gated class *without* a report is fatal there,
  not ignored.
- No compat source roots (`src/main/java-flink1`/`java-flink2`): nothing here implements `Sink`
  across the 1.x/2.x API gap. Adding a sink test-double that does would need the seam — prefer
  keeping such doubles in the module that needs them.
