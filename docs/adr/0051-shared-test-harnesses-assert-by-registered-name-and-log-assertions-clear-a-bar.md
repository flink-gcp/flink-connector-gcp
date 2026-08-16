<!--
Copyright 2026 The flink-gcp authors

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

# ADR-0051: Shared test harnesses assert by registered name, and log assertions clear a narrow bar

- Status: Accepted
- Date: 2026-08-02 ([#206] `StubWriterInitContext`) through 2026-08-06 ([#323] `LogCapture`)
- Issues: [#206], [#208], [#210], [#280], [#323]
- Modules: test-utils (consumed by every module's tests)

## Decision

- **`StubWriterInitContext` answers what a sink reads and throws for everything else** — the
  unsupported methods are the point: a sink growing a new dependency on the context shows up as
  a failing test rather than a silent null (it arrived with [#205] in the BigQuery test tree and moved when Pub/Sub needed it). The metric group is held in a **field** (identity
  assertions), and the mailbox is a real `FakeMailboxExecutor`. The field was a null-returning
  `Proxy` until [#208]; the replacement was forced — once a writer captures counters in its
  constructor, the proxy and the metrics half could not coexist.
- **`TestSinkWriterMetricGroup` is the one sink metric-group harness** ([#208]): a
  `ProxyMetricGroup` over a `MetricListener` group with the FLIP-33 standard counters registered
  under their documented names. Everything is asserted **by registered name**, so a renamed or
  unregistered metric fails its test — the two obvious alternatives are unusable
  (`UnregisteredMetricsGroup` hands out a fresh `SimpleCounter` per call;
  `InternalSinkWriterMetricGroup` has no `mock(...)` in either supported Flink line, measured on
  1.20.4 and 2.2.1). It brings `flink-test-utils` at `provided`, so **a module using the harness
  declares `flink-test-utils` at test scope itself** — non-transitivity is the property this pom
  rests on. Bigtable's private `RecordingSinkWriterMetricGroup` predated it and was deleted by
  [#237].
- **An assertion names the metric with a string literal, never the constant the class under test
  declares** ([#280]): a `static final String` is **inlined into the test class at compile
  time**, so `counter(XMetrics.TABLES_CREATED)` compares the constant against itself and passes
  for any value. Measured — mutating a constant's value left the constant-referencing test green
  while killing the literal-spelling one; fifteen production names were unpinned that way when
  the rule was written. The same inlining is a build trap: a mutant run bakes the mutated string
  into `target/`, so a `just verify` without `clean` then fails on a string no longer in the
  tree.
- **`TestSinkCommitterMetricGroup`** ([#210]) is the committer sibling, admitted with a
  **single** consumer (the FILE_LOADS committer's `loadJobsSubmitted` is the repository's only
  custom committer metric). It registers the framework's five committer counters under the names
  a **reporter** sees (`totalCommittables` &c., read from `MetricNames` in flink-runtime 2.2.1)
  — *not* the `getNumCommittables*Counter` accessor names; [#210]'s issue text called them
  `numCommittables*`, and a docs page written from that would have named metrics no reporter
  emits.
- **`LogCapture` is the shared log-assertion harness** ([#323]), **deliberately narrow: 9 asserted
  sites, against 59 `warn`/`error` sites in the main trees (recounted 2026-08-16; 25 when this was
  written, so the ratio has widened rather than the bar having moved)**. The bar: the log is the report — remove the
  assertion and the branch has nothing identifying the event left. Today's sites: `FailureHandlers.LogAndDrop` (the log is the whole behaviour), the Bigtable batcher's absorbed shutdown report ([#238]), `BoundedShutdown`'s three warnings ([#265]/[#312], and the failure a shutdown reports after `close()` has already given up — the ninth site, added by [#726], where nothing else survives to name the failure at all), the two FILE_LOADS quota warnings, `LoadJobOrchestrator`'s live-schema warning, and the Pub/Sub sink's stalled-wait warning ([#333], ADR-0052 — a publish that never answers is never counted as a failure, so no counter names the state). The bar exists because the
  cost is real: an assertion couples a test to the *wording* of a message. Measured 2026-08-06
  over the 24 sites that existed then: 13 were already driven by an existing test, and for 9 of those the same
  test already asserts a counter, a returned value or an absorbed exception — asserting the log
  there was tried and **reverted**; do not re-add them. **The remaining sites are unasserted on
  purpose, and nothing tracks them as a gap** — a count is not carried here, because it goes stale
  every time a connector logs ([#336] proposed opening injection points to reach
  five of them and was closed; [#337] gave `BigQueryLoadJobRunner` — the class holding two of them — its first unit test and **added no log assertion**, which is the bar applied rather than skipped: its swallowed temporary-table delete is identified by the test that scripts the failure and sees nothing escape, and its probe warning by the sequence of ids the test reads back). The backend is log4j2 and both mechanisms [#323] proposed are
  unavailable (logback absent; log4j's `ListAppender` ships only in a test-jar this build does
  not resolve), so the appender is hand-rolled; `log4j-core` is deliberately not declared in the
  pom (nothing manages a log4j version, and a hand pin that drifts from Flink's runtime
  classpath fails at runtime). Four log4j2 mechanics the implementation works around are in the
  javadoc (slf4j-style logger-name derivation, level forced on the `LoggerConfig`, level only
  widened, per-instance appender names). **A test asserting a log was not emitted must sit
  beside one asserting a log was**, on the same logger — an empty capture is the expected result
  of both a working capture and a broken one. Every module ships `log4j2-test.properties`
  (added by [#323]; without one log4j2 falls back to `ERROR`, the [#244] failure shape), and
  `theAmbientConfigurationIsWhatTheseTestsAssume` asserts that precondition.

[#205]: https://github.com/laughingman7743/flink-connector-gcp/issues/205
[#206]: https://github.com/laughingman7743/flink-connector-gcp/issues/206
[#208]: https://github.com/laughingman7743/flink-connector-gcp/issues/208
[#210]: https://github.com/laughingman7743/flink-connector-gcp/issues/210
[#237]: https://github.com/laughingman7743/flink-connector-gcp/issues/237
[#238]: https://github.com/laughingman7743/flink-connector-gcp/issues/238
[#244]: https://github.com/laughingman7743/flink-connector-gcp/issues/244
[#265]: https://github.com/laughingman7743/flink-connector-gcp/issues/265
[#280]: https://github.com/laughingman7743/flink-connector-gcp/issues/280
[#312]: https://github.com/laughingman7743/flink-connector-gcp/issues/312
[#323]: https://github.com/laughingman7743/flink-connector-gcp/issues/323
[#333]: https://github.com/laughingman7743/flink-connector-gcp/issues/333
[#336]: https://github.com/laughingman7743/flink-connector-gcp/issues/336
[#337]: https://github.com/laughingman7743/flink-connector-gcp/issues/337
