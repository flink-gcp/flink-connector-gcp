# Detailed guidance — flink-connector-gcp-test-utils

Rules for the shared test-utils module (#27). Read before adding anything here; each decision's
record — context, evidence, declined alternatives — is the named ADR under `docs/adr/`.

## Scope (`docs/adr/0050`)

- **Test-support code only, forever** — main-code sharing goes to `flink-connector-gcp-base`.
  **Every dependency is `provided`** (non-transitive, which is what keeps the SQL uber-jars'
  shade-scope mediation undisturbed). A type moves in only once it has multiple consumers; no
  forced unification of emulator container fixtures. When the rule fires, move only the half
  the second consumer can use — a helper naming a connector type cannot compile in a SQL
  module's relocated tests, and `TestReaderMetrics` is that case in every source module. **Where the
  two copies' contracts disagree, take the richer one and rewrite the minority's assertions**
  (#437); a moved double becomes `@Internal public final`.
- `testutils.sql` holds `ShadedJar` and the three abstract SQL test bases; the
  unrelocated-package allow-list split is the **intersection**, never the union.
- **Real-GCP gating annotations never move here** — `scripts/e2e-gated-its.sh` greps the
  literals on concrete classes; a meta-annotation or shared base would silently defeat it.
- The justfile `binary-compat`/`e2e` install lists name this module; a rename must update them.
- **The bar for a test in this module is narrow**: only behaviour a consumer *cannot* reach
  (`AwaitsTest`, `LogCaptureTest`). Anything covered incidentally by a consumer's ITs stays
  uncovered here.
- No compat source roots; a cross-major sink test-double belongs in the module that needs it.

## Harnesses (`docs/adr/0051`)

- `StubWriterInitContext` answers what a sink reads and throws for everything else — the throws
  are the point. `TestSinkWriterMetricGroup` / `TestSinkCommitterMetricGroup` are the only
  metric-group harnesses; everything asserts **by registered name**, and the committer names are
  the reporter's (`totalCommittables` …), not the accessor names.
- `CollectingSourceOutput` is what a `RecordEmitter` is handed, `CollectingReaderOutput` what
  `pollNext` takes; the second wraps the first and hands every split the same output. Their
  `timestamps()` is **padded, not sparse** — a record emitted without one appears as `null`, so an
  assertion tells "one record, no timestamp" from "no record", which an `isEmpty()` cannot. The
  source-side context fakes split by **assignment direction**: `FakeSplitEnumeratorContext<SplitT>`
  and `FakeSourceReaderContext` here serve the *pull*-assigned sources, and Pub/Sub keeps its own
  because it is push-assigned and its coordinator-facing methods throw. That the shared pair cannot
  serve a push-assigned source is the point (`docs/adr/0050`). The reader context **takes** a metric
  group rather than building one — building it would put flink-runtime's unannotated
  `InternalSourceReaderMetricGroup` into this module's tier audit.
- **An assertion names a metric with a string literal, never the class's constant** — constants
  inline at compile time, so a constant-referencing assertion passes for any value. The same
  inlining means a mutant run leaves poisoned classes in `target/`; `clean` before believing a
  later run.
- `LogCapture` assertions clear a narrow bar: the log is the report, nothing else identifies the
  event — `docs/adr/0051` enumerates the sites that clear it, and a new one joins that list rather
  than being counted silently. Every other warn/error site is unasserted **on purpose**; do not
  re-add the nine reverted assertions. A not-emitted assertion sits beside an emitted one on the same logger.
  Every module ships `log4j2-test.properties`; the four log4j2 traps are in `LogCapture`'s
  javadoc, and `log4j-core` stays undeclared in the pom.
