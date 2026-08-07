# CLAUDE.md — flink-connector-gcp-test-utils

Rules for the shared test-utils module (#27). Read before adding anything here; each decision's
record — context, evidence, declined alternatives — is the named ADR under `docs/adr/`.

## Scope (`docs/adr/0050`)

- **Test-support code only, forever** — main-code sharing goes to `flink-connector-gcp-base`.
  **Every dependency is `provided`** (non-transitive, which is what keeps the SQL uber-jars'
  shade-scope mediation undisturbed). A type moves in only once it has multiple consumers; no
  forced unification of emulator container fixtures. When the rule fires, move only the half
  the second consumer can use — a helper naming a connector type cannot compile in a SQL
  module's relocated tests.
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
- **An assertion names a metric with a string literal, never the class's constant** — constants
  inline at compile time, so a constant-referencing assertion passes for any value. The same
  inlining means a mutant run leaves poisoned classes in `target/`; `clean` before believing a
  later run.
- `LogCapture` assertions clear a narrow bar: the log is the report, nothing else identifies the
  event. The other 17 warn/error sites are unasserted **on purpose**; do not re-add the nine
  reverted assertions. A not-emitted assertion sits beside an emitted one on the same logger.
  Every module ships `log4j2-test.properties`; the four log4j2 traps are in `LogCapture`'s
  javadoc, and `log4j-core` stays undeclared in the pom.
