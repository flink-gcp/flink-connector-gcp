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
- **Real-GCP gating annotations never move here.** `scripts/e2e-gated-its.sh` discovers the gated
  suite by grepping the `@EnabledIfEnvironmentVariable` literal on concrete classes under the
  connector modules and expects a surefire report per match — a meta-annotation or a base class in
  this module would make that grep silently return nothing.
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
