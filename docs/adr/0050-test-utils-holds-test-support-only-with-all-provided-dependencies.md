<!--
Copyright 2026 laughingman7743

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

# ADR-0050: test-utils holds test-support only, with all-`provided` dependencies and no forced fixture unification

- Status: Accepted
- Date: 2026-08-01 ([#27]); `testutils.sql` 2026-08-07 ([#290])
- Issues: [#27], [#290], [#26], [#181]
- Modules: test-utils
- Current behavior: (Claude-facing module; nothing user-rendered)

## Decision

- **Test-support code only, forever.** Production code shared across connectors — retry ([#61]), DLQ/metrics ([#37]) — goes to
  `flink-connector-gcp-base`. Utilities live in `src/main/java` here only so siblings can
  consume the plain jar at `test` scope (the `flink-test-utils` shape).
- **Every dependency is `provided`.** Consumers' test classpaths already carry what they
  exercise, and `provided` is non-transitive, so depending on this module adds no new resolution
  path to any artifact — which is what keeps the SQL uber-jars' shade-scope mediation
  undisturbed (ADR-0015's test-scope demotion trap).
- **No forced unification of emulator container fixtures.** The goccy BigQuery and aertje Cloud
  Tasks fixtures are hand-rolled, single-consumer, structurally unlike the testcontainers
  `PubSubEmulatorContainer`; they stay in their modules. Only what has multiple consumers moves
  here — and [#290] is what that rule looks like when it fires: the BigQuery SQL smoke test
  needed the goccy container, so **its container half moved and nothing else did**
  (`testutils.bigquery.BigQueryEmulatorContainers` owns image/ports/wait strategy/stock REST
  client; `AbstractBigQueryEmulatorITCase` keeps the halves naming connector types). Not
  tidiness: the SQL module runs its tests against the *relocated* connector, so a helper naming
  a connector type could not compile there at all.
- **`testutils.sql` holds the shaded-module test bases** ([#290], discharging the [#26]
  trigger): `ShadedJar` plus three abstract JUnit bases each `flink-sql-connector-gcp-*` extends.
  **Abstract bases rather than assertion-free helpers**, decided with the user: every assertion
  and message in one place, at the price of `junit-jupiter`/`assertj-core` in this pom
  (`provided` like everything else). Three things the extraction changed because the second
  consumer needed it: the artifact-count floor is **per module**; the relocated
  `ManagedChannelProvider` SPI name is **derived from the shaded prefix**; and the
  unrelocated-package allow-list is split into shared and per-module halves — **the intersection,
  not the union, and the difference was a real hole** (a union carried `org/checkerframework/`,
  which only the BigQuery tree has, so a BOM bump bringing checker-qual into the Pub/Sub tree
  would have shipped it unrelocated with the packaging test green;
  `everyExemptionOnTheAllowListIsInTheJar` is the second direction that makes the intersection a
  property). `ShadedJar.of` rejects a prefix containing `_`, which was a pom comment enforced by
  nothing.
- **Real-GCP gating annotations never move here.** `scripts/e2e-gated-its.sh` discovers the
  gated suite by grepping the `@EnabledIfEnvironmentVariable` literal on concrete classes under
  the connector modules — a meta-annotation or base class here would make that grep silently
  return nothing; the same holds for `@Tag("gated")` ([#245] — `--check-tags` greps both
  literals).
- **The justfile install lists name this module** — `binary-compat` and `e2e` run goal-only /
  `-pl`-scoped Maven, which cannot resolve a reactor sibling from source ([#181]), so both
  install it into `~/.m2` first. A rename or a new similarly-consumed module must update those
  recipes.
- **The bar for a test in this module is narrow** ([#244] added the first): a helper is normally
  exercised by its consumer, so a test here earns its place only when the consumer *cannot*
  reach the behaviour (`AwaitsTest` — diagnosis code that runs only after a timeout;
  `LogCaptureTest` — failure modes indistinguishable from "the log was never emitted" in a
  consumer).
- No compat source roots: nothing here implements `Sink` across the 1.x/2.x gap; a sink
  test-double that does belongs in the module that needs it.

[#26]: https://github.com/laughingman7743/flink-connector-gcp/issues/26
[#37]: https://github.com/laughingman7743/flink-connector-gcp/issues/37
[#61]: https://github.com/laughingman7743/flink-connector-gcp/issues/61
[#27]: https://github.com/laughingman7743/flink-connector-gcp/issues/27
[#181]: https://github.com/laughingman7743/flink-connector-gcp/issues/181
[#244]: https://github.com/laughingman7743/flink-connector-gcp/issues/244
[#245]: https://github.com/laughingman7743/flink-connector-gcp/issues/245
[#290]: https://github.com/laughingman7743/flink-connector-gcp/issues/290
