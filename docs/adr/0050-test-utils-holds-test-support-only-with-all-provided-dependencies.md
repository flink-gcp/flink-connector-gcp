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

# ADR-0050: test-utils holds test-support only, with all-`provided` dependencies and no forced fixture unification

- Status: Accepted
- Date: 2026-08-01 ([#27]); `testutils.sql` 2026-08-07 ([#290]); the source-reader outputs
  2026-08-09 and the pull-assignment context fakes 2026-08-10 ([#437]); the Cloud Tasks
  emulator fixture 2026-08-17 ([#776])
- Issues: [#27], [#290], [#26], [#181], [#437], [#776]
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
  Tasks fixtures were hand-rolled and single-consumer when this was decided, structurally unlike
  the testcontainers `PubSubEmulatorContainer`; a fixture stays in its module until a second
  consumer exists. Only what has multiple consumers moves here — and [#290] is what that rule
  looks like when it fires: the BigQuery SQL smoke test needed the goccy container, so **its
  container half moved and nothing else did**
  (`testutils.bigquery.BigQueryEmulatorContainers` owns image/ports/wait strategy/stock REST
  client; `AbstractBigQueryEmulatorITCase` keeps the halves naming connector types). Not
  tidiness: the SQL module runs its tests against the *relocated* connector, so a helper naming
  a connector type could not compile there at all. The aertje fixture followed the same path on
  2026-08-17 ([#776]): the Cloud Tasks SQL uber-jar's smoke test had become its second consumer
  (with the container declaration and the plaintext stock-client construction duplicated —
  identical code, the connector copy alone carrying the explanatory comment), so both halves moved — `testutils.cloudtasks.CloudTasksEmulatorContainers` and
  `CloudTasksTestClients`, neither naming a connector type. A refinement, not a reversal: the
  rule is unchanged, its "single-consumer" premise for that fixture had simply been overtaken.
- **The second firing of that rule is the source-reader outputs** ([#437]): the BigQuery source
  ([#390]) wrote its own `CollectingSourceOutput` and `CollectingReaderOutput` because
  `flink-connector-base`'s test jar is a dependency of no module here, which made the Pub/Sub
  source's the first copy and BigQuery's the second, so both moved to `testutils`. The Bigtable
  scan source ([#216]) landed a third `CollectingSourceOutput` while this move was in flight,
  differing from the Pub/Sub file only in its package line and its visibility; it is deleted here
  too, so `CollectingSourceOutput` arrives with three consumers and `CollectingReaderOutput` with
  two. A follow-up then moved the source-side context fakes on the same rule, once a measurement
  showed what their divergence actually tracks. Three decisions came out of it.
  - **The context fakes split by assignment direction, not by connector.**
    `FakeSourceReaderContext.sendSplitRequest()` throws for Pub/Sub, whose source never requests a
    split, and records for BigQuery and Bigtable, whose whole assignment protocol *is* the request;
    `FakeSplitEnumeratorContext` records the order of assignments against no-more-splits signals
    only where a pull enumerator's correctness is a sequence. **So the rule is "the push-assigned
    source keeps its own", not "every source keeps its own"**: the two pull-assigned sources' fakes
    were measured as differing by the split type, a metric group and their log strings — which is
    not a divergence at all — so they became one generic `FakeSplitEnumeratorContext<SplitT extends
    SourceSplit>` and one `FakeSourceReaderContext` in `testutils`, while Pub/Sub's two stayed.
    That the shared pair *cannot* serve a push-assigned source is the point rather than a
    limitation: its throwing methods are what would fail a Pub/Sub test that started requesting
    splits. The shared reader context **takes** its metric group rather than building one, which
    keeps the unannotated flink-runtime `InternalSourceReaderMetricGroup` out of this module's main
    sources and so out of the Flink API tier audit. `TestReaderMetrics` sits in the same packages and is a near-copy as
    well, and cannot move at all: each names its own connector's `*SourceReaderMetrics`, and those
    are unrelated `public final` classes with no supertype to name instead — the "a helper naming a
    connector type" case the [#290] bullet above ends on.
  - **Where two copies' contracts disagree, the move takes the richer one and rewrites the
    minority's assertions.** The two `timestamps()` were incompatible, not a subset relation as
    [#437] assumed: Pub/Sub's padded a record emitted without a timestamp as `null`, BigQuery's
    omitted it entirely, and only Pub/Sub's had `failOnCollect`. The padded contract won on three
    consumers to one — Pub/Sub's, Bigtable's and, once its assertion was rewritten, BigQuery's — so
    `BigQueryRecordEmitterTest.emitsWithoutATimestamp` moved from
    `assertThat(output.timestamps()).isEmpty()` to `containsExactly((Long) null)`. The rewrite is
    not a concession: the emptiness assertion also passed when nothing had been emitted at all.
  - **A moved test double becomes `@Internal public final`**, since all five copies were
    package-private and a consumer now reaches them across a module boundary.
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
[#27]: https://github.com/laughingman7743/flink-connector-gcp/issues/27
[#37]: https://github.com/laughingman7743/flink-connector-gcp/issues/37
[#61]: https://github.com/laughingman7743/flink-connector-gcp/issues/61
[#181]: https://github.com/laughingman7743/flink-connector-gcp/issues/181
[#216]: https://github.com/laughingman7743/flink-connector-gcp/issues/216
[#244]: https://github.com/laughingman7743/flink-connector-gcp/issues/244
[#245]: https://github.com/laughingman7743/flink-connector-gcp/issues/245
[#290]: https://github.com/laughingman7743/flink-connector-gcp/issues/290
[#390]: https://github.com/laughingman7743/flink-connector-gcp/issues/390
[#437]: https://github.com/laughingman7743/flink-connector-gcp/issues/437
[#776]: https://github.com/flink-gcp/flink-connector-gcp/issues/776
