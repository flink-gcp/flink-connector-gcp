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

# ADR-0137: A cross-connector name diverges only to name a real difference

- Status: Accepted
- Date: 2026-08-23; retry-counter wording revised by [#1051] (2026-08-23)
- Issues: [#1043], [#782], [#1051]
- Modules: all connectors
- Current behavior: the rules in this record, with the judged divergence table appended below
  as evidence; the drift half is routed as sub-issues [#1047]–[#1053]

## Context

The [#1043] review built a concept-to-name table across the five connectors on three surfaces —
class names, option keys and public builder setters, metric names — and judged every divergence
as drift to unify or a deliberate difference that stays. The renames are sub-issues of the
module audits ([#1047]–[#1053]). This ADR is the other half: the divergences that survived
judgment, and the rules that make them defensible, recorded once so the next connector and the
next reviewer inherit them instead of re-litigating.

The standing cross-connector normative text was thin. [ADR-0038] fixes the metric-name *shape*
per module and names "diff the inventories" as the whole cross-module mechanism; the
`add-a-connector-option` skill tells an option key to "match a sibling connector's spelling for
the same concept" and points at [#782] as the review that holds it; class names had no
cross-connector vocabulary record beyond [ADR-0055]'s placement and SPI-implementation rules.
The review found the vocabulary largely coherent — and the coherence follows a small number of
previously unstated rules.

## Decision

**A name is shared across connectors when the concept is shared.** `recordsSkipped`,
`emulator-endpoint`, `destinationResolver`, `startPosition`, the whole change-stream lag metric
family, the `recovery*` triple (whose two `retry*` stragglers are renamed by [#1051] and
[#1053]) — the fleet spells shared concepts one way, and any new name for an existing concept
starts from the sibling spelling. Where a name below *diverges*, the divergence names something
real; a divergence that names nothing is drift and gets renamed.

**At an SDK-owned seam, the vendor's word wins; on a connector-owned mechanism, the house
idiom wins.** This is [ADR-0028]'s `retry*` (gax `RetrySettings`) versus `recovery*`
(connector-owned budget) rule, and the review found it generalizes:

- *Batching keys.* An SDK-owned batcher takes `sink.batching.*` with the SDK's own leaf
  vocabulary — Pub/Sub's `element-count-threshold`, `request-byte-threshold`,
  `delay-threshold` are gax `BatchingSettings`' field names, the same move Flink's
  Elasticsearch connector makes with its client's word (`sink.bulk-flush.*`). A
  connector-owned buffer takes Flink's own idiom — Spanner's `sink.buffer-flush.max-*` is the
  JDBC/HBase shape, correctly, because the Spanner writer accumulates its own buffer (its
  `max-commit-delay` leaf is the Spanner API's own `maxCommitDelay`, a commit-time option the
  vendor applies server-side — the vendor word riding the house prefix).
  BigQuery's `max-append-request-bytes` names the `AppendRows` request limit. The one deviation
  (Bigtable fronting the same gax fields under renamed leaves) is routed to [#1052], not
  defended.
- *In-flight and flow-control bounds.* A connector-owned ledger says `inFlight`
  (`maxInFlightMessages`, `maxInFlightEntries`, `maxInFlightTasks` — the writer's own
  accounting; Pub/Sub's sink deliberately leaves the SDK flow controller unexposed). An
  SDK-owned bound takes the SDK's own spelling: BigQuery's
  `maxInflightRequests`/`maxInflightBytes` front `StreamWriter`'s
  `setMaxInflightRequests`/`setMaxInflightBytes` — the lowercase `f` is the vendor's casing,
  carried deliberately, not drift — and its `maxRetryDuration` is
  `StreamWriter.Builder.setMaxRetryDuration`'s own name, a different vendor knob (with a
  different `0` meaning) than Pub/Sub's gax-backed `retryTotalTimeout`, not one field spelled
  twice — and gax flow control keeps gax's word where it is fronted,
  the source's `scan.flow-control.max-outstanding-element-count`/`-request-bytes`
  (`DefaultSubscriberFactory` passes them to `FlowControlSettings`). The dead-letter queue's
  `maxOutstandingMessages`/`outstandingDeadLetters` were measured against this rule and failed
  it — the DLQ bounds its own list of unawaited publish futures, and no `FlowControlSettings`
  exists on the sink side — so they are routed to [#1050] as drift, not defended here.
- *Bounded-read class vocabulary.* BigQuery reads **read streams** ([ADR-0079]), Bigtable
  **scans** the `ReadRows` family ([ADR-0080]), Spanner reads **batch**-transaction partitions
  ([ADR-0085]) — each module's word is its vendor's, and [ADR-0083] already records the
  per-connector split nouns as deliberate. (Each module still owes itself *one* word per path;
  the intra-module mixes are routed to [#1052] and [#1053].)
- *Planning metrics.* The planning-happened-once counter names the vendor planning call where
  one exists: `readSessionsCreated` (`CreateReadSession`), `rowKeySamplesTaken`
  (`SampleRowKeys`). Spanner's planner may issue `partitionQuery`, `partitionRead` or
  `partitionReadUsingIndex` depending on the read, so its counter names the act
  (`readsPlanned`) — the abstract form is the rule's escape hatch, not a deviation.

**A sink implementation names its write family in the vendor's spelling** — the RPC where the
RPC is the family (`BigtableMutateRowsSink` [ADR-0041], `CloudTasksCreateTaskSink`), the write
method where that is the family (`BigQueryFileLoadsSink`, `SpannerMutationsSink` — "Mutations,
no DML" is Spanner's recorded write-method decision, [ADR-0075]), the SDK resource where the
resource is the seam (`PubSubPublisherSink`). `AppendErrorClassifier` keeps the `AppendRows`
family word rather than a connector prefix — its module has three write families for a
classifier to be about, so the family word is the precise one; the connector-prefixed siblings
(`PubSubErrorClassifier`, `BigtableErrorClassifier`, `SpannerErrorClassifier`) classify their
module's single write family, and the read side is role-named everywhere it exists
(`BigtableLookupErrorClassifier`, `SpannerLookupErrorClassifier`), so neither form is
ambiguous.

**A sink options object is named for what it configures, in three branches.** One write path →
`<Product>WriterOptions` (Cloud Tasks, Bigtable, Spanner). Several write methods → one options
class per family, named for the family (`BufferedStreamOptions`, `DefaultStreamOptions`,
`FileLoadsOptions` — a single `BigQueryWriterOptions` cannot exist). An options object that
configures one SDK resource *as a whole* → the resource's name (`PubSubPublisherOptions`,
`PubSubSubscriberOptions` — the `Publisher`/`Subscriber` clients; Pub/Sub is also the one
connector with a source-side options object, so `Writer`/`Reader` symmetry was never
available). The resource branch takes the whole object: a single SDK knob on a writer-shaped
options class (Cloud Tasks' `channelPoolSize`) does not move it out of the first branch.

**Creation-settings prefixes name different things and both stay.** `sink.table-create.*`
(BigQuery, Bigtable) names the resource being created; Pub/Sub's `sink.auto-create.*` /
`scan.auto-create.*` ([ADR-0014]) names a behavior that spans both directions — Pub/Sub is the
one connector that also creates on the read side. The sink-side setters agree
(`tableCreateOptions`, `topicCreateOptions`); the read side has no creation setter at all —
`SubscriptionCreateOptions` arrives as an argument of the `subscription(…)` overload.

**`*Destination` names the resource a connector is pointed at, in either direction.** Four
sink write targets under a `DestinationResolver` plus the source-side `SubscriptionDestination`
already spell it so, and Bigtable's `TableDestination` — taken by the sink, both sources and
the lookup ([ADR-0041]) — shows the noun is not sink-bound. Spanner's `SpannerDatabase`, the
same `of(project, …)` shape in the same role, was the one outlier; it is renamed
`DatabaseDestination` by [#1053] rather than defended.

**Metric vocabulary rules the review affirmed, beyond [ADR-0038]'s shape rules:**

- *The qualifier is shared; the noun names the unit the connector counts* ([ADR-0082]'s
  module-local reasoning, recorded here as the fleet rule). `inFlightAppends`,
  `inFlightEntries`, `inFlightTasks`, `inFlightMessages`; `parkedEntries`, `parkedTasks`,
  `parkedMessages`.
- *The state ladder is `buffered` → `inFlight` → `parked`*: held before hand-off; handed over
  and unresolved; handed back and awaiting resumption. The direction (a sink's buffer vs a
  source's) is the metric group's to say, not the name's — Spanner's sink `bufferedBytes` and
  Pub/Sub's source `bufferedBytes` are both correct under it. (`outstandingDeadLetters`, the
  one ladder value spelled outside it, is routed to [#1050].)
- *The noun-order flip is the deliberate counter/gauge marker.* [ADR-0038] owns the shape rule
  (a counter names the event, a gauge the state); what it does not say, and the review
  affirmed, is that the resulting near-anagrams are signal rather than accident — Pub/Sub's
  `parkedSplits`/`splitsParked` pair documents the flip in its javadoc, and the cross-module
  pair `assignedSplits` (gauge, reader-keyed continuous enumerator) vs `splitsAssigned`
  (counter, pull-assignment protocol) reads under the same key.
- *A retry counter's name gives the unit; its javadoc says whose loop it counts. There is no
  fleet-wide existence rule.* BigQuery's `appendRetries` counts its connector-owned recovery
  loop's re-issues while its `readRetries` counts the client library's own `ReadRows` retries
  ("which nothing else reports" — `BigQueryMetricNames`); Cloud Tasks owns its retry loop and
  registers no counter (a per-module decision its module reference records — `errorClass`
  classifies failed attempts rather than retry volume: first failures count, and a retry selected
  by a nested transient status is counted under the outer status);
  Bigtable's SDK owns the retries and none is exposed ([ADR-0043]). Existence is a per-module
  decision on the per-module records; the cross-connector rule is only that `mutationsRetried`
  and `appendRetries` name genuinely different units, so their shapes differ legitimately.
- *`last…` is the optional most-recent qualifier; `longest…` is the running max.* An unprefixed
  duration gauge (`deadLetterFlushMillis`) reads as most-recent; the max is the one a scrape
  can catch, which is why it earns the prefix.

**Absolute instants:** the Kafka-shaped startup pair spells epoch millis
(`scan.startup.timestamp-millis`), and a point-in-time read consistency choice is an ISO-8601
instant (`source.snapshot-time` — a different concept, not a divergent spelling of the same
one).

## Alternatives declined

- **A checker for cross-connector vocabulary.** The mechanism stays [ADR-0038]'s: diff the
  inventories, and the [#782] review as the audit. The rules above need language judgment
  (which word is "the vendor's"), which is what [ADR-0117] already concluded about the
  shape rules' soft half; a checker would hold the mechanical shell and miss the substance.
- **A shared constants module for shared names** — re-considered for this decision and declined
  by citation: [ADR-0038] records it as built first and withdrawn, because it splits each
  connector's inventory across two modules to close one narrow drift, and nothing about the
  cross-connector framing changes that cost.
- **Renaming the survivors for uniformity's sake** — `SpannerMutationsSink` to an RPC name,
  `AppendErrorClassifier` to a connector prefix or `PubSubPublisherOptions` to
  `*WriterOptions`: each would erase the information its divergence carries
  under the rules above.

## Consequences

- The drift half of the review lands through [#1047]–[#1053]; once those close, every name
  divergence across the five connectors is either gone or covered by a rule in this ADR.
- This ADR supersedes [ADR-0028]'s Cloud Tasks `retry*` exception (which Spanner had copied);
  ADR-0028 keeps the historical text under a supersession banner pointing here, and [#1051] and
  [#1053] execute the rename.
- The serialization-SPI rename ([#1048]) reverses an alternative [ADR-0055] declined and takes
  its own superseding record there, not here.
- [ADR-0082] carries a pointer marking its unit-naming reasoning as generalized here.

## Evidence — the judged divergence table

Measured at `4fb67806`. Three name surfaces; every divergence carries a verdict and a route.
Judgments and routing were decided with the maintainer on 2026-08-23; the review-thread copy
is the [#1043] comment on [#782]
(<https://github.com/flink-gcp/flink-connector-gcp/issues/782#issuecomment-5383548055>); this
appendix is the durable record. One-connector-only
classes are excluded here — they belong to [#1044](https://github.com/flink-gcp/flink-connector-gcp/issues/1044)'s counterpart matrix.

### Verdict legend

- **A — recorded deliberate**: a record already justifies it; no action.
- **B — deliberate, unrecorded**: stays, and is recorded now (ADR or module reference) rather
  than silently skipped.
- **C — drift, internal-only**: rename is cheap and does not hold the release; routed to a
  module-audit sub-issue.
- **D — drift, user-facing**: `@Public`/`@PublicEvolving` class, option key, public setter, or
  metric name; lands before the 1.0.0 tag; routed to a module-audit sub-issue.

A load-bearing fact for every D verdict: the japicmp gate is currently inert — the reference
version 1.0.0 is unpublished, `ignoreNonResolvableArtifacts` passes the check, and no
`<excludes>` list exists (`pom.xml:117-120`, `838-859`). Every rename below is free today. What
the tag freezes differs by surface: `@Public` types freeze under japicmp; `@PublicEvolving`
classes and option keys may still change at a minor release with a release-notes entry; metric
names are held by review, not japicmp. Pre-tag is the one moment every rename is free of all of
those costs, which is why the D verdicts land now. ADR-0124 keeps split and
enumerator-state types below the frozen line permanently.

---

### Surface 1 — class names

Concept-to-name matrix (tier: P = `@Public`, PE = `@PublicEvolving`, I = `@Internal`,
– = unannotated):

| Concept | BigQuery | Pub/Sub | Cloud Tasks | Bigtable | Spanner |
|---|---|---|---|---|---|
| Sink facade / builder | `BigQuerySink(Builder)` P | `PubSubSink(Builder)` P | `CloudTasksSink(Builder)` P | `BigtableSink(Builder)` P | `SpannerSink(Builder)` P |
| Sink impl | `BigQuery{BufferedStream,DefaultStream,FileLoads}Sink` I | `PubSubPublisherSink` I | `CloudTasksCreateTaskSink` I | `BigtableMutateRowsSink` I | `SpannerMutationsSink` I |
| Writer | `BigQuery{Buffered,Default}StreamWriter`, `FileLoadsWriter` I | `PubSubWriter` I | `CloudTasksWriter` I | `BigtableWriter` I | `SpannerWriter` I |
| Writer metrics | `{BufferedStream,DefaultStream,FileLoads}WriterMetrics` I | **`PubSubSinkWriterMetrics`** I | `CloudTasksWriterMetrics` I | `BigtableWriterMetrics` I | `SpannerWriterMetrics` I |
| Sink options | `BufferedStreamOptions`, `DefaultStreamOptions`, `FileLoadsOptions` P | `PubSubPublisherOptions` P | `CloudTasksWriterOptions` P | `BigtableWriterOptions` P | `SpannerWriterOptions` P |
| Serialization SPI | **`BigQueryProtoSerializer`** P | `PubSubSerializationSchema` P | `CloudTasksSerializationSchema` P | `BigtableSerializationSchema` P | `SpannerMutationSerializationSchema` P |
| Deserialization SPI | **`BigQueryRowDeserializer`** P | `PubSubDeserializationSchema` P | — | `BigtableRowDeserializationSchema` P | `SpannerStructDeserializationSchema` P |
| Error classifier | **`AppendErrorClassifier`** I | `PubSubErrorClassifier` I | *(none)* | `BigtableErrorClassifier` I | `SpannerErrorClassifier` I |
| Failed element | `FailedRow` P (`sink.failure`) | `FailedMessage` P | `FailedTask` P | `FailedMutation` P | `FailedMutation` P |
| Destination type | `TableDestination` P (`sink`) | `TopicDestination` P | `QueueDestination` P | `TableDestination` P (root) | `SpannerDatabase` P (root) |
| Source impl | `BigQueryStorageReadSource` I | `PubSubStreamingPullSource` I | — | `BigtableReadRowsSource` I | `SpannerBatchReadSource` I |
| Enumerator | `BigQueryReadSplitEnumerator` I | `PubSubSplitEnumerator` I | — | `BigtableScanSplitEnumerator` I | **`SpannerPartitionSplitEnumerator`** I |
| Enumerator state | `BigQueryReadEnumeratorState` I | `PubSubEnumeratorState` I | — | `BigtableScanEnumeratorState` I | `SpannerBatchEnumeratorState` I |
| Split | **`BigQueryReadStreamSplit`** I | `SubscriptionSplit` I | — | `RowRangeSplit` I / `ChangeStreamPartitionSplit` I | `PartitionSplit` I / **`SpannerChangeStreamPartitionSplit`** I |
| Table row SPI | **`RowDataSerializer`/`RowDataDeserializer`** I | `RowData{Ser,Deser}ializationSchema` I | `RowDataSerializationSchema` I | `RowData{Ser,Deser}ializationSchema` **–** | `RowData{Ser,Deser}ializationSchema` I |
| Options mapper (writer) | family-named `*OptionsMapper` I | `PublisherOptionsMapper` I | **`CloudTasksWriterOptionsMapper`** I | `WriterOptionsMapper` I | `WriterOptionsMapper` I |
| Credentials | `BigQueryCredentials` I, root | `PubSubCredentials` I, root | **`CloudTasksCredentials`** –, `sink.writer` | `BigtableCredentials` I, root | `SpannerCredentials` I, root |

#### Judged divergences (class names)

**C1. Serialization SPI: `*Serializer`/`*Deserializer` (BigQuery) vs `*SerializationSchema`/
`*DeserializationSchema` (all four siblings).** `@Public`, user-facing — the most visible naming
divergence in the repository, and the ecosystem is unanimous against it: Flink core's SPI is
`SerializationSchema`/`DeserializationSchema`, and the official connectors name theirs
`KafkaRecordSerializationSchema`/`KafkaRecordDeserializationSchema`,
`PulsarSerializationSchema`, `KinesisDeserializationSchema` — the vocabulary this repository's
other four connectors already use. ADR-0055 declined renaming `BigQueryProtoSerializer` once,
but on intra-module grounds (the [#125](https://github.com/flink-gcp/flink-connector-gcp/issues/125) format split; ~20-test churn) that predate both the
cross-module lens and the 1.0.0 freeze; `BigQueryRowDeserializer` has no record at all.
**Verdict: D — rename the family (`BigQueryProtoSerializationSchema`,
`BigQueryRowDeserializationSchema` — the exact pattern of Bigtable's
`BigtableRowDeserializationSchema` — and the format facades with them); a superseding ADR
records the reversal of ADR-0055's declined alternative. Routed as a sub-issue of [#777](https://github.com/flink-gcp/flink-connector-gcp/issues/777).**

**C2. BigQuery table-layer `RowDataSerializer`/`RowDataDeserializer`.** `@Internal`; the four
siblings agree on `RowDataSerializationSchema`/`RowDataDeserializationSchema`. Pure drift.
**Verdict: C — folded into C1's sub-issue (same vocabulary, same module).**

**C3. Bounded-read vocabulary: Read (BigQuery) / Scan (Bigtable) / Batch (Spanner).** All
`@Internal`. The cross-connector difference is vendor vocabulary — read stream/session
(ADR-0079), scan over the `ReadRows` family (ADR-0080), the SDK's batch transaction (ADR-0085) —
and ADR-0083 already records per-connector split nouns as deliberate. **Verdict: B for the
cross-connector difference (record the vendor-word rule once).** The *intra-module* mixes are
drift: Spanner names one path three ways (`SpannerBatchReadSource`, `SpannerBatchEnumeratorState`,
`SpannerPartitionSplitEnumerator`, `PartitionSplit`) — **C → [#781](https://github.com/flink-gcp/flink-connector-gcp/issues/781) sub-issue**; Bigtable names one
path two ways (`BigtableReadRowsSource` vs `BigtableScanSplitEnumerator`/
`BigtableScanEnumeratorState`) — **C → [#780](https://github.com/flink-gcp/flink-connector-gcp/issues/780) sub-issue**.

**C4. Split-name connector prefix.** `@Internal`, and ADR-0124 keeps splits below the frozen
line explicitly. Majority convention is unprefixed (`SubscriptionSplit`, `RowRangeSplit`,
`PartitionSplit`, `ChangeStreamPartitionSplit`); `BigQueryReadStreamSplit` and
`SpannerChangeStreamPartitionSplit` carry the prefix. The sharpest instance: Bigtable's
`ChangeStreamPartitionSplit` and Spanner's `SpannerChangeStreamPartitionSplit` are the same
concept, one prefixed. **Verdict: C — `ReadStreamSplit` → [#777](https://github.com/flink-gcp/flink-connector-gcp/issues/777) sub-issue,
`ChangeStreamPartitionSplit` → [#781](https://github.com/flink-gcp/flink-connector-gcp/issues/781) sub-issue, with their serializers.** [#783](https://github.com/flink-gcp/flink-connector-gcp/issues/783) does not bind
here: it re-examines the tier of CDC *surfaces*, and split types stay `@Internal` by ADR-0124
regardless of its outcome.

**C5. Sink options objects: three conventions.** All eight `@Public`. Family-scoped names
(`BufferedStreamOptions`, `DefaultStreamOptions`, `FileLoadsOptions`) are structurally forced —
BigQuery has three write methods, so a single `BigQueryWriterOptions` cannot exist (ADR-0028
records the shape in passing). Pub/Sub's role names (`PubSubPublisherOptions`,
`PubSubSubscriberOptions`) name the SDK resource the options configure — the `Publisher` and
`Subscriber` clients — which is the same rule ADR-0055 applies to SPI implementations; Pub/Sub is
also the only connector with a source-side options object, so `Writer`/`Reader` symmetry was
never available. `*WriterOptions` is the single-write-path form. **Verdict: B — record the
three-branch rule.**

**C6. `PubSubSinkWriterMetrics`.** The only writer-metrics class with `Sink` in the name and the
only `public` one (the visibility exists for cross-package tests: `ResidueCounterTest` sits in
the module-root test package). Named in ADR-0007/0009/0010 but never justified. **Verdict: C →
[#778](https://github.com/flink-gcp/flink-connector-gcp/issues/778) sub-issue (rename `PubSubWriterMetrics`; the executor decides whether visibility can also
drop).**

**C7. `SpannerMutationsSink` vs `BigtableMutateRowsSink`/`CloudTasksCreateTaskSink`.**
`@Internal`. Bigtable and Cloud Tasks name the RPC; Spanner names the write method, and
"Mutations, no DML" *is* Spanner's recorded write-method decision (ADR-0075) — the same
name-the-write-method rule that produced `BigQueryFileLoadsSink`. **Verdict: B — record one
sentence; no rename.**

**C8. `AppendErrorClassifier` unprefixed.** `@Internal`. The prefix names the `AppendRows`
family, consistent with the family-vocabulary rule — BigQuery has three write families for a
classifier to be about, so the family word is the precise one; the connector-prefixed siblings
classify their module's single write family, and the read side is role-named everywhere it
exists (`BigtableLookupErrorClassifier`, `SpannerLookupErrorClassifier`). Its `public`
visibility is recorded (ADR-0030: the committer imports it from a sibling package). **Verdict:
B — record.**

**C9. Opener and factory implementations.** `Default*` (the house style, ADR-0122) vs
SDK-resource-prefixed where `Default*` distinguishes nothing (`ReadClient*`, `DataClient*`,
`BatchClient*`, `WriteClient*` — ADR-0055, reaffirmed by ADR-0131). Both conventions recorded.
**Verdict: A.**

**C10. `CloudTasksWriterOptionsMapper`.** The mapper family's rule is "named after the options
class it maps, connector prefix dropped" (`PublisherOptionsMapper` ← `PubSubPublisherOptions`;
`WriterOptionsMapper` ← `BigtableWriterOptions`/`SpannerWriterOptions`). [#1043](https://github.com/flink-gcp/flink-connector-gcp/issues/1043)'s seed framed the
bare names as the deviation; the rule inverts it — Cloud Tasks' prefixed mapper is the outlier.
`@Internal`. **Verdict: C → [#779](https://github.com/flink-gcp/flink-connector-gcp/issues/779) sub-issue (rename `WriterOptionsMapper`).**

**C11. Package placement.** Bigtable `TableDestination` at the module root: recorded deviation
(ADR-0041 — the source facade takes the same value). BigQuery `sink.failure`: recorded
grandfathered exception (ADR-0055). `CloudTasksCredentials` package-private in `sink.writer`:
derivable from the same rule that makes the four siblings root-public (`@Internal`-public exists
only because sub-packages must import it; Cloud Tasks has one consumer in one package).
**Verdict: A for the first two; B (one sentence in the Cloud Tasks module reference) for the
third.**

**C12. Unannotated main-tree types.** `AGENTS.md` requires a Flink API annotation on every
main-tree class, and no checker holds the repository's own classes (the tier audit polices
*imported Flink* types only). Verified unannotated: Bigtable
`table.sink.RowDataSerializationSchema`, `table.source.RowDataDeserializationSchema`,
`BigtableLookupErrorClassifier`, `BigtableRowLookup`; Spanner `SpannerLookupErrorClassifier`,
`SpannerRowLookup`, `SpannerDatabaseRowLookup`, `SpannerLookupKeyEncoder`. Siblings annotate the
same shapes `@Internal` (Pub/Sub, Cloud Tasks, and Spanner's own `table.sink` class). The
seventh review round added `CloudTasksCredentials` to the list: it stays package-private (C11)
and gains `@Internal`. **Verdict: C → [#780](https://github.com/flink-gcp/flink-connector-gcp/issues/780), [#781](https://github.com/flink-gcp/flink-connector-gcp/issues/781) and [#1051](https://github.com/flink-gcp/flink-connector-gcp/issues/1051) sub-issues (add `@Internal`; sweep
each module for others while there).**

**C13. Two `@Public` `StartPosition` classes.** `base.source.StartPosition` (change-stream
start: `latest()`/`earliest()`/`at()`/`ago()`) and `pubsub.source.StartPosition` (mode +
timestamp, deliberately mirroring Kafka's `scan.startup` pair — its javadoc says so). Same
concept in the user's sense, deliberately different shapes, one shared simple name whose cost is
an import clash in a job using both connectors. **Verdict: D — rename Pub/Sub's to
`PubSubStartPosition` → [#778](https://github.com/flink-gcp/flink-connector-gcp/issues/778) sub-issue.** Not a [#783](https://github.com/flink-gcp/flink-connector-gcp/issues/783) item: the Pub/Sub class is not a CDC
surface, and the base class keeps its name.

Non-findings (stated so they are not re-litigated): `RowStreamOpener` appearing in both BigQuery
and Bigtable is the vocabulary rule working, not drift, as are `TableDestination`,
`TableCreateOptions`, `FailedMutation`, `WriterOptionsMapper`, `ChangeStreamStartPositionMapper`
recurring across modules; writer names are uniform (`*Writer`); reader-side metrics names are
uniform (`*SourceReaderMetrics`).

---

### Surface 2 — option keys and public builder setters

Every item here is user-facing: option keys live on `@PublicEvolving` `*ConnectorOptions`
classes, setters on `@Public` builders/options. Concept matrix (key / setter; only
divergence-bearing rows — identical-by-construction rows are summarized below):

| Concept | BigQuery | Pub/Sub | Cloud Tasks | Bigtable | Spanner |
|---|---|---|---|---|---|
| read-side key prefix | **`source.*`** (12 keys) | `scan.*` | — | `scan.*` | `scan.*` |
| in-flight count | `sink.default-stream.max-inflight-requests` / `maxInflightRequests` | `sink.in-flight.max-messages` / `maxInFlightMessages` | `sink.max-in-flight-tasks` / `maxInFlightTasks` | `sink.in-flight.max-entries` / `maxInFlightEntries` | — |
| in-flight bytes | `…max-inflight-bytes` / `maxInflightBytes` | `sink.in-flight.max-bytes` / `maxInFlightBytes` | — | `sink.in-flight.max-bytes` / `maxInFlightBytes` | — |
| batch by count | — | `sink.batching.element-count-threshold` | — | `sink.batching.element-count` | `sink.buffer-flush.max-mutations`, `…max-cells` |
| batch by bytes | `sink.*.max-append-request-bytes` | `sink.batching.request-byte-threshold` | — | `sink.batching.byte-size` | `sink.buffer-flush.max-size` |
| batch latency | `sink.default-stream.flush-interval` | `sink.batching.delay-threshold` | — | — | `sink.buffer-flush.max-commit-delay` |
| connector-owned retry budget | `sink.*.recovery.*` / `recovery*` | `sink.recovery.*` / `recovery*` | **`sink.retry.*` / `retry*`** | `sink.recovery.*` / `recovery*` | **`sink.retry.*` / `retry*`** |
| total SDK retry budget | `sink.*.retry.max-duration` / `maxRetryDuration` | `sink.retry.total-timeout` / `retryTotalTimeout` | — | — | — |
| sink failure handler | **`failureHandler`** | `failedMessageHandler` | `failedTaskHandler` | `failedMutationHandler` | `failedMutationHandler` |
| source deserializer setter | `deserializer` | **`deserializationSchema`** | — | `deserializer` | `deserializer` |
| fixed sink destination setter | **`destination(…)`** | `topic(…)` | `queue(…)` | `table(…)` | `database(…)` |
| per-destination metrics | `sink.{default-stream,file-loads}.per-destination-metrics` | `sink.metrics.per-destination` | `sink.metrics.per-destination` | `sink.metrics.per-destination` | — |
| creation settings prefix | `sink.table-create.*` | `sink.auto-create.*`, `scan.auto-create.*` | — | `sink.table-create.gc-rule.*` | — |
| absolute instant | `source.snapshot-time` (ISO-8601 string) | `scan.startup.timestamp-millis` (long) | — | `scan.startup.timestamp-millis`, **`scan.end-timestamp-millis`** (long) | `scan.startup.timestamp-millis` (long) |

Identical by construction (no work): `project`, `emulator-endpoint`/`emulatorEndpoint`,
`service-account-key-file`/`serviceAccountKeyFile`, `serializer`, `destinationResolver`,
`sink.create-disposition`/`createDisposition`, `sink.max-consecutive-rejections`,
`destinationIdleTimeout`, `startPosition`, `resumeFallback`, the
`recovery{InitialBackoff,MaxBackoff,MaxAttempts}` triple, the `*{Include,Exclude}List` filters,
`lookup.async`, `scan.change-stream.changelog-mode`.

#### Judged divergences (options)

**O1. "In flight" is spelled three ways in keys and two ways in setters.** *(Corrected
2026-08-23: the BigQuery half of the first verdict was wrong — the independent review found
`maxInflightRequests`/`maxInflightBytes` front `StreamWriter.Builder.setMaxInflightRequests`/
`setMaxInflightBytes`, so the lowercase `f` is the vendor's own casing and renaming it would
erase that information.)* Connector-owned ledgers spell the grouped house form
`sink.in-flight.max-<unit>` / `maxInFlight<Unit>` (Pub/Sub, Bigtable); SDK-owned bounds keep
the SDK's spelling (BigQuery). **Verdict: BigQuery — B, recorded in ADR-0137's in-flight rule;
Cloud Tasks' flat `sink.max-in-flight-tasks` (a connector-owned ledger) still regroups to
`sink.in-flight.max-tasks` — D → [#1051](https://github.com/flink-gcp/flink-connector-gcp/issues/1051).**

**O2. BigQuery's read side uses `source.*` where every sibling uses `scan.*` (12 keys).**
ADR-0014 records `scan.` as "not a choice — Flink's read-side prefix", and BigQuery's own table
page already mixes prefixes (`scan.parallelism` from `FactoryUtil` beside `source.query`). No
record explains the deviation. Also subsumes the one flat retry key
(`source.retry-max-attempts` vs everyone's dotted `retry.max-attempts`). **Verdict: D — its own
sub-issue of [#777](https://github.com/flink-gcp/flink-connector-gcp/issues/777) (12-key user-facing rename; the largest single item on this surface).**

**O3. Batching: three key vocabularies.** Measured against the Flink ecosystem and the SDKs:
JDBC/HBase use `sink.buffer-flush.max-rows`/`max-size`/`interval`; Elasticsearch uses its
client's own word (`sink.bulk-flush.*`); Kafka has no Flink-level batching keys. In this
repository, Spanner's `sink.buffer-flush.*` names a **connector-owned** buffer in the Flink
idiom — conformant on both axes, and its `max-commit-delay` leaf is the Spanner API's own
`maxCommitDelay` field (a commit-time option the vendor applies server-side, not a local flush
trigger), the vendor word riding the house prefix; Pub/Sub's `sink.batching.element-count-threshold`/
`request-byte-threshold`/`delay-threshold` are gax `BatchingSettings`' own field names fronting
an **SDK-owned** batcher — the Elasticsearch pattern; BigQuery's `max-append-request-bytes`
names the `AppendRows` request limit (vendor word). Bigtable fronts the same gax fields as
Pub/Sub but under renamed keys (`element-count`, `byte-size` →
`setElementCountThreshold`/`setRequestByteThreshold` in `DefaultMutationBatcherFactory`).
**Verdict: the rule is recorded — an SDK-owned batcher takes `sink.batching.*` with the SDK's
own leaf vocabulary; a connector-owned buffer takes Flink's `sink.buffer-flush.*` idiom — and
the one deviation from it is renamed: Bigtable `sink.batching.element-count` →
`element-count-threshold`, `sink.batching.byte-size` → `request-byte-threshold` (D → [#780](https://github.com/flink-gcp/flink-connector-gcp/issues/780)
sub-issue).**

**O4. Per-destination metrics key: `sink.metrics.per-destination` (three connectors) vs
`sink.<family>.per-destination-metrics` (BigQuery).** Same setter (`perDestinationMetrics`)
everywhere; pure word-order drift. **Verdict: D → [#777](https://github.com/flink-gcp/flink-connector-gcp/issues/777) sub-issue (adopt
`sink.<family>.metrics.per-destination`).**

**O5. Connector-owned retry budgets named `retry*` in Cloud Tasks and Spanner, `recovery*`
elsewhere.** ADR-0028's rule: bare `retry*` is the SDK layer's vendor vocabulary, `recovery*`
the connector-owned budget; Cloud Tasks is recorded there as "the standing cross-module
asymmetry" — tolerated because its module has no second retry layer. Spanner then shipped the
same shape, making it two modules and eroding the rule's readability. **Verdict: D — unify:
rename both modules' keys and setters to `recovery.*`/`recovery*` ([#779](https://github.com/flink-gcp/flink-connector-gcp/issues/779) and [#781](https://github.com/flink-gcp/flink-connector-gcp/issues/781) sub-issues);
the ADR-0028 asymmetry note is retired by the renames (its refinement rides those PRs).**

**O6. Total retry budgets: `maxRetryDuration` (BigQuery) vs `retryTotalTimeout` (Pub/Sub).**
*(Corrected 2026-08-23: the "one gax field" premise measured false — BigQuery's value goes to
`StreamWriter.Builder.setMaxRetryDuration` (`StreamWriterRowAppenderFactory:165,210`), the
Storage Write SDK's own knob, whose `0` means no limit; Pub/Sub's goes to gax
`RetrySettings.totalTimeout`, whose `0` carries gax's attempt-bounded meaning. Two different
vendor knobs, each already carrying its vendor's name — renaming would erase that and promise
the wrong zero semantics.)* **Verdict: B — record under the vendor-word rule; the rename was
withdrawn from [#1049](https://github.com/flink-gcp/flink-connector-gcp/issues/1049).**

**O7. `failureHandler` (BigQuery) vs `failed<Noun>Handler` (four siblings).** Not drift:
ADR-0114 renamed `failedRowHandler` → `failureHandler` deliberately when the handler widened to
`BigQueryFailure` (`UnroutableRecord` + `FailedRow`) — a single noun would be wrong. The
siblings' handlers each take exactly one failure type. **Verdict: A.** (ADR-0017's mention of the
old `failedRowHandler` name needs no fix: it sits in a section already marked
superseded-historical under its ADR-0114 banner.)

**O8. Pub/Sub's source deserializer setter is `deserializationSchema`; BigQuery, Bigtable and
Spanner say `deserializer` (6 builders).** The SPI *types* are `*DeserializationSchema` in
Bigtable too, so the divergence is the setter name only, 6:1 against Pub/Sub. **Verdict: D →
[#778](https://github.com/flink-gcp/flink-connector-gcp/issues/778) sub-issue (rename to `deserializer`).**

**O9. Resource-creation prefixes: `sink.table-create.*` (BigQuery, Bigtable) vs
`sink.auto-create.*`/`scan.auto-create.*` (Pub/Sub).** ADR-0014 records the `auto-create`
choice, weighed inside Pub/Sub (source/sink symmetry — Pub/Sub is the one connector that also
creates on the *read* side, where "table-create" has no analogue). Setters already agree
(`<resource>CreateOptions`). **Verdict: B — record; the prefixes name different things (a
resource vs a behavior that spans directions) and each is coherent in place.**

**O10. BigQuery's fixed-destination setter is `destination(TableDestination)`; every sibling
names the resource (`topic`, `queue`, `table`, `database`) — and BigQuery's own *source* takes
the same `TableDestination` through `table(…)`.** One type, two setter names inside one
connector, and the source's name is the one that matches the fleet. **Verdict: D → [#777](https://github.com/flink-gcp/flink-connector-gcp/issues/777)
sub-issue (sink adopts `table(…)`).**

**O11. `SpannerDatabase` is the only resource value object not named `*Destination`.**
*(Corrected 2026-08-23 — twice: the first defense (resolver-bound) fails on the source-side
`SubscriptionDestination`, and the second (direction-spanning) fails on Bigtable's
`TableDestination`, which the sink, both sources and the lookup all take (ADR-0041). No
defense for the different noun survived measurement, and the maintainer decided rename.)*
**Verdict: D → [#1053](https://github.com/flink-gcp/flink-connector-gcp/issues/1053) (rename to `DatabaseDestination`, unprefixed like its four siblings; the
`database(…)` setters and the `of(…)` shape stay). ADR-0137 records the rule: `*Destination`
names the resource a connector is pointed at, in either direction.**

**O12. Pub/Sub bounds the same concept as `maxInFlightMessages` (sink) and
`maxOutstandingMessages` (DLQ).** *(Corrected 2026-08-23 after the records-PR review measured
the first verdict's premise false.)* The sink's bound is the writer's own ledger (the SDK flow
controller is deliberately not exposed) — but so is the DLQ's: `maxOutstandingMessages` bounds
the DLQ's own list of unawaited publish futures, and no `FlowControlSettings` exists anywhere on
the sink side. gax's `outstanding` vocabulary belongs only to the source's
`scan.flow-control.max-outstanding-*` keys, which genuinely front gax flow control. Two words
for the connector-ledger concept inside one module is drift. **Verdict: D → [#1050](https://github.com/flink-gcp/flink-connector-gcp/issues/1050) (rename the
DLQ knob and the `outstandingDeadLetters` metric to the `inFlight` vocabulary; DLQ types are
`@Experimental`, so the rename is cheap).**

**O13. Absolute instants: epoch-millis longs (`scan.startup.timestamp-millis` et al.) vs one
ISO-8601 string (`source.snapshot-time`).** Different concepts (a startup position vs a
point-in-time read consistency choice), and the millis spelling is Kafka's own convention for
the startup pair. **Verdict: B — record; no rename.**

**O14. Bigtable's stop position is flat `scan.end-timestamp-millis` beside grouped
`scan.startup.*` and `scan.resume-fallback.*`.** Flink Kafka's vocabulary for the same concept
is `scan.bounded.mode` + `scan.bounded.timestamp-millis`. **Verdict: D → [#780](https://github.com/flink-gcp/flink-connector-gcp/issues/780) sub-issue (adopt
the ecosystem's `scan.bounded.*` shape).**

**O15. Spanner partition keys.** *(Corrected 2026-08-23: the `max-partitions` half was
withdrawn — `maxPartitions` is `PartitionOptions.Builder.setMaxPartitions`' own name
(`SpannerSourceBuilder:332`), so the apparent stutter is the vendor's word and stays under the
vendor-word rule.)* The surviving half: `scan.partition.size` drops the `-bytes` its setter
(`partitionSizeBytes`) carries, against a repository where byte-valued keys keep their unit.
**Verdict: D → [#781](https://github.com/flink-gcp/flink-connector-gcp/issues/781) sub-issue (`scan.partition.size-bytes`).**

Non-findings: `emulator-rest-endpoint` (second transport, ADR-0029);
`channel-pool-size` vs `{min,max}-connections-per-region` (different vendor objects: gax channel
pool vs Storage Write API multiplexing pool — ADR-0134); `decode.trailing-bytes` and
`null-string-literal` top-level placement (ADR-0136, ADR-0086/HBase lineage);
`max-concurrent-streams-per-subtask` vs `max-concurrent-queries-per-subtask` (each names the
unit it counts); `parallel-pull-count` (the SDK's own word); `sink.shutdown-timeout`
(Pub/Sub-only concept).

---

### Surface 3 — metric names

The five `*MetricNames.java` inventories were diffed in full (ADR-0038 names that diff as the
consistency mechanism). Converged and healthy: `recordsSkipped` (all five, both directions),
`rowsRead`, `tablesCreated`/`topicsCreated`, the entire change-stream lag family — Bigtable and
Spanner independently agree on `queuedChangeStreamPartitions`,
`queuedChangeStreamPartitionLagMillis`, `unassignedChangeStreamPartitionLagMillis`,
`missedHeartbeatIntervals`, `changeStreamPartitionsDiscovered`,
`changeStreamRecordsSkippedWithoutChange` — and the `…Millis` suffix universally.

#### Judged divergences (metrics)

**M1. Retry counters: `appendRetries`/`readRetries` (BigQuery) vs `mutationsRetried`
(Spanner).** *(Corrected 2026-08-23: the "existence rule" the first version claimed measured
false — `readRetries` counts the client library's own `ReadRows` retries, and Cloud Tasks owns
its retry loop yet registers no counter.)* Both shapes are ADR-0038-legal, and the units
genuinely differ — `appendRetries` counts re-issued requests, `mutationsRetried` counts
mutations re-sent — so each name names the unit it counts (ADR-0082's rule), and each javadoc
says whose loop it counts. Existence is a per-module decision on per-module records; there is no
fleet rule to hold. **Verdict: B — record the unit reading; no rename.**

**M2. `assignedSplits` (Pub/Sub, gauge) vs `splitsAssigned` (three bounded sources, counter).**
Instrument-correct under ADR-0038 (gauge names state, counter names event), and the repository
already uses the noun-order flip deliberately as the counter/gauge marker
(`parkedSplits`/`splitsParked`, documented in-javadoc). The quantities also differ (continuous
reader-keyed enumerator vs pull-assignment protocol). **Verdict: B — promote the noun-order-flip
rule from a javadoc aside to the record, so the anagram reads as signal.**

**M3. `bufferedBytes` names a sink write buffer in Spanner and a source read buffer in
Pub/Sub.** No collision (different operator groups), and the coherent reading — `buffered` =
held before hand-off, on either side; the metric group carries the direction — fits every
current name (`bufferedMutations`/`bufferedCells` vs `inFlightEntries` is the
not-yet-sent/sent distinction working). **Verdict: B — record the
`buffered`/`inFlight`/`parked` state ladder once.**

**M4. `outstandingDeadLetters`.** Follows O12's corrected verdict: the DLQ's ledger is
connector-owned, so the fifth adjective names nothing. **Verdict: D → [#1050](https://github.com/flink-gcp/flink-connector-gcp/issues/1050), with the knob.**

**M5. "Planning happened once" counters: `readSessionsCreated` (BigQuery, vendor op
`CreateReadSession`), `rowKeySamplesTaken` (Bigtable, vendor op `SampleRowKeys`),
`readsPlanned` (Spanner).** *(Corrected 2026-08-23: Spanner's planner issues `partitionQuery`,
`partitionRead` or `partitionReadUsingIndex` depending on the read
(`BatchClientPartitionPlanner:194-208`) — there is no single vendor call to name, which is why
the abstract name exists.)* **Verdict: B — record: the vendor-call rule applies where one call
exists; with three possible RPCs the counter names the act. The rename was withdrawn from
[#1053](https://github.com/flink-gcp/flink-connector-gcp/issues/1053).**

**M6. "Most recent value" gauges: Spanner prefixes `last…`
(`lastChangeStreamRecordWaitMillis`); Pub/Sub leaves the latest unprefixed
(`deadLetterFlushMillis`) and reserves `longest…` for the maximum.** Pub/Sub's shape carries an
operational argument ([#405](https://github.com/flink-gcp/flink-connector-gcp/issues/405): the max is what a scrape can actually catch) that Spanner's gauge
lacks a counterpart for. **Verdict: B — record the qualifier convention (`last` optional,
`longest` = running max); the missing `longest…` counterpart on the Spanner side is routed to
the [#781](https://github.com/flink-gcp/flink-connector-gcp/issues/781) sub-issue as an observation, not a naming fix.**

**M7. Filter counters encode different axes: Spanner's `changeStreamRecordsFilteredByTable`
names the filter dimension; Bigtable's `changeStreamMutationEntriesFiltered` names only the
unit.** Each is unambiguous beside its module's docs, and the units genuinely differ. **Verdict:
B-grade; covered by the unit-naming rule; no rename.**

Non-findings: `inFlight<Unit>`/`parked<Unit>` noun differences (recorded, ADR-0082 and the
`PARKED_SPLITS` javadoc); send-verb differences (`batchesSent`, `deadLettersPublished`,
`loadJobsSubmitted` — each the vendor's noun-verb pair); `unassignedReaders` (a different
quantity than `unassignedSplits`, not a rename); no custom error counters anywhere (the
`errorClass.*` scheme and Flink standards cover it); the `num`-prefix ban (mechanized);
`columnFamiliesAdded` vs `schemaReconciliations` (different repairs, both recorded).

---

### Routing

Rename work is filed as sub-issues of the module audit issues; a following PR records the
verdict-B rules and the staleness fixes.

| Route | Items |
|---|---|
| [#777](https://github.com/flink-gcp/flink-connector-gcp/issues/777) sub-issue: `source.*` → `scan.*` migration | O2 (12 keys, includes the flat `source.retry-max-attempts`) |
| [#777](https://github.com/flink-gcp/flink-connector-gcp/issues/777) sub-issue: serialization SPI rename | C1 + C2 (superseding ADR for ADR-0055's declined alternative) |
| [#777](https://github.com/flink-gcp/flink-connector-gcp/issues/777) sub-issue: naming alignment, small items | O4, O10, C4 (`ReadStreamSplit`) — O1's BigQuery half and O6 withdrawn (vendor names) |
| [#778](https://github.com/flink-gcp/flink-connector-gcp/issues/778) sub-issue: naming alignment | C6, C13 (`PubSubStartPosition`), O8, O12/M4 (DLQ `outstanding` vocabulary) |
| [#779](https://github.com/flink-gcp/flink-connector-gcp/issues/779) sub-issue: naming alignment | O5 (`retry*` → `recovery*`), C10, O1 (key grouping — the surviving half), C12 (`CloudTasksCredentials` gains `@Internal`) |
| [#780](https://github.com/flink-gcp/flink-connector-gcp/issues/780) sub-issue: naming alignment | O3 (gax leaf names), O14 (`scan.bounded.*`), C3 (Scan/ReadRows mix), C12 |
| [#781](https://github.com/flink-gcp/flink-connector-gcp/issues/781) sub-issue: naming alignment | O5 (`retry*` → `recovery*`), C3 (Batch/Partition mix), C4 (split prefix), O11 (`DatabaseDestination`), O15, C12, M6 (observation) — M5 withdrawn (three vendor RPCs) |
| This ADR + module-reference pointers | C3 rule, C5, C7, C8, C11 (Cloud Tasks), O1 (BigQuery half), O9, O13, M1, M2, M3, M6 rule; staleness: ADR-0055 tier text ×3 |
| [#729](https://github.com/flink-gcp/flink-connector-gcp/issues/729) | `docs/content/docs/reference/_index.md` omits Spanner from the SQL connector list |
| No action (A) | C9, C11 (BigQuery/Bigtable), O7, non-findings lists |

[#782]: https://github.com/flink-gcp/flink-connector-gcp/issues/782
[#1043]: https://github.com/flink-gcp/flink-connector-gcp/issues/1043
[#1047]: https://github.com/flink-gcp/flink-connector-gcp/issues/1047
[#1048]: https://github.com/flink-gcp/flink-connector-gcp/issues/1048
[#1050]: https://github.com/flink-gcp/flink-connector-gcp/issues/1050
[#1051]: https://github.com/flink-gcp/flink-connector-gcp/issues/1051
[#1052]: https://github.com/flink-gcp/flink-connector-gcp/issues/1052
[#1053]: https://github.com/flink-gcp/flink-connector-gcp/issues/1053
[ADR-0014]: 0014-the-pubsub-table-layer-maps-onto-the-datastream-builders.md
[ADR-0028]: 0028-default-stream-tuning-recovery-vs-retry-naming-eviction-and-flush-interval.md
[ADR-0038]: 0038-metric-names-are-per-connector-inventories-in-one-shape-without-flinks-num-prefix.md
[ADR-0041]: 0041-the-bigtable-sink-is-implemented-on-four-checked-sdk-facts.md
[ADR-0043]: 0043-bigtable-metrics-are-the-series-standard-reached-late.md
[ADR-0055]: 0055-connector-packages-follow-one-skeleton-and-a-layer-exists-only-where-a-sibling-can-arrive.md
[ADR-0075]: 0075-the-spanner-sink-batch-writes-and-owns-the-whole-retry-loop.md
[ADR-0079]: 0079-the-bigquery-source-splits-by-read-stream-and-its-enumerator-keeps-no-ledger.md
[ADR-0080]: 0080-the-bigtable-scan-source-splits-by-sampled-row-key-range.md
[ADR-0082]: 0082-the-bigtable-batch-knobs-are-bounded-flush-triggers-and-the-client-counts-mutations.md
[ADR-0083]: 0083-the-pull-assignment-split-enumerator-is-one-base-class.md
[ADR-0085]: 0085-the-spanner-batch-source-splits-by-server-planned-partition.md
[ADR-0117]: 0117-metric-tables-are-held-bidirectionally-to-connector-inventories.md
