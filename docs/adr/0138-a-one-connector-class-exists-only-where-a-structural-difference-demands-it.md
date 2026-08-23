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

# ADR-0138: A one-connector class exists only where a structural difference demands it

- Status: Accepted
- Date: 2026-08-23; revised by [#1056], [#1057], and [#1059] (2026-08-23)
- Issues: [#1044], [#782], [#1056], [#1057], [#1059]
- Modules: base, all connectors, test-utils
- Current behavior: the decisions below are the durable record of the gaps judged deliberate;
  the review-thread copy of the full matrix, including the routed half, is the [#1044] comment
  at <https://github.com/flink-gcp/flink-connector-gcp/issues/782#issuecomment-5383714375>. The
  `elapsedMillis` alignment from [#775] is implemented by [#1056], the `resolveRestored` rename
  was completed through [#1052], and the Cloud Tasks classifier extraction was completed through
  [#779].
  The shared Spanner emulator test client from [#776] is implemented by [#1057], and the Spanner
  change-stream reader decomposition from [#781] is implemented by [#1059].

## Context

The [#1044] review built a class-counterpart matrix across the five connectors' main trees,
extended to `test-utils` for the doubles, and judged every class that exists in one connector
with no counterpart in the others: a missing base abstraction, a missing shared test double, or
a connector-specific need. The first two kinds are routed to the audits; this ADR records the
third, so the next reader meets a reason instead of rediscovering the question. [ADR-0137] is
the same move for names; this is the move for presence.

Several gaps were already recorded and are only cross-referenced by the matrix — Cloud Tasks
never creates queues ([ADR-0048]), BigQuery alone wraps its admin in a retrying decorator with
Bigtable's non-adoption recorded beside it ([ADR-0071], [ADR-0073]), the Pub/Sub-local context
fakes beside the shared pair ([ADR-0050]), Spanner's batch seams keeping their lazy lifecycle
out of a holder ([ADR-0131]). The gaps below had no record.

## Decision

**Spanner has no destination-routing surface, deliberately.** No `DestinationResolver`, no
`FixedDestinationResolver`, no `CreateDisposition`, no `TableCreateOptions`, and no
`DestinationMetrics` import — because the sink's destination is a *database* and the
serializer's `Mutation` already names its own table ([ADR-0075]): per-record routing exists
inside the value, so a resolver SPI would have nothing left to resolve, and a per-destination
metrics cardinality would be the serializer's to choose. [ADR-0074] lists the four connectors
that publish the resolver shape; this records why Spanner is not among them. The destination
value object's *name* is a separate, already-decided matter: [#1043]'s O11 measured its
keep-defenses false, and `SpannerDatabase` is renamed `DatabaseDestination` by [#1053] under
[ADR-0137]'s `*Destination` rule — what is deliberate here is the absent routing surface, not
the noun.

**Spanner has no admin seam and no auto-create, deliberately.** BigQuery, Pub/Sub and Bigtable
each pair an admin interface with an impl because their resources can be created from what the
sink already holds. Creating a relational Spanner table is not that: the connector would have to
derive a typed DDL statement from the Flink schema and execute it as a long-running operation —
a design of its own, with schema-ownership questions attached, not a `create()` call behind a
disposition flag. The sink writes mutations to tables the user owns. An auto-create proposal is
a feature with its own issue and record, not a gap repair.

**Spanner's table sink writes no metadata, deliberately.** The siblings' `WritableMetadata`
enums each map to a per-record out-of-band field the service defines — the cell timestamp
(Bigtable), message attributes and the ordering key (Pub/Sub), task target fields (Cloud
Tasks), CDC sequence inputs (BigQuery). A Spanner mutation carries only declared column values;
there is no out-of-band field for a metadata column to write, so `SpannerDynamicSink` is a
plain `DynamicTableSink`.

**Bigtable wraps the restore seam; Spanner uses it bare — both on purpose.** The base
`StartPositionResolver` javadoc names the split: most connectors use `resolveRestored`
directly, and a connector whose partition topology must restart as one unit uses
`inspectRestored` + `resolveFallback`. Spanner is the second kind and calls the base API at the
enumerator, whole-ledger, with nothing in between. Bigtable is the first kind — its enumerator
also calls the base API directly — and additionally resolves splits *at the reader*, which is
the path `ChangeStreamRestoreResolver`/`DefaultChangeStreamRestoreResolver` exist for: they
give the base seam the three things reader-side resolution needs — serializability, the
reader's one credentials load, and a split-shaped signature. The wrapper is the shipping
address, not a second policy.

**A simple name shared by the two change-stream modules is per-product vocabulary, not a shared
type.** `ChangeStreamStartMode`, `ScanMode`, `ChangeStreamChangelogMode` and
`PartitionProgressEvent` each exist in both Bigtable and Spanner; the first two are textually
near-identical today, the third names two orthogonal axes (a physical representation versus
changelog completeness), the fourth carries different payloads for different coordinators.
Textual identity today does not promote a type to base: base has no table layer, and a shared
enum would forbid exactly the per-product value addition — a Bigtable-only mode, a Spanner-only
mode — that per-module enums exist to allow. The name is shared because the concept is shared
([ADR-0137]); the type is per-module because the products may diverge.

**The change-stream read path's shape is one class per role, deliberately.** Both
`*ChangeStreamSource` classes are single classes where every bounded path splits a facade from
an impl — there is one impl, so there is nothing for a facade to hide. Both readers implement
`SourceReader` directly with no `*SplitReader` — a continuous partition-lifecycle reader does
not fit the fetcher framework's split-fetch loop, which [ADR-0101]/[ADR-0103] bound differently.
Inside that shared shape, both readers route record-specific emission, split-state advance, and
coordinator events through an emitter plus mutable split state; [#1059] aligned Spanner with the
Bigtable reference. The `@PublicEvolving` tier of the source classes was decided by [#783] and is
unchanged by this internal alignment.

**A client holder exists where a shared construction branch does.** `BigQueryReadClients`,
`BigQueryWriteClients`, `BigtableDataClients` and `SpannerClients` centralize an
emulator-versus-credentials branch reached from several classes. Cloud Tasks has one
construction site inside `DefaultTaskCreatorFactory` — a holder with one user is a rename
([ADR-0131]'s own words). Pub/Sub builds clients at five sites — the publisher and subscriber
factories, both admins, and the dead-letter queue — each fronting its own emulator branch;
whether that repetition ever earns a holder is the question [#1046] already owns for the other
modules, and it is not decided here.

## Alternatives declined

- **A base `ChangeStreamStartMode`/`ScanMode`.** Measured: the pairs are near-identical today,
  and that is the strongest form of the proposal — it still buys only the deletion of two small
  enums while forbidding per-product values and giving base its first table-layer type. Declined
  above; a product change that forks the vocabulary re-opens nothing, it is the rule working.
- **A Spanner `DestinationResolver` (or a no-op `CreateDisposition`) for fleet symmetry.** Both
  would be interfaces with nothing to decide, kept so a table reads uniformly — the
  consolidation-shaped move [#1044]'s own charter declines.
- **An extracted Cloud Tasks classifier recorded as deliberate.** The inverse was decided: the
  inline placement is the outlier with no reason attached, so the class is extracted ([#779])
  rather than the absence defended.

## Consequences

- `MetricValues` in `base.metrics` owns the overflow-safe `elapsedMillis` policy used by Bigtable
  and Spanner ([#775], implemented by [#1056]). The `resolveRestored` private-method rename was
  completed through [#1052], and the Cloud Tasks classifier extraction was completed through
  [#779].
  `SpannerTestClients` ([#1057] implements [#776]) replaces all three inline emulator-client
  constructions found in the current test trees.
  `SpannerChangeStreamRecordEmitter` and `ChangeStreamPartitionSplitState` implement the reader
  decomposition routed through [#781] and completed by [#1059].
- The module references for Spanner and Bigtable carry pointers to the records above beside the
  designs they qualify.
- Both [#782] artifacts are posted, and every finding of the cross-module review is routed or
  recorded — the review's completion condition. Whether the per-module audits and [#782] close
  on that is the maintainer's call.

[#782]: https://github.com/flink-gcp/flink-connector-gcp/issues/782
[#1043]: https://github.com/flink-gcp/flink-connector-gcp/issues/1043
[#1044]: https://github.com/flink-gcp/flink-connector-gcp/issues/1044
[#775]: https://github.com/flink-gcp/flink-connector-gcp/issues/775
[#776]: https://github.com/flink-gcp/flink-connector-gcp/issues/776
[#779]: https://github.com/flink-gcp/flink-connector-gcp/issues/779
[#781]: https://github.com/flink-gcp/flink-connector-gcp/issues/781
[#783]: https://github.com/flink-gcp/flink-connector-gcp/issues/783
[#1046]: https://github.com/flink-gcp/flink-connector-gcp/issues/1046
[#1052]: https://github.com/flink-gcp/flink-connector-gcp/issues/1052
[#1053]: https://github.com/flink-gcp/flink-connector-gcp/issues/1053
[#1056]: https://github.com/flink-gcp/flink-connector-gcp/issues/1056
[#1057]: https://github.com/flink-gcp/flink-connector-gcp/issues/1057
[#1059]: https://github.com/flink-gcp/flink-connector-gcp/issues/1059
[ADR-0048]: 0048-the-cloud-tasks-sink-owns-its-retry-loop-and-never-creates-queues.md
[ADR-0050]: 0050-test-utils-holds-test-support-only-with-all-provided-dependencies.md
[ADR-0071]: 0071-a-lost-table-creation-race-is-retried-by-a-wrapped-table-admin.md
[ADR-0073]: 0073-bigtable-auto-creation-parks-not-found-and-repairs-through-an-ensure.md
[ADR-0074]: 0074-the-bigtable-writer-pools-a-batcher-per-table-over-a-client-per-instance.md
[ADR-0075]: 0075-the-spanner-sink-batch-writes-and-owns-the-whole-retry-loop.md
[ADR-0101]: 0101-the-spanner-change-stream-reader-bounds-asynchronous-partition-queries.md
[ADR-0103]: 0103-the-bigtable-change-stream-reader-bounds-asynchronous-partition-reads.md
[ADR-0131]: 0131-spanners-two-batch-seams-keep-their-own-lazy-handle-lifecycle.md
[ADR-0137]: 0137-a-cross-connector-name-diverges-only-to-name-a-real-difference.md
