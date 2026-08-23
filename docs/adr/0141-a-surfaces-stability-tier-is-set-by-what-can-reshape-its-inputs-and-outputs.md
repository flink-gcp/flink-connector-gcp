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

# ADR-0141: A surface's stability tier is set by what can reshape its inputs and outputs

- Status: Accepted
- Date: 2026-08-23 (measured 2026-08-23)
- Issues: [#783](https://github.com/flink-gcp/flink-connector-gcp/issues/783),
  [#728](https://github.com/flink-gcp/flink-connector-gcp/issues/728),
  [#706](https://github.com/flink-gcp/flink-connector-gcp/issues/706)
- Modules: all
- Current behavior: `docs/content/_index.md` § API reference

## Context

[ADR-0124](0124-the-stability-boundary-at-1-0-0-is-a-promoted-public-entry-surface-checked-by-japicmp.md)
computed the `@Public` promotion mechanically, and
[#783](https://github.com/flink-gcp/flink-connector-gcp/issues/783) asked, per type, whether the
youngest promoted surfaces — the BigQuery CDC types and the Bigtable and Spanner change-stream
types — are shapes this project is prepared to evolve only through deprecate-and-add within 1.x.
Answering it needed a rule, and the candidate rule was annotation-shaped: a surface built on a
vendor `@BetaApi` would be `@PublicEvolving`, an Alpha one `@Experimental`.

Measuring the vendor annotations against the `libraries-bom` 26.86.0 pins falsified the literal
form of that rule twice over. First, the change-stream surfaces are not in fact Beta-backed: the
Bigtable change-stream client and model classes carry
`@InternalApi("Intended for use by the BigtableIO in apache/beam only.")` — a weaker promise than
Beta — while the Spanner change-stream path calls no vendor change-stream API at all (it issues
plain SQL over the change-stream table-valued function), and BigQuery CDC is a schema-level
convention (`_change_type` proto fields) with no client surface. All three *services* are GA.
Second, `@BetaApi` clients are called internally throughout the repository, under surfaces whose
services are long GA: the whole Storage Read source path runs through the class-level `@BetaApi`
`BigQueryReadClient`, and the client-owned emulator routes run through the `@BetaApi`
`setChannelConfigurator`. A vendor client annotation is a fact about the client library's
willingness to change its Java surface, not about the service, and a rule keyed on it would demote
most of what ADR-0124 promoted.

## Decision

**A surface's stability tier is set by what can reshape its inputs or outputs within 1.x, not by
the annotations on the vendor clients it calls through.** Five clauses decide a tier:

- A surface whose service feature is in **Preview or Beta** on the service side is at most
  `@PublicEvolving`: the service may still move, and a minor release is the promised channel for
  following it.
- A surface whose service feature is **Alpha**, or whose shape is blocked on an **unresolved
  upstream issue**, is `@Experimental`: the inputs or outputs are known-unsettled, and the tier
  says so where the reader decides whether to depend on it.
- A surface that **mirrors or exposes a vendor type the vendor marks unstable**
  (`@BetaApi`, `@InternalApi`) in its own public API is at most `@PublicEvolving`: the vendor is
  free to reshape what the mirror mirrors, and freezing the mirror would promise what the vendor
  has not.
- A vendor Beta or Internal API that is **only called internally** never sets the tier. It is
  recorded — in this ADR's inventory and in the module reference — the way
  [ADR-0041](0041-the-bigtable-sink-is-implemented-on-four-checked-sdk-facts.md) and
  [ADR-0129](0129-the-cloud-tasks-sink-keeps-one-create-rpc-per-record-and-declines-v2beta3-batchcreatetasks.md)
  record theirs, and a bump that breaks it is an internal repair, invisible through the connector
  API.
- A surface that has **not yet survived a published release under real use** may be held at
  `@PublicEvolving` even when no other clause applies. Youth never forces a demotion — it only
  permits holding a new surface one tier below the freeze until its first release — and the
  choice is recorded per surface, here or in the adopting ADR, so the next reader sees a decision
  rather than a precedent.

A demotion under this rule still obeys ADR-0124's closure: a type named in a `@Public` signature
goes down only to `@Experimental`, as a recorded closure stop; a group with no outside `@Public`
referrer may go down to `@PublicEvolving` whole.

**Applied to [#783](https://github.com/flink-gcp/flink-connector-gcp/issues/783):**

| Surface | Tier | Reason |
| --- | --- | --- |
| BigQuery CDC (`CdcOptions`, `CdcChangeType(Provider)`, `CdcSequenceNumberProvider` + 4 implementations, `SpannerCdcSequenceNumber`, `CdcTableOptions(Provider)`, `CdcTableReconciliationPolicy`) | `@Experimental` | [#706](https://github.com/flink-gcp/flink-connector-gcp/issues/706) is open and its resolution shapes what the sequence-number providers receive; the surface is named by `@Public` `BigQuerySinkBuilder` setters, so `@Experimental` is the closure-coherent way down |
| Bigtable change stream (`BigtableChangeStreamSource(Builder)`, the deserialization schemas, `BigtableChangeStreamMutation` and its nested types) | `@PublicEvolving` | our record model mirrors the vendor's `@InternalApi` change-stream model, which is still growing (`AddToCell`/`MergeToCell` are recent, the aggregate `Value` is `@BetaApi`), and a new abstract accessor on a `@Public` type is a japicmp break ([ADR-0126](0126-bigtable-change-stream-entries-dispatch-through-a-package-private-visitor.md)) |
| Spanner change stream (`SpannerChangeStreamSource(Builder)`, the deserialization schema, `DataChangeRecord`, `Mod`, `ModType`, `ValueCaptureType`) | `@PublicEvolving` | the youth clause, alone: the service is GA and no vendor API is involved, but the surface is weeks old, has not survived a release under real use, and its record model is documented as governed together with Bigtable's |
| Everything else ADR-0124 promoted, including `BigQuerySinkBuilder`, `BigQuerySink`, `BigtableSource`, `SpannerSource` and the Storage Read source surface | `@Public` (unchanged) | the services are GA and the Beta client surfaces underneath are internal calls, which the fourth clause keeps out of the tier decision |

The Table API half of the CDC surface — the `sink.cdc.*` option keys — stays on the
`@PublicEvolving` `BigQueryConnectorOptions` with every other BigQuery option: the class-level
annotation cannot split per field, the SQL contract is the option-key strings held by
`check-option-docs` rather than the Java binary API, and a CDC option that must follow
[#706](https://github.com/flink-gcp/flink-connector-gcp/issues/706) moves at a minor release
with a release-notes entry, which the table page's CDC section states to its readers.

## Evidence

The vendor annotations were read from the `-sources.jar` files of the `libraries-bom` 26.86.0
pins (google-cloud-bigtable 2.81.0, google-cloud-spanner 6.120.0, google-cloud-pubsub 1.153.0,
google-cloud-tasks 2.95.0, google-cloud-bigquerystorage 3.31.0, google-cloud-bigquery 2.69.0,
google-cloud-core 2.73.0, gax 2.83.0). The inventory of unstable vendor surfaces in use by main
trees, all internal calls under the fourth clause:

- **base**: `InstantiatingGrpcChannelProvider.Builder.setChannelConfigurator` is `@BetaApi`
  (`EmulatorChannels.plaintextProvider` — the five client-owned emulator sites of ADR-0081's
  split; the three caller-owned `openPlaintextChannel`/`fixedProvider` sites do not touch it).
- **bigquery**: `BigQueryReadClient` and `BigQueryReadSettings` are class-level `@BetaApi`
  (`BigQueryReadClients`, `ReadClientSessionCreator` — the whole Storage Read source path;
  the Storage Read API service is GA); `TimePartitioning.Builder.setField` and
  `TableInfo.getLabels`/`setLabels` are method-level `@BetaApi` (`BigQueryTableAdmin`,
  `BigQueryCdcTableService`). The Storage Write path (`StreamWriter`, `BigQueryWriteClient`,
  `JsonToProtoMessage`) carries no Beta annotation.
- **pubsub**: `AckReplyConsumerWithResponse` is class-level `@BetaApi` (the exactly-once
  delivery preview; `AckHandle`, `DefaultSubscriberFactory`), and
  `Subscriber.Builder.setSubscriberShutdownSettings` is method-level `@BetaApi`
  (`DefaultSubscriberFactory`; the settings type itself is unannotated).
- **cloudtasks**: `ChannelPoolSettings` is class-level `@BetaApi`
  (`DefaultTaskCreatorFactory`; the knob
  [ADR-0134](0134-the-cloud-tasks-channel-pool-is-an-explicit-knob-defaulting-to-the-clients-single-channel.md)
  is built on).
- **bigtable**: `BigtableDataClient.newBulkMutationBatcher(TargetId)` and
  `BigtableBatchingCallSettings` are `@BetaApi` and `RowMutationEntry.toProto()` is
  `@InternalApi` (already recorded by ADR-0041); the change-stream client methods
  (`readChangeStreamAsync`, `generateInitialChangeStreamPartitions`) and model classes are
  `@InternalApi` and the aggregate `Value` model is `@BetaApi` (already recorded by
  [ADR-0097](0097-the-bigtable-change-stream-coordinator-checkpoints-the-partition-ledger.md));
  `GCRules` is class-level `@BetaApi` (`BigtableTableAdmin`); the admin methods the module
  calls — `createTable`, `getTable`, `modifyFamilies` on the sink's ensure, `getTable` and
  `getAppProfile` on the change-stream preflight — are `@ObsoleteApi` (a deprecation-adjacent
  tier, not an instability one).
- **spanner**: `com.google.cloud.ByteArray` and `com.google.cloud.Date` are class-level
  `@BetaApi` (`RowDataToSpannerValueConverter`, `DataChangeRecordToRowDataConverter`); every
  Spanner client surface the module calls (`DatabaseClient.batchWriteAtLeastOnce`,
  `BatchClient`, `Statement`, the option types) is GA.

The reference graph behind the closure claims: no `@Public` type outside the two change-stream
groups names a change-stream type, so the whole-group `@PublicEvolving` demotion leaves no frozen
signature pointing below the line; the CDC group is named by four `BigQuerySinkBuilder` setters,
which the ADR-0124 revision records as closure stops.

## Alternatives declined

- **The literal rule, "uses a vendor `@BetaApi` ⇒ `@PublicEvolving`"**: the measurement above
  shows it demotes the BigQuery Storage Read source, the Pub/Sub exactly-once path, the Cloud
  Tasks sink, and every emulator-capable surface — most of the ADR-0124 promotion — for client
  Java-surface caution that says nothing about the service or about this project's own API shape.
- **Keying the tier on service maturity alone**: it answers nothing for #783, because all three
  services in question are GA; the youth of a shape and an open upstream issue are what made
  these surfaces demotion candidates, so the rule must name those inputs too.
- **No rule, deciding each surface by taste**: #783 itself shows the cost — without a rule the
  per-type table has no column for "why", and the next young surface re-litigates the same
  question from nothing.
- **Demoting types inside `@Public` signatures to `@PublicEvolving`**: reopens the mixed-tier
  signatures ADR-0124's closure exists to prevent; `@Experimental` closure stops are the recorded
  exception, so a demotion that cannot take its referrers with it goes there or nowhere.

## Consequences

- 24 main-tree files flip annotations (12 change-stream files to `@PublicEvolving`, 12 CDC files
  to `@Experimental`); the ADR-0124 revision carries the census deltas and the new closure stops.
- Neither japicmp profile compares the `@Experimental` CDC types themselves, and the
  change-stream surfaces remain only patch-checked, so within 1.x the change-stream shapes may
  move at minors with release-notes entries and the CDC members may follow
  [#706](https://github.com/flink-gcp/flink-connector-gcp/issues/706) — but not freely: the four
  frozen `BigQuerySinkBuilder` setters still name `CdcOptions`, `CdcTableOptions`,
  `CdcTableOptionsProvider` and `CdcTableReconciliationPolicy`, so renaming one of those types,
  or changing a setter's signature, trips japicmp through the frozen builder (the narrowed
  latitude ADR-0124's closure stops record).
- `docs/content/_index.md` § API reference states the rule's user-facing half, so a reader can
  predict a surface's tier from what it exposes.
- Adopting a Preview/Beta service feature now carries a tier decision at introduction time, in
  the adopting ADR, instead of inheriting whatever tier the surrounding surface has.
- The inventory above is point-in-time at `libraries-bom` 26.86.0; a BOM bump can move vendor
  annotations in either direction, and a bump that promotes a mirrored vendor surface to GA is
  the trigger for reconsidering the mirror's own tier.
