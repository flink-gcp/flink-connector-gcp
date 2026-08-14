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

# ADR-0110: BigQuery CDC augments default-stream rows without changing the table schema

- Status: Accepted
- Date: 2026-08-13
- Issues: [#65](https://github.com/laughingman7743/flink-connector-gcp/issues/65),
  [#625](https://github.com/laughingman7743/flink-connector-gcp/issues/625)
- Modules: bigquery
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md#change-data-capture`

## Context

BigQuery change data capture consumes `UPSERT` and `DELETE` mutations through the Storage Write
API default stream.
Each protobuf row carries a required `_CHANGE_TYPE` pseudocolumn and may carry a
`_CHANGE_SEQUENCE_NUMBER` pseudocolumn that orders mutations for one primary key.
These values describe the mutation rather than physical table columns.

The existing sink delegates physical schema derivation and row conversion to a
`BigQueryProtoSerializer`.
That contract supports fixed and dynamic destinations, derives the descriptor before opening a
write stream, and treats a `null` serialized row as a skip.
CDC must preserve those boundaries because the same serializer supplies table auto-creation and
schema reconciliation.

## Decision

**CDC is an optional serializer layer for the at-least-once default-stream writer.**
`CdcOptions` requires a serializable change-type provider and accepts an optional serializable
sequence-number provider.
The builder rejects the options with buffered exactly-once streams and file loads because BigQuery
accepts CDC mutations only through the default stream.

**The serializer remains the sole source of the physical `TableSchema`.**
CDC uses the shared post-serializer protobuf engine introduced by ADR-0113, but its two fields are
the engine's allowlisted write-only case.
For the write stream, the engine copies the serializer's protobuf file descriptor and adds the
pseudocolumn fields to the selected row message without adding physical columns.
It preserves nested messages and file dependencies, allocates field numbers after the physical
fields while skipping protobuf and message-specific reserved ranges, and caches each augmented
descriptor by the serializer's schema fingerprint.
A changed fingerprint therefore produces a new augmented descriptor without adding CDC
pseudocolumns to schema reconciliation; serializers with evolving descriptors must update that
fingerprint as required by the serializer contract.

**The serializer's skip decision precedes CDC metadata extraction.**
A `null` serialized row invokes neither provider and follows the existing skip contract.
Every emitted row must have a non-null change type, and every emitted row must have a non-null
sequence when the sequence provider is configured.
A sequence has one to four slash-separated hexadecimal sections of at most 16 digits each and is
canonicalized to uppercase.
Provider failures and invalid row metadata use the existing row-failure path.

**CDC descriptor conflicts remain configuration failures.**
The default-stream writer derives the augmented descriptor after resolving the destination but
before entering row-failure handling or opening destination state.
A physical field that case-insensitively matches either pseudocolumn therefore fails the write
instead of being dropped or dead-lettered once per record.

## Evidence

- Serializer tests cover both change types, optional and canonical sequences, invalid and null
  metadata, serializer skips, field-number reservations, nested descriptors, pseudocolumn
  collisions and job-graph serialization.
- Builder tests cover CDC option propagation and rejection by incompatible write methods.
- Default-stream writer tests parse appended CDC bytes with the exact per-destination augmented
  descriptor and prove that descriptor conflicts fail before serialization or stream creation.

## Alternatives declined

- **Add the pseudocolumns to the serializer's `TableSchema`.**
  This would make table creation and schema reconciliation treat BigQuery write metadata as
  physical columns.
- **Require callers to build the pseudocolumns into each serializer.**
  This would duplicate descriptor mutation, sequence validation and skip ordering across input
  formats and source systems.
- **Represent change types as arbitrary strings.**
  A typed enum rejects unsupported mutations before a Storage Write API request.
- **Allow a missing sequence from individual rows.**
  One sink-wide choice keeps the stream descriptor stable and prevents accidental mixing of
  ordered and unordered mutations.
- **Route descriptor conflicts through the failed-row handler.**
  Drop and dead-letter policies could then hide a destination-wide schema error while every row is
  rejected.

## Consequences

DataStream callers can supply CDC mutations independently of their physical serializer and can use
the same API with dynamic destinations and evolving descriptors.
The destination must already have a BigQuery primary key because the current table-creation API
cannot declare one or configure `max_staleness`; #627 owns that work.
Flink SQL changelog conversion and Debezium PostgreSQL, MySQL and Spanner metadata mappings remain
separate layers tracked by #626, #629, #631 and #633.
