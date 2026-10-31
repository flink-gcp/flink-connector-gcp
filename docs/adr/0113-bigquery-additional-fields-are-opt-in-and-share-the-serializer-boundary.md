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

# ADR-0113: BigQuery additional fields are opt-in and share the serializer boundary

- Status: Accepted
- Date: 2026-08-14; refined 2026-10-31 ([#1950], [#1954])
- Issues: [#641](https://github.com/flink-gcp/flink-connector-gcp/issues/641), [#1950], [#1954]
- Modules: bigquery
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md#additional-physical-fields`

## Context

A caller may need operational columns such as an ingestion source, processing timestamp or stable
identifier even when those values are not part of the record serializer's input schema.
Putting each value into the application serializer duplicates table-schema, protobuf-descriptor and
row-conversion work.
Adding a protobuf field only to the bytes is unsafe because the
[Storage Write API reports extra fields as a schema mismatch](https://docs.cloud.google.com/bigquery/docs/write-api-best-practices#handle_schema_updates).

The sink has three write methods that consume the same serializer contract through different
boundaries.
Default and buffered streams send protobuf rows directly, while file loads parse the protobuf row
and stage it as Avro or Parquet.
Table auto-creation and schema reconciliation must therefore see the same additional fields as every
row path.

BigQuery's
[CDC contract](https://docs.cloud.google.com/bigquery/docs/change-data-capture#specify_changes_to_existing_records)
already defines two write-only pseudocolumns that the connector adds to the default-stream
descriptor.
Those names have a documented BigQuery write contract and must not become physical table columns;
ordinary additional fields have no such exception.

## Decision

**Additional physical fields are fully opt-in on `BigQuerySinkBuilder`.**
When neither `additionalFields(...)` nor the existing `cdcOptions(...)` decorator is configured,
the configuration delegates the table schema, descriptor, fingerprint and serialized row directly
to the configured serializer.
Omitting `additionalFields(...)` by itself adds no physical fields or provider calls; independently
configured CDC metadata retains its established write-only behavior.
Configuring it supplies an ordered, job-graph-serializable list of `AdditionalField` declarations.

**The public declarations live beside the common serializer SPI.**
`ProtoMessageSerializationSchema`, `AvroRecordSerializationSchema` and `JsonDocumentSerializationSchema` all produce the
`TableSchema`, protobuf descriptor and protobuf row bytes defined by `BigQueryProtoSerializationSchema`.
The sink adds fields after that common boundary, so the public `AdditionalField` types belong in
`sink.serializer`, not in a format-specific package or a generic utility package.
Each provider still receives the original serializer input element.

**Each additional field is a physical singular scalar column.**
A declaration owns a protobuf-compatible name, one explicit BigQuery type, a `NULLABLE` or
`REQUIRED` policy and a serializable provider over the original element.
The supported types are `BOOL`, `BYTES`, `DATE`, `DATETIME`, `DOUBLE`, `GEOGRAPHY`, `INT64`,
`NUMERIC`, `BIGNUMERIC`, `STRING`, `TIME`, `TIMESTAMP` and `JSON`.
Repeated values, records, `RANGE`, `INTERVAL` and flexible column names are outside this API.

**One post-serializer protobuf engine owns physical fields and BigQuery's allowlisted write-only
fields.**
Physical fields extend the `TableSchema`, write descriptor and serialized row for all three write
methods.
Write-only fields can be constructed only from the internal allowlist containing
`_CHANGE_TYPE` and `_CHANGE_SEQUENCE_NUMBER`; they extend the write descriptor and row but never
the table schema.
The engine preserves descriptor dependencies and reserved ranges, and derives a fresh augmented
schema and descriptor whenever the serializer's schema fingerprint changes for a destination.

**The serializer's decision precedes every provider.**
A `null` serialized row remains a skip and invokes no additional-field provider.
For an emitted row, providers run in declaration order.
A `null` value omits a `NULLABLE` field and fails a `REQUIRED` field; provider exceptions and values
of the wrong Java type follow the existing failed-row path and name the field.
Schema and descriptor collisions fail before that row-level boundary.

**The serializer remains the source of the schema fingerprint.**
Additional-field declarations are immutable for the job graph, so a changing serializer fingerprint
still drives descriptor refresh and schema reconciliation.
Auto-creation and every reconciliation proposal use the extended physical schema.
The existing additive-schema policy decides whether a new additional column can be added to an
existing table; declaring it `REQUIRED` does not bypass BigQuery's widening restrictions.

## Evidence

- Builder tests prove that omission is a no-op and configured options survive job-graph
  serialization for all three write methods.
- Serializer tests cover declaration order, every supported scalar encoding, null policy, provider
  failures, skips, collisions, evolving descriptors, reserved field numbers and the write-only
  allowlist.
- Writer tests parse the actual rows emitted by both Storage Write API paths and the staged Avro
  record produced by `FILE_LOADS`.
- Auto-creation tests prove that the physical additional fields are part of the proposed table
  schema.
- Existing CDC tests run through the shared engine and retain write-only pseudocolumn behavior.

## Alternatives declined

- **Augment only protobuf rows.**
  BigQuery rejects undeclared ordinary fields, and file loads also derive their staging schema from
  the physical table schema.
- **Add fields implicitly.**
  Existing jobs must not gain fields, provider calls or schema changes without requesting them.
- **Expose arbitrary write-only field names.**
  Only BigQuery's documented CDC pseudocolumns have a contract that permits descriptor-only fields.
- **Accept nested or repeated values in the first API.**
  Their Java value contract, descriptor construction and file-load conversion need a separate
  design rather than an untyped recursive convention.
- **Put the feature in the Table API.**
  SQL computed columns belong to Flink's resolved physical schema and do not need a second
  per-record provider surface in this change.

## Consequences

DataStream callers can add ordered operational columns once and use the same declaration with
default streams, buffered streams and file loads.
The option is absent by default, so existing sinks retain their exact serializer behavior.
Configured fields participate in table creation, schema reconciliation, protobuf conversion and
file staging as ordinary BigQuery columns.
CDC keeps its separate default-stream-only API and its two pseudocolumns remain write-only.

## Revision: appended field numbers survive schema evolution (2026-10-31)

[#1950] found that the engine numbered the appended fields from the base descriptor's highest field number, so the numbers followed the base.
Both Storage Write API writers append a row serialized before a fingerprint change through a stream writer rebuilt with the evolved descriptor, without re-encoding it.
The default-stream writer does so for the rows pending at the rebuild and for the failed batches a repair re-sends.
The buffered writer drains the old rows under the old descriptor first, but a schema mismatch during that drain reopens the appender with the serializer's current descriptor and re-sends them under it.
When the base appended a column, as the serializer contract asks, the column took the number an appended field had held and every appended field moved up by one.
Writer tests reproduced it on both writers: a retained CDC row arrived with its `_CHANGE_TYPE` value in the new column and no change type, and a retained row with a `REQUIRED` additional field no longer parsed.

**The appended fields count down from 262,143** (`2^18 - 1`, the highest field number whose tag still encodes in three bytes), in declaration order, skipping the base's own field numbers, protobuf's reserved 19,000 to 19,999, and the message's reserved and extension ranges.
A base that appends its fields after its highest number reaches one of them only once that number nears the ceiling, which a descriptor numbered from one does past 261,000 fields; [a BigQuery table holds at most 10,000 columns](https://docs.cloud.google.com/bigquery/quotas).
A message whose extension range covers the ceiling, such as proto2's `extensions 100 to max`, gets the numbers just below that range, which its own fields reach sooner; the numbering it had before always followed its fields, so this is no regression, and the check below covers it.
A message with no free number at or below the ceiling, for example one reserving `1` to `262,143`, gets the free numbers above both the ceiling and its own highest field, as the old numbering would have given it, rather than a refusal; those numbers follow its highest field, so an evolution that adds a field above it moves them and fails the write through the check below.
The numbers therefore stay fixed across additive evolution, which is what keeps the writer Javadoc's claim, that previously serialized bytes stay valid under the new descriptor, true for the augmented row as well as the serializer's.

**An evolution that still moves an appended field fails the job.**
When a destination's fingerprint changes, the engine compares each appended field's new number with the one it held and throws if they differ, naming the field and the numbers the base must leave as they were.
The writers derive the schema in `prepare`, before the row-failure boundary, so a failure policy does not see the failure when the fingerprint changed between records.
A serializer that changes its fingerprint while serializing a record reaches the derivation inside `serialize` instead; the revision for [#1954] below keeps that failure out of the row-failure boundary as well.
The check is per process: a restarted task holds no retained rows and derives the evolved descriptor afresh, so it does not fail again, while a serializer that reaches the same evolution again after every restart fails each time until its numbering changes.
The comparison does not use the weakly keyed schema cache, whose key is whichever equal destination object derived the entry first: a writer retaining rows need not hold that object, so the entry can be collected while rows remain, for example when a resolver returns a fresh destination per record and the first record was skipped.
The numbers each destination was first given are kept instead in a strongly held map, keyed by the destination's table path so that the cache's weak key stays collectable, and only for serializers that report a fingerprint, the only ones that can evolve.
An entry is a few integers per destination and is not released when a writer evicts the destination.

The alternatives were weighed as follows.

- **Keep the previous numbers per destination and allocate only new ones.**
  The base takes the colliding number in exactly the case at issue, so the appended field would have to move anyway.
- **Re-encode retained rows under the new descriptor.**
  The writers keep bytes, not records, and the default stream's retry and repair paths would each need the old descriptor to decode them.
- **Refuse to rebuild while rows are retained.**
  It would turn every additive evolution on a CDC or additional-fields sink into a job failure.
- **Count down from the top of the field-number space.**
  Tags above `2^25` take five bytes, two more per appended field per row than below `2^18`, for no gain in the case the ceiling already covers.
  The chosen ceiling itself costs up to two bytes per appended field per row over the old numbering, whose tags took one or two bytes below 2,048 base fields; a two-byte ceiling (`2^11 - 1`, 2,047) would sit inside the reach of a table's 10,000 columns.

## Revision: a schema derived while serializing fails the job (2026-10-31)

[#1954] found that the derivation inside `serialize` reported its failures as the record's.
The writers call `prepare` before their row-failure boundary so that a destination-wide schema conflict fails the job.
The engine derives the schema again after the serializer has serialized the record, and that second derivation runs inside the boundary.
A serializer that changes its fingerprint while serializing a record is therefore first seen there, and a conflict in its new schema reached the failure handler as a serialization failure of that record.
Writer tests reproduced it under a recording handler, which, like log-and-drop, lets the write go on: a table-schema name collision on all three write methods, and the field-number check of the revision above on the default stream, the one write method that accepts CDC options. Each time the record was handed to the handler and the write succeeded.
Under log-and-drop or a dead-letter handler, the job failed only when the destination's next record reached `prepare`, and a bounded job whose last record for the destination was that one finished green without it.

**A derivation failure inside `serialize` now fails the job.**
The engine wraps it in `SchemaDerivationException`, and each writer rethrows that type ahead of its row-failure `catch`.
The wrapper names the destination and keeps the derivation's own failure as its cause.
Only a custom serializer can fail there: every built-in one, including the Table API's, reports a `null` fingerprint, which never changes, so it reaches the derivation inside `serialize` only after its cache entry was collected and derives the schema `prepare` just accepted.
A serializer that changes its fingerprint and then skips or fails the record never reaches that derivation; nothing is written under the changed schema, and the destination's next record meets the conflict in `prepare`.

The alternative was weighed as follows.

- **Take the derivation out of `serialize` and repeat `prepare` after it, outside the boundary.**
  The engine adds the appended fields with the descriptor it derives, and their value providers fail per record, so the writers would need two row-failure boundaries around one call that one boundary covers today, in each of the three writers.

[#1950]: https://github.com/flink-gcp/flink-connector-gcp/issues/1950
[#1954]: https://github.com/flink-gcp/flink-connector-gcp/issues/1954
