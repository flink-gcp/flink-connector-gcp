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
- Date: 2026-08-14
- Issues: [#641](https://github.com/flink-gcp/flink-connector-gcp/issues/641)
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
