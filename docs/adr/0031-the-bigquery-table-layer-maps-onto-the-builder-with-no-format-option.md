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

# ADR-0031: The BigQuery table layer maps onto the builder, with no `format` option

- Status: Accepted
- Date: 2026-08-06
- Issues: [#287], [#542] (under [#57]; ADR-0014 holds the shared mapping rules)
- Modules: bigquery (`table`, `table.sink`)
- Current behavior: `docs/content/docs/connectors/table/bigquery.md`

## Decision

The `table` layer is a *mapping* onto `BigQuerySink.builder()`, never a second implementation —
the Pub/Sub rules (ADR-0014) apply unchanged. What is this module's own:

- **There is no `format` option**, the one deliberate divergence from the Pub/Sub layer's shape:
  a Pub/Sub message has an opaque payload so a `SerializationFormatFactory` decides its bytes,
  while a BigQuery row is structured and the DDL schema *is* the schema.
- Adding `toString()` to `WriteMethod` and `CreateDisposition` changed the builder's three
  `"(write method is %s)"` messages, which now pass `name()` — they name
  `WriteMethod.FILE_LOADS` in the same sentence, so the two spellings must not mix.
  `LoadJobSpec.toString()` uses Google's `JobInfo.CreateDisposition`, not ours, and the
  deterministic FILE_LOADS job id hashes destination and URIs only, so neither is affected.
- **`RowDataSerializer` is `@Internal` in `table.sink`, not a public `sink.serializer.rowdata`
  family member**: promotion is cheap later (nothing is published), and starting internal keeps
  the new Flink-type mapping out of the API-tier audit surface until it has settled. Its schema
  options are the `@Internal` `RowDataSchemaOptions` rather than a reused `AvroSchemaOptions`.
- **Two mapping rows are measured rather than inherited.** `TIMESTAMP` → `DATETIME` and
  `TIMESTAMP_LTZ` → `TIMESTAMP`, the opposite of the Dataproc connector, which stores a
  wall-clock value as an instant and vice versa. And `TIME(p)` is rejected above **p = 3, not
  the 6 the [#57] design table states**: `RowData` carries a time of day as an `int` of
  *milliseconds* (its own javadoc table, read off flink-table-common 2.2.1), so a `TIME(6)`
  column could only ever be filled to millisecond precision, and a schema claiming more than the
  values can carry is worse than a rejection. Related, also measured: Flink caps `DECIMAL`
  precision at 38, so **no SQL decimal can reach the BIGNUMERIC rejection** — the bound stays in
  the converter as the invariant it shares with the Avro path, and a test pins that nothing
  reaches it.
- **A marked `ROW` is rendered as JSON text** (`RowDataJsonRenderer`), which the Avro path has
  no counterpart for — its JSON marker is string-only, and only the protobuf path prints a
  message. Decided with the user against matching Avro. The renderer is a plan built from the
  column's `LogicalType`, so an unrenderable nested type (a `MULTISET`, a map with non-string
  keys) fails at graph construction rather than per record; `flink-json`'s
  `RowDataToJsonConverters` was declined as a dependency — a format module on the connector
  core, plus `@Internal` Flink types needing api-tier entries. A marked `STRING` still goes
  through verbatim and unvalidated.
- `PARTITIONED BY` is **rejected, not consumed** — no `SupportsPartitioning`, so the clause
  fails at plan time rather than being silently ignored; ingestion-time partitioning has no
  column to name, so the clause could never have covered the whole feature.
- `perDestinationMetrics` has a `ConfigOption` since this landed.
- **`service-account-key-file` remains a direct builder mapping.**
  The table layer carries the path to `serviceAccountKeyFile(...)`; it does not parse credential
  JSON or place a credential object in the job graph.
  Blank paths and combinations with either credential-free emulator endpoint fail in the factory
  with DDL option names, while the builder keeps the equivalent DataStream checks.

[#57]: https://github.com/laughingman7743/flink-connector-gcp/issues/57
[#287]: https://github.com/laughingman7743/flink-connector-gcp/issues/287
[#542]: https://github.com/laughingman7743/flink-connector-gcp/issues/542
