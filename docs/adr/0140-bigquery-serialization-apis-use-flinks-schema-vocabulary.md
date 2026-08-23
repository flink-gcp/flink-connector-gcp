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

# ADR-0140: BigQuery serialization APIs use Flink's schema vocabulary

- Status: Accepted
- Date: 2026-08-23
- Issues: [#1048](https://github.com/flink-gcp/flink-connector-gcp/issues/1048)
- Modules: bigquery
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md` sections
  "Protobuf messages", "Avro records", "JSON records", and "Deserialization"

## Context

ADR-0055 declined renaming `BigQueryProtoSerializer` when the input-format packages were split.
The measured cost was churn in the sink core, writers, and about twenty tests, while the class's Javadoc already explained that every input format produced protobuf wire rows.

The cross-connector naming review in [#1043](https://github.com/flink-gcp/flink-connector-gcp/issues/1043) added evidence that ADR-0055 did not weigh.
BigQuery alone named its public conversion SPIs `*Serializer` and `*Deserializer`; Pub/Sub, Cloud Tasks, Bigtable, and Spanner use `*SerializationSchema` and `*DeserializationSchema` for the same user role.
The BigQuery table layer carried the same isolated vocabulary internally.

The timing also changed the cost comparison.
The six user-facing types in this rename are `@Public`, while the two table-layer types are
`@Internal`.
No connector artifact has been published, and the japicmp reference remains unavailable until
1.0.0.
Renaming before that release changes source in the repository and its examples; renaming afterwards would break the frozen public entry surface.

This ADR supersedes only ADR-0055's declined serializer-name alternative.
Every other package, layer, and implementation-naming decision in ADR-0055 remains accepted and is incorporated here by reference.

## Decision

BigQuery's user-implemented conversion SPIs use Flink's `SerializationSchema` and `DeserializationSchema` vocabulary, and the ready-made format facades use the same suffixes:

| Previous name | Current name |
| --- | --- |
| `BigQueryProtoSerializer` | `BigQueryProtoSerializationSchema` |
| `BigQueryRowDeserializer` | `BigQueryRowDeserializationSchema` |
| `ProtoMessageSerializer` | `ProtoMessageSerializationSchema` |
| `AvroRecordSerializer` | `AvroRecordSerializationSchema` |
| `JsonDocumentSerializer` | `JsonDocumentSerializationSchema` |
| `RowDataSerializer` | `RowDataSerializationSchema` |
| `RowDataDeserializer` | `RowDataDeserializationSchema` |

`JsonDocumentSerializerOptions` becomes `JsonDocumentOptions`.
The object controls JSON conversion's unknown-field policy rather than schema derivation, so `JsonSchemaOptions` would misstate its role and the facade-derived `JsonDocumentSerializationSchemaOptions` would preserve the word the rename removes from its siblings.

The packages remain `sink.serializer` and `source.serializer` because they name the conversion concern rather than a public type.
Methods such as `serializer(...)`, `deserializer(...)`, `serialize(...)`, and `deserialize(...)` remain unchanged because they name actions and values rather than SPI types.
Internal implementation helpers and Flink state serializers keep `Serializer` or `Deserializer` where that word describes their actual role.

The previous public classes are removed without deprecated forwarding aliases.
No published artifact can refer to those class names, and retaining both vocabularies would turn a pre-release correction into permanent duplicate API.

## Evidence

The [#1043](https://github.com/flink-gcp/flink-connector-gcp/issues/1043) concept-to-name table measured the five connector modules and classified the public SPI and internal table-layer names as one divergence.
The renamed BigQuery types retain the same annotations, signatures, factory methods, serialization identifiers, and implementations; the module tests and compiled documentation examples therefore exercise the same code paths under the new names.

## Alternatives declined

**Keep the existing names because the implementation already works.**
The names are the only connector-level divergence for this shared role, and the change becomes materially more expensive once 1.0.0 freezes the `@Public` surface.

**Keep deprecated aliases for source compatibility.**
There is no published source or binary contract to preserve, while an alias would expose two names for one SPI and would itself join the frozen surface.

**Rename the serializer packages too.**
ADR-0055's package skeleton remains coherent: the packages group record conversion, while the class suffix identifies the public SPI role.

## Consequences

Repository source, tests, documentation, and examples use the new names together.
The change does not alter row conversion, schema derivation, skip behavior, failure handling, or connector-managed checkpoint layouts.
Source code written against an unpublished checkout must update imports and type names; no released job can contain the removed classes.
