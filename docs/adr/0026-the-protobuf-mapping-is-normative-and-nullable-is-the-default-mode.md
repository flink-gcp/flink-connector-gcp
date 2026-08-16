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

# ADR-0026: The protobuf mapping is normative for every serializer, and `NULLABLE` is the default mode

- Status: Accepted
- Date: 2026-07-26 ([#124] Part 1; [#145] moved Avro onto the same default the same day)
- Issues: [#124], [#145]
- Modules: bigquery (`sink.serializer.*`)
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md` § Column modes,
  § Nullability

## Decision

`ProtoToTableSchemaConverter` derives the mode from presence only under
`ProtoSchemaOptions.Builder.deriveRequiredColumns()`, and the default stays **`NULLABLE`**.
Reasons, in order of weight: proto3's presence-less form is the spelling you get by *not*
thinking about nullability, so deriving `REQUIRED` from it by default would make nearly every
scalar column of an auto-created table `REQUIRED` on the strength of a syntax default; and
`REQUIRED` is the mode BigQuery cannot walk back.

**This mapping is normative for every serializer** — every write path goes through a protobuf
row (`STORAGE_API_*` directly; the Avro and JSON serializers via
`BQTableSchemaToProtoDescriptor`; FILE_LOADS converts that same row into the file it stages) —
so **[#145] moved Avro onto this default and this method name**, rather than the reverse, and
both serializers take `deriveRequiredColumns()` with only the signal differing (a `["null", T]`
union there, presence here). [#145] carried that argument on a claim about the staging format
which [#281] withdrew (ADR-0019 has the measurement); the narrower every-path-is-protobuf claim
stands. **Neither default is to be flipped per format again**: that is the whole point of the
two agreeing. This supersedes the "not symmetric on purpose" reasoning first recorded on
[#124], which weighed Avro-schema faithfulness in isolation.

- There is **no inverse switch on either side** — `allFieldsNullable()` was removed from Avro by
  [#145] and never added here: with a `NULLABLE` default it would mean exactly "don't call the
  opt-in", and two inverse switches need a documented meaning per combination.
- **The name went through two rejected candidates**, so don't re-open it:
  `deriveRequiredFromPresence()` names a protobuf mechanism and cannot be shared with Avro;
  `deriveRequiredFromSchema()` was worse — *everything* here is derived from the schema, so the
  qualifier distinguished nothing. `deriveRequiredColumns()` names what appears on the BigQuery
  side, where the irreversibility lives, and matches the `allowNewFields()` /
  `allowFieldRelaxation()` vocabulary borrowed from Aiven's connector. Getter is
  `isDeriveRequiredColumns()` — house style. The polarity is a **deliberate deviation** from
  that connector, whose `allBQFieldsNullable` defaults to `false`.
- The predicate is `isRequired() || !hasPresence()`, **two clauses because a proto2 `required`
  field has presence and is mandatory all the same**; `hasPresence()` is the **full**
  disjunction (`isProto3Optional || MESSAGE || GROUP || isExtension() || containingOneof != null
  || fieldPresence != IMPLICIT`, guarded by `!isRepeated()`) — write it out when reasoning,
  because the `MESSAGE` clause is the one that gets forgotten. `isRepeated()` is tested
  **first**, so a repeated marked field stays `REPEATED JSON`/`REPEATED GEOGRAPHY`; a mutant
  reordering those two lines fails seven tests.
- **A singular marked column is never `REQUIRED`**, stated about the marking rather than about
  presence: `ProtoRowConverter`'s `omitEmptyString` (the [#50] rule) is set to `!hasPresence()`,
  *identical* to the `REQUIRED` trigger, and `BQTableSchemaToProtoDescriptor` builds its row
  descriptor with no syntax → proto2 → `LABEL_REQUIRED` enforced by `build()`, so the pair would
  throw `UninitializedMessageException` on every record omitting the field (verified by
  mutation). The broader rule loses fidelity only for a proto2 `required` JSON field, which is
  worth one clause.
- A **proto3** map entry's `key`/`value` have implicit presence and so become `REQUIRED`,
  converging with the Avro path — but that scope is load-bearing: a message-valued map keeps a
  `NULLABLE` value (the `MESSAGE` clause), and in proto2 both entry fields have explicit
  presence and stay `NULLABLE`.
- The **value path is unchanged** — the issue body's claim that this writes `0`/`""` where NULL
  was written before is wrong, since `MessagePlan.convert` skips only on
  `hasPresence() && !hasField()` and a presence-less scalar was already written with its
  default. `SchemaUnifier` needed no change: it only relaxes, so derived-`REQUIRED` against an
  existing `NULLABLE` column is a silent no-op already pinned by `modesAreNeverTightened`.

[#50]: https://github.com/laughingman7743/flink-connector-gcp/issues/50
[#124]: https://github.com/laughingman7743/flink-connector-gcp/issues/124
[#145]: https://github.com/laughingman7743/flink-connector-gcp/issues/145
[#281]: https://github.com/laughingman7743/flink-connector-gcp/issues/281
