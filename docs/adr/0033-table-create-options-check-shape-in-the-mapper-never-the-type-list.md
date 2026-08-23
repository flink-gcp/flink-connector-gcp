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

# ADR-0033: `sink.table-create.*` checks shape in the mapper, never a clonable type list

- Status: Accepted
- Date: 2026-08-06
- Issues: [#289] (under [#57]; the missing-table masking that issue also produced is ADR-0030)
- Modules: bigquery (`table.sink`)
- Current behavior: `docs/content/docs/connectors/table/bigquery.md`

## Decision

The four `sink.table-create.*` keys map onto `TableCreateOptions` through
`TableCreateOptionsMapper`, under ADR-0031's rules. `TimePartitioningType` gained the
`toString()` its siblings have; `BigQueryTableAdmin` bridges to the client library with
`Type.valueOf(name())`, so the constant names stay the contract and only the DDL spelling is
new.

- **The mapper owns seven rejections, and one of them has no builder backstop at all**: a
  `time-partitioning.field` without a `.type` is unrepresentable through the builder's two
  `timePartitioning` overloads, so there is no exception to inherit and this check is the only
  thing between a DDL and a silently unpartitioned table. Two more restate a rule the *builder*
  also has, in option keys a SQL user can act on (creation settings beside an explicit
  `create-never`; an expiration without a granularity). The remaining four restate a rule only
  the *service* has, and the builder could not make any of them because it never sees a schema:
  a column the DDL does not declare, a partitioning column of a type BigQuery cannot partition
  on, `hour` over a `DATE` column, and a repeated or nested clustering column.
- **The column check is a check only this layer can make**: in SQL the DDL *is* the created
  table's schema, while the DataStream API takes its schema from the serializer per destination
  and does not have it when the options are configured. Matching is **case-insensitive** —
  `RowTypeToTableSchemaConverter` already rejects columns differing only by case, so it is
  unambiguous, and it cannot refuse a table BigQuery would have created whichever way the
  service resolves the name; the value reaches the builder unchanged rather than being
  normalised.
- **The line is shape versus type list, and the first draft drew it in the wrong place.**
  Checked: existence; the three types time-unit partitioning is defined over (`DATE`,
  `TIMESTAMP`, `DATETIME` — here Flink's `DATE`, `TIMESTAMP_LTZ`, `TIMESTAMP`); that a `DATE`
  column has day, month and year granularity only; and "top-level, non-repeated" for
  clustering. None of those can grow — they are the shape of the feature. Not checked: the
  **clusterable scalar type list**, which has grown before (`RANGE`), so a copy here would
  eventually refuse a table BigQuery would create — a false rejection being worse than the late
  true one it prevents. A `DOUBLE` or `TIME` clustering column therefore still reaches the
  service. The declined-because-it-moves argument covers only the scalar list; using it to wave
  off the structural rules is what left `ARRAY` and `hour`-on-`DATE` unguarded until review.
- The reflective option-completeness test keys setter **names** to a *list* of options, unlike
  its two siblings' one-to-one maps: `timePartitioning` is overloaded, so one name carries both
  the granularity and the column. `BigQueryDynamicSinkTest`'s eleven positional arguments were
  replaced by a named-argument holder plus a reflective check that every field of the sink is
  actually varied.

[#57]: https://github.com/flink-gcp/flink-connector-gcp/issues/57
[#289]: https://github.com/flink-gcp/flink-connector-gcp/issues/289
