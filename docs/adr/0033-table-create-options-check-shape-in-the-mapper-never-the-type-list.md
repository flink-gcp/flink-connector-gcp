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
- Date: 2026-08-06; refined 2026-11-16 ([#2169]) and 2026-11-17 ([#2175], [#2182], [#2183])
- Issues: [#289] (under [#57]; the missing-table masking that issue also produced is ADR-0030),
  [#2169], [#2175], [#2182], [#2183]
- Modules: bigquery (`table.sink`; the reading revision, `table.source`)
- Current behavior: `docs/content/docs/connectors/table/bigquery.md`

## Decision

The four `sink.table-create.*` keys map onto `TableCreateOptions` through
`TableCreateOptionsMapper`, under ADR-0031's rules. `TimePartitioningType` gained the
`toString()` its siblings have; `BigQueryTableAdmin` bridges to the client library with
`Type.valueOf(name())`, so the constant names stay the contract and only the DDL spelling is
new.

- **The mapper owns seven rejections, and two of them have no builder backstop at all**: a
  `time-partitioning.field` without a `.type` is unrepresentable through the builder's two
  `timePartitioning` overloads, so there is no exception to inherit and this check is the only
  thing between a DDL and a silently unpartitioned table; and creation settings beside an explicit
  `create-never` are accepted by `build()`, which takes `tableCreateOptions(...)` under either
  disposition. One more restates a rule the *builder* also has, in option keys a SQL user can act
  on (an expiration without a granularity). The remaining four restate a rule only
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

## Revision: a partition filter requirement needs a granularity (2026-11-16)

[#2169] added `sink.table-create.require-partition-filter`, which maps onto
`TableCreateOptions.requirePartitionFilter(boolean)`.
`build()` rejects a requirement without time partitioning, because BigQuery would drop it silently:
on 2026-11-16 a `tables.insert` request carrying `requirePartitionFilter: true` and no partitioning
created the table, and `tables.get` returned it without the field, while the same request with day
partitioning kept it.
The mapper restates that rejection in option keys, as it does for an expiration without a
granularity, so it now owns eight rejections.
An explicit `false` without a granularity is accepted by both, since it asks for nothing an
unpartitioned table lacks.

## Revision: a partition filter requirement is refused on a CDC table (2026-11-17)

[#2175] added a ninth rejection: `sink.table-create.require-partition-filter = 'true'` together with
`sink.cdc.enabled = 'true'`, which
[ADR-0205](0205-bigquery-cdc-tables-are-not-created-with-a-partition-filter-requirement.md) records.
The sink builder and the CDC provisioning refuse it too, so the mapper's rejection restates theirs in
option keys.
It runs before the granularity checks, because adding a granularity would not make the combination
valid.

## Revision: the create-never rejection has no builder counterpart (2026-11-17)

The Decision first counted creation settings beside an explicit `create-never` among the rules the
builder also has.
`build()` has no such rule; it accepts `tableCreateOptions(...)` under `CREATE_NEVER` and never
applies them, so the mapper's rejection is the only one ([#2182]).
The bullet above now says so.
The count of seven, and the eight and nine the later revisions reach, are unchanged.

## Revision: reading such a table, and ingestion-time partitioning (2026-11-17)

Two consequences of the partition filter requirement were measured on 2026-11-17 against
`us-central1` ([#2183]), and neither adds a check.

The Storage Read API enforces the requirement on a read session as BigQuery does on a query.
On a day-partitioned table that required it, a session was refused ("Cannot query over table …
without a filter over column(s) … that can be used for partition elimination") with no row
restriction, with one on another column, and with one that joined the partitioning column to
another with `OR`; it read the rows with one on the partitioning column, alone or joined to another
with `AND`.
The connector's Table source therefore starts on such a table only when its combined restriction, a
pushed-down `WHERE` condition or `scan.row-restriction`, constrains the partitioning column.
Without one the job fails when the source starts, in the enumerator's planning call, with that
message as the cause.
A table partitioned on ingestion time takes the restriction on `_PARTITIONTIME`, or on
`_PARTITIONDATE`, which BigQuery defines only for daily partitioning: on 2026-11-18 a query naming it
on an hourly table was refused as an unrecognized name.
The Table source exposes neither as a column, so `scan.row-restriction` supplies it.
The connector does not check any of this before the session: the refusal names the columns it
needs, and refusing such a read is the purpose of the requirement.

A table the sink created with day partitioning and no column kept the requirement under every write
method (default stream, exactly-once, and `FILE_LOADS` appending, `write-truncate` and
`write-truncate-data`), the writes succeeded, and a query without a filter was refused, so the
builder and the mapper accept ingestion-time partitioning with the requirement.
Right after the write, the rows of both Storage Write API methods had a NULL `_PARTITIONTIME`, so a
filter on a recent `_PARTITIONTIME` counted none of them; `_PARTITIONTIME IS NULL` counted both and
satisfies the requirement.
The rows of the load jobs were already in the current day's partition.
A Table source read restricted to a recent `_PARTITIONDATE` right after a default-stream write
returned none of its two rows and no error, while one that added `OR _PARTITIONTIME IS NULL` read
both, which is why the pages tell a reader to add it.

`BigQueryRequirePartitionFilterRealGcpITCase` pins the source's refusals (no restriction, another
column through `scan.row-restriction`, an `OR` with another column) and its pushdown, the
`scan.row-restriction` read of an ingestion-time table, and the requirement on an ingestion-time
table the default stream creates, counted and read with filters that hold whether or not BigQuery
has moved the rows into a partition yet.

[#57]: https://github.com/flink-gcp/flink-connector-gcp/issues/57
[#289]: https://github.com/flink-gcp/flink-connector-gcp/issues/289
[#2169]: https://github.com/flink-gcp/flink-connector-gcp/issues/2169
[#2175]: https://github.com/flink-gcp/flink-connector-gcp/issues/2175
[#2182]: https://github.com/flink-gcp/flink-connector-gcp/issues/2182
[#2183]: https://github.com/flink-gcp/flink-connector-gcp/issues/2183
