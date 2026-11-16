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

# ADR-0205: BigQuery CDC tables are not created with a partition filter requirement

- Status: Accepted
- Date: 2026-11-17
- Issues: [#2175](https://github.com/flink-gcp/flink-connector-gcp/issues/2175), following
  [#2169](https://github.com/flink-gcp/flink-connector-gcp/issues/2169)
- Modules: bigquery
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md` § Table auto-creation;
  `docs/content/docs/connectors/table/bigquery.md` § Table creation

## Context

[#2169] added `TableCreateOptions.requirePartitionFilter(boolean)`
(`sink.table-create.require-partition-filter`), which every table the sink creates takes, including
a table its CDC provisioning creates (ADR-0112).
The gated run after that change merged showed that a CDC table carrying the requirement refuses
queries that filter on the partitioning column too:

> The query cannot be executed because the table … requires partition filter and contains unapplied
> upsert data that is stale based on the max_staleness setting.

Google's
[change data capture documentation](https://cloud.google.com/bigquery/docs/change-data-capture)
gives the mechanism and names the limit.
When `max_staleness` is unset or zero, or the table as of its last background apply is older than
`max_staleness`, a query performs a runtime merge of that table with the unapplied upserts, and
"during a runtime merge, partition pruning is bypassed because BigQuery can't determine in advance
which partitions are affected by unapplied streamed changes".
No filter can then satisfy the requirement, and the same page lists "the `requirePartitionFilter`
table option" among what a CDC table that performs runtime merges "cannot support".
The sink's writes themselves succeed, so the failure surfaces only later, on the queries against the
table.

## Decision

**The sink does not create a CDC table with a partition filter requirement, whatever its
`max_staleness`.**
It refuses the combination before the table is created, at the first point that knows both halves.

- SQL: `TableCreateOptionsMapper` rejects `sink.table-create.require-partition-filter = 'true'`
  together with `sink.cdc.enabled = 'true'` when the table is planned, naming both option keys,
  ahead of its granularity checks.
- DataStream: under `CREATE_IF_NEEDED`, `BigQuerySinkBuilder.build()` rejects fixed
  `tableCreateOptions(...)` that require a partition filter together with `cdcOptions(...)`.
  A `tableCreateOptionsProvider(...)` is resolved only when a table is created, so
  `CdcTableProvisioner` refuses its options there, before `tables.insert`, as it refuses a creation
  under `CREATE_NEVER` or without a declared primary key; the refusal is not a creation request.
- Every message says that a user who accepts the refusals can add the requirement to the table with
  `ALTER TABLE`, because the sink never applies creation options to an existing table.
- `BigQueryRequirePartitionFilterRealGcpITCase` pins the service behavior this rests on: a CDC
  table created with the requirement outside the sink, whose filter prunes it while it is empty,
  refuses both a filtered and an unfiltered query once the sink has written upserts to it under
  `create-never`.

## Evidence

Measured against `us-central1`, one run per row: one write of three upserts through the sink into a
day-partitioned CDC table with the requirement, then a filtered `COUNT(*)` every 30 seconds.

| Date | `max_staleness` | Filtered query |
|---|---|---|
| 2026-11-16 | none | refused for the whole 6 minutes observed |
| 2026-11-17 | 2 minutes | refused for the whole 13 minutes observed |
| 2026-11-17 | 5 minutes | refused until about 4m20s; answered with 0 rows at about 4m20s–4m50s; refused again from about 5m20s; first correct answer (2 rows) at about 12m50s |

The 5-minute run's first background apply came about 13 minutes after the write, and the 2-minute
run saw none within its 13 minutes, so in both runs the apply came later than the interval.
BigQuery does not document that cadence.
The brief 0-row answers in the 5-minute run read a state with none of the write's rows, before any
apply; they are recorded, not explained.

## Alternatives declined

- **Allow the requirement when `max_staleness` is set.**
  With `max_staleness` a filtered query was answered correctly only after a background apply, which
  came after the interval in both runs.
  The connector cannot tell whether a given interval is long enough, and a few minutes of
  freshness, the usual reason for CDC, left the table refusing queries for most of the time
  measured.
- **Allow it and document the refusals.**
  Declined by the owner: the configuration contradicts itself (the requirement exists to refuse full
  scans, and a CDC table's query-time merge is one), and the failure surfaces on every downstream
  query rather than in the job that configured it.
  This differs from leaving a loud service error to the user: the job that set the option never sees
  it.

## Consequences

- A CDC sink cannot create a table that guards itself against full scans; the user adds the guard by
  hand and owns the refusals it brings.
- A non-CDC table, a CDC table that already exists, and a CDC sink under `CREATE_NEVER` are
  unaffected.
- If BigQuery starts pruning partitions during the query-time merge, the pinned gated case fails
  with the filtered query answered, and this decision is revisited rather than left stale.

[#2169]: https://github.com/flink-gcp/flink-connector-gcp/issues/2169
