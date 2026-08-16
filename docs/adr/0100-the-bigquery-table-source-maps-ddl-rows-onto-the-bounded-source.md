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

# ADR-0100: The BigQuery table source maps DDL rows onto the bounded source

- Status: Accepted
- Date: 2026-08-12
- Issues: [#542](https://github.com/laughingman7743/flink-connector-gcp/issues/542),
  [#566](https://github.com/laughingman7743/flink-connector-gcp/issues/566)
- Modules: bigquery
- Current behavior: `docs/content/docs/connectors/table/bigquery.md`

## Context

The BigQuery module already has one bounded DataStream source for a table, a query result, or an
opt-in materialized view.
SQL had only the sink side of the `bigquery` factory, so the same read paths and their explicit
credential option were not reachable from a DDL.
The Table API needs to preserve the source's planning, restore, retry, and query-job contracts
without introducing a second implementation.

## Decision

**The existing `bigquery` factory serves both `DynamicTableSinkFactory` and
`DynamicTableSourceFactory`.**
A sink or direct table source requires `project`, `dataset`, and `table`.
A source carrying `source.query` requires either `project` or `source.parent-project` as its billing
project and ignores no missing destination part because the query result determines the table that
is read.
`source.query` and `source.materialize-views` are mutually exclusive.

**The table source is a bounded scan mapped directly onto `BigQuerySource.builder()`.**
Every source runtime option maps onto the existing builder, including query placement and reuse,
row restriction, snapshot time, stream counts, fetch size, retry attempts, emulator endpoints, and
the service-account key-file path.
The source advertises insert-only changelog mode and uses Flink's standard `scan.parallelism`.
It does not offer lookup or unbounded semantics because the underlying Storage Read source offers
neither.

**Top-level planner projection becomes a Storage Read `selectedFields` request.**
The converter addresses the returned Avro record by physical field name and emits fields in planner
order, so DDL order and projection order do not become an accidental positional contract.
When a query needs no output column, Flink retains the first physical column as a carrier; the
source reads and emits that one-field row, and the remaining planner projection discards it.
Nested projection is not advertised.

**The table layer converts the Storage Read API's documented Avro shapes directly to Flink's
internal values.**
The mapping covers scalar, decimal, temporal, nested row, array, map, and multiset shapes and is
serializable with the job graph.
Storage Read's Avro `time-micros` value is converted to Flink's millisecond-based internal value.
Flink 1.20 and 2.2 resolve SQL `TIME(p)` to `TIME(0)` before the connector sees it, while 2.3
retains precision through `TIME(3)`; a programmatically constructed catalog schema can carry
`TIME(1)` through `TIME(3)` on every supported version. `TIMESTAMP(6)` and `TIMESTAMP_LTZ(6)`
preserve BigQuery's microseconds.
A decimal overflow fails the read rather than silently producing `NULL`.

**BigQuery ranges are source-only rows.**
The documented Storage Read representation of `RANGE<T>` is a record with nullable `start` and
`end` fields, where null means an unbounded endpoint.
The source maps `RANGE<DATE>` to `` ROW<`start` DATE, `end` DATE> ``, `RANGE<DATETIME>` to a row of
`TIMESTAMP` fields and `RANGE<TIMESTAMP>` to a row of `TIMESTAMP_LTZ` fields; the endpoint names
need backticks because both are Flink SQL keywords.
The sink continues to derive an ordinary `STRUCT` from the same row declarations and never guesses
that field names imply a range.

**BigQuery intervals remain unsupported.**
A real-service measurement on 2026-08-13 returned an undocumented Avro record named
`google.sqlType.INTERVAL` with `months`, `days` and `microseconds` fields.
One populated value carried all three components, including sub-millisecond precision.
Flink separates year-month and day-time intervals, and its internal day-time value counts
milliseconds, so neither logical type preserves that BigQuery value.
The source rejects both Flink interval families when the job graph is built and rejects the
measured record if a DDL attempts to expose its components as a `ROW`.

**SQL filters are not translated.**
The connector exposes `source.row-restriction` as the existing BigQuery expression surface and
does not implement `SupportsFilterPushDown` until a translation can preserve Flink SQL semantics.

## Evidence

- Factory tests cover direct-table, query, and view modes; every source option; projection and the
  zero-column carrier; source copying and serialization; credential propagation to JobManager and
  TaskManager clients; and direction-specific validation.
- Converter tests cover the documented Storage Read Avro scalar, decimal, temporal, nested,
  repeated, and map representations, projected order, zero-field rows, and plan-time type
  rejections.
- `BigQueryTableSourceITCase` runs bounded projected scans and `COUNT(*)` through a real Flink
  planner against the BigQuery emulator.
- `BigQueryTableSourceFidelityITCase` runs the production factory and converter against BigQuery's
  own writer schemas for native, decimal, temporal, nested, repeated and range values.
  Its interval arm reads a required control beside the measured record before verifying the
  connector's rejection.
- The underlying DataStream source suites remain the evidence for read-session restore, query-job
  reuse, retry, and real-BigQuery behavior.

## Alternatives declined

- **Build a separate SQL source implementation**: it would duplicate the enumerator, reader,
  query materialization, and restore contracts and allow their behavior to drift.
- **Require `dataset` and `table` for query sources**: those values do not decide the query result
  and would turn placeholders into a false requirement.
- **Translate planner filters opportunistically**: BigQuery row restrictions are not Flink SQL
  expressions, and a partial translation could filter out rows that Flink still needs to evaluate.
- **Read every column after projection**: Storage Read charges for scanned bytes, so losing
  projection at the Table boundary would be a material cost regression.
- **Map BigQuery `INTERVAL` to either Flink interval family**: choosing year-month discards the
  day-time components, while choosing day-time discards months and rounds microseconds to
  milliseconds.
- **Expose the measured `INTERVAL` record as a source-only `ROW`**: the record is absent from the
  Storage Read Avro contract, and publishing its current fields would make an undocumented service
  detail part of the connector's API.

## Consequences

The DDL's physical column names and logical types must agree with the table or query result because
planning does not fetch the live schema.
The same service-account key must exist at the configured path on every JobManager and TaskManager
that can plan or run the bounded read.
Query and view sources use both REST and Storage Read clients, while a direct table source uses only
Storage Read.
SQL users who need server-side filtering must write a BigQuery row restriction explicitly or use a
query source.
Range declarations are asymmetric by design: they read BigQuery ranges but write BigQuery structs.
No DDL declaration reads or writes a BigQuery interval losslessly.
