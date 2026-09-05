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
- Date: 2026-08-12; BigQuery read-side option keys migrated by
  [#1047](https://github.com/flink-gcp/flink-connector-gcp/issues/1047) (2026-08-23)
- Issues: [#542](https://github.com/flink-gcp/flink-connector-gcp/issues/542),
  [#566](https://github.com/flink-gcp/flink-connector-gcp/issues/566),
  [#1047](https://github.com/flink-gcp/flink-connector-gcp/issues/1047),
  [#1233](https://github.com/flink-gcp/flink-connector-gcp/issues/1233),
  [#1234](https://github.com/flink-gcp/flink-connector-gcp/issues/1234)
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
A source carrying `scan.query` requires either `project` or `scan.parent-project` as its billing
project and ignores no missing destination part because the query result determines the table that
is read.
`scan.query` and `scan.materialize-views` are mutually exclusive.

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
Flink 1.20 and 2.2 resolve SQL `TIME(p)` to `TIME(0)` before the connector sees it, while 2.3 and
newer retain precision through `TIME(3)`: 2.3 is where that changed, and `2.4-SNAPSHOT` behaves as
2.3 does, measured by the weekly `next` row once #933 had corrected a test that keyed the boundary
to 2.3 alone. A programmatically
constructed catalog schema can carry `TIME(1)` through `TIME(3)` on every supported version. `TIMESTAMP(6)` and `TIMESTAMP_LTZ(6)`
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

**SQL filters are conservative Storage Read prefilters.**
The source implements `SupportsFilterPushDown`, but every accepted filter also remains in Flink's
residual list.
The accepted list records a filter that contributed a best-effort necessary condition, including a
partially translatable `AND`; it does not claim that BigQuery evaluated the whole filter.
Each generated restriction must be a necessary condition for the Flink predicate because a row
BigQuery excludes cannot be recovered by residual evaluation.
The residual then rejects any row that BigQuery admits but Flink does not.

The supported comparison matrix covers scalar types whose necessary conditions can be rendered
without fetching the BigQuery schema.
Integer, `DATE`, `DECIMAL`, `FLOAT`, `DOUBLE`, `BYTES` / `VARBINARY`, `BINARY(n)`, `TIME(0..3)`, `TIMESTAMP(0..6)`, and `TIMESTAMP_LTZ(0..6)` columns accept
`=`, `<>`, `<`, `<=`, `>`, and `>=` against typed literals.
`BOOLEAN` accepts `=` and `<>`, BigQuery `STRING` accepts `=`, and those supported column types
accept `IS NULL` and `IS NOT NULL`.
String `<>` and ordered string comparisons are not translated.

Some generated comparisons are deliberately weaker than the Flink predicate.
A `DECIMAL` restriction expands its bound by one unit at the declared Flink scale because reading
a BigQuery decimal through that scale can round the source value.
A Flink `FLOAT` restriction uses adjacent single-precision values as bounds because the source
narrows a BigQuery `FLOAT64` value before residual evaluation.
`DOUBLE` uses the finite double literal directly.
`TIMESTAMP` maps to a BigQuery `DATETIME` literal, and `TIMESTAMP_LTZ` maps to a UTC BigQuery `TIMESTAMP` literal.
Precision 6 preserves BigQuery microseconds and uses the literal directly.
At lower timestamp precisions and for `TIME`, the converter truncates fractional seconds.
For a precision-aligned literal `L` at precision `p`, every BigQuery value in the closed interval from `L` to `U = L + (10^(6-p) - 1)` microseconds converts to `L`.
Equality therefore becomes `field >= L AND field <= U`; `<= L` becomes `<= U`, and `> L` becomes `> U`.
The `<`, `>=`, and `<>` comparisons use `L` directly, with `<>` deliberately admitting other microseconds in the same bucket for the residual to reject.
The inclusive upper bound stays within the same second, including at midnight and the maximum BigQuery year.
Unaligned or out-of-range literals remain residual rather than being rounded by the translator.

Binary comparisons render each byte as a two-digit hexadecimal escape inside a GoogleSQL bytes literal, including an empty literal for an empty byte array.
This avoids text decoding and does not depend on a Storage Read function allowlist.
Ordered binary predicates use Flink's unsigned lexicographic comparison, including shorter-prefix ordering; `BigQueryFilterPushDownRealGcpITCase` checks the generated restrictions against Flink's scalar byte comparison on the same rows.
A `BINARY(n)` declaration preserves the byte array returned by Storage Read, including shorter and longer arrays; the source converter does not pad or truncate it.
Direct comparisons use those actual bytes, without adjusting the literal to the column's declared length.
Only a direct field reference and a resolved byte-array literal are translated, including reversed operands.
A remaining field-side or literal-side cast stays residual; a literal cast that Flink has already folded uses its resulting bytes.
On Flink 1.20.4, 2.2.1, and 2.3.0, SQL equality and inequality between different fixed lengths retain a field-side `VARBINARY` cast, whereas the four ordered comparisons can reach the source directly.
`IS NULL` and `IS NOT NULL` do not depend on the declared length and are translated independently.

BigQuery `JSON` and `GEOGRAPHY` string equality is unsupported.
The planner does not fetch the BigQuery schema, so a Flink `STRING` declaration cannot distinguish
ordinary `STRING` from `JSON` or `GEOGRAPHY`; a generated string restriction against an unsupported
physical type can be rejected when the Storage Read session is created.
A collated `STRING` equality can admit additional rows, which the Flink residual removes.
Fixed-length character columns, nested fields,
complex types, field-to-field comparisons, casts, and functions remain only with Flink.

An `AND` may push any translatable child because each child is necessary when the whole predicate
is true.
An `OR` is pushed only when every branch translates, because dropping one branch could exclude a
row that Flink needs.
Identifiers come from the physical schema index and are always quoted and escaped.
Literals are rendered by logical type; untyped string concatenation is not a translation path.

`scan.row-restriction` remains the explicit BigQuery expression surface.
When it and a generated restriction coexist, the source validates the explicit value first,
parenthesizes both operands separately, and combines them with `AND`.
The source counts the combined expression as UTF-8 and admits each generated restriction only
while the result remains within the Storage Read API's 1 MB row-restriction limit.
The translator first selects measured fragments, then renders the selected tree directly into one generated-text buffer.
The measurement includes escaped identifiers, UTF-8 literals, operators, separators, every enclosing wrapper, and the separately parenthesized explicit restriction.
String literals retain surrogate validation; bytes literals retain their hexadecimal escapes.
Scalar comparisons that generate multiple bounds are admitted as one unit, including every occurrence of the identifier.
No rejected compound, escaped identifier, or large literal needs its rendered text allocated.
The final buffer and its resulting string use space proportional to the admitted byte budget; the later combination with an explicit restriction also fits that budget.
This is not a bound on total heap or cumulative allocation: Flink owns the input expressions, and traversal metadata, scalar conversion, fragment objects, and accepted/residual lists also allocate.

An `AND` visits children in order, skips unsupported or oversized children, and tries later children with the remaining budget.
An `OR` requires a necessary condition from every branch, allowing a partial `AND` within a branch.
A failed `OR` releases its tentative conditions and budget; it does not retry earlier branches with fewer conjuncts.
This preserves fitting restrictions and can admit a partial `AND` whose combined translation previously exceeded the limit.
All original filters remain residuals so that fallback does not change the result.
A query source leaves the configured query untouched and applies both restrictions to the Storage
Read session over its materialized result.

## Evidence

- Factory tests cover direct-table, query, and view modes; every source option; projection and the
  zero-column carrier; source copying and serialization; credential propagation to JobManager and
  TaskManager clients; and direction-specific validation.
- Converter tests cover the documented Storage Read Avro scalar, decimal, temporal, nested,
  repeated, and map representations, projected order, zero-field rows, and plan-time type
  rejections.
- `BigQueryTableSourceITCase` runs bounded projected scans and `COUNT(*)` through a real Flink
  planner against the BigQuery emulator, including generated string, double, null, conjunction,
  and disjunction restrictions.
- `BigQueryTablePlanTest` proves each supported scalar family reaches the source, every predicate
  remains a residual, unsupported shapes stay above the source, and direct and query sources share
  the ability.
- `BigQueryFilterPushDownRealGcpITCase` measures fewer returned rows and serialized Avro response
  bytes and submits generated string, decimal, float, double, binary, `TIME`, `DATETIME`, and `TIMESTAMP`
  restrictions to the real Storage Read API before deleting its bounded temporary table.
  The binary and temporal cases compare row IDs with an unrestricted read converted at each declared precision, asserting that no matching row is lost and that residual evaluation restores the expected row set.
- `BigQueryBinaryFilterSemanticsITCase` feeds converted `RowData` through a bounded Flink SQL pipeline, retaining actual byte lengths and evaluating all six comparisons, reversed operands, nulls, and constant and field casts at declared lengths 1, 2, and 4.
  The same `BinaryFilterOracle` evaluates unrestricted and restricted real Storage Read batches for fixed-length binary predicates; the service test asserts containment before residual evaluation and equality afterwards.
- `BigQueryTableSourceFidelityITCase` runs the production factory and converter against BigQuery's
  own writer schemas for native, decimal, temporal, nested, repeated and range values.
  Its interval arm reads a required control beside the measured record before verifying the
  connector's rejection.
- The underlying DataStream source suites remain the evidence for read-session restore, query-job
  reuse, retry, and real-BigQuery behavior.

### Compound construction allocation

Measured for [#1234](https://github.com/flink-gcp/flink-connector-gcp/issues/1234) on 2026-09-06 with Temurin 17.0.20 on arm64 macOS.
The baseline is commit `85ac1f5c631a0f6a136d48d6406eeeead2e215b4`; the candidate uses the measured-fragment construction above.
Each case ran in a fresh JVM with `-Xms64m -Xmx512m`, three warmups, and five measured translations; the table reports the median current-thread allocated bytes from `com.sun.management.ThreadMXBean`.
Inputs were constructed before warmup and measurement, and each result was retained through a volatile reference.
The count is cumulative allocation during one translation, not peak or retained heap and not a service-throughput measurement.

The harness uses one STRING or BYTES field named `value` and repeats references to one equality leaf.
Wide cases put all leaves in one call; nested cases repeatedly wrap the previous expression as the left child and add one leaf on the right.
The ASCII literal contains 200,000 `a` characters, the control literal contains 80,000 U+0001 characters, and the bytes literal contains 120,000 zero bytes.
Each literal individually fits after escaping.
The late-failure case wraps the wide OR and an unsupported NOT in another OR.
The explicit-restriction case supplies 500,000 `x` characters as a size-only input; no service parses that synthetic input.

| expression | leaves | baseline allocated bytes | candidate allocated bytes | candidate combined restriction bytes |
|---|---:|---:|---:|---:|
| wide AND, ASCII | 8 | 9,609,152 | 1,609,280 | 800,075 |
| wide AND, ASCII | 32 | 38,435,272 | 1,630,208 | 800,075 |
| wide OR, ASCII | 32 | 38,435,432 | 6,032 | 0 |
| nested AND, ASCII | 32 | 236,466,744 | 1,638,000 | 800,135 |
| nested OR, ASCII | 32 | 236,463,928 | 8,312 | 0 |
| late-failure OR, ASCII | 1 | 1,001,752 | 1,840 | 0 |
| wide AND, controls | 32 | 176,679,368 | 1,949,488 | 960,037 |
| wide OR, controls | 32 | 176,678,760 | 3,648 | 0 |
| wide AND, bytes | 32 | 92,196,136 | 1,949,488 | 960,039 |
| wide AND, ASCII with explicit restriction | 32 | 39,435,360 | 829,544 | 900,050 |

Zero restriction bytes means no generated condition was admitted.
Every baseline case in this table rejected its whole expression; the candidate AND cases retain the fitting children.
Increasing the wide AND from 8 to 32 leaves still allocates traversal and fragment objects, but does not render another 24 large literals.
The late-failure case establishes that even a fitting tentative OR is not rendered before its enclosing OR succeeds.
Unit tests independently hold exact and exceeded UTF-8 budgets, large escaped identifiers, scalar bounds, nested composition, and budget recovery after a rejected OR.
A local Flink execution test checks the emitted integer/null/Boolean composition and residual row sets, including nulls; it does not replace the existing real-service scalar evidence.

## Alternatives declined

- **Build a separate SQL source implementation**: it would duplicate the enumerator, reader,
  query materialization, and restore contracts and allow their behavior to drift.
- **Require `dataset` and `table` for query sources**: those values do not decide the query result
  and would turn placeholders into a false requirement.
- **Remove translated filters from Flink residual evaluation**: even within the necessary-condition
  matrix, the residual prevents a row that BigQuery admits but Flink rejects from reaching the
  result.
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
SQL users get best-effort server-side filtering for the supported subset and retain Flink's final
evaluation.
Other predicates require an explicit BigQuery row restriction or a query source when server-side
filtering matters.
Range declarations are asymmetric by design: they read BigQuery ranges but write BigQuery structs.
No DDL declaration reads or writes a BigQuery interval losslessly.
