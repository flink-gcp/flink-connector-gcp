---
title: Bigtable async SQL functions
type: docs
weight: 41
---

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

# Bigtable async SQL functions

These functions return the result of one atomic Bigtable row operation to a SQL query.
`BigtableCheckAndMutateFunction` returns whether its predicate matched.
`BigtableReadModifyWriteFunction` returns the final cells changed by ordered append and increment rules.
Both are available in **Flink 2.x streaming mode**, including bounded streaming queries.
The Flink 1.20 artifacts do not contain these classes; use the [DataStream request APIs]({{< relref "docs/connectors/datastream/bigtable" >}}) when that version needs results.

Put the matching [`flink-sql-connector-gcp-bigtable` jar]({{< relref "docs/connectors/table/bigtable" >}}#getting-the-connector-onto-the-classpath) on the SQL Client or SQL Gateway and cluster classpaths.
Register the function with `CREATE TEMPORARY SYSTEM FUNCTION`, configure a named request through `SET`, and call it in a query.
The SQL aliases below are chosen at registration.
Create the destination table, its families, and a single-cluster application profile with single-row transactions enabled before running the examples.
Authentication uses application-default credentials unless a key file is configured.

## Return a conditional outcome

This query changes `cf:status` to `paid` only when its latest value is `pending`.
The first value argument is the expected value and the second is the replacement.
The omitted `otherwise` branch makes a mismatch a successful `false` result without a mutation.

{{< sql-snippet file="flink/BigtableAsyncSqlFunctions.sql" tag="conditional-outcome" >}}

The result is `BOOLEAN`: `true` means the predicate matched, and `false` means it did not.
Either branch can contain mutations, so `false` does not generally mean that no write occurred.
A request failure fails the query; it never becomes `false`.

## Return changed cells

This query increments `cf:visits` and appends to `cf:history` in one row transaction.
Increment operands are `BIGINT`; cast integer literals explicitly as shown.

{{< sql-snippet file="flink/BigtableAsyncSqlFunctions.sql" tag="changed-cells" >}}

The result has the fixed type `ROW<row_key BYTES, cells ARRAY<ROW<family STRING, qualifier BYTES, value BYTES, timestamp_micros BIGINT, value_int64 BIGINT>>>`.
The row key and every cell's qualifier and value remain binary.
`timestamp_micros` is the service timestamp in microseconds since the epoch.
`value_int64` is the signed eight-byte big-endian value only when the last rule for that family and qualifier is an increment; otherwise it is NULL.
An eight-byte append result is still binary, with NULL in `value_int64`.

The returned cells contain their final values after all rules in this request.
They exclude untouched cells and intermediate values between repeated rules.
Rules run in numeric index order, including repeated operations on one cell; response positions are not rule indexes.
Bigtable determines overflow and rejects incompatible stored values.

## Named configuration

| Option | Default | Meaning |
|---|---|---|
| `bigtable.functions` | **required** for each referenced name | Map namespace for named SQL request templates; set individual `bigtable.functions.<name>.<property>` entries. |

A name must be a nonblank literal string without dots or leading or trailing whitespace.
Argument zero is that name; the next argument is the row key.
Each `value-argument` or timestamp argument binding counts the arguments **after the row key from zero**.
The planner validates the selected settings and input types when translating the executable plan, then serializes a fixed template.
Changing `SET` values does not change a running job.
Keep the `bigtable.functions` namespace in one configuration layer.
An entry in Flink's session configuration hides the entire map inherited through its root configuration layer; properties already in the same session configuration remain together.
Registration and planning do not load credentials or contact Bigtable.

All properties below are relative to `bigtable.functions.<name>.`.
Unknown properties, conflicting bindings and properties inapplicable to an operation are rejected.
Use separate names for conditional templates and read-modify-write templates.
Project, instance and table are fixed for each function call site, as are its families and qualifiers.

| Property | Default | Meaning |
|---|---|---|
| `project`, `instance`, `table` | **required** | Bigtable destination; each is one resource-path component. |
| `app-profile-id` | *unset ⇒ service default profile* | Application profile used by the data client; it must support single-row transactions. |
| `service-account-key-file` | *unset ⇒ application-default credentials* | Key file available on task managers. |
| `emulator-endpoint` | *unset ⇒ real service* | Emulator `host:port`; cannot be combined with a key file. |
| `request-timeout` | `20 s` | Positive SDK deadline, expressible as signed 64-bit nanoseconds; must be shorter than the Flink async timeout after millisecond conversion. |

Row keys and ordinary value arguments use the existing [SQL cell type mapping]({{< relref "docs/connectors/table/bigtable" >}}#type-mapping).
The row-key expression and every supplied operand must have a `NOT NULL` SQL type, including operands unused by the template or referenced only by an unselected conditional branch.
Planning rejects nullable types even when their current values are non-NULL.
Row keys must also be nonempty.
There is no NULL-based omission or fallback to a literal.
Sources must honor their declared `NOT NULL` schema.

`CAST` preserves the input's nullability, and a `WHERE ... IS NOT NULL` filter does not make a nullable column's declared type non-nullable.
Use non-nullable source columns or an expression whose inferred result type is `NOT NULL`.
An explicit `COALESCE` with a non-nullable fallback can produce that type, but the fallback must match the application's write semantics.
This example treats a missing increment as zero:

{{< sql-snippet file="flink/BigtableAsyncSqlFunctions.sql" tag="non-null-operands" >}}

### Targets, values and timestamps

A cell target requires a nonblank `family` and exactly one of `qualifier` (UTF-8) or `qualifier-base64` (canonical padded Base64).
An empty qualifier is valid.
A value requires exactly one of `value-argument`, `value-utf8`, `value-base64` or `value-int64`.
A literal `value-int64` is a signed 64-bit decimal; ordinary values encode it as eight big-endian bytes.
Base64 values use the standard alphabet and canonical padding; an empty ordinary cell value is valid.

A timestamp takes either `<stem>-micros` for a signed BIGINT literal or `<stem>-argument` for a BIGINT argument binding.
Both forms together are invalid.
The stems used below are `timestamp`, `start-timestamp` and `end-timestamp`.
A range includes its start and excludes its end; a supplied start is nonnegative, a supplied end is positive, and the end must exceed the start when both are present.
Timestamps must satisfy the destination's granularity; the connector passes explicit values to the service.

### Conditional predicates

Set `predicate.type` and the corresponding attributes below under `predicate.`.
A predicate matches when its filter leaves at least one cell.

| Type | Attributes | Meaning |
|---|---|---|
| `row-exists` | None | Tests the entire stored row, including undeclared families. |
| `cell-exists` | Cell target | Tests whether that cell has a version. |
| `latest-cell-value-equals` | Cell target and value | Selects the latest version first, then compares bytes. |
| `family-equals` | `family` | Selects an exact family. |
| `qualifier-equals` | Qualifier representation | Selects an exact binary qualifier across families. |
| `value-equals` | Value | Selects cells with those exact bytes. |
| `timestamp-range` | Both timestamp range bounds | Selects versions in the half-open range. |
| `cells-per-column` | Positive INT `count` | Retains at most that many versions per column. |
| `cells-per-row` | Positive INT `count` | Retains at most that many cells per row. |
| `chain` | `children.0.type`, `children.1.type`, … and each child's attributes | Applies filters successively to the remaining cell set. |
| `interleave` | Same recursive children form | Applies every child to the input cell set and interleaves the results. |

Children may themselves be chains or interleaves.
For example, a `chain` can place `family-equals` at `predicate.children.0.type` and an `interleave` at `predicate.children.1.type`, whose children live under `predicate.children.1.children.0.` and subsequent indexes.
A chain is not Boolean AND between predicates on distinct cells: after selecting one qualifier, a subsequent filter for a different qualifier sees no cells.
These are the connector's filter compositions; raw protobuf, regex and SDK Condition filters are not configuration inputs.

### Conditional mutations

Put ordered mutations under `then.0.`, `then.1.`, … and `otherwise.0.`, `otherwise.1.`, ….
Each item requires `operation` and its attributes below.
One whole branch may be absent; both empty is invalid.
Each branch supports at most 100,000 operations.

| Operation | Attributes | Meaning |
|---|---|---|
| `set-cell` | Cell target, value, optional timestamp | Writes a cell; omitted timestamp uses a per-cell millisecond-aligned writer clock, while explicit `-1` requests server time. Other negative timestamps are invalid. |
| `delete-cells` | Cell target, optional start and end timestamps | Deletes versions in the range; omitted bounds are unbounded. |
| `delete-family` | `family` | Deletes all cells in the family. |
| `delete-row` | None | Deletes the entire row. |
| `add-to-cell` | Cell target, value, required nonnegative timestamp | Contributes input to a pre-existing aggregate family. |
| `merge-to-cell` | Same as `add-to-cell` | Merges an aggregate state into a pre-existing aggregate family. |

Aggregate argument bindings accept BIGINT as a typed integer and BINARY/VARBINARY as typed bytes.
The UTF-8 and Base64 literal forms supply typed bytes, and `value-int64` supplies a typed integer.
The service checks that the family aggregation type accepts the selected input.

### Read-modify-write rules

Put ordered rules under `rules.0.`, `rules.1.`, ….
At least one and at most 100,000 rules are required.
Each item has `operation`, a cell target and one value binding.
`append` accepts character or binary values and rejects empty values.
`increment` accepts BIGINT and preserves zero and negative amounts as operations.
Rules do not take timestamps.

All branch, rule and filter-child indexes are consecutive decimal integers starting at zero, with no leading zeros.

## Async execution and delivery

Set `table.exec.async-scalar.max-attempts` explicitly to `1` before planning either function.
The Flink 2.2.1 default is three attempts, which can repeat a committed write after an ambiguous error; specialization rejects that default and any other value.
The SDK and connector also make one attempt.
Query recovery can still replay a write that Bigtable already committed, causing another increment, append, or a different conditional outcome.
These functions therefore have **at-least-once side effects across recovery**, without a transaction linking the returned SQL record to a checkpoint.

`table.exec.async-scalar.max-concurrent-operations` bounds outstanding async inputs per operator subtask and defaults to `10` in Flink 2.2.1.
Each specialized function holds at most one instance client.
Set `table.exec.async-scalar.timeout` above `request-timeout`; the examples use one minute over the default 20-second RPC deadline.
The SQL operator has no per-invocation timeout callback to this UDF.
Normally the shorter SDK deadline fails first; if the outer timeout ends the task, closing the function cancels outstanding RPCs.
Cancellation cannot prove that Bigtable did not apply a write.

Ordered query output does not serialize concurrent requests to the same row.
Use append-only input, consume the function result, and account for SQL expression evaluation: unused expressions can be pruned, and separate function calls cause separate writes.
The functions disable deterministic evaluation and constant folding; that does not make SQL evaluation exactly once per source row.
Every invalid input or service error fails its async result; there is no dropping failure handler.

Request metrics use the existing [single-row metric definitions]({{< relref "docs/connectors/datastream/bigtable" >}}) under the function's `bigtableSql.<name>` metric group.
They count attempts in a subtask, not unique service applications across recovery.
