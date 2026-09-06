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

# ADR-0161: Bigtable async SQL functions specialize named request templates

- Status: Accepted
- Date: 2026-09-06
- Issue: [#1181](https://github.com/flink-gcp/flink-connector-gcp/issues/1181)
- Refines: ADR-0148, ADR-0152, ADR-0155

## Context

SQL sinks consume records without returning their conditional outcomes or changed cells.
The supported Flink 2.x floor provides `AsyncScalarFunction`, while Flink 1.20 does not.
The owner chose SQL-only registration and configuration, named settings shared by queries, composable filters alongside three convenient predicates, and raw changed-cell bytes accompanied by an increment's numeric value.

## SQL contract

Register `BigtableCheckAndMutateFunction` and `BigtableReadModifyWriteFunction` from `io.github.flink.gcp.connector.bigtable.table.function` with `CREATE TEMPORARY SYSTEM FUNCTION`.
The examples name them `BT_CHECK_AND_MUTATE` and `BT_READ_MODIFY_WRITE`; SQL registration owns the alias.
Each call takes a literal settings name, a row key, and zero or more value arguments.
Settings use `SET 'bigtable.functions.<name>.<property>' = '<value>'`.
The name cannot contain a dot because the configuration namespace uses that separator.
Connection settings fix one project, instance, table and optional application profile per specialized function.
ADC is the default; a service-account key-file path or emulator endpoint follows the existing credential contract.
Neither planning nor registration loads credentials or opens a client.

`SpecializedFunction` reads the selected settings and declared argument types during executable-plan translation.
It validates and snapshots an immutable request template into the runtime function.
Changing session settings cannot reconfigure a running job.
The namespace belongs to one configuration layer: a session prefix-map entry replaces the map inherited through `TableConfig`'s root configuration rather than merging properties or names.
On 2026-09-06, one executable-plan probe on each of Flink 2.2.1 and 2.3.0 placed a complete increment template in `TableConfig.setRootConfiguration`, registered the function, disabled Flink retries, and successfully explained `SELECT BT('test', 'key')`.
Setting only `bigtable.functions.test.rules.0.value-int64` in the session then made the same plan fail because the root template's project setting was hidden.
These probes exercised explicit Java `TableEnvironment` root/session layers; SQL Client and Gateway configuration loading may place cluster-provided properties into the session layer instead.
The function does not look up settings for each record or resolve a destination from a row.
The numbered `value-argument` bindings count arguments after the row key from zero.
Row keys and ordinary values use the existing `CellValueCodec` types and encodings.
Type checks and argument-index checks run before submission.
The row key and every supplied operand must have a `NOT NULL` SQL type; nullable expressions fail during planning, including unused operands and operands of an unselected conditional branch.
On 2026-09-06, SQL execution probes on Flink 2.2.1 and 2.3.0 showed that Flink bypasses `eval` when an argument is NULL, so runtime validation inside the function cannot enforce this contract for nullable SQL inputs.
Requiring non-nullable input types retains the scalar call shape and rejects the query before any write; a ROW-container signature with runtime field validation was declined in favor of this planning boundary.
The check concerns declared types, so a nullable column remains rejected after `WHERE ... IS NOT NULL` or an ordinary cast.
An explicit `COALESCE` with a non-nullable fallback can supply a non-nullable operand when that replacement matches the application's semantics.
Both conditional branches are still constructed, and direct invocation retains defensive checks for a NULL row key or referenced value.

The `predicate.type` shortcuts are `row-exists`, `cell-exists` and `latest-cell-value-equals`.
The composable leaves are `family-equals`, `qualifier-equals`, `value-equals`, `timestamp-range`, `cells-per-column` and `cells-per-row`.
`chain` and `interleave` contain consecutive `children.<index>.*` definitions, recursively, in numeric order.
These map to the existing public filter model; SDK nested Condition filters and raw filter protobufs are not inputs.
A chain transforms one cell set in order, rather than computing Boolean AND across separate cells.
The latest-value shortcut always selects the latest version before comparing bytes.

`then.<index>.*` and `otherwise.<index>.*` define ordered SetCell, DeleteCells, DeleteFamily, DeleteRow, AddToCell and MergeToCell operations.
One branch may be omitted; both empty is an error.
Selecting an empty branch is successful and returns the predicate result.
The attributes follow #1226's fixed targets, literal bindings, binary qualifiers, typed aggregate values and timestamp rules, with `*-argument` replacing `*-column`.
SetCell's omitted timestamp uses the existing millisecond-aligned writer clock; explicit -1 requests server time.
Aggregate timestamps are explicit and nonnegative.
NULL never means omit an operation or select a default.

`rules.<index>.*` defines ordered append and increment rules, including repeated cells and mixed operations.
Append accepts character or binary values, while increment accepts BIGINT.
Empty appends fail; zero and negative increments remain operations.
The service owns arithmetic, including overflow and invalid stored values.
Indexes in branches, rule lists and filter children are consecutive canonical nonnegative decimals starting at zero.
Unknown, conflicting and inapplicable attributes fail with the complete setting key.
The existing per-request operation limits remain in force.

The conditional function returns BOOLEAN, including a successful false result.
The read-modify-write function returns `ROW<row_key BYTES, cells ARRAY<ROW<family STRING, qualifier BYTES, value BYTES, timestamp_micros BIGINT, value_int64 BIGINT>>>`.
Only the cells changed by the request are returned, with their final bytes and service timestamps.
For a cell whose last rule is increment, `value_int64` decodes its eight-byte signed big-endian result; otherwise this field is NULL, even when an append happens to leave eight bytes.
The numeric conversion is correlated by family and binary qualifier, not by response position.
There is no complete-row snapshot or intermediate answer per rule.

## Runtime and replay

The SQL adapter reuses the single-row async request runtime, operation adapters, credential loader, failure classification, thread-safe counters and close/cancellation machinery.
The shared runtime accepts a metric group directly so SQL does not manufacture a DataStream runtime context.
Each specialized function has one fixed destination and at most one active instance client.
Flink's `table.exec.async-scalar.max-concurrent-operations` bounds outstanding invocations per operator subtask, with its default of 10.
The connector adds no request queue and never waits for a write RPC in `eval`.
Client initialization follows the shared runtime on first use.

The SDK deadline remains the only connector-controlled request timeout, defaulting to the existing 20 seconds.
`table.exec.async-scalar.timeout` must exceed it after conversion to milliseconds.
Flink 2.2.1 defaults `table.exec.async-scalar.max-attempts` to 3 and retries any exceptional async result.
Both functions require an explicit value of 1 and reject other values during specialization, naming the required SET statement.
They add no SDK or connector retry.
Both `isDeterministic()` and `supportsConstantFolding()` return false.

The SQL operator's timeout does not call a timeout method on the scalar UDF.
An RPC normally reaches its shorter SDK deadline first; a task ending after an operator timeout closes the function and cancels outstanding requests.
The adapter does not claim immediate per-invocation cancellation from an outer SQL timeout.
Service failures complete the result exceptionally with the existing routing and ambiguous-outcome diagnostics.
There is no dropping failure handler on the async surface.

Query recovery may evaluate an already committed write again, changing the branch chosen or appending/incrementing again.
Ordered SQL results do not order separate RPCs against the same row.
SQL expression evaluation is not a once-per-source-row side-effect contract: an unused expression may be removed, and separately expressed calls are separate writes.
Examples consume the result and use append-only input; a checkpoint does not commit a Bigtable transaction.

## Compatibility and validation

The two new entry types are `@PublicEvolving` under ADR-0141's new-surface clause.
Their Flink bases, `AsyncScalarFunction` and `SpecializedFunction`, are also `@PublicEvolving`.
Implementation and version-specific tests live only in the Flink 2.x source root; the 1.20 artifacts contain neither function.
The supported Java async scalar planner is streaming-only on the current floor and ceiling, so finite acceptance input also uses streaming mode.
Flink 1.20 continues to use the result-discarding SQL sinks or the result-emitting DataStream APIs.
The new public types are additions to the 2.x artifact, not removals from the 1.20 artifact; they require no japicmp exclusion.
`flink-table-api-java` is provided for the official execution-option constants, and the API-tier source inventory includes its sources jar.

Tests cover executable SQL translation and execution, configuration isolation, all filter and mutation forms, ordered mixed rules, binary data, NULL and schema failures, result conversion, failures, deadlines, capacity, cancellation and late completion.
The existing request-ledger and DataStream suites protect the shared lifecycle change.
Source-backed registration and invocation examples reach planner translation on 2.x; the 1.20 validation boundary proves that the functions are unavailable.
Credential-gated service tests compare both conditional outcomes and changed-cell answers with direct reads, using an approved ephemeral instance and verified teardown.

## Alternatives

Java-only registration would not meet the selected SQL Client and SQL Gateway workflow.
A literal configuration MAP in every call was declined in favor of reusable named SET settings.
Per-record opcode arrays and JSON strings would impose a second command representation on each row.
Numbered definitions fix the operation structure while ordinary SQL expressions supply the operands.
The SQL function implementation does not implement #1226's sink or add it as a prerequisite.
