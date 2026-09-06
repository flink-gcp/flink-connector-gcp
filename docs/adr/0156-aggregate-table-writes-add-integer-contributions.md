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

# ADR-0156: Aggregate Table writes add integer contributions

- Status: Accepted
- Date: 2026-09-06
- Issue: [#1176](https://github.com/flink-gcp/flink-connector-gcp/issues/1176)
- Modules: bigtable
- Current behavior: `docs/content/docs/connectors/table/bigtable.md`

## Decision

The opt-in `sink.write-mode = aggregate` accepts INSERT-only contributions using the existing row-key and family/qualifier DDL shape.
`sink.aggregate.column-family-types` declares every physical family as `int64-sum`, `int64-min`, `int64-max`, or `int64-hll`.
Unknown, missing and extra family declarations, raw families, non-integer qualifiers and incompatible explicit options fail during planning.
TINYINT, SMALLINT, INT and BIGINT inputs widen losslessly to INT64.
All four aggregators receive AddToCell with an integer input, a raw qualifier and a concrete timestamp.

This refines the issue body with the owner: HLL accepts integer contributions directly rather than requiring a client-side sketch and MergeToCell.
The pinned google-cloud-bigtable 2.82.0 MutationApi accepts INT64 inputs for aggregate families, including the INT64 HLL type exposed by its admin Type model.
The published [data API](https://docs.cloud.google.com/bigtable/docs/reference/data/rpc/google.bigtable.v2#mutation) distinguishes AddToCell input values from MergeToCell accumulator state.
No sketch library is added.
Existing DataStream serializers can still submit MergeToCell.

A null family or scalar cell contributes nothing.
An input with no non-null cell fails rather than submitting an empty entry.
INSERT is the only accepted row kind, and repeated inputs for the same row key remain separate contributions.
An updating GROUP BY produces replacement aggregate results, not contributions, and is rejected by changelog negotiation.
The connector does not subtract prior values or retain per-input aggregation state.
The input DDL is sink-only: a separate read DDL uses BIGINT for numeric aggregate state and BYTES for HLL state.

Timestamp metadata remains optional, following ADR-0149.
An absent or null timestamp uses the connector millisecond-aligned writer clock, read per written cell.
Explicit timestamps retain the existing range checks and optional millisecond truncation.
A stable timestamp addresses the same aggregate cell but does not deduplicate a SUM contribution.
A regenerated timestamp can address a different cell version, including on replay.
Use a stable bucket timestamp when events should contribute to one time bucket.

## Provisioning and reconciliation

The public serializable ColumnFamilyType vocabulary represents raw and the four INT64 aggregate families.
TableCreateOptions adds a three-argument columnFamily overload accepting a type and nullable GC rule, without creating an ambiguous two-argument overload.
Existing family/rule getters and overloads remain compatible; a type absent from an old job graph means raw.
SDK type models never enter the job graph.

SQL aggregate writers inspect the fixed destination during writer creation, before admitting data.
CREATE_IF_NEEDED ensures the declared table and families; CREATE_NEVER only validates their existence and types.
DataStream options declaring aggregate types validate existing families before a destination first receives data; missing tables and families retain the existing bounded repair path.
Ordinary raw-only writers keep their lazy admin behavior and reconcile existing families by name only.
Metadata validation requires bigtable.tables.get in addition to the selected write and creation permissions.

The ensure remains add-only and bounded by the declared family count plus one.
When options declare any aggregate type, every reconciliation read checks all declared existing families before adding missing ones, including raw declarations and after a concurrent creation wins.
Raw-only options specify creation types without constraining existing types: an ordinary write can skip an existing aggregate family and need a different raw family added.
A mismatch reports the destination, family, expected type and actual type; it never converts a family or enters the row failure handler.
Compare aggregate kind and normalized input encoding, excluding output-only state_type.
Normalize omitted INT64 encoding to big-endian and clear only its deprecated bytes_type field, which the pinned admin schema explicitly ignores; retain unknown input fields so an unrecognized encoding cannot pass validation.
Undeclared families do not constrain the sink, and unknown types in them must not prevent inspecting declared families.
Existing GC rules are neither compared nor updated.
SQL automatic creation still requires a version or age retention rule, independent of write mode.
Creation metrics count actual table creation and additions to existing tables.

## Delivery and validation

The existing MutateRows writer owns batching, checkpoint draining, isolation and repair.
A successfully completed checkpoint does not prove each SUM input contributed only once.
SDK retries, isolation resubmissions and Flink replay do not provide a durable input identity or a Flink exactly-once contract.
With an unchanged target timestamp and no intervening deletion or GC, repeating the same MIN, MAX or HLL contribution leaves its aggregate result unchanged; this does not change the sink delivery guarantee.

Unit tests assert exact admin and mutation protos, logical-type validation, nulls, timestamp conversion, family reconciliation races, job-graph compatibility and failure cleanup.
Independent-review probes reproduced a raw-only repair rejection beside an existing aggregate family and rejection of the schema's ignored bytes_type field; focused regressions now hold both equivalences and mixed-schema mismatch rejection.
Planner tests cover repeated same-key inputs and rejection of updating changelogs on supported Flink versions.
Credential-gated service tests verify all four aggregate types, fixed-timestamp reapplication and writer-clock versions.
HLL cardinality is checked with the service HLL_COUNT.EXTRACT function; emulator results alone establish no service behavior.
On 2026-09-06, all four cases of `BigtableAggregateTableRealGcpITCase` passed against one ephemeral Bigtable instance with one SSD node in `us-central1-b`, using Flink 2.2.1 and google-cloud-bigtable 2.82.0.
With and without a PRIMARY KEY, the fixed-timestamp cases observed SUM `11` then `22`, MIN `3`, MAX `5`, and HLL_COUNT.EXTRACT `2` after reapplying the inputs through CREATE_NEVER.
The writer-clock case observed absent MIN/MAX cells after the first submission and two SUM versions of `7` after replay.
Reconciliation added three typed families, then none on a second ensure, preserved the existing family's absent GC rule, and rejected a mismatched SQL declaration before any row was written.
After the suite completed, an independent instance-list request confirmed the generated instance was absent and the test project contained no remaining instances.
