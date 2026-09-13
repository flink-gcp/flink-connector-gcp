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

# ADR-0167: Bigtable conditional SQL commands compile DDL into request templates

- Status: Accepted
- Date: 2026-09-13
- Issue: [#1226](https://github.com/flink-gcp/flink-connector-gcp/issues/1226)
- Refines: [ADR-0152](0152-conditional-writes-own-their-request-and-outcome.md)

## Decision

Add `conditional` to `sink.write-mode` using the owner-approved specification in #1226.
During draft review, the owner renamed the proposed `check-and-mutate` mode to `conditional`.
The name follows the existing behavior-oriented modes, such as `insert-if-absent`, `append` and `increment`, and matches the `sink.conditional.*` option namespace.
The underlying RPC remains CheckAndMutateRow; the mode can mutate either branch, including when the predicate does not match.
One DDL fixes the predicate, ordered operations and target cells; physical input columns supply values.
Calculations and conversions stay in SELECT expressions.
This command schema has a separate factory validation path and dynamic sink, preserving the ordinary family/qualifier schema and all existing write modes.
It accepts INSERT-only input, rejects PRIMARY KEY and metadata declarations, and is write-only.
Column references exactly name top-level physical columns; unused columns do not contribute mutations and need not have cell encodings.

The row-key option and every referenced column are resolved at planning time.
Three named predicates expose row existence, cell existence and latest-cell byte equality.
The last fixes filter order to cell selection, latest-version selection, then equality.
The `then` and `otherwise` options use Flink mapType and its standard expanded spelling.
Each map contains consecutive canonical decimal indexes from zero, followed by a closed set of operation attributes.
Mixed packed and expanded spellings, unknown or inapplicable attributes and conflicting bindings fail planning under their actual option keys.
Each branch permits at most 100,000 mutations and at least one branch must be nonempty.

Internal serializable templates retain resolved column positions, declared types and literal values.
They construct the existing connector-owned ConditionalRequest at runtime without reparsing DDL, capturing codec lambdas or extending the public DataStream model with placeholders.
The Flink 2.x SQL functions retain their separate argument-binding and NOT NULL contracts; this sink is available on both supported majors.
All referenced input bindings must be nonnull at runtime, including bindings in the branch that the service will not select.
Both branches are constructed and validated before the RPC is submitted.
Empty qualifiers and values are allowed; an empty encoded row key is not.
UTF-8 and canonical padded RFC 4648 Base64 literals bind bytes, and int64 literals use the existing eight-byte big-endian encoding for predicates and SetCell.
Aggregate BIGINT values bind int_value and BYTES values bind bytes_value, including aggregate UTF-8 and Base64 literals.
Other aggregate column types are rejected rather than widened.

SetCell without a timestamp uses ADR-0149's per-cell millisecond-aligned writer clock.
An explicit -1 requests server time through the existing conditional adapter; other negative timestamps fail.
Aggregate timestamps are explicitly bound nonnegative microseconds, including zero, and never default to a clock.
Explicit timestamps are preserved without truncation, leaving table-granularity validation to the service.
DeleteCells uses optional inclusive-start and exclusive-end bounds and rejects empty or reversed intervals before submission.
An explicit end of zero describes an empty interval in the nonnegative timestamp domain and is rejected, preserving ConditionalMutation's existing positive-end requirement instead of converting it to the service's unbounded sentinel.

Reuse the single-row runtime, request option mapper, conditional outcome counters and empty-branch policy.
Successful RPC and predicate outcomes are recorded before fail-on-empty fails the job; this failure bypasses dropping handlers.
The policy remains valid with two nonempty branches.
No batcher, pre-read, additional retry or mandatory admin lookup is introduced.
Known routing policies must be single-cluster with single-row transactions; an ID alone retains the existing service-cause diagnostic.
Reject explicit options this mode cannot apply, including scan/lookup, batching, table creation, legacy timestamps, null-cell encoding and staged exactly-once delivery.

## Delivery boundaries

The sink discards results after waiting for the RPC and draining accepted requests at checkpoints.
At-least-once replay can reevaluate a predicate and choose a different branch after a lost acknowledgement.
Fail-on-empty insertion can therefore fail repeatedly after a successful first application.
There is no ordering promise across requests, exactly-once effect or transaction across rows.
Flink ON CONFLICT is not the destination-side condition.

## Validation

Planning, serialization, fake-client and SQL execution tests cover the predicates, operations, bindings, outcomes and mode boundaries on Flink 1.20.4, 2.2.1 and 2.3.0.
Source-backed SQL examples reach planner translation.
Credential-gated service acceptance extends the existing conditional suite and requires approval of resources and cleanup before execution.
Aggregate acceptance uses a compatible int64-sum family and reads stored accumulator bytes for MergeToCell, following ADR-0041; it does not claim coverage of every aggregate type.

## Alternatives

JSON in WITH strings adds a second configuration grammar and was declined.
Per-record opcode arrays require union-like schemas and move operation-dependent validation to runtime.
Arbitrary composable filters remain a DataStream feature, and result-emitting SQL functions retain their own interface.
Routing command columns through BigtableTableSchema would misdiagnose multiple scalar inputs as multiple row keys.
