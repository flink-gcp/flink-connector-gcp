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

# ADR-0152: Conditional writes own their request and outcome

- Status: Accepted
- Date: 2026-09-05
- Issue: #1179; general SQL commands follow in #1226
- Refines: ADR-0148

## Context

The single-row runtime can issue CheckAndMutateRow but has no public conditional request API.
Its sink completion callback currently discards the Boolean response.
A successful false response may execute a nonempty false branch, so treating false as a failed write would lose the operation's meaning.

## Decision

Expose immutable, serializable connector-owned filters, ordered mutations, requests and results under `sink.conditional`, annotated `@PublicEvolving`.
SDK `Mutation` and `Filters.Filter` are serializable, but mutable SDK builders must not define the connector's public API or become shared job configuration.
An internal adapter builds them for each invocation using the actual resolved table.
The request contains a row key, a filter and two ordered mutation lists; at least one list must be nonempty and neither may exceed 100,000 mutations.
Filters select cell sets and compose through chain or interleave; a chain is not boolean AND across distinct cells.
Latest-value equality first selects one column, then its latest version, then compares the bytes.
Nested condition filters are not exposed because CheckAndMutateRow does not support them.

SetCell, DeleteCells, DeleteFamily, DeleteRow, AddToCell and MergeToCell retain their order.
Aggregate values distinguish raw bytes (`raw_value`), typed bytes (`bytes_value`) and a typed int64 (`int_value`).
ADR-0041's 2026-09-05 service measurement found that Int64 Sum MergeToCell requires `bytes_value`, which SDK 2.82.0's `Value` wrapper cannot represent.
The adapter builds mutation protobufs and wraps their ordered list through `Mutation.fromProtoUnsafe`; the connector model owns timestamp and per-branch count validation.
This wrapper is `@BetaApi` in SDK 2.82.0; the dependency remains internal, and compilation plus the ordered wire-mutation tests check it on client upgrades.
It bypasses the SDK mutation builder's 200 MiB byte-size guard; the connector does not promise a local request-byte bound.
`AggregateValue.bytes` preserves the typed transport variant without exposing a protobuf or SDK value type to user serializers.
The gated conditional test reads an accumulator produced by Bigtable before merging it, without assuming the accumulator's encoding.
Explicit timestamps retain their microseconds; only SetCell accepts the service's -1 timestamp sentinel.
The SDK adapter uses the server-timestamp-capable protobuf wrapper internally.
Aggregate timestamps are required and nonnegative, including zero.
The table serializer retains ADR-0149's per-cell millisecond writer clock when metadata supplies no timestamp.
Both SQL write modes reject metadata before the Unix epoch, including -1 microsecond: a SQL instant must not become the DataStream API's server-time sentinel.
This check runs before optional millisecond truncation; epoch zero remains valid.

The sink discards results after recording completion and conditional outcomes.
The async helper emits the input and an explicitly serialized result containing the resolved destination, row key, predicate match and whether the selected mutation list was nonempty.
Neither resolver nor serializer is called again to construct the result.
Request models cross the job graph through Java serialization; emitted outcomes use a versioned Flink field serializer.
`FailedRequest` retains its null payload: a durable dead-letter wire format is not defined by Java job-graph serialization.
The async helper uses normal orderedWait/unorderedWait with `maxInFlightRequests` as operator capacity and an explicit operator timeout above `requestTimeout` after Flink's truncation to milliseconds.
The public helper exposes no retry entry points.
The shared runtime retains the timeout repair delivered by #1203, including its completion and ledger-ordering guarantees.
Ordered output does not order independent requests against the same row.

`EmptyBranchPolicy.IGNORE` is the default; `FAIL` fails the job after recording the successful RPC and predicate outcome.
This policy concerns the selected list, not whether its mutations changed stored bytes, and bypasses dropping failure handlers.
Async counters remain thread-safe and sink outcome processing remains on the task thread.
The runtime still makes one RPC attempt with the SDK deadline and no connector retry.

The common SQL option `sink.write-mode` defaults to `upsert` and adds `insert-if-absent` here.
The latter keeps the ordinary family/qualifier schema and accepts only INSERT input.
An unset RPC predicate tests for any cell in the stored row, including undeclared families; the true branch is empty and the false branch writes the input cells.
Batching, byte flow-control, automatic creation/repair and insert-only compatibility options are rejected when explicitly supplied to this mode.
Conditional options are rejected under ordinary upsert.
Issue #1177 later adds keep-latest to this same option, and #1226 adds DDL-defined commands with named predicates and numbered WITH options.
JSON and result-emitting SQL functions are outside this change.

Conditional writes require single-cluster routing with single-row transactions enabled.
An app-profile ID alone does not justify an admin lookup or another permission requirement; service failures retain their cause and explain the routing constraint.
ADR-0148's status classification remains unchanged: an unambiguous `INVALID_ARGUMENT` reaches the sink failure handler even if the cause is a profile rejection; `FAILED_PRECONDITION` fails the job.
The routing hint states a prerequisite, not a diagnosis of every failure.
A dropping handler can therefore discard a profile rejection reported as `INVALID_ARGUMENT`; the default handler fails the job, and the async surface routes no failure to a handler.
The 2026-09-05 gated run observed `FAILED_PRECONDITION` with both a transaction-disabled single-cluster profile and a multi-cluster profile.
Both reported that single-row transactions were not allowed by the profile; this measurement does not promise one status for every invalid profile configuration.
Row-level atomicity does not imply exactly-once delivery: an applied request with a lost acknowledgement can replay and select a different branch.
In particular, fail-on-empty insertion can fail repeatedly after recovery because the first attempt already created the row.
Checkpoint completion drains accepted sink requests; it does not commit Bigtable transactions.

## Validation

Model and fake-client tests hold immutable requests, binary equality, ordered branches, skips, outcome counts, policy failures, cancellation and correlation.
Serializer and job tests hold job-graph transport and emitted type snapshots without a generic Kryo fallback.
Table planning and execution tests cover supported Flink versions, ordinary upsert regressions and repeated same-row INSERT inputs.
Real-service acceptance is credential-gated and needs explicit resource and cleanup approval; emulator evidence is not evidence of application-profile or aggregate service semantics.
After approval, all four cases in `BigtableConditionalRealGcpITCase` passed on 2026-09-05 with Flink 2.2.1 and Bigtable SDK 2.82.0, without failures, errors or skips.
The run covered both predicate outcomes, DeleteRow followed by SetCell and Int64 Sum AddToCell/MergeToCell yielding 5, SQL whole-row insert-if-absent with an undeclared family, and both incompatible profiles above.
The profile assertions inspect the preserved service status and explanation because Flink may transport a vendor exception as `SerializedThrowable`.
The single ephemeral one-node instance was deleted by teardown, and a subsequent instance listing independently confirmed its absence.

## Alternatives

Publishing SDK builders would couple API evolution and aliasing to the client library.
A read followed by a write would not provide an atomic insert-if-absent operation.
Flink ON CONFLICT controls planner/job behavior rather than the destination's atomic operation.
A discarded-result counter would duplicate every successful sink completion.
An automatic retry would conceal ambiguous outcomes, especially for aggregate mutations and conditions whose truth changes after the first attempt.
