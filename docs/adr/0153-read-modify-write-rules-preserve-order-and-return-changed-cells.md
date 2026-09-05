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

# ADR-0153: Read-modify-write rules preserve order and return changed cells

- Status: Accepted
- Date: 2026-09-06
- Issue: [#1180](https://github.com/flink-gcp/flink-connector-gcp/issues/1180)
- Refines: ADR-0148

## Context

Append and increment operate on the current contents of a raw Bigtable cell.
ADR-0148 provides their request-response runtime, but applications need a serializable request contract and SQL needs a declared interpretation of each input cell.
The [Data API](https://cloud.google.com/bigtable/docs/reference/data/rpc/google.bigtable.v2#readmodifywriterowrequest) applies rules in order and returns the final contents of the cells modified by the request.

## Decision

Expose immutable, serializable `ReadModifyWriteRequest` and `ReadModifyWriteRule` models under `sink.readmodifywrite`, annotated `@PublicEvolving`.
A request contains a nonempty binary row key and between one and 100,000 ordered rules.
An append carries bytes; an increment carries a signed Java `long`.
DataStream callers may mix operations and address the same cell repeatedly in one request.
Neither the model nor its adapter sorts, combines or removes rules.
SDK request builders remain internal and receive the resolved destination at submission.
Empty append values are rejected, matching the pinned Java SDK's builder; zero and negative increments remain valid.
Binary qualifiers may be empty.

The sink discards successful responses after recording completion.
The async helper emits the original input paired with a `ReadModifyWriteResult` containing the actual destination and a connector-owned `BigtableRow`.
The latter holds changed cells, with their raw values and service timestamps, rather than a complete row snapshot or a response per rule.
The runtime converts the SDK response once; constructing the result does not repeat resolution or serialization.
Request configuration crosses the job graph through Java serialization, while emitted results use explicit Flink type information and a versioned field serializer that composes `BigtableRowSerializer`.
`FailedRequest` retains its null payload because a durable dead-letter encoding is not defined here.

The helpers follow ADR-0152's capacity, timeout, serializer lifecycle and input-correlation contracts.
They expose normal `orderedWait` and `unorderedWait`, with no retry entry points.
Ordered output does not order independent RPCs against the same row.
The shared runtime makes one attempt, drains accepted sink requests at checkpoints, and fails ambiguous outcomes with an explicit replay diagnostic.
A recovery can apply an already committed append or increment again.
The existing request metrics count submitted attempts within the running subtask; they cannot identify unique records or deduplicate applications across recovery.
Single-cluster routing with single-row transactions enabled is required, without a new admin lookup or permission in the data path.

SQL extends the existing `sink.write-mode` with `append` and `increment`; `upsert` remains its default.
Each DDL fixes one operation and one project, instance, table and sink application profile.
It retains the existing row-key and family/qualifier schema.
Append accepts CHAR, VARCHAR, BINARY and VARBINARY cells using the existing codec; increment accepts BIGINT cells as signed increments.
The DDL types describe each input operand, not a bound on the accumulated stored value.
Rules follow family and qualifier declaration order.
NULL families and cells omit operations, regardless of `null-string-literal`; an input with no remaining rule fails.
A null or empty row key and an empty append value fail too.
Only INSERT input is accepted, preserving repeated inputs rather than materializing an upsert.
The SQL sink waits for completion and discards the returned row; result-emitting SQL functions remain in [#1181](https://github.com/flink-gcp/flink-connector-gcp/issues/1181).

The Table mapper reuses the shared request options and rejects explicitly supplied batch, auto-creation, repair, conditional-only and insert-only compatibility settings.
Read-modify-write chooses its timestamps at the service, so writable cell-timestamp metadata and its truncation setting are rejected.
Source-side options retain their existing uses when one DDL is also read.

The connector sends increment operands unchanged and delegates arithmetic, including overflow and invalid stored-cell handling, to Bigtable.
It performs no preliminary read, local addition, saturation or independent overflow rejection.
The documented API requires an existing increment target to contain an eight-byte big-endian signed integer; the service decides the result or rejection of the whole atomic request.
Boundary observations belong in the real-service validation record, not in a promise inferred from an emulator.
When `AddToCell` expresses the workload, [aggregate cells](https://cloud.google.com/bigtable/docs/writes#appends) are recommended; read-modify-write remains available for raw families and callers needing the returned state.

## Validation

Model, fake-client and Flink job tests cover ordered and repeated rules, append, signed increments, response conversion, serialization, failures, cancellation and checkpoint completion.
Table planning and execution tests cover mode-specific types, NULL omission, rejected options and repeated INSERT inputs on the supported Flink versions.
Credential-gated real-service acceptance covers rule order, returned cells and direct reads, arithmetic boundaries, malformed stored values and application-profile prerequisites.
Its fixture and cleanup require explicit resource approval before execution.

## Alternatives

Publishing a mutable SDK request would couple the API and job configuration to the client library.
A map keyed by column would lose repeated rules and their order.
A read followed by a client-side write would lose atomicity under concurrent updates.
Per-record SQL opcodes and numbered destination options add a second SQL schema for a contract the existing family/qualifier form can express.
An automatic retry cannot determine whether an unacknowledged non-idempotent request committed.
