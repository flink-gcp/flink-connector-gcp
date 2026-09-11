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

# ADR-0163: Bigtable checkpointed writes stage immutable mutations and retain row markers

- Status: Accepted
- Date: 2026-09-07
- Issue: [#1211](https://github.com/flink-gcp/flink-connector-gcp/issues/1211)
- Supersedes: only ADR-0104's unsettled Bigtable staged-mode decision and event-identity requirement for that mode
- Modules: bigtable
- Current behavior: `docs/content/docs/connectors/delivery-guarantees.md`

The staged runtime and both API surfaces are implemented under [ADR-0165](0165-bigtable-implementation-precedes-final-stage2-acceptance.md), which moves final Stage 2 acceptance after implementation.
Release and the full service evaluation remain pending under [#1211](https://github.com/flink-gcp/flink-connector-gcp/issues/1211).

## Context

The Bigtable sink currently sends mutations before checkpoint completion and remains at-least-once.
[ADR-0104](0104-exactly-once-modes-use-service-native-replay-protection-and-pass-a-performance-gate.md) declines a common committer around eager writes because a committer cannot retract their visible effects.
The owner chose a Bigtable-specific staged mode: its writer does not reach the target table, and its committer first writes after checkpoint completion.
That changes the premise, while retaining ADR-0104's performance gates and the other connectors' decisions.

The owner clarified two requirements during design.
Recovery means resuming checkpoint-owned work without applying it twice, not rolling Bigtable back when an operator manually selects an older snapshot.
Replay markers are retained without automatic deletion; their storage cost is measured and documented instead of imposing a time-limited recovery window.
This first change records the design and local protocol evidence; it exposes no new sink mode and leaves #1211 open.

## Delivery contract

The planned `EXACTLY_ONCE` mode protects each staged mutation envelope once its owning checkpoint has completed, while its marker remains present and the deployment uses the supported routing and recovery conditions below.
An envelope is one accepted serializer result for one destination row, not an application event identified outside Flink.
Separate inputs remain separate envelopes even if their contents are identical.
Serialization returning null skips the input before staging under ADR-0001.

No target mutation is sent before checkpoint completion; completion authorizes commit rather than making all rows visible at one instant.
Rows become visible individually as their conditional commits succeed.
There is no cross-row atomicity, cross-table atomicity, ordering across writer subtasks, or rollback of previously committed effects.
An application timestamp and row key can still make two distinct writes address the same cell version; exactly-once application does not turn those writes into two stored versions.

The guarantee assumes recovery from the latest completed checkpoint with the sink's state preserved, or a successfully finished stop-with-savepoint resumed from that savepoint.
Discarding checkpoint-owned committables can lose records even though the source resumes after them.
Manually restoring an older snapshot cannot remove later Bigtable effects and may re-stage its source replay with new identities.
External deletion of markers, family/table recreation, and concurrent deployments that mutate the reserved family are outside the guarantee.

A stop-with-savepoint that fails to reach FINISHED is also outside the guarantee.
As recorded and source-inspected on both Flink lines in [ADR-0158](0158-cloud-tasks-checkpointed-creation-stages-named-tasks-and-commits-after-the-checkpoint.md#flink-lifecycle-on-the-supported-lines-2026-09-06), Flink notifies that savepoint complete but does not put it in the completed-checkpoint store.
Selecting the preceding checkpoint after the savepoint's mutations have committed gives replayed inputs new envelope identities.
Operators must resume from the completed savepoint after such a failed stop to preserve those identities.
The local writer-close failure reports a non-recoverable `StopWithSavepointStoppingException`; it does not demonstrate automatic fallback to the preceding checkpoint.
The operator probe simulates those notifications and replay.
The [local MiniCluster harness](evidence/0163-bigtable-local-staged-harness.md) now exercises a writer-close failure after the stop savepoint is created and contrasts explicit recovery from that savepoint with recovery from the preceding retained checkpoint.
It does not claim to have observed automatic fallback; production-runtime acceptance remains pending.

## Staging and checkpoint ownership

Use Flink committer operator state for the initial mode, without an external staging table.
The writer resolves and serializes once, freezes the destination resource, row key, application profile, ordered mutations including timestamps, marker family and a random 128-bit identity, and stages the immutable request.
Generate the identity with `SecureRandom` and encode its sixteen bytes as 32 lowercase hexadecimal qualifier characters.
Neither recovery nor rescaling regenerates a persisted identity or calls the serializer/resolver again.

`prepareCommit()` transfers every staged request and clears the writer's list.
Flink emits these committables before its writer snapshot hook and snapshots the committer's collector; the writer-to-committer edge is aligned.
The writer needs no `SupportsWriterState` and must not keep a second checkpointed copy of the same requests.
A checkpoint that fails without a later completed snapshot can discard its unsent envelopes and let the source replay them under new identities.
A later completed checkpoint includes earlier pending intervals, including an aborted checkpoint's committables.

The owning checkpoint's snapshot predates the commit attempts.
Restoring it therefore re-commits even requests a prior process applied successfully; the same-row marker absorbs those attempts.
Flink initializes restored committables before `open`, so format, schema, profile and destination checks must precede every possible restored send, not live only in writer startup.
Version the committable serializer explicitly and reject incompatible versions before any request is submitted.
The production envelope must also validate its reserved-family and marker structure on deserialization; protobuf parsing alone is not that validation.

The initial writer limits are 100,000 staged entries and 64 MiB of serialized request size plus a 256-byte accounting allowance per entry, both configurable through a dedicated staged-options object.
The allowance is an admission charge, not a measured upper bound on Java object memory.
Crossing either limit fails synchronously with the limits, retained counts and a checkpoint-interval sizing diagnostic.
Waiting for room on the writer thread would prevent the next barrier from releasing the list.

These limits bound an interval, not total committer heap.
Flink can retain one batch for each barrier since the last notified completion; ordinary savepoints and delayed notifications prevent a fixed bound derived only from concurrent-checkpoint settings.
Size the deployment from pending committables, snapshot copies and observed heap, and require the checkpoint timeout to cover accumulated conditional work.
The local [sizing record](evidence/0163-bigtable-staged-state-sizing.md) distinguishes measured wire/snapshot bytes, sampled heap and extrapolations.
An external staging table would add pre-checkpoint service writes, staging-row identities, retrieval and orphan cleanup; it is not included in the initial mode.
If the service evaluation finds Flink staging impractical for a workload, record that workload as unsupported or revise this decision before implementing another staging backend.

## Atomic replay marker

Reserve a dedicated raw column family, configurable and required explicitly, outside the application's data families.
The probe calls it `flink_commit`; the production API must not silently select or create that name.
It has no GC rule, and the application must preserve it and its marker cells across every allowed restore.
One marker qualifier is the envelope identity, with value `1` and timestamp zero.
Timestamp zero makes the stored marker version fixed; it is safe only with no age-based GC on this family.

Build a `CheckAndMutateRow` predicate selecting exactly that family and qualifier.
The true branch is empty.
The false branch applies the original ordered mutations and appends the marker mutation in the same row-atomic operation.
Google documents this [conditional read and write as one atomic action](https://docs.cloud.google.com/bigtable/docs/writes#conditional-writes).
Keep at most 99,999 user mutations so the marker fits within the documented 100,000-mutation branch limit.

A row-wide highest checkpoint ID is insufficient: the second record in the same checkpoint, or a different subtask's record, would look already committed.
Per-envelope qualifiers distinguish them without depending on subtask numbering or checkpoint-ID reuse.
No separate per-subtask committed-checkpoint ledger is needed; it could not resolve a crash between individual row effects, while the row marker does.
Identity collisions remain the probabilistic limit of the 128-bit construction, not an assertion that random generation is mathematically collision-free.

Reject `DeleteRow` and any mutation addressing the reserved family before staging.
Deleting that family's old markers followed by writing the current marker would allow a delayed retry of an older envelope to apply again.
Data-family and selected-column deletes can preserve the markers, but leave the physical row present when all application cells have been deleted.
Whole-row existence predicates and row-key-only scans can observe that row; ordinary data-family filters hide its cells, not necessarily every keys-only query.
The API documentation and Table delete tests must describe this consequence instead of promising physical row deletion.

Markers grow by one cell per applied envelope, including on hot rows.
They have no automatic TTL or connector sweeper.
Manual cleanup is allowed only after retiring every corresponding checkpoint/savepoint and stopping all writers/committers that could submit those envelopes; a timeout alone does not establish that an old RPC cannot still apply.
The marker footprint, hot-row growth and storage cost are part of Stage 2's workload limits.

## Runtime and API integration

Use the existing single-row client lifecycle and request construction seams under `sink.singlerow`, with a committer stage that drives bounded requests without a writer mailbox executor.
Preserve cancellation of the original RPC future.
On successful application, finish the committable; on a predicate match, call `signalAlreadyCommitted()`.
For an ambiguous outcome or any other failure after staging, fail the commit without a dropping failure handler; restore retries the unchanged envelope.
The SDK still makes one attempt with its deadline, and no automatic retry is added to the existing conditional or read-modify-write APIs.

Require an explicit application profile using single-cluster routing and single-row transactions.
All conflicting conditional writes must reach the same cluster, as Google's [routing contract](https://docs.cloud.google.com/bigtable/docs/routing#single-row-transactions) requires.
Build/plan validation checks supplied options; it cannot infer a remote profile's policy from its ID.
Before any target send, including restore initialization, read the actual profile and table metadata and reject an incompatible profile, absent/typed/GC-managed marker family, or unavailable validation permission.
This mode deliberately adds `bigtable.appProfiles.get` and `bigtable.tables.get` prerequisites; existing at-least-once sinks keep their current validation and permission behavior.
For dynamic destinations, validate each resolved destination before its first send and keep the original resolved resource in state.
Provisioning the reserved family and preventing administrative changes remain operator responsibilities; startup readback does not lock service configuration.

The later API change adds a delivery-guarantee selector to `BigtableSinkBuilder` and `sink.delivery-guarantee` to SQL, defaulting to `AT_LEAST_ONCE`.
`sink.write-mode` remains independent.
The initial compatible modes are `upsert`, `keep-latest` and `aggregate`; general conditional requests, `insert-if-absent`, `append`, `increment` and the async APIs are excluded.
Nested conditional filters are not supported by `CheckAndMutateRow`, so the user predicate cannot simply wrap the marker predicate.
Table layer option checks must name the SQL keys through `OptionSetters` and preserve the original builder diagnostic.
The Table runtime must stop assuming every result of `BigtableSinkBuilder.build()` is a `BigtableMutateRowsSink`, including aggregate-family validation.

Support streaming execution with exactly-once checkpointing and checkpoints after tasks finish, including bounded streaming inputs.
Reject batch/automatic execution, disabled checkpointing and at-least-once checkpoints at graph construction, using the existing pre-commit topology validation pattern.
An end-of-input committable has the next checkpoint ID, not a sentinel that can enforce this restriction.
Expose staged entry/byte gauges and deduplicated request counts when the runtime ships; use Flink's pending-committable metric for collector backlog.

## Evidence and acceptance

`BigtableStagedCommitLifecycleTest` drives Flink's actual writer and committer operators with `StagedMutationTestSink`.
The fake evaluates the emitted marker predicate and atomically applies its exercised SetCell/AddToCell subset; atomic service behavior is an assumption from Google's specification, not something the fake proves.
The tests cover no sends before completion, distinct same-row inputs, response loss mid-commit, identical restored wire requests, repeated restoration, an uncompleted tail, aborted intervals, 2-to-1/3 rescaling, checkpointed end of input, state-discard loss, marker protection, staging caps and state-format rejection.
They also show the absence of rollback and the fresh-identity replay hazard when completion is notified for a snapshot older-checkpoint recovery does not retain.
All twelve cases passed on Flink 2.2.1 and 1.20.4 on 2026-09-07, without failures, errors or skips.
The original operator harness invokes checkpoint hooks explicitly; it does not exercise JobManager checkpoint selection, real transport, schema preflight, graph validation or production memory limits.
The subsequent [local MiniCluster harness](evidence/0163-bigtable-local-staged-harness.md) adds coordinator-driven checkpoint recovery, stop/rescale coverage, graph validation and SDK transport against an emulator.
Its local calibration remains separate from service performance and production factory acceptance.

The 2026-09-05 [#1210 measurement](https://github.com/flink-gcp/flink-connector-gcp/issues/1210) supplies the primitive baseline: conditional writes averaged 5,562 ops/s at 198 ms client p95, against bulk's 3,792 ops/s at 306 ms, under the same fresh-JVM condition.
Its AddToCell probe read 1,000 rows with sum 9 and nine markers each; every tenth submission replayed the preceding event rather than contributing a tenth distinct event.
Those are service-primitive observations, not a Flink staged-mode throughput or latency result.

The [Stage 2 protocol](evidence/0163-bigtable-staged-performance-protocol.md) covers the complete cost required before release; ADR-0165 supersedes only its ordering before implementation.
The [timed experiment harness](evidence/0163-bigtable-stage2-experiment-harness.md) adds bounded measurement inventory, metadata preflight and exact-resource lease supervision.
Its small-run record does not pass the full matrix or authorize a production API.
The [checkpoint-stall diagnostic](evidence/0163-bigtable-stage2-checkpoint-stalls.md) reproduces a later-checkpoint timeout during a long local commit invocation; it retains the service timeout and leaves the gate pending.
The [admission diagnostic](evidence/0163-bigtable-stage2-admission-diagnostics.md) adds local outstanding-input controls to distinguish admitted work from RPC concurrency without changing the formal gate.
Resource creation and execution require a separate approval of concrete targets, lifetime, operation caps and cost.
The benchmark implementation is experimental test code; it is not a public sink or permission to release one.
Real-service recovery acceptance must exercise the actual production factory and both API entry points once implemented, including response-loss recovery, aggregates, incompatible profiles, retained markers and exact-target cleanup.

## Delivery sequence

1. Preserve the accepted protocol and historical diagnostic evidence; keep #1211 open.
2. Implement and locally verify the production runtime, DataStream/Table APIs and operational documentation under ADR-0165.
3. Reconcile the existing aggregate cost authorization, freeze the reviewed production source and service plan, then perform actual-factory recovery acceptance and the unchanged formal Stage 2 evaluation.
4. Record the verdict, supported workload limits and verified cleanup before release or closing #1211.

## Alternatives declined

- Wrap the eager writer in a committer: effects already visible before completion cannot be hidden by adding an interface.
- Use only checkpoint IDs or subtask progress: these do not distinguish repeated same-row inputs or partially applied commits.
- Require application event keys: the selected contract identifies a staged envelope and must work when the application has no deduplication key.
- Automatically expire markers: this would impose a recovery deadline and permit duplicates once protection expires; the owner chose retained markers.
- Describe checkpoint completion as global atomic visibility or arbitrary rollback: the primitive commits one row at a time and does not undo earlier commits.
