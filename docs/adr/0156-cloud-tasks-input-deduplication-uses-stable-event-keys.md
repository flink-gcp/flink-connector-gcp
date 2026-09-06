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

# ADR-0156: Cloud Tasks input deduplication uses stable event keys

- Status: Accepted
- Date: 2026-09-06
- Issues: [#1238](https://github.com/flink-gcp/flink-connector-gcp/issues/1238), [#1240](https://github.com/flink-gcp/flink-connector-gcp/issues/1240)
- Refines: [ADR-0104](0104-exactly-once-modes-use-service-native-replay-protection-and-pass-a-performance-gate.md)'s Cloud Tasks disposition
- Modules: cloudtasks
- Current behavior: `docs/content/docs/connectors/datastream/cloudtasks.md`

## Context

The intended use case is a source such as Pub/Sub delivering the same logical event more than once, with Cloud Tasks suppressing the resulting duplicate task creation.
Protection within Google's published name-retention semantics is useful even though re-creation after that protection ends remains possible.
This is separate from preventing Cloud Tasks itself from dispatching one task more than once.
Google explicitly permits [duplicate handler execution][duplicate-execution].

The proposal in [#1240](https://github.com/flink-gcp/flink-connector-gcp/issues/1240) assigns a new persisted random identity to each accepted non-null record, stages the task in Flink state, and creates it after checkpoint completion.
That identity survives recovery of the same staged envelope.
It does not identify two separately accepted input records as the same event.
Two copies of a Pub/Sub event arriving in one successful checkpoint receive different identities and create two tasks, even with perfect clocks, no failures and unlimited name retention.
Forbidding `taskIdExtractor` and Table `task-id` metadata in that mode removes the existing mechanism for identifying those duplicate inputs.

Today's sink already accepts a stable application key through `taskIdExtractor(...)` or writable `task-id` metadata.
It hashes that key with SHA-256, composes the full name in the resolved queue, and treats `ALREADY_EXISTS` for a named task as success.
The [creation reference][create-task] describes name collisions; the applicable configured [Queue `tombstoneTtl` contract][queue] protects a deleted or executed task's name for that duration.
[ADR-0154](0154-support-follows-published-google-cloud-specifications.md) permits relying on those published semantics without a private vendor assurance.

## Decision

Use the existing stable-key path for duplicate-input suppression and defer the proposed random-identity staged-commit mode.
No new delivery option, public type, builder method, Table metadata, metric or persisted state format is adopted by this decision.
The existing at-least-once default and [ADR-0048](0048-the-cloud-tasks-sink-owns-its-retry-loop-and-never-creates-queues.md)/[ADR-0049](0049-exactly-three-cloud-tasks-failures-are-routed-and-the-argument-half-never-scans.md) remain in force.
In particular, the default fail-job policy remains the prerequisite for the existing at-least-once delivery statement, serializer null remains an intentional counted skip, and queue administration remains operator-owned.

The supported identity is an immutable logical task in one queue.
For [Pub/Sub redelivery][pubsub-exactly-once], the message ID remains the same.
That ID is unique within its topic, so a sink merging topics must also include the topic's identity in the key.
For separately published copies of a business event, use an application event ID stable across those publications; different publications may have different message IDs.
An entity ID such as an order ID is insufficient when several legitimate events for that entity must each create a task.

Both copies must resolve to the same queue and key, and their intended task definition must remain the same.
A stable Pub/Sub message ID alone does not make a serializer deterministic: a serializer can derive a new schedule or body from the current time or mutable external state.
The sink does not compare definitions on `ALREADY_EXISTS`, and the winning creation is not replaced by a later payload or schedule.
When a changed definition represents a new task, include its event/content/schedule version in the key.
The promise is bounded named-task deduplication; name expiry, unsupported administrative histories and operator replays outside that protection can produce another physical task.
The connector does not promise exactly-once handler execution.

Checkpoint-coordinated visibility is a separate possible requirement: creation only after a completed checkpoint would keep uncompleted checkpoint records out of the queue.
It is not required to collapse duplicate input, and stable-key deduplication does not provide it today.
This decision does not reinterpret staging as merely a performance optimization or claim the existing sink has a committer.
The G1 protocol gate in [#1240](https://github.com/flink-gcp/flink-connector-gcp/issues/1240) has not accepted an `EXACTLY_ONCE` protocol, and this issue's disposition must not unblock [#1242](https://github.com/flink-gcp/flink-connector-gcp/issues/1242) or its dependent implementation stages.
The independent performance gate in [#1241](https://github.com/flink-gcp/flink-connector-gcp/issues/1241) remains a performance decision under ADR-0104; a passing measurement would not establish the missing protocol property.

## Recovery analysis

The useful service condition is precise: requests with the same full name cannot create another task while the live task or its tombstone reserves that name.
It is legitimate to document that finite condition and the possibility of duplicates afterwards.
It does not establish that every effect of a recovered connector remains inside the condition.
An ordinary [ambiguous timeout][rpc-code] is possible within the published specification; it is not evidence of a service contract violation.

For the stronger connector claim, retain ADR-0104's sufficient-condition model.
Let `t0` be a durable origin established before any create, `H` the minimum applicable tombstone retention over the supported queue lifecycle, `E` the maximum elapsed-time underestimation, `S` the maximum time from the last authorization check to the last possible create effect, and `R` the authorized replay age.
An effect authorized at measured age at most `R` occurs no later than `t0 + R + E + S`.
The first creation and removal happen at `c` and `d`, with `t0 <= c <= d`; the name is protected until at least `d + H >= t0 + H`.
Thus `R + E + S < H` suffices, conditional on live-name exclusion and exclusive identity ownership in the supported queue lifecycle.
Equality is not a safety margin.

An absolute client RPC deadline closes some pre-send suspension cases, but [gRPC cancellation][cancellation] does not itself stop arbitrary server work.
Consider an old create whose outcome remains unknown, a replacement that creates the same identity within its authorization budget, execution/removal of that task, expiry of its tombstone, and a later effect of the old create.
The service can accept that last create after name release without violating its documented name-retention condition.
A local check on the replacement does not fence the old effect.
This is a logical counterexample to the stronger client-only protocol, not an observed Cloud Tasks incident or an assertion about its normal RPC latency.

Calling effects after retention expiry outside the guarantee makes the narrower service-conditional claim valid; that is the named-task behavior already supported.
It does not prove a connector-enforced physical-creation bound for all attempts authorized inside `R`.
This record adopts neither a numerical `E`/`S` nor a recovery-window default, and does not require exhaustive G0 probes or individual vendor confirmation.
A future protocol must identify which of these two claims it actually offers.

## Flink lifecycle evidence

Source inspection used Flink 1.20.4, 2.2.1 and 2.3.0, matching the LTS and current supported minors.
The inspected writer, committer, request and collector-manager source files are identical between 2.2.1 and 2.3.0.
The 1.20.4 implementation differs in construction/compatibility details and collector merging, but the callback order and synchronous retry behavior below are the same.
These are framework observations, not evidence of a production Cloud Tasks staged sink.

| Surface | 1.20.4 | 2.2.1 / 2.3.0 | Design consequence |
|---|---|---|---|
| Writer checkpoint | `flush(false)`, `prepareCommit`, emit committables before writer `snapshotState` | Same | An emitted envelope must be captured downstream in the checkpoint; clearing the writer list is not itself durable ownership transfer. Failure of that checkpoint restores earlier state/source positions. |
| Commit authorization | `notifyCheckpointComplete` commits collected checkpoints up to the completed ID | Same | A test must distinguish serialization/snapshot from checkpoint completion. Completed checkpoints may authorize incremental, non-atomic task creation. |
| Restore | Recovered committables are committed in `initializeState`, before `open` | Same | Validate recovery deadlines and prerequisites before any restored create, not only in a later callback. |
| Retry | `retryLater` increments the request count; the manager immediately calls `commit` again in one bounded loop | Same | `sink.committer.retries = N` allows `N + 1` callback rounds in that invocation. There is no delayed mailbox retry implied by the method name. |
| Callback return | Requests left without an error/retry signal become committed | Same | Returning while asynchronous work is unresolved loses it from Flink's pending set. `signalFailedWithKnownReason` also finalizes a failed request instead of enforcing fail-job retention. |
| Residence and ordering | One checkpoint manager must finish before the next; remaining requests after the retry loop throw | Same | Budget the aggregate callback residence over all pending checkpoints, not just one RPC timeout. Initialization and the task's other work can be delayed. |
| End of input | Writer flushes with `true` and emits; checkpoint-disabled/batch committers can commit without a completed checkpoint | Same | Bounded/end-of-input and stop-with-savepoint require separate job-level evidence before inclusion in a future mode. |
| State redistribution | List operator state for writer and serialized committable collectors | Same mechanism; collector merge implementation differs | Rescaling must preserve envelope identity and owners; constructor/API compatibility alone does not prove redistribution correctness. |

`CloudTasksStagedCommitLifecycleTest` drives the actual writer/committer factories with a service-free test sink.
Its five deterministic cases cover pre-barrier ordering and no creation before completion, repeat completion, replay of the original serialized committable during initialization, synchronous retry counts/failure, and the different checkpointed/checkpoint-disabled end-input paths.
The callback thread assertion observes the harness caller; the production task-thread consequence follows the operator callback control flow, not a simulated transport callback.
The harness does not run a JobManager or prove externalized checkpoint retention after FAILED/CANCELED, stop-with-savepoint, rescaling, Cloud Tasks retention, or any service effect bound.

Relevant upstream source families are the [1.20.4 sink operators][flink1-operators] and [2.3.0 sink operators][flink2-operators], especially `SinkWriterOperator`, `CommitterOperator`, `StatefulSinkWriterStateHandler`, `CheckpointCommittableManagerImpl` and `CommitRequestImpl`.
The corresponding 2.2.1 sources were read from the pinned Maven source jars.

## Requirements before a different staged proposal can be adopted

A future design must establish a use case for checkpoint-coordinated creation and explicitly choose whether duplicate input is in scope.
It must not claim random identities collapse separately accepted logical duplicates.
The following requirements preserve the unresolved work without publishing speculative API or wire-format commitments.

| Invariant | Required design and failure oracle |
|---|---|
| Immutable recovery identity | Persist full queue and Task bytes, a stable identity, pre-create origin, numeric retention and version/mode header even when empty. Duplicate inputs and distinct legitimate events must have separately specified identities. Restore/rescale must not regenerate names or refresh deadlines. |
| Durable ownership | Specify writer-to-committer transfer for normal, aborted and subsumed checkpoints. Inject failure at each boundary; either a completed checkpoint owns the envelope or earlier source/state recovery reproduces input that has never been created. |
| Ambiguous/partial outcomes | Model creation followed by response loss, success of only part of a checkpoint, and a stale attempt after a replacement. No unresolved request may become success; only documented same-name collision outcomes can count as prior creation. |
| Recovery budget | Derive elapsed-time uncertainty across machines and restores; never persist cross-process `nanoTime`. Check before every send and set the RPC's absolute deadline to the smaller remaining authorization/RPC budget. A fake late effect must be able to falsify the stronger timing claim despite a local timeout. |
| Terminal state | Use durable externalized checkpoints retained on failure/cancellation and a bounded deployment restart policy, rather than assuming an arbitrary exception disables restart. A MiniCluster/deployment test must reach FAILED and CANCELED, retain readable unresolved state, and reject every expired restart before sending. |
| Heap capacity | Account for buffered envelopes, pending checkpoints, objects, serializer and restore copies. Fail with a sizing diagnostic at the staging cap; a test must show `write()` never waits for a barrier blocked behind that same write. |
| Validation boundary | Separate builder/planner/runtime checks from unobservable queue history, clock bounds, exclusive deployment ownership and durable-storage prerequisites. Verify the applicable queue configuration without creating or updating queues; do not infer history from current readback. |
| Scope and migration | Prove checkpointed STREAMING and one-job stop-with-savepoint continuation first. Reject unsupported batch, disabled checkpointing, bounded final commit and incompatible state versions. Test empty headers and rescaling. Historical/forked restore and dropping unmapped state remain excluded; a stateless downgrade cannot be claimed detectable without a mechanism. |
| Failure policy and operations | Specify null skips, supported task-ID/failure-handler combinations, metrics and pre-staging connector-owned validation. Distinguish immutable poison envelopes from repairable environment errors. Retained invalid/expired state needs an operator runbook; manual loss/duplicate-risk recovery and handler execution are outside a physical-creation guarantee. |

## Independent consultation

An independent `claude-fable-5-1` consultation on 2026-09-06 examined public repository excerpts about Cloud Tasks, Pub/Sub and ADR-0154 with no tool access, private memory, local configuration or implementation edits supplied.
Its focused use-case assessment agreed that random per-record identities do not collapse duplicate input and that existing stable task keys address the stated retention-bounded use case.
The recommendation was checked against `CloudTasksWriter`'s naming and `ALREADY_EXISTS` branches and `CloudTasksTaskCreationITCase`'s named/unnamed and queue-scope controls.

Two qualifications were retained when incorporating that advice.
Staging has a checkpoint-visibility benefit distinct from request count, even when stable-key naming already suppresses re-creation.
Also, equal source message IDs do not by themselves guarantee equal serialized task definitions.
The consultation is design input; tests and source inspection have the specific limits recorded above.

## Alternatives declined

- Add the proposed random-identity mode to deduplicate Pub/Sub input: two accepted copies still have distinct task names, so the motivating duplicate survives.
- Infer event identity from equal payload bytes: identical content can represent two legitimate events, while differently serialized copies can represent the same event.
- Call checkpoint staging exactly-once handler execution: Cloud Tasks permits duplicate dispatch of a single task.
- Treat client timeout as non-creation or automatic fencing: an ambiguous result is a supported API outcome, and cancellation alone supplies no last-effect bound.
- Reopen the blanket G0 stop or demand a private service assurance: ADR-0154 already permits relying on published name protection, which is sufficient for the existing stable-key use case.

[create-task]: https://docs.cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues.tasks/create
[queue]: https://docs.cloud.google.com/tasks/docs/reference/rest/v2beta3/projects.locations.queues
[duplicate-execution]: https://docs.cloud.google.com/tasks/docs/common-pitfalls#duplicate_execution
[pubsub-exactly-once]: https://docs.cloud.google.com/pubsub/docs/exactly-once-delivery#redelivery_versus_duplicate
[rpc-code]: https://docs.cloud.google.com/tasks/docs/reference/rpc/google.rpc#code
[cancellation]: https://grpc.io/docs/guides/cancellation/
[flink1-operators]: https://github.com/apache/flink/tree/release-1.20.4/flink-streaming-java/src/main/java/org/apache/flink/streaming/runtime/operators/sink
[flink2-operators]: https://github.com/apache/flink/tree/release-2.3.0/flink-runtime/src/main/java/org/apache/flink/streaming/runtime/operators/sink
