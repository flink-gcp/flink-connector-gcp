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

# ADR-0158: Cloud Tasks checkpointed creation stages named tasks and commits them after the checkpoint

- Status: Accepted
- Date: 2026-09-06
- Issues: [#1238](https://github.com/flink-gcp/flink-connector-gcp/issues/1238), [#1240](https://github.com/flink-gcp/flink-connector-gcp/issues/1240)
- Supersedes: only [ADR-0104](0104-exactly-once-modes-use-service-native-replay-protection-and-pass-a-performance-gate.md)'s Cloud Tasks decision ("bounded effectively-once task creation only")
- Refines: [ADR-0048](0048-the-cloud-tasks-sink-owns-its-retry-loop-and-never-creates-queues.md) (the stateless writer becomes the default mode's property) and [ADR-0049](0049-exactly-three-cloud-tasks-failures-are-routed-and-the-argument-half-never-scans.md) (the routed failures belong to the default mode)
- Modules: cloudtasks
- Current behavior: `docs/content/docs/connectors/delivery-guarantees.md` § Cloud Tasks (unchanged until the mode ships)

This record is the G1 protocol decision of [#1238](https://github.com/flink-gcp/flink-connector-gcp/issues/1238).
It defines a delivery mode; it does not enable one.
The runtime is built by [#1242](https://github.com/flink-gcp/flink-connector-gcp/issues/1242) through [#1244](https://github.com/flink-gcp/flink-connector-gcp/issues/1244) after an applicable primitive performance gate passes, and the mode is released only if [#1246](https://github.com/flink-gcp/flink-connector-gcp/issues/1246) accepts its end-to-end cost.
The [#1241 repeat](evidence/0104-cloudtasks-stage1-1241.md#result-on-2026-09-06) was inconclusive and supplied no primitive performance pass.

## Context

The Cloud Tasks sink creates every task eagerly and flushes outstanding requests at the checkpoint barrier ([ADR-0048](0048-the-cloud-tasks-sink-owns-its-retry-loop-and-never-creates-queues.md)).
A completed checkpoint is therefore a durability boundary, not a visibility boundary: a task created for a record that a later restore replays exists twice unless the record carried a stable key through `taskIdExtractor(...)` or the Table `task-id` metadata, and a task created between two checkpoints is visible to the queue even if the job never completes the checkpoint that covers its record.

Two things follow that the existing sink cannot offer.
First, an application without a stable per-record key has no protection against a restore creating a task again.
Second, no application can keep tasks out of the queue until the checkpoint that covers their records has completed, which matters when a task's handler acts on state the same checkpoint also commits elsewhere.

[ADR-0104](0104-exactly-once-modes-use-service-native-replay-protection-and-pass-a-performance-gate.md) declined "a common two-phase-commit layer" because "a committer around an eager API cannot retract an effect already visible at the service".
That premise binds a design whose writer has already created the task.
It does not bind a design whose pre-commit never reaches the service: if nothing is created before checkpoint completion, there is nothing to retract, and the commit's replay safety rests on the same primitive ADR-0104 already accepts for this connector, a caller-chosen task name that the service rejects with `ALREADY_EXISTS` while it remembers the name.

The G0 investigation in [#1239](https://github.com/flink-gcp/flink-connector-gcp/issues/1239) recorded what that primitive does and does not promise (ADR-0104 § Cloud Tasks recovery feasibility gate).
[ADR-0154](0154-support-follows-published-google-cloud-specifications.md) then settled that published Google specifications are the assumptions a connector may rely on, and that a guarantee depending on unpublished behavior is narrowed or declined rather than held for a vendor assurance.
An earlier disposition of this issue proposed relying on the existing stable-key path alone and deferring the staged mode, because two separately received copies of one logical event receive two random identities under this design.
The owner declined that disposition on 2026-09-06: the mode must work when no deduplication key can be produced, and what the service does not guarantee becomes a documented limitation rather than a reason not to build it.
This record keeps that analysis where it is right (random identities do not collapse logical duplicates) and builds the mode around it.

## Decision

### The guarantee and its name

The Cloud Tasks sink gains an opt-in delivery mode, `EXACTLY_ONCE`, beside the existing default, `AT_LEAST_ONCE`.
In the new mode the writer creates nothing.
It stages each accepted record as an immutable, named task envelope; a Flink committer creates the staged tasks after the checkpoint that owns them has completed; and a recovered job re-creates the same names, which the service answers with `ALREADY_EXISTS` for every task it already holds or remembers.

The guarantee is **exactly-once task creation per staged envelope within the documented scope and recovery window**: no task is created for an uncompleted checkpoint, and no staged envelope produces more than one task while the queue's name retention covers it and the deployment stays inside the scope below.
This is the delivery-guarantees page's own definition of an exactly-once service write, which is scoped to "its documented scope and recovery window" and is distinct from end-to-end exactly-once processing.
Four things are outside the guarantee, stated in the user documentation with the mode:

- **Handler execution.** Cloud Tasks may dispatch one task more than once ([duplicate execution][duplicate-execution]); the handler stays at-least-once.
- **Logical duplicates without a key.** Two input records that mean the same event are two envelopes and two tasks unless `taskIdExtractor(...)` or `task-id` gives them one key; with a key, the existing semantics hold, and the winning task is "neither compared nor updated" (ADR-0104).
- **Effects the published specification does not bound.** A create the service accepts after its name protection has lapsed (§ Recovery window), an administrative history that removes a name without a tombstone (purge, queue deletion and recreation, a shortened `tombstoneTtl`), and a request an old process handed to the transport whose server-side effect lands after the replacement's task was created, dispatched, removed and forgotten.
  None of these is excluded by a published contract, so none is promised against.
- **A stop-with-savepoint that does not reach FINISHED.** Flink notifies a synchronous savepoint complete, so the committer creates its tasks, but does not add the savepoint to the completed-checkpoint store; if any task then fails to finish, whether the commit itself or an unrelated operator, Flink fails over to the last checkpoint, whose replay re-stages the savepoint's records under new random names (§ Terminal behavior and runbook).

Handler-side deduplication remains the application's protocol.
The mode does not add a task header for it; every task carries its name, which the service already delivers to the handler in the task-name request header it adds to each dispatch (`X-CloudTasks-TaskName` for HTTP targets, `X-AppEngine-TaskName` for App Engine targets).

### Identity

Every staged task is named.
When the sink has a `taskIdExtractor(...)` or the Table sink selects `task-id`, the name is the existing SHA-256 hex digest of the extracted key, so the new mode changes nothing about which records collapse.
Otherwise the writer mints a **random identity**: 128 bits from `SecureRandom`, rendered as 32 lowercase hex characters.
Both forms satisfy the task-id grammar and both are well distributed, which the [creation reference][create-task] asks for because sequential names raise latency and error rates.
The identity is minted once, at staging, and persisted inside the envelope; no restore, rescale or retry regenerates it.

A random identity identifies an envelope, not an event.
It is the correct identity for "this accepted record, staged once" and nothing more; the guarantee above says so rather than letting the word deduplication suggest otherwise.

### The envelope

A `StagedTaskEnvelope` is immutable after staging and carries:

| Field | Content | Why it is in the envelope |
|---|---|---|
| Format | The `SimpleVersionedSerializer` version plus a magic number | An incompatible state version is rejected by the deserializer with a message naming both versions; no envelope is silently reinterpreted. |
| Queue path | `projects/P/locations/L/queues/Q` | The committer creates in the queue the writer resolved, and the readback check below runs against it. |
| Origin | `originEpochMillis`, the writer's wall clock at staging | The durable origin `t0` of ADR-0104's inequality, established before any create and never refreshed. |
| Deadline | `authorizationDeadlineMillis`, computed at staging from the options then in force (§ Recovery window) | The commit uses the earlier of this value and the deadline recomputed from the current options and the origin, so a later configuration change tightens an old envelope and never loosens it. |
| Task | The complete `Task` wire bytes with `name` set | The committer sends exactly these bytes; parsing is deferred to commit so a staged envelope costs its wire size, not a protobuf object graph. |

One committable is one envelope, so Flink reports and counts per task.
A commit that throws is retried from the checkpoint's snapshot, which holds every request of that checkpoint, so the retry re-sends every name and the ones already created answer `ALREADY_EXISTS`.

### State machine and ownership

An envelope moves through five states; the transitions and their owners are the protocol.

| State | Owner | Transition |
|---|---|---|
| STAGED | The writer's in-heap list | `write()` appended it after serialization succeeded and the caps below held. |
| EMITTED | The committer operator's in-memory collector, under the pending checkpoint N | `prepareCommit()` returned every staged envelope and cleared the list; Flink emitted them as committables of checkpoint N before the writer's snapshot hook. |
| OWNED | Checkpoint N's committer state | Checkpoint N completed. From here the envelope survives any restore from N or later, at any parallelism. |
| CREATED / DEDUPLICATED | The service | The committer's `CreateTask` returned `OK`, or `ALREADY_EXISTS`; the request is left unsignaled or signaled already committed, respectively, so Flink drops it from the pending set. |
| EXPIRED | The retained checkpoint | The authorization deadline passed before a send; the committer throws without sending, the job fails, and the envelope stays in the last completed checkpoint. |

Ownership transfers at `prepareCommit()`, and the transfer is durable only when checkpoint N completes.
The lifecycle facts this rests on were measured on the pinned Flink versions (§ Evidence):

- The committer sends only from `notifyCheckpointComplete(k)`, for every pending checkpoint with id at most k.
  Nothing else sends in the supported scope; the end-of-input path that commits without a completed checkpoint exists only when checkpointing is disabled or the job runs in batch mode, both excluded below.
- An envelope emitted for checkpoint N that never completes is either committed by a later checkpoint N+1 whose committer snapshot contains it, or discarded with N's aborted snapshot.
  In the second case the source replays the records and the writer stages them again with fresh random identities.
  That is correct rather than a duplication risk: nothing was ever sent for the discarded identities, and had any checkpoint at or after N completed, the restore point would be that checkpoint, not N-1.
  A synchronous savepoint is the one completed snapshot Flink does not choose as a restore point on failover; § Terminal behavior and runbook records what follows.
- A commit of N that partially succeeded before the process died restores N, whose committer state holds all of N's requests as received.
  The re-commit sends every name again; the ones already created answer `ALREADY_EXISTS`, the rest are created.
  The commit is idempotent by name within the retention window, and by nothing else: no `GetTask` lookup is made, because a task that ran and was deleted is also absent (ADR-0104).
- The committer's operator state is a list of serialized collectors, redistributed whole per old subtask and merged by checkpoint id and then by writer subtask id.
  Nothing about a name depends on a subtask index, so the committer's parallelism may change at any restore.

The writer keeps **no Flink state**.
Both barrier and end-of-input paths drain the staged list through `prepareCommit()` before the writer's snapshot hook runs, and Flink forces the writer-to-committer edge to be aligned, so an envelope is in the writer's list or in a committable, never in both and never in neither.
The staged sink therefore does not implement `SupportsWriterState`; the envelope's own format field is the versioned header, and a writer with no state cannot hold an envelope by construction.
Upgrade and downgrade are detected by Flink, not by a header.
A stateless `AT_LEAST_ONCE` checkpoint restores an `EXACTLY_ONCE` writer as empty, which is correct.
An `EXACTLY_ONCE` checkpoint restored into an `AT_LEAST_ONCE` sink fails at state assignment with an unmatched committer operator state, because the committer snapshots its collector at every checkpoint and the stateless sink has no committer.
Only `allowNonRestoredState` gets past that, and what it drops is the last checkpoint's owned envelopes, which is loss of those records, not duplication; dropping unmapped state is excluded from the scope for that reason.

### Recovery window

The commit is safe while the service still remembers every name the envelope could have created.
ADR-0104 § Recovery inequality records the sufficient condition `R + E + S < H` and defines its terms; this record applies it as an **authorization deadline** and owns the three configured terms:

```text
deadline(options) = origin + nameRetention - clockSkewAllowance - requestTimeout
envelope.deadline = deadline(options at staging)
send only while now < min(envelope.deadline, deadline(current options))
```

The check runs immediately before every send, including every retry attempt, in the committer's own process; R is the envelope's age at that check and is not configured.
Equality is not accepted; the strict inequality is the margin.

- **H is `nameRetention`**, with the published one-hour default for queues Cloud Tasks created ([v2beta3 Queue `tombstoneTtl`][queue]); queues created from `queue.yaml` or `queue.xml` retain names for nine days per the [creation reference][create-task] and are configured accordingly.
  The connector reads it back when it can (§ Validation and prerequisites) but never infers the queue's history from a current value.
- **E is `clockSkewAllowance`**, the relative error between the writer host's clock at staging and the committer host's clock at the check.
  No published figure bounds it; the default is a conventional five minutes, documented as a convention and not as a specification.
- **S is `requestTimeout`**, the absolute deadline of the RPC, and it bounds the client side only.
  The committer computes a gRPC `Deadline` at the check and passes it with the call, rather than the client library's relative timeout, which is converted to a deadline only when the call is dispatched and therefore leaves a pause between check and dispatch unbounded.
  The default is 20 seconds, the pinned client's own `createTask` timeout.
  gRPC transparent retries and reconnects respect the deadline; a server-side effect after the deadline is the documented limitation named in the guarantee, and no local mechanism can exclude it (ADR-0104's counterexample stands).

The window a deployment actually has for an envelope is `nameRetention - clockSkewAllowance - requestTimeout` from staging, a little under 55 minutes at the defaults, and the part of it spent before the owning checkpoint completes, up to about one checkpoint interval plus the checkpoint's duration, is not available for recovery.
The user documentation states the arithmetic and that a deployment whose recovery objective exceeds it must configure the queue's `tombstoneTtl` and `nameRetention` together.

The connector persists no cross-process `nanoTime`.
The origin is a wall-clock instant because only a wall clock can be compared across hosts and restarts, and the clock-skew term exists because that comparison is imperfect.

### Committer protocol

`createCommitter` opens the client; the writer opens none in this mode.
`commit(requests)` runs as a mail on the committer task's mailbox thread, and the committer context exposes no `MailboxExecutor`, so the committer drives a blocking bounded-concurrency loop rather than the writer's mailbox dispatch:

1. Deserialize each envelope's header; a mode or version it does not recognize fails the commit before any send.
2. For each request, in the order received, wait for an in-flight slot (at most `maxInFlightTasks`), evaluate the authorization deadline, and send `CreateTask` with the absolute deadline.
3. On `OK`, leave the request unsignaled; Flink treats an unsignaled request as committed.
   On `ALREADY_EXISTS`, call `signalAlreadyCommitted()`, which is Flink's documented meaning for exactly this outcome and feeds its already-committed counter, and count `tasksDeduplicated`.
4. On a transient status (`UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`) or `NOT_FOUND`, wait out the existing recovery schedules of `CloudTasksWriterOptions`, including the separate short `NOT_FOUND` budget, then return to step 2 for that request.
   A `DEADLINE_EXCEEDED` is ambiguous, the task may exist; the retry sends the same name, so the ambiguity resolves to `OK` or `ALREADY_EXISTS`.
5. On any other status, on an exhausted schedule, or on an expired deadline, throw.
   The commit fails and the job restarts from the last completed checkpoint.
   Flink then re-commits every request of that checkpoint from its snapshot, including the ones this attempt had already created, which answer `ALREADY_EXISTS`; per-request progress does not survive a thrown commit, and does not need to.

The committer uses neither `retryLater` nor `signalFailedWithKnownReason`.
The first re-invokes `commit` synchronously in one bounded loop and adds nothing over the loop above; the second finalizes a failed request, which would turn a failed create into a silently dropped task.
Throwing is the BigQuery committers' precedent in this repository and is the only outcome that keeps the envelope owned.

The loop's residence on the task thread is bounded by the schedules: in the worst case one wave of `maxInFlightTasks` requests holds the thread for the schedule's attempts times `requestTimeout` plus the backoff sum, about three minutes at the defaults, and a commit with more envelopes than slots takes proportionally more waves.
While the commit runs, the committer's next barrier waits behind it.
The documentation states the sizing rule: `execution.checkpointing.timeout` must exceed the worst-case commit residence, or an expired checkpoint fails the job, which re-commits and repeats.

### Validation and prerequisites

Each check runs where it can see what it checks.

| Check | Where | Outcome |
|---|---|---|
| `EXACTLY_ONCE` with a destination resolver other than the fixed one `queue(...)` installs | `CloudTasksSinkBuilder.build()` | Rejected; the first release supports one fixed queue, so the readback and the retention assumption have one target. |
| `EXACTLY_ONCE` with a failure handler other than `failJob()` | `build()` | Rejected; a dropping or dead-lettering handler would let a commit that failed become success, and the committer has no handler context in this release (§ Alternatives declined). |
| Staged options without `EXACTLY_ONCE` | `build()` and the Table factory | Rejected, naming the option key in the Table layer through the module's `OptionSetters`. |
| `nameRetention` not greater than `clockSkewAllowance` plus `requestTimeout` | The options builder | Rejected, naming the three values; otherwise every envelope's deadline would precede its origin and the first commit would fail every task. |
| `taskIdExtractor(...)` or `task-id` with `EXACTLY_ONCE` | `build()` | Accepted; it selects the stable-key identity. |
| Serializer returned `null` | Writer | Skipped and counted, as in every mode ([ADR-0001](0001-a-serializer-returning-null-skips-the-record.md)). |
| Serializer set a task name, a task above the 100 KB size limit the creation reference documents, App Engine and HTTP target constraints the sink already owns | Writer, before staging | Failed as the serializer-class failure the sink already routes ([ADR-0049](0049-exactly-three-cloud-tasks-failures-are-routed-and-the-argument-half-never-scans.md)), which under `failJob()` fails the job; nothing invalid is staged, so no envelope is poison by construction. The size limit is a constant, not an option: the only direction a knob could move it is above the limit, which would stage an envelope the service rejects forever. |
| Batch or automatic runtime mode, checkpointing disabled, `CheckpointingMode` other than `EXACTLY_ONCE`, checkpoints after tasks finish disabled | The sink's `addPreCommitTopology`, at graph construction | Rejected with a message naming the setting; this is the BigQuery buffered-stream sink's precedent, and it reaches the Table path because the planner uses the same translator. Under at-least-once alignment records processed after a barrier would be staged into the barrier's checkpoint and replayed after a restore; without checkpoints after tasks finish a bounded streaming job's tail would never commit. |
| Bounded input in the Table sink | `CloudTasksDynamicSink`, at planning | `ValidationException` naming the option key, from `Context.isBounded()`. |
| Queue retention readback (`verifyQueueRetention`, default on) | `createCommitter`, before the first commit | Reads the queue through the v2beta3 `GetQueue` and requires `tombstoneTtl >= nameRetention`; an absent field is accepted only when `nameRetention` is at most the published one-hour default. Needs `cloudtasks.queues.get`. Skipped under an emulator endpoint, because the emulator implements v2 only. Never calls `UpdateQueue`. |
| Full staging buffer | Writer | Fails the job with a sizing diagnostic (§ Heap). |
| Envelope format or mode not recognized | Deserializer, committer | Fails before any send. |
| Expired envelope | Committer, before the send | Fails before the send (§ Terminal behavior and runbook). |

Some conditions cannot be observed from a sink and are deployment prerequisites, documented as such and excluded from the guarantee when violated:

- Externalized checkpoints retained on failure and cancellation, and a restart strategy that stops after a bounded number of attempts, so an expired or invalid envelope is preserved for an operator instead of lost or retried forever.
- No restore from a checkpoint older than the latest completed one, no concurrent fork of one job from one checkpoint, and no `allowNonRestoredState`.
- No `PurgeQueue`, queue deletion and recreation, or reduction of `tombstoneTtl` during the lifetime of any staged or owned envelope.
  The queue's `taskTtl` is not a separate removal path for this purpose: the published reference describes it as deleting the task once it has lived that long, dispatched or not, and the tombstone applies after deletion.
- Batch execution and checkpoint-disabled streaming stay excluded even where the graph check cannot see them; bounded streaming input with checkpointing is inside the protocol, because its tail commits on the final checkpoint's completion.

A runtime cannot observe whether it runs in batch mode or whether a commit was triggered without a completed checkpoint: the writer and committer contexts expose the restored checkpoint id, job and task information and metric groups, and `commit` carries no checkpoint id.
The graph-construction check is therefore the enforcement point, not a runtime guard.

### Heap

Staged envelopes live on the heap until their checkpoint completes, and the same bytes exist more than once: in the writer's list, as a copy on the chained writer-to-committer edge, in the committer's collector, in the collector copy the committer snapshots at every checkpoint and the serialized form the state backend writes, on restore as raw state and deserialized objects, and at commit as up to `maxInFlightTasks` parsed requests.
This record estimates the peak at three to four times the staged bytes per writer subtask for one checkpoint's batch; the implementation measures its own multiplier and prints that one.

The writer's caps bound one batch, not what the committer holds.
`prepareCommit()` clears the writer's list at every barrier, a savepoint's included, and Flink keeps a batch in the committer until a checkpoint at or above its own id completes **and is notified**.
The invariant, not a formula, is what a deployment sizes for: **the committer holds one batch for every barrier that has passed the writer since the last notified completion**, and the writer's caps bound each batch, not their number.
Three things pass a barrier without releasing anything.
An ordinary savepoint is completed but never notified, only a synchronous one is, so its batch parks until the next completed checkpoint; a checkpoint that fails after its barrier reached the writer parks its batch until then too; and `execution.checkpointing.max-concurrent-checkpoints` above one lets several barriers be in flight at once.
**No configuration gives this count a ceiling, and the record does not pretend otherwise.**
`execution.checkpointing.tolerable-failed-checkpoints` bounds only *consecutive counted* failures: an ordinary savepoint's success resets that counter without releasing anything, and Flink counts neither a subsumed checkpoint nor several decline reasons.
`execution.checkpointing.max-concurrent-checkpoints` admits ordinary checkpoints against the coordinator's pending count, which is not the committer's: a savepoint taken while unaligned checkpointing is off is forced and admitted past that count anyway, and, for the checkpoints it does gate, the coordinator frees the slot when one completes while the completion notification is an asynchronous message to the task, so two checkpoints can complete and a third barrier reach the writer before the committer has processed a notification, even at one concurrent checkpoint and no failures.
Every notification the committer does process commits every batch at or below its id, so the count collapses whenever notifications are being processed; what has no bound is the interval before that.
Sizing is therefore from the observable, not from arithmetic: Flink's standard pending-committables gauge on the committer (§ Metrics) counts the held tasks per committer subtask, and the documentation tells an operator to size the committer's heap from that gauge's peak times the per-task size limit times the copies above, and to alert on it.
The graph-construction check logs the two settings it can read, in the log of the process that builds the graph, because they say how much room a deployment has left itself; savepoint cadence and notification latency are neither of them visible there.

The writer bounds the list by `maxStagedTasks` (default 100,000) and `maxStagedBytes` (default 64 MiB, counting each task's wire size plus 256 bytes).
Exceeding either **fails the job with a sizing diagnostic** that names both caps, the counts at failure, and the multiplier.
It never waits in `write()` for the barrier that would drain the list: that barrier is behind the blocked record in the same input channel and cannot arrive.
The diagnostic's remedy is a shorter checkpoint interval, a larger cap with a larger heap, or lower parallelism per host, and the documentation says which.

### Terminal behavior and runbook

An envelope whose authorization deadline has passed, or whose format or mode the committer does not recognize, fails the commit before any send.
Under the prerequisites above the job reaches FAILED or is canceled with its last completed checkpoint retained, and every restart from that checkpoint fails the same way at `initializeState`, before `open`, because Flink re-commits restored requests there.
The failure message names the envelope's queue, origin and deadline, the clock reading that missed it, the option that overrides, and the runbook page; it is the first and only symptom, so it has to carry the diagnosis.
The retained state is readable and the operator decides.

The `expiredEnvelopePolicy` option names the decision; its default `FAIL` is the behavior above, and the three overrides are outside the guarantee and say so in their documentation:

- `ASSUME_COMMITTED`: treat every restored expired envelope as created.
  This is safe for exactly one flow, resuming a job that reached FINISHED through stop-with-savepoint.
  A terminating savepoint snapshots the committer's requests before the completion notification that commits them, and the task finishes only after that commit succeeded, so its savepoint always holds committed requests that look pending.
  A resume more than the window after the stop therefore fails at startup with nothing actually outstanding, and this override is the documented way through.
- `CREATE_ANYWAY`: send regardless of the deadline, for a crashed job whose operator has decided that a second task for some records is preferable to losing them.
- `DROP`: skip the envelope, for a crashed job whose operator has established through the handler's own records that the envelope's task ran, or has decided that losing it is preferable to a duplicate.

Stop-with-savepoint has a second flow, and it is the one Flink-side hazard in the supported scope.
A synchronous savepoint is notified complete, so the committer creates its tasks, but Flink does not add a savepoint to its completed-checkpoint store.
If the job then does not reach FINISHED, because the commit threw or because any other task failed after the savepoint completed, Flink's stop-with-savepoint handler fails over globally and the job restarts from the last completed checkpoint N; the source replays every record between N and the savepoint, the writer stages them again, and records without a stable key receive new random names, so the tasks the savepoint's commit had created, some or all of them, exist twice.
The connector cannot tell a savepoint's commit from a checkpoint's, because `commit` carries no such flag, so it cannot refuse the send, and it cannot see the other tasks at all.
A stop-with-savepoint that does not reach FINISHED is therefore outside the guarantee; the runbook is to cancel the restarted job before its next checkpoint completes and resume from the savepoint, whose committer state re-commits the same names, and the documentation says that a stable key removes the hazard.

The same runbook covers a commit that fails with a non-retryable status such as `INVALID_ARGUMENT`: with `failJob()` enforced, the job restarts until the operator repairs or overrides, exactly as the existing writer behaves under `failJob()` today.
Manual recovery after expiry is a loss or duplicate risk the operator takes knowingly; the mode's guarantee ends where the window ends.

### Options and public API

A `CloudTasksStagedOptions` value (immutable, built from `builder().build()`) carries the mode's knobs; the sink builder accepts it beside `writerOptions(...)`, and `sink.staged.*` Table keys map onto it through the module's `OptionSetters` ([ADR-0133](0133-a-table-option-value-the-builder-rejects-is-renamed-to-its-option-key.md)).
The mode is a sink-root enum, `CloudTasksDeliveryGuarantee`, selected by `deliveryGuarantee(...)` on the builder and by `sink.delivery-guarantee` in the Table API, whose values render as `at-least-once` and `exactly-once` in DDL, the module's enum convention.
The key is offered as the cross-connector spelling for the concept; the committer-based Bigtable mode of [#1211](https://github.com/flink-gcp/flink-connector-gcp/issues/1211) decides in its own ADR whether it shares it, and [ADR-0137](0137-a-cross-connector-name-diverges-only-to-name-a-real-difference.md) is why the key is not a Cloud Tasks-specific name.

Both new types are `@PublicEvolving` under [ADR-0141](0141-a-surfaces-stability-tier-is-set-by-what-can-reshape-its-inputs-and-outputs.md)'s youth clause, recorded here: the mode has not survived a published release, the deferred committer-side failure handler and dynamic destinations (§ Alternatives declined) would reshape the options' validation, and the label is conditional on the gates below.
The builder method and the Table key are the entry surface and stay on the `@Public` builder and factory.

| Knob | Default | Role |
|---|---|---|
| `deliveryGuarantee` | `AT_LEAST_ONCE` | Selects the eager writer or the staged sink; every existing default, name, failure policy and public signature is unchanged in the default mode. |
| `nameRetention` | 1 h | H; the published default for Cloud Tasks-created queues. |
| `clockSkewAllowance` | 5 min | E; a convention, not a specification. |
| `requestTimeout` | 20 s | S; the absolute per-RPC deadline, the pinned client's own value, spelled as the Bigtable single-request runtime spells the same concept. |
| `maxStagedTasks` / `maxStagedBytes` | 100,000 / 64 MiB | The heap caps. |
| `verifyQueueRetention` | on | The `GetQueue` readback. |
| `expiredEnvelopePolicy` | `FAIL` | The runbook override. |

Existing `CloudTasksWriterOptions` keep their meaning: `maxInFlightTasks` bounds the committer's concurrency, the two recovery schedules bound its retries, `channelPoolSize` sizes the committer's client, and `perDestinationMetrics` is unaffected because there is one destination.
`Duration` knobs are bounded at the setter as [ADR-0068](0068-duration-budgets-are-bounded-at-the-setter-by-what-a-nanosecond-clock-can-express.md) requires; whether the committer re-checks the bound on a deserialized options instance is decided at implementation and recorded there.

### Metrics

The writer registers two gauges, staged task count and staged bytes, and keeps `recordsSkipped`.
The committer registers `tasksDeduplicated` and otherwise relies on Flink's standard committer metrics: the pending-committables gauge that Flink's collector registers per committer subtask and the already-committed counter that `signalAlreadyCommitted()` feeds.
A connector-owned bytes gauge on the committer was considered and is not possible: the committer sees a checkpoint's requests only inside `commit`, after completion, while Flink's collector holds them before that.
Nothing is counted only at close, because a counter incremented as the operator closes is never scraped.
Names are settled at implementation through the metric inventory and its checker.

### Package shape for the implementation

The staged mode is a second write method of the one `CreateTask` family: same RPC, same serializer SPI, same client, same failure classification.
[ADR-0055](0055-connector-packages-follow-one-skeleton-and-a-layer-exists-only-where-a-sibling-can-arrive.md)'s test yields no family layer, and its `sink.committer` stage is admitted "as the topology requires".
The classes land as `sink/CloudTasksStagedCreateTaskSink` (`@Internal`; `CrossVersionSink`, `SupportsCommitter`, and `SupportsPreCommitTopology` for the graph check), `sink/StagedTaskEnvelope` and its serializer (`@Internal`, at the sink root because both stages import them), `sink/CloudTasksStagedOptions` and `sink/CloudTasksDeliveryGuarantee` (`@PublicEvolving`), `sink/writer/CloudTasksStagedWriter` (`@Internal`), and `sink/committer/` for `CloudTasksStagedCommitter`, the deadline arithmetic and the blocking creation loop (`@Internal`).
The `TaskCreatorFactory` seam moves to the committer in this mode, and `TaskCreator.createTask` gains the absolute deadline as a parameter.
`CloudTasksSinkBuilder.build()` keeps returning `Sink<T>` and dispatches on the mode.
No cross-major source root is needed: `SupportsCommitter`, `SupportsPreCommitTopology` and `CommitterInitContext` are shared by both supported Flink lines, and the graph check does not read a committable's checkpoint id.

## Evidence

### Flink lifecycle on the supported lines (2026-09-06)

Source inspection used Flink 1.20.4, 2.2.1 and 2.3.0, the LTS and the two current minors.
The sink operator sources (`SinkWriterOperator`, `CommitterOperator`, `CheckpointCommittableManagerImpl`, `CommitRequestImpl`, `CommittableCollector`) are identical between 2.2.1 and 2.3.0; the 1.20.4 versions differ in construction and collector-merging details and agree on every fact below.

| Fact | Where it is | Design consequence |
|---|---|---|
| Barrier order: `flush(false)`, `prepareCommit()`, emit committables in the pre-barrier hook, then the writer's `snapshotState` | `SinkWriterOperator.prepareSnapshotPreBarrier` and `snapshotState` | Ownership transfers at `prepareCommit()`; the writer needs no state. |
| The committer snapshots a copy of its collector at every checkpoint, with requests in the received state | `CommitterOperator.snapshotState` | An `EXACTLY_ONCE` checkpoint always carries committer state, which is what detects a downgrade; a thrown commit is retried with every request of that checkpoint. |
| Commit runs from `notifyCheckpointComplete(k)` for every pending checkpoint with id at most k | `CommitterOperator.notifyCheckpointComplete` | An aborted checkpoint's committables are committed by the next completed one or discarded with the aborted snapshot; nothing sends before completion. |
| Restored requests are committed in `initializeState`, before `open` | `CommitterOperator.initializeState` | The expiry check and the format check run before any restored create. |
| `retryLater` re-invokes `commit` synchronously, up to `sink.committer.retries` more rounds, then throws; requests left unsignaled become committed; `signalFailedWithKnownReason` finalizes the request | `CheckpointCommittableManagerImpl.commit`, `CommitRequestImpl` | The committer throws instead of signaling; `signalAlreadyCommitted()` is used for `ALREADY_EXISTS`. |
| End of input emits the staged committables under `lastKnownCheckpointId + 1`, not a sentinel | `SinkWriterOperator.endInput` (bytecode of 2.2.1 checked: `lastKnownCheckpointId`, `lconst_1`, `ladd`, `emitCommittables`) | Bounded streaming input is inside the protocol; a pre-commit topology cannot detect end of input by a sentinel id, so the graph check is the enforcement point. `Long.MAX_VALUE` appears only as the committer's own bound in `CommitterOperator.endInput` when checkpointing is disabled or the mode is batch. |
| Collector state is a list of serialized collectors, redistributed whole and merged by checkpoint id then writer subtask id | `CommitterOperator.initializeState`, `CommittableCollector.merge` | Rescaling the committer preserves every owned envelope; identity never depends on a subtask index. |
| The writer-to-committer edge is forced aligned | `SinkTransformationTranslator` | No envelope is captured in channel state between the two operators. |
| A synchronous savepoint is notified complete but is not added to the completed-checkpoint store; a stop-with-savepoint whose tasks do not finish fails over globally | `CheckpointCoordinator.completePendingCheckpoint` (`isSavepoint` excludes the store, `isSynchronous` sends the notification), `StopWithSavepointTerminationHandlerImpl` | A stop-with-savepoint that does not reach FINISHED, for any reason in any task, restarts from the last checkpoint and replays the savepoint's records (§ Terminal behavior and runbook). |
| `CommitterInitContext` exposes the restored checkpoint id, job and task information and a metric group, and no `MailboxExecutor` | `flink-core` 1.20.4 and 2.2.1 | The committer runs a blocking bounded loop on the mailbox thread. |
| `Committer.CommitRequest` offers `getCommittable`, `getNumberOfRetries`, `signalFailedWithKnownReason`, `signalFailedWithUnknownReason`, `retryLater`, `updateAndRetryLater`, `signalAlreadyCommitted` on both lines | `flink-core` 1.20.4 and 2.2.1 | The protocol uses only `signalAlreadyCommitted`. |

`CloudTasksStagedCommitLifecycleTest` in the Cloud Tasks module drives Flink's real `SinkWriterOperatorFactory` and `CommitterOperatorFactory` against a service-free probe sink, in seven cases: the pre-barrier hook calls `flush(false)` then `prepareCommit()` and the writer's snapshot hook does not, and nothing is created before completion; a repeated completion commits nothing again; a restore re-commits the original serialized committable during initialization; `retryLater` is one synchronous bounded loop that cannot silently succeed; checkpointed end of input waits for completion; checkpoint-disabled end of input commits without completion; a checkpoint that is aborted is committed once by the next completed one; and end of input emits under the next checkpoint id rather than a sentinel.
The harness runs the operators in one thread and calls the hooks itself, so the order of the pre-barrier hook against the snapshot hook, the JobManager's savepoint handling, externalized-checkpoint retention, rescaling and the service are source-inspection facts here, not test results.
Those are the test designs below.

### Service contract

The service facts are ADR-0104's G0 record and ADR-0154's reading of it, and this record adds none: the collision outcome, the retention field and its range, the client's timeout, and the limits of a client deadline are cited there.

### Deterministic fault tests the implementation owes

Each row names the invariant, the harness that can break it deterministically, and the observation that fails if the invariant is broken.
The existing harnesses are `FakeTaskCreator` (scripted futures including `enqueuePending()`), `ManualTimeSource`, the operator harness above, and MiniCluster cases with the Cloud Tasks emulator through `AbstractCloudTasksEmulatorITCase`.
Four seams do not exist yet and the implementation adds them: a fake creator reachable through the serialized sink inside the operator harness and the MiniCluster, in the static-registry shape the probe sink uses, because `createWriter` and `createCommitter` build the production factory today; a MiniCluster extension with a cluster client, because the existing job case can only execute a job and cannot restart it from a retained checkpoint, stop it with a savepoint or resume it; a clock seam reachable inside a serialized job sink; and a metric-capturing committer metric group, because `signalAlreadyCommitted()` is observable only through Flink's already-committed counter.
The gated real-GCP acceptance in [#1245](https://github.com/flink-gcp/flink-connector-gcp/issues/1245) checks integration, not these invariants.

| Invariant | Harness and stimulus | Failure oracle |
|---|---|---|
| No creation before completion | Operator harness with the production sink and the fake creator: write, `prepareSnapshotPreBarrier`, `snapshot` | The fake's request list is non-empty before `notifyOfCompletedCheckpoint`. |
| Restore re-commits the original bytes and name | Script one pending future, snapshot the committer, drop the harness, restore into a new committer | A restored request differs from the pre-crash one in name or `Task` bytes, or a new random name appears. |
| Expired envelope is rejected before the send | `ManualTimeSource` at the authorization deadline | `commit` completes, or the fake saw any request. |
| Creation with a lost acknowledgement is success | Script `DEADLINE_EXCEEDED` then `ALREADY_EXISTS` for one name | The already-committed counter is not 1, `tasksDeduplicated` is not 1, or `commit` throws. |
| The staging cap fails without blocking | Writer unit test with `maxStagedTasks = 3`, a fourth `write()`, a yield-counting fake mailbox executor | No exception naming both caps, or the yield count is non-zero (the existing fake's `yield()` blocks on an empty mailbox, so a counter or a timeout is what makes this oracle a failure rather than a hang). |
| An oversized task never becomes an envelope | A serializer output above the size limit | An envelope exists, or the failure is not the serializer-class failure. |
| An unknown format version is rejected | `deserialize(version + 1, bytes)` on the envelope serializer, and a committer fed such a committable | No exception, or a request reaches the fake. |
| Rescaling preserves names once each | Snapshot at committer parallelism 2, restore at 1 and at 3 via repartitioned subtask state | The union of created names differs from the staged set, or a name is created twice. |
| A downgrade is detected, and the loss under the flag is exactly the documented one | MiniCluster: `EXACTLY_ONCE` job whose fake creator withholds the answers to the last checkpoint's creates, so the retained checkpoint owns envelopes whose creates never completed; cancel before the checkpoint timeout (the committer's blocking wait must honor interruption, which is part of its contract); restart with the `AT_LEAST_ONCE` sink under the same uid, once without and once with `allowNonRestoredState` | Without the flag the restore succeeds; with the flag the restore fails, or a task whose body is one of the withheld records' is ever created by any name (the expected outcome is a successful restore and those records never reaching the queue, which is the documented loss). |
| Terminal failure retains and rejects | MiniCluster with retention on cancellation and no restart strategy; a map that throws after checkpoint N; restart from the retained path with the clock at origin plus retention | The restart reaches `open`, or `listTasks` on the paused queue shows a new task. |
| Graph construction rejects unsupported execution | Batch and automatic mode, checkpointing disabled, at-least-once checkpointing, checkpoints after tasks finish disabled | `getStreamGraph()` succeeds; the Table plan test with a bounded source produces no `ValidationException`. |
| Stop-with-savepoint commits before FINISHED and resumes within the window | MiniCluster: stop with savepoint, then resume from it inside the window | Tasks missing after the stop, or a second task after the resume. |
| A stop-with-savepoint that does not finish behaves as documented | MiniCluster, two arms, restart strategy fixed-delay: the fake creator fails the second of two creates terminally; and every create completes, then an unrelated operator throws on its own completion notification, sequenced after the fake has answered every create. Each arm runs twice: once letting the restarted job complete a checkpoint, once cancelling it before that and resuming from the savepoint | In the first run the restarted job's next completed checkpoint does not create a second task by body for a record the savepoint's commit had created (the documented hazard did not occur, so the documentation is wrong); in the second run resuming from the savepoint leaves a body the savepoint's commit had rejected without a task, or creates a second task for a body it had accepted (the remedy is wrong; the fake answers `ALREADY_EXISTS` only for a name whose create it accepted, so the designed re-send of an accepted name is not a failure and the rejected one must be created). |
| The `ASSUME_COMMITTED` override is exactly as safe as documented | Resume the same savepoint after the window with the override | Any request reaches the emulator. |
| End to end, every record becomes exactly one dispatched task | The existing emulator job case under `EXACTLY_ONCE` | The dispatched request count by body differs from the record count. |

## Alternatives declined

- **Use the existing stable-key path only and defer the staged mode** (the earlier disposition of this issue): a stable key is not available to every application, and no key gives checkpoint-coordinated visibility. The analysis that two logical duplicates receive two random identities is kept as a stated limitation.
- **Keep a versioned header, or the envelopes themselves, in writer state**: Flink's operator order drains the writer's list before every writer snapshot, so envelopes there would be a second owner of the same bytes; a header there detects neither upgrade nor downgrade, which Flink's committer state already does, and the envelope carries the format version. A writer with no state is the stronger form of "no envelope lives here".
- **Use `retryLater` for transient failures**: it re-invokes `commit` synchronously a bounded number of times and adds no delay or deadline awareness over the loop the committer already runs.
- **Use `signalFailedWithKnownReason` for a rejected create**: it finalizes the request, so a create the service refused would leave Flink's pending set as if it had happened.
- **Derive identity from the payload bytes**: identical bytes can be two legitimate events and differently serialized bytes one event; identity is a key the application gives or a random value, never a hash of content.
- **Detect end of input in a pre-commit topology through a sentinel checkpoint id**: measured false on both lines; end of input emits `lastKnownCheckpointId + 1`.
- **Use the client library's relative timeout as S**: it becomes a deadline only at dispatch, so a pause between the authorization check and dispatch is unbounded; the absolute gRPC deadline computed at the check is used instead.
- **Pin only the retention term in the envelope**: lowering the skew allowance or the request timeout after staging would loosen an old envelope's deadline by the same mechanism; the envelope pins the computed deadline and the commit takes the earlier of the pinned and the recomputed value.
- **Read `tombstoneTtl` as proof of a name's history**: a current value describes configuration, not what applied when an earlier task was removed (ADR-0104); the readback is a prerequisite check and the history is a deployment prerequisite.
- **Look a name up with `GetTask` before re-creating**: absence does not mean the task never ran, and the create's own `ALREADY_EXISTS` is the only documented collision outcome.
- **Make the task size limit an option**: the documented limit is a service constant; the only outcome a knob changes is staging an envelope the service will reject on every attempt.
- **Reuse Flink's `DeliveryGuarantee` enum for the mode value**: it is a Flink `@PublicEvolving` type, so a Flink minor could reshape a value set that sits inside this builder's frozen signature ([ADR-0141](0141-a-surfaces-stability-tier-is-set-by-what-can-reshape-its-inputs-and-outputs.md)), and its `NONE` value has no meaning here and would need rejecting at build time; a module-owned enum with the two spellings avoids both.
- **Open the failure handler in the committer so `INVALID_ARGUMENT` at commit can be routed**: deferred, not declined. It needs a committer-side failure-handler context in the base module and a decision on what `signalFailedWithKnownReason` after routing means for the guarantee; the first release enforces `failJob()` and the runbook.
- **Dynamic destinations in the first release**: the envelope already carries its queue path, so the restriction is about validation, not the format; it is lifted when the readback and the retention assumption are defined per queue.
- **Refuse the send when the commit belongs to a savepoint**: `commit` carries no such flag on either line, so the committer cannot tell; the stop-with-savepoint that does not reach FINISHED is documented instead.

## Consequences

- ADR-0104's Cloud Tasks decision is superseded by this record; its evidence, its performance thresholds and the G0 analysis remain in force and are what the deadline arithmetic here is built on.
- ADR-0048's stateless writer and ADR-0049's routed failures describe the default mode; both are refined in place with a pointer here.
- [#1242](https://github.com/flink-gcp/flink-connector-gcp/issues/1242) builds the writer, envelope, serializer and staging caps with the operator-harness and writer tests above; [#1243](https://github.com/flink-gcp/flink-connector-gcp/issues/1243) builds the committer, the deadline arithmetic, the graph check, the readback and the DataStream entry point with the MiniCluster tests and the four seams they need; [#1244](https://github.com/flink-gcp/flink-connector-gcp/issues/1244) adds the Table option, the planner check, and the documentation named next.
- The documentation that changes when the mode ships: the delivery-guarantees sink matrix and Cloud Tasks section, the DataStream page's task naming and delivery sections, the Table page's delivery section, and the option reference; each states the guarantee, the four exclusions, the window arithmetic, the heap rule, the checkpoint-timeout rule, the prerequisites and the runbook.
- The label `EXACTLY_ONCE` is conditional on the tests above passing on both supported Flink lines and on both ADR-0104's primitive performance gate and [#1246](https://github.com/flink-gcp/flink-connector-gcp/issues/1246) accepting the cost; a mode that fails either is not released under that name.

[create-task]: https://docs.cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues.tasks/create
[queue]: https://docs.cloud.google.com/tasks/docs/reference/rest/v2beta3/projects.locations.queues
[duplicate-execution]: https://docs.cloud.google.com/tasks/docs/common-pitfalls#duplicate_execution
