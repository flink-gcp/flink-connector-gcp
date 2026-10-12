# Cloud Tasks Tier-3 applications

This module holds two internal applications.
The measurement application supplies the DataStream workload and per-attempt observations for [issue #1246](https://github.com/flink-gcp/flink-connector-gcp/issues/1246); every section from [Compared paths](#compared-paths) to [Resource boundary and remaining acceptance](#resource-boundary-and-remaining-acceptance) describes it.
The [recovery application](#recovery-application) is the workload of the cluster recovery trial of [#1362](https://github.com/flink-gcp/flink-connector-gcp/issues/1362).
Both run on Java 17 and use the reactor's Flink version and Cloud Tasks connector.
The module is built only by the `tier3-cloudtasks` Maven profile and is never deployed to Maven Central.

## Build

Run `mise x -- just tier3-cloudtasks-verify` from the repository root.
The build creates `target/cloudtasks-measurement.jar`, which holds both applications, and copies the runtime dependency JARs to `target/image-lib/`; the weekly floor and LTS builds verify it on both supported Flink lines.
The single-host rig of [ADR-0162](../../../docs/adr/0162-cloud-tasks-implementation-precedes-final-performance-acceptance.md) runs its lean probe from this module's test sources.
The measurement images are no longer published: only the cluster measurement session ran one, and [#1723](https://github.com/flink-gcp/flink-connector-gcp/issues/1723) removed that session.
The recovery application's image is built from this module's [Dockerfile](Dockerfile) on the Flink 2.2.1 base.
The CI lane builds it without publishing, and the [image publication](../../images/README.md) publishes it as `cloudtasks-recovery`.

## Compared paths

| Arm | Runtime and task name |
| --- | --- |
| `UNNAMED` | Production eager writer without a task-ID extractor |
| `NAMED_HASH` | Production eager writer with the SHA-256 digest of run/cell/sequence |
| `NAMED_RANDOM_CONTROL` | Production eager writer with the same extractor, followed by a benchmark-only projection to the first 128 digest bits |
| `STAGED_HASH` | Production staging writer and committer with the same SHA-256 extractor |
| `STAGED_RANDOM` | Production staging writer and committer with their persisted SecureRandom identity |

The random-name control matches the staged random path's 32-character, distributed name shape.
It is not a public random-naming option for the eager sink and does not match SecureRandom's identity-generation CPU cost.
Its extra request projection and hashing are part of the observed control cost and must be calibrated before interpreting an incremental staging comparison.
Retries project to the same name; the adapter never generates a fresh identity on retry.
The support thresholds still compare the total staged path against `UNNAMED`, as required by ADR-0162.

The app constructs sinks with the public builder and observes clients through existing internal factory seams.
The writer, committer, retry schedules, absolute deadlines, retention verification and immutable committable format remain production code.
The request adapter is used only by `NAMED_RANDOM_CONTROL`.
No queue administration belongs to the application.

## Cell inputs

Every execution supplies one finite cell; repetition and arm order belong to the external experiment manifest.
Arguments use `--name value` pairs; missing values, duplicate options and unknown options are rejected.

| Argument | Accepted value |
| --- | --- |
| `--run-id`, `--cell-id` | Separate lowercase labels of 1–40 characters; within a run, cell IDs distinguish every arm, configuration and repetition |
| `--queue` | An explicitly approved `ct1246-` queue in `flink-gcp/us-central1` |
| `--target` | Synthetic HTTPS target without credentials, query or fragment; the queue must remain paused |
| `--arm` | One arm from the table above |
| `--body-bytes` | 1024 or 65536, including the observation header |
| `--parallelism` | 1, 4 or 16 sink subtasks |
| `--concurrency` | 1, 4 or 16 in-flight tasks per sink subtask |
| `--checkpoint-seconds` | 1, 10 or 60 |
| `--channel-pool-size` | 1, 4 or 8 per sink client; defaults to 1 |
| `--records` | Record-count mode: total source records, including warm-up; at most 100000 |
| `--warmup-records` | Record-count mode: at least 1 and strictly less than the total; the analyzer excludes these sequence numbers |
| `--distribution` | `even` or `skew`; skew assigns 90% to subtask zero and distributes the rest over other subtasks |
| `--offered-rate` | Finite positive source ceiling, at most 10000 records/s |
| `--warmup-seconds` | Window mode: 1–120 seconds |
| `--observation-seconds` | Window mode: 1–600 seconds |
| `--record-limit` | Window mode: explicitly approved source ceiling, at most 10000000 |
| `--attempt-limit` | Window mode: explicitly approved per-creator ceiling, from 1 through three times the generated record count |
| `--control-delay-millis` | Window calibration only: 0 (default) or 100; a nonzero value requires `UNNAMED`, parallelism/concurrency 1 and CSV output |
| `--emit-attempts` | `true` (default), or `false` for a window calibration that measures accounting without CSV formatting/output |
| `--evidence-root` | Prefix the rows and receipts are written under: a hierarchical URI with a scheme and a non-empty path, ending in `/`, without a query or fragment, and for `file:` without a host. Defaults to `gs://flink-gcp-cloudtasks-benchmark/runs/`, which is what the removed cluster session used without passing it; a rig that writes elsewhere passes its own, and the single-host controller passes one inside each run's directory |

Record-count mode remains available for finite wiring checks.
Window mode requires all four window arguments and rejects `--records` and `--warmup-records`.
It generates `ceil(offeredRate * (warmupSeconds + observationSeconds + 2 * checkpointSeconds + 1))` records and rejects a configuration that exceeds `--record-limit` before building the job.
The extra two nominal checkpoint intervals and one second separate the intended observation window from end-of-input flushing.
This is a finite input budget, not a guarantee of elapsed time: source pacing, backpressure, checkpoint delays and startup still need external timing evidence.
Window mode has no warm-up sequence prefix; its analyzer must select the actual time window and reconcile its tail.

The source has one subtask and uses Flink Datagen pacing.
A capped observation answers an offered-load question, not an uncapped capacity question.
Source pacing and logging overhead require calibration on the execution host before performance claims.
At parallelism one, both distribution settings necessarily produce the same partition assignment.
The payload contains deterministic pseudorandom padding that differs between sequence numbers, avoiding an all-zero checkpoint compression fixture.
The measured sizes are HTTP body sizes; task names, target configuration and protobuf framing add wire bytes.

Named hash paths derive their task identity from run ID, cell ID and sequence.
The manifest must assign a fresh run/cell pair to every execution across arms, configurations and repetitions sharing a queue.
Reusing that pair is reserved for deliberate replay, since `NAMED_HASH` and `STAGED_HASH` otherwise address the same names.
The lifecycle collector reconciles `ALREADY_EXISTS` by task name and sequence with earlier attempts and recovery evidence from the same cell.
A prior attempt can create a task before its response is lost; a later retry can therefore deduplicate within a fresh cell.
Unexplained duplicates invalidate a fresh-creation cell, and deliberate replay remains a separate workload.

The runner must choose a warm-up and observation window that includes regular completed checkpoints at the selected interval.
A short finite cell can finish through end-of-input flushing before any periodic checkpoint; that is a wiring check, not steady checkpoint-cost evidence.
The application does not drain the warm-up prefix as a separate phase or decide whether a cell is statistically usable.

Both staged arms use one-hour name retention, one-minute clock-skew allowance and a 20-second request timeout, with the production default staging caps and fail-before-send expiry behavior.
All arms explicitly set `recoveryMaxAttempts` to 3 and `notFoundRecoveryMaxAttempts` to 1, replacing the production defaults of 8 and 3; the other retry settings retain their defaults.
These are experiment inputs, not a recommended recovery budget.
The runner must read back queue retention and pause state, and account for checkpoint age, backlog and incident response before admitting a cell.
A staging-cap failure is an incompatible cell result; it must not be hidden by silently changing the input rate or staging cap.

## Raw observations

Each completed or synchronously failed RPC attempt writes one comma-separated line beginning with `CT1246`.
Fields after that marker are: run ID, cell ID, arm, client-incarnation UUID, observation-process UUID, origin-process UUID, sequence, attempt ordinal, serializer-origin wall time in milliseconds, serializer-origin monotonic time, send monotonic time, completion monotonic time, serializer-to-completion nanoseconds, status, and task name.
All monotonic values are in nanoseconds.
Task bodies, target URLs and exception messages are excluded.
Named failures retain the requested name so deduplicated replay can be reconciled.
`CANCELLED` includes cancellation of the observed future, which can follow local deadline expiry, teardown or caller cancellation.
It does not establish the reason for cancellation; an RPC reporting `DEADLINE_EXCEEDED` retains that status.
The row describes the RPC future, whereas the committer classifies its local expiry as `DEADLINE_EXCEEDED` in connector error-class metrics; those two observations need not carry the same status.

Window mode writes the rows through the installed Flink filesystem plugin as gzip-compressed CSV parts under `<evidence root><run>/cells/<cell>/rows/<incarnation>-<NNNNNN>.csv.gz`, which is `gs://flink-gcp-cloudtasks-benchmark/runs/` unless `--evidence-root` names another.
Part indices start at `000001` and are contiguous within a creator incarnation.
Each part is created with `NO_OVERWRITE`, so a repeated incarnation fails instead of replacing evidence.
A part is opened by its first row and rolled before the next row once it holds 8 MiB of uncompressed rows or has been open for 60 seconds.
Only a closed part is durable and counted; an abrupt process loss loses at most the open part.
Pod logs carry only Flink logs in window mode.
Record-count mode still prints the rows to standard output for local wiring checks.

The payload records the origin on entry to serialization.
The completion timestamp precedes output formatting and is captured before notifying the production runtime of the RPC result.
This interval includes body generation, naming, staging, checkpoint waiting, client admission and RPC completion after the serializer entry.
It excludes upstream source/partition waiting and time before the serializer is invoked.
A response is an acknowledgement upper bound on service visibility; the app does not infer subsecond visibility from Cloud Tasks' second-truncated `create_time`.

A restored envelope may cross JVM incarnations.
Its serializer-to-completion value is `-1` when the process UUID changes; the offline analyzer excludes and counts that sentinel rather than treating it as a latency, and compares no monotonic clock across JVMs.
The wall-clock origin is retained for separately bounded recovery observations.
Per-attempt rows include retries and are not themselves unique-record throughput; the analyzer must reconcile sequence/name outcomes and successful job completion.

Successful RPC completion is published only after the evidence output accepts the row; in window mode that means the row entered the open part, not that the part is durable.
An output failure becomes a failed result unless the observation future has already been cancelled.
Caller cancellation is forwarded to the underlying RPC and can complete the observation future before its evidence row is written.
Row formatting and compression remain part of the workload's CPU and backpressure cost, even though they are outside the recorded completion timestamp.
Per-record output requires an evidence-loss check and a measured overhead control before service results can support a verdict.

## Calibration controls

The delay control sleeps for 100 milliseconds inside serialization after the payload origin timestamp is captured.
It adds a known latency cost and, with one serializer subtask, limits input processing to at most roughly ten records per second before other costs.
It does not simulate a Cloud Tasks regression or alter the production RPC deadline/retry implementation.
The host calibration must demonstrate that its throughput/latency observations detect this injected regression at an offered rate above the control's ceiling.
An interrupted delay fails serialization and preserves the thread's interrupted flag.

The counts-only control validates returned names and dispatch counts but skips CSV formatting/output.
Its terminal observation count is the number of successful observer callbacks, not a count of written CSV rows.
Compare it with an otherwise identical CSV-enabled run to assess output overhead using independent throughput/resource observations.
A counts-only run cannot supply a per-record p95 or complete CSV evidence.
Every receipt identifies both controls; the collector refuses a main cell whose receipts do not carry `control_delay_millis=0` and `csv_enabled=true`.
Controls and receipt export still need execution-host validation; the local checks do not establish acceptable instrument overhead.

## Independent receipt files

Window mode writes small JSON receipts through the installed Flink filesystem plugin under `<evidence root><run>/cells/<cell>/receipts/`.
Record-count mode performs no receipt-storage access.
The input generator registers a fresh source incarnation before its first mapped sequence and records the final mapping separately.
A restored source can start above sequence zero; these receipts report mapping boundaries, not downstream delivery, checkpoint completion or task creation.

Each RPC client registers a creator incarnation before its first attempt; that UUID also identifies its CSV rows.
Registration failure closes the client and prevents admitting it to the sink.
At close, a separate terminal receipt records reserved attempts, completed calls, successful observer callbacks, output failure, attempt-limit exhaustion and client-close failure.
It also records the rows in closed parts as `rows_exported`, the number of closed parts as `parts_closed`, and whether closing the open part failed as `rows_flush_failed`.
The client is closed first, then the open part, and only then is the snapshot taken, so a flush failure is visible in the receipt and is still raised to the runtime after the receipt is written.
The attempt count comes from RPC admission, independently of the CSV exporter.
The terminal snapshot is complete only when all three counts agree, none of those failures occurred, and, for a CSV-enabled cell, `rows_exported` equals `observations`.
The counts-only control writes no rows, so its snapshot does not require that equality.
Close with an outstanding callback produces an incomplete snapshot; a later callback never upgrades that persisted snapshot.
An abrupt process loss can leave a registration without a terminal receipt.

The reconciler (`flink_tier3/cloudtasks/evidence.py`) discovers registrations from the receipts rather than the logs, requires matching terminal receipts for a steady-state result, and compares the rows decoded from the parts with `rows_exported` and the terminal counts.
Thus losing the final CSV row or all rows from one registered creator cannot be concealed by counting the remaining CSV itself.
A complete receipt certifies local accounting and that every part closed without a reported error; it does not certify successful service creation, a read-back of the parts, job success or a performance pass.
Read back the receipts and reconcile record/task outcomes and job status before accepting a cell.
Recovery evidence must explicitly account for incomplete prior incarnations instead of inventing their missing observations.

Receipts carry schema version 1, run/cell/arm identity, role, incarnation, process UUID and wall/monotonic clock samples.
They contain no task body, target URL or exception text.
Wall/monotonic samples do not prove synchronized clocks across hosts.
On the single-host rig, `flink_tier3.cloudtasks.vmanalyze` applies the preregistered window rule to checkpoint completions the probe is to record in its own clock, and records which clock each instant belongs to, preserving the cross-JVM latency exclusion above; the probe does not record them yet, so such a run has no window ([ADR-0162](../../../docs/adr/0162-cloud-tasks-implementation-precedes-final-performance-acceptance.md)).
The worker writes receipts under the run's own directory, and the controller records their digests when the run finishes.
Receipt writes and their failure/overhead behavior still require execution-host calibration.

## Resource boundary and remaining acceptance

The source count and per-client-incarnation RPC ceiling are finite.
Each client refuses sends after three times `--records` attempts in record-count mode, or the explicit `--attempt-limit` in window mode, including retries.
This is not a global run ceiling: external admission must account for parallelism, restarts, replay, all cells, observation reads and administrative operations.
The application does not terminate Pods, delete queues or override the lifecycle supervisor.

A cluster execution would still need the reviewed image, a Cloud Tasks delivery and admission, which the cluster rig no longer has, fixed numeric resource and cost approval, verified queue settings, and cleanup evidence.
The full assessment additionally needs three repetitions after warm-up, a sensitivity control, checkpoint/heap/copy/pending/backlog/recovery observations, and the ADR-0104 variability and support decisions.
Local tests validate observation and runtime wiring; they establish no real-service performance or release verdict.

## Recovery application

`CloudTasksRecoveryJob` writes a finite, checkpointed input through the production `CloudTasksSink` in `EXACTLY_ONCE` (staged) mode to one paused queue in `flink-gcp/us-central1`.
The JAR's manifest names it, and the image installs the JAR as `/opt/flink/usrlib/cloudtasks-recovery.jar`.
The application creates no queue and changes, resumes or deletes none: the lifecycle runner creates the run's queue paused before the job starts and deletes it at cleanup.

### Recovery arguments

Arguments use `--name value` pairs; missing values, duplicate options and unknown options are rejected.

| Argument | Accepted value |
| --- | --- |
| `--run-id` | The Tier-3 run label: 1–40 lowercase letters, digits and inner hyphens |
| `--queue` | The queue ID: `ct1362-` followed by a run label |
| `--records` | Total input records, from 2, so both sink subtasks receive input, to 360,000, one hour at the highest rate |
| `--records-per-second` | Source ceiling from 1 to 100 |
| `--phase` | `initial` (default) or `upgrade` |
| `--require-restored` | `false` (default) or `true`; `--phase upgrade` requires `true` |

### Graph and sink

One source subtask generates the sequences 0 to `records - 1` at the ceiling with Flink Datagen.
An identity operator, also one subtask, checks that each sequence is the next one and keeps the trial identity, a lineage UUID and its position in operator state.
A restored state must carry the same run, queue, record count and rate, so state from another trial or configuration fails the job; `--require-restored true` refuses a fresh start.
Sequence `s` goes to sink subtask `s mod 2`, and the sink runs at parallelism 2 with maximum parallelism 128.
The source, identity and sink UIDs are `ct1362-source-v1`, `ct1362-input-v1` and `ct1362-sink-v1`.
Checkpoints run every 30 seconds, one at a time.

The sink is built with the public builder: a fixed `queue(...)`, `deliveryGuarantee(EXACTLY_ONCE)`, and the default staged and writer options.
Under those defaults the committer reads the queue's `tombstoneTtl` back before it creates a task and refuses a queue that keeps names for less than an hour, so the workload identity needs `cloudtasks.queues.get` beside `cloudtasks.tasks.create`.
Each task is an HTTP POST to `https://ct1362.invalid/task` whose body repeats its task key; RFC 6761 reserves `.invalid`, and a paused queue dispatches nothing.
The committers create each checkpoint's tasks in one burst rather than at the source rate.
The ceiling is 100 records per second because [#1246](https://github.com/flink-gcp/flink-connector-gcp/issues/1246) measured staged creation keeping up at that rate on four subtasks with 10-second checkpoints. A 30-second checkpoint of it is 3,000 creates, which two committers on the default single channel, measured at about 210 creates per second per subtask in [#937](https://github.com/flink-gcp/flink-connector-gcp/issues/937), send in seconds and below the 500 per second at which Google's 500/50/5 guidance starts a new queue.

### Task names

The task key of sequence `s` is `<run-id>/<s>`, and the sink names the task with the key's SHA-256 digest in lowercase hex: `projects/flink-gcp/locations/us-central1/queues/<queue>/tasks/<digest>`.
A listed task name therefore maps back to one sequence through the digests of the run's keys, and a name outside that table did not come from this input.
A record replayed after a recovery addresses the same name, and the connector counts the service's `ALREADY_EXISTS` for it as success.

### Progress lines

The identity operator writes three kinds of line to the Flink log:

```text
event=cloudtasks-progress run_id=<run> phase=<phase> lineage=<uuid> restored=<true|false> processed=<n> sequence=<n - 1>
event=cloudtasks-snapshot run_id=<run> phase=<phase> lineage=<uuid> checkpoint_id=<id> processed=<n>
event=cloudtasks-checkpoint-complete run_id=<run> phase=<phase> lineage=<uuid> checkpoint_id=<id>
```

A progress line is logged for the first record of each incarnation, whenever the processed count reaches a multiple of `30 × records-per-second`, and for the last record.
It is the shape the shared recovery exercise in [`exercise.py`](../../../tools/tier3/src/flink_tier3/exercise.py) parses, and `RecoveryInputTest` holds it against that pattern.
The lineage stays the same across a restore, and `restored` reports whether this incarnation started from state.

### Local recovery tests

`RecoveryJobITCase` runs the deployed graph and the production sink against `FakeCloudTasks`, an in-process `CreateTask` server that refuses a repeated name with `ALREADY_EXISTS`.
A run, and a run stopped with a savepoint and resumed from it in the `upgrade` phase, each end with exactly the expected task names.
The fake has no queue to read, and the connector skips the retention readback for an emulator endpoint; what the real service does is the trial's question.
