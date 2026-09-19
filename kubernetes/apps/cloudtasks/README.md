# Cloud Tasks measurement application

This internal application supplies the DataStream workload and per-attempt observations for [issue #1246](https://github.com/flink-gcp/flink-connector-gcp/issues/1246).
It runs on Java 17 and uses the reactor's Flink version and Cloud Tasks connector.
It is built only by the `tier3-cloudtasks` Maven profile and is never deployed to Maven Central.

## Build

Run `mise x -- just tier3-cloudtasks-verify` from the repository root.
The build creates `target/cloudtasks-measurement.jar` and retains the unmodified runtime dependency JARs in `target/image-lib/`.
The [publication workflow](../../../.github/workflows/tier3-images.yaml) selects Flink 2.2.1 or 1.20.4, verifies that reactor version and supplies its reviewed AMD64 base digest to the Dockerfile.
The two payloads use separate GAR packages: `cloudtasks-measurement` and `cloudtasks-measurement-flink120`.
The image build checks that the matching bundled GCS plugin exists before enabling it.
Its digest must be published and independently verified before a later delivery selects it.
This change does not publish an image, admit a Kubernetes workload or create a queue.

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
The analyzer must reconcile `ALREADY_EXISTS` by task name and sequence with earlier attempts and recovery evidence from the same cell.
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

The payload records the origin on entry to serialization.
The completion timestamp precedes output formatting and is captured before notifying the production runtime of the RPC result.
This interval includes body generation, naming, staging, checkpoint waiting, client admission and RPC completion after the serializer entry.
It excludes upstream source/partition waiting and time before the serializer is invoked.
A response is an acknowledgement upper bound on service visibility; the app does not infer subsecond visibility from Cloud Tasks' second-truncated `create_time`.

A restored envelope may cross JVM incarnations.
Its serializer-to-completion value is `-1` when the process UUID changes; an analyzer must not treat that sentinel as a valid latency or compare monotonic clocks across JVMs.
The wall-clock origin is retained for separately bounded recovery observations.
Per-attempt rows include retries and are not themselves unique-record throughput; the analyzer must reconcile sequence/name outcomes and successful job completion.

Successful RPC completion is published only after evidence output succeeds.
An output failure becomes a failed result unless the observation future has already been cancelled.
Caller cancellation is forwarded to the underlying RPC and can complete the observation future before its evidence row is written.
Logging remains part of the workload's CPU and backpressure cost, even though its formatting is outside the recorded completion timestamp.
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
Every receipt identifies both controls; the later main-measurement admission and analyzer must require `control_delay_millis=0` and `csv_enabled=true`.
Controls and receipt export still need execution-host validation; the local checks do not establish acceptable instrument overhead.

## Independent receipt files

Window mode writes small JSON receipts through the installed Flink GCS filesystem plugin under `gs://flink-gcp-cloudtasks-benchmark/runs/<run>/cells/<cell>/receipts/`.
Record-count mode performs no receipt-storage access.
The input generator registers a fresh source incarnation before its first mapped sequence and records the final mapping separately.
A restored source can start above sequence zero; these receipts report mapping boundaries, not downstream delivery, checkpoint completion or task creation.

Each RPC client registers a creator incarnation before its first attempt; that UUID also identifies its CSV rows.
Registration failure closes the client and prevents admitting it to the sink.
At close, a separate terminal receipt records reserved attempts, completed calls, successful observer callbacks, output failure, attempt-limit exhaustion and client-close failure.
The attempt count comes from RPC admission, independently of the CSV exporter.
The terminal snapshot is complete only when all three counts agree and none of those failures occurred.
Close with an outstanding callback produces an incomplete snapshot; a later callback never upgrades that persisted snapshot.
An abrupt process loss can leave a registration without a terminal receipt.

The collector must discover registrations independently of the logs, require matching terminal receipts for a steady-state result, and compare exported rows with the terminal counts.
Thus losing the final CSV row or all rows from one registered creator cannot be concealed by counting the remaining CSV itself.
A complete receipt certifies local accounting, not successful service creation, durable CSV export, job success or a performance pass.
Read back the receipts and reconcile record/task outcomes and job status before accepting a cell.
Recovery evidence must explicitly account for incomplete prior incarnations instead of inventing their missing observations.

Receipts carry schema version 1, run/cell/arm identity, role, incarnation, process UUID and wall/monotonic clock samples.
They contain no task body, target URL or exception text.
Wall/monotonic samples do not prove synchronized clocks across hosts.
The analyzer must establish the observation window and clock bounds independently, preserving the cross-JVM latency exclusion above.
The worker writes receipts to temporary benchmark storage; the lifecycle collector must export them to retained evidence before deleting the run prefix.
Receipt writes and their failure/overhead behavior still require execution-host calibration.

## Resource boundary and remaining acceptance

The source count and per-client-incarnation RPC ceiling are finite.
Each client refuses sends after three times `--records` attempts in record-count mode, or the explicit `--attempt-limit` in window mode, including retries.
This is not a global run ceiling: external admission must account for parallelism, restarts, replay, all cells, observation reads and administrative operations.
The application does not terminate Pods, delete queues or override the lifecycle supervisor.

Publication and execution still require the reviewed image, Cloud Tasks delivery/admission support, fixed numeric resource and cost approval, verified queue settings, and cleanup evidence.
The full assessment additionally needs three repetitions after warm-up, a sensitivity control, checkpoint/heap/copy/pending/backlog/recovery observations, and the ADR-0104 variability and support decisions.
Local tests validate observation and runtime wiring; they establish no real-service performance or release verdict.
