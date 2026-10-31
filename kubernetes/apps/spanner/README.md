# Spanner Tier-3 recovery application

This internal Flink 2.2.1 / Java 17 application is the workload of the Spanner recovery trials of [issue #1360](https://github.com/flink-gcp/flink-connector-gcp/issues/1360) and its Change Streams sub-issue [#1872](https://github.com/flink-gcp/flink-connector-gcp/issues/1872), as their [scope](https://github.com/flink-gcp/flink-connector-gcp/issues/1360#issuecomment-6270858712) decided.
One job graph carries all three components the trials recover.
A finite, checkpointed input reaches the production `SpannerSink`, the production `SpannerChangeStreamSource` relays what the sink wrote, and the production `SpannerSource` copies a seeded table.
Every connector uses the workload's application default credentials; no emulator endpoint can be configured from the arguments.

## Build and local validation

```sh
mise x -- just tier3-spanner-verify
```

The opt-in `tier3-spanner` Maven profile keeps the application outside published connector artifacts.
The build produces `target/spanner-recovery.jar` and copies runtime dependencies into `target/image-lib/`.
The Dockerfile adds these files to `/opt/flink/usrlib/` over the reviewed Flink image and enables its bundled GCS filesystem plugin.
The entry point is `io.github.flink.gcp.connector.tier3.spanner.SpannerRecoveryJob`.
The [image publication workflow](../../../.github/workflows/tier3-images.yaml) packages it as `spanner-recovery` on the fixed Flink 2.2.1 / Java 17 AMD64 base, and the application CI lane builds the Dockerfile without publishing.

Unit tests cover argument and input bounds, the input row, the relay's deserializer and row, the seed copy's row, the delivery names, and refusal of state from another trial.
A MiniCluster test runs the production graph against the Spanner emulator through savepoint upgrades at parallelism 2→2, 2→1 and 1→2.
It requires every input row, every input key relayed and every seed row copied afterwards, with deliveries from both phases in each relayed table; a pace too fast finishes the copy before the savepoint and fails it.
The same test class pins the graph's operator UIDs, vertex names, parallelisms and max parallelism.

The emulator is not evidence about the service.
It says nothing about workload identity, service partition changes, how the service plans a batch read's partitions, or how the service treats a change-stream bound in the future; the deployed trials measure those.

## Database layout

The rig's runner creates one instance per run in project `flink-gcp`, named `sp1360-` followed by a run label, and passes it as `--instance`.
The application expects one GoogleSQL database, `recovery`, in it:

| Object | Contents |
| --- | --- |
| Table `input` | `k STRING(MAX) NOT NULL, n INT64 NOT NULL`, primary key `k`. The input sink writes it. |
| Change stream `input_changes` | `FOR input`. The Change Streams source reads it. |
| Table `relay` | `k STRING(MAX) NOT NULL, delivery STRING(MAX) NOT NULL, commit_micros INT64 NOT NULL`, primary key `(k, delivery)`. |
| Table `seed` | `k STRING(MAX) NOT NULL, n INT64 NOT NULL`, primary key `k`. The runner fills it before the job starts, and the batch read reads it. |
| Table `batch_out` | `k STRING(MAX) NOT NULL, delivery STRING(MAX) NOT NULL, n INT64 NOT NULL`, primary key `(k, delivery)`. |

The database's version retention period must cover the trial window, because the batch read reads `seed` at the snapshot it planned when the job first started, and a restore resumes at that snapshot.

## Graph and what each table records

Record `n` of `--records` has the key `<n mod 16 in hex>/<n, seven digits>`.
The input runs at parallelism one; every other vertex runs at `--parallelism`, one or two.

- **Sink into `input`:** each record writes its key and sequence through `insertOrUpdate`. A replay writes the same values again, so the final table holds one row per record however often the sink executed the write; its executions are counted from the change stream instead.
- **Change Streams relay from `input` into `relay`:** for every row a change modifies, the relay writes the key with a delivery name, `<phase>/<uuid>/<n>`: the phase, a UUID drawn when the task instance opens, and the delivery's position in that instance. The row holds the change's commit time in microseconds.
  - A key's rows count the source's deliveries of its changes.
  - Each write the sink executes commits under a commit time of its own, so the distinct commit times of a key count the sink's executions of its write.
  - A delivery that repeats a commit time already relayed for its key is the source's replay.
  - A change whose keys are not one input key is relayed under its raw keys, which match no input key, for the oracle to report.
- **Batch copy from `seed` into `batch_out`:** the batch read reads `seed` at one snapshot, and each row it delivers is written with a delivery name of the same shape.
  - A key's rows count its deliveries, so a partition the source reads again after a restore adds a row for each of its seed rows.
  - A pace holds each reading subtask to `--seed-rows-per-second`, so the read's partitions stay unfinished through both recoveries of a trial. The copy's total rate is that times the subtasks holding partitions, which a rescale changes, and a row late after a stall is not followed by a burst.
  - The pace is chained into the source's task, so no network buffer of read-ahead rows stands between a checkpoint barrier and the source, and a barrier waits at most one pause.
  - Each partition's read is one streaming read, which the pinned client (google-cloud-spanner 6.124.0) ends after an hour without retrying, so the rig sizes the seed and the pace to read every partition within that.

Every execution produces a change record, including a write that repeats identical values, as [#1878's identical-write probe](https://github.com/flink-gcp/flink-connector-gcp/issues/1878#issuecomment-6525682846) measured through both `BatchWrite` and `Commit`.
The execution count is still a lower bound, because the relay can miss a commit: a global restart before the first completed checkpoint starts a new coordinator at a later `latest()`, so commits from before it are not relayed, although the restarted input writes every key again, and the Change Streams oracle judges keys, so it does not detect such a lost commit of a key relayed otherwise.
The trials inject their faults only after a completed checkpoint.

On a fresh start the Change Streams source reads from the instant its coordinator initializes, and it stops at `--change-stream-end`, so the job finishes once the input is complete, the source reaches its end and the copy is done.
Starting there loses nothing, for three reasons:

- The input sink holds its batch until a checkpoint. Each two-column row costs two of its 80,000 cells, so the batch overflows at 40,000 rows, minutes of input at the highest rate, while a checkpoint's input is a few thousand.
- The coordinator refuses to snapshot before it has initialized, so no checkpoint completes before the start.
- The input outlasts the coordinator's initialization. The sink also writes when its input ends, without a checkpoint, so an input of a few records at a high rate could finish before the start; a trial's input runs for many minutes.

Spanner refuses a start before the change stream's creation, so the source cannot start earlier; the rig would have to render the start before the database exists.
The start is read from the JobManager's clock and the commit times from the service's, which holds only while the JobManager's clock does not lead the service's by more than the time from initialization to the first checkpoint, as for the Bigtable trial.
A restore resumes from the checkpointed positions instead.
The bound is inclusive and checkpointed with the partition ledger, so every phase of a run passes the same `--change-stream-end`; the source refuses a restore under another.

Flink chains the graph into four vertices, whose names the rig's vertex selection reads:

| Vertex | Parallelism |
| --- | --- |
| `Source: Spanner input -> Spanner input identity` | 1 |
| `Spanner input: Writer` | `--parallelism` |
| `Source: Spanner change stream -> Spanner relay delivery -> Spanner relay: Writer` | `--parallelism` |
| `Source: Spanner seed -> Spanner seed pace -> Spanner seed delivery -> Spanner batch copy: Writer` | `--parallelism` |

Progress lines use the `spanner-progress` event in the shape the shared recovery exercise parses; snapshot and checkpoint-complete lines use the `spanner-snapshot` and `spanner-checkpoint-complete` events.

## Arguments

| Argument | Meaning |
| --- | --- |
| `--run-id` | The Tier-3 run label. |
| `--instance` | The run's instance ID, `sp1360-` followed by a label. |
| `--records` | Records in the input, 2 to 360,000. |
| `--records-per-second` | Offered input rate, 1 to 100. |
| `--change-stream-end` | An ISO-8601 instant at which the Change Streams source stops, inclusive. |
| `--seed-rows-per-second` | The pace of the seed copy, 1 to 1,000. |
| `--phase` | `initial` or `upgrade`. |
| `--require-restored` | `true` refuses a fresh start; `upgrade` requires it. |
| `--parallelism` | Subtasks of every vertex but the input, 1 or 2 (default 2). |

The run, instance, input and change-stream end form the trial identity that the restored input state must match; the phase, parallelism and seed pace may change in an upgrade.
The check runs only while the input is active: once the input has finished, Flink restores its vertex as finished without initializing its state, so a later restore is not checked.

## Deployment definition

[`pkg/spanner.#Application`](../../pkg/spanner/application.cue) defines the job on Flink 2.2.1 / Java 17, and the [`spanner-recovery`](../../lifecycle/spanner.cue) lifecycle scenario renders its initial and upgrade phases from one input identity.
The delivery supplies the run ID, image digest, phase, parallelism and change-stream end, and optionally the record count and rate, 180,000 records at 100 per second unless given, and the seed pace, 50 rows per second per reading subtask unless given; the package refuses more than 30 minutes of input.
It names the instance `sp1360-<run-id>`, which a run ID of the full 40 characters keeps at 47 of Spanner's 64, and passes no emulator endpoint or credentials file.

The deployment runs as the `tier3-spanner/spanner` KSA, which selects the `tier3-spanner` GSA through Workload Identity; the [GCP grants](../../../opentofu/flink-gcp/tier3-spanner.tf) limit it to the `sp1360-` instances.
Checkpoints, savepoints and Kubernetes HA data sit beneath `gs://flink-gcp-tier3-spanner/runs/<run-id>/`, deleted one day after creation, and two consecutive failed checkpoints are tolerated before the job restarts.
One JobManager on normal capacity and one or two single-slot Spot TaskManagers each request and limit 1 CPU, 2 GiB memory and 1 GiB ephemeral storage, as the other recovery applications do.
Savepoint upgrades disable last-state fallback, and the Operator reports the upgrade savepoint in the application's status, where the shared recovery exercise reads it.
The `tier3-spanner` namespace, its KSA and RBAC are in the [bootstrap root](../../../opentofu/tier3-bootstrap/README.md#spanner-recovery-foundation) ([#1875](https://github.com/flink-gcp/flink-connector-gcp/issues/1875)), and the shared preflight and the Operator's watch set include it ([#1876](https://github.com/flink-gcp/flink-connector-gcp/issues/1876)); the [recovery trial](#recovery-trial) below creates the instance and its database, seeds it and judges the trial.

## Recovery trial

The `spanner-recovery` scenario of the [lifecycle](../../lifecycle/README.md) runs this application once as a FlinkDeployment in `tier3-spanner` against an instance it creates for the run, measures the job before and after each recovery, and judges each of the three components on its own table.
It is the trial set of [#1360](https://github.com/flink-gcp/flink-connector-gcp/issues/1360) and [#1872](https://github.com/flink-gcp/flink-connector-gcp/issues/1872), and [ADR-0165](../../../docs/adr/0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md#spanner-recovery-trial) records its design ([#1877](https://github.com/flink-gcp/flink-connector-gcp/issues/1877)).
It follows the [Bigtable recovery trial](../bigtable/README.md#recovery-trial), whose resource lifecycle, finite-input exercise, measurement windows and campaign it shares; this section names what differs.

### Trial and estimate

Every trial has the same input, 180,000 records at 100 records per second, which is 30 minutes of input in a 90-minute window, and the same seed, 120,000 rows copied at 50 rows per second per reading subtask.
It runs a savepoint upgrade and then deletes a Pod, and its kind names the fault that makes it a trial of its own:

| Kind | Upgrade | Pod deleted | Subtasks of every vertex but the input, initial then upgrade |
| --- | --- | --- | --- |
| `jm-replacement` | Keeps its sizes | The JobManager | 2, 2 |
| `tm-replacement` | Keeps its sizes | The TaskManager that reported the latest input | 2, 2 |
| `rescale-out` | Rescales the job | The JobManager | 1, 2 |
| `rescale-in` | Rescales the job | The JobManager | 2, 1 |

The change-stream end is 3,900 seconds after the window's start, as for the Bigtable trial, so dispatch derives it from the window and the trial carries it.
Two reading subtasks copy the seed in about 20 minutes and one in about 40, so the copy is still reading through both recoveries and done long before the end.
The job finishes at the end, which leaves the table reads 540 seconds, up to a minute before the cleanup start.
Render a trial's offline proposal, which carries the estimate the owner approves, from the repository root:

```sh
mise x cue uv -- uv run --locked --package flink-tier3 --no-dev flink-tier3 render \
  --scenario spanner-recovery \
  --run-id example-1360 --nonce aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --started-at 2026-10-28T00:00:00Z --expires-at 2026-10-28T01:30:00Z \
  --active-seconds 5220 --revision "$(git rev-parse HEAD)" \
  --application-image us-central1-docker.pkg.dev/flink-gcp/flink-tier3/spanner-recovery@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --spanner-trial tm-replacement > /tmp/spanner-proposal.json
```

A proposal is not an approval: it carries `approved: false` and an empty `approval.json`, and rendering calls no cloud API.
The estimate is the same for every kind, about USD 2.24: the Pods for the window as the Bigtable estimate prices them, two hours of the 100-processing-unit instance at USD 0.09 an hour, and a USD 1.00 reserve for storage, state, evidence and telemetry.
The two hours round the 90-minute window up on the assumption, not measured, that the instance is billed for at least an hour.

### Dispatch

The [run workflow](../../../.github/workflows/tier3-run.yaml) admits `spanner-recovery` with two inputs beyond the common ones: `spanner_trial`, the kind, which the dispatch refuses when it is left at `none`; and `application_digest`, the published `spanner-recovery` GAR digest, verified live at dispatch and never pinned.
The owner approves the run's estimate before dispatching it; dispatching is the approval, as for the other scenarios.
The workflow job runs for up to 120 minutes, as for the other 90-minute trials.
Any run ID fits, because the run's instance is `sp1360-<run-id>`, at most 47 of the 64 characters an instance ID takes.
One dispatch can carry a campaign of the remaining kinds and a repeat budget, as the [Bigtable dispatch section](../bigtable/README.md#dispatch) describes; for example, `spanner_trial=jm-replacement`, `run_id=sp1878-jm-a1`, `campaign=tm-replacement,rescale-out,rescale-in` and `repeats=2` run the four trials from one dispatch, for at most six attempts.

### Instance lifecycle

After the supervisor heartbeats and the Operator is ready, the runner records the instance's intent, reads that the name is free, and records the creation intent and a call marker in one control write.
It then creates a Standard-edition instance of 100 processing units in `regional-us-central1` and reads it until it is `READY`, polling the instance rather than the create's long-running operation.
It creates the `recovery` database with one `CreateDatabase` call, whose extra statements create the four tables, the change stream and the two-hour version retention atomically with it, reads the database until it is `READY`, and reads it and its DDL back.
The readback compares the names of the tables and the change stream and the retention; a database that differs emits `spanner-database-deviation` and fails admission, and cleanup deletes the instance.
A database create that failed transiently, timed out or answered 409 is read back, and repeated while the database is absent, until the startup deadline, as a Bigtable table create is.

The runner then writes the seed on one multiplexed session, 120,000 rows in 12 single-use commits of 10,000 rows, each 20,000 of the 80,000 mutations a commit takes.
Each commit is an `insertOrUpdate` of fixed values, so one that failed transiently, or aborted and was answered 409, is repeated until the startup deadline.
It splits `seed` at `4`, `8` and `c`, one split point per request and 65 seconds apart: at 100 processing units the service adds one split point a minute and refuses a request over that whole with 429, which the runner retries until the startup deadline.
[#1877's measurement](https://github.com/flink-gcp/flink-connector-gcp/issues/1877#issuecomment-6472333184) planned the seed's batch read as one partition without them and seven at once with them.
The split points take about two minutes of the 600 seconds admission allows.
The runner's role cannot read rows or split points, so the receipt records the seed rows and the split points the service accepted rather than reading them back.
Only then does the runner open the namespace quota for three Pods and create the FlinkDeployment.
An instance that already carries the name, and a create the service refuses although the name read free, refuse the run and the instance is not deleted; the marker stays until the instance first reads back, as for the Bigtable instance.

Only the supervisor deletes the instance, and only one whose creation intent the record holds.
It waits for the runner's release and for the namespace to hold no Pod, deletes the instance, which takes the database and its split points with it, and reads it back until it is gone; #1877 measured a deletion with live split points accepted at the first call.
A deletion refused with status 400 or 409, or a call that failed transiently, emits `spanner-delete-pending` and is left to cleanup's next pass.
Shared cleanup then removes the run's state under `runs/<run-id>/` and `.inprogress/flink-gcp-tier3-spanner/runs/<run-id>/` in the `flink-gcp-tier3-spanner` state bucket.

A run whose cleanup never confirms the deletion, or whose create kept its marker, gets no idle receipt and keeps the environment lock, and the completed-execution recovery of the [lifecycle runbook](../../lifecycle/README.md#cleanup-and-recovery) repeats the cleanup but never clears a marker.
Then an operator verifies the instance by hand and deletes it:

1. Read `spanner-delete-pending` or the receipt's reason in the run's evidence, so a refusal that names a cause is resolved first.
2. Run `gcloud spanner instances describe sp1360-<run-id> --project=flink-gcp` and confirm that the instance is the run's: display name `sp1360 <run-id>` (or, for a run ID over 23 characters, `sp1360` with the run ID's first 14 characters and an eight-character hash), configuration `regional-us-central1` and 100 processing units, created within the run's window.
   For a kept marker that reads `NOT_FOUND`, the create's outcome is unknown, and no documented bound settles it.
   Spanner makes an instance readable, `CREATING`, as soon as its create request completes, but the runner's 20-second request timeout is the client's, not the service's.
   Describe it again at least ten minutes later and list `gcloud spanner instances list --project=flink-gcp --filter='name:sp1360-'`.
   One that appears is the run's to delete.
   If none appears, record the run ID with the incident, and list `sp1360-` instances again before the next Spanner dispatch, so a late landing is still found and deleted.
3. Run `gcloud spanner instances delete sp1360-<run-id> --project=flink-gcp` and describe it again until it is `NOT_FOUND`.

The control record still holds the marker or the unconfirmed deletion, so releasing the lock afterwards is incident recovery outside the rig, as for a BigQuery table create.

### Exercise and oracles

The supervisor runs the shared recovery exercise on the application's `spanner-progress` lines, with the faults and the three measurement windows of the Bigtable trial.
A reading is a `spanner-measurement` evidence event; the exercise finds the four vertices by the names [the graph](#graph-and-what-each-table-records) chains them under, because three of them are sources.
Its families are each component vertex's task, network and connector metrics and its subtasks' shares of the latest completed checkpoint, and each TaskManager's memory metrics, as for the Bigtable trial.
The change-stream vertex's connector family also takes Flink's `currentOutputWatermark`, which every operator of its chain registers; the source operator's is the source's watermark.
The Change Streams coordinator's `changeStreamPartitionsDiscovered` is read from the vertex's JobManager operator metrics for the report only; a failed read is recorded as a named absence.
A lost TaskManager restarts the input's failover region and the relay and copy subtasks it held, which share no edge with any other, so the exercise accepts up to three restores for it.

Before the baseline window opens, once a strong single-use query `SELECT 1 FROM batch_out LIMIT 1` returns a row, the supervisor writes the 16 late seed rows, numbered 120,000 to 120,015, in one single-use `insertOrUpdate` commit on its own multiplexed session.
A copied row exists only after the batch read planned its snapshot, so the late rows are committed after the snapshot the copy reads.
The record keeps `late_seed`, the commit timestamp and the row count, and the baseline window opens and the upgrade begins only after that record is written; a call that fails transiently, or a commit that aborts and is answered 409, emits `spanner-late-seed-pending` and is repeated at the next poll.
The exercise also keeps the plan the batch read's enumerator logs on the JobManager, `Spanner planned <n> partition(s) for the … at read timestamp <t> and parallelism <p>.`, or its one-partition form, for the batch report.

When the upgraded application reaches `FINISHED`, after the change stream's end, with the whole input processed, the supervisor reads `input`, `relay`, `seed` and `batch_out` with single-use queries in key order, in pages of up to 5,000 rows.
The first page is read strongly and returns its read timestamp, at which every later page is read, so the four tables are read at one snapshot; a page of `relay` or `batch_out` resumes after both columns of its key, and a page shorter than 5,000 rows ends its table.
A transiently failing query is repeated every 5 seconds until the read window closes, 600 seconds after the application finishes or a minute before the cleanup start, whichever is first; any other failure ends the run without a verdict.
Each oracle reports counts and the first 20 keys of each kind of deviation:

- **Sink, `input`:** every input key holds its sequence and no other key is there.
  It reports each key's executions, counted as the key's distinct commit times in `relay`, a lower bound only through a commit the relay missed, as [Graph and what each table records](#graph-and-what-each-table-records) describes.
- **Change Streams, `relay`:** every input key was delivered at least once, no delivered key lies outside the input, no key's last relayed commit is later than the change stream's end, and the source's watermark advanced after each recovery.
  The watermark is the least of its subtasks' `currentOutputWatermark`, and each window keeps its greatest reading: the baseline's must be above Flink's `Long.MIN_VALUE`, the upgraded window's above the baseline's and the finishing window's above the upgraded one's.
  The oracle is judged only when the input's last progress line came at least 120 seconds before the change stream's end and every window read the watermark; otherwise it is unjudged, never passed.
  It judges keys, not commits, so a lost second commit of a key whose other commit was relayed is not detected.
  It reports replays, deliveries beyond a key's commits, the deliveries of each phase, and the coordinator's greatest discovered partition count in each window.
- **Batch read, `batch_out`:** `seed` holds the 120,000 seed rows and the 16 late ones, each with its own value, every seed key was copied with one value, its own, and no foreign or late key was copied.
  It reports the replayed keys and extra deliveries, the replays by the key's first character, which the split points divide the table by, the greatest `partitionsReread` in each window, the plan's partition count and read timestamp, and whether the late commit came after that read timestamp.

### Verdict and analysis

The run is `usable` when both recoveries are proved, all three oracles pass, the Change Streams one having been judged, and each of the three windows read every family; anything short of that is `inconclusive`.
The exercise decides the verdict with the transition that completes the run, and the final receipt's `success` is that verdict.

`just tier3-analyze <dir>` recomputes the verdict from a downloaded evidence directory, with the coverage and each window's greatest watermark rebuilt from the exported readings and the recoveries' timing, as for the Bigtable trial.
It reads the oracle reports from the record rather than rebuilding them: the supervisor exports no rows, and the instance's deletion removes the tables.

The four trials of [#1878](https://github.com/flink-gcp/flink-connector-gcp/issues/1878) are preregistered in the [Spanner recovery trial preregistration](../../../docs/adr/evidence/0165-spanner-recovery-preregistration-1878.md), and each kind reached `usable`, as their [findings](../../../docs/adr/evidence/0165-spanner-recovery-findings-1878.md) record.
The measurements of #1877 all ran as the owner, so the runner's and the supervisor's grants in the [foundation's ADR section](../../../docs/adr/0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md#spanner-recovery-foundation) were first run by these trials, and no attempt's exported evidence holds a `PERMISSION_DENIED`.
