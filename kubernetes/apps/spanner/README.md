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

Whether a write that repeats identical values produces a change record at all is not established here; the trials' pilot measures it, and until then a key relayed once may have been written more than once.
The execution count is also a lower bound for a second reason: a global restart before the first completed checkpoint starts a new coordinator at a later `latest()`, so commits from before it are not relayed, although the restarted input writes every key again.
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
Checkpoints, savepoints and Kubernetes HA data sit beneath `gs://flink-gcp-tier3-spanner/runs/<run-id>/`, deleted one day after creation.
One JobManager on normal capacity and one or two single-slot Spot TaskManagers each request and limit 1 CPU, 2 GiB memory and 1 GiB ephemeral storage, as the other recovery applications do.
Savepoint upgrades disable last-state fallback, and the Operator reports the upgrade savepoint in the application's status, where the shared recovery exercise reads it.
The `tier3-spanner` namespace, its KSA and RBAC follow in [#1875](https://github.com/flink-gcp/flink-connector-gcp/issues/1875), the Operator's watch set in [#1876](https://github.com/flink-gcp/flink-connector-gcp/issues/1876), and the rig scenario that creates the instance and its database, seeds it and judges the trial in [#1877](https://github.com/flink-gcp/flink-connector-gcp/issues/1877).
