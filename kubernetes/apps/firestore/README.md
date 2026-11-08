# Firestore Tier-3 recovery application

This internal Flink 2.2.1 / Java 17 application is the workload of the Firestore recovery trials of [issue #1879](https://github.com/flink-gcp/flink-connector-gcp/issues/1879), as its [scope](https://github.com/flink-gcp/flink-connector-gcp/issues/1879#issuecomment-6707006890) decided.
One job graph carries all four components the trials recover.
A finite, checkpointed input reaches the production Native-mode `FirestoreSink` and the production Datastore-mode `DatastoreSink`, and the production `FirestoreSource` and `DatastoreSource` each copy a seeded collection or kind.
Every connector uses the workload's application default credentials; no emulator endpoint can be configured from the arguments.

## Build and local validation

```sh
mise x -- just tier3-firestore-verify
```

The opt-in `tier3-firestore` Maven profile keeps the application outside published connector artifacts.
The build produces `target/firestore-recovery.jar` and copies runtime dependencies into `target/image-lib/`.
The Dockerfile adds these files to `/opt/flink/usrlib/` over the reviewed Flink image and enables its bundled GCS filesystem plugin.
The entry point is `io.github.flink.gcp.connector.tier3.firestore.FirestoreRecoveryJob`.
The [image publication workflow](../../../.github/workflows/tier3-images.yaml) packages it as `firestore-recovery` on the fixed Flink 2.2.1 / Java 17 AMD64 base, and the application CI lane builds the Dockerfile without publishing.

Unit tests cover argument and input bounds, the input writes, the copies, the Datastore-mode deserializer, the delivery names, and refusal of state from another trial.
A MiniCluster test runs the production graph against the Firestore emulator, one container per mode, through savepoint upgrades at parallelism 2→2, 2→1 and 1→2.
It requires every input record in both databases and every seed record copied afterwards in both, with deliveries from both phases in each copy, and fewer keys copied by the upgrade than the seed holds, which only a read that resumed inside its split gives; a pace too fast finishes a copy before the savepoint and fails it.
The same test class pins the graph's operator UIDs, vertex names, parallelisms and max parallelism.

The emulator is not evidence about the service.
It implements neither `PartitionQuery` nor `__scatter__` sampling, so each read there is one split: the Native-mode source asks for one partition when it is pointed at the emulator, which the client library answers without a call, and the Datastore-mode splitter samples no keys and falls back to one split.
It also says nothing about workload identity, how the service plans a read's splits, or the service's read-time window; the deployed trials measure those.

## Database layout

The rig's runner creates two Standard-edition databases per run in project `flink-gcp`, one in Native mode and one in Datastore mode, named `fs1879-<run-id>-n` and `fs1879-<run-id>-d`, and passes them as `--native-database` and `--datastore-database`.
Both databases use the same names, as collections in the Native-mode database and as kinds in the Datastore-mode one:

| Collection or kind | Contents |
| --- | --- |
| `input` | One document or entity per input record, named by its key, with the integer field `n`. The input sinks write it. |
| `seed` | The same shape. The runner fills it before the job starts, and the batch reads read it. |
| `batch_out` | One document or entity per delivery, named `<key>@<delivery>`, with the string fields `k` and `delivery` and the integer field `n`. |

The Native-mode source reads `seed` as a collection group, so no other collection in the database may be called `seed`.
The Datastore-mode source reads the kind `seed` in the default namespace.

Both sources read at one read time, planned when the job first starts, and a restore resumes at that read time.
Without point-in-time recovery the service refuses a read time older than about an hour, and with it a read time that old must fall on a whole minute, which the planned read time does not ([ADR-0185](../../../docs/adr/0185-the-firestore-e2e-suite-creates-an-ephemeral-database-per-gated-class.md)).
So the rig sizes the seed and the pace to finish both reads, and every restore of them, well inside an hour of the job's first start.

## Graph and what each collection records

Record `n` of `--records` has the key `<n mod 16 in hex>-<n, seven digits>`, which holds no `/`, so it can name a document.
The input runs at parallelism one; every other vertex runs at `--parallelism`, one or two.

- **Sinks into `input`:** each record writes its sequence through `FirestoreWrite.set` in the Native-mode database and through `DatastoreMutation.upsert` in the Datastore-mode one. A replay writes the same values again, so each final `input` holds one document or entity per record however often the sink executed the write. Neither database has a change feed in this graph, so a repeated execution is not observable.
- **Batch copies from `seed` into `batch_out`:** each source reads its database's `seed` at one read time, and each record it delivers is written with a delivery name, `<phase>:<uuid>:<n>`: the phase, a UUID drawn when the task instance opens, and the delivery's position in that instance.
  - A key's copies count its deliveries. Both sources resume a split after the last record they emitted before the restored checkpoint, so a restore adds a copy only for what was emitted after that checkpoint.
  - Each read asks for four splits, more than the largest trial parallelism, so a rescale has splits to move. Both counts are upper bounds that the service answers below for a small seed.
  - A pace holds each reading subtask to `--seed-records-per-second`, so the reads' splits stay unfinished through both recoveries of a trial. The copy's total rate is the pace times the subtasks holding splits, which a rescale changes, and a record late after a stall is not followed by a burst.
  - The pace's bound, 200, stays below what the copy sinks' ramp-up throttles pass from their first write, which a restore starts again: 500 operations per second for the whole Datastore-mode sink and 500 for each Native-mode subtask. So the pace, not a throttle, sets each read's length.
  - The pace is chained into the source's task, so no network buffer of read-ahead records stands between a checkpoint barrier and the source, and a barrier waits at most one pause.

The job finishes once the input is complete and both copies are done.

Flink chains the graph into five vertices, whose names the rig's vertex selection reads:

| Vertex | Parallelism |
| --- | --- |
| `Source: Firestore input -> Firestore input identity` | 1 |
| `Firestore native input: Writer` | `--parallelism` |
| `Firestore datastore input: Writer` | `--parallelism` |
| `Source: Firestore native seed -> Firestore native seed pace -> Firestore native seed delivery -> Firestore native batch copy: Writer` | `--parallelism` |
| `Source: Firestore datastore seed -> Firestore datastore seed pace -> Firestore datastore seed delivery -> Firestore datastore batch copy: Writer` | `--parallelism` |

Progress lines use the `firestore-progress` event in the shape the shared recovery exercise parses; snapshot and checkpoint-complete lines use the `firestore-snapshot` and `firestore-checkpoint-complete` events.

## Arguments

| Argument | Meaning |
| --- | --- |
| `--run-id` | The Tier-3 run label. |
| `--native-database` | The run's Native-mode database ID: `fs1879-` and a lowercase name of letters, digits and hyphens that ends in a letter or digit, at most 63 characters in all. |
| `--datastore-database` | The run's Datastore-mode database ID, of the same form and different from the Native-mode one. |
| `--records` | Records in the input, 2 to 360,000. |
| `--records-per-second` | Offered input rate, 1 to 100. |
| `--seed-records-per-second` | The pace of each seed copy per reading subtask, 1 to 200. |
| `--phase` | `initial` or `upgrade`. |
| `--require-restored` | `true` refuses a fresh start; `upgrade` requires it. |
| `--parallelism` | Subtasks of every vertex but the input, 1 or 2 (default 2). |

The run, both databases and the input form the trial identity that the restored input state must match; the phase, parallelism and seed pace may change in an upgrade.
The check runs only while the input is active: once the input has finished, Flink restores its vertex as finished without initializing its state, so a later restore is not checked.

The [GCP grants](../../../opentofu/flink-gcp/tier3-firestore.tf) and the [`firestore-recovery`](../../lifecycle/firestore.cue) delivery are in place, and so are the namespace and the Operator's watch over it ([#2083](https://github.com/flink-gcp/flink-connector-gcp/issues/2083)).

## Recovery trial

The `firestore-recovery` scenario of the [lifecycle](../../lifecycle/README.md) runs this application once as a FlinkDeployment in `tier3-firestore` against two databases it creates for the run, measures the job before and after each recovery, and judges each of the four components on its own collection or kind.
It is the trial set of [#1879](https://github.com/flink-gcp/flink-connector-gcp/issues/1879), and [ADR-0165](../../../docs/adr/0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md#firestore-recovery-trial) records its design ([#2084](https://github.com/flink-gcp/flink-connector-gcp/issues/2084)).
It follows the [Spanner recovery trial](../spanner/README.md#recovery-trial), whose resource lifecycle, finite-input exercise, measurement windows and campaign it shares; this section names what differs.
The [preregistration](../../../docs/adr/evidence/0165-firestore-recovery-preregistration-2085.md) of [#2085](https://github.com/flink-gcp/flink-connector-gcp/issues/2085) fixes its deployed runs; the first pilot stopped on the Datastore-mode seed before the application existed, as its revision records.

### Trial and estimate

Every trial has the same input, 180,000 records at 100 records per second, which is 30 minutes of input in a 90-minute window, and the same seed in each database, 108,000 records copied at 50 records per second per reading subtask.
Its kinds, their faults and their subtask counts are the [Spanner trial's](../spanner/README.md#trial-and-estimate).

Both reads read at one read time planned when the job first starts, and the service refuses a read time older than about an hour, as [Database layout](#database-layout) describes, so the seed is sized from both ends.
Two reading subtasks copy a seed in 1,080 seconds, past the failover's Pod deletion, which the Spanner trials made about 830 seconds into the input, so the copies are still reading through both recoveries.
One reading subtask, which a read the service plans as one split gives, copies it in 2,160 seconds; with both recoveries' downtime, at most 114 and 101 seconds in the Spanner trials, and a checkpoint interval read again after each restore, the read ends about 2,440 seconds after the first start, inside 45 minutes.
The job finishes once both copies are done, and the oracles then have up to 1,200 seconds, which the slowest job still leaves before the cleanup start.
Render a trial's offline proposal, which carries the estimate the owner approves, from the repository root:

```sh
mise x cue uv -- uv run --locked --package flink-tier3 --no-dev flink-tier3 render \
  --scenario firestore-recovery \
  --run-id example-1879 --nonce aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --started-at 2026-11-08T00:00:00Z --expires-at 2026-11-08T01:30:00Z \
  --active-seconds 5220 --revision "$(git rev-parse HEAD)" \
  --application-image us-central1-docker.pkg.dev/flink-gcp/flink-tier3/firestore-recovery@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --firestore-trial tm-replacement > /tmp/firestore-proposal.json
```

The proposal is not an approval, as for the [Spanner trial](../spanner/README.md#trial-and-estimate).
The estimate is the same for every kind, about USD 3.07: the Pods for the window as the Spanner estimate prices them, about USD 1.06, the planned operations, about USD 1.02, and a USD 1.00 reserve for storage, state, evidence and telemetry.
The operations are 792,032 writes, each database's input, seed, late records and copies, at USD 0.09 per 100,000, and 1,008,032 reads, each copy's read of its seed and the oracles' reads of `input`, `seed` and `batch_out`, at USD 0.03 per 100,000.
Both prices are planning assumptions for Standard-edition operations without the free tier; on 2026-11-08 they matched the `us-central1` prices on the [pricing page](https://cloud.google.com/firestore/pricing), which the [preregistration](../../../docs/adr/evidence/0165-firestore-recovery-preregistration-2085.md#estimate) records.
A database is billed by operation and storage, so the estimate has no instance hours.

### Dispatch

A run is dispatched as the [Spanner trial](../spanner/README.md#dispatch) is, with `firestore_trial` naming the kind and `application_digest` the published `firestore-recovery` GAR digest.
Any run ID fits, because the databases are `fs1879-<run-id>-n` and `fs1879-<run-id>-d`, at most 49 of the 63 characters a database ID takes.
The `tier3-firestore` namespace, its place in the shared preflight's inventory and the Operator's watch over it are in place ([#2083](https://github.com/flink-gcp/flink-connector-gcp/issues/2083)).

### Database lifecycle

After the supervisor heartbeats and the Operator is ready, the runner records the intent, which names both databases, reads that neither name is taken, and records the creation intent and a call marker in one control write.
It creates the Native-mode database with the Firestore Admin API's `databases.create`, Standard edition in `us-central1` without point-in-time recovery or delete protection, and reads it until it exists, rather than polling the create's long-running operation, which neither role may read.
It then creates the Datastore-mode database the same way under the same marker; a create that failed transiently, timed out or answered 409 is read back, and repeated while the database is absent, until the startup deadline.
A database reports no state while it is created, so existing is what admission waits for.

The runner then writes each seed, 108,000 records in 216 commits of 500 writes, the most one commit takes: through the Firestore API's `documents:commit` in Native mode and the Datastore API's non-transactional `commit`, which names the database, in Datastore mode.
Every Datastore API call also sends the `x-goog-request-params` routing header naming the database, without which the service refuses it with 400 "Missing routing header" ([#2130](https://github.com/flink-gcp/flink-connector-gcp/issues/2130)).
Each write sets fixed values without a precondition, so a commit that failed transiently, or aborted and was answered 409, is repeated until the startup deadline.
The receipt records each database's readback and the seed size the service accepted.
Neither the creation nor the 432 commits has been timed: the first pilot created both databases and wrote the Native-mode seed, but its evidence records no per-call time; the startup budget is the Spanner scenario's 600 seconds.
Only then does the runner open the namespace quota for three Pods and create the FlinkDeployment.
A database that already carries a run's name, and a Native-mode create the service refuses although the name read free, refuse the run and nothing is deleted; the marker stays until both databases have read back, because either create could still land behind cleanup until then.

Only the supervisor deletes the databases, and only ones whose creation intent the record holds.
It waits for the runner's release and for the namespace to hold no Pod, deletes each database that still reads back, and reads both again; a deletion runs on as a long-running operation, so cleanup's next pass reads until neither database is found.
A deletion refused with status 400 or 409, or a call that failed transiently, emits `firestore-delete-pending` and is left to that next pass.
Shared cleanup then removes the run's state under `runs/<run-id>/` and `.inprogress/flink-gcp-tier3-firestore/runs/<run-id>/` in the `flink-gcp-tier3-firestore` state bucket.

A run whose cleanup never confirms the deletion, or whose create kept its marker, gets no idle receipt and keeps the environment lock, and the completed-execution recovery of the [lifecycle runbook](../../lifecycle/README.md#cleanup-and-recovery) repeats the cleanup but never clears a marker.
Then an operator verifies the databases by hand and deletes them:

1. Read `firestore-delete-pending` or the receipt's reason in the run's evidence, so a refusal that names a cause is resolved first.
2. Run `gcloud firestore databases describe --database=fs1879-<run-id>-n --project=flink-gcp`, and the same for `fs1879-<run-id>-d`, and confirm that each is the run's: type `FIRESTORE_NATIVE` or `DATASTORE_MODE`, location `us-central1`, created within the run's window.
   For a kept marker that reads `NOT_FOUND`, the create's outcome is unknown; list `gcloud firestore databases list --project=flink-gcp` again at least ten minutes later, and delete one that appears.
3. Run `gcloud firestore databases delete --database=<database> --project=flink-gcp` for each and describe it again until it is `NOT_FOUND`.

The control record still holds the marker or the unconfirmed deletion, so releasing the lock afterwards is incident recovery outside the rig, as for a BigQuery table create.

### Exercise and oracles

The supervisor runs the shared recovery exercise on the application's `firestore-progress` lines, with the faults and the three measurement windows of the Spanner trial.
A reading is a `firestore-measurement` evidence event; the exercise finds the five vertices by the names [the graph](#graph-and-what-each-collection-records) chains them under, because three of them are sources.
Its families are each of the four component vertices' task, network and connector metrics and its subtasks' shares of the latest completed checkpoint, and each TaskManager's memory metrics.
The connector family is each mode's writer metrics, which the input sink and the batch copy both register, and for a copy also its reader's `documentsRead` or `entitiesRead`; the enumerators register on the JobManager, which no vertex reading reaches.
A lost TaskManager restarts the input's failover region and the two copy subtasks it held, which share no edge with any other, so the exercise accepts up to three restores for it.

Before the upgrade, once a query of one document or entity in a database's `batch_out` returns one, the supervisor writes the 16 late seed records, numbered 108,000 to 108,015, into that database's `seed` in one commit.
A copy exists only after the batch read planned its read time, so the late records are committed after that time.
The record keeps `late_seed`, each mode's commit time (in Datastore mode, whose non-transactional commit answers none, the latest update time of its writes) and record count, and the upgrade begins only once both are written; a call that fails transiently, or a commit that aborts and is answered 409, emits `firestore-late-seed-pending` and is repeated at the next poll.
The exercise also keeps the plan each batch read's enumerator logs on the JobManager, `Planned <n> split(s) for … of projects/flink-gcp/databases/<database> at read time <t> and parallelism <p>.`, its warning form for fewer splits than the parallelism, which a semicolon ends there, or its one-split form, for the batch report.

When the upgraded application reaches `FINISHED` with the whole input processed, the supervisor reads each database's `input`, `seed` and `batch_out` in key order, in pages of up to 2,000.
A Native-mode page resumes after the last document's name and a Datastore-mode page at the batch's end cursor.
A database's first page is read now and returns its read time, at which every later page of that database is read.
A transiently failing query is repeated every 5 seconds until the read window closes, 1,200 seconds after the application finishes or a minute before the cleanup start, whichever is first; any other failure ends the run without a verdict.
Each oracle reports counts and the first 20 keys of each kind of deviation, for each mode on its own:

- **Sink, `input`:** every input key holds its sequence and no other key is there.
  It reads only the final state: neither database has a change feed in this graph, so how often the sink executed a write is not observable, and the analysis says so instead of counting executions.
- **Batch read, `batch_out`:** `seed` holds the 108,000 seed records and the 16 late ones, each with its own value, every seed key was copied with one value, its own, and no foreign or late key was copied.
  It reports the replayed keys and extra copies, which only records emitted after a restored checkpoint produce and which are not failed, the copies of each phase, the plan's split count and read time, whether the late commit came after that read time, and the read's span: from the plan's read time, or the first copy without one, to the last copy's update time.
  The span is reported, not judged: a read that outlived the service's read-time bound fails its job, which ends the run as a failed application.

In Datastore mode a kind query also returns entities of the kind under an ancestor, so in `input` and `seed` only a root entity whose key has a name can match a record, and any other, like an entity keyed by a numeric ID, is foreign; a copy in `batch_out`, in either mode, is identified by its `k` and `delivery` fields, not by its key, and a copy without `k` is foreign.

### Verdict and analysis

The verdict and its recomputation are the [Spanner trial's](../spanner/README.md#verdict-and-analysis), over four oracles and without a watermark: the run is `usable` when both recoveries are proved, all four oracles pass and each of the three windows read every family.
The supervisor exports no documents, and the databases' deletion removes them, so `just tier3-analyze <dir>` reads the oracle reports from the record.
