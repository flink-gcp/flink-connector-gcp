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

The [GCP grants](../../../opentofu/flink-gcp/tier3-firestore.tf) and the [`firestore-recovery`](../../lifecycle/firestore.cue) delivery are in place; the namespace and Operator watch set follow in [#2083](https://github.com/flink-gcp/flink-connector-gcp/issues/2083), and the trial scenario in [#2084](https://github.com/flink-gcp/flink-connector-gcp/issues/2084).
