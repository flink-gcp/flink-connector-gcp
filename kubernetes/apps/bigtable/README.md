# Bigtable Tier-3 recovery application

This internal Flink 2.2.1 / Java 17 application is the workload of the Bigtable recovery trials that [issue #1316](https://github.com/flink-gcp/flink-connector-gcp/issues/1316) and [issue #1317](https://github.com/flink-gcp/flink-connector-gcp/issues/1317) share, as their [scope](https://github.com/flink-gcp/flink-connector-gcp/issues/1316#issuecomment-6247061185) decided.
One job graph carries all three components the trials recover.
A finite, checkpointed input reaches the production `BigtableSink` in both delivery modes, and the production `BigtableChangeStreamSource` relays what the `EXACTLY_ONCE` sink applied.
Every connector uses the native transport and the workload's application default credentials; no emulator endpoint can be configured.

## Build and local validation

```sh
mise x -- just tier3-bigtable-verify
```

The opt-in `tier3-bigtable` Maven profile keeps the application outside published connector artifacts.
The build produces `target/bigtable-recovery.jar` and copies runtime dependencies into `target/image-lib/`.
The Dockerfile adds these files to `/opt/flink/usrlib/` over the reviewed Flink image and enables its bundled GCS filesystem plugin.
The entry point is `io.github.flink.gcp.connector.tier3.bigtable.BigtableRecoveryJob`.
The [image publication workflow](../../../.github/workflows/tier3-images.yaml) packages it as `bigtable-recovery` on the fixed Flink 2.2.1 / Java 17 AMD64 base, and the application CI lane builds the Dockerfile without publishing.

Unit tests cover argument and input bounds, the row keys and contributions both sinks write, the relay's deserializer and delivery cells, and refusal of state from another trial.
A MiniCluster test runs the graph with in-memory connectors through savepoint upgrades at parallelism 2→2, 2→1 and 1→2, and requires every record at each connector afterwards.
Another builds the graph with the production connectors and pins its operator UIDs, vertex names, parallelisms and max parallelism.
A third pins each production connector through its public surface: sink class and mode, app profile, marker family, the mutation its serializer writes, its table, and the source's boundedness; the source's start position is not visible outside the connector's package.

The real connectors do not run locally.
The staged sink reads its app profile through the instance admin API, which the Bigtable emulator does not serve, and the change-stream source has no emulator endpoint.
The connector module's own tests cover most of their behavior, including a staged `AddToCell` commit against the service.
None of its change-stream tests reads an aggregate `AddToCell` change from the service, so the deployed trials are the first run of that path, as of the whole graph.

## Instance layout

The rig's runner creates one instance per run in project `flink-gcp`, named `bt1316-` followed by a run label, at most 33 characters, and passes it as `--instance`.
The application expects these resources in it:

| Resource | Contents |
| --- | --- |
| App profile `recovery` | Single-cluster routing with single-row transactions allowed. The staged sink requires it, and the change-stream source reads its partitions under it. |
| Table `eo` | Family `agg` of type INT64 SUM, family `flink_commit` raw without a GC rule (the staged sink's markers), and a change stream with 24 hours' retention. Initial splits at `4`, `8` and `c` give the source at least four partitions, as the gated Change Streams test measures. |
| Table `alo` | Family `agg` of type INT64 SUM. |
| Table `relay` | Family `r`. |

## Graph and what each table records

Record `n` of `--records` has the row key `<n mod 16 in hex>/<n, seven digits>`, so consecutive records spread over every initial partition of `eo`.
The input runs at parallelism one; every other vertex runs at `--parallelism`, one or two.

- **`EXACTLY_ONCE` sink into `eo`:** each record adds 1 to `agg:count` at timestamp 1000 µs, and the staged commit writes the contribution and one marker in a single conditional write. Each row's SUM is therefore 1 and it has exactly one marker.
- **`AT_LEAST_ONCE` sink into `alo`:** the same contribution without staging. A replay after a restore adds to the SUM again, and so does a client retry of an entry whose first attempt was applied, so a SUM above 1 counts extra contributions without naming their cause.
- **Change-stream relay from `eo` into `relay`:** for every change that adds to `agg`, the relay writes the row key with one cell in family `r`. Its qualifier is `<phase>/<uuid>/<n>`: the phase, a UUID drawn when the task instance opens, and the delivery's position in that instance. Its value is the change's commit time in microseconds. A change the source delivers again, whether after a restore or within one instance (as when the connector restarts a partition that lost its continuation token), therefore adds a qualifier, and the number of qualifiers in a relay row counts its deliveries. A change without a contribution, such as one holding only markers, is skipped; a row outside the input is relayed for the oracle to report.

On a fresh start the source reads from the instant its enumerator initializes, and it stops at `--change-stream-end`, so the job finishes once the input is complete and the source reaches its end.
Starting there loses nothing: the enumerator refuses to snapshot before it has initialized, so no checkpoint completes, and the staged sink commits nothing, before the start.
The start is read from the JobManager's clock, while commit times and the service's check come from the service's clock.
The service refuses a start later than its current time, and a JobManager clock running ahead would also place the start after commits made just after the first checkpoint, so both hold only while the JobManager's clock does not lead the service's by more than the time from initialization to the first commit, which no setting bounds from below.
The GKE nodes keep Google's time synchronization, which keeps that lead far below a checkpoint's duration, but it is an assumption of the trial rather than a guarantee.
A restore resumes from the checkpointed positions instead, so the start is chosen once per run without an argument the rig would have to render before the table exists.
The source signals its end only when no partition is missing, so a partition the connector is still reconciling holds the job past the end.
The connector recovers a gap 2 minutes after it first observes it when pending merges' continuation tokens cover it and 20 minutes after when they do not, and then replays it; neither grace is measured from the end, so neither bounds when the job finishes.
A restored partition whose position has fallen outside the table's retention fails the job rather than skipping ahead; the connector's restart of a partition that lost its token is instead moved forward to a minute inside the retention, which a position from a trial of a few hours never needs.
The service treats the end as inclusive, in whole microseconds, so the service's read includes a change committed at the end and none committed after it.
The rig therefore fixes an end it expects the input's last commit to precede, and judges the relay oracle only when the input finished in time, as the [fault sequence](#fault-sequence) describes.

Flink chains the graph into four vertices, whose names the rig's vertex selection reads:

| Vertex | Parallelism |
| --- | --- |
| `Source: Bigtable input -> Bigtable input identity` | 1 |
| `Bigtable EXACTLY_ONCE: Writer -> Bigtable EXACTLY_ONCE: Committer` | `--parallelism` |
| `Bigtable AT_LEAST_ONCE: Writer` | `--parallelism` |
| `Source: Bigtable change stream -> Bigtable relay delivery -> Bigtable relay: Writer` | `--parallelism` |

Progress lines use the `bigtable-progress` event in the shape the shared recovery exercise parses; snapshot and checkpoint-complete lines use the `bigtable-snapshot` and `bigtable-checkpoint-complete` events.

## Arguments

| Argument | Meaning |
| --- | --- |
| `--run-id` | The Tier-3 run label. |
| `--instance` | The run's instance ID, `bt1316-` followed by a label. |
| `--records` | Records in the input, 2 to 360,000. |
| `--records-per-second` | Offered input rate, 1 to 100. The staged committers apply each checkpoint's input in one burst. |
| `--change-stream-end` | An ISO-8601 instant at which the source stops. |
| `--phase` | `initial` or `upgrade`. |
| `--require-restored` | `true` refuses a fresh start; `upgrade` requires it. |
| `--parallelism` | Subtasks of every vertex but the input, 1 or 2 (default 2). |

The run, instance, input and change-stream end form the trial identity that the restored input state must match; the phase and parallelism may change in an upgrade.
The check runs only while the input is active: once the input has finished, Flink restores its vertex as finished without initializing its state, so a later restore is not checked.
The faults all land while the input runs, because the fault sequence ends a run without a verdict when its input finishes before the job runs again after the last fault.

## Deployment definition

[`pkg/bigtable.#Application`](../../pkg/bigtable/application.cue) defines the job on Flink 2.2.1 / Java 17, and the [`bigtable-recovery`](../../lifecycle/bigtable.cue) lifecycle scenario renders its initial and upgrade phases from one input identity.
The delivery supplies the run ID, image digest, phase, parallelism and change-stream end, and optionally the record count and rate, unless given the fault sequence's 2,400 records at 10 per second; the package refuses more than 30 minutes of input.
It names the instance `bt1316-<run-id>`, so the run ID has at most 26 characters, and passes no emulator endpoint or credentials file.

The deployment runs as the `tier3-bigtable/bigtable` KSA, which selects the `tier3-bigtable` GSA through Workload Identity; the [GCP grants](../../../opentofu/flink-gcp/tier3-bigtable.tf) limit it to the `bt1316-` instances.
Checkpoints, savepoints and Kubernetes HA data sit beneath `gs://flink-gcp-tier3-bigtable/runs/<run-id>/`, deleted one day after creation, and two consecutive failed checkpoints are tolerated before the job restarts.
One JobManager on normal capacity and one or two single-slot Spot TaskManagers each request and limit 1 CPU, 2 GiB memory and 1 GiB ephemeral storage, as the other recovery applications do.
Savepoint upgrades disable last-state fallback, and the Operator reports the upgrade savepoint in the application's status.
The `tier3-bigtable` namespace, its KSA and RBAC are in the [bootstrap root](../../../opentofu/tier3-bootstrap/README.md#bigtable-recovery-foundation) ([#1860](https://github.com/flink-gcp/flink-connector-gcp/issues/1860)), and the [recovery trial](#recovery-trial) below creates the instance and judges the trial.

## Recovery trial

The `bigtable-recovery` scenario of the [lifecycle](../../lifecycle/README.md) runs this application once as a FlinkDeployment in `tier3-bigtable` against an instance it creates for the run, injects every fault kind in one run, and judges each of the three components on its own table.
It is the trial set of [#1316](https://github.com/flink-gcp/flink-connector-gcp/issues/1316) and [#1317](https://github.com/flink-gcp/flink-connector-gcp/issues/1317), and [ADR-0165](../../../docs/adr/0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md#bigtable-recovery-trial) records its design ([#1854](https://github.com/flink-gcp/flink-connector-gcp/issues/1854)).
It follows the [Cloud Tasks recovery trial](../cloudtasks/README.md#recovery-trial), whose resource lifecycle and finite-input exercise it shares; this section names what differs.

### Fault sequence

The `sequence` kind runs the trial under [ADR-0204](../../../docs/adr/0204-tier3-recovery-trials-tolerate-faults-and-fail-only-on-lost-data.md) ([#2153](https://github.com/flink-gcp/flink-connector-gcp/issues/2153)), and it is the only kind dispatched; the single-fault kinds ran under ADR-0165.
Once a completed checkpoint follows the initial input, one run injects the savepoint upgrade, the JobManager's deletion, the deletion of the TaskManager reporting the input, and a rescale from two subtasks to one and back; the rescales are the approved upgrade with only its parallelism and its `--parallelism` argument changed.
Before each fault, and before the end, the supervisor waits until the job runs again; it records the faults and everything it waits out, and stops a run, as the [Firestore fault sequence](../firestore/README.md#fault-sequence) does, and the job's restart strategy is unbounded.

The input is 2,400 records at 10 records per second, four minutes of input.
The job consumes little of it before it runs again after the last fault, because the input pauses while the job is down and a deleted JobManager or TaskManager restores from the last checkpoint: the first [Spanner](../spanner/README.md#fault-sequence) and [Firestore](../firestore/README.md#fault-sequence) `sequence` runs ran again after their last fault with 254 and 494 seconds of input still to come, as their findings' timelines record, so each had consumed about 106 seconds of it, and four minutes outlasts the five faults.
The change stream ends 1,800 seconds after the window's start, and the job cannot finish before it.
The input is short because it must finish well before that end.
The exported evidence of [#1855](https://github.com/flink-gcp/flink-connector-gcp/issues/1855)'s four usable trials shows their input beginning 505 to 722 seconds into the window, and the five faults made the Spanner and Firestore inputs finish 245 to 263 seconds later than their length, which would leave about eight to eleven minutes for interruptions beyond the relay's 120-second commit margin.
A run should reach its verdict about 31 minutes into its window.
The first `sequence` run, `bt2153-seq-a1`, began its input 516 seconds into the window, finished it 292 seconds later than its length with 632 seconds to spare beyond the commit margin, and reached `usable` 31.3 minutes into its window, as its [findings](../../../docs/adr/evidence/0204-bigtable-fault-sequence-findings-2153.md) record.
A partition still being reconciled holds the job past the end, and nothing bounds how long, as [the graph](#graph-and-what-each-table-records) describes; a hold that outlasts the run's hour stops it at its time limit.

When the job has finished after the last fault, with its whole input processed, the three oracles below read the tables, and `usable` means all three pass; `eo` is judged exactly once.
The Change Streams relay reads `eo`, so the exactly-once path runs in this scenario rather than in [#2148](https://github.com/flink-gcp/flink-connector-gcp/issues/2148)'s.
The relay is judged only when the input's latest progress line, the one that showed it whole, came at least 120 seconds before the change stream's end; a restore after the input finished can replay it and commit rows whose staged commit it discarded only then, so the latest line counts, not the first.
A run whose input finished later leaves the relay unjudged and is not `usable`, and it can run again as it is.
A restore after the input's latest progress line also leaves the relay unjudged: it can commit rows its state carried without logging a line, and its recorded time, when the JobManager assigned the state, precedes those commits by as long as the tasks take to deploy; such a run can run again as it is too.
Timing explains only a missing row, so an unjudged relay that counts a foreign row, a cell outside its family, a row `eo` did not apply, a delivery that holds no commit time or a commit after the change stream's end has not passed, and the run does not run again as it is ([#2279](https://github.com/flink-gcp/flink-connector-gcp/issues/2279), [#2321](https://github.com/flink-gcp/flink-connector-gcp/issues/2321)).
The run completes only on a poll whose checkpoint statistics answered, so that the latest restore is read.
`just tier3-analyze` decides a `sequence` run by its oracles and lists its faults.

### Trial and proposal

The trial's kind is `sequence`, with the input and change-stream end of the [fault sequence](#fault-sequence), and both phases run two subtasks of every vertex but the input.
Dispatch derives the change-stream end from the window, and the trial carries it.
Render a trial's offline proposal from the repository root:

```sh
mise x cue uv -- uv run --locked --package flink-tier3 --no-dev flink-tier3 render \
  --scenario bigtable-recovery \
  --run-id example-1316 --nonce aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --started-at 2026-10-20T00:00:00Z --expires-at 2026-10-20T01:30:00Z \
  --active-seconds 5220 --revision "$(git rev-parse HEAD)" \
  --application-image us-central1-docker.pkg.dev/flink-gcp/flink-tier3/bigtable-recovery@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --bigtable-trial sequence > /tmp/bigtable-proposal.json
```

A proposal is not an approval: it carries `approved: false` and an empty `approval.json`, and rendering calls no cloud API.

### Dispatch

The [run workflow](../../../.github/workflows/tier3-run.yaml) admits `bigtable-recovery` with two inputs beyond the common ones: `bigtable_trial`, the kind, which the dispatch refuses when it is left at `none`; and `application_digest`, the published `bigtable-recovery` GAR digest, verified live at dispatch and never pinned.
Dispatching is the approval, as for the other scenarios, and no estimate is approved first, since ADR-0204 bounds the run by its time limit.
The workflow job runs for up to 120 minutes, as for the other 90-minute trials.
The run ID has at most 26 characters, because the run's instance is `bt1316-<run-id>` and an instance ID has at most 33.

The [`bigtable-recovery`](../../lifecycle/bigtable.cue) delivery takes the input that the [deployment definition](#deployment-definition) describes from the optional `bigtable_records` and `bigtable_records_per_second` tags, the change-stream end from the required `bigtable_change_stream_end` tag, a UTC instant at which the Change Streams source stops, and sizes every vertex but the input and the TaskManagers at the parallelism in [parallelism.cue](../../lifecycle/parallelism.cue).
Dispatch renders it at the sequence's fixed input and a change-stream end 1,800 seconds into the run's window.

### Instance lifecycle

After the supervisor heartbeats and the Operator is ready, the runner records the instance's intent, reads that the name is free, and records the creation intent and a call marker in one control write.
It then creates the instance with one SSD node in the cluster `recovery` in `us-central1-b`, reads it until it is `READY`, creates `eo`, `alo`, `relay` and the `recovery` app profile, and reads all four back.
It polls the instance rather than the create's long-running operation, so admission needs no permission on operations.
A table or app-profile create that failed transiently, timed out or answered 409 is read back, and repeated while the resource is absent, until the startup deadline: the service goes on creating after a request that timed out, and a table with initial splits and a change stream can outlast the 20-second transport timeout.
The readback compares each table's family names and `eo`'s change stream, and the app profile's single-cluster routing with single-row transactions; only then does the runner open the namespace quota for three Pods and create the FlinkDeployment.
An instance that already carries the name refuses the run, which then deletes nothing, and so does a create the service refuses although the name read free.
The create answers with a long-running operation, so the marker stays until the instance first reads back, in any state: until then the operation could still create an instance after cleanup found none.
From then on a failure creating its tables or reading them back clears the marker and keeps the creation intent, so cleanup deletes the instance; a create that did not return, or whose instance never read back before admission stopped, keeps its marker, and then neither the release nor cleanup proceeds, as for a Cloud Tasks queue.

Only the supervisor deletes the instance, and only one whose creation intent the record holds.
It waits for the runner's release and for the namespace to hold no Pod, disables `eo`'s change stream, deletes the instance and reads it back until it is gone.
The service refuses to delete an instance whose table has a change stream, and disabling one is itself an operation, so a deletion or disable refused with status 400 or 409, or a call that failed transiently, is left to cleanup's next pass, which reads the change stream again before disabling it.
Shared cleanup then removes the run's state under `runs/<run-id>/` and `.inprogress/flink-gcp-tier3-bigtable/runs/<run-id>/` in the `flink-gcp-tier3-bigtable` state bucket.

### Exercise and oracles

The supervisor runs the shared [fault sequence](#fault-sequence) on the application's `bigtable-progress` lines.
When the application has finished after the last fault and after the change stream's end, with the whole input processed, the supervisor reads `eo`, `alo` and `relay` in pages of up to 5,000 rows, each resuming after the last row key as the service sent it, until a page comes back empty.
A transiently failing read, including a stream that ends with an error after it began, is repeated every 5 seconds until the read window closes, 600 seconds after the application finishes or a minute before the run's one-hour limit, whichever is first; any other failure ends the run without a verdict.
Each oracle reports counts and the first 20 row keys of each kind of deviation:

- **`eo`:** every input row is present, its `agg:count` SUM is 1, it has exactly one marker, and no row or family lies outside the input.
- **`alo`:** every input row is present with a SUM of at least 1; contributions beyond one are counted and reported, not failed.
- **`relay`:** every row `eo` applied was relayed at least once, no row lies outside the input, and every delivery of an input row holds a commit time, none after the change stream's end; deliveries beyond one are counted and reported, not failed, and a relayed row that `eo` did not apply fails it.
  A delivery holds a commit time when its value is the decimal number of microseconds the application writes; a row with a delivery of any other value is counted as `wrong_value`, and a foreign row or a cell outside family `r` is reported as such, without its value being interpreted as a commit time.

The relay is judged only when the input's latest progress line came at least 120 seconds before the change stream's end and no restore followed that line, as the [fault sequence](#fault-sequence) describes.

### Verdict and analysis

The run is `usable` when it completed and all three oracles passed, the relay's having been judged; anything short of that is `inconclusive`, with `recovery-incomplete`, `relay-oracle-unjudged` or `<component>-oracle-not-passed`, for `eo`, `alo` or `relay`, as its reasons; an unjudged relay reads as `relay-oracle-unjudged` unless it counts a foreign row, a cell outside its family, a row `eo` did not apply, a delivery that holds no commit time or a commit after the change stream's end, which read as `relay-oracle-not-passed`.
The exercise decides the verdict with the transition that completes the run, and the final receipt's `success` needs that verdict to be `usable` and the run's settlement to succeed, so a failed evidence write or state cleanup makes it false beside a `usable` verdict; a run whose cleanup is not confirmed writes no receipt and keeps the environment lock.

`just tier3-analyze <dir>` recomputes the verdict from a downloaded evidence directory and lists the faults the run injected, and it reports a single-fault run as the [Cloud Tasks section](../cloudtasks/README.md#verdict-and-analysis) does, without a verdict and as `single-fault-trial`.
It reads the oracle reports from the record rather than rebuilding them: the supervisor exports no rows, and the instance's deletion removes the tables.
A record written before [#2279](https://github.com/flink-gcp/flink-connector-gcp/issues/2279) stored `relay-oracle-unjudged` for an unjudged relay that also counted a definitive failure, so it now reads as `relay-oracle-not-passed` with `verdict-differs-from-the-record`.
A record written before [#2321](https://github.com/flink-gcp/flink-connector-gcp/issues/2321) holds no `wrong_value` or `after_end` count, which the analyzer cannot add, so its verdict does not cover a delivery's commit time.

The four trials of [#1855](https://github.com/flink-gcp/flink-connector-gcp/issues/1855) are preregistered in the [Bigtable recovery trial preregistration](../../../docs/adr/evidence/0165-bigtable-recovery-preregistration-1855.md), and each kind reached `usable`, as their [findings](../../../docs/adr/evidence/0165-bigtable-recovery-findings-1855.md) record.
Their pilot showed the unconditional creator role enough for the runner to create its `bt1316-` instance, which does not test whether IAM checks a create against the project, one of the premises the grants' [ADR section](../../../docs/adr/0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md#bigtable-recovery-foundation) left unmeasured; it did measure the other, that the instance-name conditions admit the workload's and the supervisor's data-plane calls, which the oracles' reads depend on.
