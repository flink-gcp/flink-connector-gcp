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
A change committed after the end is not relayed, so the rig chooses an end after the input's last commit and judges the relay oracle only when the input finished before it.

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
The trials' recoveries all complete while the input runs, because the shared exercise fails a run whose input finishes before its post-recovery window.

## Deployment definition

[`pkg/bigtable.#Application`](../../pkg/bigtable/application.cue) defines the job on Flink 2.2.1 / Java 17, and the [`bigtable-recovery`](../../lifecycle/bigtable.cue) lifecycle scenario renders its initial and upgrade phases from one input identity.
The delivery supplies the run ID, image digest, phase, parallelism and change-stream end, and optionally the record count and rate, 180,000 records at 100 per second unless given; the package refuses more than 30 minutes of input.
It names the instance `bt1316-<run-id>`, so the run ID has at most 26 characters, and passes no emulator endpoint or credentials file.

The deployment runs as the `tier3-bigtable/bigtable` KSA, which selects the `tier3-bigtable` GSA through Workload Identity; the [GCP grants](../../../opentofu/flink-gcp/tier3-bigtable.tf) limit it to the `bt1316-` instances.
Checkpoints, savepoints and Kubernetes HA data sit beneath `gs://flink-gcp-tier3-bigtable/runs/<run-id>/`, deleted one day after creation.
One JobManager on normal capacity and one or two single-slot Spot TaskManagers each request and limit 1 CPU, 2 GiB memory and 1 GiB ephemeral storage, as the other recovery applications do.
Savepoint upgrades disable last-state fallback, and the Operator reports the upgrade savepoint in the application's status, where the shared recovery exercise reads it.
The `tier3-bigtable` namespace, its KSA and RBAC are in the [bootstrap root](../../../opentofu/tier3-bootstrap/README.md#bigtable-recovery-foundation) ([#1860](https://github.com/flink-gcp/flink-connector-gcp/issues/1860)), and the [recovery trial](#recovery-trial) below creates the instance and judges the trial.

## Recovery trial

The `bigtable-recovery` scenario of the [lifecycle](../../lifecycle/README.md) runs this application once as a FlinkDeployment in `tier3-bigtable` against an instance it creates for the run, measures the job before and after each recovery, and judges each of the three components on its own table.
It is the trial set of [#1316](https://github.com/flink-gcp/flink-connector-gcp/issues/1316) and [#1317](https://github.com/flink-gcp/flink-connector-gcp/issues/1317), and [ADR-0165](../../../docs/adr/0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md#bigtable-recovery-trial) records its design ([#1854](https://github.com/flink-gcp/flink-connector-gcp/issues/1854)).
It follows the [Cloud Tasks recovery trial](../cloudtasks/README.md#recovery-trial), whose resource lifecycle, finite-input exercise and measurement windows it shares; this section names what differs.

### Trial and estimate

Every trial has the same input, 180,000 records at 100 records per second, which is 30 minutes of input in a 90-minute window, and runs a savepoint upgrade and then deletes a Pod.
Its kind names the fault that makes it a trial of its own:

| Kind | Upgrade | Pod deleted | Subtasks of every vertex but the input, initial then upgrade |
| --- | --- | --- | --- |
| `jm-replacement` | Keeps its sizes | The JobManager | 2, 2 |
| `tm-replacement` | Keeps its sizes | The TaskManager that reported the latest input | 2, 2 |
| `rescale-out` | Rescales the job | The JobManager | 1, 2 |
| `rescale-in` | Rescales the job | The JobManager | 2, 1 |

The change-stream end is 3,900 seconds after the window's start, so dispatch derives it from the window and the trial carries it.
The input begins within the 1,200 seconds the exercise allows and runs 1,800 seconds, and each recovery pauses it for up to the exercise's 600-second recovery budget.
In the usable trials the input finished 1,250 to 1,485 seconds before the end, but a trial whose input starts late and whose recoveries both run long can finish past it, and its relay is then unjudged rather than judged on a partial stream.
The job then finishes at the end, which leaves the table reads 540 seconds, up to a minute before the cleanup start; a job whose input finishes past the end finishes with the input and leaves them less.
A partition the connector is still reconciling at the end holds the job past it, as [the graph](#graph-and-what-each-table-records) describes, and a job held past the cleanup start ends the run without a verdict.
Render a trial's offline proposal, which carries the estimate the owner approves, from the repository root:

```sh
mise x cue uv -- uv run --locked --package flink-tier3 --no-dev flink-tier3 render \
  --scenario bigtable-recovery \
  --run-id example-1316 --nonce aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --started-at 2026-10-20T00:00:00Z --expires-at 2026-10-20T01:30:00Z \
  --active-seconds 5220 --revision "$(git rev-parse HEAD)" \
  --application-image us-central1-docker.pkg.dev/flink-gcp/flink-tier3/bigtable-recovery@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --bigtable-trial rescale-out > /tmp/bigtable-proposal.json
```

A proposal is not an approval: it carries `approved: false` and an empty `approval.json`, and rendering calls no cloud API.
The estimate is the same for every kind, about USD 3.36: the Pods for the window as the Cloud Tasks estimate prices them, two node-hours of the one-node instance at USD 0.65, since the window is billed as two hours, and a USD 1.00 reserve for storage, state, evidence and telemetry.

### Dispatch

The [run workflow](../../../.github/workflows/tier3-run.yaml) admits `bigtable-recovery` with two inputs beyond the common ones: `bigtable_trial`, the kind, which the dispatch refuses when it is left at `none`; and `application_digest`, the published `bigtable-recovery` GAR digest, verified live at dispatch and never pinned.
The owner approves the run's estimate before dispatching it; dispatching is the approval, as for the other scenarios.
The workflow job runs for up to 120 minutes, as for the other 90-minute trials.
The run ID has at most 26 characters, because the run's instance is `bt1316-<run-id>` and an instance ID has at most 33.

One dispatch can carry a campaign: `campaign` lists the kinds to run after the dispatched one, comma-separated, and `repeats`, 0 to 9, the attempts it may repeat.
The run ID then reads `<prefix>-<kind>-a<attempt>`, with `jm`, `tm`, `ro` or `ri` for the kind, which dispatch checks before anything runs.
Once an attempt is idle and has released the lock, its receipt decides the next one, which the workflow dispatches itself and which waits behind the current run in the workflow's concurrency group:

- **`usable`**: the next kind, at attempt 1.
- **A stop the platform caused, while repeats remain**: the same kind at the next attempt. A stop before the application existed counts as one, because no connector code ran, whether a system Pod preempted the supervisor or a service call never answered. So does "Unplanned workload interruption", which the exercise raises only for a workload Pod the scheduler preempted, a node drain or upgrade evicted, or whose node shut down or was lost.
- **Anything else**: the campaign ends for the owner to read. This covers "Unplanned workload replacement", since a TaskManager whose JVM died is replaced too, "Unplanned workload eviction", which may follow the Pod's own storage limit, "Unplanned container restart", a failed oracle, and a run that did not reach idle.

The owner approves the campaign's estimate once, for its kinds and its repeats, by dispatching it.
A cause that recurs before the application exists, such as a refused grant, spends the repeats, which bound what it costs.
Three things end a campaign early, each before its next attempt holds anything: a CI plan or apply that takes the environment lock between attempts, another dispatch of the run workflow while a campaign runs, which GitHub's one pending run in the concurrency group replaces or is replaced by, and a run ID that would exceed 26 characters or already has evidence.
Each attempt checks out `main` as it is at its own dispatch unless `rig_sha` names a commit, so merge no rig change while a campaign runs.
For example, `bigtable_trial=tm-replacement`, `run_id=bt1855-tm-a4`, `campaign=rescale-out,rescale-in` and `repeats=2` run the three remaining trials from one dispatch, for at most five attempts.

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

The supervisor runs the shared recovery exercise on the application's `bigtable-progress` lines, with the faults and the three measurement windows of the Cloud Tasks trial.
A reading is a `bigtable-measurement` evidence event; the exercise finds the four vertices by the names [the graph](#graph-and-what-each-table-records) chains them under, because two of them are sources.
Its families are each component vertex's task, network and connector metrics and its subtasks' shares of the latest completed checkpoint, and each TaskManager's memory metrics, which describe the job, since the components share TaskManagers.
The source's enumerator metrics register on the JobManager, which no vertex reading reaches.

When the upgraded application reaches `FINISHED`, after the change stream's end, with the whole input processed, the supervisor reads `eo`, `alo` and `relay` in pages of up to 5,000 rows, each resuming after the last row key as the service sent it, until a page comes back empty.
A transiently failing read, including a stream that ends with an error after it began, is repeated every 5 seconds until the read window closes, 600 seconds after the application finishes or a minute before the cleanup start, whichever is first; any other failure ends the run without a verdict.
Each oracle reports counts and the first 20 row keys of each kind of deviation:

- **`eo`:** every input row is present, its `agg:count` SUM is 1, it has exactly one marker, and no row or family lies outside the input.
- **`alo`:** every input row is present with a SUM of at least 1; contributions beyond one are counted and reported, not failed.
- **`relay`:** every row `eo` applied was relayed at least once and no row lies outside the input; deliveries beyond one are counted and reported, not failed, and a relayed row that `eo` did not apply fails it.

The relay is judged only when the input's last progress line came at least 120 seconds before the change stream's end, the time its staged commit can take, because a change committed after the end is not relayed.

### Verdict and analysis

The run is `usable` when both recoveries are proved, all three oracles pass, the relay's having been judged, and each of the three windows read every family; anything short of that is `inconclusive`.
The exercise decides the verdict with the transition that completes the run, and the final receipt's `success` is that verdict.

`just tier3-analyze <dir>` recomputes the verdict from a downloaded evidence directory, with the coverage rebuilt from the exported readings and the recoveries' timing, as the Cloud Tasks section does.
It reads the oracle reports from the record rather than rebuilding them: the supervisor exports no rows, and the instance's deletion removes the tables.

Before the first trial, the [preregistration](https://github.com/flink-gcp/flink-connector-gcp/issues/1855) measures what the grants' [ADR section](../../../docs/adr/0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md#bigtable-recovery-foundation) leaves unmeasured: that a create is checked against the project, and that the instance conditions apply on the data plane, which the oracles' reads depend on.
