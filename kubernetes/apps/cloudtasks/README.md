# Cloud Tasks Tier-3 recovery application

This internal Flink 2.2.1 / Java 17 application is the workload of the Cloud Tasks recovery trial, which [issue #1362](https://github.com/flink-gcp/flink-connector-gcp/issues/1362) introduced.
`CloudTasksRecoveryJob` writes a finite, checkpointed input through the production `CloudTasksSink` in `EXACTLY_ONCE` (staged) mode to one paused queue in `flink-gcp/us-central1`.
The JAR's manifest names it, and the image installs the JAR as `/opt/flink/usrlib/cloudtasks-recovery.jar`.
The application creates no queue and changes, resumes or deletes none: the lifecycle runner creates the run's queue paused before the job starts and deletes it at cleanup.

## Build and local validation

```sh
mise x -- just tier3-cloudtasks-verify
```

The opt-in `tier3-cloudtasks` Maven profile keeps the application outside published connector artifacts.
The build produces `target/cloudtasks-recovery.jar` and copies runtime dependencies into `target/image-lib/`.
The [Dockerfile](Dockerfile) adds these files to `/opt/flink/usrlib/` over the reviewed Flink 2.2.1 image and enables its bundled GCS filesystem plugin.
The [image publication workflow](../../../.github/workflows/tier3-images.yaml) packages it as `cloudtasks-recovery`, and the application CI lane builds the Dockerfile without publishing.

`RecoveryJobITCase` runs the deployed graph and the production sink against `FakeCloudTasks`, an in-process `CreateTask` server that refuses a repeated name with `ALREADY_EXISTS`.
A run, and a run stopped with a savepoint and resumed from it in the `upgrade` phase at the same sink parallelism, from two subtasks to one and from one to two, each end with exactly the expected task names.
The fake has no queue to read, and the connector skips the retention readback for an emulator endpoint; what the real service does is the trial's question.

## Task names

The task key of sequence `s` is `<run-id>/<s>`, and the sink names the task with the key's SHA-256 digest in lowercase hex: `projects/flink-gcp/locations/us-central1/queues/<queue>/tasks/<digest>`.
A listed task name therefore maps back to one sequence through the digests of the run's keys, and a name outside that table did not come from this input.
A record replayed after a recovery addresses the same name, and the connector counts the service's `ALREADY_EXISTS` for it as success.

## Graph and sink

One source subtask generates the sequences 0 to `records - 1` at the ceiling with Flink Datagen.
An identity operator, also one subtask, checks that each sequence is the next one and keeps the trial identity, a lineage UUID and its position in operator state.
A restored state must carry the same run, queue, record count and rate, so state from another trial or configuration fails the job; `--require-restored true` refuses a fresh start.
The sink's parallelism is not part of that identity, so a savepoint taken at one parallelism restores at the other.
Sequence `s` goes to sink subtask `s mod p` at sink parallelism `p`, and the sink keeps maximum parallelism 128 at either.
The job is two vertices, the source chained to the identity operator and the sink's writer chained to its committer, which is what the rig's measurements read; `RecoveryJobITCase` holds that shape.
The source, identity and sink UIDs are `ct1362-source-v1`, `ct1362-input-v1` and `ct1362-sink-v1`.
Checkpoints run every 30 seconds, one at a time, and two consecutive failed checkpoints are tolerated before the job restarts.

The sink is built with the public builder: a fixed `queue(...)`, `deliveryGuarantee(EXACTLY_ONCE)`, and the default staged and writer options.
Under those defaults the committer reads the queue's `tombstoneTtl` back before it creates a task and refuses a queue that keeps names for less than an hour, so the workload identity needs `cloudtasks.queues.get` beside `cloudtasks.tasks.create`.
Each task is an HTTP POST to `https://ct1362.invalid/task` whose body repeats its task key; RFC 6761 reserves `.invalid`, and a paused queue dispatches nothing.
The committers create each checkpoint's tasks in one burst rather than at the source rate.
At one subtask, a 30-second checkpoint's 3,000 creates would take its committer about 14 seconds at the per-subtask rate below, inside the interval.
The ceiling is 100 records per second because [#1246](https://github.com/flink-gcp/flink-connector-gcp/issues/1246) measured staged creation keeping up at that rate on four subtasks with 10-second checkpoints. A 30-second checkpoint of it is 3,000 creates, which two committers on the default single channel, measured at about 210 creates per second per subtask in [#937](https://github.com/flink-gcp/flink-connector-gcp/issues/937), send in seconds and below the 500 per second at which Google's 500/50/5 guidance starts a new queue.

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

## Arguments

Arguments use `--name value` pairs; missing values, duplicate options and unknown options are rejected.

| Argument | Accepted value |
| --- | --- |
| `--run-id` | The Tier-3 run label: 1–40 lowercase letters, digits and inner hyphens |
| `--queue` | The queue ID: `ct1362-` followed by a run label |
| `--records` | Total input records, from 2, so both sink subtasks receive input, to 360,000, one hour at the highest rate |
| `--records-per-second` | Source ceiling from 1 to 100 |
| `--phase` | `initial` (default) or `upgrade` |
| `--require-restored` | `false` (default) or `true`; `--phase upgrade` requires `true` |
| `--sink-parallelism` | `2` (default) or `1`: the sink's writer and committer subtasks; an upgrade may change it, which rescales the sink |

## Recovery trial

The `cloudtasks-recovery` scenario of the [lifecycle](../../lifecycle/README.md) runs this application once as a FlinkDeployment in `tier3-cloudtasks`, measures it before and after each recovery, and judges whether every input record became exactly one task.
It is the trial of [#1362](https://github.com/flink-gcp/flink-connector-gcp/issues/1362), and [ADR-0165](../../../docs/adr/0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md#cloud-tasks-recovery-trial) records its design; [#1777](https://github.com/flink-gcp/flink-connector-gcp/issues/1777) added the fault kinds and the measurements, as its [section](../../../docs/adr/0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md#cloud-tasks-fault-kinds-and-measurement-windows) records.
Its grants and its CUE delivery are described in the [OpenTofu runbook](../../../opentofu/README.md#cloud-tasks-cluster-resources) and the [lifecycle runbook](../../lifecycle/README.md).
The one [preregistered](https://github.com/flink-gcp/flink-connector-gcp/issues/1362#issuecomment-6127161854) trial reached `usable` on its first attempt, `ct-1362-a1`, on 2026-10-12, as its [findings](../../../docs/adr/evidence/0165-cloudtasks-recovery-findings-1362.md) record.
The `tm-replacement`, `rescale-out` and `rescale-in` trials of [#1778](https://github.com/flink-gcp/flink-connector-gcp/issues/1778) are preregistered in the [fault trial preregistration](../../../docs/adr/evidence/0165-cloudtasks-fault-trials-preregistration-1778.md), and ran the image of the [Cloud Tasks fault trial publication](../../../docs/adr/evidence/0165-tier3-image-publications.md#cloud-tasks-fault-trial-publication) and, once [#1794](https://github.com/flink-gcp/flink-connector-gcp/issues/1794) made `INTERNAL` transient, that of the [Cloud Tasks INTERNAL retry publication](../../../docs/adr/evidence/0165-tier3-image-publications.md#cloud-tasks-internal-retry-publication).
The TaskManager replacement and the rescale in each reached `usable`, and the rescale out reached no verdict in six attempts and is rerun under [#1799](https://github.com/flink-gcp/flink-connector-gcp/issues/1799), as their [findings](../../../docs/adr/evidence/0165-cloudtasks-fault-trials-findings-1778.md) record.
The rerun, [preregistered](../../../docs/adr/evidence/0165-cloudtasks-rescale-out-preregistration-1799.md) with the image of the [Cloud Tasks CANCELLED retry publication](../../../docs/adr/evidence/0165-tier3-image-publications.md#cloud-tasks-cancelled-retry-publication), reached `usable` on its second counted attempt, as its [findings](../../../docs/adr/evidence/0165-cloudtasks-rescale-out-findings-1799.md) record.

### Trial and estimate

Every trial has the same input, 180,000 records at 100 records per second, which is 30 minutes of input in a 90-minute window, and runs a savepoint upgrade and then deletes a Pod.
Its kind names the fault that makes it a trial of its own:

| Kind | Upgrade | Pod deleted | Sink subtasks, initial then upgrade |
| --- | --- | --- | --- |
| `jm-replacement` | Keeps its sizes | The JobManager | 2, 2 |
| `tm-replacement` | Keeps its sizes | The TaskManager that reported the latest input | 2, 2 |
| `rescale-out` | Rescales the sink | The JobManager | 1, 2 |
| `rescale-in` | Rescales the sink | The JobManager | 2, 1 |

Each sink subtask runs on a single-slot TaskManager of its own, so a phase with one subtask runs one TaskManager.
`jm-replacement` is the trial `ct-1362-a1` ran.
Render a trial's offline proposal, which carries the estimate the owner approves, from the repository root:

```sh
mise x cue uv -- uv run --locked --package flink-tier3 --no-dev flink-tier3 render \
  --scenario cloudtasks-recovery \
  --run-id example-1362 --nonce aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --started-at 2026-10-13T00:00:00Z --expires-at 2026-10-13T01:30:00Z \
  --active-seconds 5220 --revision "$(git rev-parse HEAD)" \
  --application-image us-central1-docker.pkg.dev/flink-gcp/flink-tier3/cloudtasks-recovery@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --cloudtasks-trial rescale-in > /tmp/cloudtasks-proposal.json
```

A proposal is not an approval: it carries `approved: false` and an empty `approval.json`, and rendering calls no cloud API.

The estimate is the same for every kind.
It prices the operator, the supervisor and the three application Pods for the window, which a rescale trial's phase with one TaskManager does not use in full, at the reviewed Autopilot rates, every task operation the trial plans at USD 0.40 per million without the monthly free tier, and a USD 1.00 reserve for state, evidence and telemetry.
The operations are the 180,000 creates, one checkpoint's records re-created for each of the two recoveries, two whole listings in pages of 1,000, and ten queue calls.

### Dispatch

The [run workflow](../../../.github/workflows/tier3-run.yaml) admits `cloudtasks-recovery` with two inputs beyond the common ones: `cloudtasks_trial`, the kind, which the dispatch refuses when it is left at `none`; and `application_digest`, the published `cloudtasks-recovery` GAR digest, verified live at dispatch and never pinned.
The owner approves the run's estimate before dispatching it; dispatching is the approval, as for the other scenarios.
The workflow job runs for up to 120 minutes, because the 90-minute window's serial budget is 114 minutes.

Dispatch checks the trial kind, the digest, the run ID and an existing run's evidence first, then snapshots the foundation, renders and verifies the proposal, takes live image receipts and binds the approval into the delivery before it takes the environment lock.
A `rig_sha` older than the trial kinds takes no `--cloudtasks-trial` and fails at its argument parsing, before the lock.
The run's queue is `ct1362-<run-id>` in `flink-gcp/us-central1`.
Choose a run ID that has not been used: a deleted queue's name can stay unavailable for a tombstone window of up to three days, and a used ID already has evidence, which dispatch refuses.
A dispatch can also carry a campaign of the remaining kinds and a repeat budget, as the [Bigtable dispatch section](../bigtable/README.md#dispatch) describes for both scenarios.

The [`cloudtasks-recovery`](../../lifecycle/cloudtasks.cue) delivery takes the input from the optional `cloudtasks_records` and `cloudtasks_records_per_second` tags, 180,000 records at 100 per second unless given and at most 30 minutes of input, and the kind from the `cloudtasks_trial` tag, `jm-replacement` unless given, which sizes each phase's sink and TaskManagers from [faults.cue](../../lifecycle/faults.cue).
Dispatch renders it at the dispatched kind and the input every kind shares.

### Queue lifecycle

After the supervisor heartbeats and the Operator is ready, the runner records the queue's intent, reads that the name is free, records the creation intent and a call marker in one control write, creates the queue with a one-hour `tombstoneTtl`, pauses it, and reads it back until it reports `PAUSED` with the approved configuration.
The readback is also what proves the create took effect: the [DeleteQueue reference](https://docs.cloud.google.com/tasks/docs/reference/rest/v2beta3/projects.locations.queues/delete) says a create can appear to succeed without creating the queue, and recommends reading it back.
Only then does the runner open the namespace quota for three Pods and create the FlinkDeployment.
A queue that already carries the name refuses the run, which then deletes nothing.

A create the service answers with 409, although the name read free just before, names a queue, or a deleted name's tombstone, that is not this run's: the run clears its creation intent and fails, deleting nothing.
The runner releases its authority over the queue when it settles, without stopping the run, and retries the release on every poll; it makes no queue call after admission.
A create that did not return keeps its marker, and then neither the release nor cleanup proceeds: the create could still land after cleanup found the name absent, so an operator decides.
A create refused before it was sent, for example because the admission deadline passed while the marker was being written, clears the marker and the creation intent.

Only the supervisor deletes the queue, and only one whose creation intent the record holds.
It waits for the runner's release and for the namespace to hold no Pod, deletes the queue and reads it back until it is gone, and shared cleanup then removes the run's state under `runs/<run-id>/` and `.inprogress/flink-gcp-tier3-cloudtasks/runs/<run-id>/` in the `flink-gcp-tier3-cloudtasks` state bucket.

### Exercise and oracle

The supervisor runs the shared recovery exercise on the application's `cloudtasks-progress` lines, with the faults the trial's kind names: one savepoint upgrade, a rescale for `rescale-out` and `rescale-in`, while the initial input still flows, then one Pod deletion while the upgraded input flows.
Each recovery is proved by a restored savepoint or checkpoint, advanced input and a new completed checkpoint; a rescale also by its checkpoints' subtask count moving as approved, and a TaskManager replacement also by the deleted Pod being gone under the same JobManager.
The input lineage must stay the one the first progress line reported.
A Pod interruption at any stage, a Pod replacement outside a recovery, and a job restart the exercise did not plan, read from the restores the job's checkpoint statistics count, each stop the run as inconclusive, as [ADR-0165](../../../docs/adr/0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md#unplanned-job-restarts) records.

The exercise holds three measurement windows and reads the job's REST service once a minute throughout:

| Window | Opens | Closes |
| --- | --- | --- |
| `baseline` | One minute after the first poll that sees the initial input flowing on the whole set of Pods, a warm-up the window does not count | When the upgrade begins, at the first checkpoint triggered five minutes or more after the window opened |
| `upgraded` | The upgrade's proof | When the deletion begins, five minutes or more later; the failover must not restore behind the latest checkpoint triggered since the window opened |
| `finishing` | The failover's proof | Five minutes later; input that finishes sooner fails the run |

A reading is a `cloudtasks-measurement` evidence event carrying the stage it was taken in and its window, or none: a reading during startup, a recovery or the rest of the input counts toward no window.
Its families are the ones the BigQuery trials read: the sink vertex's task, network and connector metrics, each TaskManager's memory metrics, and each sink subtask's share of the latest completed checkpoint.
The connector family is the staged writer's and committer's metrics, among them `tasksDeduplicated`, which counts the creates the service refused as existing since the task last started, so a reading after a recovery counts what its replay found existing, and the expired-envelope counters.
The committer's `currentCommit` replay-budget gauges hold a value only during a commit, and the commit that replays a restored checkpoint runs once, while its task initializes, so a reading catches it only by chance and more often catches an ordinary checkpoint's commit; what a recovery spends of the budget is bounded instead from Flink's timestamps, as [ADR-0165](../../../docs/adr/0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md#cloud-tasks-fault-kinds-and-measurement-windows) records.

When the upgraded application reaches `FINISHED` with the whole input processed, the supervisor reads the queue back and lists its tasks in the basic view, page by page.
It exports every page as a `cloudtasks-task-page` evidence event, holding the listed task ids, any name outside the run's queue and the ids of any task with a dispatch attempt.
The oracle maps each id back to its sequence through the SHA-256 of `<run-id>/<sequence>` and passes only when every sequence is listed once, nothing else is listed and nothing was dispatched.
A failing listing is repeated 15 seconds later while another attempt as long as the last still fits the listing window, which closes 600 seconds after the application finishes or a minute before the cleanup start, whichever is first; each attempt exports its own pages, and a transiently failing call is retried every 5 seconds.
A queue or task-listing call still failing when the window closes, however it fails, leaves the last whole attempt in place, so a listing still failing then is the trial's finding and is recorded, not raised.
A non-transient error from such a call before then, any failure of the run's own state reads, which precede each attempt and recur once a poll, and a window that closes on the first attempt end the run without a verdict.

### Verdict and analysis

The run is `usable` when both recoveries are proved, the queue read back `PAUSED` before the listing, the oracle passed, and each of the three windows read every family; anything short of that is `inconclusive`.
A family counts as read only where a reading returned one of its metrics, not where a request named it.
The exercise decides the verdict with the transition that completes the run, and the final receipt's `success` is that verdict.

`just tier3-analyze <dir>` recomputes the verdict from a downloaded evidence directory: it rebuilds the oracle report from the pages of the attempt the record names and the coverage from the exported readings, reports a page or reading missing from the export as a problem that leaves the run without a verdict, and reports a stored report or verdict that the evidence does not reproduce.
`ct-1362-a1` read no family, so its evidence recomputes to `inconclusive` under these rules; archived evidence is read with the revision that produced it.
It also reports each recovery's timing, from the supervisor's record of its stage to Flink's restore and to the proving checkpoint's acknowledgement, as the BigQuery section does, and when the committer's replay of the restored snapshot began, after Flink's restore and before the restored job's first checkpoint, both read from the exported `checkpoints` polls.
It measures that window from the snapshot's last acknowledgement, which precedes its completion by however long Flink takes to finalize it, so the figures can only overstate what the recovery spent.
That is what the recovery spent of each restored envelope's replay budget; a recovery whose snapshot or next checkpoint no exported poll saw is reported unreadable.
A usable verdict makes no claim about a handler, since a paused queue dispatches nothing.
