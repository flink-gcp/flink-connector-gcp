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

# Spanner recovery trials: preregistration

This is the preregistration for the deployed Spanner trials of [#1878](https://github.com/flink-gcp/flink-connector-gcp/issues/1878), which serve [#1360](https://github.com/flink-gcp/flink-connector-gcp/issues/1360) and its Change Streams sub-issue [#1872](https://github.com/flink-gcp/flink-connector-gcp/issues/1872) within [ADR-0165](../0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md#spanner-recovery-trial).
One job carries the production `SpannerSink` into `input`, the production `SpannerChangeStreamSource` relaying `input`'s change stream into `relay`, and the production `SpannerSource` copying `seed` into `batch_out`, so each trial recovers all three components, as the [scope](https://github.com/flink-gcp/flink-connector-gcp/issues/1360#issuecomment-6270858712) decided.
This record fixes the pilot and the campaign, their input and seed, the estimate and repeat allowance, what the pilot measures, the acceptance criteria, the stop conditions and the cleanup checks.
It is posted on #1360, and nothing is dispatched until the owner approves it there.

## Campaign

| Order | Dispatch | `spanner_trial` input | Upgrade | Pod deleted | Subtasks of every vertex but the input, initial then upgrade | First run ID |
| ---: | --- | --- | --- | --- | --- | --- |
| 1 (pilot) | pilot | `jm-replacement` | savepoint upgrade | JobManager | 2, 2 | `sp1878-jm-a1` |
| 2 | campaign | `tm-replacement` | savepoint upgrade | the TaskManager that reported the latest input | 2, 2 | `sp1878-tm-a1` |
| 3 | campaign | `rescale-out` | savepoint rescale out | JobManager | 1, 2 | `sp1878-ro-a1` |
| 4 | campaign | `rescale-in` | savepoint rescale in | JobManager | 2, 1 | `sp1878-ri-a1` |

The pilot is dispatched alone through the [run workflow](../../../.github/workflows/tier3-run.yaml): `scenario=spanner-recovery`, `spanner_trial=jm-replacement`, `run_id=sp1878-jm-a1`, the `application_digest` under [Campaign window](#campaign-window), no `campaign` input and `repeats=0`, as the [runbook's dispatch section](../../../kubernetes/apps/spanner/README.md#dispatch) describes.
With neither a campaign nor a repeat, the workflow chooses no next attempt, so the pilot runs once whatever its result.
Only after the operator has read its receipt and it is `usable`, or the owner has decided otherwise on what it shows, is one campaign dispatched for the other three: `spanner_trial=tm-replacement`, `run_id=sp1878-tm-a1`, `campaign=rescale-out,rescale-in`, `repeats=2` and the same `application_digest`.
The campaign runs `tm-replacement` and then the kinds of its `campaign` input in the order named, `rescale-out` and then `rescale-in`.
Its two repeats are one budget the three kinds share, not two for each: a repeat is dispatched with one repeat fewer, and a kind that reaches `usable` passes what is left to the next, so the campaign makes at least three attempts and at most five.
Each run ID reads `<prefix>-<kind>-a<attempt>`, with `jm`, `tm`, `ro` or `ri` for the kind; a dispatch that names a campaign or a repeat checks that form against its `spanner_trial` before anything runs, which the pilot's does not, though its run ID has the same form.
A repeat takes the next attempt suffix, such as `sp1878-tm-a2`.
Every run ID fits the 40-character limit, and the run's instance is always `sp1360-<run-id>`, such as `sp1360-sp1878-jm-a1`, 19 of the 64 characters Spanner allows, with the display name `sp1360 <run-id>`.
Each trial runs a savepoint upgrade and then deletes a Pod, so the four together give each component the five faults of the Tier-3 measurement set: a savepoint upgrade, a JobManager replacement, a TaskManager replacement, a rescale out and a rescale in.

Once a campaign attempt is idle and has released the environment lock, the campaign reads its receipt, as the [Bigtable dispatch section](../../../kubernetes/apps/bigtable/README.md#dispatch) describes:

- a `usable` verdict dispatches the next kind at attempt 1;
- while repeats remain, a stop before the application existed, or "Unplanned workload interruption", dispatches the same kind at the next attempt;
- anything else ends the campaign for the owner to read.

The pilot gates the other trials by the operator's reading rather than the campaign's, because it is the first deployed run of the application, the instance lifecycle, the grants and the oracles, and whatever stops it is read before anything else is spent.
Each attempt checks out `main` as it is at its dispatch, so no rig change is merged while the pilot or the campaign runs.
Every admitted attempt is listed in the findings, finished or not.

The oracles answer different questions, and each reaches only part of a fault:

- **Sink, `input`**: every input key holds its sequence and no other key is there.
  The sink is at-least-once and writes `insertOrUpdate`, so the table cannot show a repeated write; the sink oracle counts each key's executions as its distinct commit times in `relay`, reported and never failed. The identical-write probe below showed that every execution of a write produces a change record, so the count is the number of executions, provided the relay delivered every record; the Change Streams oracle judges that delivery.
- **Change Streams, `relay`**: every input key was delivered at least once, no delivered key lies outside the input, no key's last relayed commit is later than the change stream's end, and the source's watermark advanced after each recovery.
  It judges keys, not commits, so a lost second commit of a key whose other commit was relayed is not detected; deliveries beyond a key's commits are the source's replays, reported apart from the sink's executions, and no exactly-once delivery is claimed.
- **Batch read, `batch_out`**: `seed` holds its 120,000 rows and the 16 late ones, every seed row was copied with its own value, and no late or foreign row was copied.
  A key copied more than once is a whole-partition replay, reported with the readers' `partitionsReread`; no intra-partition resume is assumed.
  The late rows are committed after the read planned its snapshot, so a copied late row would show a recovery that re-planned at a newer snapshot.

## Input, seed and window

Every trial has the same input, seed and change-stream end, which `spanner.plan.validate_trial` refuses to vary, and the same timings:

- **Input**: 180,000 records at 100 records per second, 30 minutes, with checkpoints every 30 seconds.
- **Seed**: 120,000 rows the runner writes before the application exists, in 12 single-use commits of 10,000 rows on one multiplexed session.
  The runner then splits `seed` at `4`, `8` and `c`, one split point per request and 65 seconds apart, because at 100 processing units the service adds one split point a minute and refuses a request over that whole with 429.
  [#1877's measurement](https://github.com/flink-gcp/flink-connector-gcp/issues/1877#issuecomment-6472333184) planned the read of this seed as one partition without split points and as seven with them.
- **Copy throttle**: 50 rows per second per reading subtask, so two subtasks copy the seed in about 20 minutes and one in about 40, and the copy is still reading through both recoveries.
  Each partition is one streaming read, which the pinned client ends after an hour without retrying, and seven partitions keep each read to minutes.
- **Change-stream end**: 3,900 seconds after the window's start, which dispatch derives from the window, so the job finishes once the input is complete, the source reaches its end and the copy is done.
- **Window**: 5,400 seconds from admission, whose last 900 are the cleanup reserve, so cleanup starts at 4,500 seconds.
  The table reads have 600 seconds after the application finishes, cut to a minute before the cleanup start, which leaves them 540 seconds after a job that finishes at the change stream's end.
  The database keeps two hours of versions, which covers the window, since a restored batch read resumes at the snapshot it planned at the job's first start.
- **Timings** (`policy.SPANNER_OBSERVATIONS`): 600 seconds from the window's start to the application's creation, 1,200 to input flowing, 600 for each recovery to prove itself, a 60-second warm-up, the `baseline`, `upgraded` and `finishing` windows of 300 seconds each read every 60 seconds, and 120 seconds for the upgrade to begin after the baseline.
- **Cleanup**: the supervisor waits for the runner's release and for the namespace to hold no Pod, deletes the instance, which takes its database, split points and sessions with it, reads it back until it is gone, and removes the run's Flink state, as [Cleanup checks](#cleanup-checks) verifies.

## Estimate

Each trial is estimated at USD 2.24 (`2.2361875`) by `spanner.plan.estimate`, the figure every rendered proposal carries:

- USD 1.06 (`1.0561875`) for the Operator, the supervisor and the three application Pods over the whole 90-minute window at the reviewed Autopilot rates, as for Bigtable.
- USD 0.18 for two hours of the 100-processing-unit instance at USD 0.09 an hour, a tenth of the USD 0.90 node-hour for a Standard regional instance in `us-central1` on the [pricing page](https://cloud.google.com/spanner/pricing).
  The two hours round the 90-minute window up on the assumption, not measured, that an instance is billed for at least an hour.
- USD 1.00 reserve for storage, state, evidence and telemetry.

The four trials come to USD 8.94 (`8.94475`).
The pilot is one attempt, since it repeats nothing, and the campaign's repeat allowance for platform interruptions is two attempts beyond its three trials, so the trials are approved for at most six attempts, the pilot's one and the campaign's five, and USD 13.42 (`13.417125`).
A rescale runs one TaskManager in one of its phases and is still charged two, and an attempt that stops early is charged in full, so this is an estimate, not a billing cap.
A repeat the owner approves by hand, under a condition below that the campaign cannot read, counts against the same six attempts; a seventh attempt needs this record revised and approved again.

## What the pilot measures

[#1877's measurement](https://github.com/flink-gcp/flink-connector-gcp/issues/1877#issuecomment-6472333184) ran every call as the owner, because impersonating `tier3-runner` and `tier3-supervisor` was not available, so the grants of the [Spanner recovery foundation](../0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md#spanner-recovery-foundation) are premises until the pilot runs them.
The pilot is the first run of each premise below, and each is read from the run's evidence as follows.

- **`CreateDatabase` with its extra statements under `spanner.databases.create` alone.**
  The runner holds no `spanner.databases.updateDdl`.
  The premise holds when `spanner-instance-admitted` reads back the four tables, the `input_changes` change stream and the two-hour retention.
  A 403 on the runner's `POST …/databases` refutes it.
- **The runner's session and seed commits under the instance-name condition.**
  IAM checks `spanner.sessions.create` on the database, whose name the condition matches, but the data calls' permissions on the session's name, which the conditions documentation does not list for Spanner.
  The premise holds when the admitted receipt records 120,000 seed rows and the three split points; a 403 on `…/sessions`, `…:commit` or `…:addSplitPoints` refutes it.
- **The workload's data calls under the same condition**: the sink's `BatchWrite` and `INFORMATION_SCHEMA` read, the change stream's metadata read and query, and the batch read's read-only transaction, `PartitionRead` and reads.
  The premise holds when the oracles find the input, the relay and the copy complete; a `PERMISSION_DENIED` in an exported `pod-log` refutes it.
- **The supervisor's calls under the same condition**: its session, the strong query that finds a copied row, the late-seed commit, the oracle queries, and the instance's deletion and readback.
  The premise holds when the record holds `late_seed`, the three oracle reports and a `cleaned` instance; a 403 in a `spanner-late-seed-pending` cause, the run's stop reason or the cleanup refutes it.
- **Admission within the 600-second startup budget.**
  #1877 measured the instance `READY` 13 to 17 seconds after its create, the database a second after its call and each seed commit in about 3 seconds, and the split points add 130 seconds of spacing, so admission is expected to take about three minutes beyond the supervisor's and the Operator's readiness.
  The findings report the time from the approval's `started_at` to `spanner-instance-admitted`.
  A startup failure before the application exists, such as "Spanner split points were not added in time", is a timing finding.
- **A split point repeated after a lost response.**
  The runner repeats an `addSplitPoints` request that failed without an answer, or was refused with 429, until the startup deadline, and the evidence records no repeat that succeeded.
  Only a repeat the service refuses shows, as the runner's stop reason naming `:addSplitPoints` and its status: that is a rig finding, since the runner should then read the repeated split point as accepted.
  The batch plan's partition count shows whether all three split points took effect.
- **Whether a write that repeats identical values produces a change record.**
  This was measured before the pilot rather than left to the trials. A dedicated probe it writes the same key with the same value twice through `BatchWrite`, the sink's API, and twice through `Commit`, into the trial's exact `input` table and its `input_changes` change stream, and reads the change stream back.
  [It does](https://github.com/flink-gcp/flink-connector-gcp/issues/1878#issuecomment-6525682846): on 2026-10-30, `sp1360-probe-2` (deleted afterwards) recorded an `UPDATE` with equal old and new values for the identical write through both APIs, and six writes produced six data change records with matching commit timestamps.
  The trials therefore read the sink's executions per key without the lower-bound caveat, and the pilot does not measure this again.
- **The batch plan.**
  The batch report carries the partition count and read timestamp from the line the enumerator logs on the JobManager; #1877 measured seven, and the pilot records it again.

A refuted grant premise stops the trials before any further dispatch, as [Stop conditions](#stop-conditions) describes; the other readings are findings and stop nothing on their own.

## Campaign window

The trials pull `spanner-recovery` from the [publication](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/40916550826) of main commit `34be4f08c691bfa7955b52bbc02955185271be8b` on 2026-10-29 UTC, the first after the scenario merged, which the images README records under [Spanner recovery publication](../../../kubernetes/images/README.md#spanner-recovery-publication).
The Operator runs the `operator` mirror and the supervisor the `lifecycle-tools` image that [pins.cue](../../../kubernetes/images/pins.cue) and the Operator's Helm values select; pins.cue adopted that run's lifecycle-tools digest, and the Operator's digest is unchanged, so its Helm pin stands.
A Spanner dispatch checks these three images:

| Package | Digest | Created (UTC) | Deletion-eligible (UTC) |
| --- | --- | --- | --- |
| `operator` | `sha256:2c667a1c14bdb7365f16811dd98a576d6e4993ccbcc369dc53fb75081116ee78` | 2026-10-29T17:24:57Z | 2026-11-05T17:24:57Z |
| `lifecycle-tools` | `sha256:d487ae2e859d392ed7caf9d419c236e475cfaa519f0954a3da28af3badf9d362` | 2026-10-29T17:25:42Z | 2026-11-05T17:25:42Z |
| `spanner-recovery` | `sha256:75a2d78a2315c02406c3dbc5dc7576d245f68ad110a23d94628541faffcf5c1f` | 2026-10-29T17:26:36Z | 2026-11-05T17:26:36Z |

Every attempt names `application_digest=sha256:75a2d78a2315c02406c3dbc5dc7576d245f68ad110a23d94628541faffcf5c1f`, which the run resolves to the application image `us-central1-docker.pkg.dev/flink-gcp/flink-tier3/spanner-recovery@sha256:75a2d78a2315c02406c3dbc5dc7576d245f68ad110a23d94628541faffcf5c1f`; the digest stays a dispatch input verified live, never pinned.

Dispatch refuses an image unless the run's window ends a day before its seven-day deletion eligibility, and the `operator` mirror is the earliest, so every trial's window must end before 2026-11-04T17:24:57Z.
The pilot takes one attempt of at most 120 minutes, the workflow job's limit.
The campaign's three attempts take about three and a half hours of serial dispatch at the 72 minutes that separated the Bigtable campaign's attempts, and its five take at most ten hours at the workflow job's limit, so the campaign is dispatched at least ten hours before that deadline.
An attempt the campaign would dispatch past it is refused at its own dispatch, which ends the campaign, and the remaining kinds then need a republication first.

## Before dispatch

The pilot, and then the campaign, is dispatched only after these are read, in this order, immediately before that dispatch:

- Before the pilot, the identical-write probe of [What the pilot measures](#what-the-pilot-measures) has run, its result is recorded there, and its instance `sp1360-probe-2` is deleted, as it was on 2026-10-30.
- Before the campaign, the pilot's receipt and `just tier3-analyze` over its exported evidence have been read, and the pilot is `usable` with its fault read as [Acceptance criteria](#acceptance-criteria) describes, or the owner has decided otherwise on what it shows; its [Cleanup checks](#cleanup-checks) have been read, and its batch plan's partition count is seven or the owner has decided on the measured value.
- The `application_digest` is the `spanner-recovery` digest under [Campaign window](#campaign-window), [pins.cue](../../../kubernetes/images/pins.cue) names the `lifecycle-tools` digest `sha256:d487ae2e…` listed there, and the `spanner-recovery` image, the `operator` mirror `sha256:2c667a1c…` and the `lifecycle-tools` pin are in the registry.
- `_control/environment.json` and every `_control/runs/` record are absent from `gs://flink-gcp-tier3-evidence`, every earlier `approval.json` has its `result.json`, and no other `tier3-run` workflow run is queued or in progress.
  The Bigtable campaign of [#1855](https://github.com/flink-gcp/flink-connector-gcp/issues/1855) released the environment on 2026-10-22.
- No `runs/sp1878-` prefix exists in the evidence bucket but the pilot's attempts, and `gcloud spanner instances list --project=flink-gcp --filter='name:sp1360-'` lists no instance, which before the pilot also shows that #1877's `sp1360-probe-1` is gone.
- The dispatch's first window, admission plus 90 minutes, ends before 2026-11-04T17:24:57Z, the deadline under [Campaign window](#campaign-window); for the campaign, with the ten hours it may take.

An attempt the campaign dispatches itself is admitted on its own reads of the lock and the run record, so the reads above are made once before the pilot and once before the campaign; an attempt the owner approves by hand gets them again.

## Acceptance criteria

Each trial is accepted as a measurement when its final receipt and `just tier3-analyze` over its exported evidence agree on a `usable` verdict, with no problem in the analyzer's report.
The fault is read from the evidence too, since the verdict does not check which ran:

- the approval's `spanner_trial.kind` is the trial's, and the analyzer's timing table names `savepoint-upgrade`, `rescale-out` or `rescale-in` for the upgrade and `jm-replacement` or `tm-replacement` for the failover as the table above does;
- a JobManager failover records a new JobManager Pod UID, and the TaskManager replacement records the deleted TaskManager as the one its rule named, with the JobManager's UID unchanged;
- a rescale's upgrade proof records the checkpointed subtasks moving between 4 and 7, the input at one subtask and the three component vertices at one or two.

The findings also publish, as findings rather than thresholds:

- each recovery's duration from Flink's restore and checkpoint timestamps, which `just tier3-analyze` reports;
- the measurement families per component vertex and window, with TaskManager memory and network for the job, since the three components share TaskManagers;
- the sink's execution histogram, the relay's replays, replayed keys and deliveries by phase, and the copy's replayed keys, extra deliveries, replays by key prefix and greatest `partitionsReread` in each window;
- the batch plan's partition count and read timestamp, and whether the late commit followed that timestamp (`late_after_plan`);
- the coordinator's greatest `changeStreamPartitionsDiscovered` in each window, and the readers' partition metrics, so that a service partition change stays distinct from a Flink rescale; a trial that observes none says so;
- whether the copy was still reading at each recovery, from the copy's `rowsRead` and `partitionsReread` in the window after it, since a copy that had finished would leave that recovery untested for the batch read;
- the readings of [What the pilot measures](#what-the-pilot-measures), the instance lifecycle's events and the [Cleanup checks](#cleanup-checks).

## Verdicts and oracle results

A run's verdict is `usable` or `inconclusive`; there is no failed verdict.
It is `usable` when both recoveries are proved, all three oracles pass, the Change Streams one having been judged, and each of the `baseline`, `upgraded` and `finishing` windows read every family; anything short of that is `inconclusive` with its reasons:

- `recovery-incomplete`, `missing-upgrade` or `missing-failover`: the run stopped before its oracles, and its stop reason decides what follows.
- `sink-oracle-not-passed` or `batch-oracle-not-passed`: that oracle's verdict is `fail`.
- `change_stream-oracle-unjudged`: the input's last progress line came less than 120 seconds before the change stream's end, so a late commit may not have been relayed, or a window read no watermark.
  An unjudged oracle is never passed.
- `change_stream-oracle-not-passed`: judged and failed, including a watermark that did not advance after a recovery.
- `unsampled-<window>` or `unobserved-<family>-in-<window>`: a window missed a family.

Each oracle's own verdict is `pass` or `fail`, and the reports count and name up to 20 keys of each deviation.
A run that ends on a listing, state-read or oracle-read error stores no completion, which the analyzer reads as `inconclusive`.
The coordinator's partition count and the batch plan are reported, never judged: a coordinator metric that is not listed is recorded as a named absence, and a plan line not read is recorded as `unavailable`.

## Stop conditions

The conditions of the [Cloud Tasks fault trials](0165-cloudtasks-fault-trials-preregistration-1778.md#stop-conditions) apply, with the instance in place of the queue, read from the same evidence, as the [Bigtable trials](0165-bigtable-recovery-preregistration-1855.md#stop-conditions) adopted them.
The campaign acts on each receipt first, as [Campaign](#campaign) describes; the conditions decide what follows a campaign that ended, and whether the owner approves a repeat the campaign could not read.
The pilot repeats nothing: it names no campaign and `repeats=0`, so a stop before the application exists, whatever its cause, ends it, and the operator reads it before any further dispatch.
A grant premise the pilot refutes, the first run of the foundation's IAM grants, therefore stops the trials after one attempt, before anything else is spent.
The campaign repeats every stop before the application exists, whatever its cause, because no connector code ran; a cause that recurs there spends its two repeats, which bound its cost within the six approved attempts.
The operator does not cancel an attempt to save a repeat, since a cancelled run keeps the environment lock until the completed-execution recovery of the [lifecycle runbook](../../../kubernetes/lifecycle/README.md#cleanup-and-recovery) runs.

### Spanner conditions

These come first, in this order:

- **A grant premise is refuted**: the runner's database create, session, seed commit or split point is refused with 403, or the supervisor's session, query, late-seed commit, deletion or readback is, or an exported `pod-log` shows the workload's call refused with `PERMISSION_DENIED`.
  This is a grant finding: no further dispatch, the grants are repaired in their own change, and this record is revised and approved again before another attempt.
  In the pilot any refusal ends the run at its one attempt and the campaign is not dispatched.
  In the campaign a runner refusal comes before the application exists, so the campaign repeats it until its two repeats are spent; a supervisor or workload refusal ends the campaign at that attempt.
- **The instance's name is not free, or its create is refused although the name read free**: the run deletes nothing; the campaign repeats it under the next attempt suffix, and in the pilot the owner decides on reading it whether to approve that repeat by hand; the same result on the repeat is a rig finding, and nothing else is dispatched.
- **The instance create did not answer and the instance never read back before admission stopped**: the run keeps its call marker, gets no idle receipt and keeps the lock, which ends the campaign.
  The operator verifies and deletes the instance by the [runbook's procedure](../../../kubernetes/apps/spanner/README.md#instance-lifecycle), and the owner decides how the environment is recovered.
- **`spanner-database-deviation`, or a seed commit or split point refused with a status the runner does not repeat**: a rig finding; stop before another dispatch, at once in the pilot and once the campaign's repeats of it are spent in the campaign.
  A refused repeat of a split point is the lost-response case [What the pilot measures](#what-the-pilot-measures) names.
- **Admission does not finish within the 600-second startup budget**: a timing finding; stop, and the budget is widened in its own change before another dispatch.
- **The `baseline` stage expires without a `late_seed` record**, because `batch_out` held no row: read the JobManager's plan line, the copy's `rowsRead` and any `spanner-late-seed-pending` cause.
  A refused read or write is a grant finding; a read that never planned or never wrote is an application or connector finding.
  Stop and read the evidence before another attempt.
- **The sink oracle reports a missing key or a wrong value**: a connector finding, recorded rather than repeated.
  A foreign key in `input`, which nothing but the sink writes, is a rig finding; stop.
- **The Change Streams oracle, judged, reports a missing key, a key outside the input, a commit relayed after the end, or a watermark that did not advance after a recovery**: a connector finding, recorded rather than repeated; stop.
- **The Change Streams oracle is unjudged**: a timing finding if the input finished less than 120 seconds before the end, an instrument finding if a window read no watermark; stop and read the evidence before another attempt.
  The usable Bigtable trials finished their input 1,250 to 1,485 seconds before the end, and Spanner's admission adds about three minutes of seeding and split points before the input starts, so a slow recovery is what spends the rest of that margin.
- **The batch oracle fails**:
  - with the seed report failing, the runner's seed or the supervisor's late rows are not what was written: a rig finding;
  - with the seed passing, a missing seed key or a wrong value is a connector finding;
  - a late key copied with `late_after_plan` true is a connector finding, a read at a newer snapshot than the one planned; with it false it is a rig finding, since the late commit then preceded the snapshot; with it unread, the copied row that gated the late commit still dates it after the plan, so it is read as a connector finding;
  - a foreign key is a rig finding.
  Each is recorded rather than repeated; stop.
- **The job does not finish before the cleanup start**, for example because the copy outlasted the change stream's end or a partition held the source past it: a finding about the end condition or the copy's pace; stop and read the evidence before another attempt.
- **After cleanup an `sp1360-` instance still exists**: stop the trials, with no further dispatch, and delete it by hand by the runbook's procedure before anything else.

### Events the Bigtable campaign met

The [Bigtable findings](0165-bigtable-recovery-findings-1855.md#observations) recorded six events outside the connectors.
Each is classified here in advance:

- **A `kube-system` Pod preempts the supervisor** (`bt1855-tm-a1`, `bt1855-tm-a2`).
  Before supervision starts, the supervisor Job replaces a disrupted Pod up to twice; a stop before the application exists is repeated by the campaign, a platform stop listed in the findings and needing no further approval, and in the pilot it ends the run and the owner decides on reading it whether to approve a repeat by hand.
  After the application exists, the run stops and the campaign ends; the preemptor is read from the cluster's events at the time, since the evidence records only that the supervisor Job had no Pod, and a platform cause gets one repeat the owner approves by hand.
- **A create gets no answer in time** (`bt1855-tm-a3`).
  The runner reads the instance until it exists, reads back and repeats an unanswered or 409 database create (`spanner-create-uncertain`), and repeats unanswered seed commits and split points, all until the startup deadline.
  A deadline reached that way is the startup condition above, and an instance that never reads back is the kept-marker condition above.
- **An evidence or control write gets no answer** (`bt1855-tm-a4`).
  [#1869](https://github.com/flink-gcp/flink-connector-gcp/issues/1869) reads such a write back; one still unresolved is an evidence failure, which withholds `usable` and ends the campaign: a rig finding, so stop, repair and approve again.
- **A TaskManager's loss restores once per failover region** (`bt1855-tm-a5`).
  [#1871](https://github.com/flink-gcp/flink-connector-gcp/issues/1871) counts those restores as one recovery, and the Spanner exercise accepts up to three: the input with its sink, and the lost TaskManager's relay subtask and copy subtask, which share no edge with any other.
  "Unplanned job restart" during the TaskManager replacement is read against the JobManager log: more regions than three, each restoring the same checkpoint, is a rig finding; anything else is an unplanned restart.
  Either way, stop, repair and approve again.
- **A replacement TaskManager waits for an Autopilot scale-up** (`bt1855-ri-a1`).
  [#1882](https://github.com/flink-gcp/flink-connector-gcp/issues/1882) set the Bigtable budget to 600 seconds, and Spanner's is 600 from the start.
  "Recovery exercise deadline expired" falls to the Cloud Tasks deadline condition: with a TaskManager Pod not Running, fewer TaskManagers than the phase runs, or a platform cause within the stage, the owner approves one repeat by hand; otherwise it is any other inconclusive attempt.
  A recovery slow enough to spend the margin before the change stream's end leaves the relay unjudged, which the condition above covers.
- **A Spot reclamation is invisible to the exercise** (`bt1855-ri-a2`).
  [#1892](https://github.com/flink-gcp/flink-connector-gcp/issues/1892) now reads the node's `Shutdown` event for a Pod lost outside a planned recovery, so a reclamation reads as "Unplanned workload interruption", which the campaign repeats, and which in the pilot ends the run for the owner to decide on reading it whether to approve a repeat by hand.
  If it still reads "Unplanned workload replacement", the campaign ends; the operator reads Compute Engine's `compute.instances.preempted` audit entry and the kubelet's shutdown log for the node, and a platform cause gets one repeat the owner approves by hand, under the Cloud Tasks replacement condition; otherwise the replacement is the application's, so stop.

### Readings that stop nothing

- **The batch plan's partition count.**
  #1877 measured seven, and every trial's plan line is recorded, the pilot's first.
  The attempt is judged as its verdict decides; if the count differs from seven, the owner decides on the measured value whether the rescale trials need a rerun, and a pilot count other than seven is decided before the campaign is dispatched.
  One partition at 50 rows per second takes 2,400 seconds before any replay, so such a copy may outlast the change stream's end, which the cleanup-start condition above covers.
- **No plan line was read.**
  The report carries `unavailable` and `late_after_plan` stays unread; the batch oracle still judges every row.
- **The coordinator metric is absent** in a window: recorded as a named absence; the findings then read service partition changes from the readers' `changeStreamQueriesStarted`, `activeChangeStreamQueries` and `queuedChangeStreamPartitions`, or report them unobserved.
- **The admission duration**, as [What the pilot measures](#what-the-pilot-measures) reads it.

### Environment conditions

The environment conditions of the Cloud Tasks record apply as written: an attempt that does not reach verified idle, leaves a refreshed plan nonempty or keeps the lock stops everything until the environment is recovered; an analyzer that gives an attempt no verdict is re-run on a fresh download before anything else is concluded; and an image reaching its retention margin stops the remaining trials until a republication.
The campaign is not dispatched until the pilot's receipt has been read: a pilot that does not reach `usable` for any reason not above is repaired, and this record revised and approved again, before any other trial, unless the owner decides otherwise on what it shows.

## Cleanup checks

The owner decided that each run's Spanner instance, Pods and Flink state are deleted promptly after it, and that its evidence and the published images are left to their automatic lifecycles.
The supervisor waits for the namespace to hold no Pod, deletes the run's instance, with its database, split points and sessions, reads it back until it is gone, and removes the run's Flink state.
Once each run's receipt is written, after the pilot and after every campaign attempt, even while the campaign's next attempt runs, the operator verifies from the receipt and directly:

- `gcloud spanner instances describe sp1360-<run-id> --project=flink-gcp` reports the run's instance not found, and the receipt, if the run created it, records it `cleaned`.
- `gcloud storage ls --recursive gs://flink-gcp-tier3-spanner/` lists nothing under `runs/<run-id>/` or `.inprogress/flink-gcp-tier3-spanner/runs/<run-id>/`.
- The receipt records verified idle, with no Pod in the `tier3-spanner` namespace, and the empty refreshed plans, and `_control/runs/<run-id>.json` is gone.

After the pilot, and again after the campaign ends, with no attempt running, the operator also verifies:

- `gcloud spanner instances list --project=flink-gcp --filter='name:sp1360-'` lists no instance; every run's instance is `sp1360-<run-id>`, so this covers every `sp1878-` attempt.
- The `tier3-spanner` namespace holds no Pod and no FlinkDeployment, and its `tier3-idle` quota is back to its idle values.
- `_control/environment.json` is gone.

An instance still present stops the trials and is deleted by hand by the [runbook's procedure](../../../kubernetes/apps/spanner/README.md#instance-lifecycle), as [Stop conditions](#stop-conditions) describes, and Flink state still present is deleted by hand once read rather than left to the state bucket's one-day lifecycle.
Nothing else is deleted by hand.
The run's evidence under `runs/<run-id>/` in `gs://flink-gcp-tier3-evidence`, the record the findings are written from and which is downloaded and kept until they merge, is left to that bucket's 30-day lifecycle, and the published images to the registry's 7-day cleanup policy.
The findings record each of these reads with its time.

## Interpretations the owner must confirm

1. Each trial is run until it reaches `usable` within the approved attempts; variance across repetitions is not claimed.
2. Acceptance is the `usable` verdict with the fault read above; recovery durations, families, replay counts and partition counts are published beside it, never pass or fail criteria.
3. A missing input key or wrong sink value, a missing or late relayed key, a watermark that did not advance, and a missing, wrong or late batch row are connector defects unless a condition above names the rig; the sink's extra executions, the relay's replays and the copy's extra deliveries are expected and counted.
4. The findings, including every inconclusive attempt and every reading above, are published beside this record as `0165-spanner-recovery-findings-1878.md`, and that change closes #1360, #1872 and #1878.

## Revisions after dispatch

The owner approved this revision on 2026-10-31, after the first campaign ended with two kinds not yet run, and before the second campaign below was dispatched.

**How the first campaign ended.** It was dispatched as preregistered (run 41088249409), and ended once its two repeats were spent:

- `sp1878-tm-a1` stopped at "Unplanned workload interruption". A `system-cluster-critical` `konnectivity-agent` Pod preempted the JobManager during an Autopilot defragmentation.
- `sp1878-tm-a2` was usable.
- `sp1878-ro-a1` stopped before the application existed. A `kube-dns` Pod preempted the supervisor about 30 seconds after it started.
- `sp1878-ro-a2` stopped at "Unplanned job restart". A GCS `compose` call timed out while writing checkpoint 71's `_metadata`. With Flink's default of zero tolerable checkpoint failures, that one failure restarted the job.

None of these points to a connector defect. With the pilot, five of the six approved attempts were spent, `jm-replacement` and `tm-replacement` were usable, and `rescale-out` and `rescale-in` remained.

**The second campaign.** One more campaign is approved, with the application and every other part of this record unchanged:

- inputs: `spanner_trial=rescale-out`, `run_id=sp1878-ro-a3`, `campaign=rescale-in`, `repeats=2`, and the same `application_digest`;
- at most four more attempts, so nine in all;
- at most USD 8.94 more (`8.94475`), so USD 20.12 (`20.1256875`) in all.

The reads of [Before dispatch](#before-dispatch) are made again before it.

**Corrections the findings carry:**

- [Events the Bigtable campaign met](#events-the-bigtable-campaign-met) says the supervisor Job replaces a disrupted Pod up to twice before supervision starts. That is false: the Job's `backoffLimit` is 0, and `sp1878-ro-a1`'s Job ended at once with `BackoffLimitExceeded`.
- A kube-system Pod preempting the JobManager after admission is a variant that list does not name. Its classification, "Unplanned workload interruption", is the one the campaign repeats.
- The checkpoint-storage timeout matches no condition here, so it ended the first campaign. The findings report it as inconclusive, together with the application's zero tolerance of checkpoint failures.

The owner approved a second revision on 2026-10-31, after the second campaign ended with `rescale-in` not yet run, and before the dispatch below.

**How the second campaign ended.** It was dispatched as approved above (run 41150053853):

- `sp1878-ro-a3` was usable, with both repeats still unspent.
- The runner's settle read of the `tier3-system` namespace then failed on one connection reset by peer. The attempt's lifecycle job failed before it chose a next attempt, so the job that dispatches one was skipped and no `rescale-in` attempt ran.
- [#1937](https://github.com/flink-gcp/flink-connector-gcp/issues/1937) made every Tier-3 Kubernetes call repeat such a failure. [PR #1938](https://github.com/flink-gcp/flink-connector-gcp/pull/1938) merged it as `ceae8b28d31206a22eeeaf1a7a56c2339a17b478` before the dispatch below.

With the pilot, six of the nine approved attempts are spent. `jm-replacement`, `tm-replacement` and `rescale-out` are usable, and `rescale-in` remains.

**The `rescale-in` dispatch.** The owner approves one more dispatch by hand, with the application and every other part of this record unchanged:

- inputs: `spanner_trial=rescale-in`, `run_id=sp1878-ri-a1`, no `campaign` input, `repeats=2`, and the same `application_digest`;
- at most three attempts, `sp1878-ri-a1` to `sp1878-ri-a3`, so nine in all, as the second campaign's approval allowed;
- at most USD 6.71 more (`6.7085625`), so the total stays within USD 20.12 (`20.1256875`);
- every attempt's window ends before 2026-11-04T17:24:57Z, so the dispatch is made no later than 2026-11-04T10:00Z.

The repeats follow the campaign's rule: a stop before the application existed, or "Unplanned workload interruption", dispatches the next attempt, and anything else ends the dispatch for the owner to read.
The reads of [Before dispatch](#before-dispatch) were made again at 2026-10-31T05:57Z, immediately before it, on `main` at `ceae8b28d31206a22eeeaf1a7a56c2339a17b478`: the three digests are in the registry and pins.cue names the `lifecycle-tools` one, no `_control/environment.json` or `_control/runs/` record exists, all 47 `approval.json` records have their `result.json`, the only `runs/sp1878-` prefixes are the six attempts above, no `sp1360-` instance exists, and no `tier3-run` run is queued or in progress.
Each attempt's [Cleanup checks](#cleanup-checks) are read once its receipt is written.
The two rig changes the findings make, a description of the sink's execution count that states the identical-write probe's result and a tolerance of two failed checkpoints in every application, are merged only after the last attempt ends, so they do not apply to it.
