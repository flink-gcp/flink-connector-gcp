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

# Firestore recovery trials: preregistration

This is the preregistration for the deployed Firestore trials of [#2085](https://github.com/flink-gcp/flink-connector-gcp/issues/2085), which serve [#1879](https://github.com/flink-gcp/flink-connector-gcp/issues/1879) within [ADR-0165](../0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md#firestore-recovery-trial).
One job carries the production Native-mode `FirestoreSink` and Datastore-mode `DatastoreSink` into `input`, and the production `FirestoreSource` and `DatastoreSource` copying `seed` into `batch_out`, each in its own database, so each trial recovers all four components, as the [scope](https://github.com/flink-gcp/flink-connector-gcp/issues/1879#issuecomment-6707006890) decided.
This record fixes the pilot and the campaign, their input and seed, the estimate and repeat allowance, what the pilot measures, the acceptance criteria, the stop conditions and the cleanup checks.
It follows the [Spanner preregistration](0165-spanner-recovery-preregistration-1878.md), whose exercise, windows and campaign the Firestore scenario shares, and takes over its rules only where a section below says so.
It is posted on [#1879](https://github.com/flink-gcp/flink-connector-gcp/issues/1879), and nothing is dispatched until the owner approves it there.

## Campaign

| Order | Dispatch | `firestore_trial` input | Upgrade | Pod deleted | Subtasks of every vertex but the input, initial then upgrade | First run ID |
| ---: | --- | --- | --- | --- | --- | --- |
| 1 (pilot) | pilot | `jm-replacement` | savepoint upgrade | JobManager | 2, 2 | `fs2085-jm-a1` |
| 2 | campaign | `tm-replacement` | savepoint upgrade | the TaskManager that reported the latest input | 2, 2 | `fs2085-tm-a1` |
| 3 | campaign | `rescale-out` | savepoint rescale out | JobManager | 1, 2 | `fs2085-ro-a1` |
| 4 | campaign | `rescale-in` | savepoint rescale in | JobManager | 2, 1 | `fs2085-ri-a1` |

The pilot is dispatched alone through the [run workflow](../../../.github/workflows/tier3-run.yaml): `scenario=firestore-recovery`, `firestore_trial=jm-replacement`, `run_id=fs2085-jm-a1`, the `application_digest` under [Campaign window](#campaign-window), no `campaign` input and `repeats=0`, as the [runbook's dispatch section](../../../kubernetes/apps/firestore/README.md#dispatch) describes.
With neither a campaign nor a repeat, the pilot runs once whatever its result.
Once it has been read as [Before dispatch](#before-dispatch) describes, one campaign is dispatched for the other three: `firestore_trial=tm-replacement`, `run_id=fs2085-tm-a1`, `campaign=rescale-out,rescale-in`, `repeats=2` and the same `application_digest`.
The campaign's two repeats are one budget the three kinds share, as in the Spanner campaign, so it makes at least three attempts and at most five.
A repeat takes the next attempt suffix, such as `fs2085-tm-a2`.
The run's databases are `fs1879-<run-id>-n` and `fs1879-<run-id>-d`, such as `fs1879-fs2085-jm-a1-n`, 21 of the 63 characters a database ID takes.
Each trial runs a savepoint upgrade and then deletes a Pod, so the four together give each component the five faults of the Tier-3 measurement set.

The campaign reads each receipt itself:

- a receipt that reads `success`, which needs a `usable` verdict and no evidence failure, dispatches the next kind at attempt 1;
- while repeats remain, a stop before the application existed, whatever its cause, or "Unplanned workload interruption", dispatches the same kind at the next attempt;
- anything else ends the campaign for the owner to read.

So a stop condition below that falls before the application exists ends the pilot at once but, in the campaign, ends it only once its repeats are spent; each such repeat is a full attempt against the six approved.
Each attempt checks out `main` as it is at its dispatch.
Since [#2115](https://github.com/flink-gcp/flink-connector-gcp/issues/2115) the environment lock no longer holds other development back, so a merge may land between attempts; the findings name each attempt's rig commit and list any change between attempts to the rig's paths, `tools/`, `kubernetes/`, `opentofu/`, `.github/` and the `justfile`.
Every admitted attempt is listed in the findings, finished or not.

The oracles judge each mode on its own, four components in all:

- **Sink, `input`, in each database**: every input key holds its sequence and no other key is there.
  Neither database has a change feed in this graph, so the oracle reads only the final state, and how often the sink executed a write is not observable; the findings say so instead of counting executions.
- **Batch read, `batch_out`, in each database**: `seed` holds its 108,000 records and the 16 late ones, every seed key was copied with its own value, and no late or foreign key was copied.
  A key copied more than once was emitted again after a restored checkpoint; such copies are reported, not failed, and no exactly-once delivery is claimed.
  The late records are committed after the read planned its read time, so a copied late record would show a recovery that read at a newer time than the one planned.

## Input, seed and window

Every trial has the same input and seed, which `firestore.plan.validate_trial` refuses to vary, and the same timings:

- **Input**: 180,000 records at 100 records per second, 30 minutes, with checkpoints every 30 seconds, written to both databases.
- **Seed**: 108,000 records in each database, which the runner writes before the application exists, in 216 commits of 500 writes per database.
- **Copy throttle**: 50 records per second per reading subtask, so two subtasks copy a seed in 1,080 seconds and one in 2,160.
  Both reads read at one read time planned at the job's first start, and without point-in-time recovery the service refuses a read time older than about an hour, so the seed is sized to keep the copies reading through both recoveries while the slowest layout, one split, still finishes about 41 minutes after the first start, as the [runbook's arithmetic](../../../kubernetes/apps/firestore/README.md#trial-and-estimate) shows.
- **Window**: 5,400 seconds from the window's start, whose last 900 are the cleanup reserve, so cleanup starts at 4,500 seconds.
  The oracle reads have 1,200 seconds after the application finishes, cut to a minute before the cleanup start.
- **Timings** (`policy.FIRESTORE_OBSERVATIONS`): a startup budget of 600 seconds from the window's start to the application's creation, which takes in both databases' creation and seeding; 1,200 seconds to input flowing, 600 for each recovery to prove itself, a 60-second warm-up, the `baseline`, `upgraded` and `finishing` windows of 300 seconds each read every 60 seconds, and 120 seconds for the upgrade to begin after the baseline.
  The application tolerates two failed checkpoints, as every Tier-3 application now does, which the Spanner trials did not.
- **Cleanup**: the supervisor waits for the runner's release and for the namespace to hold no Pod, deletes both databases, reads them until neither is found, and removes the run's Flink state, as [Cleanup checks](#cleanup-checks) verifies.

## Estimate

Each trial is estimated at USD 3.07 (`3.0714259`) by `firestore.plan.estimate`, the figure every rendered proposal carries:

- USD 1.06 (`1.0561875`) for the Operator, the supervisor and the three application Pods over the whole 90-minute window at the reviewed Autopilot rates, as for Spanner.
- USD 1.02 (`1.0152384`) for the planned operations in both databases: 792,032 writes at USD 0.09 per 100,000 and 1,008,032 reads at USD 0.03 per 100,000.
  The code carries these prices as planning assumptions; on 2026-11-08 they matched the `us-central1` prices for document writes and reads on the [pricing page](https://cloud.google.com/firestore/pricing).
  The free tier is not counted: the page allows one free database per project, and the run's databases are not it.
  Deleting a database is not a document delete, and storage of under 100 MB per database for under two hours is negligible.
- USD 1.00 reserve for storage, state, evidence and telemetry.

The four trials come to USD 12.29 (`12.2857036`).
The pilot is one attempt, and the campaign's repeat allowance is two attempts beyond its three trials, so the trials are approved for at most six attempts and USD 18.43 (`18.4285554`).
An attempt that stops early is charged in full, and the operations leave out the few thousand records a restore emits again, so this is an estimate, not a billing cap.
A repeat the owner approves by hand counts against the same six attempts; a seventh needs this record revised and approved again.

## What the pilot measures

Nothing in the scenario has run against the service: the database creation and readback, the seed commits, the REST shapes of both data APIs, the read plans and the grants of the [Firestore recovery foundation](../0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md#firestore-recovery-foundation) are premises until the pilot runs them.
Each is read from the run's evidence as follows.

- **The runner's creates under the creator role.**
  The runner holds `datastore.databases.create` through a custom role, and `roles/datastore.user` conditioned on the `fs1879-` database names, which is what it reads each database with, before the create and after it.
  The premise holds when `firestore-databases-admitted` reads back both databases, Native and Datastore mode; a 403 on `POST …/databases` or on a database's `GET` refutes it.
- **Data calls under the name condition, in both modes.**
  Firestore documents IAM conditions on a database's name for Native mode; that the Datastore API's calls, which name the database in the request body, are tested against the same name is a premise.
  The runner's seed commits test it first: its receipt records the seed size only once every commit in both modes has answered, and a 403 on `…/documents:commit` or `projects/flink-gcp:commit` refutes it.
  The supervisor's late-seed commits and oracle reads test it again.
- **The workload's calls**: the Native sink's batch writes, the Datastore sink's commits, the Native source's `PartitionQuery` and queries at a read time, and the Datastore source's `__scatter__` sampling and queries at a read time.
  The premise holds when all four oracles pass; a `PERMISSION_DENIED` in an exported `pod-log` refutes it.
- **The supervisor's deletion under the deleter role**, a custom role holding `datastore.databases.delete` under the same name condition.
  The premise holds when the receipt records both databases `cleaned`.
  A 403 on the deletion or on its readback is raised out of cleanup, with no `firestore-delete-pending` event, and the run keeps the lock; only a 400, a 409 or a transient failure leaves `firestore-delete-pending` for cleanup's next pass.
- **The startup budget.**
  Neither the creates nor the 432 seed commits have been measured; the findings report the time from the approval's `started_at` to `firestore-databases-admitted`, and from there to the application's creation.
  Overrunning the budget stops the run with "Firestore startup deadline expired", or, when a create, readback or seed commit is the call that crosses it, with "Firestore operation deadline exceeded" or a request timeout such as "POST Firestore request failed: ReadTimeout".
  The stop usually falls before the application exists; an application create that began inside the budget and answered after it leaves the application created, so the receipt does not read as a stop before the application existed.
- **The REST shapes.**
  A commit or query answer the rig cannot read stops the run with a message naming it, such as "Firestore commit returned no commit time" or "Firestore query response is malformed".
- **The read plans.**
  The batch report carries each read's split count and read time from the line its enumerator logs on the JobManager, and the record keeps them under `batch_plan`.
  Both reads ask for four splits; the service may plan fewer for a seed this small, and the seed is sized so that one split still finishes.
- **The read's span**, from the planned read time, or from the first copy when no plan line was read, to the last copy's update time; the runbook's arithmetic puts the slowest at about 41 minutes.
- **The oracle reads' rate.**
  Each database's three collections or kinds, about 400,000 documents or entities, are read in pages of 2,000 within the 1,200-second read window, at a rate nothing has measured.

A refuted grant premise stops the trials before any further dispatch, as [Stop conditions](#stop-conditions) describes; the other readings are findings and stop nothing on their own.

## Campaign window

The trials pull `firestore-recovery` from the publication of main commit `035622999a8e0612a71c5783a4caf780a5766179` on 2026-11-08 UTC, the first after the scenario merged, which the images README records under [Firestore recovery publication](0165-tier3-image-publications.md#firestore-recovery-publication).
The supervisor runs the `lifecycle-tools` image that [pins.cue](../../../kubernetes/images/pins.cue) adopted from that run, and the Operator the 1.16.1 `operator` mirror that its Helm values pin.
A Firestore dispatch checks these three images:

| Package | Digest | Created (UTC) | Deletion-eligible (UTC) |
| --- | --- | --- | --- |
| `operator` | `sha256:91250bb08c4f1ea358b201680e355139a530cb7318439926d62c6387d7c1369d` | 2026-11-07T14:41:26Z | 2026-11-14T14:41:26Z |
| `lifecycle-tools` | `sha256:3ad53a9c6e2c5edac17228bc3b284ab43957db4e7287eef85fb77add89ee07db` | 2026-11-08T13:53:25Z | 2026-11-15T13:53:25Z |
| `firestore-recovery` | `sha256:2ccf5d437bc9ebd8a384fe1d0f47ff53c48494528b9d11410b433cb50897c300` | 2026-11-08T13:54:14Z | 2026-11-15T13:54:14Z |

Every attempt names `application_digest=sha256:2ccf5d437bc9ebd8a384fe1d0f47ff53c48494528b9d11410b433cb50897c300`, which the run resolves to the application image `us-central1-docker.pkg.dev/flink-gcp/flink-tier3/firestore-recovery@sha256:2ccf5d437bc9ebd8a384fe1d0f47ff53c48494528b9d11410b433cb50897c300`; the digest stays a dispatch input verified live, never pinned.
Dispatch refuses an image unless the run's window ends a day before its seven-day deletion eligibility, and the `operator` mirror is the earliest, so every trial's window must end before 2026-11-13T14:41:26Z.
The pilot takes one attempt of at most 120 minutes, the workflow job's limit, and the campaign's five attempts at most ten hours, plus the minutes each next dispatch waits for a runner, so the campaign is dispatched no later than 2026-11-13T04:00Z, about 40 minutes inside that bound.
An attempt the campaign would dispatch past the deadline is refused at its own dispatch, which ends the campaign, and the remaining kinds then need a republication first.

## Before dispatch

The pilot, and then the campaign, is dispatched only after these are read, immediately before that dispatch:

- Before the campaign, the pilot's receipt and `just tier3-analyze` over its exported evidence have been read, and the pilot is accepted, with its fault read, as [Acceptance criteria](#acceptance-criteria) describes, or the owner has decided otherwise on what it shows; its [Cleanup checks](#cleanup-checks) have been read.
- The `application_digest` is the `firestore-recovery` digest under [Campaign window](#campaign-window), pins.cue names the `lifecycle-tools` digest listed there, and the three images are in the registry.
- `_control/environment.json` and every `_control/runs/` record are absent from `gs://flink-gcp-tier3-evidence`, every earlier `approval.json` has its `result.json`, and no other `tier3-run` workflow run is queued or in progress.
- No `runs/fs2085-` prefix exists in the evidence bucket but the earlier attempts', and `gcloud firestore databases list --project=flink-gcp` lists no `fs1879-` database.
- The dispatch's first window, its start plus 90 minutes, ends before the deadline under [Campaign window](#campaign-window); for the campaign, with the ten hours it may take.

An attempt the campaign dispatches itself is admitted on its own reads of the lock and the run record; an attempt the owner approves by hand gets these reads again.

## Acceptance criteria

Each trial is accepted as a measurement when its final receipt reads `success` true, and the receipt and `just tier3-analyze` over its exported evidence agree on a `usable` verdict, with no problem in the analyzer's report.
The receipt's `success` is checked on its own because an evidence failure after the recovery, such as a supervisor log that could not be exported, sets it false and records `evidence_failure` while the recovery's verdict still reads `usable`, and the analyzer does not read those fields.
The fault is read from the evidence too, since the verdict does not check which ran:

- the approval's `firestore_trial.kind` is the trial's, and the analyzer's timing table names the upgrade and the failover as the table above does;
- a JobManager failover records a new JobManager Pod UID, and the TaskManager replacement records the deleted TaskManager as the one its rule named, with the JobManager's UID unchanged;
- a rescale's upgrade proof records the checkpointed subtasks moving between 5 and 9, the input at one subtask and the four component vertices at one or two.

The findings also publish, as findings rather than thresholds:

- each recovery's duration from Flink's restore and checkpoint timestamps;
- the measurement families per component vertex and window, network included, with TaskManager memory for the job, since the four components share TaskManagers;
- each copy's replayed keys, extra copies and copies by phase;
- each read's split count, read time and span, and whether the late commit followed the read time;
- whether each copy was still reading at each recovery, from its reader's `documentsRead` or `entitiesRead` in the window after it, since a copy that had finished would leave that recovery untested for its source;
- the readings of [What the pilot measures](#what-the-pilot-measures), the database lifecycle's events and the [Cleanup checks](#cleanup-checks).

## Verdicts and oracle results

A run's verdict is `usable` or `inconclusive`; there is no failed verdict.
It is `usable` when both recoveries are proved, all four oracles pass, and each of the `baseline`, `upgraded` and `finishing` windows read every family; anything short of that is `inconclusive` with its reasons:

- `recovery-incomplete`: the run did not reach its final stage, and its stop reason decides what follows.
  It may have stopped before the recoveries, after them, or while the oracles read.
- `missing-upgrade` or `missing-failover`: that recovery was not proved.
- `native_sink-oracle-not-passed`, `datastore_sink-oracle-not-passed`, `native_batch-oracle-not-passed` or `datastore_batch-oracle-not-passed`: that oracle has no report or its verdict is `fail`.
  A run that stopped before its oracles carries all four with no oracle having run, so these are read as oracle failures only when the record holds the oracle reports.
- `unsampled-<window>` or `unobserved-<family>-in-<window>`: a window missed a family.

Each oracle's own verdict is `pass` or `fail`, and the reports count and name up to 20 keys of each deviation.
A run that ends on a listing, state-read or oracle-read error stores no completion, which the analyzer reads as `inconclusive`.
A read plan that is not read is recorded as `unavailable` and never judged.

## Stop conditions

The pilot repeats nothing, so any stop ends it and the operator reads it before any further dispatch.
In the campaign, the conditions below decide what follows once the campaign has acted on the receipt, as [Campaign](#campaign) describes: one that falls before the application exists ends the campaign only after its repeats are spent.

### Firestore conditions

In this order:

- **A grant premise is refuted**: the runner's database read, create or seed commit is refused with 403, or the supervisor's late-seed commit, oracle read, deletion or deletion readback is, or an exported `pod-log` shows the workload's call refused with `PERMISSION_DENIED`.
  This is a grant finding: no further dispatch by hand, the grants are repaired in their own change, and this record is revised and approved again before another attempt.
- **A database's name is not free, or the Native-mode create is refused although the name read free**: the run deletes nothing; the campaign repeats it under the next attempt suffix, and in the pilot the owner decides on reading it whether to approve that repeat by hand; the same result on the repeat is a rig finding, and nothing else is dispatched.
- **A create kept its call marker**, because the Native-mode create's answer was lost or a database never read back before startup stopped: the run gets no idle receipt and keeps the lock, which ends the campaign.
  The operator verifies and deletes the databases by the [runbook's procedure](../../../kubernetes/apps/firestore/README.md#database-lifecycle), and the owner decides how the environment is recovered.
- **A seed commit refused with a status the runner does not repeat, or a commit or query answer the rig cannot read**: a rig finding; stop.
- **The startup budget is overrun**, read as "Firestore startup deadline expired", or as "Firestore operation deadline exceeded" or a request timeout at the startup deadline before the application exists: a timing finding; stop, and the budget is widened in its own change before another dispatch.
  The campaign repeats it only when the receipt reads that the application was never created; one created late, as [What the pilot measures](#what-the-pilot-measures) describes, ends the campaign.
- **The `baseline` stage expires without both `late_seed` records** ("Recovery exercise deadline expired: baseline"), because a `batch_out` held no copy: read `outcomes.late_seed` for the database that lacks one, the JobManager's plan lines, the copies' reader metrics and any `firestore-late-seed-pending` cause.
  A refused read or write is a grant finding; a read that never planned or never wrote is an application or connector finding.
  Stop and read the evidence before another attempt.
- **A sink oracle reports a missing key or a wrong value**: a connector finding, recorded rather than repeated.
  A foreign key in `input`, which nothing but the sinks write, is a rig finding; stop.
- **A batch oracle fails**:
  - with the seed report failing, the runner's seed or the supervisor's late records are not what was written: a rig finding;
  - with the seed passing, a missing seed key or a wrong value is a connector finding;
  - a late key copied with the late commit after the planned read time is a connector finding, a read at a newer time than the one planned; with the commit before it, a rig finding; with no plan line read, the copy that gated the late commit still dates it after the plan, so it is read as a connector finding;
  - a foreign key is a rig finding.
  Each is recorded rather than repeated; stop.
- **The job fails after a read's time aged past the service's bound**: it stops as "Unplanned job restart" or on the job's `FAILED` state, and no oracle report exists; the cause is read from the read time under `outcomes.batch_plan` and the service's error in the exported `pod-log`.
  A finding about the seed's size or the copy's pace; stop before another attempt.
- **The job does not finish before the cleanup start**, read as "Recovery exercise deadline expired: finishing", or "admission/test deadline" when the cleanup start passes between the supervisor's polls: a finding about the end condition or the copy's pace; stop and read the evidence before another attempt.
- **The oracle reads do not finish within their window**: the record's stage is the oracle stage with both recoveries proved, and the stop reason is "Firestore operation deadline exceeded", or a request timeout at the window's end; a timing finding; stop, and the window is widened in its own change before another attempt.
- **After cleanup an `fs1879-` database still exists**: stop the trials, with no further dispatch, and delete it by hand by the runbook's procedure before anything else.

### Events earlier campaigns met

The Spanner record classified the events the Bigtable campaign met, and the [Spanner findings](0165-spanner-recovery-findings-1878.md#observations) corrected and extended that list; these apply here as follows:

- **A system Pod preempts the supervisor before the application exists**: the supervisor Job's `backoffLimit` is zero, so the run stops before the application existed, which the campaign repeats.
- **A system Pod preempts a workload Pod, or its node shuts down, after the application exists**: "Unplanned workload interruption", which the campaign repeats.
  If it reads instead as "Unplanned workload replacement", the campaign ends, and a platform cause read from the cluster's events gets one repeat the owner approves by hand.
- **A create gets no answer in time**: for the Native-mode create, the first, the run stops at once and keeps its call marker, which the kept-marker condition covers even if the database later appears; for the Datastore-mode create the runner reads it back and repeats it (`firestore-create-uncertain`) until the startup deadline, which the startup condition covers, or the kept-marker condition when the database never reads back.
- **An evidence or control write, or the final log export, fails and stays unresolved**: an evidence failure, which sets the receipt's `success` false, so the attempt is not accepted and the campaign ends; a rig finding.
- **A TaskManager's loss restores once per failover region**: the exercise accepts up to three, the input's region with both sinks and the lost TaskManager's two copy subtasks; more is a rig finding, and anything else is an unplanned restart.
- **A replacement TaskManager waits for an Autopilot scale-up** past the 600-second recovery budget ("Recovery exercise deadline expired"): with a TaskManager Pod not Running or a platform cause within the stage, the owner approves one repeat by hand; otherwise it is any other inconclusive attempt.
- **An infrastructure apply disturbs a running attempt**, for example by resetting its quotas or the Operator's scale: a repeat the owner approves by hand.

Each repeat by hand counts against the six approved attempts.

### Readings that stop nothing

- **A read's split count**, including one split, which the seed is sized to finish; the owner decides on the measured value whether a rescale trial whose copy held one split needs a rerun.
- **No plan line was read**: the report carries `unavailable`; the batch oracle still judges every record.
- **A copy that had finished before a recovery**: the verdict stays as computed, and the owner decides whether that trial needs a rerun for its source.
- **The startup duration and the oracle reads' rate**, as [What the pilot measures](#what-the-pilot-measures) reads them.

### Environment conditions

An attempt that does not reach verified idle, or keeps the lock, stops everything until the environment is recovered; an analyzer that gives an attempt no verdict is re-run on a fresh download before anything else is concluded; and an image reaching its retention margin stops the remaining trials until a republication.
A pilot that does not reach `usable` for any reason not above is repaired, and this record revised and approved again, before any other trial, unless the owner decides otherwise on what it shows.

## Cleanup checks

The owner decided that each run's databases, Pods and Flink state are deleted promptly after it, and that its evidence and the published images are left to their automatic lifecycles.
Once each run's receipt is written, the operator verifies from the receipt and directly:

- `gcloud firestore databases describe --database=fs1879-<run-id>-n --project=flink-gcp`, and the same for `-d`, report each database not found, and the receipt, if the run created them, records them `cleaned`.
- `gcloud storage ls --recursive gs://flink-gcp-tier3-firestore/` lists nothing under `runs/<run-id>/` or `.inprogress/flink-gcp-tier3-firestore/runs/<run-id>/`.
- The receipt records verified idle, with no Pod in the `tier3-firestore` namespace, and `_control/runs/<run-id>.json` is gone.

After the pilot, and again after the campaign ends, with no attempt running, the operator also verifies that `gcloud firestore databases list --project=flink-gcp` lists no `fs1879-` database and that `_control/environment.json` is gone.
A database still present stops the trials and is deleted by hand by the runbook's procedure, and Flink state still present is deleted by hand once read rather than left to the state bucket's one-day lifecycle.
Nothing else is deleted by hand.
The run's evidence under `runs/<run-id>/` in `gs://flink-gcp-tier3-evidence` is downloaded and kept until the findings merge, and is otherwise left to that bucket's 30-day lifecycle, and the published images to the registry's 7-day cleanup policy.
The findings record each of these reads with its time.

## Interpretations the owner must confirm

1. Each trial is run until it is accepted, as [Acceptance criteria](#acceptance-criteria) describes, within the approved attempts; variance across repetitions is not claimed.
2. Acceptance is the receipt's `success` with the `usable` verdict and the fault read above; recovery durations, families, extra copies, split counts, spans and whether each copy was still reading at each recovery are published beside it, never pass or fail criteria.
3. A missing input key or wrong sink value, and a missing, wrong or late copied record, are connector defects unless a condition above names the rig; the copies' extra deliveries are expected and counted, and how often a sink executed a write is not observed.
4. The findings, including every inconclusive attempt and every reading above, are published beside this record as `0165-firestore-recovery-findings-2085.md`, and that change closes #2085 and #1879.

## Revisions after dispatch

The owner approved this record on 2026-11-08, and the pilot was dispatched as preregistered once its pins had merged.
The first revision below was approved on #1879 on 2026-11-10, after the pilot stopped and before its re-run, and the second on 2026-11-11, after the re-run stopped; the third, after the second re-run stopped, reduces what is approved rather than widening it, and awaits the owner's approval there.

**How the pilot ended.**
`fs2085-jm-a1` ([run 42496716547](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/42496716547)), dispatched at 2026-11-08T15:49Z on `main` at `310dab35ce7479e7164f472a6adbaa7f1a668530`, stopped before the application existed:

- the runner created both databases and read both back, and wrote the Native-mode seed, so the creator role and the name-conditioned data grant held for those calls;
- its first Datastore-mode seed commit, `POST datastore.googleapis.com/v1/projects/flink-gcp:commit`, was refused with HTTP 400, which the runner does not repeat;
- cleanup deleted both databases, the run reached verified idle and released the lock, and the receipt records the databases `cleaned`.

The rig's refusal kept only the status, so the cause was read from a probe the owner approved, run with the owner's own credentials rather than the runner's: a Datastore-mode database `fs1879-probe-2085-d` in `us-central1`, created at about 16:15Z and deleted by 16:19Z, took the rig's exact commit and answered 400 `INVALID_ARGUMENT` "Missing routing header", asking for `x-goog-request-params: project_id=flink-gcp&database_id=<database>`.
The same commit with that header succeeded, and a `runQuery` without it was refused the same way, so the supervisor's late-seed commits and the oracle reads would have failed too.
Under [Firestore conditions](#firestore-conditions) this is a seed commit refused with a status the runner does not repeat, a rig finding.

**The repair.**
[#2130](https://github.com/flink-gcp/flink-connector-gcp/issues/2130) sends the routing header on every Datastore API call the rig makes, and has the shared REST helper keep the service's own message in a call it sees refused, so the evidence names the cause of the next refusal whenever the first 8 KiB of the service's error body can be read and parsed within the call's deadline.
It changes no input, timing, oracle or grant, and the application image is unchanged.
Since the probe did not run as the runner, whether the Datastore API's calls are tested against the database-name condition, a premise under [What the pilot measures](#what-the-pilot-measures), is still open, and the re-run reads it.

**The re-run.**
The owner approves the pilot again, with everything else in this record unchanged:

- inputs: `firestore_trial=jm-replacement`, `run_id=fs2085-jm-a2`, no `campaign` input, `repeats=0`, and the same `application_digest`, dispatched once #2130 has merged;
- then, after it is read as [Before dispatch](#before-dispatch) describes, the campaign as preregistered, from `fs2085-tm-a1`;
- at most one more attempt, so seven in all, `fs2085-jm-a1` included;
- at most USD 3.07 more (`3.0714259`), so USD 21.50 (`21.4999813`) in all;
- every attempt's window still ends before 2026-11-13T14:41:26Z, and the campaign is still dispatched no later than 2026-11-13T04:00Z.

The reads of [Before dispatch](#before-dispatch) are made again before the re-run.

**How the re-run ended.**
`fs2085-jm-a2` ([run 42846539231](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/42846539231)), dispatched at 2026-11-10T15:38Z on `main` at `b1d2514957b8d1d7684a892e7a844c25b67d33f1` after the reads of [Before dispatch](#before-dispatch), also stopped before the application existed:

- its window started at 15:40:55Z; the runner created both databases, read both back, wrote the Native-mode seed and was writing the Datastore-mode seed when it raised "Firestore operation deadline exceeded", and the supervisor stopped admission at 15:51:07Z, the 600-second startup deadline;
- no Datastore API call was refused with a status the runner does not repeat, as the 400 "Missing routing header" was, so the routing header holds; a refusal the runner repeats, such as 429 or 503, leaves no record; how many Datastore-mode commits landed is not recorded, so whether the database-name condition admits the runner's Datastore calls is still open;
- cleanup deleted both databases, the run reached verified idle and released the lock, and the receipt records the databases `cleaned`.

Under [Firestore conditions](#firestore-conditions) this is the startup budget overrun, a timing finding.
The runner created the databases, read them back and wrote the two seeds one after another: in `fs2085-jm-a1`, the creates and the Native-mode seed alone had taken until about 470 seconds into its window, when its first Datastore-mode commit was refused.

**The repair.**
That condition widens the budget, but the budget cannot widen alone: the rig's timing policy ends the slowest job about 3,100 seconds into the 5,400-second window and gives the oracles 1,200 seconds before the cleanup start at 4,500, so a longer startup also needs a longer window, a new estimate and longer campaign bounds.
The owner chose instead to shorten admission.
[#2135](https://github.com/flink-gcp/flink-connector-gcp/issues/2135) sends the Datastore-mode create right after the Native-mode one, so the two creates run together, then writes the two seeds together, one commit to each database at a time with admission checked before every pair, since each database takes its own ramp of writes.
It also emits `firestore-databases-read-back`, so that with the existing `firestore-databases-admitted` the next run times the creates and readbacks apart from the seeds.
It changes no input, budget, window, oracle or grant, and the application image is unchanged.
Whether the two together fit in the 600 seconds is unmeasured, and a second overrun is read by this condition again, with those events to size the budget.

**The second re-run.**
The owner approves the pilot again, with everything else in this record unchanged:

- inputs: `firestore_trial=jm-replacement`, `run_id=fs2085-jm-a3`, no `campaign` input, `repeats=0`, and the same `application_digest`, dispatched once #2135 has merged;
- then, after it is read as [Before dispatch](#before-dispatch) describes, the campaign as preregistered, from `fs2085-tm-a1`;
- at most one more attempt, so eight in all, `fs2085-jm-a1` and `fs2085-jm-a2` included;
- at most USD 3.07 more (`3.0714259`), so USD 24.57 (`24.5714072`) in all;
- every attempt's window still ends before 2026-11-13T14:41:26Z, and the campaign is still dispatched no later than 2026-11-13T04:00Z.

The reads of [Before dispatch](#before-dispatch) are made again before the second re-run.

**How the second re-run ended.**
`fs2085-jm-a3` ([run 42936263803](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/42936263803)), dispatched at 2026-11-11T01:04Z on `main` at `ccf04a89b4da3ea9de2044aa706c02b55bf9928a`, whose only change since #2136 was the `libraries-bom` update of #2134, outside the rig's paths:

- its window started at 01:05:28Z; the databases read back at 01:09:41Z and were admitted with both seeds at 01:12:28Z, 420 seconds in, so the startup budget held;
- the supervisor's Datastore-mode late-seed commit and `batch_out` query succeeded, so the database-name condition admits those calls;
- the upgrade restored savepoint 15 after 42.3 s and the JobManager failover restored checkpoint 26 after 36.6 s, both proved;
- at 01:41:59Z, in the `finishing` stage after its window had closed, the platform preempted the JobManager Pod, and the run stopped as "Unplanned workload interruption; trial is inconclusive"; the preemptor was most likely a `konnectivity-agent` Pod displaced by the autoscaler's scale-down of a `pool-1` node, though that Pod was gone before its UID could be matched;
- cleanup deleted both databases, the run reached verified idle and released the lock.

Under [Firestore conditions](#firestore-conditions) this is a system Pod preempting a workload Pod after the application exists; the oracles never ran.

**The repair.**
[#2140](https://github.com/flink-gcp/flink-connector-gcp/issues/2140) has the Firestore exercise wait out the platform's interruption of a workload Pod, and what follows it, once both recoveries are proved and the `finishing` window has closed, as ADR-0165's "Unplanned job restarts" describes.
From then on that condition no longer stops a run: an attempt that carries on is accepted as [Acceptance criteria](#acceptance-criteria) describes, and the findings report its causes and extra copies.
Before that point, and for a restart with no platform interruption before it, the conditions read as before.
It changes no input, budget, window, oracle or grant, and the application image is unchanged.

**The third re-run.**
The pilot is repeated once more as a repeat the owner approves by hand, within the eight attempts already approved:

- inputs: `firestore_trial=jm-replacement`, `run_id=fs2085-jm-a4`, no `campaign` input, `repeats=0`, and the same `application_digest`, dispatched once #2140 has merged;
- then, after it is read as [Before dispatch](#before-dispatch) describes, the campaign from `fs2085-tm-a1` with `campaign=rescale-out,rescale-in` and `repeats=1` rather than 2, so that it makes at most four attempts and the total stays at eight;
- no more money: still at most USD 24.57 (`24.5714072`) in all;
- every attempt's window still ends before 2026-11-13T14:41:26Z, and the campaign is still dispatched no later than 2026-11-13T04:00Z.

The reads of [Before dispatch](#before-dispatch) are made again before the third re-run.

**How the trials ended.**
The owner paused the trials on 2026-11-11 after `fs2085-tm-a1`, and a single run under [ADR-0204](../0204-tier3-recovery-trials-tolerate-faults-and-fail-only-on-lost-data.md) replaced them.
Its [findings](0204-firestore-fault-sequence-findings-2147.md) replace the findings file item 4 of [Interpretations the owner must confirm](#interpretations-the-owner-must-confirm) names, list every attempt, and say which promised readings they do not report.
