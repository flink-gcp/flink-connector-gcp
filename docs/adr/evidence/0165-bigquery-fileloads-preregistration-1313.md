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

# BigQuery FILE_LOADS deployed recovery trial: preregistration

This is the preregistration for the deployed FILE_LOADS trial of [#1552](https://github.com/flink-gcp/flink-connector-gcp/issues/1552), the correctness half of [#1313](https://github.com/flink-gcp/flink-connector-gcp/issues/1313), within [ADR-0165](../0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md).
It fixes the trial, its estimate, the acceptance criteria, the stop conditions and the cleanup checks.
Nothing here authorizes a dispatch: the owner approves the estimate before the first attempt, and each attempt is approved on its own.

## Trial

| `trial` input | Mode | Destinations | Staging format | Sink inputs | Run ID |
| --- | --- | ---: | --- | --- | --- |
| `fl-10` | FILE_LOADS | 10 | Avro | the connector's defaults | `bq1313-fl-10-a1` |

The trial asks whether a FILE_LOADS checkpoint commit survives the deployed boundary: separate JobManager and TaskManager processes, a committer vertex apart from the writers whose one active subtask shares a TaskManager Pod with one writer and loads the other writer's files across Pods, load jobs that outlive the Pod that submitted them, staging in Cloud Storage under GKE Workload Identity, and Operator-managed recovery.
It runs the exercise the Storage Write trials ran: a savepoint upgrade, then a JobManager failover, then visibility.
It does not ask how finalization scales; finalization concurrency, staging-file size and the Avro-versus-Parquet comparison belong to [#1553](https://github.com/flink-gcp/flink-connector-gcp/issues/1553) and [#1554](https://github.com/flink-gcp/flink-connector-gcp/issues/1554).

The input is the scenario's 30 minutes at 1 MiB/s: 1,843,200 rows of 1,024 serialized bytes, checkpointed every 120 seconds.
The five sink inputs are the connector's defaults as the [BigQuery runbook](../../../kubernetes/apps/bigquery/README.md#trial-inputs) lists them, so a commit stages about 120 MiB before conversion, about 6 MiB per destination from each of the two writers: its files stay below the 16 MiB rollover and its load jobs far below the 256 MiB boundary the performance half measures.
Payload, duration, Pods and the query budget are otherwise the scenario's, as the runbook states them at the dispatched commit.

One trial is the campaign: it is one cell, and variance across repetitions is not claimed.
A repetition takes the next attempt suffix, `-a2` and so on, under the rules of the [Storage Write preregistration](0165-bigquery-trial-preregistration-1312.md) of [#1312](https://github.com/flink-gcp/flink-connector-gcp/issues/1312) for burned run IDs, final receipts and mispaired dispatches; the run ID prefix is `bq1313-` because the attempt belongs to the FILE_LOADS scenario's record.
Every admitted attempt is listed in the findings, finished or not.

## Estimate

Each attempt is estimated at USD 2.35 by `bigquery_plan.estimate`, the figure every rendered proposal carries.
It charges all five running Pods for the whole 90-minute window (about USD 1.06), all twelve 4 GiB query reservations at USD 6.25/TiB (about USD 0.29) and USD 1.00 of other incremental cost, so it is an estimate, not a billing cap, and existing cluster standing charges are outside it.

`bigquery_plan.estimate` does not price what FILE_LOADS adds; it is expected to fit the reserve with a wide margin.
[Batch loading through the shared slot pool is free](https://cloud.google.com/bigquery/pricing#data-ingestion-pricing): at most one load job per destination per checkpoint, up to about two hundred in all.
[Staging](https://cloud.google.com/storage/pricing) writes about the trial's 1,800 MiB of input, converted to Avro, to the regional Standard bucket `flink-gcp-tier3-bigquery` (`us-central1`), and holds each file only until the commit that loads it, or for the bucket's one-day lifecycle rule if a failure abandons it, so storage costs well under one cent; some hundreds of staged files at a few Class A requests each come to at most a few thousand operations at USD 0.005 per 1,000, under two cents.
The pricing pages were read on 2026-10-10.
A repeat adds USD 2.35 and is approved before its dispatch like any other.

## Campaign window

Every image the trial pulls — `operator`, `lifecycle-tools` for the supervisor and `bigquery-recovery` — comes from the [FILE_LOADS trial publication](../../../kubernetes/images/README.md#bigquery-file_loads-trial-publication), which ran when the registry held no version at all, so the `operator` mirror got a fresh creation time without a manual deletion.
The oldest of the three is `operator`, created 2026-10-09T16:22:01Z (UTC; `gcloud` prints local time unless asked), and dispatch refuses an image unless the run's window ends a day before its seven-day deletion eligibility, so every attempt's window, which ends 90 minutes after admission, must end before 2026-10-15T16:22:01Z.
An attempt whose window would end later needs a new publication and a reviewed change adopting its `lifecycle-tools` digest in `pins.cue`; since copying a digest the registry still holds keeps its creation time, that publication waits for the registry to delete the current versions or for them to be deleted by hand.
The `bigquery-recovery` image was built from main `ef183e0c6`, which carries the FILE_LOADS mode of [#1549](https://github.com/flink-gcp/flink-connector-gcp/issues/1549).

## Before dispatch

Each attempt is dispatched only after these are read, in this order, immediately before it:

- The OpenTofu apply of the merged change that grants `bigquery.jobs.listAll` succeeded, and the `flink-gcp`, `tier3-bootstrap` and `tier3-operator` plans refreshed after it are empty.
- The custom role `tier3BigQuerySupervisorJobs` reads back with both `bigquery.jobs.list` and `bigquery.jobs.listAll`, and the project policy binds it to the `tier3-supervisor` service account: a missing `listAll` fails the run closed, but a missing binding leaves the supervisor listing only its own jobs, which nothing in the run would notice.
- The environment lock `_control/environment.json` and every `_control/runs/` record are absent, and every earlier `approval.json` has its `result.json`.
- The window, admission plus 90 minutes, ends before the deadline under [Campaign window](#campaign-window).

## Acceptance criteria

The trial is accepted as a measurement when its final receipt and `just tier3-analyze` over its exported evidence agree on a `usable` verdict.
For this mode that verdict requires both recoveries proved, the query oracle passed with zero duplicate, missing and invalid rows as in EO mode, and every family the Storage Write trials read — the sink's task metrics, the network metrics, the connector's gauges and TaskManager memory — plus the committer's metrics and the per-subtask checkpoint statistics, each read in the baseline window and again after the second recovery.
The oracle's exact row count is its report in the receipt's `recovery.outcomes.query`.

The issue asks for more than the verdict reads, for an attempt whose application reached `FINISHED`; these come from the receipt's `bigquery.file_loads` record and the [Cleanup checks](#cleanup-checks):

- No connector-issued job left unfinished when cleanup listed them, and no temporary table.
- The staging prefix's leftovers, as cleanup recorded them, accounted for. A recorded `unreadable` listing accounts for nothing. The connector documents that a file finalized for a checkpoint a failure abandoned stays behind for the bucket's lifecycle rule, and this trial's JobManager failover can abandon one checkpoint, so such leftovers are published, not counted against the trial. One checkpoint is in flight at a time, and at this trial's sizes it stages one file per destination per writer, so an abandoned one leaves at most 20, which is also how many names cleanup records. More than 20 leftovers, or a recorded name that a load job which reached `DONE` lists among its sources, since a completed commit deletes what it loaded, is a defect.
- The [Cleanup checks](#cleanup-checks) passing.

Measured values — the writer subtasks' end-to-end checkpoint duration and its share beyond the synchronous and asynchronous parts, which is where finalization shows on Flink 2.2.1, the committer's start delay and `lastCommitDurationMillis`, the load jobs submitted, throughput, memory and backpressure — are findings to publish, not thresholds.

## Stop conditions

The verdict is `usable` or `inconclusive`, and an attempt that stops early is `inconclusive` too, so the cause decides what follows: it is read from the `cleanup-start` reason in the run's events, the `bigquery-unfinished-connector-jobs` event, the query slot records under `runs/<run-id>/bigquery/queries/` and the receipt.
The first of the conditions below that matches decides what happens to the trial; the environment conditions after them apply in addition, whichever matched.

- The supervisor is preempted before application admission, as kube-dns did to `bq1312-alo-10-a2`: the attempt spent no BigQuery query and burned only its run ID; redispatch it as the next attempt once approved.
- The run fails with "A listed BigQuery job has no readable id": the supervisor's listing returned a job id that is not one, which is what a listing without `bigquery.jobs.listAll` returns for another principal's job. Until [#1552](https://github.com/flink-gcp/flink-connector-gcp/issues/1552) granted `listAll` and failed such an id closed, a supervisor holding `bigquery.jobs.list` alone would have passed over the workload's own jobs as foreign. Seen now, it means `listAll` is missing: the oracle and every cleanup pass raise it, so the control record and the environment lock stay until the grant is restored or the environment is recovered by hand, while the run's tables expire at the window's end and its state prefix under the bucket's one-day rule. Stop, restore the grant and recover the environment before another attempt.
- The oracle reports a routing violation, a duplicate, or rows still missing once every query slot or the visibility phase is spent, or finds a connector job still running after the application finished: a connector finding, recorded rather than repeated.
- Cleanup of an attempt whose application reached `FINISHED` records an unfinished job, a temporary table, or a staged file a `DONE` load job lists: a connector finding, recorded rather than repeated, even when the verdict is `usable`. An attempt stopped before `FINISHED` is torn down mid-commit, so its leftovers are expected and not findings.
- The supervisor's audit refuses the run's state prefix for its 1 GiB or 10,000-object ceiling, which staged files count against: stop; whether the connector or the ceiling is wrong is the owner's call before another attempt.
- The attempt is `inconclusive` for a reason outside the connector — a Spot TaskManager preempted, a JobManager replaced by the platform: repeat it once; a second such result leaves the trial inconclusive.
- Any other `inconclusive` attempt: stop, repair what it exposed in its own change, and approve again before another attempt.

The environment conditions, in addition:

- The attempt does not reach verified idle, a refreshed plan is not empty, or the environment lock is not released: stop, and recover the environment before any further dispatch.
- `just tier3-analyze` reports the attempt `unexported`, or an image reaches its retention margin: as the Storage Write record says; repeating the attempt repairs neither.

Dispatch outside the weekly E2E workflow's run, which starts Saturdays at 01:17 UTC and submits FILE_LOADS jobs under the same `flink-bq-` id shape in the same project: the supervisor attributes jobs by their sources and destinations, so they are not the run's, but a foreign one whose read fails counts as unfinished and holds the oracle and cleanup until it finishes, and they make the direct job read below harder to read.

## Cleanup checks

The rig refuses to finish until verified idle, as the Storage Write record describes; for FILE_LOADS its cleanup also waits for every connector-issued job to be `DONE` before deleting a table, deletes any temporary table, and records the staged objects before state cleanup deletes the run's prefixes.
After the attempt these are read directly, with the owner's credentials, which hold `bigquery.jobs.listAll`:

- No table named for the attempt's run ID (`bq_<run_id>_d<n>`, hyphens as underscores) and no `tmp_` table exists in `flink_gcp_tier3_bigquery`.
- No job created between the approval's `started_at` and the end of cleanup that loads from `gs://flink-gcp-tier3-bigquery/runs/<run-id>/staging/` or writes one of the run's tables is pending or running.
- No object remains under `gs://flink-gcp-tier3-bigquery/runs/<run-id>/` or under `.inprogress/flink-gcp-tier3-bigquery/runs/<run-id>/` in the same bucket.
- No object of the run remains in `tier3-system`, and the `_control/runs/<run-id>.json` record and `_control/environment.json` lock are gone.

## Interpretations the owner must confirm

1. The campaign is one trial, `fl-10`, at one attempt; variance across repetitions is not claimed.
2. Instrument acceptance is the `usable` verdict; the job, temporary-table and staging records are acceptance items of the issue that the verdict does not read, and a staged file left by an abandoned checkpoint is published rather than counted against the trial.
3. A duplicate is a defect, as in EO mode.
4. The findings, including every inconclusive or failed attempt, are published in a record beside this one, `0165-bigquery-fileloads-findings-1313.md`.
