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

# BigQuery deployed recovery trials: preregistration

This is the preregistration for the deployed BigQuery Storage Write trials of [#1312](https://github.com/flink-gcp/flink-connector-gcp/issues/1312), within [ADR-0165](../0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md).
It fixes the campaign, its estimate, the acceptance criteria for the instrument, the stop conditions and the cleanup checks.
Nothing here authorizes a dispatch: the owner approves the campaign's estimate before the first trial, each trial needs the images of [#1425](https://github.com/flink-gcp/flink-connector-gcp/issues/1425), and each dispatch is approved on its own.

## Campaign

| Order | `trial` input | Mode | Destinations | Run ID |
| ---: | --- | --- | ---: | --- |
| 1 | `alo-10` | ALO | 10 | `bq1312-alo-10-a1` |
| 2 | `eo-10` | EO | 10 | `bq1312-eo-10-a1` |
| 3 | `alo-50` | ALO | 50 | `bq1312-alo-50-a1` |
| 4 | `eo-50` | EO | 50 | `bq1312-eo-50-a1` |

One dispatch is one trial: it fixes one delivery method and one destination count, so both methods at both counts need four.
The first is a pilot. The others run only once it has reached a `usable` verdict and verified idle, because a first deployed run is where a contract between the Java application and the Python rig fails if it is going to; the Cloud Tasks calibration's first attempt failed that way on a number both sides had been written to agree on.
Payload, duration, Pods and the query budget are the scenario's and the same for every trial, as the [BigQuery runbook](../../../kubernetes/apps/bigquery/README.md) states them at the dispatched commit.

A repetition is identified by its run ID, and the run ID by its attempt suffix: `-a1` is the first attempt, and a repeat under [Stop conditions](#stop-conditions) takes `-a2`.
Dispatch writes `approval.json` under `runs/<run-id>/` in the evidence bucket once it holds the lock and has rechecked the foundation, and refuses any run ID that already has evidence there, so an admitted attempt burns its ID whether or not it finishes; a dispatch refused before that write ran nothing and leaves its ID free.
Dispatch does not bind a run ID to a trial: the operator types both, and this record's convention is `bq1312-<trial>-a<N>`. Each approval records its run ID beside the trial it ran, so a mispaired dispatch is detectable from its evidence rather than refused, and is listed here as the trial it actually ran.
Every admitted attempt ends with `result.json` beside it, written by finalization in the run workflow or in the recovery workflow that follows a run automatically; its `success` is true only for a `usable` verdict, so an attempt that finished inconclusive records `false` too, and its verdict and reasons say which it was; until the receipt exists the next dispatch refuses to take the lock. This record lists every admitted attempt, finished or not.

## Estimate

Each trial is estimated at USD 2.35 by `bigquery_plan.estimate`, which every rendered proposal also carries; the four come to USD 9.40.
It charges all five running Pods for the whole 90-minute window, all twelve 4 GiB query reservations and USD 1.00 of other incremental cost, so it is an estimate, not a billing cap, and existing cluster standing charges are outside it.
The oracle reads only the `run_id`, `sequence` and `destination` columns and bills far less than its reservation.
A repeat under [Stop conditions](#stop-conditions) adds USD 2.35 each and is approved before its dispatch like any other.

## Campaign window

Every image version becomes eligible for deletion seven days after creation, pinned or not, cleanup deletes it asynchronously, and copying a digest the registry still holds does not renew it, as the [image runbook](../../../kubernetes/images/README.md) records; dispatch refuses an image unless the trial's window ends a day before that eligibility.
A trial pulls three images: `operator`, `lifecycle-tools` for the supervisor and `bigquery-recovery`; the `flink` base is a layer of the application image.
Every version in the repository on 2026-09-23 was created between 2026-09-19T22:56Z and 2026-09-20T06:50Z (UTC; `gcloud` prints local time unless asked), and `operator` is a digest mirror, so the [#1425](https://github.com/flink-gcp/flink-connector-gcp/issues/1425) publication runs after the registry is observed to have deleted the current versions; otherwise the mirror keeps its old creation time and expires mid-campaign.
Each trial's 90-minute window must then end within six days of the oldest creation time among its three images.
The four trials take about eight hours of serial dispatch: each holds the environment lock for its window, cleanup included, and for the workflow's plan proof after it.

## Acceptance criteria for the instrument

Each trial is accepted as a measurement when its final receipt and `just tier3-analyze` over its exported evidence agree on a `usable` verdict: both recoveries proved, the query oracle passed, and the sink's task metrics, the network metrics, the connector's gauges and TaskManager memory each read in the baseline window and again after the second recovery.
Running all four usable is what #1312's first acceptance item asks of both delivery methods, and the same four verdicts carry its observation, recovery and oracle items.
Measured values — throughput, memory, GC, backpressure, active writers, checkpoint duration and state — are findings to publish, not thresholds: #1312 forbids reusing component throughput as a production target, and no number in this document is one.

## Stop conditions

- The pilot is not `usable`: stop the campaign, repair what it exposed, revise this document and approve again before any other trial.
- Any trial does not reach verified idle, a refreshed plan is not empty, or the environment lock is not released: stop, and recover the environment before any further dispatch.
- The oracle reports a routing violation, a duplicate in EO mode, or rows still missing once every query slot or the visibility phase is spent: the trial aborts on its own; this is a connector finding, recorded rather than repeated.
- A trial is `inconclusive` for a reason outside the connector — a Spot TaskManager preempted, a JobManager replaced by the platform: repeat it once as `-a2`; a second such result leaves that cell inconclusive, and the campaign continues without it.
- `just tier3-analyze` reports a trial `unexported`: download the run's whole evidence prefix again and re-analyze, which repairs an incomplete download. If it is still `unexported` and the bucket lacks the run's `result.json`, finalization never ran: recover the environment as above. Any other lasting `unexported` — a missing, malformed or unplaceable record in the bucket itself — is an instrument defect: stop, as for an unusable pilot. Repeating the trial repairs none of these.
- An image reaches its retention margin: stop; the remaining trials need a republication first.

## Cleanup checks

Before the environment lock is released the rig itself refuses to finish unless every table it recorded is confirmed deleted, nothing remains under the run's state prefix, the supervisor Job and ConfigMap are gone, the application namespace holds no workload and nothing outside its baseline but a zero-replica Operator ReplicaSet, the Operator is back at zero replicas with its namespace quotas restored and the three refreshed plans for `flink-gcp`, `tier3-bootstrap` and `tier3-operator` are empty; the final receipt records that verified idle.
The next dispatch also refuses to take the lock while any run record, the lock itself or an approval without its final receipt remains.
After each trial, and before the next is dispatched, these are read directly:

- No table named for the attempt's run ID (`bq_<run_id>_d<n>`, hyphens as underscores) exists in `flink_gcp_tier3_bigquery`: the rig deletes only tables it recorded an intent for, and refuses to provision over one it did not.
- No object of the run remains in `tier3-system`: idle verification inventories the application namespace, but in `tier3-system` it checks only the recorded supervisor objects, the Operator's replicas and the quota, so a non-Pod object the runner did not record would go unseen.
- The `_control/runs/<run-id>.json` record and `_control/environment.json` lock are gone, which confirms that any recovery has finished before the next dispatch is attempted rather than leaving the refusal above to say so.

## Interpretations the owner must confirm

1. The campaign is the four trials above, one attempt each, the first a pilot; variance across repetitions is not claimed.
2. Instrument acceptance is the `usable` verdict and nothing else; a measured value is never a pass or fail criterion.
3. In ALO mode duplicates are reported observations, not failures; in EO mode a duplicate is a defect.
4. The findings, including every inconclusive or aborted attempt, are published in a record beside this one; none is dropped for having failed.
