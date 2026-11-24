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

# ADR-0204: Tier-3 recovery trials tolerate faults and fail only on lost or corrupted data

- Status: Accepted
- Date: 2026-11-12; revised by [#2279](https://github.com/flink-gcp/flink-connector-gcp/issues/2279) (2026-11-23: an unjudged relay with a failure timing cannot explain has not passed) and [#2321](https://github.com/flink-gcp/flink-connector-gcp/issues/2321) (2026-11-24: the Bigtable relay counts a commit after the end too)
- Issues: [#2143](https://github.com/flink-gcp/flink-connector-gcp/issues/2143), [#2144](https://github.com/flink-gcp/flink-connector-gcp/issues/2144), [#2145](https://github.com/flink-gcp/flink-connector-gcp/issues/2145), [#2146](https://github.com/flink-gcp/flink-connector-gcp/issues/2146), [#2148](https://github.com/flink-gcp/flink-connector-gcp/issues/2148), [#2150](https://github.com/flink-gcp/flink-connector-gcp/issues/2150), [#2151](https://github.com/flink-gcp/flink-connector-gcp/issues/2151), [#2152](https://github.com/flink-gcp/flink-connector-gcp/issues/2152), [#2153](https://github.com/flink-gcp/flink-connector-gcp/issues/2153), [#2154](https://github.com/flink-gcp/flink-connector-gcp/issues/2154), [#2277](https://github.com/flink-gcp/flink-connector-gcp/issues/2277), [#2278](https://github.com/flink-gcp/flink-connector-gcp/issues/2278), [#2279](https://github.com/flink-gcp/flink-connector-gcp/issues/2279), [#2321](https://github.com/flink-gcp/flink-connector-gcp/issues/2321)
- Modules: tools/tier3, kubernetes
- Evidence: [Firestore fault sequence findings](evidence/0204-firestore-fault-sequence-findings-2147.md), [Spanner fault sequence findings](evidence/0204-spanner-fault-sequence-findings-2154.md), [Bigtable fault sequence findings](evidence/0204-bigtable-fault-sequence-findings-2153.md), [BigQuery fault sequence findings](evidence/0204-bigquery-fault-sequence-findings-2150.md), [Pub/Sub fault sequence findings](evidence/0204-pubsub-fault-sequence-findings-2151.md), [Cloud Tasks fault sequence findings](evidence/0204-cloudtasks-fault-sequence-findings-2152.md), [BigQuery exactly-once fault sequences findings](evidence/0204-bigquery-exactly-once-fault-sequence-findings-2148.md)
- Partially supersedes: [ADR-0165](0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md)'s rules for ending an attempt, its measurement windows and verdicts, one fault per trial, its input sizes, and spend approval from an estimate

## Context

Tier-3 runs each connector's Flink job on a real GKE Autopilot cluster, injects faults on purpose and then reads back what the job wrote.
Its purpose is to show that the connector keeps working through faults on real Kubernetes without losing data.
It is neither a load test nor a test of long-running behavior.

Under [ADR-0165](0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md) the rig grew into a measurement harness instead.
An attempt ended on any unplanned interruption, except a platform interruption after a Firestore run's last window ([#2140](https://github.com/flink-gcp/flink-connector-gcp/issues/2140)), although faults are what the trials are built from.
Every verdict required each metric family to be read in each measurement window, and the BigQuery, Cloud Tasks, Bigtable, Spanner and Firestore trials ran 30 minutes of input in a 90-minute run, with three 5-minute windows fitted inside the input.
Each of those trials combined a savepoint upgrade with one other fault, so their five fault kinds took four runs, each with its own setup.
Spend was approved per dispatch, for a run or a campaign of runs, from an estimate.

The [inventory](https://github.com/flink-gcp/flink-connector-gcp/issues/2144#issuecomment-6801324565) of [#2144](https://github.com/flink-gcp/flink-connector-gcp/issues/2144) measured the result.
Of 87 admitted attempts, 41 were accepted.
Of the 46 that were not, 24 failed on a rig defect, 16 on a platform event, 1 on a measurement gap and 4 on other causes.
One failed on a connector defect, which restarted the job from its retained checkpoint.

Autopilot removes and replaces Pods according to its own node decisions, and when and how often it does so cannot be predicted.
A rule that counts interruptions, restarts or attempts therefore measures the cluster, not the connector.
What can be checked is the end state: whether the data the job was given is all there.

## Decision

### The verdict is the data the job wrote

Each run writes to destinations only it uses.
Once the job has completed, the run reads them back, and the trial passes when they hold what they should:

- for an at-least-once path, every expected record with its expected value; duplicates are allowed and reported, and counts need not match;
- for an exactly-once path, exactly one committed effect per expected record.

A wrong value for an expected record fails the trial.
So does a record whose key is not in the expected set, since nothing but the job writes there; a duplicate of an expected key is not such a record.
Data missing once the job has completed fails the trial too.
Nothing else decides the verdict: the number of interruptions, restarts, recoveries or attempts, recovery timings and metrics are not conditions.

The job has **completed** when it has consumed all of its input and committed it: a bounded job reaches `FINISHED`, and a streaming source stops when its input ends, the job finishes, and a checkpoint or final commit has completed.
Completion is read from the job, never from the destinations, so missing data is a failure and not an unfinished run.

A source that cannot end, as the Pub/Sub source cannot, never lets its job finish, so its completion is read from the job another way ([#2151](https://github.com/flink-gcp/flink-connector-gcp/issues/2151)).
Once the job runs again after the last fault, it has completed when every expected record is in its destination, or when the service's longest acknowledgement deadline has passed since the later of the job's latest restore and its latest progress, and a checkpoint triggered after that has completed.
The input is through by then, and every lease the stopped attempts held has lapsed, so its message has been delivered again or is lost: a redelivery the job processes logs progress, which moves the wait on.
The destination only ends the wait early when nothing is missing, so missing data is still a failure and not an unfinished run.

Exactly-once paths run as their own trials ([#2148](https://github.com/flink-gcp/flink-connector-gcp/issues/2148)), with the same faults and a different sink mode and verdict.
They are BigQuery's `STORAGE_API_EXACTLY_ONCE` and `FILE_LOADS` write methods, one trial each, `eo-10-sequence` and `fl-10-uploader-sequence`.
Two exactly-once sinks are judged by the exactly-once rule in their own scenario's run instead.
Bigtable's experimental exactly-once sink runs beside the at-least-once sink in one job, and its Change Streams relay reads the exactly-once table ([#2153](https://github.com/flink-gcp/flink-connector-gcp/issues/2153)).
The Cloud Tasks sink's `EXACTLY_ONCE` mode is the only sink its application writes through, and the listing that judges it passes only on exactly one task per record, carrying that record's body ([#2152](https://github.com/flink-gcp/flink-connector-gcp/issues/2152)).
The Spanner and Firestore scenarios write with idempotent operations, so they are judged as at-least-once.

### Faults are injected while data flows, and any number of others are waited out

One run injects each of the five fault kinds once, one after another while the input is flowing: a savepoint upgrade, a JobManager replacement, a TaskManager replacement, a rescale in and a rescale out.
A kind that cannot apply to a scenario is left out, and the scenario's runbook says which and why.
The rig injects them itself because the platform may not interrupt a short run at all; injecting each kind once guarantees the run went through every fault.
No fault is aimed at the commit of BigQuery's, Bigtable's or Cloud Tasks' exactly-once sink: each lands wherever the commits happen to be ([#2148](https://github.com/flink-gcp/flink-connector-gcp/issues/2148)), and a run's findings report which commit state each restore met where the evidence shows it.
Before each fault, and before completion, the rig waits until the job is running again, whatever caused the last restart: running, reconciled to the last change of the application, with a JobManager and the TaskManagers the run's parallelism needs, and logging progress after a restore that followed the fault, since the stopped job may still log for a while.
The input and a batch read's seed are sized to outlast the fault sequence; an input that finishes before the job is running again after the last fault leaves the run unconfirmed.
A change-stream source stops at an end the plan fixes from the window's start, and the job cannot finish before it, so that end leaves the input room to finish and its last commit room to be relayed; a run whose input finished too close to the end cannot judge its relay, which no commit after the end reaches, so it is not usable and can run again as it is.
A restore after the input's latest progress also leaves the relay unjudged: the restored state may commit writes it carried without logging progress, and the restore's recorded time precedes those commits by as long as its tasks take to deploy, so it bounds nothing.
Timing explains only a relay's missing rows, so an unjudged relay that also counts a failure timing cannot explain, such as a row outside the input or a commit after the end, has not passed, and its run does not run again as it is ([#2279](https://github.com/flink-gcp/flink-connector-gcp/issues/2279), [#2321](https://github.com/flink-gcp/flink-connector-gcp/issues/2321)).
The run records which faults it injected and when.

Everything else is waited out at any stage and recorded, however often it happens: a preemption, a Spot reclamation, a node shutdown, an eviction, or a Pod, container or Flink job restart, whatever its cause.
The job's restart strategy is unbounded for the same reason.
Transient failures of the Kubernetes, REST or storage APIs are retried.

### What stops a run

A run stops early only to stay safe or bounded, and each stop has one meaning:

- a check that it is acting on its own resources with the approved manifest fails: abnormal, a rig or manifest defect;
- the job reaches a terminal `FAILED` state: abnormal, a possible connector defect;
- it reaches its time limit: abnormal;
- it is cancelled: unconfirmed.

A run stopped before completion has no verdict.
An abnormal run is investigated, and its cause recorded on the connector's Tier-3 issue, before that scenario runs again; an unconfirmed run can run again as it is.
Cleanup and its verification are unchanged.

### Runs are short, and time bounds them

A run should take 20 to 30 minutes from admission to its verdict: setup, a few minutes of input, the fault sequence, and the read-back.
Inputs and seeds last only as long as the fault sequence needs, not 30 minutes.

Pods and database instances are billed for the time they run, and an unpredictable number of interruptions makes a spend estimate meaningless.
Each run therefore has a time limit of one hour from admission to its verdict, after which it stops, cleans up and is recorded as abnormal; cleanup runs after the limit and is not counted.
A run that should take half an hour and is still going after an hour is not riding out interruptions; something is wrong.
Within the limit the job may be interrupted any number of times.
Charges per operation or per byte, such as Firestore reads and writes or BigQuery queries, grow with the data and with replays rather than with time; the small inputs keep them small.
Spend is no longer estimated or approved per dispatch.

## Consequences

Attempts are no longer spent on events the trials are meant to survive, and one run covers every fault kind.
Tier-3 no longer reports how fast a connector recovers or what its metrics did, and a regression that only slows recovery or adds restarts goes unnoticed by its verdict.
The restarts a run records still show where a connector failed and recovered, as the Cloud Tasks committer defects did ([#1794](https://github.com/flink-gcp/flink-connector-gcp/issues/1794), [#1796](https://github.com/flink-gcp/flink-connector-gcp/issues/1796)), and the findings report them.
Firestore's `sequence` trial is the first scenario on these rules ([#2146](https://github.com/flink-gcp/flink-connector-gcp/issues/2146)), Spanner's the second ([#2154](https://github.com/flink-gcp/flink-connector-gcp/issues/2154)), Bigtable's the third ([#2153](https://github.com/flink-gcp/flink-connector-gcp/issues/2153)), BigQuery's `alo-10-sequence` trial the fourth ([#2150](https://github.com/flink-gcp/flink-connector-gcp/issues/2150)), Pub/Sub's `sequence` trial the fifth ([#2151](https://github.com/flink-gcp/flink-connector-gcp/issues/2151)) and Cloud Tasks' the sixth ([#2152](https://github.com/flink-gcp/flink-connector-gcp/issues/2152)), and BigQuery's exactly-once trials joined its at-least-once one ([#2148](https://github.com/flink-gcp/flink-connector-gcp/issues/2148)).
With every scenario moved, no dispatch runs a single-fault trial or a campaign, and no proposal carries an estimate, nor does the smoke run, whose estimate shared that code ([#2277](https://github.com/flink-gcp/flink-connector-gcp/issues/2277)); the single-fault exercise, its windows and its verdicts are removed ([#2278](https://github.com/flink-gcp/flink-connector-gcp/issues/2278)).
The rework's cleanup is complete: every scenario's exercise builds on the one fault-sequence exercise, decides its verdict through one skeleton with its own reasons, and is analysed along one path, the trial fields only the single-fault trials varied are gone, and the query slots and package defaults sized for them now fit the sequence ([#2279](https://github.com/flink-gcp/flink-connector-gcp/issues/2279)).
The analysis reports an archived single-fault run whose trial names the single-fault kind it ran, or for BigQuery a trial without a record count that names the single-fault fault it injected or no fault, without a verdict, as `single-fault-trial`, to be analysed at the revision its approval names; any other trial, such as the first Cloud Tasks run's, which names no kind, is reported as malformed, and its run has no verdict either.
Past evidence keeps the interpretation of the revision that produced it.

## Alternatives declined

- **Bounding interruptions, restarts or attempts**: Autopilot's node decisions cannot be predicted, so any count measures the cluster rather than the connector.
- **Approving spend from an estimate**: the number of interruptions, and so the run time, cannot be predicted; a time limit bounds the time-billed resources directly, and small inputs bound the rest.
- **Keeping metric coverage and measurement windows as conditions**: they measure the run, not the data, and once they go, nothing that remains needs a 30-minute input or a 90-minute run.
- **Proving for each fault that it restored a specific state**, as the single-fault exercise's recovery proofs did (kept in the [consultation](https://github.com/flink-gcp/flink-connector-gcp/issues/2145#issuecomment-6801486048) on [#2145](https://github.com/flink-gcp/flink-connector-gcp/issues/2145)): a restore that skips records shows up as lost data in the end state, which the verdict checks; a restore from an older state only replays records, which at-least-once allows, so the end state does not reveal it, and that is accepted.
- **Aiming a fault at an exactly-once sink's commit** ([#2148](https://github.com/flink-gcp/flink-connector-gcp/issues/2148)): every commit that BigQuery's, Bigtable's and Cloud Tasks' exactly-once trials judge is idempotent per unit, so a restore during one leaves a mix of units committed before it and units the restored job commits, two states that a restore before or after the commit reaches too, and a loss or duplicate on either path is in the end state the verdict reads.
  A BigQuery buffered-stream commit flushes each stream that received rows since the last checkpoint to its offset; a repeated flush to the same offset succeeds, as the restores after both deletions in the [exactly-once runs](evidence/0204-bigquery-exactly-once-fault-sequence-findings-2148.md#where-the-faults-landed-against-the-commits) measured, and the committer also takes an `ALREADY_EXISTS` answer as already flushed; streams the writer abandons are never finalized, so a restored commit can still flush them.
  Every job of a FILE_LOADS commit has an id derived from its destination, files and checkpoint, and a restored commit re-attaches to it, awaiting one still running and resubmitting under a derived retry id only one that failed; two ADR-0165 trials interrupted such a commit on GKE, [#1313](evidence/0165-bigquery-fileloads-findings-1313.md) just after its loads ended and [#1685](evidence/0165-bigquery-fileloads-inflight-findings-1685.md) while they ran, and both found every row exactly once with no load submitted twice, although in both the loads had finished by the time the restored commit re-attached, so neither awaited a running one.
  Bigtable's staged commit writes each row's contribution and marker in one conditional write.
  The Cloud Tasks staged commit creates each task under a name derived from its record's key and takes an `ALREADY_EXISTS` answer as already created; the effects [ADR-0158](0158-cloud-tasks-checkpointed-creation-stages-named-tasks-and-commits-after-the-checkpoint.md) leaves unbounded need a name's protection to lapse or a task to be dispatched or removed, and the Cloud Tasks trial's queue stays paused, keeps names for an hour, and has no task removed before the verdict, so a create repeated after one the service accepted is answered `ALREADY_EXISTS` however a fault is timed, and one never accepted is left to the restored commit, subject to ADR-0158's authorization deadline, which bounds every restored envelope rather than a commit and under the default policy fails the commit instead of sending an expired one; a commit stopped part-way is exercised directly by [ADR-0158's recovery acceptance](evidence/0158-cloudtasks-recovery-1245.md), whose partial-commit cases cancel the job after a create whose response they withhold and restore it, also at another committer parallelism, and passed on both Flink lines.
  Aiming at a buffered-stream flush, which follows a checkpoint only for streams that received rows, would need a delay hook in the application.
