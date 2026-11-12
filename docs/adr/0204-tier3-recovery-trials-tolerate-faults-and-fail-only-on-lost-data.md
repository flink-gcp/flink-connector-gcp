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
- Date: 2026-11-12
- Issues: [#2143](https://github.com/flink-gcp/flink-connector-gcp/issues/2143), [#2144](https://github.com/flink-gcp/flink-connector-gcp/issues/2144), [#2145](https://github.com/flink-gcp/flink-connector-gcp/issues/2145), [#2146](https://github.com/flink-gcp/flink-connector-gcp/issues/2146)
- Modules: tools/tier3, kubernetes
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

Exactly-once paths run as their own scenario ([#2148](https://github.com/flink-gcp/flink-connector-gcp/issues/2148)), with the same faults and a different sink mode and verdict.
They are BigQuery's `STORAGE_API_EXACTLY_ONCE` and `FILE_LOADS` write methods, the Cloud Tasks sink's `EXACTLY_ONCE` mode and Bigtable's experimental exactly-once sink.
The Spanner and Firestore scenarios write with idempotent operations, so they are judged as at-least-once.

### Faults are injected while data flows, and any number of others are waited out

One run injects each of the five fault kinds once, one after another while the input is flowing: a savepoint upgrade, a JobManager replacement, a TaskManager replacement, a rescale in and a rescale out.
A kind that cannot apply to a scenario is left out, and the scenario's runbook says which and why.
The rig injects them itself because the platform may not interrupt a short run at all; injecting each kind once guarantees the run went through every fault.
Before each fault, and before completion, the rig waits until the job is running again, whatever caused the last restart: running, reconciled to the last change of the application, with a JobManager and the TaskManagers the run's parallelism needs, and logging progress after a restore that followed the fault, since the stopped job may still log for a while.
The input and a batch read's seed are sized to outlast the fault sequence; an input that finishes before the job is running again after the last fault leaves the run unconfirmed.
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
Firestore's `sequence` trial is the first scenario on these rules ([#2146](https://github.com/flink-gcp/flink-connector-gcp/issues/2146)); each other scenario keeps ADR-0165's rules until its own issue moves it, and the single-fault trials, campaigns, estimates and windows are removed once none uses them.
Until then every Firestore proposal, the `sequence` one included, still carries the scenario's estimate, which nothing approves.
Past evidence keeps the interpretation of the revision that produced it.

## Alternatives declined

- **Bounding interruptions, restarts or attempts**: Autopilot's node decisions cannot be predicted, so any count measures the cluster rather than the connector.
- **Approving spend from an estimate**: the number of interruptions, and so the run time, cannot be predicted; a time limit bounds the time-billed resources directly, and small inputs bound the rest.
- **Keeping metric coverage and measurement windows as conditions**: they measure the run, not the data, and once they go, nothing that remains needs a 30-minute input or a 90-minute run.
- **Proving for each fault that it restored a specific state**, as the current recovery proofs do (kept in the [consultation](https://github.com/flink-gcp/flink-connector-gcp/issues/2145#issuecomment-6801486048) on [#2145](https://github.com/flink-gcp/flink-connector-gcp/issues/2145)): a restore that skips records shows up as lost data in the end state, which the verdict checks; a restore from an older state only replays records, which at-least-once allows, so the end state does not reveal it, and that is accepted.
