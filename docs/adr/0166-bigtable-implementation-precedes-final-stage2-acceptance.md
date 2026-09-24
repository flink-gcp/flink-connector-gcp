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

# ADR-0166: Bigtable implementation precedes final Stage 2 acceptance

- Status: Accepted
- Date: 2026-09-11; refined 2026-09-13; revised by [#1319](https://github.com/flink-gcp/flink-connector-gcp/issues/1319) and [#1327](https://github.com/flink-gcp/flink-connector-gcp/issues/1327) (2026-09-14); execution rules refined 2026-09-19; Stage 2 verdict recorded 2026-09-21; mode retained 2026-09-21; drain attributed 2026-09-23; default concurrency measured 2026-09-23; committer limit attributed 2026-09-24
- Issues: [#1211](https://github.com/flink-gcp/flink-connector-gcp/issues/1211), [#1319](https://github.com/flink-gcp/flink-connector-gcp/issues/1319), [#1327](https://github.com/flink-gcp/flink-connector-gcp/issues/1327), [#1454](https://github.com/flink-gcp/flink-connector-gcp/issues/1454), [#1464](https://github.com/flink-gcp/flink-connector-gcp/issues/1464), [#1476](https://github.com/flink-gcp/flink-connector-gcp/issues/1476)
- Supersedes: only the implementation-start ordering in ADR-0104 and ADR-0163 for Bigtable staged writes
- Modules: bigtable
- Current behavior: `docs/content/docs/connectors/delivery-guarantees.md`

## Decision

The owner selected implementation before final evaluation on 2026-09-11 after reviewing the remaining delivery work.
Implement the ADR-0163 runtime, DataStream entry point, Table mapping, operational documentation and production-path local tests together.
The implementation PR may be reviewed and merged before the service evaluation.
Release and supported-workload claims still require production-factory recovery acceptance on both supported Flink lines and the full formal Stage 2 assessment.
Closing #1319 requires the recorded correctness acceptance; the Stage 2 verdict is tracked by #1327.
This accepts the risk that the completed implementation may prove unsuitable for some workloads.

The retained protocol is unchanged: 108 performance cells, three measured repetitions per arm, the preregistered warm-up and observation periods, throughput/p95 thresholds and variability rule, correctness acceptance, hot-row growth and marker storage measurements.
Admission-limited diagnostic rows do not pass this gate.
Complete notification work includes every earlier checkpoint collection drained by that notification; a per-invocation measurement cannot stand in for it.
Do not weaken a failing cell by limiting outstanding work, increasing capacity, repeating it or extending a lease without a new explicit decision.

The separately identified seven-cell diagnostic profile was retired with the admission experiment; its plan and observations remain historical evidence.
The lease rejects new creation and claims for that profile while retaining exact-owned-resource cleanup.
Do not repeat the completed 40-run local matrix solely because production code was added.
Migrate useful correctness oracles to the production path and remove redundant acceptance tests when replaced; retain the formal harness and its ownership, reservations, supervision, stop and exact-resource cleanup checks while they remain needed for final evaluation.
Existing evidence remains historical evidence and must not be relabeled as production acceptance.

## Implementation boundary

Runtime creation revalidates deserialized staged limits and request options, including durations converted to nanoseconds under ADR-0068.
The writer freezes the destination, application profile, user mutation bytes and a fresh 128-bit identity before checkpoint ownership transfers to Flink's committer collector.
The production committer validates restored envelopes and remote marker/profile/schema policy before sending them, including sends during operator initialization.
The existing single-row factory owns the data clients and preserves single-attempt RPC deadlines; a commit failure fails the checkpoint recovery attempt without dropping or replacing its envelopes.
The staged writer owns no service client, and interval admission limits do not claim a global bound on committer heap.

The public selector and staged-options types are `@Experimental` closure stops under ADR-0141/ADR-0124 because the final assessment may constrain this unreleased mode's configuration.
The existing at-least-once builder behavior remains its default.
Table `upsert`, `keep-latest` and `aggregate` can select the new mode; SQL deletes clear declared data families while preserving the marker family and undeclared families.
The marker family must be provisioned independently as raw with no GC rule, and an explicit single-cluster transactional application profile is required.

## Local verification ownership

`BigtableStagedCommitLifecycleTest` now drives the production builder, writer, committer, metadata clients and data SDK against loopback services.
Its existing immutable-identity, partial response-loss, aborted-interval, rescaling and older-restore oracles are migrated rather than duplicated.
`BigtableProductionStagedJobITCase` exercises successful stop-with-savepoint followed by rescaling through the production sink; it replaces the duplicate successful-stop diagnostic test.
`ProductionRecoveryJobITCase` owns automatic response-loss recovery through both production API factories using the same scenario as the [production service lease](evidence/0163-bigtable-production-recovery-service-plan.md), replacing the earlier production DataStream-only automatic-recovery case.
The separate failed-stop diagnostic remains because it explains the excluded recovery boundary.
The standalone state-sizing probe retains its original experimental sink and limits, preserving the meaning of recorded measurements.
The formal performance harness now delegates to the production staged sink, including its writer, committer, serializer and topology validation, as recorded in the [production preparation evidence](evidence/0163-bigtable-production-stage2-preparation.md).
Final evaluation still requires calibration and a reviewed service execution package.

## Service authorization and completion

The existing authorization is at most USD 20 in aggregate, including earlier usage.
An earlier estimate below USD 2 is not confirmed billing or a new allowance.
Before any service creation, reconcile billed usage, a conservative allowance for unbilled usage and outstanding reservations using current prices, and prove room for the proposed reservation.
Freeze reviewed source, classpath, resource identities, exact commands and targets before executing an approved service plan on the local host with a fresh lease and independent supervision.
Stop on disappeared resources, failed, censored or empty observations, or exhausted limits; delete only exactly owned resources and verify their absence.
For the retained-trial Stage 2 campaign, the [2026-09-19 refinement](#lean-execution-and-recorded-failures-2026-09-19) records a failed, censored or empty observation and proceeds to the next preregistered run instead of stopping.
Missing financial evidence blocks charging, not local implementation.

The next completion evidence must come from the implemented mode and both API entry points.
It must establish recovery after ambiguous responses, aggregate contributions, retained-marker behavior, routing rejection and cleanup, then report the formal performance verdict and supported workload boundaries.
Neither local green tests nor a merged implementation PR establishes this acceptance.

## Acceptance tracking refinement (2026-09-13)

The owner requested a separate follow-up for the remaining acceptance after [#1298](https://github.com/flink-gcp/flink-connector-gcp/pull/1298) merged.
The implementation issue #1211 may close once its remaining obligations are transferred to [#1319](https://github.com/flink-gcp/flink-connector-gcp/issues/1319).
That follow-up owns production-service recovery acceptance on both supported Flink lines, native remote transport evidence, production-path integration of the formal harness, the unrestricted Stage 2 assessment and the supported-workload verdict.
The release/support conditions, protocol thresholds and aggregate cost authorization are unchanged.

Reuse matching sink deployment evidence from [#1316](https://github.com/flink-gcp/flink-connector-gcp/issues/1316) with its source, runtime and oracle limitations; GKE is not a prerequisite when the approved local host can establish the required observation.
[#1317](https://github.com/flink-gcp/flink-connector-gcp/issues/1317) covers Change Streams source deployment recovery and does not discharge sink acceptance.
Historical handovers and measurements retain their original issue references; current acceptance is tracked by #1319.

## Scope split and cost reconciliation (2026-09-14)

The owner reconciled the aggregate authorization and split the remaining acceptance into a correctness part and a performance part on 2026-09-14.
The protocol, its 108 cells, three repetitions, thresholds and variability rule are unchanged; only the tracking and the execution environment decision change, so this is a refinement rather than a reversal.

The [reconciliation record](evidence/0163-bigtable-production-recovery-service-plan.md#reconciliation-on-2026-09-14) bounds prior usage under this authorization at USD 2.60 from the project's Bigtable admin audit log and current catalog prices; billing export is not configured, and the owner accepted that bound as the billed-plus-unbilled figure.
No reservation is outstanding.
The correctness scope reserves USD 2 for the production recovery lease and at most USD 5.20 for the native-transport gated class on both Flink lines, four runs in total, every run counted at two clock node-hours; with the first lease attempt's one node-hour recorded in the plan, USD 10.45 of the USD 20 ceiling is committed.

Under #1319 the correctness scope is the [production recovery lease](evidence/0163-bigtable-production-recovery-service-plan.md) through both API entry points on both Flink lines and the [native-transport acceptance](evidence/0163-bigtable-native-transport-acceptance.md) through the connector's own TLS and application-default-credentials branch.
Both passed on 2026-09-14 and are recorded in those evidence files: four lease workers with 128 identities, 388 wire attempts, one discarded response and 260 duplicates each, and the gated class with all six invocations on each Flink line.
That correctness acceptance closes #1319; the Stage 2 verdict under #1327 then declined the mode on 2026-09-21, as the last section of this ADR records.
The native-transport class joins the weekly gated suite; the owner accepted its recurring instance cost on 2026-09-14.

The production-path integration of the timed harness, the unrestricted Stage 2 assessment, the sustained hot-row growth and physical storage measurements and the performance verdict were routed to [#1327](https://github.com/flink-gcp/flink-connector-gcp/issues/1327) on 2026-09-14; the next section records the instrument preparation that landed there.
Two measured facts justify deferring them rather than running them under this authorization.
The [protocol](evidence/0163-bigtable-staged-performance-protocol.md) needs at least 19.2 hours of warm-up and admission before drain, teardown and setup, about 30 node-hours or USD 20 of node cost alone, in more than 30 one-hour leases on a host that must stay up for days.
The execution host used so far is in Japan, and the client p95 of a conditional request from it has measured near 180 ms (a 178 ms upper bound at in-flight 4 on the 2026-09-07 lease; 194 to 206 ms at 1,000 in flight on 2026-09-05) against a server-side p95 under 6 ms, a gap no record yet attributes to network distance or to the client; the only completed pair of the [2026-09-07 lease](evidence/0163-bigtable-stage2-experiment-harness.md) measured the staged visibility p95 at 23.3 seconds, 64.1 times bulk, in one repetition that includes checkpoint waiting and an incomplete bulk warm-up.
That record establishes the measured failure, not its cause; the owner's decision is to run the assessment from compute co-located in `us-central1`, through the Tier-3 rig or a separately approved host, so that network distance is removed as a candidate explanation before the criterion is judged.
Its cost authorization is separate from this one and is recorded in the next section; a change to the latency criterion would still need its own amending ADR.
That verdict was recorded on 2026-09-21 and declined the mode, so release and supported-workload claims stay withheld and user-facing documentation keeps describing the mode as experimental and unsupported.

## Stage 2 instrument preparation (2026-09-14)

The [production preparation record](evidence/0163-bigtable-production-stage2-preparation.md) belongs to the formal assessment in [#1327](https://github.com/flink-gcp/flink-connector-gcp/issues/1327).
For that assessment, the owner selected an additional USD 20 ceiling, separate from the existing #1319 allowance, and minimum spending with the original protocol preserved.
The preferred execution uses one retained Bigtable free trial instance and regular GCE compute in `us-central1`, subject to verified trial eligibility, host calibration and a reviewed lifecycle that owns the retained instance across bounded worker leases.
The current per-instance lease cannot provide that lifecycle; the preparation records no service measurement or support verdict.

## Lean execution and recorded failures (2026-09-19)

The owner decided on 2026-09-19 to run the assessment without the external execution package prepared between 2026-09-14 and 2026-09-19, which had grown an independent owner host, SSH control channel, prepaid transfer ledgers, kernel wire quotas and a per-cell calibration matrix without ever creating a resource.
The protocol, its 108 cells, three repetitions, periods, thresholds and variability rule are unchanged; only execution rules change, so this is a refinement.

- One regular `e2-standard-4` host in `us-central1-b` runs the campaign, with a Compute Engine maximum run duration and `DELETE` termination action as the host deadline; there is no second host.
- The controller is temporary automation kept outside tracked source; it only sequences the merged journal, trial adapter, workers and Monitoring capture, and its source is attached to the assessment record.
- Calibration is the protocol's minimum: one no-service observation per arm against a 100 ms slower control, plus one largest-cell observation to freeze heap and capacity inputs.
- A failed, empty or censored run is recorded as `FAILED` and the campaign proceeds to the next preregistered run without repeating it; the earlier stop-on-first-failure rule would have left most cells unmeasured after a checkpoint-timeout failure. A failure may be recorded after the worker's deadline, because a late checkpoint timeout is the expected failure, but only until the next supervisor heartbeat observes the expired worker; the run reservation (`runOverheadSeconds`) must therefore cover the whole failure path of checkpoint timeout, drain limit and JVM teardown, and the lean campaign reserves 300 seconds per run. The journal counts recorded failures in `failedRuns`, so a completed campaign whose runs all failed is visibly not a measured one. Ownership loss, expired supervision, an interrupted observation or an outcome that cannot be recorded still stops the campaign.
- On a stop, the lean controller's supervisor preserves the free trial instance for the owner's decision instead of deleting it, because the trial cannot be recreated; a completed campaign still deletes it and verifies absence. The tracked `Stage2CampaignSupervisor.cleanup` path, which deletes the instance on every stop, is unchanged and is not used by that controller.
- The owner also decided to judge the result under the unchanged latency criterion and to record the resulting verdict, expected to be a decline for most cells because staged visibility includes checkpoint waiting; a criterion change remains a separate decision with its own amending ADR.

This section describes the execution rules as they stood on 2026-09-19, and the campaign it governed has since run.
The journal, trial adapter, supervisor, workers and Monitoring capture it names were removed on 2026-09-22 under [the retention decision](#the-mode-is-retained-2026-09-21), so the present-tense references above, including `Stage2CampaignSupervisor.cleanup`, describe code that no longer exists.

## Stage 2 verdict (2026-09-21)

The campaign ran on 2026-09-20 and 2026-09-21, completed all 108 cells, and the assessment declines the mode at the ADR-0104 gate.
Its measurements, failure modes and cost are in the [assessment record](evidence/0163-bigtable-stage2-assessment.md); this section records only the decision and what follows from it.

Fifteen of the 108 cells produced three valid repetitions in both arms.
Every one of them fails the visibility criterion, with a staged p95 between 129 and 1,546 times bulk against a constrained-opt-in limit of 4x, so no measured workload is supported.
Throughput is not the reason: it clears the constrained bound in thirteen of those cells, although the protocol's variability rule leaves the throughput comparison itself inconclusive in twelve of the fifteen.
The decline rests on visibility alone, and a margin of two orders of magnitude is not one that within-cell spread can close.
In the remaining 93 cells the staged arm could not produce three valid repetitions: 244 of its 324 runs ended without an observation, 156 of them censored at the staging capacity and 78 at checkpoint expiry, while all 324 bulk runs succeeded under the same ceilings.

Release and supported-workload claims therefore stay withheld, which is the state user-facing documentation already describes; what changes is that the reason is now measured rather than pending.
The correctness acceptance recorded under #1319 is unaffected.

Both auxiliary observations ran.
The sustained phase's marker timecourse failed inside the campaign through a defect in its sampler's client configuration, and was repeated on 2026-09-21 with the repaired sampler: one hot row accumulated 1,445,269 marker cells over thirty minutes, growing linearly with no reclamation during the run.
Nothing of the protocol's coverage is now outstanding.

The matrix ran on a Bigtable free trial and cost nothing in service charges; the assessment record holds the full cost ledger.

## The mode is retained (2026-09-21)

The verdict above left two questions open, and the owner settled them on the day it was recorded.

**The mode stays, as experimental development functionality.**
A declined gate withholds a supported-workload claim; it does not establish that the capability should not exist.
Google's own Bigtable connector offers an exactly-once mode built differently, and a sink that participates in Flink's two-phase commit is what the framework's vocabulary calls exactly-once support.
Removing the selector would withdraw that capability to report a performance result, which is not what the result says.

The at-least-once path already covers the common case: a stable `setCell` timestamp targets the same version, so a replay overwrites rather than accumulates.
Where it does not cover is the reason this mode has a use at all.
A repeated aggregate `AddToCell` contributes again at the same timestamp, and no timestamp choice makes a Sum idempotent, so a replayed input changes the aggregate; the marker the staged mode retains is what suppresses that second contribution.
A workload whose correctness turns on that distinction is the one for which a delayed row beats a doubled one.

**The latency criterion is not amended, but the verdict is read in two parts.**
ADR-0104 compares a candidate against the eager at-least-once sink, so for a checkpoint-owned mode it charges the checkpoint wait by construction: a staged write cannot be visible before its checkpoint completes, and no implementation of this design changes that.
The measured visibility is that wait plus the drain of the conditional commits the completion releases, and the ratio the gate reports combines the two.
Separating them changes what the verdict means.
The campaign's staged p95 of 5.7 to 48.9 seconds, at commit concurrencies of 1 to 16 and checkpoint intervals of one and ten seconds, exceeded the interval by 4.7 to 38.9 seconds; waiting for the checkpoint accounts for at most one interval, so the excess is checkpoint completion and the commit drain rather than the wait, though a difference of percentiles does not measure either one.

The campaign committed with at most 1, 4 or 16 conditional writes in flight, because the matrix passed its in-flight dimension to the production sink as `maxInFlightRequests`, and [#1464](https://github.com/flink-gcp/flink-connector-gcp/issues/1464) then measured the default of 100 in the same zone.
At the default the committer's bound was reached, with up to 100 requests outstanding per committer, and one committer drained about twice what a concurrency of 16 had allowed; the service accepted at least about 12,600 conditional writes per second on distinct rows and was not the limit.
Part of the remaining gap to the at-least-once sink is the design: the staged mode writes each mutation with its own `CheckAndMutateRow`, because the marker check and the write must be atomic and the Bigtable data API has no multi-row conditional write, so every write pays a round trip and the client's per-call processor cost that a `MutateRows` batch amortizes.
[#1476](https://github.com/flink-gcp/flink-connector-gcp/issues/1476) then identified what stops a single committer short of that rate, and found that a third to two-fifths of the per-write processor time #1464 attributed to the staged path was the instrument's own, most of what the staged path cost beyond a bare client.
The committer sends every conditional write from its one committing thread.
In the instance's zone that thread was on a processor for 74 to 85% of its commit time while the host stayed about three-quarters idle, so a committer is bound by what one thread can send through the client; a larger bound added about a fifth, and with a lighter instrument and a bound of 400 one committer drained 11,865 to 12,245 writes per second against 12,328 to 13,062 for a warmed bare client.
Over a long round trip the same committer is bound by the round trip instead, and its drain grew in proportion to `maxInFlightRequests`.
Nearly all of the committer's waiting is at its bound rather than at the end of a batch, and a wait for any completion instead of the oldest was measured and declined: it did not raise the drain and doubled the committing thread's processor time per write, because collecting the oldest first also collects the requests already complete behind it.
The connector's own code on the commit path took about 1% of the task thread's samples, so converting the stored request or resolving destinations differently is not pursued; most of the staged path's allocation comes from Flink serializing each pending committable eight times per checkpoint, which [#1486](https://github.com/flink-gcp/flink-connector-gcp/issues/1486) tracks upstream.
More subtasks raised the drain on the sixteen-processor host and lowered it on the four-processor one.
At the default the staged visibility p95 on distinct rows at a one-second interval was 3.5 to 6.6 seconds with one subtask and 7.2 to 7.6 seconds with four on the four-processor host, and 1.7 to 3.5 seconds on the sixteen-processor host, against 23 to 61 milliseconds for the at-least-once sink; at a ten-second interval a commit could outlast the interval and visibility grew through most runs.
A hot key is bounded by the service instead: one row accepted about 1,600 to 2,200 conditional writes per second at every concurrency from 16 upwards, and the staged hot-key runs at one subtask drained at or just below that plateau.
The service-side results, the node's rate and the single-row plateau, are not explained by the absence of a batched conditional write; the per-write cost on the client is.

The obligation this leaves is documentary.
The pages a user reads must say which part of the cost is the guarantee, which part is the commit drain, and which part is the service's single-row limit.
They must also say what is known about the drain: every staged write is its own conditional request, which costs more per write than a batch, `maxInFlightRequests` bounds how many are outstanding, and one committer is bound by its sending thread near the instance and by the round trip far from it, so more subtasks raise the rate where processors are free and a larger bound helps mainly over a long round trip.

Changing the criterion itself, for example to measure a checkpoint-owned mode against a checkpoint-aligned baseline, would affect the Cloud Tasks mode under ADR-0158 as well, and still needs its own superseding ADR under the rule this ADR already states.

The campaign machinery built for the one-shot run is removed with this decision, because the trial it adopts cannot be created again in this project and a repeat assessment would reimplement its sequencing against whatever it then needs.
The measurement instrument and the integration tests that drive the production staged path through it stay.
