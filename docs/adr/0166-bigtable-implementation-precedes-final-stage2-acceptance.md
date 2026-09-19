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
- Date: 2026-09-11; refined 2026-09-13; revised by [#1319](https://github.com/flink-gcp/flink-connector-gcp/issues/1319) and [#1327](https://github.com/flink-gcp/flink-connector-gcp/issues/1327) (2026-09-14); execution rules refined 2026-09-19
- Issues: [#1211](https://github.com/flink-gcp/flink-connector-gcp/issues/1211), [#1319](https://github.com/flink-gcp/flink-connector-gcp/issues/1319), [#1327](https://github.com/flink-gcp/flink-connector-gcp/issues/1327)
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
That correctness acceptance closes #1319; release and supported-workload claims still wait for the Stage 2 verdict under #1327.
The native-transport class joins the weekly gated suite; the owner accepted its recurring instance cost on 2026-09-14.

The production-path integration of the timed harness, the unrestricted Stage 2 assessment, the sustained hot-row growth and physical storage measurements and the performance verdict were routed to [#1327](https://github.com/flink-gcp/flink-connector-gcp/issues/1327) on 2026-09-14; the next section records the instrument preparation that landed there.
Two measured facts justify deferring them rather than running them under this authorization.
The [protocol](evidence/0163-bigtable-staged-performance-protocol.md) needs at least 19.2 hours of warm-up and admission before drain, teardown and setup, about 30 node-hours or USD 20 of node cost alone, in more than 30 one-hour leases on a host that must stay up for days.
The execution host used so far is in Japan, and the client p95 of a conditional request from it has measured near 180 ms (a 178 ms upper bound at in-flight 4 on the 2026-09-07 lease; 194 to 206 ms at 1,000 in flight on 2026-09-05) against a server-side p95 under 6 ms, a gap no record yet attributes to network distance or to the client; the only completed pair of the [2026-09-07 lease](evidence/0163-bigtable-stage2-experiment-harness.md) measured the staged visibility p95 at 23.3 seconds, 64.1 times bulk, in one repetition that includes checkpoint waiting and an incomplete bulk warm-up.
That record establishes the measured failure, not its cause; the owner's decision is to run the assessment from compute co-located in `us-central1`, through the Tier-3 rig or a separately approved host, so that network distance is removed as a candidate explanation before the criterion is judged.
Its cost authorization is separate from this one and is recorded in the next section; a change to the latency criterion would still need its own amending ADR.
Release and supported-workload claims continue to require that verdict, and user-facing documentation keeps describing the mode as experimental and not yet supported until it is recorded.

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
