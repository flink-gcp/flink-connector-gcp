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

# ADR-0165: Bigtable implementation precedes final Stage 2 acceptance

- Status: Accepted
- Date: 2026-09-11
- Issue: [#1211](https://github.com/flink-gcp/flink-connector-gcp/issues/1211)
- Supersedes: only the implementation-start ordering in ADR-0104 and ADR-0163 for Bigtable staged writes
- Modules: bigtable
- Current behavior: `docs/content/docs/connectors/delivery-guarantees.md`

## Decision

The owner selected implementation before final evaluation on 2026-09-11 after reviewing the remaining delivery work.
Implement the ADR-0163 runtime, DataStream entry point, Table mapping, operational documentation and production-path local tests together.
The implementation PR may be reviewed and merged before the service evaluation.
Release, supported-workload claims and closing #1211 still require production-factory recovery acceptance on both supported Flink lines and the full formal Stage 2 assessment.
This accepts the risk that the completed implementation may prove unsuitable for some workloads.

The retained protocol is unchanged: 108 performance cells, three measured repetitions per arm, the preregistered warm-up and observation periods, throughput/p95 thresholds and variability rule, correctness acceptance, hot-row growth and marker storage measurements.
Admission-limited diagnostic rows do not pass this gate.
Complete notification work includes every earlier checkpoint collection drained by that notification; a per-invocation measurement cannot stand in for it.
Do not weaken a failing cell by limiting outstanding work, increasing capacity, repeating it or extending a lease without a new explicit decision.

The separately identified seven-cell diagnostic profile is optional diagnostic work, not an implementation prerequisite.
Run it only to answer a concrete unresolved question under its own committed plan and lease prerequisites.
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
The formal performance harness still needs production-path integration and a reviewed service manifest before final evaluation.

## Service authorization and completion

The existing authorization is at most USD 20 in aggregate, including earlier usage.
An earlier estimate below USD 2 is not confirmed billing or a new allowance.
Before any service creation, reconcile billed usage, a conservative allowance for unbilled usage and outstanding reservations using current prices, and prove room for the proposed reservation.
Freeze reviewed source, classpath, resource identities, exact commands and targets before executing an approved service plan on the local host with a fresh lease and independent supervision.
Stop on disappeared resources, failed, censored or empty observations, or exhausted limits; delete only exactly owned resources and verify their absence.
Missing financial evidence blocks charging, not local implementation.

The next completion evidence must come from the implemented mode and both API entry points.
It must establish recovery after ambiguous responses, aggregate contributions, retained-marker behavior, routing rejection and cleanup, then report the formal performance verdict and supported workload boundaries.
Neither local green tests nor a Ready implementation PR alone closes #1211.
