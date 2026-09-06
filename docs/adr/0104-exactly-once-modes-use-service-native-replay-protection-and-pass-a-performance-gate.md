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

# ADR-0104: Exactly-once modes use service-native replay protection and pass a performance gate

- Status: Accepted
- Date: 2026-08-13; revised by [#596](https://github.com/flink-gcp/flink-connector-gcp/issues/596) (2026-08-14)
  and by [#1208](https://github.com/flink-gcp/flink-connector-gcp/issues/1208),
  [#1210](https://github.com/flink-gcp/flink-connector-gcp/issues/1210), and
  [#1211](https://github.com/flink-gcp/flink-connector-gcp/issues/1211) (2026-09-05), and
  [#1239](https://github.com/flink-gcp/flink-connector-gcp/issues/1239) (2026-09-06)
- Issues: [#591](https://github.com/flink-gcp/flink-connector-gcp/issues/591),
  [#596](https://github.com/flink-gcp/flink-connector-gcp/issues/596),
  [#1208](https://github.com/flink-gcp/flink-connector-gcp/issues/1208),
  [#1210](https://github.com/flink-gcp/flink-connector-gcp/issues/1210),
  [#1211](https://github.com/flink-gcp/flink-connector-gcp/issues/1211),
  [#1239](https://github.com/flink-gcp/flink-connector-gcp/issues/1239),
  [#1241](https://github.com/flink-gcp/flink-connector-gcp/issues/1241)
- Modules: bigquery, pubsub, cloudtasks, bigtable, spanner
- Current behavior: `docs/content/docs/connectors/delivery-guarantees.md`

[ADR-0154](0154-support-follows-published-google-cloud-specifications.md) supersedes only the blanket Cloud Tasks G0 stop and its mandatory investigation prerequisites.
The original stronger recovery claim remains unproved; every other decision, correctness requirement, measurement and performance gate in this record remains in force.
[ADR-0158](0158-cloud-tasks-checkpointed-creation-stages-named-tasks-and-commits-after-the-checkpoint.md) supersedes only the Cloud Tasks decision below: it defines the checkpointed-creation mode [#1240](https://github.com/flink-gcp/flink-connector-gcp/issues/1240) was asked for, scoped to published service semantics, and leaves this record's evidence and performance gates in force.

## Context

Only the BigQuery sink implements Flink committer and writer-state interfaces.
Its exactly-once Storage Write API method persists each active destination's BUFFERED stream and
next explicit append offset, emits a committable at a checkpoint, and calls `FlushRows` after
checkpoint completion.
This differs deliberately from BigQuery's COMMITTED-stream example because rows appended before a
Flink checkpoint must remain invisible if Flink restores an earlier checkpoint.
ADR-0022 records that stream lifecycle.

The Pub/Sub, Cloud Tasks, Bigtable, and Spanner sinks are stateless writers.
They flush outstanding requests before a checkpoint barrier, which makes a completed checkpoint a
durability boundary but does not stop a restored job from submitting the same record again.
Adding a Flink committer around those eager APIs would not retract an effect already visible at the
service.

The destination services expose different replay primitives and therefore cannot inherit one
meaningful two-phase-commit abstraction:

- Pub/Sub assigns the published message ID and documents exactly-once delivery for pull
  subscriptions, not publisher request deduplication.
- Cloud Tasks accepts a caller-chosen task ID and rejects a recently used ID with `ALREADY_EXISTS`,
  but task handlers still execute at least once.
- Bigtable can conditionally mutate one row atomically when an application profile routes to one
  cluster and explicitly enables single-row transactions.
- Spanner `BatchWrite` has no replay protection, while a short read-write transaction can atomically
  bind a ledger marker to effects in the same database.

Correctness alone is insufficient for a public mode.
Conditional reads, task-name lookups, and transactional ledgers can reduce throughput or increase
tail latency enough to make a nominal guarantee impractical.
Issue #591 therefore preregistered correctness, performance, and variability gates before any
real-service measurement.

## Decision

**Do not add a common two-phase-commit layer to the non-BigQuery sinks.**
A new exactly-once or effectively-once mode is eligible only when the destination has a replay-safe
primitive that atomically binds a stable application event identity to the protected effect.
The mechanism may be an eager idempotent or transactional write; it does not have to be a Flink
two-phase commit.

**BigQuery remains the only connector with a supported exactly-once sink mode.**
A committer-based Bigtable mode — a connector-specific committer whose pre-commit never reaches
the target table, not the common layer declined above — is planned under
[#1211](https://github.com/flink-gcp/flink-connector-gcp/issues/1211) and will be settled by its
own ADR.
Cloud Tasks checkpointed creation, proposed under [#1238](https://github.com/flink-gcp/flink-connector-gcp/issues/1238), is defined by ADR-0158 and requires an applicable primitive performance pass before its runtime work proceeds.
The [#1241 repeat](evidence/0104-cloudtasks-stage1-1241.md#result-on-2026-09-06) was inconclusive and did not meet that prerequisite.
Other non-BigQuery exactly-once implementations or additional performance stages require a concrete non-idempotent user requirement that the existing write shapes cannot satisfy.

The connector documentation distinguishes four boundaries:

- checkpoint durability;
- at-least-once submission;
- an idempotent or bounded effectively-once destination effect; and
- an exactly-once service write, which still does not establish end-to-end exactly-once processing.

The connector-specific decisions are:

- **BigQuery remains the only checkpoint-coordinated exactly-once sink.**
  Its buffered-stream implementation uses the same offset replay protection as the official
  committed-stream example but adds checkpoint-controlled visibility.
- **Pub/Sub publisher-side exactly-once is declined as a connector-only feature.**
  A producer event attribute can support downstream deduplication, but the topic can still contain
  duplicate physical messages.
- **Cloud Tasks retains bounded effectively-once task creation only.**
  DataStream `taskIdExtractor(...)` and Table API `task-id` metadata supply the stable identity,
  and `ALREADY_EXISTS` is successful creation within the service's name-retention window.
  A repeated ID neither compares nor updates the existing task.
  The handler remains at least once, and no broader guarantee is claimed.
  ADR-0158 defines the checkpointed-creation mode proposed in [#1238](https://github.com/flink-gcp/flink-connector-gcp/issues/1238) within Google's published specification; the tests it owes validate the connector protocol within that scope.
  The [#1239](https://github.com/flink-gcp/flink-connector-gcp/issues/1239) analysis below identifies the limits of a stronger recovery claim; it does not require individual vendor confirmation before work on documented semantics can proceed.
  ADR-0154 records this distinction.
- **The Bigtable same-row conditional write passed Stage 1 on 2026-09-05, and the eager marker
  mode built on it is not a supported connector mode.**
  Every protected mutation and its marker must share a row, the application profile must use
  single-cluster routing with transactional writes enabled, and marker retention must exceed the
  replay horizon; multi-row effects are outside this candidate.
  The eager mode is not built: the direction chosen on
  [#1211](https://github.com/flink-gcp/flink-connector-gcp/issues/1211) is a mode that rides
  Flink's two-phase commit and uses the same conditional write as its commit path, and that
  issue's design ADR settles the identity the commit binds, the cost it pays, and whether the
  exactly-once name is reserved for such modes (evidence below).
- **Only the Spanner 100-record ledger-transaction shape remains correctness-feasible, but its
  performance result is inconclusive and it is not a supported connector mode.**
  The ledger and effects must share a database and a short read-write transaction.
  The connector must not keep a transaction open across a Flink checkpoint.

The stronger Bigtable and Spanner candidates address narrower non-idempotent effects than ordinary
idempotent keyed mutations.
Spanner's individual mutation replay safety does not promise ordering between same-key
`BatchWrite` mutation groups.
Spanner's schema, retention, batching, and failure-policy costs are not justified without a
concrete requirement that the current mutation upserts cannot meet; Bigtable's are weighed by
[#1211](https://github.com/flink-gcp/flink-connector-gcp/issues/1211)'s design ADR.
Cloud Tasks already exposes its useful replay primitive through both connector APIs, while Pub/Sub
exposes no publisher-side replay primitive to add.

If such a requirement reopens the Spanner candidate, it must first repeat Stage 1 with evenly distributed keys.
For the Cloud Tasks proposal in [#1238](https://github.com/flink-gcp/flink-connector-gcp/issues/1238), the approved repeat in [#1241](https://github.com/flink-gcp/flink-connector-gcp/issues/1241) stopped without the required repetitions and supplied no performance pass.
Passing Stage 1 permits Stage 2 measurement, not implementation.
Stage 2 would require separate resource and cost approval and would cover 64 KiB payloads, hot
keys, concurrency and Flink parallelism 1, 4, and 16, and checkpoint intervals of 1, 10, and 60
seconds.
For Bigtable, which passed Stage 1 on 2026-09-05 (evidence below), that measurement runs inside
[#1211](https://github.com/flink-gcp/flink-connector-gcp/issues/1211)'s design work under its own
resource and cost approval rather than as a standalone stage, and the list above binds it.
Any implementation that passed those gates would require a connector-specific ADR to settle
identity, schema or marker ownership, retention, failure routing, recovery tests, and operational
limits.

The preregistered support thresholds remain the decision rule:

| Outcome | Throughput against baseline | Candidate p95 against baseline |
|---|---:|---:|
| General support | at least 70% | no more than 2x |
| Constrained opt-in | at least 25% | no more than 4x |
| Decline | below 25% | or above 4x |

Correctness is a prerequisite.
Each cell has a warm-up and three measured repetitions, and a throughput range greater than 10% of
the mean is inconclusive.

## Evidence

### Implementation and contract audit

The audit used `libraries-bom` 26.85.1 and the connector sources on 2026-08-13.
Only the two BigQuery exactly-once methods create committers or persist sink writer state.
The other writers wait for acknowledged requests in `flush()` and persist no sink state.

The service contracts support the boundaries above:

- [BigQuery's Storage Write API guide](https://cloud.google.com/bigquery/docs/write-api-streaming)
  uses explicit offsets for exactly-once appends and demonstrates a COMMITTED stream.
- [Pub/Sub exactly-once delivery](https://cloud.google.com/pubsub/docs/exactly-once-delivery) is a
  pull-subscription feature and warns that multiple publisher calls can still produce multiple
  messages.
- [Cloud Tasks task creation](https://cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues.tasks/create)
  documents caller-specified ID deduplication, `ALREADY_EXISTS`, retention of recently used names,
  and increased lookup latency.
- [Bigtable writes](https://cloud.google.com/bigtable/docs/writes#conditional) and
  [routing](https://cloud.google.com/bigtable/docs/routing#single-row-transactions) constrain
  conditional atomicity to one row and single-cluster transactional routing.
- [Spanner batch write](https://cloud.google.com/spanner/docs/batch-write) explicitly has no replay
  protection, while [read-write transactions](https://cloud.google.com/spanner/docs/transactions)
  atomically apply reads and writes and can abort when left idle.

Local correctness probes on 2026-08-13 submitted each candidate twice.
The Bigtable emulator observed one same-row effect and one marker after a conditional replay.
The Spanner emulator observed one non-idempotent increment and one ledger row after transactional
replay.
The existing Cloud Tasks integration case ran five task-creation tests, including deterministic-ID
replay and `ALREADY_EXISTS` normalization.

### Stage 1 real-service measurement

Stage 1 intended to use 1 KiB evenly distributed records and bounded client concurrency.
The three services ran sequentially, every billable target was deleted after its run, and each
target was reported absent after cleanup.
These are service-client measurements, not end-to-end Flink measurements.

- Bigtable used one SSD node in `us-central1-b`, one single-cluster transactional application
  profile, concurrency 1,000, 10-second warm-ups, and three 30-second repetitions per arm.
- Spanner used a Standard regional `us-central1` instance with 100 processing units, concurrency
  16, 10-second warm-ups, and three 30-second repetitions per arm.
- Cloud Tasks used one paused `us-central1` queue, concurrency 1,000, 5,000-task warm-ups, and three
  50,000-task repetitions per arm.

| Service and arm | Throughput repetitions | p95 latency repetitions | Result |
|---|---|---|---|
| Bigtable bulk baseline | 4,931 / 4,788 / 4,875 ops/s | 233 / 246 / 224 ms | Baseline |
| Bigtable same-row conditional marker | 5,736 / 5,825 / 5,865 ops/s | 203 / 182 / 178 ms | Ratios met the general thresholds; formal result inconclusive |
| Spanner `BatchWrite(100)` baseline | 8,896 / 8,703 / 8,651 records/s | 201 / 207 / 210 ms | Baseline |
| Spanner ledger transaction, 1 record | 47.7 / 48.9 / 49.0 records/s | 347 / 343 / 342 ms | Declined |
| Spanner ledger transaction, 10 records | 439 / 471 / 440 records/s | 478 / 484 / 485 ms | Declined |
| Spanner ledger transaction, 100 records | 3,826 / 3,840 / 4,029 records/s | 731 / 656 / 539 ms | Ratios met the constrained thresholds; formal result inconclusive |
| Cloud Tasks unnamed baseline | 383.8 / 391.8 / 404.5 tasks/s | 3,288 / 2,849 / 2,692 ms | Baseline |
| Cloud Tasks deterministic task ID | 402.0 / 389.9 / 360.2 tasks/s | 2,765 / 2,987 / 5,217 ms | Inconclusive: 10.9% throughput range |

Evidence review found that the Bigtable and Spanner harness used unique but lexicographically
increasing keys rather than the preregistered evenly distributed keys.
Their ratios are observations, not formal gate passes.
Both Stage 1 results were inconclusive and required a compliant repeat before Stage 2.
No repeat ran on 2026-08-13 because it required new resource and cost approval; Bigtable's
repeat is recorded below.

The Bigtable candidate throughput range was 2.2% of its mean, and the baseline range was 2.9%.
The 10% replay arms measured 5,808--5,857 ops/s and preserved one effect and marker per event.
A concurrency-one control measured 6.0 ops/s and demonstrated that the harness detected a known
throughput regression.

The Spanner 100-record candidate throughput range was 5.2% of its mean.
Its 10% replay arm measured 4,554 records/s and preserved one effect and ledger row per event.
The serialized control measured 292 records/s.

The Cloud Tasks candidate averaged 97.6% of baseline throughput and 1.24x baseline p95, but its
10.9% throughput range exceeded the preregistered limit.
The result is inconclusive rather than a pass.
Its 10% replay arm measured 409 tasks/s, and duplicate names returned `ALREADY_EXISTS` without a
second task creation.
The paused queue dispatched no handlers, and the serialized control measured 4.5 tasks/s.
No extra repetition ran because it would have exceeded the approved operation count and cost
estimate.

### Stage 1 repeat for Cloud Tasks (2026-09-06)

The approved [#1241 repeat](evidence/0104-cloudtasks-stage1-1241.md) preregistered separate one-channel and eight-channel comparisons for SHA-256 and 128-bit random task names.
It stopped during the first hash-replay warm-up after two concurrent submissions returned successful responses with the same expected task name, violating the one-success/one-collision oracle.
Only the first one-channel random, hash and unnamed measurements completed; every comparison is inconclusive because the required repetitions and controls are missing, and each completed measured arm also recorded post-window admission-limiter waiting.
The one-channel queue was deleted and separately verified absent; the eight-channel queue was never created.
The linked record preserves the raw observations and their limits: two successful responses do not establish two persisted tasks or Flink recovery behavior.

### Stage 1 repeat for Bigtable with evenly distributed keys (2026-09-05)

[#1208](https://github.com/flink-gcp/flink-connector-gcp/issues/1208) repeated the Bigtable
Stage 1 on 2026-09-05 with resource approval recorded on the issue before creation.
The preregistered arms, payload, concurrency, warm-up, repetitions, replay share, and serialized
control were those of 2026-08-13; two inputs moved, and two checks were added.
The key scheme: both arms used a 64-bit hash prefix (`splitmix64` of a per-arm salt and the
logical index, 16 hex digits) followed by the logical index, deterministic per logical index so
the replay arm targets the same row.
The client library: it followed the repository pin, `libraries-bom` 26.87.0 where the 2026-08-13
run had 26.85.1, and the effect of that move was not measured separately.
The first added check ran at the start of every benchmark run, and once before the instance
existed: it bucketed 100,000 generated keys by first hex digit, and the hashed scheme's max/min
bucket ratios were 1.049, 1.064, and 1.050 against a limit of 1.2, while the 2026-08-13 increasing
scheme put all 100,000 keys in one bucket and failed the same check, so the check can fail.
The second added check is the 1,000-row read-back described below.
The run used the 2026-08-13 configuration above and an application profile created with
single-cluster routing and transactional writes enabled, plus one repeat inside the same instance,
on tables already holding the first run's rows, because the first run exceeded the variability
limit.

| Run and arm | Throughput repetitions | p95 latency repetitions | Mean and range |
|---|---|---|---|
| Run 1, bulk baseline | 4,795 / 4,623 / 4,418 ops/s | 257 / 268 / 303 ms | 4,612 ops/s, range 8.2% |
| Run 1, same-row conditional marker | 5,563 / 5,589 / 4,074 ops/s | 207 / 195 / 368 ms | 5,075 ops/s, range 29.8% |
| Run 1, conditional marker with 10% replay | 5,615 / 4,830 / 3,742 ops/s | 201 / 267 / 453 ms | 4,729 ops/s, range 39.6% |
| Repeat, bulk baseline | 4,677 / 4,415 / 4,727 ops/s | 263 / 287 / 243 ms | 4,606 ops/s, range 6.8% |
| Repeat, same-row conditional marker | 5,593 / 4,121 / 4,162 ops/s | 197 / 317 / 354 ms | 4,625 ops/s, range 31.8% |
| Repeat, conditional marker with 10% replay | 4,789 / 3,758 / 3,600 ops/s | 266 / 369 / 493 ms | 4,049 ops/s, range 29.4% |

The candidate measured 110.0% of baseline throughput at 0.93x baseline p95 in run 1 and 100.4% at
1.09x in the repeat, inside the general-support thresholds both times.
The baseline arm stayed within the 10% variability limit in both runs; the candidate arms did
not, so the formal result is inconclusive twice and the one authorized repeat is consumed.
The serialized control measured 5.9 ops/s in both runs.

Correctness held in both runs: the single-row check applied on the first attempt and observed
the marker on the second, and a 1,000-row sample of rows the replay arm submitted twice read back
exactly one payload cell and one marker cell each, in a family with no garbage-collection rule
where a duplicate write would have been a second version.

The variation has a shape, though two runs of three repetitions cannot establish it.
In both runs the conditional path measured about 5,600 ops/s in its first repetition and about
4,100 ops/s in a later one; the bulk path declined the same way in run 1 at lower amplitude, 4,795
to 4,418 ops/s, and not in the repeat.
The repeat's first repetition, on a candidate table that already held more than 800,000 rows from
run 1, measured 5,593 ops/s, which cumulative table growth alone does not explain; no client-side
or service-side metric was collected that would name a cause, and neither random variation nor a
table-state effect is excluded.
Each repetition submitted between 109,000 and 170,000 operations.
The instance lived 13 minutes and was reported absent by the harness and by
`gcloud bigtable instances list` after deletion; the harness source is attached to the issue.

### Amended Stage 1 for Bigtable with JVM-isolated repetitions (2026-09-05)

[#1210](https://github.com/flink-gcp/flink-connector-gcp/issues/1210) ran on 2026-09-05 the
amended protocol the [#1208](https://github.com/flink-gcp/flink-connector-gcp/issues/1208)
revision of this ADR required — the repeat's within-run decline instrumented — preregistered with
resource approval on the issue before creation.
It kept the 2026-08-13 arms, payload, concurrency, warm-up, repetitions, replay share, and
serialized control and the repeat's evenly distributed keys.
It added every arm repetition in its own JVM after a 30-second idle, the JVM's garbage-collection
time and heap recorded per repetition, and a read-only Cloud Monitoring query over the run window.
One SSD node, `libraries-bom` 26.87.0, an application profile with single-cluster routing and
transactional writes enabled from creation, and fresh keys in every JVM.

| Arm | Throughput repetitions | p95 latency repetitions | Mean and range |
|---|---|---|---|
| Bulk baseline | 3,778 / 3,792 / 3,807 ops/s | 316 / 308 / 292 ms | 3,792 ops/s, range 0.8% |
| Same-row conditional marker | 5,566 / 5,513 / 5,608 ops/s | 195 / 206 / 194 ms | 5,562 ops/s, range 1.7% |
| Conditional marker with 10% replay | 5,631 / 5,623 / 5,496 ops/s | 192 / 194 / 214 ms | 5,583 ops/s, range 2.4% |

The candidate measured 146.7% of baseline throughput at 0.65x baseline p95, every arm's range is
within the 10% limit, and the serialized control measured 5.6 ops/s, so the conditional write
passed Stage 1 under the preregistered rule with general-support ratios.
The 1,000-row read-back of rows the replay arm submitted twice found no duplicate cell.

The instrumentation places the limiting latency on the client side of the service: the single
node's CPU load peaked at 0.20 to 0.24, server-side p95 latency per minute stayed at 3.5 to 5.8 ms
for `CheckAndMutateRow` and 7.9 ms per `MutateRows` request, and client-observed p95 was 198 and
306 ms.
With 1,000 requests in flight, throughput was of the order of in-flight divided by
client-observed latency — 5,051 and 3,268 ops/s at p95 against 5,562 and 3,792 measured, with mean
latency not recorded — and the
[#1208](https://github.com/flink-gcp/flink-connector-gcp/issues/1208) decline did not recur once
each repetition ran in its own process; the mechanism inside the client was not instrumented.
The bulk path measured 3.8k ops/s here against 4.6k to 4.8k on
[#1208](https://github.com/flink-gcp/flink-connector-gcp/issues/1208)'s instance, a gap this run
did not measure, while the conditional path was unchanged at 5.5k to 5.6k; the 146.7% ratio
therefore compares the two paths on this instance under this protocol and is not a cross-run
figure.

An aggregate-counter probe outside the gate answered the open capability question: the service
accepted `AddToCell` on an `int64Sum` family inside `CheckAndMutateRow`'s conditional branch, and
with ten submissions per row of which one re-sent the previous event, all 1,000 rows read back
the sum and marker count of the nine distinct events, so a marker-guarded conditional mutation
absorbs a replayed increment.
The first read-back expected ten and reported a failure; the expectation was corrected to the
nine distinct events the replay scheme delivers and the probe re-run on fresh rows with the same
values, both outputs on the issue.
The instance lived 34 minutes and was reported absent by the harness and by
`gcloud bigtable instances list` after deletion; the harness, driver, and raw output are attached
to the issue.

### Cloud Tasks recovery feasibility gate (2026-09-06)

The investigation in [#1239](https://github.com/flink-gcp/flink-connector-gcp/issues/1239) evaluated the stronger recovery scope proposed in [#1238](https://github.com/flink-gcp/flink-connector-gcp/issues/1238).
Its initial no-go required retention and last-effect bounds across all proposed lifecycle and failure histories before dependent work could proceed.
The superseded reopening condition required evidence resolving those contracts, finite E and S, and positive `H - E - S` before the dependent protocol and performance work could advance.
The [original G0 decision and unexecuted probe plan](https://github.com/flink-gcp/flink-connector-gcp/blob/89b6a72e2ac5215659dabe2817120ce3d9513649/docs/adr/0104-exactly-once-modes-use-service-native-replay-protection-and-pass-a-performance-gate.md#cloud-tasks-recovery-feasibility-gate-2026-09-06) preserve the full matrix, prerequisites and reopening questions at the revision this decision replaces.
[ADR-0154](0154-support-follows-published-google-cloud-specifications.md) clarifies that published Google specifications are the service assumptions the connector may rely on.
The analysis does not establish the original stronger claim, but individual vendor confirmation and coverage of undocumented service behavior are not prerequisites for implementing a mode within published semantics.
The protocol work in [#1240](https://github.com/flink-gcp/flink-connector-gcp/issues/1240) selected and defined that supported scope in ADR-0158; the performance repeat in [#1241](https://github.com/flink-gcp/flink-connector-gcp/issues/1241) retains this ADR's measurement gates.
Neither is held for a private service guarantee.
No checkpointed Cloud Tasks mode is enabled by this clarification, and the recovery window ADR-0158 derives is configured per deployment, not a numerical default of this record.

The audit was based on documentation and local source inspection.
No real-GCP retention probe ran, no resources were created or changed, and no vendor inquiry was sent.
The long-lived service-probe proposal in the original revision remained unexecuted; it is not a required acceptance suite for relying on documented retention.

#### Evidence boundary and SDK access

The local inspection used repository commit `c34c9d1dd8f3e2d8680b983068e772f10a31507e` and its effective Cloud Tasks module POM, generated offline with Maven's `help:effective-pom` goal.
It resolved `libraries-bom` 26.87.0 to `google-cloud-tasks`, `proto-google-cloud-tasks-v2`, and `proto-google-cloud-tasks-v2beta3` 2.96.0, with gax/gax-grpc 2.84.0, grpc-api/core/netty-shaded 1.82.4, and protobuf-java 4.33.6.
Source inspection used those artifacts' source jars, rather than the independently moving `latest` Java reference.
The public proto links below pin googleapis/googleapis commit `64aa30b277168edd20efee0c9ceb4ca01248931d`; REST, release-note and gRPC pages were read on 2026-09-06.
Where they disagree, this record retains the disagreement instead of choosing the longest duration.

| Surface | Evidence available locally or publicly | Implementation boundary |
|---|---|---|
| v2beta3 administrative client | The 2.96.0 client contains `CloudTasksClient.getQueue(GetQueueRequest)` and `updateQueue(UpdateQueueRequest)`; the matching proto exposes `Queue.task_ttl` and `tombstone_ttl` as duration fields, with builder setters and presence accessors. | Successful service readback and enforcement are not implied by Java method availability. |
| `GetQueue` | [The method][ct-g0-get] requires `cloudtasks.queues.get`. An omitted read mask returns all fields except statistics; an explicit proto mask can request `name,state,task_ttl,tombstone_ttl,purge_time`. | A present duration describes current configuration, not the history of existing task names. An absent field is not a measured default. |
| `UpdateQueue` | The [2021-01-14 release note][ct-g0-release] explicitly made both TTL fields configurable. The [request][ct-g0-beta-rpc] accepts an update mask; an empty mask updates all fields. [The method][ct-g0-update] can create a missing queue. | A design must use the documented configuration semantics and state its supported lifecycle; it must not infer historical TTL changes from current readback. The connector must not use update as a prerequisite check. |
| v2 `CreateTask` | [The request][ct-g0-v2-rpc] contains the parent, task and response view. The current adapter invokes `createTaskCallable().futureCall(request)`. | There is no absolute create-expiry or queue-generation precondition in that request. `Task.dispatch_deadline` bounds handler dispatch, not task creation. |
| Client timing | In 2.96.0 `CloudTasksStubSettings`, `createTaskSettings` selects the empty `no_retry_1_codes` set and `no_retry_1_params`, whose initial/max RPC timeout and total timeout are 20 seconds. | An empty gax retry set does not establish a service-side effect bound or rule out gRPC transparent retry. It also does not replace the sink's own retry loop. |

#### Published retention and lifecycle scope

The [v2beta3 Queue reference][ct-g0-beta-resource] specifies that a task's tombstone is retained after deletion or execution for the configured `tombstoneTtl`, preventing creation of another task with the same name until expiry.
The field's range is one hour to nine days, with a one-hour default for Cloud Tasks-created queues.
This is published service behavior on which an applicable connector design may rely; it does not need a separate vendor answer or a measurement of every removal path to become usable.
The separate `taskTtl` field governs how long the task itself may live, not how long a deleted name remains reserved.

The [v2 creation reference][ct-g0-create] describes name release as up to 24 hours, or up to nine days for queues created from `queue.yaml` or `queue.xml`.
The pinned [v2 proto][ct-g0-v2-rpc] instead says approximately one hour or nine days respectively.
These estimates must not be converted into an exact minimum for an unconfigured queue or substituted for the applicable configured field contract.
The API version, queue origin and configuration used by a proposed mode must be explicit.

A stable retention configuration established before the first task is a possible supported scope.
A design relying on it may exclude retention changes, purge, queue deletion/recreation or legacy administration during the relevant task and recovery lifetime, rather than promise behavior across those histories.
Such exclusions concern the proposed recovery guarantee; they do not change which destinations today's sink accepts.
[Queue deletion][ct-g0-delete] documents a queue-name reuse restriction, not preservation of task tombstones across queue generations.
Current `GetQueue` readback describes configuration and does not establish the history of already-used task names.
These are boundaries for the connector's claim, not a requirement to investigate every administrative operation before implementing documented task creation.

The exact documented collision outcome for a live or remembered task name is `google.rpc.Code.ALREADY_EXISTS` (numeric code 6).
No different outcome was observed, because no service calls were made.
`NOT_FOUND`, `FAILED_PRECONDITION`, `DEADLINE_EXCEEDED`, `UNAVAILABLE` and `UNKNOWN` cannot be promoted to prior-creation success from the cited contracts.
Nor does an absent `GetTask` result show that a task never existed or never ran.

#### Recovery inequality and the stale-request counterexample

The following is a sufficient-condition analysis for the original stronger proposal, not an additional promise about Cloud Tasks.
A later protocol may select a narrower supported scope, but must still handle failures allowed within that scope correctly.

For one immutable task identity, let `t0` be its persisted deadline origin, established before any create, and let `t` be its last local deadline authorization check.
All incarnations must preserve `t0` and the same queue and task bytes.
Let **H** be the minimum tombstone lifetime, in real elapsed time, over every permitted removal path and administrative history; **E** the maximum underestimation of elapsed time relevant to that lifetime; **S** the maximum real time from that last check to the last possible create effect; and **R** the authorized replay budget.
If measured age at the check is at most `R`, the last effect must occur by `t0 + R + E + S`.
For a task first created at `c` and removed at `d`, `t0 <= c <= d`, so a valid minimum H protects its name at least until `d + H >= t0 + H`.
Consequently, `R + E + S < H` is a sufficient timing condition for no same-identity recreation, conditional on live-name exclusion, exclusive ownership of that task identity and the stable queue/retention lifecycle described above.
Equality leaves no strict margin and is not accepted.

Clock uncertainty must be derived across the actual machines and recovery interval.
Write an endpoint wall-clock reading as real time plus clock error `e` and timestamp representation error `q`.
The measured age is `(t - t0) + e_check - e_origin + q_check - q_origin`, so its underestimation is `e_origin - e_check + q_origin - q_check`.
Absolute endpoint error bounds give the conservative sum `b_origin + b_check + q_bound_origin + q_bound_check`, but only if those bounds cover drift, clock steps, suspension and restart for the whole deployment.
Twice one clock bound is a special case requiring equal established bounds, not an assumption.
A process-local monotonic clock cannot be compared across restarts or hosts, and a [new task][ct-g0-v2-task]'s output-only, second-truncated `create_time` neither preserves `t0` nor authorizes a retry that must be safe before creation.
No deployment clock bound was supplied or measured, so this investigation assigns no numerical E.

Even assuming `E = 0` and a perfectly enforced three-day H, a local deadline check alone admits this trace:

1. An old incarnation passes its final check for the persisted identity, then suspends before issuing its request.
2. A replacement restores the completed checkpoint, creates that identity within R, and the task is executed and deleted.
3. After its tombstone expires, the old incarnation resumes and issues the already-authorized request with a fresh relative RPC timeout. The name is available again.

This is a counterexample to that client-only protocol, not a real-GCP observation.
Installing an absolute local RPC deadline before the pause closes some pre-dispatch cases, but does not supply a server effect bound for an already-sent or buffered request.
In the pinned grpc-core source, `ClientCallImpl.startInternal` checks its effective deadline and `AbstractClientStream.setDeadline` encodes remaining time in the timeout header; these are client transport mechanisms, not an atomic Cloud Tasks create-expiry check.
[gRPC deadlines][ct-g0-deadlines] and [cancellation][ct-g0-cancel] leave stopping application work to the server application.
[Transparent retry][ct-g0-retry] can occur even without an explicit retry policy; this does not imply duplicate service handling, but its reconnect and scheduling delay belongs in S.
The inspected Cloud Tasks request and method contracts provide neither a server-enforced absolute creation expiry nor a finite bound from client cancellation to the last possible create effect.
Thus this analysis supplies no finite S for the original failure and suspension scope.
A connector must not turn its local timeout into a guarantee that creation never occurred; narrowing a mode's scope also requires checking that its implementation respects the resulting boundary.

#### Design and validation within the published specification

The next protocol decision must state the supported API and queue configuration, identity ownership, checkpoint lifecycle and recovery conditions.
It must describe how the implementation preserves task identity and bytes, interprets ambiguous results, handles expiry and retains unresolved state on terminal failure.
If a promised property depends on behavior that Google does not specify, narrow or decline that property instead of making an individual vendor inquiry a prerequisite for the whole feature.
An exclusion cannot be used to dismiss a connector bug or a failure that is possible within the stated supported scope.

Tests must cover request construction and status handling, immutable replay identity, checkpoint/restore paths and boundary failures under the selected semantics.
A fake client must represent ambiguous outcomes, including creation followed by a lost acknowledgement, rather than model every timeout as non-creation.
Real-service acceptance can check integration and detect discrepancies with published behavior; successful observations do not extend the service specification.
Neither exhaustive administrative-history probes nor a private confirmation of published tombstone behavior are required.
Any real-GCP run still needs its own resource, cost and cleanup approval.
The [unexecuted probe plan](https://github.com/flink-gcp/flink-connector-gcp/blob/89b6a72e2ac5215659dabe2817120ce3d9513649/docs/adr/0104-exactly-once-modes-use-service-native-replay-protection-and-pass-a-performance-gate.md#preregistered-service-probes) used one-hour controls and a three-day candidate setting; these are not connector recovery defaults.

The existing stateless writer continues to provide bounded named-task deduplication.
Correctness and performance review of a future checkpointed mode remain necessary; this documentation clarification neither implements the mode nor establishes the original proposal's numerical recovery bound.

[ct-g0-beta-resource]: https://docs.cloud.google.com/tasks/docs/reference/rest/v2beta3/projects.locations.queues
[ct-g0-get]: https://docs.cloud.google.com/tasks/docs/reference/rest/v2beta3/projects.locations.queues/get
[ct-g0-update]: https://docs.cloud.google.com/tasks/docs/reference/rest/v2beta3/projects.locations.queues/patch
[ct-g0-release]: https://docs.cloud.google.com/tasks/docs/release-notes#January_14_2021
[ct-g0-beta-rpc]: https://github.com/googleapis/googleapis/blob/64aa30b277168edd20efee0c9ceb4ca01248931d/google/cloud/tasks/v2beta3/cloudtasks.proto
[ct-g0-v2-rpc]: https://github.com/googleapis/googleapis/blob/64aa30b277168edd20efee0c9ceb4ca01248931d/google/cloud/tasks/v2/cloudtasks.proto
[ct-g0-v2-task]: https://github.com/googleapis/googleapis/blob/64aa30b277168edd20efee0c9ceb4ca01248931d/google/cloud/tasks/v2/task.proto
[ct-g0-create]: https://docs.cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues.tasks/create
[ct-g0-delete]: https://docs.cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues/delete
[ct-g0-deadlines]: https://grpc.io/docs/guides/deadlines/
[ct-g0-cancel]: https://grpc.io/docs/guides/cancellation/
[ct-g0-retry]: https://grpc.io/docs/guides/retry/

## Alternatives declined

- **Add `SupportsCommitter` to every sink** — a committer around an eager API cannot hide or
  retract a publish, task creation, row mutation, or batch write that the service has already made
  visible; [#1211](https://github.com/flink-gcp/flink-connector-gcp/issues/1211)'s committer defers
  the Bigtable write until commit and is not this alternative.
- **Call producer event IDs Pub/Sub exactly-once** — downstream deduplication can protect a consumer
  effect, but does not prevent duplicate messages in the topic.
- **Keep a Spanner transaction open from pre-commit to checkpoint completion** — the source offset
  is not part of that transaction, Flink checkpoints can outlive the transaction, and Spanner can
  abort idle read-write transactions.
- **Implement Bigtable or Spanner from the observed Stage 1 ratios** — Stage 1 does not measure
  Flink recovery, checkpoint interval, hot-key contention, large payloads, or the connector's
  failure-routing semantics, and Spanner's key-distribution deviation prevents even a formal
  Stage 1 pass; Bigtable's pass of 2026-09-05 changes what is planned
  ([#1211](https://github.com/flink-gcp/flink-connector-gcp/issues/1211)), not this rule.
- **Treat the Cloud Tasks averages or the Bigtable repeat's ratios as a pass** — doing so would
  discard the variability rule after observing its result and turn a preregistered gate into
  post-hoc judgment; the Bigtable pass came from a preregistered amendment, not from relaxing the
  rule.
- **Continue measuring without a concrete non-idempotent requirement** — Spanner and Bigtable
  already expose idempotent keyed mutation shapes for workloads that respect each service's
  ordering constraints, while Cloud Tasks already exposes bounded task-creation deduplication.
  The stronger candidates impose service-specific schema and failure-policy costs without an
  identified workload that needs them.
  The 2026-09-05 Bigtable repeat and its amended run were the authorized exceptions, made so that
  the record holds a compliant measurement rather than a deviation; the Flink-level measurement
  inside [#1211](https://github.com/flink-gcp/flink-connector-gcp/issues/1211)'s design work is
  planned under that issue's own approval, not under this rule.
  The Cloud Tasks proposal in [#1238](https://github.com/flink-gcp/flink-connector-gcp/issues/1238) likewise ran a separately approved Stage 1 repeat in [#1241](https://github.com/flink-gcp/flink-connector-gcp/issues/1241); its inconclusive outcome supplied no performance pass.
- **Match the "Exactly Once out of the box" of the
  [google/flink-connector-gcp Bigtable sink](https://github.com/google/flink-connector-gcp/blob/main/connectors/bigtable/README.md#exactly-once)**
  — compared 2026-09-05: that sink is a plain `Sink` and `SinkWriter` that flushes a bulk-mutation
  batcher at the checkpoint barrier, with no committer, writer state, or two-phase commit; three of
  its four built-in serializers stamp each cell with the Flink record timestamp when the record
  carries a positive one and with the writer's wall clock otherwise, and the fourth hands the whole
  entry to a user function.
  Its guarantee rests on Bigtable's cell idempotence under an explicit timestamp, which this
  connector already documents as the replay-safe shape of its at-least-once sink, so there is no
  capability for the marker candidate to match.

## Consequences

- User documentation states what each current sink guarantees without equating checkpoint flush,
  idempotence, two-phase commit, and end-to-end exactly-once processing.
- The delivery-guarantees guide names the google/flink-connector-gcp sink's "Exactly Once out of
  the box" as the same explicit-timestamp effect, so a reader comparing the two finds a difference
  in naming, not in capability.
- BigQuery's BUFFERED-stream implementation is documented as related to, but not a copy of, the
  official COMMITTED-stream example.
- Pub/Sub has no connector-only implementation issue to pursue.
- Cloud Tasks keeps its existing bounded creation guarantee through both connector APIs.
  The proposal in [#1238](https://github.com/flink-gcp/flink-connector-gcp/issues/1238) proceeds through a protocol scoped to published service semantics and the existing performance gates, as clarified by ADR-0154.
  Individual vendor confirmation is not a prerequisite; an unsupported stronger guarantee is outside the implementation scope.
- Bigtable and Spanner keep their current keyed write shapes and their documented replay and
  ordering boundaries.
  Bigtable's next step is the committer-based mode planned under
  [#1211](https://github.com/flink-gcp/flink-connector-gcp/issues/1211); Spanner's candidate
  advances only when a concrete non-idempotent requirement justifies reopening its measurement.
- No unsupported exactly-once mode is added by this documentation decision.
