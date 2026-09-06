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
  [#1239](https://github.com/flink-gcp/flink-connector-gcp/issues/1239)
- Modules: bigquery, pubsub, cloudtasks, bigtable, spanner
- Current behavior: `docs/content/docs/connectors/delivery-guarantees.md`

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
own ADR; no other non-BigQuery exactly-once implementation or additional performance stage is
planned without a concrete non-idempotent user requirement that the existing write shapes cannot
satisfy.

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
  The checkpointed-creation proposal in [#1238](https://github.com/flink-gcp/flink-connector-gcp/issues/1238)
  received a no-go at its service-contract gate on 2026-09-06 ([#1239](https://github.com/flink-gcp/flink-connector-gcp/issues/1239), evidence below).
  No positive recovery window is justified until retention and late-effect bounds are established.
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

If such a requirement reopens a candidate, Spanner must first repeat Stage 1 with evenly
distributed keys, and Cloud Tasks must first produce a stable run-to-run result.
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

**G0 is no-go for the scope proposed in [#1238](https://github.com/flink-gcp/flink-connector-gcp/issues/1238).**
The investigation in [#1239](https://github.com/flink-gcp/flink-connector-gcp/issues/1239) establishes no positive, usable recovery window from the contracts inspected below.
In particular, even granting a fixed minimum tombstone lifetime, the available contracts do not bound the last possible creation effect of an old request after its final deadline check.
This is a contract and protocol result, not an observation that Cloud Tasks violated its retention specification.
No real-GCP probe ran, no GCP resources were created or changed, and no vendor inquiry was sent.
The service observations and cleanup results in the preregistered plan below therefore remain unexecuted, not passed.
The dependent protocol and performance work in [#1240](https://github.com/flink-gcp/flink-connector-gcp/issues/1240) and [#1241](https://github.com/flink-gcp/flink-connector-gcp/issues/1241) remains gated.

#### Evidence boundary and SDK access

The local inspection used repository commit `c34c9d1dd8f3e2d8680b983068e772f10a31507e` and its effective Cloud Tasks module POM, generated offline with Maven's `help:effective-pom` goal.
It resolved `libraries-bom` 26.87.0 to `google-cloud-tasks`, `proto-google-cloud-tasks-v2`, and `proto-google-cloud-tasks-v2beta3` 2.96.0, with gax/gax-grpc 2.84.0, grpc-api/core/netty-shaded 1.82.4, and protobuf-java 4.33.6.
Source inspection used those artifacts' source jars, rather than the independently moving `latest` Java reference.
The public proto links below pin googleapis/googleapis commit `64aa30b277168edd20efee0c9ceb4ca01248931d`; REST, release-note and gRPC pages were read on 2026-09-06.
Where they disagree, this record retains the disagreement instead of choosing the longest duration.

| Surface | Evidence available locally or publicly | What remains unproved |
|---|---|---|
| v2beta3 administrative client | The 2.96.0 client contains `CloudTasksClient.getQueue(GetQueueRequest)` and `updateQueue(UpdateQueueRequest)`; the matching proto exposes `Queue.task_ttl` and `tombstone_ttl` as duration fields, with builder setters and presence accessors. | Successful service readback and enforcement are not implied by Java method availability. |
| `GetQueue` | [The method][ct-g0-get] requires `cloudtasks.queues.get`. An omitted read mask returns all fields except statistics; an explicit proto mask can request `name,state,task_ttl,tombstone_ttl,purge_time`. | A present duration describes current configuration, not the history of existing task names. An absent field is not a measured default. |
| `UpdateQueue` | The [2021-01-14 release note][ct-g0-release] explicitly made both TTL fields configurable. The [request][ct-g0-beta-rpc] accepts an update mask; an empty mask updates all fields. [The method][ct-g0-update] can create a missing queue. | Read-after-update propagation, effects on existing tasks/tombstones, and cross-version enforcement require confirmation. The connector must not use update as a prerequisite check. |
| v2 `CreateTask` | [The request][ct-g0-v2-rpc] contains the parent, task and response view. The current adapter invokes `createTaskCallable().futureCall(request)`. | There is no absolute create-expiry or queue-generation precondition in that request. `Task.dispatch_deadline` bounds handler dispatch, not task creation. |
| Client timing | In 2.96.0 `CloudTasksStubSettings`, `createTaskSettings` selects the empty `no_retry_1_codes` set and `no_retry_1_params`, whose initial/max RPC timeout and total timeout are 20 seconds. | An empty gax retry set does not establish a service-side effect bound or rule out gRPC transparent retry. It also does not replace the sink's own retry loop. |

#### Queue and removal matrix

The [v2beta3 Queue contract][ct-g0-beta-queue] specifies tombstone retention after deletion or execution, with a configurable range of one hour to nine days and a one-hour default for Cloud Tasks-created queues.
That is stronger wording than the v2 name-release estimates, but the cited sources do not settle all lifecycle and cross-version questions required by G0.
The matrix's unknowns are not claims that the service omits protection.

| Queue origin and v2 task target | Published evidence | G0 disposition |
|---|---|---|
| Cloud Tasks-created, HTTP | [v2 REST creation][ct-g0-create] gives a name-release delay of up to 24 hours; the pinned [v2 proto][ct-g0-v2-rpc] gives approximately one hour. The beta Queue fields apply at queue level. | Neither v2 estimate supplies an exact positive lower bound. Verify an explicit three-day beta setting with v2 HTTP creation. |
| Cloud Tasks-created, App Engine | The same v2 collision text applies; beta `app_engine_http_queue` settings concern this target arm. [App Engine queue access][ct-g0-beta-resource] also requires `appengine.applications.get`. | Verify independently with v2 App Engine tasks; do not transfer HTTP observations. |
| queue.yaml/xml-created, HTTP | v2 collision text distinguishes queue origin, while beta target-specific settings distinguish target type. The cited material does not establish this combination's configuration/enforcement behavior. | Record creation acceptance or exact rejection before any retention inference; an unsupported combination is not a passing cell. |
| queue.yaml/xml-created, App Engine | REST gives up to nine days for name release; the v2 proto says approximately nine days. Beta Queue documents a fixed maximum task TTL for these queues. [Mixed management][ct-g0-yaml] can disable other queues. | Preserve origin as a separate arm. Confirm whether tombstone TTL updates apply; use an isolated project for legacy configuration uploads. |

| Removal or update | Documented behavior | Retention obligation and current evidence gap |
|---|---|---|
| Task remains live | Same-name creation returns `ALREADY_EXISTS` under both v2 and beta creation contracts. | The result identifies a name collision, not equal payloads. Queue namespace ownership and immutable persisted identity are prerequisites for interpreting it as this task's prior creation. |
| Successful execution | [Task lifecycle][ct-g0-v2-rpc] deletes a task after a successful response; beta Queue describes a tombstone after execution. | Confirm the configured duration for each target and v2-created task. Duplicate handler dispatch is not duplicate physical creation. |
| Manual `DeleteTask` | The [v2 method][ct-g0-v2-rpc] permits deletion of scheduled or dispatched tasks; beta Queue describes a tombstone after deletion. | Confirm the same duration on both task states. Client response time is not the exact deletion timestamp. |
| `PurgeQueue` | [Purge][ct-g0-purge] removes tasks and can take up to one minute; dispatch can occur before it takes effect. | The purge method does not separately specify tombstone creation, treatment of existing tombstones, or a lower bound. The generic deletion wording needs confirmation for this bulk path. `purge_time` alone does not answer it. |
| Retry exhaustion | [RetryConfig][ct-g0-v2-queue] deletes the task when the applicable attempt and duration conditions are exhausted. | Configure both conditions explicitly and observe actual removal; `max_attempts` alone is not the removal-time oracle. Confirm tombstone behavior independently of successful execution. |
| Task TTL expiry | Beta Queue documents deletion regardless of dispatch, a minimum task TTL of ten days, a maximum of ten years and a 31-day default for Cloud Tasks-created queues. Legacy-created queues have a fixed maximum. | Automatic expiry needs its own retention evidence. The UpdateQueue method's blanket 31-day creation statement also needs reconciliation with the field contract. A short emulator test cannot establish this path. |
| Lower or raise `task_ttl` | Configurability is documented for beta queues; legacy task TTL is documented as fixed. | The applicability to existing tasks, the expiry origin and the tombstone when an existing task becomes overdue are unspecified in the inspected field/update text. Raising TTL must not be assumed to restore a deleted task. |
| Lower or raise `tombstone_ttl` | The field is configurable within its documented range. | The inspected text does not say whether old tombstones retain their original expiry, shrink, or extend. Raising the current value cannot prove that an earlier shortening did not already release a name. |
| Delete and recreate the queue | [REST deletion][ct-g0-delete] describes a possible queue-name exclusion of up to three days and requires GetQueue confirmation after apparent recreation; the pinned v2 proto instead says seven days. | Queue-name exclusion is not task-name retention across queue generations. No persistence contract was found for old task tombstones. Exclude recreation throughout recovery and stale-request lifetime; do not derive H from either delay. |

The exact documented collision outcome for a live or remembered task name is `google.rpc.Code.ALREADY_EXISTS` (numeric code 6).
No different outcome was observed, because no service calls were made.
`NOT_FOUND`, `FAILED_PRECONDITION`, `DEADLINE_EXCEEDED`, `UNAVAILABLE` and `UNKNOWN` cannot be promoted to prior-creation success from the cited contracts.
Nor does an absent `GetTask` result show that a task never existed or never ran.

#### Recovery inequality and the stale-request counterexample

For one immutable task identity, let `t0` be its persisted deadline origin, established before any create, and let `t` be its last local deadline authorization check.
All incarnations must preserve `t0` and the same queue and task bytes.
Let **H** be the minimum tombstone lifetime, in real elapsed time, over every permitted removal path and administrative history; **E** the maximum underestimation of elapsed time relevant to that lifetime; **S** the maximum real time from that last check to the last possible create effect; and **R** the authorized replay budget.
If measured age at the check is at most `R`, the last effect must occur by `t0 + R + E + S`.
For a task first created at `c` and removed at `d`, `t0 <= c <= d`, so a valid minimum H protects its name at least until `d + H >= t0 + H`.
Consequently, `R + E + S < H` is a sufficient timing condition for no same-identity recreation, conditional on live-name exclusion and the namespace/lifecycle prerequisites below.
Equality leaves no strict margin and is not accepted.

Clock uncertainty must be derived across the actual machines and recovery interval.
Write an endpoint wall-clock reading as real time plus clock error `e` and timestamp representation error `q`.
The measured age is `(t - t0) + e_check - e_origin + q_check - q_origin`, so its underestimation is `e_origin - e_check + q_origin - q_check`.
Absolute endpoint error bounds give the conservative sum `b_origin + b_check + q_bound_origin + q_bound_check`, but only if those bounds cover drift, clock steps, suspension and restart for the whole deployment.
Twice one clock bound is a special case requiring equal established bounds, not an assumption.
If the vendor's TTL clock does not already guarantee H in real elapsed time, its conversion error must also be bounded and counted once in E, or H must be reduced accordingly.
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
Thus no finite S is justified for the proposed failure and suspension scope, even if a retention probe later supports the candidate H.

#### Prerequisites and an operational objective

The following are candidate requirements for reopening the gate, not features enforced by today's stateless writer.

| Requirement | Checkable fact | Deployment obligation |
|---|---|---|
| Retention history | Beta GetQueue exposes current TTLs and purge time. | Establish applicable policy before the first possible create and preserve it through all pending replay and old-request lifetimes. Current readback is insufficient; do not assume an audit-log sample proves complete history or per-tombstone expiry. |
| Removal policy and task lifetime | Current task TTL and retry settings can be read. | Cover automatic expiry and retry exhaustion even if manual deletion/purge is forbidden. If a permitted path lacks protection, H is zero; if its protection is unknown, no positive H is justified. |
| Queue and namespace | Read the fixed pre-provisioned queue and check task-name construction. | Preserve queue generation and exclusive identity ownership. Ban deletion/recreation, historical rollback and concurrent forks throughout the applicable lifetime. Persist queue, task bytes, origin and retention assumptions unchanged. |
| Elapsed time | Read configured clock-health signals and the persisted origin. | Supply bounds that remain valid across machines, clock steps and outages. A health observation at startup alone does not bound E. |
| Late effects | Inspect client deadlines, retry settings and cancellation paths. | Supply a bounded pause/transport/service-effect contract, or an externally enforced mechanism that also covers requests already admitted to the service. Killing the old process or revoking its future egress alone does not recall those requests. |
| Recoverability | Check checkpoint mode and retained externalized state in the later protocol work. | Retain pending state on terminal failure and stop unsafe replay after expiry; neither a plain exception nor a restart loop establishes this. |

The operational objective is to finish every pending identity's creation, including catch-up after an incident, before its original deadline while retaining unresolved state if that cannot be done safely.
Choose the required budget from non-overlapping worst-case intervals: age of the oldest pending origin at outage onset, outage duration, additional diagnosis/restore time not included in the outage, and post-restore backlog/checkpoint/commit drain time.
Call their sum `R_required`; a useful choice requires `R_required <= R < H - E - S`.
The task's execution backlog after successful creation is not creation recovery work, but its TTL still introduces a removal path.
No existing numerical recovery objective was provided; the investigation was asked to determine the feasible range first.
Because H across the full scope and a finite S remain unjustified, no supported numerical range is established, not a one-hour or 24-hour default.
One hour and three days below are experimental settings, not product correctness defaults.

For a future prerequisite reader, the additional administrative permission is `cloudtasks.queues.get`; the [Enqueuer role][ct-g0-iam] does not include it.
`cloudtasks.queues.update` belongs to a separate administrator/probe principal, not the sink.
The [RPC permission reference][ct-g0-beta-permissions] and IAM inventory also identify `cloudtasks.queues.create/delete/purge/pause/resume` for disposable queue management, and `cloudtasks.tasks.create/get/list/delete` for probe work.
`cloudtasks.tasks.fullView` is needed only when a FULL view is requested; creation status and BASIC metadata suffice for these probes.
App Engine access additionally needs `appengine.applications.get`; deploying or starting a target fixture and reading administrative history require their own narrowly scoped permissions and approvals.

#### Preregistered service probes

The following plan is frozen before any service results.
Its purpose is to falsify retention assumptions and collect vendor-reproducible observations; passing samples cannot establish an undocumented minimum or a universal upper bound on S.
Resolve the missing service-effect contract before paying for the long-lived arms unless a separately approved diagnostic run has a stated purpose.
Use v2 for every task creation and v2beta3 for TTL administration; record both API versions.
Use three independently named cohorts per applicable queue-origin/target/removal combination, with random, uniformly distributed task names and synthetic content only.
Each observation time gets an independent task identity: a successful boundary probe must not create a new tombstone that contaminates a later observation of the original one.

| Probe | Setup and observations | Observable result |
|---|---|---|
| Configuration and controls | Get an existing isolated queue, update only `tombstone_ttl` to `259200s` (three days), then Get with and without an explicit read mask. Keep separate default/one-hour control queues. Exercise `3599s` and `777601s` on separate disposable controls. Read back task TTL independently. | Require present, exact intended durations before retention observations. Record exact rejection/status for invalid values; do not replace ignored/absent settings with defaults. A fresh-name v2 create must succeed and a second live-name create must return `ALREADY_EXISTS`. |
| Successful execution and manual deletion | Run separate HTTP/App Engine success cohorts with a controlled 2xx handler. Delete separate scheduled and dispatched cohorts. Record the deletion interval between last confirmed live and first confirmed absent observations, alongside handler evidence. | Test same-name creation immediately, at two hours, near the one-day point, and around the three-day boundary. Any successful create definitely before the assumed expiry falsifies that arm. Handler receipts alone do not count physical creations. |
| Purge and retry exhaustion | Purge both a live cohort and a queue containing existing tombstones; keep an unpurged control. For a separate always-failing handler set both `max_attempts = 2` and positive `max_retry_duration = 60s`, with a bounded observation period. | Record purge response and actual disappearance separately. Do not assume two dispatches prove exhaustion. If disappearance is not observed, classify removal timing as inconclusive. Compare collisions with the unpurged/deletion controls. |
| Tombstone updates | Use independent queues and cohorts removed before and after `3600s -> 259200s` and `259200s -> 3600s`. Include a cohort whose original short tombstone has already expired before raising the value. | Observe at two hours and around three days against unchanged controls. Record whether old names release under the old or new setting. An expired name remaining available after a raise disproves using current readback as retention history. |
| Task TTL and its updates | For Cloud Tasks-created queues, use paused tasks and an explicit ten-day TTL; compare independent old/new cohorts under `31d -> 10d` and `10d -> 31d`, including tasks already older than ten days at a lowering. | Observe expiry independently and test the resulting name protection. Do not shorten the test with an emulator or a past schedule time. If a legacy-created queue retains its documented fixed maximum, mark natural expiry unmeasured and seek a contract; do not wait ten years or treat manual deletion as the same path. |
| Queue recreation | In an isolated lifecycle arm, delete a queue containing live names and existing tombstones. Poll recreation only within a preregistered observation limit; confirm every apparent recreation with GetQueue before replaying old names. | Failure to recreate within the limit is inconclusive, not proof of task retention. If recreation succeeds, record old-name collision outcomes and new queue readback. Never infer task protection from the queue-name exclusion delay. |
| Lost responses and late requests | Separate response suppression after an admitted create from a pre-send suspension. Also exercise restart/failover with an old incarnation, a broken connection and reconnect, with identifiable attempts and explicit local deadlines. | Track API creation responses, live-task observations and same-name outcomes separately. A demonstrable second physical creation falsifies the candidate. An ambiguous timeout or duplicate handler dispatch does not. Failure to reproduce a late effect cannot bound S. |

Before service execution, add an approved run manifest naming the exact project, locations, disposable queues, HTTP/App Engine targets, identities/principals, retention arms, start/stop times, and cleanup owner.
Include numeric caps on created tasks, API calls, observed dispatches, fixture instance-hours, total lifetime and cost, with the concrete monitoring and stop actions for each; a billing alert is not a hard cost cap.
The legacy upload arm requires an isolated project or an already verified isolated fixture because queue.yaml/xml administration can affect queues omitted from the upload.
Keep App Engine compute stopped during multi-day retention waits and start it only for the dispatch observations that need it.
Approve short, retention-boundary, task-expiry and legacy/lifecycle arms separately; extending an observation limit requires approval before extending resource lifetime.
No resource names, budget, fixture deployment or real-GCP execution is authorized by this document.

For each arm record the preregistration revision, client/proto versions, UTC and monotonic attempt timestamps, clock uncertainty, opaque cohort/attempt IDs, configuration before and after updates, gRPC status numbers/names, BASIC task metadata and control outcomes.
Keep credentials, authorization headers, payloads and environment files out of the public artifact; publish sanitized raw observations rather than only averages.
For boundary classification, use deletion and attempt time intervals widened by clock uncertainty: a collision definitely before expiry is expected, a successful create definitely before expiry is a failure, and overlap with the boundary is inconclusive.
A fresh-name success control distinguishes a collision from general inability to create; a same-name success definitely after expiry is the required falsifying control showing the probe can detect recreation.
If the latter never occurs before the approved stop time, report it as inconclusive rather than extending the run silently.
Cleanup must enumerate exact owned queues, delete them and confirm absence, then stop any started App Engine fixture and confirm zero instances; record partial cleanup failures and leave the arm incomplete until resolved.

#### Conditions for reopening G0

A vendor inquiry would ask whether configured beta tombstone TTL is a minimum for v2-created tasks across the matrix, exactly which removal paths create tombstones, and how TTL changes affect existing tasks and tombstones.
It would also ask for the queue-origin/target combinations, queue-generation behavior, retention-clock semantics and any finite last-effect bound or server-enforced absolute expiry for CreateTask after a client timeout, cancellation, delayed delivery or reconnect.
These are prepared questions, not external communication already performed.
Reopen only with evidence that resolves the required contracts and an explicit deployment mechanism/assumption set yielding finite E and S and positive `H - E - S`.
The existing bounded named-task behavior remains available; this investigation neither supersedes its runtime design nor approves the proposed checkpointed mode.

[ct-g0-beta-resource]: https://docs.cloud.google.com/tasks/docs/reference/rest/v2beta3/projects.locations.queues
[ct-g0-get]: https://docs.cloud.google.com/tasks/docs/reference/rest/v2beta3/projects.locations.queues/get
[ct-g0-update]: https://docs.cloud.google.com/tasks/docs/reference/rest/v2beta3/projects.locations.queues/patch
[ct-g0-release]: https://docs.cloud.google.com/tasks/docs/release-notes#January_14_2021
[ct-g0-beta-queue]: https://github.com/googleapis/googleapis/blob/64aa30b277168edd20efee0c9ceb4ca01248931d/google/cloud/tasks/v2beta3/queue.proto
[ct-g0-beta-rpc]: https://github.com/googleapis/googleapis/blob/64aa30b277168edd20efee0c9ceb4ca01248931d/google/cloud/tasks/v2beta3/cloudtasks.proto
[ct-g0-v2-rpc]: https://github.com/googleapis/googleapis/blob/64aa30b277168edd20efee0c9ceb4ca01248931d/google/cloud/tasks/v2/cloudtasks.proto
[ct-g0-v2-task]: https://github.com/googleapis/googleapis/blob/64aa30b277168edd20efee0c9ceb4ca01248931d/google/cloud/tasks/v2/task.proto
[ct-g0-v2-queue]: https://github.com/googleapis/googleapis/blob/64aa30b277168edd20efee0c9ceb4ca01248931d/google/cloud/tasks/v2/queue.proto
[ct-g0-create]: https://docs.cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues.tasks/create
[ct-g0-purge]: https://docs.cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues/purge
[ct-g0-delete]: https://docs.cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues/delete
[ct-g0-yaml]: https://docs.cloud.google.com/tasks/docs/queue-yaml
[ct-g0-iam]: https://docs.cloud.google.com/tasks/docs/access-control
[ct-g0-beta-permissions]: https://docs.cloud.google.com/tasks/docs/reference/rpc/google.cloud.tasks.v2beta3
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
  The proposal in [#1238](https://github.com/flink-gcp/flink-connector-gcp/issues/1238) is blocked at G0;
  its dependent implementation and performance repeat remain gated by the [#1239](https://github.com/flink-gcp/flink-connector-gcp/issues/1239) no-go.
- Bigtable and Spanner keep their current keyed write shapes and their documented replay and
  ordering boundaries.
  Bigtable's next step is the committer-based mode planned under
  [#1211](https://github.com/flink-gcp/flink-connector-gcp/issues/1211); Spanner's candidate
  advances only when a concrete non-idempotent requirement justifies reopening its measurement.
- No unsupported exactly-once mode is added by this documentation decision.
