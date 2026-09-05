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
  and by [#1208](https://github.com/flink-gcp/flink-connector-gcp/issues/1208) (2026-09-05)
- Issues: [#591](https://github.com/flink-gcp/flink-connector-gcp/issues/591),
  [#596](https://github.com/flink-gcp/flink-connector-gcp/issues/596),
  [#1208](https://github.com/flink-gcp/flink-connector-gcp/issues/1208)
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

**BigQuery remains the only connector for which an exactly-once sink mode is supported or
planned.**
No non-BigQuery exactly-once implementation or additional performance stage is planned without a
concrete non-idempotent user requirement that the existing write shapes cannot satisfy.

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
- **The Bigtable same-row marker candidate is correctness-feasible but performance-inconclusive,
  and is not a supported connector mode.**
  Every protected mutation and its marker must share a row, the application profile must use
  single-cluster routing with transactional writes enabled, and marker retention must exceed the
  replay horizon.
  Multi-row effects are outside this candidate.
  A compliant repeat with evenly distributed keys on 2026-09-05 met the ratios twice but failed
  the 10% variability rule twice, so the result stays inconclusive (evidence below).
- **Only the Spanner 100-record ledger-transaction shape remains correctness-feasible, but its
  performance result is inconclusive and it is not a supported connector mode.**
  The ledger and effects must share a database and a short read-write transaction.
  The connector must not keep a transaction open across a Flink checkpoint.

The stronger Bigtable and Spanner candidates address narrower non-idempotent effects than ordinary
idempotent keyed mutations.
Spanner's individual mutation replay safety does not promise ordering between same-key
`BatchWrite` mutation groups.
Their schema, retention, routing, batching, and failure-policy costs are not justified without a
concrete requirement that the current row-key, cell, or mutation upserts cannot meet.
Cloud Tasks already exposes its useful replay primitive through both connector APIs, while Pub/Sub
exposes no publisher-side replay primitive to add.

If such a requirement reopens a candidate, Spanner must first repeat Stage 1 with evenly
distributed keys, and Cloud Tasks must first produce a stable run-to-run result.
Bigtable made its compliant repeat on 2026-09-05; a further Bigtable attempt needs an amended
Stage 1, preregistered before it runs, that instruments the throughput decline the repeat showed
within each run, with its own resource and cost approval.
Passing Stage 1 would permit Stage 2 measurement, not implementation.
Stage 2 would require separate resource and cost approval and would cover 64 KiB payloads, hot
keys, concurrency and Flink parallelism 1, 4, and 16, and checkpoint intervals of 1, 10, and 60
seconds.
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

## Alternatives declined

- **Add `SupportsCommitter` to every sink** — a committer cannot hide or retract an eager publish,
  task creation, row mutation, or batch write that the service has already made visible.
- **Call producer event IDs Pub/Sub exactly-once** — downstream deduplication can protect a consumer
  effect, but does not prevent duplicate messages in the topic.
- **Keep a Spanner transaction open from pre-commit to checkpoint completion** — the source offset
  is not part of that transaction, Flink checkpoints can outlive the transaction, and Spanner can
  abort idle read-write transactions.
- **Implement Bigtable or Spanner from the observed Stage 1 ratios** — a key-distribution
  deviation (Spanner) or a variability failure (Bigtable, 2026-09-05) prevents a formal Stage 1
  pass, and Stage 1 does not measure Flink recovery, checkpoint interval, hot-key contention, large
  payloads, or the connector's failure-routing semantics.
- **Treat the Cloud Tasks averages or the 2026-09-05 Bigtable ratios as a pass** — doing so would
  discard the variability rule after observing its result and turn a preregistered gate into
  post-hoc judgment.
- **Continue measuring without a concrete non-idempotent requirement** — Spanner and Bigtable
  already expose idempotent keyed mutation shapes for workloads that respect each service's
  ordering constraints, while Cloud Tasks already exposes bounded task-creation deduplication.
  The stronger candidates impose service-specific schema and failure-policy costs without an
  identified workload that needs them.
  The 2026-09-05 Bigtable repeat was the one authorized exception, made so that the record holds
  a compliant measurement rather than a deviation.
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
- Cloud Tasks keeps its existing bounded creation guarantee through both connector APIs, and no
  broader mode or repeat is planned.
- Bigtable and Spanner keep their current keyed write shapes and their documented replay and
  ordering boundaries.
  A concrete non-idempotent requirement must justify reopening measurement before either candidate
  advances.
- No unsupported exactly-once mode is added by this documentation decision.
