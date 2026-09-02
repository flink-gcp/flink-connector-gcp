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

# ADR-0075: The Spanner sink batch-writes one mutation per group and owns the whole retry loop

- Status: Accepted
- Date: 2026-08-09 (client library facts read in google-cloud-spanner 6.120.0; emulator behaviour
  measured 2026-08-09 against `gcr.io/cloud-spanner-emulator/emulator:1.5.56`); naming revised by
  [#1053] (2026-08-23); per-attempt timeout revised by [#1134] (2026-08-29)
- Issues: [#220], [#36], [#1053], [#1134]
- Modules: spanner (`sink`, `sink.writer`)
- Current behavior: `docs/content/docs/connectors/datastream/spanner.md` § How the sink writes

## Context / Evidence

[#36]'s design comment settled the write method — Mutations, no DML, no `WriteMethod` enum — and
named `DatabaseClient.batchWriteAtLeastOnce` as the RPC, with `writeAtLeastOnce` as a fallback if
the emulator turned out not to implement `BatchWrite`. Three facts were left to be checked when
the sink was actually built, and all three were:

- **The emulator implements `BatchWrite` from v1.5.31** (emulator issue #172, opened against
  v1.5.17 and closed 2025-06-20). The fallback is therefore not needed, and the emulator suite
  drives the real write path. It did mean this module could not share the
  `google-cloud-cli:441.0.0-emulators` image the Bigtable and Pub/Sub tests used at the time — its
  bundled Spanner emulator predates the RPC — so the module pins
  `gcr.io/cloud-spanner-emulator/emulator:1.5.56` of its own. That pin stands on its own terms and
  did not move when the shared image did (2026-09-03, now `583.0.0-emulators`): the reason to hold
  a separate image is the version *floor* the write path needs, which a bundle bump does not
  settle either way.
- **A batch write request Spanner refuses is refused as a whole**, so the writer's batch limits are
  correctness rather than tidiness. Which limit each one defends is narrower than the batch write
  page's "the maximum size for a batch write request is the same as the limit for a commit request"
  suggests on its own — that sentence is about size, and the only mutation figure the quotas page
  gives for this RPC bounds one *mutation group*. ADR-0077 is the subject.
- **The client library does not retry this RPC at all.** `SpannerStubSettings` configures
  `batchWriteSettings` with `no_retry_0_codes` / `no_retry_0_params`, and the only retry wrapped
  around it in `DatabaseClientImpl.batchWriteAtLeastOnce` is `runWithSessionRetry`, which recovers
  a lost session and nothing else. Every other Google client this project builds on retries its
  write RPC internally; this one does not.

A fourth fact shapes the guarantee rather than the mechanism: **batch write has no replay
protection** — "it's possible for mutations to be applied more than once" — so idempotence is the
serializer's to supply.

## Decision

**One mutation per `MutationGroup`, and the writer owns the retry loop.**

- The serializer SPI returns a single `Mutation`, and the writer wraps each in its own
  `MutationGroup`. The group is the unit `BatchWriteResponse` reports a status for, so one
  mutation per group is what makes a refusal name a single record — which is the whole reason
  this sink writes through `batchWriteAtLeastOnce` rather than a plain commit, and it is what the
  shared `base.failure` routing needs. Measured, not assumed: every rejection shape
  `SpannerRejectionITCase` provokes comes back as a per-group status, never as a request failure.
- **The writer is synchronous, with no mailbox and no in-flight bookkeeping.** There is no
  asynchronous or self-batching form of this RPC to wrap, so the writer makes one streaming call
  and consumes it to completion on the task thread. That is a real difference from the Bigtable
  and Pub/Sub sinks of this project, which wrap SDK batchers completing futures on their own
  threads — and it is why `SpannerWriter` has none of the machinery those two need.
- **A retry re-sends exactly what is still undecided**: the groups that came back with a transient
  status, plus the groups the service never reported on, which is what a server stream that fails
  part-way through leaves behind. Mutations already applied are never re-sent, so a retry does not
  multiply the duplicates an at-least-once sink can produce. The budget is the `recovery*` knobs on
  `SpannerWriterOptions`, mapped through `toRecoverySchedule()` and jittered at
  `RetrySchedule.DEFAULT_JITTER_RATIO` like every other schedule here.
- **Each complete `BatchWrite` attempt has a 30-second default timeout.** The connector applies it
  only to the data client's `batchWriteSettings` and uses the generated client's no-retry timeout
  form, so client-library retries cannot multiply the connector's recovery budget. The timeout
  covers the whole server stream, including a stream that reports some groups and then stalls.
  The connector then retries only groups whose outcome remains undecided. Reads and administration
  retain the client library's settings.
- **The guarantee is at-least-once, and effectively-once when the serializer emits an idempotent
  operation** — `insertOrUpdate`, `replace`, `delete`. A replayed `insert` answers `ALREADY_EXISTS`
  and is routed as a per-mutation failure (ADR-0076). This is documented on the SPI and on the
  sink, not enforced.
- **The `SpannerDatabaseAccess` SPI is the seam**, holding its three operations as functional
  values over the `Spanner` service handle. `DatabaseClient` is a twenty-method interface returning
  a live `ServerStream`, so a test scripting a half-reported batch write would otherwise have to
  fake all of it — the same argument ADR-0047 made for the Bigtable batcher adapter.

## Consequences

- The sink's destination is a **database**, not a table: the mutation names its own table, so one
  sink writes to as many tables as the serializer produces. That is why `DatabaseDestination` sits at
  the module root, why the cell weights are read for the whole database, and why there are no
  per-destination metrics (their cardinality would be the serializer's to decide).
- A transient outage longer than the retry budget fails the job rather than dropping anything.
  That is the intended direction: a sink cannot drop what the service never refused.
- **The attempt timeout and retry schedule together bound the write loop.** With the defaults, ten
  30-second attempts and nine maximally jittered backoffs take at most 369.375 seconds for one
  invocation. That is not an end-to-end checkpoint guarantee: a record-triggered synchronous flush
  may already be running when a checkpoint barrier reaches the sink, and the checkpoint flush may
  invoke the loop again. Operators size the attempt timeout and recovery schedule so those writes,
  processing, alignment, other operators, and checkpoint transport fit within the checkpoint
  timeout. Larger batches or service load may require a longer attempt timeout and a recomputed
  combined budget.
- A 2026-08-29 same-window real-service comparison on a temporary 100-PU Standard regional
  instance found timeout/control throughput ratios of 104.6%, 98.4%, and 101.6%, with p95 latency
  ratios of 0.96, 1.04, and 0.98. The service-wide absolute rate varied between repetitions, so
  the evidence supports no relative regression against the concurrent control, not an absolute
  throughput claim. A preceding serial comparison varied by more than 10% within each arm and was
  treated as inconclusive. Both temporary instances and their databases were deleted.
- Because the writer sees its own retries, `mutationsRetried` and the transient half of
  `errorClass.*.errors` mean something here that they cannot mean on the sibling sinks, where the
  SDK absorbs the same work. ADR-0076 and the metrics section of the docs page say so where a
  dashboard reader would otherwise generalise.

## Alternatives declined

- **`writeAtLeastOnce` per batch** — what Apache Beam's `SpannerIO` uses, because it predates
  `BatchWrite`. It commits the whole batch atomically, so one poison row fails every mutation
  batched with it, and recovering per-row identity needs Beam's own retry-and-bisect scheme. Batch
  write makes that unnecessary.
- **Wrapping the write in a background thread to overlap it with record intake.** Nothing needs
  it: the batch is writer-local, flushes are barrier-driven, and a background sender would
  reintroduce exactly the completion-thread bookkeeping this connector does not otherwise need.
  Reopen if a measurement shows the synchronous send bounding throughput.
- **A `MutationGroup`-returning serializer overload**, for a caller wanting a parent row and its
  interleaved children applied atomically. Deferred on [#36] with the seam already in place: the
  writer batches in groups internally, so the overload is additive.

[#36]: https://github.com/flink-gcp/flink-connector-gcp/issues/36
[#220]: https://github.com/flink-gcp/flink-connector-gcp/issues/220
[#1053]: https://github.com/flink-gcp/flink-connector-gcp/issues/1053
[#1134]: https://github.com/flink-gcp/flink-connector-gcp/issues/1134
