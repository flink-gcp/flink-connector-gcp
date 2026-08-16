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
- Date: 2026-08-09 (client library facts read in google-cloud-spanner 6.119.0; emulator behaviour
  measured 2026-08-09 against `gcr.io/cloud-spanner-emulator/emulator:1.5.56`)
- Issues: [#220], [#36]
- Modules: spanner (`sink`, `sink.writer`)
- Current behavior: `docs/content/docs/connectors/datastream/spanner.md` § How the sink writes

## Context / Evidence

[#36]'s design comment settled the write method — Mutations, no DML, no `WriteMethod` enum — and
named `DatabaseClient.batchWriteAtLeastOnce` as the RPC, with `writeAtLeastOnce` as a fallback if
the emulator turned out not to implement `BatchWrite`. Three facts were left to be checked when
the sink was actually built, and all three were:

- **The emulator implements `BatchWrite` from v1.5.31** (emulator issue #172, opened against
  v1.5.17 and closed 2025-06-20). The fallback is therefore not needed, and the emulator suite
  drives the real write path. It does mean this module cannot share the
  `google-cloud-cli:441.0.0-emulators` image the Bigtable and Pub/Sub tests use — its bundled
  Spanner emulator predates the RPC — so the module pins
  `gcr.io/cloud-spanner-emulator/emulator:1.5.56` of its own.
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
  multiply the duplicates an at-least-once sink can produce. The budget is the `retry*` knobs on
  `SpannerWriterOptions`, mapped through `toRetrySchedule()` and jittered at
  `RetrySchedule.DEFAULT_JITTER_RATIO` like every other schedule here.
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
  sink writes to as many tables as the serializer produces. That is why `SpannerDatabase` sits at
  the module root, why the cell weights are read for the whole database, and why there are no
  per-destination metrics (their cardinality would be the serializer's to decide).
- A transient outage longer than the retry budget fails the job rather than dropping anything.
  That is the intended direction: a sink cannot drop what the service never refused.
- **The budget bounds attempts, not wall clock.** `no_retry_0_params` gives batch write a one-hour
  total timeout (read in 6.119.0) and this sink sets no deadline of its own, so the worst case is
  `retryMaxAttempts` hours of a blocked task thread and no completing checkpoint. Documented on the
  knob rather than capped here: a shorter deadline is a real option, but picking one for every
  workload is not something this connector can do from a default, and no measurement yet says what
  it should be.
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

[#36]: https://github.com/laughingman7743/flink-connector-gcp/issues/36
[#220]: https://github.com/laughingman7743/flink-connector-gcp/issues/220
