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

# ADR-0148: Bigtable single-row requests run on a request-response runtime beside the batcher

- Status: Accepted
- Date: 2026-09-03
- Issues: [#1178], [#1174]
- Modules: bigtable (`sink.singlerow`, `sink.singlerow.writer`, module root), base (`metrics`)
- Current behavior: `docs/content/docs/connectors/datastream/bigtable.md` § Single-row request
  writes

## Context

The Bigtable sink is built on `MutateRows`: `BigtableWriter` hands every record to a batcher and
learns the outcome of each entry from the batch response. Bigtable's two other write RPCs,
`CheckAndMutateRow` and `ReadModifyWriteRow`, cannot go through that batcher. Each is a single-row
request that returns a value — whether the predicate matched, or the row after the atomic
append/increment — and the value is the reason to call the RPC. Issues [#1179] and [#1180] add the
user-facing sinks and functions for those two RPCs, and [#1181] wraps the function surface in a
Flink 2.2+ SQL function. All three are written against what this record settles: where the code
lives, what a user receives back, where the failure boundary sits, and how the async surface
behaves under Flink's own timeout and replay rules.

The facts below were measured against `google-cloud-bigtable` 2.81.0 (`libraries-bom` 26.86.0),
Flink 2.2.1 and Flink 1.20.4:

- Both `checkAndMutateRowSettings()` and `readModifyWriteRowSettings()` default to
  `retryableCodes = []` and a 20 s total timeout, while `mutateRowSettings()` retries. The SDK
  therefore already treats these two RPCs as non-idempotent.
  `UnaryCallSettings.Builder.setSimpleTimeoutNoRetriesDuration(java.time.Duration)` sets total,
  initial and maximum RPC timeout to one value with `maxAttempts = 1`.
- The SDK's `Row` and `RowCell` are `@InternalExtensionOnly`; its request builders carry their
  own `TableId`.
- `RichAsyncFunction`, `ResultFuture` and `AsyncDataStream` are `@PublicEvolving` in both Flink
  majors and ship in `flink-streaming-java`, already a provided dependency of the module. Flink
  1.20's `ResultFuture` has no `CollectionSupplier` overload.
- `AsyncWaitOperator` checkpoints every input whose result has not been emitted and replays it
  after a restore. A completed checkpoint therefore means "emitted or replayed", never "applied".
- `MailboxExecutorImpl.execute` throws `RejectedExecutionException` once the mailbox is quiesced,
  which happens in `prepareClose()` before operators close.
- An `ApiFuture` whose callback was registered with `Runnable::run` runs
  `onFailure(CancellationException)` synchronously inside `cancel(true)`.
- `ApiFutures.transform` unwraps an `AbstractApiFuture` input to its internal Guava future, and
  Guava's cancel propagation cancels that internal future directly. The client's
  `BigtableUnaryOperationCallable.UnaryFuture` is such an input, and its `cancel` override is the
  only path to cancelling the RPC on the wire, so a cancel through the transform ends the
  transformed future and leaves the RPC running (found in review; the test that holds it failed
  against the transform before the repair).

## Decision

**The new code is a family layer, and only the new code moves into one.** ADR-0055's rule is
that a second write family costs the module the layer [#119] removed. The layer is `sink.singlerow`
— Google's own name for these two RPCs is *single-row transactions*, and `unary` is a gRPC shape
`MutateRow` also has. The mechanical move of the existing `sink.writer` into `sink.mutaterows.writer`
is deferred to its own change: the Lane B pull requests planned on [#1174] each edit those files,
and a rename racing three functional changes would cost each of them a rebase for no behavioral
gain.
ADR-0055 and ADR-0041 carry dated notes recording that the module is, for now, half-layered on
purpose.

**One request seam, and the table is resolved at start time.** A `RowRequest<R>` names its
operation and row key and builds the SDK request only inside `start(client, destination)`, using
the destination the sink's `DestinationResolver` produced. That is what reconciles per-record
destinations with SDK request objects that carry their own table id. `ReadModifyWriteRow` results
are transformed into a connector-owned `BigtableRow` on the gax thread, so no `@InternalExtensionOnly`
SDK type reaches a public surface; the same rule that keeps SDK protos out of the public API
elsewhere in this repository. The transformed future owns its `cancel` and forwards it through the
client future's own `cancel` (`ConvertedAnswer`), because `ApiFutures.transform` would not — the
evidence above — and a cancel that does not reach the wire leaves a timed-out or closed request
running to its deadline.

**The SDK deadline is the only timeout, and retryable codes stay empty.** The client factory applies
`setSimpleTimeoutNoRetriesDuration(requestTimeout)` to both stub settings; there is no connector
timer, and no connector retry loop. A retry of a non-idempotent RPC after an ambiguous failure could
apply an increment twice, which is exactly what the SDK's own defaults refuse to do. A test pins the
SDK defaults so a BOM bump that changes them fails the build rather than the semantics.

**Three failure classes, one boundary.** `INVALID_ARGUMENT` is row-level (ADR-0042's rule) and, on
the sink surface, goes to the `FailureHandler<FailedRequest>`. So is a request the client's request
builders refuse before it is sent: they check the request's own content with
`IllegalArgumentException` or `IllegalStateException`, thrown synchronously from the call — a
`CheckAndMutateRow` whose `then` and `otherwise` are both empty is the case that survives the
connector's own eager checks — and a refused call never reports through those two types
(`RejectedExecutionException`, or a failed future), so the route stays state-independent
(ADR-0042). `DEADLINE_EXCEEDED`, `UNAVAILABLE`,
`ABORTED` and `CANCELLED` are *ambiguous*: the service may have applied the request, so the job fails
with a message naming the RPC, saying so, and stating that a replay is at-least-once and that
`ReadModifyWriteRow` is not idempotent under it. Everything else, table-not-found included, is
fatal. `DEADLINE_EXCEEDED` is additionally counted as `requestsTimedOut`. There is no isolation pass
and no repair: a single-row request has exactly one identity, and creating tables is the batching
sink's feature, not this family's.

**The sink surface keeps the batching writer's shape.** `SingleRowRequestWriter` keeps a
task-thread ledger, counts a request as in flight only after the SDK accepted it, re-dispatches
every completion as a mailbox mail, catches the closed-mailbox rejection at debug, and on close
cancels every outstanding request before releasing the factory and the handler. Instance clients
follow ADR-0145: one client per `(project, instance)`, permits from the shared reaper, LRU eviction
after a drain at `maxActiveInstances`, idle eviction only after a successful non-final flush. Results
are discarded on this surface, because a `Sink` has no output; there is no counter for that, since
every completed request discards its answer here and the count would equal `requestsCompleted`.

**The async surface has no failure handler.** `BigtableRequestFunction<IN, R, OUT>` extends
`RichAsyncFunction` and completes each `ResultFuture` from the gax thread with a connector-owned
result. The `FailureHandler` contract is task-thread, and the function has no mailbox to hop back
to, so every RPC failure fails the job; [#1179] and [#1180] may model row-level outcomes as values
in their result types instead. Flink's own `timeout(input, result)` cancels the SDK future and fails
the record naming both the Flink timeout and `requestTimeout`; capacity is `AsyncDataStream`'s
operator capacity, documented as `maxInFlightRequests` until a wiring helper passes it. The function
never waits on in-flight work, so at the instance cap it evicts an LRU instance with nothing in
flight or fails deterministically naming `maxActiveInstances`; the client factory's wait for the
evicted client's close to free its slot is the one wait left, bounded by that close.

**The client reaper is shared.** The `ClientReaper` that ADR-0145 placed inside the batcher
factory moves to the module root as `BigtableClientReaper`, unchanged in contract, because two client families now take
permits from one bounded close pool. ADR-0145 carries the note.

**The counters the async surface increments from gax threads are thread-safe.** The base module's
`ErrorClassCounters` and `DestinationMetrics` take a `Supplier<Counter>`; the sink surface keeps
`SimpleCounter` and the async surface passes `ThreadSafeSimpleCounter`, as `EnumeratorCounters`
already does for the Pub/Sub source. The sink surface follows ADR-0037's rule for the standard
pair — `numRecordsSend` at acceptance, the first hand-off, and `numRecordsSendErrors` for records
routed to the failure handler — so that the two Bigtable sink families read alike on those names.

**Public building blocks are `@PublicEvolving`; the two runtimes are `@Internal`.**
`BigtableRequestOptions` (`maxInFlightRequests` 100, the same number as Flink's default async
capacity; `requestTimeout` 20 s, the measured SDK default; `destinationIdleTimeout`,
`maxActiveInstances` and `perDestinationMetrics` as the batching sink's), `BigtableRow`,
`FailedRequest` and `RowOperation` are what [#1179] and [#1180] build on. `FailedRequest` returns
`null` from `getPayloadBytes()` until those issues own a request model; serializing the SDK's
`@InternalApi` proto as a dead-letter payload is declined. No Table API option is added here — the
first consuming write-mode value arrives with [#1179] under the mode option [#1177] settles.

## Evidence

The client factory test proves both stub settings carry `retryableCodes = []` and the configured
deadline after `settings(destination)`, and separately pins the SDK defaults; a credential
provider injected at runtime reaches the settings (the module rule for a new client family). Writer
tests hold, with fakes, that capacity is released on every terminal outcome, that a synchronous
rejection by the SDK counts nothing, that a closed writer turns late completions into no-ops and
swallows the closed-mailbox rejection, that flush waits for every accepted request and fails fast on
an earlier asynchronous failure, and that capacity eviction drains first. Function tests hold the
ambiguity boundary from the callback thread, that a Flink timeout cancels and counts, that a late
callback after a timeout is ignored, that the counters are exact under concurrent completions, and
that the registered counter class is thread-safe. The emulator ITCase drives both RPCs through the
production client and reads the rows back; the MiniCluster ITCase emits `BigtableRow` downstream
through `AsyncDataStream.unorderedWait` and fails a never-answering request with the
Bigtable-named timeout message.

## Alternatives declined

**A connector retry loop around ambiguous failures.** Re-issuing a `ReadModifyWriteRow` after
`DEADLINE_EXCEEDED` can apply it twice; the SDK's empty retryable-code set is the same judgement.

**A per-call `ApiCallContext` timeout.** It duplicates what one settings builder line does and
needs its own re-check on every call site.

**Reusing `BigtableWriterOptions`.** Its knobs are batcher thresholds and in-flight bytes, neither of
which a single-row request has; sharing would leave most of its setters inert on this family.

**SDK request and row types as the public contract.** `Row` is `@InternalExtensionOnly` and the
request builders carry a table id the resolver must own.

**Promoting Pub/Sub's `InFlightTracker` to base.** It tracks bytes and entries for a batching
writer; the request ledger here is a count and an identity set — on the async surface an
identity-keyed map, so the operator's timeout finds its request by the `ResultFuture` it was
handed — and the async surface needs a thread-safe one.

**Moving `sink.writer` now.** See the first decision: it would collide with every Lane B change.

**Routing a failure handler from gax threads on the async surface.** The handler contract is
task-thread; the built-in handlers write files and Bigtable rows and are not safe to call
concurrently.

## Consequences

The module has two write families with one layered and one not until the deferred move lands. The
per-operation sinks and functions of [#1179] and [#1180] are thin over this runtime: a serializer
SPI, a `RowRequest` implementation, a result wrapper and a builder each. Users of the async surface
own the capacity and timeout relationship until the wiring helper arrives, and a replayed
`ReadModifyWriteRow` after a restore is a documented at-least-once cost, not a defect.

[#1178]: https://github.com/flink-gcp/flink-connector-gcp/issues/1178
[#1174]: https://github.com/flink-gcp/flink-connector-gcp/issues/1174
[#1177]: https://github.com/flink-gcp/flink-connector-gcp/issues/1177
[#1179]: https://github.com/flink-gcp/flink-connector-gcp/issues/1179
[#1180]: https://github.com/flink-gcp/flink-connector-gcp/issues/1180
[#1181]: https://github.com/flink-gcp/flink-connector-gcp/issues/1181
[#119]: https://github.com/flink-gcp/flink-connector-gcp/issues/119
