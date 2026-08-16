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

# ADR-0003: A vendor client's teardown may re-report a failure the connector already consumed

- Status: Accepted
- Date: 2026-08-07
- Issues: [#325](https://github.com/laughingman7743/flink-connector-gcp/issues/325),
  [#238](https://github.com/laughingman7743/flink-connector-gcp/issues/238),
  [#351](https://github.com/laughingman7743/flink-connector-gcp/issues/351)
- Modules: bigtable and pubsub carry the absorbs; the contract binds every client-wrapping SPI

## Context

[#238](https://github.com/laughingman7743/flink-connector-gcp/issues/238) found that closing
Bigtable's batcher rethrew entry failures the sink's `FailureHandler` had already handled, so a
`logAndDrop` job failed at task close. [#325](https://github.com/laughingman7743/flink-connector-gcp/issues/325)
then asked whether that no-re-report problem is a property of wrapping a client at all, and
measured every candidate SPI instead of assuming either way.

## Decision

Two of this repository's client-wrapping SPIs wrap a client that, at teardown, reports again a
failure the connector has already taken delivery of and acted on. Both absorb it; a third
implementation of either SPI would have to as well, and that is what their `close()` javadoc
says. The rule is **not** a property of wrapping a client — the two mechanisms are unrelated,
so the next SPI is measured rather than assumed either way.

Two rules the implementations turn on:

- **The absorb is per-connector, because the mechanisms are.** Nothing shared would catch both:
  `shutDownAbsorbingTheLifetimeFailureReport` catches `BatchingException` by type, and the
  subscriber's `awaitTerminated()` catches `TimeoutException | RuntimeException` because Guava
  reports a state, not a named exception. A shared helper would have to be parameterised by the
  thing that differs, which is the whole of it.
- **A client that cannot be subclassed needs its operations held as functional values**, or the
  absorb has no test. Every one of these clients is unextendable, but **not all by the same
  mechanism**, and the distinction is worth keeping because only one of them is a rule someone
  can break: gax's `Batcher` is `@InternalExtensionOnly`, so a fake would be legal Java and an
  unsupported extension; `Subscriber` and `Publisher` are non-final classes whose only
  constructor is `private`; `BigtableDataClient`'s is **package-private** (`@InternalApi("Visible
  for testing")`), and `StreamWriter`'s and `BigQueryWriteClient`'s are likewise inaccessible —
  each forbidding a subclass just as effectively. **None of them is `final`**, which six places
  in this repository asserted before the correction sweeps
  ([#324](https://github.com/laughingman7743/flink-connector-gcp/issues/324) corrected two,
  #325 three more, and the sixth — the base module's `CLAUDE.md` — outlived both). Worth the
  tally, because the claim was copied rather than checked each time. There is no mocking library
  here, so injection is the only seam — #324 for the batcher adapter, #325 for the subscriber,
  and `PubSubDeadLetterQueue`'s `publisherShutdown`/`channelShutdown` before both.

## Evidence

Measured against the vendor sources on 2026-08-06, at the versions `libraries-bom` 26.85.1 pins.

**The measured set, stated so it is reproducible**: every `@Internal` interface in this
repository that declares a `close()` and whose implementations exist to wrap a GCP client — nine
of them — plus `@Experimental` `DeadLetterQueue`, admitted on its implementation rather than its
declaration, since `PubSubDeadLetterQueue` owns a `Publisher`. Two qualifiers the wording alone
will not give you, both worth stating because a re-run otherwise disagrees with this list:
`TopicAdmin` and `SubscriptionAdmin` are **in** although neither owns a long-lived client — they
declare the `close()` and are the shape a future implementation might, which is what the
contract is for — and `SubscriptionAdmin`'s closer is the split **enumerator**, not a writer or
reader. `StagingStorage` is **out**, and this is the one exclusion that is a property of the
type rather than a judgement: it declares no `close()` at all, its teardown being the staged
object's own `OutputStream`, so there is no moment at which it could report anything a second
time.

The two that re-report:

- **gax 2.82.0 `BatcherImpl.close()`** — `MutationBatcher`, Bigtable.
  `BatcherStats.recordBatchElementsCompletion` calls `get()` on **every entry's result future**
  and accumulates the failure; the maps are never cleared for the batcher's lifetime;
  `closeAsync()` ends in `asException()` and `close()` rethrows it as
  `new BatchingException(cause.getMessage())`. #238.
- **google-cloud-pubsub 1.152.0 `Subscriber.awaitTerminated(long, TimeUnit)`** — the source's
  `NotifyingPullSubscriber`. `Subscriber extends AbstractApiService`, whose
  `InnerService extends` Guava's `AbstractService`; `checkCurrentState(TERMINATED)` on a
  `FAILED` service throws `IllegalStateException(..., failureCause())`, and that cause is the
  same `Throwable` the failure listener already recorded as `permanentError` and `pullMessages`
  already reported, wrapped in an `IOException`. Note what `Subscriber` is *not*: gax redeclares
  the service contract precisely so Guava can be shaded, so `Subscriber` is an `ApiService`, and
  the `AbstractService` is a private inner field of `AbstractApiService`. Nothing can catch or
  `instanceof` the Guava type here. #325.

**Measured not to have it**, so this is not re-derived:

- `TopicPublisher` and `DeadLetterQueue` — `Publisher.shutdown()` is an already-shut-down
  `checkState`, then `publishAllOutstanding()`, then `Waiter.waitComplete()`, which counts
  pending work and inspects no result, and `awaitTermination` returns a `boolean`. Nothing reads
  a message future, so a per-message failure is reported only through the future `publish`
  returned.
- `TaskCreator` and BigQuery's `BufferedStreamService` — `CloudTasksClient` and
  `BigQueryWriteClient` hold a gax `BackgroundResourceAggregation`, which is pure delegation
  returning `void`/`boolean`.
- `RowAppender` and `OffsetRowAppender` — bigquerystorage 3.30.0 `StreamWriter.close()`, whose
  `ConnectionWorker.cleanupInflightRequests()` completes only futures **still in the inflight
  queue**: a first report, not a repeat, and the nearest miss in the set.
- `TopicAdmin` and `SubscriptionAdmin` — no long-lived client, `close()` empty.
- `MutationBatcher`'s *client* half, which is a second teardown inside one SPI —
  `BigtableDataClient.close()` → `EnhancedBigtableStub.close()`, which is **not** a
  `BackgroundResourceAggregation` but `BigtableClientContext.close()`'s own loop over the
  context's background resources, wrapped in an `IllegalStateException`. It reports no
  per-mutation outcome.

## Consequences

**The consequences are asymmetric, and only one of them is severe.** Bigtable's duplicate
arrives after the sink's `FailureHandler` may have deliberately dropped those very entry
failures, so it converts a job that policy kept running into a failed one. Pub/Sub's first
report already fails the job, so the duplicate only adds a competing exception to a teardown the
first one is causing. Both absorb, but do not read the second as evidence that the first is
merely tidiness.

**An absorb wide enough to catch the re-report catches more than the re-report, and what else
falls in it has to be worked out per client**
([#351](https://github.com/laughingman7743/flink-connector-gcp/issues/351), on the Pub/Sub
subscriber). There, the same `IllegalStateException` also carries a failure the SDK raises
*during* our teardown — `doStop()` runs `runShutdown()` on a thread of its own under
`catch (Exception e) { notifyFailed(e); }` — which nothing has consumed, because the reader has
stopped pulling. It is still absorbed, but it is told apart and reported as its own thing.
**What the discrimination must test is whether the failure was handed to a caller, not when it
was recorded** — a distinction the first attempt got wrong twice: it snapshotted the recorded
failure before the shutdown, which on the reader's own close path is taken *after* `stopAsync()`
(every subscriber's `shutdown()` runs before any `close()`), and which in any case answers "was
it readable" rather than "was it read". A boolean set where the failure is thrown answers the
actual question and needs no ordering argument. A client whose teardown cannot raise a new
failure needs none of this — which is most of the measured set — so the question to ask of the
next one is not "does it re-report" alone but "what else does this catch cover".
