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

# ADR-0082: The Bigtable batch knobs are bounded flush triggers, and the client is what counts mutations

- Status: Accepted
- Date: 2026-08-10; revised by [#1052] (2026-08-23)
- Issues: [#436], [#1052]
- Modules: bigtable (`sink`, `sink.writer`)
- Current behavior: `docs/content/docs/connectors/datastream/bigtable.md` § Tuning,
  `docs/content/docs/reference/bigtable.md` § `BigtableWriterOptions`

## Context / Evidence

[#436] is the sibling of [#435], which bounded the Spanner sink's batch knobs (ADR-0077), and it
asked the same question of the sink that shipped first. It found three things: no upper guard on
`batchElementCountThreshold` or `batchRequestByteThreshold`, a units mismatch — the knob counts
`RowMutationEntry` objects while Bigtable's limit counts *mutations* — and one javadoc sentence
("Bigtable accepts up to 100 MB of mutations per request") it could not source.

The units half turned out to rest on a premise that measurement retires.

**The client holds a batch to 100,000 mutations by itself.** Read in `google-cloud-bigtable` 2.80.0
and `gax` 2.82.0 on 2026-08-10:

- `MutateRowsBatchingDescriptor.createResource(entry)` carries `entry.toProto().getMutationsCount()`
  into a `MutateRowsBatchResource`, whose class javadoc says why it exists at all: *"because
  MutateRowsRequest has a limit on number of mutations"*.
- `MutateRowsBatchResource.shouldFlush(maxElementThreshold, maxBytesThreshold)` returns true on
  `getElementCount() > maxElementThreshold || getByteCount() > maxBytesThreshold ||
  getMutationCount() > 100000`. The third term is a hardcoded constant, independent of every
  threshold a caller configures.
- `BatcherImpl.add` evaluates `shouldFlush` on `currentOpenBatch.resource.add(newResource)` — the
  batch *including* the arriving element — and calls `sendOutstanding()` **before** adding it. So
  the batch that goes out holds at most 100,000 mutations, and the arriving entry opens a fresh one.
- A single entry cannot breach it either: `Mutation.addMutation` refuses past
  `MAX_MUTATIONS = 100_000` ("Too many mutations per row"), and `RowMutationEntry.toProto()`
  re-checks the same per-entry number.

So `batchElementCountThreshold(50_000)` on a job writing ten cells per record does **not** send
500,000 mutations in a request, which is what [#436] §1 predicted. The knob is a flush *trigger*;
the service's limit is enforced a layer below it, in a unit this connector never has to convert to.

**There is no second guard at the batch level, and the issue's blast-radius paragraph is why that
is tolerable.** `BulkMutation` does keep a running `mutationCountSum` with a precondition refusal —
but only in `add(ByteString, Mutation)`. The batcher's request builder calls `add(RowMutationEntry)`,
which counts nothing. Were `shouldFlush`'s third term to disappear from a later client, an
over-limit request would reach the service and be refused request-level; ADR-0045's isolation pass
then re-submits each entry alone, so the cost is a degradation to one request per entry rather than
a dropped record. That is the reason this is a guardrail rather than a correctness fix.

**The unsourced sentence is unsourced.** Bigtable's [quotas page] documents no per-request size
limit for `MutateRows` at all — its size rows bound a single mutation (200 MB), a cell value
(100 MB), a row (256 MB) and a row key (4 KB). The figure in the javadoc matches something else the
client does have: `ClientOperationSettings` gives the bulk-mutation path a flow controller with
`setMaxOutstandingRequestBytes(100L * 1024 * 1024)` — 100 MiB *in flight*, not per request.

**The real ceiling is the client's own validation, and it binds far below anything the service
documents.** `BigtableBatchingCallSettings.Builder.build()` refuses a configuration where a batch
threshold is not *strictly* below the matching flow-control budget:

> if batch elementCountThreshold is set in BatchingSettings, flow control
> maxOutstandingElementCount must be > elementCountThreshold

— and the same for `requestByteThreshold` against `maxOutstandingRequestBytes`. With the budgets at
20,000 entries and 100 MiB, that makes 19,999 and 100 MiB − 1 the largest values a client can be
built with. Past either, `BigtableDataSettings.Builder.build()` throws
`IllegalArgumentException` — inside `DefaultMutationBatcherFactory.client(...)`, on a task manager,
as the writer opens, surfacing as `Failed to create a Bigtable mutation batcher for table …`.

This was found by running the ceiling this ADR first proposed.
`batchElementCountThreshold(50_000)` — [#436]'s own example of a value that "looks like half the
documented limit" — does not send an over-limit
request and never has; it fails the job at the first record. So [#436] §2's shape is right ("a value
past the limit is accepted at submission and only refused on a task manager") for a reason the issue
did not have: not the service's refusal, but the client's.

**The same reading corrected a second figure this repository carried.** ADR-0041 and the two docs
pages said that flow controller permits "1000 entries per channel and 100 MB". The code sets
`maxBulkMutateOutstandingElementCount = 20_000L`, flat, with no channel term. "1000 outstanding row
keys per channel" is the javadoc on `EnhancedBigtableStubSettings.bulkReadRowsSettings()` — a
different operation, and stale against its own code, which also sets 20,000. Nothing in the
conclusion moves: the writer's defaults (1000 entries / 64 MiB) still bind first, with more headroom
than was claimed.

## Decision

**1. Both batch knobs are bounded at the setter**, in ADR-0077's shape — a package-private
`*_LIMIT` constant, with the figure named in the `@param` rather than the symbol (a public
compile-time constant inlines into callers), and a reject/accept test pair.

Both ceilings are **one under the client's matching flow-control budget**, and both are written as
that subtraction — `CLIENT_MAX_OUTSTANDING_ENTRIES - 1`, `CLIENT_MAX_OUTSTANDING_BYTES - 1` —
rather than as 19,999 and 104,857,599, so that a client release moving a budget carries the ceiling
with it instead of leaving one that admits a value the client then refuses. Neither is a service
figure, and the byte one could not be: Bigtable documents no per-request size at all. What is
bounded is what the client will let a job configure, which is stricter, better defined, and the
thing that actually breaks.

The derivation this ADR first proposed — an element ceiling of 100,000, from the mutations Bigtable
documents per batch, in the shape `MAX_BATCH_MUTATIONS_LIMIT = MAX_BATCH_CELLS_LIMIT` takes on the
Spanner side — was **wrong by a factor of five**, and shipping it would have left the guardrail
admitting the exact value that breaks a job. It is recorded here rather than quietly dropped
because the mistake is instructive: the ceiling was derived from what the *service* documents,
while the layer that refuses first is the *client*, and only running it showed which.

**2. The units are documented, not converted.** No mutation counting on this side. The javadoc, the
reference table and the tuning section say that every count this connector exposes counts entries,
that Bigtable's limit counts mutations, and that the client reconciles them — so a job never has to
read one number against the other. The tuning section also lists all five conditions that end a
batch (the two thresholds, the client's one-second timer, the client's mutation guard, and a full
writer sending every batcher), and states that any "setting X large makes batches of X" claim has to
name the one that *binds* — ADR-0077's rule, which applies here for a different reason.

**3. What is counted is spelled "entries".** `maxInFlightMutations` becomes `maxInFlightEntries`,
and the two gauges `inFlightMutations` / `parkedMutations` become `inFlightEntries` /
`parkedEntries`: all three counted entries while naming mutations, which is the same defect in the
half of the connector [#436] did not name. **"Mutation" stays the word for the thing** —
`FailedMutation`, `MutationBatcher`, a mutation the service refused — and "entry" is the word for
what is counted. Renaming the metrics costs least today: nothing is published to Maven Central
(§ Version policy), so any dashboard that exists was built against `main` and moves with it.
Leaving them would have put the connector's option vocabulary and its metric vocabulary in
disagreement on the very point this ADR exists to settle — and `inFlightEntries` is also what the
sibling connectors' convention asks for, each of whose gauges names the unit *it* counts
(`inFlightTasks`, `inFlightBatches`, `inFlightAppends`, `inFlightMessages`). Bigtable's was the one
naming a unit its connector does not count. (The
[#1043](https://github.com/flink-gcp/flink-connector-gcp/issues/1043) review later recorded this
unit-naming reading as the fleet-wide rule —
[ADR-0137](0137-a-cross-connector-name-diverges-only-to-name-a-real-difference.md).)

**4. The in-flight bounds are warned about, not capped** — `build()` says so when
`maxInFlightEntries` or `maxInFlightBytes` is above the client's matching budget. This is the
second half of the shape ADR-0077 uses on the Spanner side, where a value that is *illegal* is
refused at the setter and one that merely *cannot take effect as meant* is warned about at
`build()`. These are the second kind: past the budget the configuration works, and what changes is
which layer bounds the sink — the client, which **blocks** the task thread instead of yielding to
the mailbox.

Refusing them instead was considered and declined on a fact ADR-0074 established: that budget is
**per client**, and this sink holds one per (project, instance). A resolver spreading records over
several instances draws on several budgets, so a writer-global bound above one of them can be
exactly what such a job means — and nothing at `build()` knows how many instances a resolver will
name. A ceiling would refuse a configuration that is correct for that job; the warning tells the
much commoner single-instance job what it has done.

The comparison is `>` rather than `>=`, and the boundary is measured rather than assumed: gax's
`BlockingSemaphore.acquire` waits while `availablePermits < permits`, so the client admits its
whole budget and blocks only on the request past it. A bound *equal* to the budget still binds
first, and `doesNotWarnAtOrBelowTheClientsOwnBudget` pins that. It also builds the defaults, since
initializing this class runs `DEFAULTS = builder().build()` — defaults that tripped the warning
would put both lines in every task manager's log for a job that configured neither knob.

**5. Every client fact this rests on is pinned by a test**, in ADR-0041's shape — a client upgrade
that moves one fails a test rather than a job.

- `BigtableClientMutationLimitTest` drives `MutateRowsBatchingDescriptor` and gax's `BatchResource`
  directly: a batch at exactly 100,000 mutations does not flush with both configured thresholds set
  to `Long.MAX_VALUE`, one past it does, and a single entry over the limit is refused.
- `DefaultMutationBatcherFactoryTest` builds the production settings with both knobs at their
  ceilings, which is the assertion that decides whether the ceilings are the right numbers at all.
  It is the test that would have caught the wrong ceiling above, and it costs no container:
  the refusal happens in `BigtableDataSettings.Builder.build()`, before anything connects.
- `BigtableWriterOptionsTest` pins the constants to the figures, so the two halves cannot drift.

## Consequences

- A job that set either knob above its ceiling now fails at submission with a message naming the
  figure, where before it submitted and then died on a task manager at its first record. Nothing in
  range changes.
- [#436]'s suggested work items 2 (count mutations per entry, "probably right") and 3 (measure what
  the service answers an over-limit `MutateRows` with, in the [#218] gated suite) are **not** done,
  and the reason is the same measurement: the first duplicates the client, and the second cannot be
  produced through this sink to be measured at all.
- The metric rename is the one user-visible break. It is taken now precisely because it is free now.
- ADR-0041's third SDK fact and ADR-0074's flow-controller paragraph are revised in place with the
  corrected figures. The lesson they now carry is ADR-0041's own rule turned on itself: an SDK fact
  is read from the code, never from the prose beside it.

## Alternatives declined

- **Counting mutations per entry, the way the Spanner sink counts cells** ([#436]'s preferred
  option). `RowMutationEntry` does not expose its mutation count, but `toProto()` does, and the
  writer already calls `toProto()` once per record — so the count is available. Declined because the
  client already does it, and a writer-side accumulator would be *worse* than the client's: the
  writer cannot observe the batcher's own sends (the element threshold, the byte threshold, the
  one-second timer), so its running count would over-estimate and force sends the service never
  needed.
- **Bounding `batchRequestByteThreshold` at a service figure.** There is none to bound at.
- **Leaving both knobs unbounded and correcting only the record.** Defensible while the ceilings
  were thought to be precautionary; not once the client turned out to refuse a job outright, which
  is exactly the "accepted at submission, refused on a task manager" shape ADR-0068 and [#435] were
  filed over.
- **Raising the client's flow-control budgets instead**, so a larger threshold becomes legal.
  Declined without measuring: exposing those knobs is the [#85] defect class itself (ADR-0041), and
  raising them moves the writer's backpressure into a controller that blocks the task thread.
- **A hard ceiling on the in-flight bounds too.** Declined for the reason under decision 4: the
  client's budget is per client, and a multi-instance resolver legitimately exceeds one of them.
  The warning is what the Spanner options do with the same shape.
- **Renaming the writer's "mutation" vocabulary wholesale** — `FailedMutation`, `MutationBatcher`,
  `mutationSent`. Declined: those name the thing rather than a count, the SPI names are public API,
  and the rule that survives is easier to apply than a blanket rename would be.
- **An emulator ITCase writing 120,000 mutations** to watch the client split them. Declined once
  `BigtableClientMutationLimitTest` pinned the mechanism deterministically: the ITCase would take
  seconds, and the client's one-second timer could split the batch first on a slow machine, leaving
  a case that quietly stops exercising what it was written for while staying green. Its other half
  — that the ceilings are configurations the client accepts — moved to
  `DefaultMutationBatcherFactoryTest`, where it needs no container and no timing at all.

[#85]: https://github.com/flink-gcp/flink-connector-gcp/issues/85
[#218]: https://github.com/flink-gcp/flink-connector-gcp/issues/218
[#435]: https://github.com/flink-gcp/flink-connector-gcp/issues/435
[#436]: https://github.com/flink-gcp/flink-connector-gcp/issues/436
[#1052]: https://github.com/flink-gcp/flink-connector-gcp/issues/1052
[quotas page]: https://cloud.google.com/bigtable/quotas
