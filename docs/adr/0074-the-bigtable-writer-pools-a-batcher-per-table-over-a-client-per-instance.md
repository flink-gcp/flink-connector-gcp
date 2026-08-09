<!--
Copyright 2026 laughingman7743

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

# ADR-0074: The Bigtable writer pools a batcher per table over a client per instance

- Status: Accepted
- Date: 2026-08-09, revised by [#436] (2026-08-10, the flow controller's figures)
- Issues: [#232], [#436]
- Modules: bigtable (`sink`, `sink.writer`)
- Current behavior: `docs/content/docs/connectors/datastream/bigtable.md` § Per-record destinations

## Context / Evidence

The sink wrote one fixed table, alone among this repository's four sinks: BigQuery, Pub/Sub and
Cloud Tasks all resolve a destination per record through a `DestinationResolver` SPI. ADR-0041
recorded the deferral rather than a preference — a bulk mutation batcher is bound to one table, so
per-record tables mean a pool of batchers, an answer to how the in-flight budget is shared, and an
eviction policy for the tail — and [#232] carried it. #217's table sink builds on this landing
first.

Checked against the pinned versions before the design was committed
(`google-cloud-bigtable` 2.80.0, gax 2.82.0):

- **A `BigtableDataClient` is built for a (project, instance) pair and hands out a batcher for any
  table in it** (`newBulkMutationBatcher(TargetId)`), while holding a channel pool and a background
  executor. So the two levels are genuinely different: batchers are per table, clients are not.
- **`Batcher.closeAsync()` composes with `close()`** (`BatcherImpl`, read in gax 2.82.0):
  `closeAsync()` memoizes its future (`if (closeFuture != null) return closeFuture`), sends what is
  buffered and refuses further admission; `close()` is `closeAsync().get()` and still rethrows the
  `BatchingException` ADR-0046's absorb expects. So a caller may start every shutdown and then wait
  on each, paying one wait rather than one per table.
- **`BigtableDataClient` reports nothing about having been closed** and cannot be subclassed
  (ADR-0047's finding, unchanged). This is what makes the client-ownership decision below a
  correctness decision rather than a tidiness one: the wrong arrangement is invisible to every
  test that injects its own closeable.

## Decision

**The resolver SPI is the siblings', and `table(...)` is sugar for it.** `DestinationResolver<T>`
and `FixedDestinationResolver` sit at the `sink` root with the shape the other three connectors
already publish; `BigtableSinkBuilder.table(...)` assigns a `FixedDestinationResolver` and
`destinationResolver(...)` assigns the user's, both writing one field so the last call wins —
`PubSubSinkBuilder`'s pair, rather than the "exactly one of the two is required" the issue's plan
sketched, which would buy nothing and forbid overriding a default.

**No `instanceof FixedDestinationResolver` fast path.** The plan proposed detecting the fixed case
and resolving once; neither `PubSubWriter` nor `BigQueryDefaultStreamWriter` does, and the one
writer that tests the type (`BigQueryBufferedStreamWriter`) does it as a *precondition*, not an
optimisation. What the fixed case pays here is a virtual call returning a field and a map lookup
whose `equals` settles on identity, against a per-record `toProto()` ADR-0041 measured at **31 ns
for its cheapest shape** (27.1 ns construction + 3.8 ns walk, one 64-byte cell) and 18 µs for a
1000-cell one. A branch that avoids a single-digit-nanosecond lookup beside that is not an
optimisation; one path is one path that gets tested.

**The destination is resolved before the record is serialized.** The issue's plan said the reverse
("serialize → null-skip → resolve"); it cannot be followed, because `FailedMutation`'s constructor
`checkNotNull`s its destination, so a record the serializer rejects would have nothing to be
reported against. Both siblings already resolve first (`CloudTasksWriter.write`,
`PubSubWriter.write`). The property the plan wanted survives unchanged: the null-skip `return`
stays ahead of the pool, so a skipped record opens no batcher — ADR-0001's placement rule, and it
matters more here than it reads, since a serializer skipping everything would otherwise open a
channel pool per phantom destination. A resolver returning `null` fails the write rather than
reaching the handler: it is a configuration failure, not a bad record, and routing it would let a
dropping policy write nothing at all under a green job.

**The in-flight budget stays writer-global.** `maxInFlightEntries` and `maxInFlightBytes` bound
the *writer's* memory and are summed across destinations, not shared out among them — the answer
both existing dynamic-destination writers give, and the one this design depends on twice:
`drainInFlight()` keeps meaning "the writer is empty", which a per-destination split would leave
with no single number to wait on, and the park bound in `write()` stays a single number rather than
a sum of caps. What the writer retains is therefore one cap's worth in flight, at most one cap's
worth parked, and **one gax accumulator per live batcher** — the third term being what per-record
destinations added, and the one a memory-sizing reader would otherwise miss.

**Clients are shared per (project, instance) and released by the factory, never by a batcher.**
`MutationBatcherFactory.create()` becomes `create(TableDestination)` and the factory becomes
`AutoCloseable`; `DefaultMutationBatcherFactory` caches a client per instance (keyed
`project/instance` as a plain string, unambiguous because `TableDestination` rejects a component
containing `/`) and closes them all in its own `close()`. `BigtableBatcherAdapter` therefore holds
**no client at all**, reversing the half of ADR-0047 that had it own one: an adapter that closed a
shared client would take its instance's sibling batchers down with it, and since the client reports
no closed state, the survivors would fail on their next send rather than at the close that broke
them. The adapter keeps ADR-0047's substance — the batcher's operations as functional values,
because `Batcher` is `@InternalExtensionOnly` and a fake must not implement it — and the sharing
itself is asserted directly, through a package-private `client(TableDestination)` whose identity is
the only observable a `BigtableDataClient` offers.

A client's lifetime is the writer's, not the last batcher's: eviction closes a batcher and leaves
the client to the factory. Refcounting was considered and declined — what grows without bound is
the *table* set, which eviction already bounds, while instances are typically one — and it would
add a second release path, a "rebuild after the last eviction" case and a second close-failure site
for no measured gain.

**The client's flow controller is per client, not per batcher, and that is measured** — because
ADR-0041's third SDK fact ("keep the writer's caps below the client's blocking flow controller") is
the one sharing a client could have invalidated. `EnhancedBigtableStub` holds a single
`bulkMutationFlowController`, built once from
`bulkMutateRowsSettings.getDynamicFlowControlSettings()`, and hands **that same instance** to every
`newMutateRowsBatcher(...)`; its limits default to 20,000 outstanding entries and 100 MiB,
blocking (`google-cloud-bigtable` 2.80.0, read 2026-08-09 in `EnhancedBigtableStub` and
`EnhancedBigtableStubSettings`, the two figures re-read in `ClientOperationSettings` on 2026-08-10
by [#436] — the "1000 per channel" this paragraph first carried was `bulkReadRowsSettings`'s stale
javadoc rather than this operation's code, and ADR-0041 records that correction in full).

So sharing subdivides nothing: the batchers of one client draw on one budget, and because the
writer's caps stayed writer-global, a writer can never have more than `maxInFlightEntries`
outstanding on any one client, whatever the table count — exactly the relationship the single-table
sink had. This is the sharpest argument against splitting the budget per destination: N
per-destination caps would **sum**, and their sum reaching the client's limit is what moves the
effective bound into a controller that blocks the task thread instead of yielding to the mailbox.

**The teardown is two-phase, and the SPI grew a `shutdown()` for it.** A batcher's `close()` is an
unbounded wait by design (ADR-0046: a bounded one would abandon mutations the service may still
apply), and a writer holding one per table would pay that wait once per table; a teardown that
overruns Flink's `task.cancellation.timeout` turns a cancelling task into a fatal TaskManager error.
`BigtableWriter.close()` therefore builds **one** `Closers.closeAll` list — every `shutdown()`, then
every `close()`, then the factory, then the table admin, then the failure handler — which is
`PubSubWriter.close()`'s shape and its reason, and #297's one-list rule at a new call site. The
factory sits after every batcher because it holds the client they send through.

**The isolation pass opens by sending every live batcher, not only the ones holding parked work.**
The solo property is writer-wide: a mutation added to an emptied accumulator and flushed at once
travels alone only if no *other* batcher is holding an entry that a delay-threshold timer may push
meanwhile. Missing one batcher fails two ways, neither of which names the cause — the drain never
reaches zero and the task thread parks inside `yield()` forever, or gax's 1 s timer sends that entry
as a batch whose rejection parks *after* the pass took its budget and trips ADR-0045's tripwire on a
healthy stream. The per-mutation send stays that mutation's batcher alone, which is safe because
`submit()` is the only thing that fills an accumulator and the loop is its only caller. The pass's
termination argument is otherwise verbatim ADR-0045's, with "solo" now reading "solo across every
batcher".

**One repair covers every table parked at the time, and ensures each of them.** `tableMissing`
becomes a *set*; `ensuredThisRepair` is a set too, and that is the load-bearing half — a boolean
"have I ensured yet", the obvious port, creates the first missing table and silently skips the rest,
then spends the whole budget re-applying against a table nothing created and dies with a message
about undeclared column families that is false. No existing test could see it, because every writer
test had one table. An ensure that fails part way through the set re-arms the failed table **and
every table it did not reach** before spending its attempt, or the untried tail would wait for a
later `NOT_FOUND` to name it again. One `TableCreateOptions` serves every table: a resolver names
tables, not schemas — the same sentence `TopicCreateOptions` already carries for topics.

The recovery budget stays per *incident*, which makes it shared across the tables of one repair: a
mutation naming an undeclared family — ADR-0073's named unrepairable case — now spends the budget
while other tables' parked work is still outstanding, and abandons it. At-least-once covers that for
the reason `close()`'s discard is covered (no checkpoint completed with it parked), but it is a real
behaviour change under a dropping policy and the docs page says so. A per-table budget was declined:
it would need a per-table attempt counter and a rule for what "the repair failed" then means, to
improve a case that is a configuration error either way.

**Eviction runs at the end of a successful `flush(false)`, and closes the batcher alone.**
`destinationIdleTimeout` mirrors `DefaultStreamOptions`' (default 1 h, `checkPositive` +
ADR-0068's `checkExpressibleInNanos`, "set a very large duration" as the documented way to say
never, an injected `LongSupplier` clock so a test can fast-forward). The placement is what makes the
issue body's eviction problem disappear: at that point every batcher has been drained and both parks
are empty, so the batcher being evicted is empty — its close sends nothing and waits for nothing,
which is what makes an unbounded close acceptable on the task thread at all. `endOfInput` skips the
sweep, as BigQuery's does, since `close()` follows and releases everything anyway.

**Per-destination counters exist, behind `perDestinationMetrics`, default off.** This *refines*
ADR-0043's "no per-destination counters, and so no option", whose stated reason was the one fixed
table. The base `DestinationMetrics` supplies them, scoped by `destination.toString()` as the
BigQuery writers scope theirs, resolved once per `DestinationState`; `recordsSkipped` takes no
handle, for the reason both siblings give — the serializer is handed the record alone, so a skip
cannot be a property of the table it would have gone to. The writer makes no exception for a
`table(...)` sink where the counters can only restate the totals: "one table" is a property of a
resolver the writer deliberately does not inspect.

## Consequences

- The one-fixed-table claims in ADR-0041, ADR-0043, ADR-0045 and ADR-0073 are refined in place;
  the docs page's "One table per sink" section is rewritten rather than repointed, which retires
  the anchor obligation [#232] carried.
- `CREATE_IF_NEEDED` beside a resolver is a different risk profile from one fixed table — a buggy
  resolver can invent a table per record, against an instance-level table limit and through
  heavyweight admin RPCs. It is documented rather than forbidden; forbidding the combination was
  weighed and declined as an asymmetry with Pub/Sub that would have to be lifted later.
- Two unbounded waits, `awaitCapacity()` and `drainInFlight()`, now have more ways to stall: the
  chance that *some* table's completion never arrives scales with the destination count.
  ADR-0052's progress bound is Pub/Sub-only, and porting it stays out of this change — but the
  exposure grew, which is what would justify the port.
- `BigtableMutateRowsSink.createWriter(batcher, …)` becomes `createWriter(factory, …)`, and the
  creation-failure guard shrinks to the factory, the admin and the handler: with lazy creation no
  batcher exists when the writer's constructor fails.

## Alternatives considered

- **A client per table.** Preserves ADR-0047's adapter verbatim and deletes the sharing question,
  at a channel pool and an executor per table — genuinely heavy above a handful of destinations,
  and a cost paid by exactly the jobs this feature is for.
- **A share of the in-flight budget per destination**, which `BigtableSinkBuilder`'s own javadoc
  used to predict. Declined above: it dissolves both the drain's meaning and the park bound.
- **Rejecting `CREATE_IF_NEEDED` beside a resolver in this change**, to keep two thirds of the
  repair's hazards out of it. Declined as above.
- **Refcounted client release.** Declined above.

[#232]: https://github.com/laughingman7743/flink-connector-gcp/issues/232
[#436]: https://github.com/laughingman7743/flink-connector-gcp/issues/436
