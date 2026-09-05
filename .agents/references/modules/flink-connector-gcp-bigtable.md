# Detailed guidance — flink-connector-gcp-bigtable

Module-scoped guidance, read when working in this module. Repository-wide rules
(build, workflow, version policy, licensing, package layout) stay in the root `AGENTS.md`.
This file holds the rules a session must follow; each decision's record — context, evidence,
declined alternatives — is the named ADR under `docs/adr/` or the docs page.

## Sink design (`docs/adr/0041`, `0042`, `0074`, `0145`)

- Implemented, never adopted or vendored; the serializer SPI keeps
  `BaseRowMutationSerializer`'s shape, and null = skip is `docs/adr/0001`'s contract.
- The four SDK facts (`@BetaApi` batcher acquisition, `@InternalExtensionOnly` `Batcher` — hence
  the `MutationBatcher` SPI and `sendOutstanding()` over `flush()`, the client's own blocking
  flow controller — keep the writer's caps below it, and `@InternalApi`
  `RowMutationEntry.toProto()`) are checked, not assumed; reread them on a client upgrade
  (`docs/adr/0041`). Two more, measured by `docs/adr/0074`: that flow controller is **one per
  client**, shared by every batcher of an instance, so sharing a client subdivides nothing and the
  writer-global caps still bind first; and `Batcher.closeAsync()` memoizes the future `close()`
  waits on, which is what makes the two-phase teardown cost one wait rather than one per table.
- **The writer's per-record `toProto()` is one of four identical constructions, three of them the
  client's own, and its cost is measured** — so it is not re-argued as an optimisation without
  engaging those numbers, and no local fix exists to argue for: the entry exposes neither its key,
  its mutations nor its size. The lever is upstream (#236, `docs/adr/0041`).
- **Retries stay in the client**: no retry knobs for what the client retries. The `recovery*`
  knobs are not that — they budget the sink-owned auto-creation repair (`docs/adr/0073`), the
  same `recovery*`-vs-`retry*` line the BigQuery options draw. `TableDestination` sits at the
  module root and `appProfileId` is a builder option, not part of it.
- **Per-record destinations are a batcher pool over a client per (project, instance)**
  (`docs/adr/0074`; client lifetime superseded by `docs/adr/0145`): `table(...)` is sugar for a
  `FixedDestinationResolver` and the two setters are last-writer-wins; **resolve runs before
  serialize** (`FailedMutation` checkNotNulls its destination) with the null-skip still ahead of
  the pool; a `null` destination fails the write and is never routed; **no `instanceof` fast
  path**. The in-flight bounds stay **writer-global** —
  that is what keeps `drainInFlight()` meaning "the writer is empty" and the park bound one number.
  The adapter holds **no client**: the factory owns them, reference-counts successful batcher
  creations, and closes a client after its last table is safely evicted; otherwise the first
  batcher to close kills its instance's siblings invisibly. Each writer subtask holds at most
  `maxActiveInstances` open-or-closing clients. Last-table release normally moves SDK close to
  bounded daemon reapers; if the runtime refuses the handoff, it closes synchronously to avoid a
  leak. The permit survives until physical close, so a creation at limit waits interruptibly
  rather than growing a close queue. The writer safely drains before evicting the least recently
  used instance at capacity. `close()` is one `Closers` list, every `shutdown()`
  before any `close()`; the isolation pass's opening `sendOutstanding()` covers **every** live
  batcher, and missing one either hangs the task thread or trips the tripwire on a healthy stream.
- **`INVALID_ARGUMENT` alone is routed, `FAILED_PRECONDITION` deliberately not** — cite gRPC's
  state-independence definition and AIP-194, never the plausibility of what a code names. The
  routing condition takes both halves, reading the chain differently (`docs/adr/0042`).

## Single-row request runtime (`docs/adr/0148`)

- `CheckAndMutateRow` and `ReadModifyWriteRow` run on their own runtime under `sink.singlerow`,
  the module's second write family. **Only the new family is layered**: the mechanical move of
  `sink.writer` into a `MutateRows` family layer is deferred to its own change so it does not
  race every Lane B pull request of milestone v1.1.0; do not fold it into a functional change.
  `BigtableClientReaper` sits at the module root because both client factories take permits
  from it (`docs/adr/0145`).
- A `RowRequest<R>` builds the SDK request only inside `start(client, destination)`, from the
  destination the resolver produced — that is what reconciles per-record destinations with SDK
  request builders that carry their own table id. No SDK `Row`, `RowCell` or request builder
  reaches a public surface: `ReadModifyWriteRow` answers are transformed into `BigtableRow` on
  the gax thread.
- **The SDK deadline is the only timeout, retryable codes stay empty, and there is no connector
  retry loop**: `requestTimeout` goes through `setSimpleTimeoutNoRetriesDuration` on both stub
  settings, and `DefaultSingleRowClientFactoryTest` pins the SDK defaults (20 s, no retryable
  codes) so a BOM bump that changes them fails the build. Do not add a retry knob.
- Failure boundary: `INVALID_ARGUMENT` is row-level (to the `FailureHandler<FailedRequest>` on
  the sink surface), and so is an `IllegalArgumentException`/`IllegalStateException` thrown
  synchronously from `RowRequest.start` — the SDK's own request validation, which is
  state-independent; any other synchronous throw is the client refusing work and fails the write.
  `DEADLINE_EXCEEDED`, `UNAVAILABLE`, `ABORTED`, `CANCELLED` and a cancelled
  future are *ambiguous* and fail the job with the message stating what a replay does to each
  RPC; everything else, `NOT_FOUND` included, is fatal. No isolation pass, no auto-creation.
  `RequestFailures` reads the chain through `BigtableErrorClassifier` so the two families
  cannot drift.
- The async surface (`BigtableRequestFunction<IN, R, OUT>`) has **no failure handler** — the
  handler contract is task-thread and answers arrive on gax threads — and never waits on
  in-flight work: at the instance cap it evicts an idle instance or fails the record naming
  `maxActiveInstances` (the factory's wait for the evicted client's close remains). Its
  counters are `ThreadSafeSimpleCounter`s through the base helpers' supplier overloads; the sink
  surface keeps `SimpleCounter`. Capacity is `AsyncDataStream`'s, documented as
  `maxInFlightRequests` through the conditional helper; Flink's operator timeout must sit above
  `requestTimeout`. Preserve the original request and resolved destination through completion;
  outcome policies run after successful-RPC and predicate counters (ADR-0152).
- **`BigtableRequestFunction.timeout` completes the result on every path** (#1203, ADR-0148's
  refinement). The operator's retry mode (`*WithRetry`) holds an input between attempts under the
  same `ResultFuture`, and Flink has no fallback if the function returns without completing: with
  no ledger entry the function fails the input naming that no request was in flight, and when it
  finds the entry settled by an answer still being handed off it completes the result itself,
  saying the request answered as the timeout elapsed. The answer drops its ledger entry only after
  the hand-off, so a missing entry means nothing is in flight and nothing is completing, but
  releases the in-flight counts before it, so the next input finds an answered instance idle at
  the cap. Do not move the ledger removal before the hand-off, do not move the count release after
  it, do not make the settle-lost timeout yield or defer its completion to the answer, and do not
  count a no-entry or an answered-at-timeout timeout under any request counter; the refinement
  records the measurement or review finding behind each. Retry mode is the job's retry,
  supported; its semantics, and the Flink 1.20/2.2 difference in what a failure raised from
  `timeout` does, are on the DataStream page and in the refinement, not here.
- The ten runtime counters are registered with their `BigtableMetricNames` constant spelled at
  each `counter(...)` call: `check-metric-docs` reads the registrations and cannot see through a
  helper that takes the name as a parameter.
- `FailedRequest.getPayloadBytes()` remains `null`: the conditional model's job-graph encoding
  is not a dead-letter format (ADR-0152). `sink.write-mode` selects ordinary `upsert` or conditional
  `insert-if-absent`; #1177 and #1226 extend this same option. Keep request options mapped through
  `RequestOptionsMapper` and guard explicit mode-incompatible options with `ConditionalOptionChecks`.
- Conditional SQL keeps the ordinary family/qualifier schema and codec. Its unset RPC predicate
  tests the entire stored row, including undeclared families. Keep INSERT-only changelog handling,
  preserve repeated inputs through the planner and retain ADR-0149's per-cell writer clock.

## Batch knobs, and entries versus mutations (`docs/adr/0082`)

- **Every count this connector exposes counts entries; Bigtable's own limit counts mutations**, and
  the two are never reconciled here because **the client does it** — `MutateRowsBatchResource`
  flushes past 100,000 mutations whatever `batchElementCountThreshold` says, and `Mutation`
  refuses to build a single entry that large. So a mutation-counting layer on this side (the
  Spanner cell-weight shape, `docs/adr/0077`) is duplication, not a gap, and an over-limit
  `MutateRows` cannot be produced through this sink to be measured. **`BulkMutation` is no second
  guard.** Its running count is on the `add(ByteString, Mutation)` overload, and the batcher
  calls `add(RowMutationEntry)`, which counts nothing. The batch-level invariant therefore rests on
  that flush alone, which is why `BigtableClientMutationLimitTest` pins it rather than trusting it.
- The knobs that count are spelled for it: `maxInFlightEntries`, `inFlightEntries`,
  `parkedEntries`. "Mutation" stays the word for the *thing* — `FailedMutation`, `MutationBatcher`,
  a mutation the service refused — and "entry" is the word for what is *counted*. A new counter
  picks by that rule rather than by which reads better.
- **Both thresholds are bounded at the setter: `batchElementCountThreshold` ≤ 19,999 and
  `batchRequestByteThreshold` ≤ 100 MiB − 1**, with package-private `*_LIMIT` constants and the
  figure in the `@param`, following `OptionChecks`' rule that a public compile-time constant inlines
  into callers.
  **Both sit one below the client's flow-control budget**, at 20,000 entries / 100 MiB.
  The constants are written as subtractions rather than literals.
  `BigtableBatchingCallSettings.Builder.build()` requires each threshold
  to be *strictly* below its budget and throws otherwise. A value past either ceiling is not a loose
  batch but `Failed to create a Bigtable mutation batcher` on a task manager. Neither ceiling is a
  service figure, and none exists: Bigtable documents no per-request size at all. The pair
  `DefaultMutationBatcherFactoryTest` (the client accepts them) and `BigtableWriterOptionsTest`
  (they are these numbers) is what keeps a ceiling one-too-high from shipping.
- **The in-flight bounds are warned about at `build()`, never capped** — ADR-0077's second shape
  (refuse the illegal, warn about what cannot take effect as meant). A ceiling would be wrong here:
  the client's budget is **per client** and the sink holds one per (project, instance), so a
  multi-instance resolver legitimately exceeds one of them, and nothing at `build()` knows how many
  instances it will name. The comparison is `>`, measured: gax admits the whole budget and blocks
  only past it. The defaults must not trip it, or every task manager logs it — pinned by a test,
  not left to chance.

## Table auto-creation (`docs/adr/0073`)

- Off by default (`CREATE_NEVER`); `CREATE_IF_NEEDED` requires `tableCreateOptions` with ≥1
  family, and the reverse combination is rejected too — a disposition without a schema is the
  feature #233 argued against. GC rules travel in the sink's own `Serializable` `GcRule` model
  (the client's `GCRules` does not serialize); its `maxAge` converts seconds-and-nanos, never
  `toNanos()`, so ADR-0068's ceiling deliberately does not apply. Two admin-path tiers to reread
  on a BOM bump, both internal calls, tier-irrelevant under `docs/adr/0141`: the client's
  `GCRules` is class-level `@BetaApi`, and the admin methods the module calls —
  `createTable`, `getTable`, `modifyFamilies` on this ensure; `getTable` and `getAppProfile` on
  the change-stream coordinator's preflight — are `@ObsoleteApi`, pointed at the proto-based
  `getBaseClient()` route.
- **`NOT_FOUND` outranks everything in the classifier** — ahead of the transient-anywhere check,
  `PubSubErrorClassifier`'s precedence — and, unlike Pub/Sub's ADR-0006, **the disposition gates
  the parking itself** (no cascades, no ordering keys). `tableMissing` is the only thing that
  makes a repair call the admin; consumed per attempt, the ensure not repeated once it has
  succeeded — and a *failing* ensure spends an attempt from the recovery schedule rather than
  the job (the SDK gives the admin RPCs no retry layer of their own).
- **Repair before isolation, and the two queues never drain each other**: a solo `NOT_FOUND`
  from the isolation pass migrates to `pendingRepair` (the tripwire invariant survives verbatim);
  a repair's re-application can park entries for isolation. `flush()` loops over both passes;
  each is self-bounding. No `repairNeeded` flag — queue non-emptiness is the trigger, and since
  `docs/adr/0074` that conclusion rests on *no ordering keys* alone, not on one fixed table.
- **One repair covers every table an incident left missing**, and `ensuredThisRepair` is a **set**
  — a boolean creates the first and silently skips the rest, then dies naming undeclared column
  families (`docs/adr/0074`). A failing ensure re-arms the failed table *and every one it did not
  reach*. One `TableCreateOptions` serves every table; the budget is shared across them, so an
  unrepairable table abandons the others' parked work. A post-ensure missing-family response is
  matched against the entry and the ensure's live-family snapshot: an absent referenced family is
  undeclared by construction and fails immediately (#432), while an ambiguous `NOT_FOUND` retains
  the bounded retry.
- `ensureTable` is idempotent and **add-only**: `CreateTable` first, on `ALREADY_EXISTS`
  reconcile by reading live families and adding only the absentees in one atomic request (a
  blind add of one existing family fails the rest with it); an existing family's GC rule is
  never compared or updated. The SDK retries neither admin RPC (empty retryable sets) and a
  `RetryingTableAdmin` tier is deferred until a rate-limit shape is observed.
- **The reconciliation is self-bounding too**, at declared families + 1 — the exact bound its own
  termination argument asserts (a losing round shrinks the missing set strictly), enforced as a
  `for` budget with a tripwire rather than left as a comment (#414). Spending it is a
  contradiction, not a slow ensure, and it fails into the recovery schedule instead of holding the
  task thread — where the only symptom would be checkpoints that stop completing, never the
  reconciliation behind them. **Its three admin operations are functional values**, bound
  to a real client only by `ensureTable`: ADR-0047's shape, and the only seam a test can drive —
  the client is final and per-call, and nothing short of interposing on the RPC stream times a
  concurrent family addition to land between one call's read and its modify.
- A parked `NOT_FOUND` **is** counted under `errorClass.NOT_FOUND.errors` per entry (no identity
  to confirm — unlike ADR-0043's batched row-level exclusion); `parkedEntries` sums both
  queues; `columnFamiliesAdded` counts additions to a pre-existing table only.
- The emulator answers a missing **table** with the service-shaped `NOT_FOUND` fan-out (measured
  2026-08-08), so the emulator suite drives that repair end-to-end; the missing-**family** leg is
  gated-suite-only (emulator says `INTERNAL`).

## Stalled waits (`docs/adr/0078`)

- **The two mailbox waits report a stall; they do not bound one.** No knob, no sink-side timeout —
  measured: the client gives up on a stalled `MutateRows` at its own 10-minute total timeout
  (601 s black hole, 586 s refused), and `yield()` was already interruptible, so neither premise
  ADR-0052 answers for Pub/Sub holds here. What survives is ten minutes in which no counter moves
  and Flink's checkpoint timeout may fail the job first, naming nothing about Bigtable.
- **The report is not separable from the loop**: `yield()` never returns while nothing arrives, so
  both waits run `tryYield()` + a 1 ms park. Three things that shape carries and a rewrite must
  keep — **the loop reads `Thread.interrupted()` itself** (`tryYield` does not, `parkNanos` does not
  clear it; dropping it silently breaks cancellation, the one property the measurement found
  working); the idle time is read **after** `tryYield` comes back empty, never before; and the park
  interval is set by mail latency, not by the warning threshold.
- Progress is stamped on the **gax callback thread**, on failure as well as success — a failure is
  the client answering. `lastCompletionNanos` and the metric reporter's `activeClients` mirror are
  the two volatile cross-thread signals. Ordinary gauges also sample scalar task-thread state, but
  no reporter receives the mutable access-order map. The warning is rate-limited
  **writer-wide**, never per wait: one `flush()` can make a whole `maxInFlightEntries` of them.
- `awaitCapacity()` sends every live batcher once per wait; `drainInFlight()` does not, because its
  callers send immediately before.

## Solo confirmation and teardown (`docs/adr/0045`, `0046`, `0047`)

- A `ROW_LEVEL` verdict answering a batched submission is parked and confirmed solo by
  `runIsolationPass()` — **the discriminator is our own submission, never the exception's
  shape** (`docs/adr/0045`, which also carries `maxConsecutiveRejections`, the #361 bound on a
  dropping policy's pass: consecutive confirmed rejections, reset by any applied mutation, never
  serializer rejections, and never the pass's own loop budget — that one is an invariant
  tripwire, not a policy).
- The batcher's shutdown report (`BatchingException`) is absorbed and WARN-logged, never thrown;
  `InterruptedException` and gax's `IllegalStateException` still propagate; the contract is on
  `MutationBatcher.close()` (`docs/adr/0046`; the cross-connector rule is `docs/adr/0003`).
  **No `SinkWriter.close()` may `yield()`** — Flink quiesces the mailbox before closing
  operators, so a teardown drain parks forever.
- `BigtableBatcherAdapter` holds **four** functional values — `add`, `sendOutstanding`,
  `closeAsync`, `close` — and **no client**: the factory owns and closes those (`docs/adr/0074`
  reversed that half of `docs/adr/0047`, whose prose still describes the original three-plus-client
  shape). Its teardown closes through `Closers.closeAll`, not `try`/`finally` (`docs/adr/0047`). A
  row-count assertion is not evidence that a flush flushed — gax's 1 s delay-threshold timer
  hides a broken `sendOutstanding()` from every emulator IT.

## Metrics (`docs/adr/0043`; conventions in the base module's detailed guidance)

- Per-destination counters behind `perDestinationMetrics`, default off, and a `table(...)` sink is
  **not** excepted — "one table" is a property of a resolver the writer does not inspect
  (`docs/adr/0074`, refining `docs/adr/0043`); `errorClass` counts RPC failures only;
  `statusCode` reports the chain's outermost classifiable status; `close()` zeroes the
  gauge-backing counters **before** `Closers.closeAll`; every failure reaching the writer is
  counted except a batched row-level rejection, whose place `parkedEntries` takes.
  `activeClients` gauges the bounded instance set; `capacityEvictions` and `idleEvictions`
  count logical client removals, not table-batcher removals.

## Scan source (`docs/adr/0080`, `0083`)

- **One coordinator client belongs to one Change Streams enumerator** (`docs/adr/0128`).
  `BigtableChangeStreamSourceConfig` carries a `ChangeStreamCoordinatorClientFactory`, not a client,
  and it is not `@Nullable` any more: the source used to mint inline only when the configuration
  held none, which left a test-injected client shared by every enumerator. `DefaultChangeStreamCoordinatorClient`'s three lazy
  accessors are now guarded by its monitor behind a one-way closed flag, as its sibling seams were:
  they were a check-then-create against fields `close()` nulls, so a teardown between a check and
  its assignment closed nothing and left the client the reconciliation scan then assigned owned by
  no one, reaching Bigtable as ADC because `close()` nulls the credentials too. `volatile` does not
  make a compound operation atomic.

- **One sampler belongs to one enumerator** (`docs/adr/0128`). `BigtableSourceConfig` carries a
  `RowKeySamplerFactory`, not a sampler, and `BigtableScanSource` mints one in both
  `createEnumerator` and `restoreEnumerator`, closing it itself if the enumerator's constructor
  throws before taking it. `RowKeySampler` is deliberately not `Serializable`. The sticky flag is a
  level down, in `LazyBigtableDataClient`, whose reader-side holders —
  `DataClientRowStreamOpener` and the change-stream `DataClientChangeStreamOpener` — are
  unaffected, because a reader's copy is deserialized per task attempt.

- **The three lazy data-client seams rely on their owners' teardown, not on client leases**
  (`docs/adr/0142`). The scan reader stops its fetchers before closing the row-stream opener; the
  Change Streams reader cancels active reads before closing its opener; and the scan enumerator
  may close its sampler while asynchronous sampling is in flight, after which the shared
  enumerator ignores the completion. `LazyBigtableDataClient.close()` clears both the client and
  pushed credentials, and credential injection takes the same monitor so nothing can restore the
  holder's provider after close. That guarantee covers the holder's reference, not another seam
  that shares the same provider, such as the Change Streams restore resolver. A lease would
  duplicate those three lifecycle protocols and make the coordinator wait for a sampling RPC it
  currently abandons during teardown.

- **The assignment protocol is the base module's** (`docs/adr/0083`): `BigtableScanSplitEnumerator`
  extends `PullAssignmentSplitEnumerator` and supplies the sampling — `restore`, the sampling call,
  the plan and its report, the counters, its own `snapshotState`. What the bullets below say about
  assignment still holds; it is just no longer written here, so a change to it changes both sources
  and belongs in `flink-connector-gcp-base`.

- **A split is one row-key range and the range is the remaining work**; a checkpoint truncates it
  to start **exclusively** at the last successfully deserialized row key. No offset exists to resume
  at — `ReadRows` takes a range — so progress is measured in input rows, never in output records.
  Like BigQuery and Spanner, the deserializer may collect zero, one, or many records while one
  successful call advances input progress once. A truncated range **may be empty** and the reader
  finishes such a split **without opening a stream** — load-bearing, not tidy: the service refuses
  an inverted range with `INVALID_ARGUMENT` rather than answering it empty (#481), so a reader that
  opened one would fail the job. The builder rejects an empty *configured* range, and that
  asymmetry is deliberate.
- **A filter naming a column family the table lacks fails the read with `NOT_FOUND`** (#481) —
  documented behaviour, not a gap: the source deliberately does not pre-validate a filter's
  families against the table, which would cost every scan a metadata read to soften an error the
  service already reports precisely.
- **The split reader's delivered key and the split state's emitted key are two clocks**: reopen from
  the reader's or the element queue is handed over twice inside one *successful* run; checkpoint the
  reader's and in-flight rows are dropped.
- **A cancelled `ServerStream` is indistinguishable from an ended one** — measured: `cancel()` makes
  the iterator report the end, and a consumer already blocked gets a thrown error instead. The
  `cancelled` flag decides whether a split finished, never the stream's behaviour.
- **A restore never re-samples**, and the `planned` flag is not `!pending.isEmpty()`: it guards
  split-id stability against tablets that moved. `rowKeySamplesTaken` reports the same fact at
  runtime (`1` fresh, `0` restored).
- **Overlapping configured ranges are merged, not rejected** — nested prefixes otherwise emit their
  shared rows twice from one green run. Prefix→range is **always** the SDK's `ByteStringRange.prefix`
  (all-`0xFF` has no successor). `RowRanges` is the one home for the range algebra, and every range
  crossing a boundary is **copied**: the vendor type is mutable and its `clone()` is not public API.
- **No `Query` in the config or the split, and no options object.** The serialization trap the design
  predicted does **not** reproduce (block-data framing bounds the read) — the reasons that survive
  are format ownership, unreadability and mutability, so the guard is a reflective field test, not a
  round-trip. An empty `*Options` class fails `check-option-docs` outright.
- **`Query.limit()` stays deferred** and the SDK agrees: `shard` refuses a request carrying one.
  `maxRowsPerFetch` and `maxBytesPerFetch` instead bound each hand-off into Flink's element queue;
  the byte estimate is measured during SDK row materialization, a single oversized row progresses,
  and one look-ahead row may sit outside the target. The Table keys are bounded-scan-only and use
  `OptionSetters`.
- **Retries stay in the client** on this side too: `ReadRowsResumptionStrategy` resumes a broken
  stream from its last key. A sampling failure **fails the job**; no single-split fallback.
- **Nothing claims Data Boost was exercised** (#248). The testable statement is that a configured
  `appProfileId` reaches the wire, and only the gated suite can make it — the emulator ignores
  profiles.
- **Split planning is never an emulator test**: the emulator models no tablets (final key plus
  ~1-in-100 randoms). Since `583.0.0-emulators` every response also *trails* an end-of-table
  marker, so a three-row table answers `['c'@2, ''@3]` and an empty one `['']`, where
  `441.0.0-emulators` answered `['c'@2]` and nothing; the planner drops empty-key samples, so no
  plan moved. Measured 2026-08-09 and 2026-09-03. The gated table is **pre-split**; the failover
  ITCase scripts both seams, because one split cannot show a reassignment.

## Change Streams source (`docs/adr/0094`, `0097`)

- **The enumerator is the metadata store.** It checkpoints unassigned and assigned partitions,
  pending merge targets, missing-partition timers, a bounded run's completed ranges, the resolved
  start time and a monotonic split-id counter; there is no
  Beam-style metadata table or change-stream name. A restored plan never calls
  `GenerateInitialChangeStreamPartitions` again.
- **A merge target waits for coverage, not a token count.** Every `CloseStream` contributes a token
  whose own partition range names its parent; only a coalesced set covering the entire target can
  become one split with the full token list. A split is the same rule's one-token case.
- **The native SDK surface stops at the reader boundary.** `GenerateInitialChangeStreamPartitions`,
  `ReadChangeStream`, `ReadChangeStreamQuery`, `ChangeStreamMutation`, and its entry types carry an
  Apache-Beam-only `@InternalApi` annotation in the pinned client; its aggregate `Value` model is
  `@BetaApi`. Because the connector-owned public model mirrors that unstable surface, the whole
  change-stream public surface — `BigtableChangeStreamSource(+Builder)`, the deserialization
  schemas, `BigtableChangeStreamMutation` and its nested types — is `@PublicEvolving`, not
  `@Public` (`docs/adr/0141`, `docs/adr/0124`'s revision). The reader inspects complete SDK
  mutations, converts only retained entries to the
  connector-owned public model, and exposes no public raw-SDK escape. With no entry filters it
  bypasses filter evaluation but still performs the public-model conversion. Reread the input
  surface on every client upgrade and extend `BigtableChangeStreamMutationConverter` — the one
  fail-closed `instanceof` chain over the SDK's own types — for every new SDK entry or value
  subtype before accepting it.
- **The connector's own entries dispatch through a visitor, not `instanceof`** (ADR-0126).
  `BigtableChangeStreamMutation.Entry` and `Value` declare package-private `accept` methods and
  `ChangeStreamMutationDispatcher` carries the two `@Internal` visitor contracts, so adding a
  subtype fails to compile at every handler — the serializer's write half, the table envelope
  converter, and the selected-cell classifier — rather than reaching a runtime message. Callers
  outside the connector branch on `Entry.getKind()` or `Value.getType()`. Three things the visitor
  does **not** hold, all of which an added subtype still needs by hand: the serializer's
  `readEntry`/`readValue`/`copyEntry`/`copyValue` stay `switch`es over a wire tag, the copy path's
  `tag < SET_CELL || tag > MERGE_TO_CELL` bound refuses a sixth tag until it is widened, and the
  SDK-side converter above still needs editing — though not finding: see the next rule.
- **The client's own entry and value sets are pinned where the bump lands, not where the job runs.**
  `BigtableChangeStreamSdkEntrySurfaceTest` asserts `Value.ValueType`'s constants and the method set
  of `ChangeStreamRecordAdapter.ChangeStreamRecordBuilder` — the surface a `ChangeStreamMutation` is
  assembled through, so every entry kind the client can build is a method on it. That is what makes
  the SDK's method-less `Entry` marker interface pinnable without a classpath scan. It is the
  reader's half of the libraries-bom bump `BigtableWriterMutationCaseTest` covers for the sink, and
  the two additions it would have caught, `addToCell` and `mergeToCell`, arrived in the same client
  release that grew `Mutation.MutationCase`.
  A `mvn compile` without `clean` can miss the breakage — measured, an incremental build reported
  success while a clean one named all three handlers.
- **The application profile is required and single-cluster.** Preflight rejects a visible
  multi-cluster policy; missing permission to read profile metadata does not add a new requirement,
  and the reader translates the service rejection instead. Start-position and restore-expiry
  behavior is ADR-0094's shared contract.
- **`ChangeStreamRestoreResolver` wraps the base restore seam because this module also resolves
  splits at the reader** (`docs/adr/0138`): the enumerator calls the base API directly, and the
  wrapper adds only what the reader-side path needs — serializability, the reader's one
  credentials load, a split-shaped signature; the policy stays the base
  `StartPositionResolver`'s. Spanner resolves at the enumerator alone and uses the base API
  bare; do not read the wrapper as a second restore policy or port it there.
- **The change-stream opener holds the shared client holder rather than its own lifecycle**
  (`docs/adr/0131`, refined by #1046). `DataClientChangeStreamOpener` carried the module's third
  copy of the lazy client lifecycle — left out of the #956 fold because its credentials arrived by
  push while the scan seams still pulled, an asymmetry #974 removed. It measures clean against
  ADR-0131's question: `open(...)` retains nothing, every stream's lifecycle lives in the reader's
  `ActiveRead`, and nothing re-checks after an unguarded call. So it delegates to
  `LazyBigtableDataClient` like the scan source's two seams, and the closed-before-use failure is
  the holder's, naming the seam and the table. The seam has no emulator-endpoint source, so the
  holder is constructed with none.
- **The service partition always goes onto the wire as `[closed start, open end)`, even when an
  endpoint is empty.** The SDK uses an empty key for an infinite endpoint but the service still
  requires the protobuf boundary oneof to be set. `RowRanges.copyOf` intentionally normalizes the
  empty key for internal algebra, so the reader reconstructs the explicit boundary pair before
  building either a `ReadChangeStream` request or an SDK continuation token (#533).
- **That normalization is owed on the way *in*, not only on the way out** (#943). Every range the
  service hands back — `generateInitialPartitions`, and every `ChangeStreamContinuationToken`
  partition — is built by `ByteStringRange.create`, which unlike the four setters leaves an empty
  key as a *bounded* bound. So `ChangeStreamCoordinatorClient.generateInitialPartitions` declares
  normalized output, `DefaultChangeStreamCoordinatorClient` folds both its branches through
  `RowRanges.copyAll`, and the reconciler folds the token partitions it reads directly. Skipping it
  is silent: a table's last partition reads as a range ending at the empty key, so no gap is ever
  reported for it, and its first never matches the `MissingPartition` remembered from the previous
  scan, so neither grace period elapses and the partition is never restarted.
- **A bounded run's finished ranges stay in the reconciliation ledger** (#951). A partition that
  reaches `boundedTimestamp` closes with no successor, so it leaves `assigned` and nothing replaces
  it, while `generateInitialPartitions` goes on reporting that keyspace for as long as the table
  exists.
  The
  enumerator therefore records those ranges under `bounded`, the reconciler counts them as covered,
  and they are checkpointed so a restore keeps the account. Skipping it deadlocks the run: a
  non-empty missing ledger blocks `signalBoundedCompletionIfDrained`, and that signal is the only
  thing that stops the scans. **The `bounded` gate is load-bearing in the other direction** — a
  continuous run has no end time to close a stream at, so a successorless close there is a loss and
  must still be restarted.
- **A bounded completion signal is owed to readers that register after the ledger drains** (#1041).
  `signalBoundedCompletionIfDrained` broadcasts only to readers registered at that moment, while
  Flink registers a later reader in the enumerator context before calling `addReader` on both
  supported minors. `addReader` therefore replays no-more-splits when `boundedComplete` is already
  set. Without that replay, an empty late reader stays at `NOTHING_AVAILABLE` instead of reaching
  `END_OF_INPUT`, so the bounded job can remain unfinished.
- **Before rendering a byte string, read `RowRanges`' class javadoc and pick by who reads the
  result** (#1012, ADR-0080). Four forms, and the choice is not made by package: escaped
  (`RowRanges.format`) for a person reading a log line or an exception message; Base64 for a pattern
  the user writes (`family:qualifierBase64` — the row-key options take Base64 only when
  `scan.row-key-encoding` asks, its default being `UTF8`);
  `toStringUtf8()` only where the value is text by construction, which among row keys, qualifiers
  and cell values means a qualifier built from a DDL field name and nothing else; and **not rendered
  at all** in a value type a user's own code logs — `FailedMutation` prints the mutation's size,
  `BigtableChangeStreamMutation` has no `toString`. An exception message is the deliberate
  exception, having one chance to name the offending row and no accessors. **That last arm bounds a
  `toString`, not an object graph**: a `FailedMutation` from a serialization failure has a `null`
  `getRowKey()`, so its `getCause()` is the only place the row can be named — and whether it is
  depends on that message: the table sink's empty-mutation refusal carries the escaped key, its
  other refusals name none, and a `FailedMutation` may equally wrap what a user's own serializer
  threw. Measured on #1012.
  `toStringUtf8()` on anything else exposes and destroys the value at once, since invalid UTF-8
  becomes U+FFFD rather than failing.
- **`RowRanges` lives at the module root**, not under `source/`: it serves both source directions
  and both halves of the table layer — 26 importers in the main tree across ten packages — which is
  ADR-0055's rule for a type belonging to the connector as a whole. Moved there in #1012. Six
  packages do not import it, the DataStream `sink/` tree among them — which for that tree follows
  from the rendering rule above rather than being a separate one.
- **`RowRanges.format` is a renderer and never an identity** (#910). Range identity is
  `ByteStringRange.equals`; the rendering call sites are correct as they are, and the four
  `StartPositionResolver.resolveRestored` arguments are labels for a log line and a message, which
  that resolver never compares.
- **`escape` spends `*` on the sentinel, so it escapes `0x2A` like an unprintable byte** (#947).
  `format` prints `*` for an absent bound, and `*` is printable, so a bound *at* that key used to
  render identically to no bound — one string for two ranges, in exactly the warning an operator
  reads to tell two partitions apart. The renderer is injective over every reachable bound shape and
  a test asserts that as a property; correctness does not rest on it, and must not be made to.
- **The connector-owned mutation supplies its own tagged serializer.** It preserves all mutation
  metadata and the ordered typed entries and values in the pinned 2.80.0 input model. Its
  `@TypeInfo` annotation keeps later `TypeInformation.of(BigtableChangeStreamMutation.class)` calls
  on that serializer instead of reflective Kryo (#533, #586).
- **The connector-owned mutation is named for the product, and compares by value** (#921, #922). The
  SDK owns the plain `ChangeStreamMutation` name, so every boundary between the two used to qualify
  one of them; `BigtableChangeStreamMutation` lets both be imported. It carries `equals` and
  `hashCode`, and **deliberately no `toString`** — the row key and cell values are the row's own
  data, and the same rule already governs Spanner's `DataChangeRecord`. A redacting `toString`
  rendering only sizes was considered and declined: redaction is a property of one implementation,
  not an invariant, since no test can pin the absence of user data in a free-form string and every
  later widening of it is individually plausible. Do not add one to either type without settling
  both connectors together.
- **Entry filters are output projection, not an RPC predicate.** Family and qualified-column regexes
  run on the complete SDK mutation before retained entries are converted for the user deserializer.
  Qualified columns match `family:` plus canonical padded standard Base64; family deletes bypass
  qualifier filters. An empty projection is delivered by default or bypasses conversion and
  deserialization under the explicit skip flag, while both paths advance token and low-watermark
  state and report removed entries (#586).
- **Concurrency is a reader-subtask bound, not a service quota** (`docs/adr/0103`). The enumerator
  assigns the absolute free-slot count a reader advertises; the reader keeps excess restored or
  successor splits in FIFO order and rotates at emitted heartbeats. Each active read has at most
  one outstanding response, and the shared handover queue has the same bound as active reads.
  Remove a response before requesting the next one, and keep the physical read slot occupied until
  a cancelled RPC actually terminates. The five-second heartbeat that paces those rotations is an
  internal constant, deliberately not the builder option Spanner's is (#980); it is also the unit of
  `missedHeartbeatIntervals`, so read `docs/adr/0103` before exposing or changing it.
- **Delivered and emitted positions remain separate for every active Change Streams partition.**
  Only the task thread advances checkpointed state. An RPC failure retains the emitted split and
  fails the task without reporting a transition; connector cancellation can rotate or close a read
  but never completes it. Reader lag gauges use the minimum checkpointed assigned position, and
  enumerator lag uses the oldest unassigned position; both clamp clock skew and overflow.
- **An estimated low watermark is checkpoint progress and observability, not a Flink source
  watermark** (`docs/adr/0109`). Bigtable explicitly permits a future record below an earlier
  estimate, so neither a coordinator-wide minimum nor a finite delay can prove the non-early
  contract. The builder exposes no native-watermark opt-in, the Table source does not implement
  `SupportsSourceWatermark`, and applications own any ordinary watermark strategy and late-data
  policy they choose.

## Table API / SQL (`docs/adr/0086`, scan `docs/adr/0092`, Change Streams `docs/adr/0106`; shared rules `docs/adr/0014`, `docs/adr/0139`)

- The `table` layer maps onto the DataStream builders, never re-implements: one `ConfigOption` per
  setter, applied through `OptionSetters` (`docs/adr/0133`), no default restated. Table-owned options have no
  builder setter behind them and form a separate partition in `BigtableOptionParityTest`. The eight
  defaulted table-owned options are `scan.mode`, `null-string-literal`, `decode.trailing-bytes`,
  `scan.row-key-encoding`, `lookup.async`, `sink.cell-timestamp.truncate-to-millis` and
  `sink.insert-only-input-mode` and `sink.write-mode`; `scan.change-stream.changelog-mode` is deliberately required so
  selecting either Change Streams interpretation remains explicit. A mapped option gaining a
  default and a defaulted table-owned option losing its own both fail. "No default restated"
  covers the *description* as well (#1045, `docs/adr/0139`):
  `BigtableConnectorOptionsTest.noDescriptionRestatesADefault` rejects the shared restatement
  phrases, and a failure edits the description — the reference page (mapped options) or the table
  page's row (table-owned options) is where a default is written. The HBase-provenance sentence
  on `null-string-literal` is deliberately outside the rule: it says whose the default is, not
  what it is.
- **Change Streams defaults to neither interpretation: its DDL explicitly selects the exact
  insert-only mutation envelope or the constrained selected-cell upsert contract** (#600, #603,
  ADR-0106).
  It preserves the row key and ordered `SetCell`, `DeleteCells`, `DeleteFamily`, `AddToCell` and
  `MergeToCell` entries through discriminated raw-value, raw-timestamp and integer fields.
  It never claims row-level update or delete semantics without before and full after images.
  Reject a primary key, any physical schema change, an unknown future SDK entry/value subtype and
  every option owned by the bounded scan instead of widening or ignoring the contract.
  **A Change Streams table is source-only**: reject `scan.mode = change-stream` on the sink path,
  and reject the Change Streams options there too. Keep the scan and lookup options a sink cannot
  act on accepted — one table legitimately scans and writes under separate application profiles.
- **Selected-cell mode is stateless because the producer supplies the complete logical value.**
  Require exactly one physical primary key and at least one non-key field; decode the mutation row
  key with `CellValueCodec` and the non-key fields with insert-only `value.format`.
  Accept only one full selected-column or selected-family delete followed by exactly one selected
  `SetCell` for `UPDATE_AFTER`, or that delete alone for a key-only `DELETE`.
  Ignore unrelated entries and fail on every ambiguous selected-cell mutation, wrong source
  cluster, selected-cell GC or aggregate mutation, and zero/multiple/null format output.
  Do not add a lookup, snapshot bootstrap, old-value state, or format metadata.
- **Readable Change Streams metadata is scalar mutation data, not source protocol state** (#601,
  ADR-0106). `mutation-type` is `STRING NOT NULL`; `source-cluster-id` is nullable and maps the GC
  model's empty id to null; `commit-timestamp` and `estimated-low-watermark` are
  `TIMESTAMP_LTZ(9) NOT NULL`; and `tie-breaker` is `INT NOT NULL`. Append only the planner-selected
  keys in the order it supplies and retain `java.time.Instant` nanoseconds. Continuation tokens
  remain checkpoint state, and a partition's estimated low watermark does not implement
  `SOURCE_WATERMARK()` (#604).
- **The DDL model and the cell encoding are Flink's HBase connector's, and the encoding is
  normative** — one atomic column is the row key, every `ROW<...>` column is a family, cell bytes
  are `Bytes` as `HBaseSerde` applies them. `HBaseSerde` is the interop target, **not**
  `HBaseTypeUtils`: the two disagree on `DATE` and `TIME`, and only the first is what a Flink SQL
  HBase job writes. `CellValueCodecTest`'s golden vectors are the record; a round-trip test would
  pass with the interop broken. Three traps they pin: `true` is `0xFF`; a `TINYINT` must not go
  through a numeric overload (a `byte` widens to `short`); and the length rules are HBase's own
  (ADR-0136, #1037) — `Bytes.toBoolean` rejects any length but one while the other fixed-width
  decoders read an overlong value's prefix, a tolerance `decode.trailing-bytes = reject` opts out
  of uniformly. The policy travels beside the declared type through `TypedFieldDecoder`; do not
  add a decode path that resolves without it. **The interop is the byte layouts, not
  the error policy**: a decimal overflowing its declared `DECIMAL(p, s)` is a decode failure —
  `IllegalArgumentException` from the codec, wrapped by the address-naming guards — never the
  silent `NULL` the HBase connector reads, which aliased real data onto the empty-cell convention
  and put a null in a NOT NULL row key (#1038, ADR-0135). Rescaling to the declared scale rounds
  and is not an overflow by itself; the overflow is judged after rounding, so a rounding carry
  can overflow a value whose stored digits look representable.
- `BigtableTableSchema` and `CellValueCodec` sit at the **`table` root**, not in a subpackage: both
  directions share them and neither may import the other (ADR-0055's module-root rule one level
  down). A colon in a family name is rejected there — `familyNameRegexFilter` refuses one even
  escaped, so such a family would be writable and never selectively readable.
- **Upsert for an updating query, and a `-D` deletes the whole row.** `UPDATE_BEFORE`, a null row
  key, a row key encoding to zero bytes and **a row whose every family is null** each fail the
  record rather than skipping it; the HBase connector drops two of them, which leaves an incomplete
  table under a green job, and the last would otherwise reach the service as a mutation-less entry
  and return an `INVALID_ARGUMENT` naming nothing.
- **An insert-only query is upsert by default, with a table-local insert-only compatibility mode**
  (#496, ADR-0102). FLIP-558's 2.3 planner may demand `ON CONFLICT` of that default when it cannot
  infer the query's upsert key, including a plain `INSERT .. VALUES`; that is the cost of exposing
  Flink's conflict strategies and Bigtable's physical upsert capability. The
  `sink.insert-only-input-mode = insert-only` escape restores #488's append answer for an
  INSERT-only requested changelog, so a clause-less statement is portable across 1.20, 2.2 and
  2.3; it must never narrow an updating requested changelog. The existing
  `CrossVersionChangelogMode` remains the only per-major API seam. The planner-wide alternative is
  `table.exec.sink.require-on-conflict = false`, which older versions ignore. The docs page's ON
  CONFLICT section is owed an edit with any change here.
- **Whether a delete may carry the upsert key alone is answered by the DDL's primary key** (#470).
  Declaring one makes that key the row key; declaring none lets the planner key its upserts on
  whatever the query is unique by, so the sink asks for whole rows and the planner completes each
  one — measured on 2.2.1, that is a `ChangelogNormalize` on a query carrying deletes and nothing
  at all on an insert-only one. Answering `true` unconditionally, as this layer did until #470,
  sends a delete with a null row key — measured end to end, the job dies on "The row-key column
  'rowkey' is null". **The completion is from what the job has seen**, so a `-D` for a key this job
  never inserted is dropped; that is the planner's behaviour and it already applied wherever a
  primary key was declared, which is why a test proving `deleteRow` must ride the insert and the
  delete on one stream or use a retract source. **`ChangelogMode.upsert(boolean)` and `keyOnlyDeletes()` do
  not exist on the 1.20 LTS build** — naming either anywhere, including in a test, breaks that
  build and not this one, which is why the answer goes through `CrossVersionChangelogMode`, package-private beside
  its only caller as `CrossVersionCheckpointId` is — unlike `CrossVersionSink`, which is public
  because sinks in sibling packages implement it.
- **Two rows for one key in one `MutateRows` have no defined winner** (the proto says entries may be
  applied in any order, even for the same row) and, inside a millisecond, no second cell version
  either. An integration test that needs an order sends them from **separate jobs**; one that
  batches them is asserting the emulator's submission order. **Separate *requests* are not enough,
  and `sink.batching.element-count-threshold` = `1` is not the escape hatch it looks like** —
  measured on #470's follow-up, one entry per request made a delete stop taking effect on the 1.20
  build, because the requests one job has in flight are concurrent rather than ordered. A test that
  cannot use two jobs — the table layer's delete test, since `ChangelogNormalize` knows only what
  its own job has seen — asserts something order-independent instead. Writable `timestamp`
  metadata is a nullable `TIMESTAMP_LTZ(6)` applied identically to every cell a row writes and
  ignored by a delete; absent or null metadata takes the writer clock the schema stamps itself,
  millisecond-aligned, through the transient `CellClock` seam (ADR-0149) — not the client
  library's. This connector stamps only mutations it builds: a DataStream serializer that builds
  its own `RowMutationEntry` owns its timestamps and still gets the client's clock.
  The client reuses one mutation for its own retry, but Flink replay serializes again, so replay
  idempotence requires a stable explicit record timestamp. Bigtable validates millisecond
  granularity by default; `sink.cell-timestamp.truncate-to-millis=true` explicitly opts into
  dropping the final three microsecond digits. **#471 measured but did not convert the
  observation into a guarantee** (ADR-0093): 86,196 same-row pairs, mirrored across request sizes
  2 through 19,998, produced zero reversals on real Bigtable. The sink retains the bulk path and
  the caveat; a permanent test asserting submission order would contradict the service contract.
- **Table creation takes its families from the DDL and its rule from two keys**, unioned when both
  are set, and **at least one is required** under `create-if-needed` — stricter than the DataStream
  API, because an at-least-once upsert sink writes another version on every replay. Defaulting the
  rule instead was declined: that would be this layer inventing a default rather than mapping one.
- **The table source serves projection as a family filter** (`docs/adr/0092`): retained families
  become an interleave of `exactMatch`, a projection retaining **no** family becomes the keys-only
  chain (`cellsPerRow(1)` + `value().strip()`) — an empty interleave would drop every row, not
  strip them — and the filter is applied even unprojected, which is what keeps undeclared families
  off the wire for `SELECT *` and makes the declared-but-absent-family `NOT_FOUND` uniform. The
  converter resolves the *original* schema plus a projected-index array; never re-derive a
  narrowed `BigtableTableSchema` — `of()` rejects the rowkey-less and empty shapes a projection
  legitimately produces. A family none of whose declared qualifiers has a cell reads as a `null`
  field (the sink's mirror; `HBaseSerde`'s row-of-nulls declined), and the latest cell version is
  chosen by the converter — `cellsPerColumn(1)` pushdown is a deferred follow-up, not a gap.
  Range keys default to UTF-8; `scan.row-key-encoding=BASE64` accepts only canonical padded RFC
  4648 standard Base64 and retains the decoded `ByteString` across scans and every lookup cache
  mode. `scan.row-ranges` is a semicolon-separated union of `[start,end)` entries with backslash
  escapes for grammar characters; one endpoint may be omitted, and diagnostics name the one-based
  entry. It is additive with prefixes and the legacy single-range pair before the existing
  normalization. The factory rejects a bound or prefix element that decodes empty because the
  client silently widens one to the whole table. **The family filter decides row membership,
  not only row width**: a row appears iff a retained family has a cell, and a keys-only query
  sees every physical row — the wide-column model's row existence, pinned by the emulator ITCase.
  HBase makes membership projection-dependent too but adds declared qualifiers individually; a
  retained family holding only an undeclared qualifier therefore appears here and not there. The
  compensating labelled-branch mapping was declined (ADR-0092).
- **Filter pushdown is exact for safe row-key predicates and best-effort for cells** (#518,
  refinements in `docs/adr/0092` and `docs/adr/0095`). Direct field-literal equality, inequality,
  `IN` and null tests become row ranges when their byte representation is exact. Ordering is
  limited to `VARCHAR` and `VARBINARY`. Fixed-width integer and temporal equality uses a prefix
  range because their decoders ignore suffix bytes by default — and under
  `decode.trailing-bytes = reject` those predicates stay residual instead, because no range is
  exact for a fixed-width key there: the prefix set admits a suffix-bearing key as an `=` match
  (unvalidated when a projection drops the key) and its complement excludes it from a `<>` scan
  that must fail on it. The converter validates the row key under `reject` even when the
  projection dropped it (ADR-0136). An empty string or binary literal remains
  residual because the SDK normalises an empty bound to unbounded, so pushing one down would widen
  the scan rather than narrow it — an SDK fact, not an emulator one. `CHAR`, `BINARY`,
  `BOOLEAN`, `DECIMAL` and floating point remain residual. Configured prefixes and configured
  ranges remain a union, then intersect with exact SQL ranges. Positive family or qualifier predicates become
  necessary existence filters but also remain residual: never push raw values across codec nulls,
  byte-order differences or cell versions. Compose the existence predicate as a conditional whose
  true branch is the projection filter, and preserve that plan in a FULL loader created from the
  filtered source. Flink 2.2 keeps extra temporal-join predicates in `LookupJoin.where` rather than
  passing them to this ability; all cache modes evaluate that residual. Point lookup membership
  uses `RowRanges.contains`, not the stricter split-planning `cuts`; a closed-start key belongs to
  the range.
- `BigtableOptionParityTest` reflects over **four** surfaces, widening the Pub/Sub precedent (which
  reflects over options builders only), and **two** further assertions ride along: no option feeds
  two setters, and every option that feeds something other than one setter is accounted for. Adding
  a setter to `BigtableSinkBuilder` now costs either an option or an exemption carrying its reason;
  `BigtableSourceBuilder` joins when the `scan.*` options do.
- The module's only new compile-scope dependency is `flink-table-common` at `provided` — measured:
  the whole surface this layer needs, up to `DefaultLookupCache`, is in it, so `flink-table-runtime`
  stays test scope and nothing here needs a `flink-api-tiers.toml` entry.

## Explicit service-account credentials (`docs/adr/0086`)

- ADC remains the default.
  `serviceAccountKeyFile(...)` and the shared Table option `service-account-key-file` carry only a
  path through Flink serialization; never serialize parsed credentials, key JSON or a provider.
- Load and scope the service-account key once per runtime component, and push the provider into
  every seam that component owns; a seam carries no path and loads nothing itself.
  Share one loaded provider across every client family that component owns: sink data and
  table-admin; scan sampling and reading; lookup data clients including FULL cache; Change Streams
  coordinator data, table-admin and instance-admin; and reader stream and restore clients.
  The seam interfaces declare the injection (`ChangeStreamOpener`, `RowStreamOpener` and
  `RowKeySampler` abstractly, `ChangeStreamRestoreResolver` defaulted because resolving can be a
  pure function of the split), so no caller reaches a seam by downcast.
- A configured path must be nonblank and is mutually exclusive with emulator mode.
  Credential-loading failures use the stable sanitized message and carry no cause, path or key
  material.
- Adding another Bigtable client family requires extending the module-local credential loader's
  scope union and adding a direct settings-injection assertion. The single-row client factory is
  the precedent: `DefaultSingleRowClientFactoryTest.injectsTheRuntimeCredentialProvider` asserts
  the provider reaches its settings, and `SingleRowRequestSinks` loads the key before it opens
  anything so a missing file fails with nothing to release.

## E2E and emulator (`docs/adr/0044`)

- The gated suite creates an ephemeral instance per gated **class** (`flink-it-<epoch>-<runId>`
  naming + two-hour sweep); nothing persistent exists to run against. The gate variables cost
  nothing by themselves since #245's `@Tag("gated")`.
- The Table-source gated class shares one ephemeral instance across its scan and Change Streams
  acceptance. The Change Streams test uses a separate single-cluster profile and table, and a
  finite end timestamp so SQL collection terminates without cancellation.
- What real Bigtable answers each rejection with is measured, not inferred; client-side
  `Mutation` limits never reach the wire and arrive as serialization failures.
- `BigtableEmulatorDeviationITCase` asserts what the *emulator* does (INTERNAL instead of
  `INVALID_ARGUMENT`/`NOT_FOUND`, per offending entry rather than per request) so an image bump has
  to declare a change — the emulator-is-not-an-authority rule enforced, not breached. It works: the
  2026-09-03 bump to `583.0.0-emulators` failed here because the emulator stopped accepting an
  empty row key **on the mutate paths** — `ReadModifyWriteRow` still accepts one, which is why that
  half moved to `BigtableEmulatorReadDeviationITCase` rather than being deleted (#1196). Treat a
  failure in either class as a measurement to record, never as a test to relax, and check whether
  the deviation moved before concluding it closed.
- `StubWriterInitContext` cannot drive this writer; emulator tests inject through
  `createWriter(batcher, mailbox, metricGroup)`, and the MiniCluster job tests cover the
  production path.
