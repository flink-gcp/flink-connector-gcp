# CLAUDE.md — flink-connector-gcp-bigtable

Module-scoped guidance, loaded when Claude works in this module. Repository-wide rules
(build, workflow, version policy, licensing, package layout) stay in the root `CLAUDE.md`.
This file holds the rules a session must follow; each decision's record — context, evidence,
declined alternatives — is the named ADR under `docs/adr/` or the docs page.

## Sink design (`docs/adr/0041`, `0042`, `0074`)

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
  (`docs/adr/0074`): `table(...)` is sugar for a `FixedDestinationResolver` and the two setters are
  last-writer-wins; **resolve runs before serialize** (`FailedMutation` checkNotNulls its
  destination) with the null-skip still ahead of the pool; a `null` destination fails the write and
  is never routed; **no `instanceof` fast path**. The in-flight bounds stay **writer-global** —
  that is what keeps `drainInFlight()` meaning "the writer is empty" and the park bound one number.
  The adapter holds **no client**: the factory owns and closes them, or the first batcher to close
  kills its instance's siblings invisibly. `close()` is one `Closers` list, every `shutdown()`
  before any `close()`; the isolation pass's opening `sendOutstanding()` covers **every** live
  batcher, and missing one either hangs the task thread or trips the tripwire on a healthy stream.
- **`INVALID_ARGUMENT` alone is routed, `FAILED_PRECONDITION` deliberately not** — cite gRPC's
  state-independence definition and AIP-194, never the plausibility of what a code names. The
  routing condition takes both halves, reading the chain differently (`docs/adr/0042`).

## Batch knobs, and entries versus mutations (`docs/adr/0082`)

- **Every count this connector exposes counts entries; Bigtable's own limit counts mutations**, and
  the two are never reconciled here because **the client does it** — `MutateRowsBatchResource`
  flushes past 100,000 mutations whatever `batchElementCount` says, and `Mutation` refuses to build
  a single entry that large. So a mutation-counting layer on this side (the Spanner cell-weight
  shape, `docs/adr/0077`) is duplication, not a gap, and an over-limit `MutateRows` cannot be
  produced through this sink to be measured. **`BulkMutation` is not a second guard**: its running
  count is on the `add(ByteString, Mutation)` overload, and the batcher calls
  `add(RowMutationEntry)`, which counts nothing — so the batch-level invariant rests on that flush
  alone, which is why `BigtableClientMutationLimitTest` pins it rather than trusting it.
- The knobs that count are spelled for it: `maxInFlightEntries`, `inFlightEntries`,
  `parkedEntries`. "Mutation" stays the word for the *thing* — `FailedMutation`, `MutationBatcher`,
  a mutation the service refused — and "entry" is the word for what is *counted*. A new counter
  picks by that rule rather than by which reads better.
- **`batchElementCount` ≤ 19,999 and `batchByteSize` ≤ 100 MiB − 1, at the setter**, with
  package-private `*_LIMIT` constants and the figure in the `@param` (`OptionChecks`' rule: a
  public compile-time constant inlines into callers). **Both are one under the client's
  flow-control budget** (20,000 entries / 100 MiB), written as that subtraction rather than as
  literals, because `BigtableBatchingCallSettings.Builder.build()` requires each threshold to be
  *strictly* below its budget and throws otherwise — so a value past either ceiling is not a loose
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
  `toNanos()`, so ADR-0068's ceiling deliberately does not apply.
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
  unrepairable table abandons the others' parked work.
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
  the client answering. `lastCompletionNanos` is the only field of this writer not confined to the
  task thread. The warning is rate-limited **writer-wide**, never per wait: one `flush()` can make a
  whole `maxInFlightEntries` of them.
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
- `BigtableBatcherAdapter` holds three functional values + an `AutoCloseable` client, and its
  teardown closes through `Closers.closeAll`, not `try`/`finally` (`docs/adr/0047`). A
  row-count assertion is not evidence that a flush flushed — gax's 1 s delay-threshold timer
  hides a broken `sendOutstanding()` from every emulator IT.

## Metrics (`docs/adr/0043`; conventions in the base module's CLAUDE.md)

- Per-destination counters behind `perDestinationMetrics`, default off, and a `table(...)` sink is
  **not** excepted — "one table" is a property of a resolver the writer does not inspect
  (`docs/adr/0074`, refining `docs/adr/0043`); `errorClass` counts RPC failures only;
  `statusCode` reports the chain's outermost classifiable status; `close()` zeroes the
  gauge-backing counters **before** `Closers.closeAll`; every failure reaching the writer is
  counted except a batched row-level rejection, whose place `parkedEntries` takes.

## Scan source (`docs/adr/0080`, `0083`)

- **The assignment protocol is the base module's** (`docs/adr/0083`): `BigtableScanSplitEnumerator`
  extends `PullAssignmentSplitEnumerator` and supplies the sampling — `restore`, the sampling call,
  the plan and its report, the counters, its own `snapshotState`. What the bullets below say about
  assignment still holds; it is just no longer written here, so a change to it changes both sources
  and belongs in `flink-connector-gcp-base`.

- **A split is one row-key range and the range is the remaining work**; a checkpoint truncates it
  to start **exclusively** at the last emitted key. No offset exists to resume at — `ReadRows` takes
  a range — so progress is measured in rows, never in records, which is what lets the deserializer
  be collector-shaped where BigQuery's cannot be. A truncated range **may be empty** and the reader
  finishes such a split **without opening a stream**; the builder rejects an empty *configured*
  range, and that asymmetry is deliberate.
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
- **`Query.limit()` stays deferred** and the SDK agrees: `shard` refuses a request carrying one. The
  per-fetch row cap is a correctness floor reachable only through a `@VisibleForTesting` setter.
- **Retries stay in the client** on this side too: `ReadRowsResumptionStrategy` resumes a broken
  stream from its last key. A sampling failure **fails the job**; no single-split fallback.
- **Nothing claims Data Boost was exercised** (#248). The testable statement is that a configured
  `appProfileId` reaches the wire, and only the gated suite can make it — the emulator ignores
  profiles.
- **Split planning is never an emulator test**: the emulator models no tablets (final key plus
  ~1-in-100 randoms; *no samples at all* for an empty table, measured 2026-08-09 against the pinned
  image). The gated table is **pre-split**; the failover ITCase scripts both seams, because one
  split cannot show a reassignment.

## E2E and emulator (`docs/adr/0044`)

- The gated suite creates an ephemeral instance per gated **class** (`flink-it-<epoch>-<runId>`
  naming + two-hour sweep); nothing persistent exists to run against. The gate variables cost
  nothing by themselves since #245's `@Tag("gated")`.
- What real Bigtable answers each rejection with is measured, not inferred; client-side
  `Mutation` limits never reach the wire and arrive as serialization failures.
- `BigtableEmulatorDeviationITCase` asserts what the *emulator* does (INTERNAL instead of
  `INVALID_ARGUMENT`/`NOT_FOUND`; accepts an empty row key) so an image bump has to declare a
  change — the emulator-is-not-an-authority rule enforced, not breached.
- `StubWriterInitContext` cannot drive this writer; emulator tests inject through
  `createWriter(batcher, mailbox, metricGroup)`, and the MiniCluster job tests cover the
  production path.
