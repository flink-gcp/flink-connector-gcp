# Detailed guidance — flink-connector-gcp-bigtable

Module-scoped guidance, read when working in this module. Repository-wide rules
(build, workflow, version policy, licensing, package layout) stay in the root `AGENTS.md`.
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

## Metrics (`docs/adr/0043`; conventions in the base module's detailed guidance)

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

## Change Streams source (`docs/adr/0094`, `0097`)

- **The enumerator is the metadata store.** It checkpoints unassigned and assigned partitions,
  pending merge targets, the resolved start time and a monotonic split-id counter; there is no
  Beam-style metadata table or change-stream name. A restored plan never calls
  `GenerateInitialChangeStreamPartitions` again.
- **A merge target waits for coverage, not a token count.** Every `CloseStream` contributes a token
  whose own partition range names its parent; only a coalesced set covering the entire target can
  become one split with the full token list. A split is the same rule's one-token case.
- **The native SDK surface is accepted deliberately.** `GenerateInitialChangeStreamPartitions`,
  `ReadChangeStream`, `ReadChangeStreamQuery` and the record models carry an Apache-Beam-only
  `@InternalApi` annotation in the pinned client. Reread that fact on every client upgrade, as
  ADR-0041 requires for the sink's checked SDK facts.
- **The application profile is required and single-cluster.** Preflight rejects a visible
  multi-cluster policy; missing permission to read profile metadata does not add a new requirement,
  and the reader translates the service rejection instead. Start-position and restore-expiry
  behavior is ADR-0094's shared contract.
- **The service partition always goes onto the wire as `[closed start, open end)`, even when an
  endpoint is empty.** The SDK uses an empty key for an infinite endpoint but the service still
  requires the protobuf boundary oneof to be set. `RowRanges.copyOf` intentionally normalizes the
  empty key for internal algebra, so the reader reconstructs the explicit boundary pair before
  building either a `ReadChangeStream` request or an SDK continuation token (#533).
- **The built-in raw-mutation deserializer supplies its own serializer.** SDK mutations contain
  immutable collection implementations that Flink's reflective Kryo path cannot copy. The
  `ChangeStreamMutationDeserializationSchema` therefore treats the immutable model as copy-safe and
  uses its Java-serialization contract for network boundaries (#533).

## Table API / SQL (`docs/adr/0086`, scan `docs/adr/0092`; shared rules `docs/adr/0014`)

- The `table` layer maps onto the DataStream builders, never re-implements: one `ConfigOption` per
  setter, `getOptional(...).ifPresent(...)`, no default restated. The **four** table-owned options
  are `null-string-literal`, `scan.row-key-encoding`, `lookup.async` and
  `sink.cell-timestamp.truncate-to-millis`, which have no builder default behind them, and
  `BigtableConnectorOptionsTest` asserts that partition **exactly** rather than exempting it — a
  mapped option gaining a default and a table-owned one losing its own both fail.
- **The DDL model and the cell encoding are Flink's HBase connector's, and the encoding is
  normative** — one atomic column is the row key, every `ROW<...>` column is a family, cell bytes
  are `Bytes` as `HBaseSerde` applies them. `HBaseSerde` is the interop target, **not**
  `HBaseTypeUtils`: the two disagree on `DATE` and `TIME`, and only the first is what a Flink SQL
  HBase job writes. `CellValueCodecTest`'s golden vectors are the record; a round-trip test would
  pass with the interop broken. Two traps they pin: `true` is `0xFF`, and a `TINYINT` must not go
  through a numeric overload (a `byte` widens to `short`).
- `BigtableTableSchema` and `CellValueCodec` sit at the **`table` root**, not in a subpackage: both
  directions share them and neither may import the other (ADR-0055's module-root rule one level
  down). A colon in a family name is rejected there — `familyNameRegexFilter` refuses one even
  escaped, so such a family would be writable and never selectively readable.
- **Upsert for an updating query, and a `-D` deletes the whole row.** `UPDATE_BEFORE`, a null row
  key, a row key encoding to zero bytes and **a row whose every family is null** each fail the
  record rather than skipping it; the HBase connector drops two of them, which leaves an incomplete
  table under a green job, and the last would otherwise reach the service as a mutation-less entry
  and return an `INVALID_ARGUMENT` naming nothing.
- **An insert-only query is answered with insert-only, and the answer is load-bearing on Flink
  2.3** (#488): FLIP-558's planner demands `ON CONFLICT` of an upsert sink with a `PRIMARY KEY`
  even for append input whenever it cannot infer the query's upsert key, and the append answer is
  what takes its sink-is-append early return instead — do not "simplify" `getChangelogMode` back
  to one unconditional mode — it is also what Flink's own HBase connector answers, by echoing the
  requested kinds. The measured trade (ADR-0086): an insert-only statement cannot carry an `ON
  CONFLICT` clause into this sink on 2.3, which costs `DO NOTHING` and `DO ERROR` and costs
  nothing for `DO DEDUPLICATE`. An updating query with an uninferrable upsert
  key meets the demand as Flink designed — but a keyed source satisfies it, upsert-key inference
  being unique-key metadata even on a retract stream, which is why the ITCase suite needs no
  escape option. The docs page's ON CONFLICT section is owed an edit with any change here.
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
  and `sink.batching.element-count` = `1` is not the escape hatch it looks like** — measured on
  #470's follow-up, one entry per request made a delete stop taking effect on the 1.20 build,
  because the requests one job has in flight are concurrent rather than ordered. A test that cannot
  use two jobs — the table layer's delete test, since `ChangelogNormalize` knows only what its own
  job has seen — asserts something order-independent instead. Writable `timestamp` metadata is a
  nullable `TIMESTAMP_LTZ(6)` applied identically to every cell a row writes and ignored by a
  delete; absent or null metadata keeps the three-argument `setCell` writer clock. The client
  reuses one mutation for its own retry, but Flink replay serializes again, so replay idempotence
  requires a stable explicit record timestamp. Bigtable validates millisecond granularity by
  default; `sink.cell-timestamp.truncate-to-millis=true` explicitly opts into dropping the final
  three microsecond digits. **#471 measured but did not convert the
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
  range because their decoders ignore suffix bytes. An empty string or binary literal remains
  residual because the SDK cannot bound the empty key that the emulator accepts. `CHAR`, `BINARY`,
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
- Load and scope the service-account key at each runtime client-creation boundary.
  Share one loaded provider across every client family that component owns: sink data and
  table-admin; scan sampling and reading; lookup data clients including FULL cache; Change Streams
  coordinator data, table-admin and instance-admin; and reader stream and restore clients.
- A configured path must be nonblank and is mutually exclusive with emulator mode.
  Credential-loading failures use the stable sanitized message and carry no cause, path or key
  material.
- Adding another Bigtable client family requires extending the module-local credential loader's
  scope union and adding a direct settings-injection assertion.

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
