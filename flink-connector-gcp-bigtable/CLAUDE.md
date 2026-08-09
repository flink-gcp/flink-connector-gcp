# CLAUDE.md — flink-connector-gcp-bigtable

Module-scoped guidance, loaded when Claude works in this module. Repository-wide rules
(build, workflow, version policy, licensing, package layout) stay in the root `CLAUDE.md`.
This file holds the rules a session must follow; each decision's record — context, evidence,
declined alternatives — is the named ADR under `docs/adr/` or the docs page.

## Sink design (`docs/adr/0041`, `0042`)

- Implemented, never adopted or vendored; the serializer SPI keeps
  `BaseRowMutationSerializer`'s shape, and null = skip is `docs/adr/0001`'s contract.
- The four SDK facts (`@BetaApi` batcher acquisition, `@InternalExtensionOnly` `Batcher` — hence
  the `MutationBatcher` SPI and `sendOutstanding()` over `flush()`, the client's own blocking
  flow controller — keep the writer's caps below it, and `@InternalApi`
  `RowMutationEntry.toProto()`) are checked, not assumed; reread them on a client upgrade
  (`docs/adr/0041`).
- **The writer's per-record `toProto()` is one of four identical constructions, three of them the
  client's own, and its cost is measured** — so it is not re-argued as an optimisation without
  engaging those numbers, and no local fix exists to argue for: the entry exposes neither its key,
  its mutations nor its size. The lever is upstream (#236, `docs/adr/0041`).
- **Retries stay in the client**: no retry knobs for what the client retries. The `recovery*`
  knobs are not that — they budget the sink-owned auto-creation repair (`docs/adr/0073`), the
  same `recovery*`-vs-`retry*` line the BigQuery options draw. One fixed table per sink;
  `TableDestination` sits at the module root and `appProfileId` is a builder option, not part of
  it.
- **`INVALID_ARGUMENT` alone is routed, `FAILED_PRECONDITION` deliberately not** — cite gRPC's
  state-independence definition and AIP-194, never the plausibility of what a code names. The
  routing condition takes both halves, reading the chain differently (`docs/adr/0042`).

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
  each is self-bounding. No `repairNeeded` flag — queue non-emptiness is the trigger.
- `ensureTable` is idempotent and **add-only**: `CreateTable` first, on `ALREADY_EXISTS`
  reconcile by reading live families and adding only the absentees in one atomic request (a
  blind add of one existing family fails the rest with it); an existing family's GC rule is
  never compared or updated. The SDK retries neither admin RPC (empty retryable sets) and a
  `RetryingTableAdmin` tier is deferred until a rate-limit shape is observed.
- A parked `NOT_FOUND` **is** counted under `errorClass.NOT_FOUND.errors` per entry (no identity
  to confirm — unlike ADR-0043's batched row-level exclusion); `parkedMutations` sums both
  queues; `columnFamiliesAdded` counts additions to a pre-existing table only.
- The emulator answers a missing **table** with the service-shaped `NOT_FOUND` fan-out (measured
  2026-08-08), so the emulator suite drives that repair end-to-end; the missing-**family** leg is
  gated-suite-only (emulator says `INTERNAL`).

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

- No per-destination counters (one fixed table); `errorClass` counts RPC failures only;
  `statusCode` reports the chain's outermost classifiable status; `close()` zeroes the
  gauge-backing counters **before** `Closers.closeAll`; every failure reaching the writer is
  counted except a batched row-level rejection, whose place `parkedMutations` takes.

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
