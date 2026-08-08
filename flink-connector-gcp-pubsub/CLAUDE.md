# CLAUDE.md — flink-connector-gcp-pubsub

Module-scoped guidance, loaded when Claude works in this module. Repository-wide rules
(build, workflow, version policy, licensing, package layout) stay in the root `CLAUDE.md`.
This file holds the rules a session must follow; each decision's record — context, evidence,
declined alternatives — is the named ADR under `docs/adr/` or the docs page.

## Provenance

- The base implementation is vendored from `GoogleCloudPlatform/pubsub` `flink-connector/`
  (decision record: issues #17 and #31); the Apache connector is only a design reference
  (table-factory plumbing, emulator harness). All packages are normalized to
  `io.github.flink.gcp.connector.pubsub.*`. Keep the README's provenance section and `NOTICE`
  accurate on any further adaptation.

## Sink (`docs/adr/0004`–`0008`, `0052`)

- Publisher-based flush-on-checkpoint stateless writer; `AsyncSinkBase` was rejected — do not
  re-propose it (`docs/adr/0004`).
- The writer owns **both** in-flight caps (`maxInFlightMessages`, `maxInFlightBytes`); the SDK
  `flowControl*` knobs are removed, not deprecated, and must not come back (`docs/adr/0004`).
- The three drains (`drainInFlight()`, named apart from `awaitCapacity()`) must keep meaning
  "empty, and `checkAsyncError`". Admission is "below the cap", never "does this message fit" —
  a fits-predicate hangs the task. A repair republishes its parked batch exempt from both caps.
- **The sink's two mailbox waits (`awaitCapacity`, `drainInFlight`) are bounded on *progress*,
  never on the call** (#333; `docs/adr/0052`): `publishProgressTimeout` restarts on every
  completion, progress is stamped on the SDK thread (the writer's one `volatile` field), the
  budget is read only after `tryYield()` comes back empty, and the loop reads
  `Thread.interrupted()` itself. Blocking at the in-flight cap flushes the SDK's batcher once,
  or its `batchDelayThreshold` would sit inside the budget. A stalled wait WARNs at a tenth of the
  budget — the answer to the default racing Flink's checkpoint timeout, chosen over ADR-0009's
  smaller default, which with ordering would turn a short outage into a restart loop. It bounds a
  stalled publisher, not a checkpoint's total spend.
- **Per-key order is restored by sorting the parked batch on a publish sequence, never by
  observation order** — anything derived from mail order is a race (#78; `docs/adr/0004`).
- **Exactly two failures are routed**: a record the serializer rejects, and a publish rejected
  `INVALID_ARGUMENT`. Outage-shaped and configuration-shaped failures are never routed
  (`docs/adr/0005`). A serializer `null` is a skip, not a failure (`docs/adr/0001`); the check
  sits ahead of `stateFor(...)`.
- Ordering beside a dropping policy is allowed; parking a cascade never depends on the create
  disposition, the repair's topic creation is gated by `DestinationState.topicMissing` alone,
  and a dropped key's resume happens in the repair, never in `routeFailedMessage`
  (`docs/adr/0006`).
- A non-solo `INVALID_ARGUMENT` is parked and confirmed by the isolation pass; only a message
  rejected solo reaches the handler, and a batched report is not counted by the error metrics
  (`docs/adr/0008`, which also carries `maxConsecutiveRejections`, the #361 bound on a dropping
  policy's pass: consecutive solo-confirmed rejections, reset by any successful publish, never
  serializer rejections, and never the recovery budget — that one caps a repair's unproductive
  attempts).
- The publisher teardown is two-phase on a daemon thread with a real bound; the deadline runs
  from `start()`; `awaitTermination` runs on the teardown thread; `retryTotalTimeout`/
  `retryMaxAttempts` are rejected beside `enableMessageOrdering(true)` (`docs/adr/0007`).
  `publisherShutdownsAbandoned` counts closes that overran their budget, reading the module-root
  `PubSubShutdownResidue` adder — the base class holds no count.
- **A table-layer check that fires inside `createDynamicTable{Source,Sink}` is wrapped by
  `FactoryUtil`, and a check whose message names Java setters needs restating in option keys**;
  one whose message needs no translation does not (`docs/adr/0007`). Never assert on an option
  key through the wrapper's own message in a factory test.

## Dead-letter queue (`docs/adr/0009`)

- `PubSubDeadLetterQueue` lives in the top-level `pubsub.deadletter` package, uses the SDK
  `Publisher` directly, and never auto-creates its topic. The envelope's `dlq-error` truncates
  on a character boundary; the cause chain stays out of the envelope.
- Both its budgets (`shutdownTimeout`, `flushTimeout`) are wait-side, one deadline per wait —
  never one per future — and both reject a `Duration` too large for `toNanos()`. Expiry throws;
  futures are not cancelled. Its knobs are documented in the datastream pages' dead-lettering
  prose, not in `reference/pubsub.md` tables (#328 tracks the checker gap; #329 its missing
  metrics).

## Metrics (`docs/adr/0010`; conventions in the base module's CLAUDE.md)

- Plain counters — every sink increment is on the task thread. `numRecordsSend` is counted
  inside `publishTo` under `firstAttempt`; `parkedMessages` is a plain field zeroed in
  `close()`; cascade cancellations are never counted; per-destination handles resolve once per
  `DestinationState`.

## Source (`docs/adr/0011`, `0012`, `0066`)

- **The reader checkpoints no splits** — the enumerator owns assignment and recomputes the plan
  on every start (`docs/adr/0011`).
- The subscriber shutdown mode is not exposed (`NACK_IMMEDIATELY` fixed); the
  running-without-checkpointing guard is `MissingCheckpointDetector`, evaluated from
  `PubSubSplitReader.fetch()`, never the record path; its budget starts at first split
  assignment and retires at the first barrier (`docs/adr/0011`).
- Teardown rules (`docs/adr/0012`): shutdowns and closes go in **one `Closers.closeAll` list**
  (#297/#350 — never a loop then a call, at every level); `awaitTerminated()` absorbs the
  client's re-report and discriminates by `permanentErrorReported`, a flag set where the failure
  is **handed to a caller** — never replace it with a pre-shutdown snapshot; a paused split is
  still watched via `checkFailure()` from `fetch()` (#348); the failed-start release is kept
  although mostly a no-op (#349); `BoundedShutdown` is deliberately not adopted here.
- **A paused split's buffer is bounded by parking its subscriber** (`docs/adr/0066`): past
  `pausedSplitBufferMaxMessages`/`MaxBytes` — either one, each defaulting to **twice** the
  flow-control limit it shadows (one lease-expiry wave is worth a whole window, and a bound at the
  limit itself parks healthy splits — `docs/adr/0066` lists the three ways) — `fetch()` stops that
  split's client and `pauseOrResumeSplits` opens a fresh one on resume. Three orderings are
  load-bearing and none is obvious: the failure check runs **before** the park (a park closes, and
  `close()` absorbs the failure); every split parked in one fetch goes through **one
  `Closers.closeAll` list, every shutdown before any close** (#297's shape — parking one at a time
  costs `splits × shutdownTimeout` serially, and alignment pauses splits as a group), with each
  `checkFailure` heading its own entries; and
  `pauseOrResumeSplits` **ends with `signalDataAvailable()`**, or a split paused while already
  over its bound leaves the next fetch waiting forever, with every guard sitting after that wait. The SDK
  defaults are read live from `Subscriber.Builder.getDefaultFlowControlSettings()` — never
  mirrored, unlike `maxAckExtensionPeriod`'s, whose SDK constant is package-private. What this
  costs is that #348 does not hold while a split is parked.
- The real-GCP gated suite (#82) is the **only** coverage of ordered dispatch, dead-letter
  forwarding, ordered seek, create-option persistence, nack-redelivery promptness and the
  permission-denied message texts. Gating annotations go on every concrete class, never the
  abstract base (`docs/adr/0011`).
- The emulator never answers the keepalive ping — idle streams cycle ~every 20 s and the log
  line is routine, not a fault (`docs/adr/0013`).

## Table API / SQL (`docs/adr/0014`)

- The `table` layer maps onto the DataStream builders, never re-implements: one `ConfigOption`
  per setter, `getOptional(...).ifPresent(...)`, no default restated, enums carry their DDL
  spelling in `toString()`, and the reflective completeness test holds the two sets equal.
- No `properties.*` passthrough; metadata is not forwarded to formats;
  `applyReadableMetadata` is guarded on the format *declaring* metadata; per-key ordering is not
  reachable from SQL (#143 — document `sink.parallelism = 1`, do not enforce).
- The two directions spell resource creation differently on purpose (sink: disposition gate +
  `sink.auto-create.*` settings; source: presence of `scan.auto-create.*` settings is the
  authorization), and **the source never creates a topic**. The `expirationTtl`/`neverExpire`
  pair is rejected only in the table layer — the builder is last-writer-wins by design.

## SQL uber-jar (`docs/adr/0015` — the record every later `flink-sql-connector-gcp-*` inherits)

- Everything bundled is relocated, `grpc-netty-shaded` included (two `META-INF/native` path
  renames; the shaded prefix must not grow an underscore); `artifactSet` stays `*:*`; never
  declare a Google artifact at `test` scope in a SQL module; invoke the licence goal through a
  phase, never bare. Read `docs/adr/0015` before changing any SQL module's pom or adding a
  third module; what is specific to a tree belongs beside that connector (the BigQuery jar's
  record is in its module).
- NOTICE prose is hand-written in `NOTICE.template`; everything mechanical is generated
  (`just update-notice` / `check-notice`) and licence texts are sha256-pinned in
  `scripts/licence-sources.toml` — curation follows the ladder in
  `.claude/skills/curate-licence-source/`, and there is no rung 4.
