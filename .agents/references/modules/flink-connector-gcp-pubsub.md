# Detailed guidance — flink-connector-gcp-pubsub

Module-scoped guidance, read when working in this module. Repository-wide rules
(build, workflow, version policy, licensing, package layout) stay in the root `AGENTS.md`.
This file holds the rules a session must follow; each decision's record — context, evidence,
declined alternatives — is the named ADR under `docs/adr/` or the docs page.

## Provenance

- The module began as an adaptation of `GoogleCloudPlatform/pubsub` `flink-connector/` (issues #17
  and #31); the Apache connector is only a design reference (table-factory plumbing, emulator
  harness). All packages are normalized to `io.github.flink.gcp.connector.pubsub.*`.
- Parts of it have since been rewritten. **Nine files still carry `Copyright 2023 Google LLC` and
  must keep it** — `StreamingPullSubscriber` (formerly `PubSubNotifyingPullSubscriber`) was
  re-measured after the #755 rewrite split its teardown out, and its two residues survive the
  split, so its deferral is resolved as kept. Four files were retired on an audit against upstream
  (`docs/adr/0123`), which also states the rule and its bias: a notice goes only when nothing
  upstream-specific survives, never because a lot has changed, and where the audit leaves room for
  judgement the notice stays. Keep the README's provenance section and `NOTICE` accurate on any
  further adaptation, and re-measure rather than assume when a rewrite touches one of the nine.

## Sink (`docs/adr/0004`–`0008`, `0052`)

- Publisher-based flush-on-checkpoint stateless writer; `AsyncSinkBase` was rejected — do not
  re-propose it (`docs/adr/0004`).
- The writer owns **both** in-flight caps (`maxInFlightMessages`, `maxInFlightBytes`); the SDK
  `flowControl*` knobs are removed, not deprecated, and must not come back (`docs/adr/0004`).
- The three drains (`InFlightTracker.drainToEmpty()`, named apart from `awaitCapacity()`) must
  keep meaning
  "empty, and `checkAsyncError`". Admission is "below the cap", never "does this message fit" —
  a fits-predicate hangs the task. A repair republishes its parked batch exempt from both caps.
- **The sink's two mailbox waits (`InFlightTracker.awaitCapacity`, `drainToEmpty`) are bounded on
  *progress*,
  never on the call** (#333; `docs/adr/0052`): `publishProgressTimeout` restarts on every
  completion, progress is stamped on the SDK thread (the tracker's one `volatile` field), the
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
  `publisherShutdownsAbandoned` counts the **sink's** closes that overran their budget, reading the
  module-root `PubSubShutdownResidue` adder — the base class holds no count; the dead-letter
  queue's own closes are a second adder under a second name (#329, below).
- **A table-layer check that fires inside `createDynamicTable{Source,Sink}` is wrapped by
  `FactoryUtil`. A single value the builder rejects is renamed to its option key through the
  module's `table.OptionSetters`** (`docs/adr/0133`); the restate-in-DDL-keys judgment remains
  only for cross-field checks (`docs/adr/0007`, refined by `docs/adr/0133`). Never assert on an
  option key through the wrapper's own message in a factory test.

## Dead-letter queue (`docs/adr/0009`)

- `PubSubDeadLetterQueue` lives in the top-level `pubsub.deadletter` package, uses the SDK
  `Publisher` directly, and never auto-creates its topic. The envelope's `dlq-error` truncates
  on a character boundary; the cause chain stays out of the envelope.
- Both its budgets (`shutdownTimeout`, `flushTimeout`) are wait-side, one deadline per wait —
  never one per future — and both reject a `Duration` too large for `toNanos()`. Expiry throws;
  futures are not cancelled. Its six knobs are documented once, as a
  `## PubSubDeadLetterQueue.builder()` section of `reference/pubsub.md`, with the other three
  reference pages linking to it; the datastream pages' dead-lettering prose keeps what they are
  *for*. `check-option-docs` reaches the class only because `option-docs.toml` names its file in
  pubsub's `sources` — nothing about the file matches `SOURCE_GLOBS` (#328).
- Its optional service-account key path is independent of the host connector's credentials.
  The queue loads the service-account-only credential in `open()`, so the path, rather than parsed
  credentials, crosses the job graph and every eligible TaskManager must mount it.
  An absent path leaves ADC in effect; emulator mode remains plaintext and credential-free and is
  rejected beside the key path (#546).
- **Its five metrics register on the *host* sink writer's group** (#329, #405) — the only group
  `FailureHandlerContext` carries — so a BigQuery job reports them beside BigQuery's own, which is
  why every one of the names carries `deadLetter`. They are `PubSubMetricNames`', by the argument
  that already puts the options on `reference/pubsub.md`. Three rules not to re-derive:
  `deadLettersPublished` counts **confirmations**, never hand-offs — the hand-off count is already
  `numRecordsSendErrors`, which every writer here increments immediately before calling the
  handler, so the premise #329 was filed on ("leaves that accounting entirely") is false;
  `deadLetterFlushMillis` is written in a `finally`, so the expiry is not the case it skips, and is
  `volatile` because a `long` is read from the reporter thread — with an empty `flush()` recording
  nothing, and `longestDeadLetterFlushMillis` beside it (#405) because waits happen faster than
  scrapes, once per *element* under `WRITE_THROUGH`; and the queue's abandoned teardowns
  count into a **second** `PubSubShutdownResidue` adder, since one name on the host's group would
  collide with the sink's and Flink drops the later registration.

## Metrics (`docs/adr/0010`; conventions in the base module's detailed guidance)

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
- Two subscriber surfaces the reader rides are `@BetaApi` in the pinned client:
  `AckReplyConsumerWithResponse` (the exactly-once-delivery preview the ack path is built on)
  and `Subscriber.Builder.setSubscriberShutdownSettings` (the settings type itself is
  unannotated). Both are internal calls, tier-irrelevant under `docs/adr/0141`; reread them on a
  BOM bump.
- Teardown rules (`docs/adr/0012`): shutdowns and closes go in **one `Closers.closeAll` list**
  (#297/#350 — never a loop then a call, at every level); `awaitTerminated()` absorbs the
  client's re-report and discriminates by `permanentErrorReported`, a flag set where the failure
  is **handed to a caller** — never replace it with a pre-shutdown snapshot; a paused split is
  still watched via `checkFailure()` from `fetch()` (#348); the failed-start release is kept
  although mostly a no-op (#349); `BoundedShutdown` is deliberately not adopted here.
- **Two of the teardown's four outcomes are counted, and the other two never will be by symmetry**
  (#358, `docs/adr/0012`): an expired wait is `subscriberShutdownsAbandoned` — the sink's spelling,
  because it is the sink's meaning — and a failure the teardown was the only report of is
  `subscriberFailuresUnreported`, named for that property because the same branch also catches a
  streaming failure that landed after the reader's last pull. The re-report and the failed-start
  release stay log-only: each accompanies a louder report of the same incident, which is the
  argument to engage before adding a third counter. Both counts are `PubSubShutdownResidue` adders
  read through `ResidueCounter`, registered by `PubSubSourceReaderMetrics` and
  incremented by nothing there — so **the reader's group reports a class-loader total, not this
  subtask's**, and a park's close increments them like any other teardown.
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
  `pauseOrResumeSplits` **ends by raising the data-available signal
  (`DataAvailabilitySignal.raise()`)**, or a split paused while already over its bound leaves the
  next fetch waiting forever, with every guard sitting after that wait. The SDK
  defaults are read live from `Subscriber.Builder.getDefaultFlowControlSettings()` — never
  mirrored, unlike `maxAckExtensionPeriod`'s, whose SDK constant is package-private. What this
  costs is that #348 does not hold while a split is parked.
- **Backpressure was measured, and the measurement narrowed the fear rather than confirming it**
  (#377; the second Evidence block of `docs/adr/0066`). A *slow* downstream frees an element-queue
  slot per batch and each slot lets exactly one more `fetch()` run, so the guards there are delayed
  by one drain interval, not skipped; only a downstream that has stopped outright freezes the loop,
  and there the mailbox is not polling either — `pollNext` is the only path a fetcher's failure has
  to the job — so a guard on another thread would report nothing sooner. Hence **gauges and no
  second bound**: `bufferedMessages`/`bufferedBytes` sum `bufferUsage()` over a registry
  `PubSubSourceReaderMetrics` owns, so they cannot come to disagree with the number the #357 bound
  reads, and a stale entry cannot corrupt them (a `shutdown()` empties the buffer). Whether the
  buffer *grows* is decided by the drain rate against
  `W / (maxAckExtensionPeriod − one lease extension)`, where the second term is the client's own
  adaptive `messageDeadlineSeconds` and **not** the subscription's ack deadline — an ack only
  covers what was already drained, so
  expiry is the only drain-independent source of permits — but that is a rate of the right order,
  **not a bound on `messagesReceived`**, because a supersede returns the permit it took (a draft
  asserted such a ceiling and CI produced 327 against 250). **Nothing bounds delivery from above in
  any shape, and the rig asserts none**: a bar on the fast arm's *drain* is one too, an arm being
  able to drain only what it was given, and CI met the half-of-requested-rate form of it exactly at
  300 against 300 (#440). How much an arm is delivered belongs to the runner — 151–195 locally
  against 175–318 on one CI runner — while what it is left *holding* reproduces on both, which is
  why that is the finding. Two facts to keep: the buffer is not the
  reader's footprint (a frozen loop held 3999 messages in the element queue, invisible to
  `bufferUsage()` and so to the bound), and on the real service **most of what a backpressured split
  is handed is redelivery churn** — 215 and 338 supersedes out of 369 and 462 deliveries, against
  exactly zero on the emulator, which never redelivers.
- The real-GCP gated suite (#82) is the **only** coverage of ordered dispatch, dead-letter
  forwarding, ordered seek, create-option persistence, nack-redelivery promptness and the
  permission-denied message texts. Gating annotations go on every concrete class, never the
  abstract base (`docs/adr/0011`).
- The emulator never answers the keepalive ping — idle streams cycle ~every 20 s and the log
  line is routine, not a fault (`docs/adr/0013`).

## Table API / SQL (`docs/adr/0014`; no-restated-default repo-wide since `docs/adr/0139`)

- The `table` layer maps onto the DataStream builders, never re-implements: one `ConfigOption`
  per setter, applied through `OptionSetters` (`docs/adr/0133`), no default restated, enums carry their DDL
  spelling in `toString()`, and the reflective completeness test holds the two sets equal.
  "No default restated" covers the *description* as well as `defaultValue()`, and covers a
  derived default too — `reference/pubsub.md` carries the derivation and the resolved value.
  `PubSubConnectorOptionsTest` checks the `ConfigOption` half directly and guards the description
  half through the restatement phrases; since #1045 the phrase list is shared by all five
  connectors' guards (`docs/adr/0139`) and a new form extends them together. A failure there is
  the rule speaking (#866). Correct the description, not the test.
- No `properties.*` passthrough; metadata is not forwarded to formats;
  `applyReadableMetadata` is guarded on the format *declaring* metadata. When writable
  `ordering-key` metadata is selected, the table sink inserts keyed routing before the writer;
  non-empty keys are stable, null and empty keys are spread, and parallelism one skips the
  exchange (#143). This is not bucketing: `sink.parallelism` is the writer count and no bucket
  count is exposed.
- The two directions spell resource creation differently on purpose (sink: disposition gate +
  `sink.auto-create.*` settings; source: presence of `scan.auto-create.*` settings is the
  authorization), and **the source never creates a topic**. The `expirationTtl`/`neverExpire`
  pair is rejected only in the table layer — the builder is last-writer-wins by design.
- `scan.auto-create.topics` maps every configured subscription to its own pre-existing topic.
  Its key set must equal `subscription`; the remaining `scan.auto-create.*` settings are shared
  across the per-subscription creation objects, and an absent map requires all subscriptions to
  exist.

## SQL uber-jar (`docs/adr/0015` — the record every later `flink-sql-connector-gcp-*` inherits)

- Everything bundled is relocated, `grpc-netty-shaded` included (two `META-INF/native` path
  renames; the shaded prefix must not grow an underscore); `artifactSet` stays `*:*`; never
  declare a Google artifact at `test` scope in a SQL module; invoke the licence goal through a
  phase, never bare. Read `docs/adr/0015` before changing any SQL module's pom or adding a
  third module; what is specific to a tree belongs beside that connector (the BigQuery jar's
  record is in its module).
- **A SQL module's pom carries its `<relocations>`, its surefire `integration-tests` override,
  `japicmp.skip` and its dependencies — nothing else.** The shade `artifactSet`, `filters` and
  `transformers`, both `maven-dependency-plugin` executions and the licence execution are in the
  root pom's `pluginManagement`, so a bundle-shape change reaches both jars at once; a module
  declares each plugin to switch it on, empty but for the shade one's relocations. That list is
  the part that must not be shared — a pattern wider than a module's own tree rewrites references
  nothing can then supply, and a union of the two lists is measured to do exactly that
  (`docs/adr/0015`). The shade plugin is configured at three levels once you count
  `flink-connector-parent`, so verify any change to that block by comparing entry names and CRCs
  of both built uber-jars against the previous build, and accept only a zero delta.
- NOTICE prose is hand-written in `NOTICE.template`; everything mechanical is generated
  (`just update-notice` / `check-notice`) and licence texts are sha256-pinned in
  `scripts/config/licence-sources.toml` — curation follows the ladder in
  `.agents/skills/curate-licence-source/`, and there is no rung 4.
