# CLAUDE.md — flink-connector-gcp-bigtable

Module-scoped guidance, loaded when Claude works in this module. Repository-wide rules
(build, workflow, version policy, licensing, package layout) stay in the root `CLAUDE.md`.

## Design decisions (do not silently revisit)

- **Implemented, not adopted or vendored** (#33, design settled 2026-08-02; the issue holds the
  full comparison): `com.google.flink.connector.gcp:flink-connector-gcp-bigtable` is a Flink
  2.1-only artifact with `GoogleCredentials` on its public API and no failure-handler SPI, and its
  upstream is dormant — so depending on it was rejected, and vendoring buys nothing once package
  normalization, AutoValue→builder conversion, options objects, `CrossVersionSink` and
  `base.failure` wiring have changed essentially every line. What *is* adopted is its
  `BaseRowMutationSerializer`'s **shape** — `@Nullable RowMutationEntry serialize(element,
  context)` — so its users port by changing the interface name. Null = skip is no longer an
  adoption but the repository's own contract, decided in #230 and now carried by all four SPIs;
  the root `CLAUDE.md` has the reasoning and the three implementation rules. Its built-in
  `GenericRecord`/`RowData` serializers are deliberately not ported: `RowData` belongs to #217, and
  an Avro convenience is additive whenever a use case appears.
- **Four SDK facts this module is built on**, each checked against `google-cloud-bigtable` sources
  rather than assumed:
  - `newBulkMutationBatcher(TargetId)` is `@BetaApi` while the `String` overload is `@Deprecated`,
    so the `TableId.of(...)` form is taken and the beta annotation accepted — there is no
    non-beta, non-deprecated way to get a batcher.
  - gax's `Batcher` is `@InternalExtensionOnly`, so a test fake must not implement it. That is why
    `MutationBatcher` exists as this module's own narrow SPI, wrapping the client batcher exactly
    as `TopicPublisher` (Pub/Sub) and `TaskCreator` (Cloud Tasks) wrap theirs. It is also why
    `sendOutstanding()` is called rather than `Batcher.flush()`: the blocking one would stall the
    task thread while the completion mails the writer's state is mutated by pile up behind it.
  - The client's bulk-mutation path has a **flow controller of its own** — 1000 entries per
    channel, 100 MB, `LimitExceededBehavior.Block` — whose static limits its public API does not
    expose (only `enableBatchMutationLatencyBasedThrottling` / `disableBatchMutationLatencyBasedThrottling`
    and the `@InternalApi` server-initiated switch). So `Batcher.add()` *can* block the task
    thread, and keeping the writer's own bounds below the client's is the only available way to
    preserve the #85 property that a full writer yields to the mailbox rather than blocking. The
    defaults (1000 / 64 MiB) do; that is why the reference page documents raising
    `maxInFlightMutations` as *moving* the bound rather than raising it, and why exposing the
    client's flow-control knobs is not the fix (it is the #85 defect class itself).
  - `RowMutationEntry.toProto()` is `@InternalApi`, and it is the only route to both the byte size
    the in-flight bound counts and the `FailedElement` payload — the entry exposes neither its key
    nor its mutations. Accepted deliberately: nothing mechanical flags it, since
    `check-flink-api-tiers` audits `org.apache.flink` imports only. If it ever disappears, the byte
    bound and `FailedMutation.getPayloadBytes()`/`getRowKey()` are the call sites to revisit.
- **`INVALID_ARGUMENT` alone is routed, and `FAILED_PRECONDITION` deliberately is not** — reversing
  #33's design comment, which listed both. The rule is the repository's, settled on #207 the same
  day: only a status an authority defines as *state-independent* may reach a handler that may drop
  it. gRPC defines `INVALID_ARGUMENT` as "problematic regardless of the state of the system" and
  AIP-194 lists it must-not-retry, while `FAILED_PRECONDITION` and `OUT_OF_RANGE` are explicitly
  state-dependent — so a mutation rejected with one of those might be accepted later, and dropping
  it is data loss. Cite the definition rather than the plausibility of the failures a code names.
  Everything else — `NOT_FOUND` (a missing table *or column family*), `PERMISSION_DENIED`,
  `UNAUTHENTICATED`, and anything the client's own retries gave up on — is fatal.
- **Routing takes both halves of a condition, and they read the cause chain differently.** No
  transient status *anywhere* in the chain (so an unstable service cannot produce a dead letter even
  behind a data-shaped status — a property of this code, not of the client surfacing one status per
  failure), **and** the chain's *first* classifiable status is `INVALID_ARGUMENT` (so an
  `INVALID_ARGUMENT` buried under an `INTERNAL` describes the inner call and does not discard a
  record over a server-side failure). The two mistakes are mirror images; both are pinned by test.
  `BigtableErrorClassifier.firstMatching(throwable, codes)` is the shared primitive, the same shape
  `CloudTasksWriter` uses.
- **Retries stay in the client, so this module has no `RetrySchedule` and no retry knobs.**
  `MutateRows` ships a non-empty retryable-code set and retries per entry
  (`EnhancedBigtableStubSettings`: `DEADLINE_EXCEEDED`, `UNAVAILABLE`, 10 ms doubling to 1 min, 10
  min total). That is the exact opposite of Cloud Tasks, whose generated client retries
  `CreateTask` on nothing and therefore owns a sink-side loop; the difference is in the clients, so
  neither is a precedent for the other.
- **One fixed table per sink, and no auto-creation.** A batcher is bound to one table, so per-record
  destinations would mean a batcher pool, a share of the in-flight budget each and an eviction
  policy — deferred until there is demand, and recorded on #33. Auto-creation is a poorer fit here
  than in the Pub/Sub sink: a table's schema *is* its column families and their garbage-collection
  policies, which a sink cannot guess.
- **`TableDestination` sits at the module root, not under `sink`.** The root file's layout rule puts
  destination types in `sink`; this deviates because #216's source facade takes the same value, and
  moving it later would churn every importer. `appProfileId` is deliberately *not* part of it: a
  profile selects client routing, not a data address, so it is a builder option. The sink has no
  family layer (#119: one write family, `MutateRows`, with no sibling in prospect —
  `checkAndMutateRow` and `readModifyWriteRow` are request-response primitives, not batched write
  paths), so `BigtableMutateRowsSink` sits beside its facade as the single-family modules' rule
  allows, and `FailedMutation` sits at the `sink` root as the post-#213 placement rule prescribes.
- **The metrics are the #37 series' standard, reached late** (#237, which also absorbed #234 —
  its "counter for *dropped* elements" is `numRecordsSendErrors`, the series having settled on one
  counter for everything routed rather than a second one for what a handler then discarded).
  #33 shipped `numRecordsSend`/`numBytesSend` ahead of the series because it was scoped to the
  three connectors that existed when it was designed; the rest arrived here. Bytes come from
  `entry.toProto().getSerializedSize()`, counted at admission (the Kafka connector's placement).
  Five decisions:
  - **No per-destination counters, and so no `perDestinationMetrics` option.** One fixed table per
    sink means `destination.TABLE.*` restates the writer's totals — the same reason #210 leaves
    them off the buffered-stream path, under its rule that no writer registers a metric it can
    never increment. A batcher pool (see the one-table decision above) is what would change this.
  - **`errorClass` counts RPC failures only, never the serializer's.** A serialization failure
    carries no status, so counting it would put every one under `UNCLASSIFIED` beside the RPC
    failures that genuinely carry none. Both sibling sinks draw the line in the same place: one
    `errorClass` call site against three routing ones.
  - **`BigtableErrorClassifier.statusCode` reports the chain's outermost classifiable status, not
    the code `classify` acted on**, mirroring `PubSubErrorClassifier.statusCode`. The two diverge
    exactly when a transient status is buried under a data-shaped one: routing scans the whole
    chain and calls it fatal, while the counter names what the failure was reported with. That is a
    real divergence rather than a theoretical one, and it is pinned by
    `namesTheOutermostStatusOfAChainItTreatsAsFatalForABuriedTransientOne` — added because the
    mutant reporting the routing decision instead **survived** the first test set.
  - **`close()` zeroes the two gauge-backing counters, and does it *before* `Closers.closeAll`.** A
    reporter can sample between `close()` and the metric group's own close, and nothing decrements
    them afterwards (the completions that would run as mailbox mails no longer run), so a writer
    torn down mid-flight would keep reporting mutations it will never wait for — the same
    lifecycle gap #210 found in the BigQuery writers and #208 had already closed in
    `PubSubWriter`. The *placement* is this module's own: `close()` throws a `BatchingException`
    on the failure path (#238), so a clear after `Closers.closeAll` would be skipped in exactly
    the case it exists for. Found in review round 2, having been missed by a round 1 that looked
    only at increment sites — **when a series brings the same shape to another connector, diff the
    `close()` paths too.**
  - **Every failure reaching the writer is counted, fatal ones included and fatal ones after the
    first.** Only the first becomes `asyncError`, but each is a mutation the client gave up on.
    The consequence, stated on the docs page rather than left to a reader to infer: since the
    retries are the client's, the sum over the transient codes is *not* this connector's retry
    volume, which is exactly what that sum means on the Cloud Tasks page. The client's own attempts
    are invisible here. The other reading the page owes: #239's batch blast radius makes both
    `numRecordsSendErrors` and `errorClass.INVALID_ARGUMENT.errors` count a whole batch per bad
    record, which is the metric that measures what a dropping policy costs.
- **The E2E suite creates an ephemeral instance per gated *class*, not per run** (#218) — the one
  deviation from that issue's settled design. When this landed it was forced: `reuseForks=false`
  meant a fresh JVM per class, where a JVM-scoped holder buys nothing. #243's root-pom override
  (`reuseForks=true`) changed the calculus — the two forks' classes run sequentially in
  long-lived JVMs, so a per-fork holder became possible — and per-class was kept anyway: a shared
  holder would still be raced by the two forks, a single class must stay runnable by hand, and
  best-effort deletion tracks per class. Nothing persistent exists to run against because a one-node instance stands at roughly
  $470/month, so `opentofu/flink-gcp` carries only the two API enablements and `roles/bigtable.admin`
  — admin because *instance* lifecycle is administrator-level, not because the data path needs it.
  Leak control is a name-encoded creation time (`flink-it-<epochSeconds>-<runId>`, 28 characters
  inside Bigtable's 33) plus a sweep of anything older than two hours at the start of each class;
  the threshold sits far above the workflow's 40-minute ceiling, so the sweep cannot reach a live
  run. The cluster id is built from the run id rather than the instance id, which at 28 characters
  leaves no room under a cluster id's own 30-character limit. Measured 2026-08-02: the two classes
  together, instance provisioning included, take about 7½ minutes.
  **`BIGTABLE_IT_PROJECT` in a shell used to make every `just verify` create instances**, because
  the gate is on the classes and `verify` runs the same `integration-tests` execution `just e2e`
  does — ~7½ minutes and a node-hour fraction on every full build the variable was visible to. The
  BigQuery and Pub/Sub gates had that shape all along and were merely cheap; being the first one
  billed per run is what forced #245, which closed it: every gated class also carries
  `@Tag("gated")`, which surefire excludes by default, so the suite is opt-in per *command*
  (`just e2e`, or `-Dtest.excluded.groups=` by hand) rather than per shell. The variable is still
  required to run the suite — the environment gate is unchanged — but setting it no longer costs
  anything by itself.
- **What real Bigtable answers, measured 2026-08-02** (client 2.80.0), which is what the connector
  page's error-handling table now states rather than infers. Routed (`INVALID_ARGUMENT`): a cell
  timestamp that is not a multiple of 1000 ("Timestamp granularity mismatch"), and an empty row key
  ("Row keys must be non-empty"). Fatal (`NOT_FOUND`): a mutation naming a column family the table
  lacks — and the service reports it for **every** entry of the batch, the good ones included.
  **Two conditions #218's text expected to measure are unmeasurable through this connector**:
  `Mutation` enforces its own limits in the private `addMutation` every mutation-adding method funnels
  through — so `deleteCells` and `deleteRow` are covered as much as `setCell`, and "more mutations
  than a row accepts" and an oversized entry are thrown client-side and arrive as *serialization*
  failures with no entry and no row key; the service never sees them. The mutation-count half was run
  (110,000 mutations, which never reached the wire); the byte half is `MAX_BYTE_SIZE` = 200 MiB read
  from the client's class file beside `MAX_MUTATIONS` = 100,000, not exercised. A
  single-cell size violation is unreachable for a second reason too: the client's bulk flow
  controller caps accumulated size at 100 MB, below Bigtable's 256 MB per row.
- **Two defects that run turned up, both filed rather than fixed here** (decided with the user):
  a routed `INVALID_ARGUMENT` fails **every entry of its batch**, because Bigtable rejects the whole
  `MutateRows` request and gax propagates that to every entry future — so a dropping policy discards
  the good records batched with the bad one (#239; the docs now say so, and
  `routesEveryEntryOfTheBatchWhenOneOfThemIsRejected` pins it so a fix has to come through there);
  and `close()` throws gax's `BatchingException`, re-reporting every entry failure of the batcher's
  lifetime whatever the policy already did with them, so a `logAndDrop` job fails at task close
  anyway (#238). Both the gated and the emulator failure tests swallow the latter in a helper naming
  the issue — that swallow is the marker to remove when #238 lands.
- **`BigtableEmulatorDeviationITCase` asserts what the *emulator* does**, which is not a breach of
  the emulator-is-not-an-authority rule but its enforcement: it records the traps so an image bump
  has to declare them. The one that matters is the **status** — the emulator answers `INTERNAL`
  where the service answers `INVALID_ARGUMENT` or `NOT_FOUND`, and `INTERNAL` is fatal to this sink,
  so an emulator-only test would conclude "fails the job" for a condition the service makes
  droppable. It also **accepts an empty row key**, storing a row that breaks the client's own read
  state machine — a state the service cannot reach. Every row of the documentation page's deviation
  table is asserted from both sides.
- **`StubWriterInitContext` cannot drive this writer**, because its metric group is a
  null-returning proxy and the writer dereferences the group in its constructor. The emulator tests
  therefore build writers through the sink's injecting `createWriter(batcher, mailbox, metricGroup)`
  overload with a batcher the production factory created — the Cloud Tasks emulator tests' shape —
  and the MiniCluster job tests are what cover the `WriterInitContext` path end to end. The
  module's own `RecordingSinkWriterMetricGroup` is **gone since #237**: it predated test-utils'
  `TestSinkWriterMetricGroup` and was superseded by it, and the shared one is strictly better here —
  it asserts by *registered name*, which is what a gauge or an `errorClass` subgroup needs and what
  a counter-holding stub cannot offer. (`UnregisteredMetricsGroup`'s own sink writer group remains
  unusable for either: it hands out a fresh `SimpleCounter` per call, leaving what the writer
  captured unreachable.)
