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
  context)`, null = skip — so its users port by changing the interface name. Its built-in
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
- **`FAILED_PRECONDITION` is routed row-level on the strength of the design comment, not of a
  measurement.** `INVALID_ARGUMENT` is the documented per-entry data rejection; the second code was
  listed in #33's design and is kept, but no example of the service returning it per entry has been
  observed. It is the one classification to re-check against the real service in #218 — the
  repository's standing rule (from the Pub/Sub classifier) is that the routed class is widened only
  with evidence that a code identifies one mutation rather than a condition, since a dropping
  handler must never see a condition. Everything else — `NOT_FOUND` (a missing table *or column
  family*), `PERMISSION_DENIED`, `UNAUTHENTICATED`, and anything the client's own retries gave up
  on — is fatal.
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
- **The send metrics live here rather than with #37.** `numRecordsSend`/`numBytesSend` are
  incremented at admission (the Kafka connector's placement), with bytes from
  `entry.toProto().getSerializedSize()`. No other sink in this repository has them yet: the #37
  series is scoped to the three existing connectors and does not reach Bigtable, so waiting would
  have meant shipping a sink with no metrics at all. Richer per-connector metrics still belong to
  that series' outcome.
- **`StubWriterInitContext` cannot drive this writer**, because its metric group is a
  null-returning proxy and the writer dereferences the group in its constructor. The emulator tests
  therefore build writers through the sink's injecting `createWriter(batcher, mailbox, metricGroup)`
  overload with a batcher the production factory created — the Cloud Tasks emulator tests' shape —
  and the MiniCluster job tests are what cover the `WriterInitContext` path end to end. Widening the
  shared stub was considered and left alone: it is test-utils, shared with concurrent work, and this
  module needed a counter-recording group of its own anyway (`UnregisteredMetricsGroup`'s sink
  writer group hands out a fresh `SimpleCounter` per call, so what the writer captured is
  unreachable from it).
