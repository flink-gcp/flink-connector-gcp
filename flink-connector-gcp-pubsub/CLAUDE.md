# CLAUDE.md — flink-connector-gcp-pubsub

Module-scoped guidance, loaded when Claude works in this module. Repository-wide rules
(build, workflow, version policy, licensing, package layout) stay in the root `CLAUDE.md`.

## Design decisions (do not silently revisit)

- **Pub/Sub**: base implementation is vendored from `GoogleCloudPlatform/pubsub`
  `flink-connector/` (decision record: issues #17 and #31); the Apache connector is only a
  design reference (table-factory plumbing, emulator harness). All packages are normalized to
  `io.github.flink.gcp.connector.pubsub.*`
- **Pub/Sub sink** (#18): Publisher-based flush-on-checkpoint stateless writer; FLIP-171
  `AsyncSinkBase` evaluated and rejected (SDK `Publisher` already batches; AsyncSink persists
  buffers into writer state). Mailbox-based backpressure with in-flight caps; writer-owned
  per-topic publishers (no JVM-wide cache); publish failures are capture-and-rethrow (the
  Apache connector's infinite republish is deliberately not adopted). Topic auto-creation (#19)
  is reactive — NOT_FOUND publishes are parked and republished after creating the topic via the
  `TopicAdmin` SPI (`sink.topics`, ALREADY_EXISTS = success), gated by `CreateDisposition`.
  Tuning (#20) lives in one `PubSubPublisherOptions` object (nested-options pattern; plain
  serializable values, no gax types on the public API; unset = SDK/sink default): batching,
  publish retries, `enableMessageOrdering`, the in-flight caps and the recovery backoff.
  In-flight bounds (#85, revising #20): the writer owns **both** caps — `maxInFlightMessages`
  (1000) and `maxInFlightBytes` (64 MiB per subtask) — and the two SDK `flowControl*` knobs #20
  exposed are **removed**, not deprecated. gax flow control could never be the byte bound an
  ordered sink needs: SDK 1.152.0 leaks a permit per publish cancelled on a paused key (so the
  builder rejected combining it with ordering — exactly where cascades pile up), and it blocks the
  task thread instead of yielding to the mailbox. Message count alone bounds no memory, since
  Pub/Sub allows 10 MiB per message. Three constraints not to re-litigate: the byte bound is an
  *additional* condition on `write`'s admission only, because the three drains (now
  `drainInFlight()`, named apart from `awaitCapacity()` for exactly this reason) must keep meaning
  "empty, and `checkAsyncError`" — #78/#110 made that load-bearing; admission is "below the cap",
  never "does this message fit", since `yield()` blocks until a mail arrives and no mail can
  arrive at zero in flight, so a fits-predicate would hang the task on an oversized message
  instead of backpressuring it; and the topic-creation repair republishes its parked batch
  **exempt from both caps**, because yielding between a key's republishes reorders it. Parked
  messages are counted by neither cap (their failure mail released them). The two writer test
  classes carry `@Timeout(30)`: the fake mailbox blocks like the real one, so a broken predicate
  hangs rather than fails.
  Ordering×repair (revised in #78): cascade cancellations are parked alongside their NOT_FOUND
  root and every repair attempt calls `resumePublish` before republishing, but **per-key order is
  restored by sorting the parked batch on a publish sequence, never by observation order** — the
  "mailbox FIFO preserves per-key order" premise this was first built on is false, because the SDK
  cancels an ordering key's queued publishes from its own thread, so a cascade can be observed
  before the failure that caused it. Anything derived from that order is a race, including
  deciding whether to park a cascade by whether something is parked already: that was the #78
  flake, and it was *also* the only thing hiding a silent ordering violation, since the parked
  list was appended in observation order too. Consequences to keep: a cancellation is never a root
  cause, so under `CREATE_IF_NEEDED` one is parked unconditionally and a fatal root is caught by
  the pre-repair drain (`drainInFlight()` → `checkAsyncError`) rather than by classifying
  the cascade; under `CREATE_NEVER` nothing is parked at all, which every parking branch must
  check, since parking is what leads to `createTopic`. Emulator
  support (#21) is a builder option `emulatorEndpoint(host:port)` — plaintext + no credentials
  for publishers (each owning its channel) and the auto-creation admin, mirroring the Apache
  connector's `withHostAndPortForEmulator`; the emulator ITs (including a MiniCluster streaming
  test through the public builder) reuse the production factory/admin, no test-only factory.
  Per-record failure policy and the fatal-exception classifier moved to #37. Decision record in
  the connector documentation page
- **Pub/Sub source** (#79, #80): FLIP-27 streaming-pull source; split = (subscription, uid), ack on
  checkpoint completion, nack on close. Tuning lives in one `PubSubSubscriberOptions` object
  (nested-options pattern, same shape as `PubSubPublisherOptions`). Two decisions deviate from the
  #80 issue text and must not be silently re-litigated:
  (a) the **subscriber shutdown mode is not exposed** — `NACK_IMMEDIATELY` is fixed because
  `WAIT_FOR_PROCESSING` waits for acknowledgements that only arrive at checkpoint completion, which
  never happens during close; only `shutdownTimeout` is a knob (an SDK enum on the public API would
  also break the #47 SQL mapping);
  (b) the "**fail when running without checkpointing**" guard **cannot read the configuration** —
  `SourceReaderContext.getConfiguration()` is the TaskManager configuration
  (`SourceOperatorFactory` passes `getTaskManagerInfo().getConfiguration()`), while
  `env.enableCheckpointing(...)` writes into the job configuration, so absence proves nothing and
  failing on it would break jobs that enable checkpointing programmatically while passing every
  MiniCluster test. Replaced by `MissingCheckpointDetector` (no checkpoint taken + messages
  outstanding + budget spent → fail), **evaluated from `PubSubSplitReader.fetch()`, not the record
  path** — once flow control fills, the client stops delivering and `pollNext` is never called
  again, so a record-driven check would go silent in exactly the state it exists to catch; the
  detector bounds the fetch park only while armed, so a healthy reader parks indefinitely as
  before. The config-derived ack-extension check is a best-effort warning only.
  `parallelPullCount > 1` is rejected with `orderingMode(PER_KEY)` rather than silently forced to 1
  (the factory still force-sets 1 so the guarantee does not rest on the SDK default). The `NACK` deserialization-failure policy is deferred to #81, where
  the `GetSubscription` preflight can verify a dead-letter policy exists
- **Pub/Sub Table API / SQL** (#47, split into #135–#138): the `table` layer is a *mapping* onto the
  DataStream builders, never a second implementation — one typed `ConfigOption` per builder setter,
  applied with `getOptional(...).ifPresent(...)` so "absent from the DDL" and "left at the
  connector's default" are the same state and no default is restated in a `ConfigOption`. A
  reflective test asserts the setter set and the option set match, which is what keeps that true
  once the key names are grouped (`sink.batching.*`, `sink.retry.*`) and no naming rule connects
  the two. **No `properties.*` passthrough**: Kafka's is a map its own client parses, Pub/Sub has
  none, and #20 already decided no gax type reaches the public API. Byte knobs are `memoryType()`,
  converted at the mapper boundary so `MemorySize` never reaches the connector API.
  **The four connector enums carry their DDL spelling in `toString()`** (`create-if-needed`,
  `per-key`, `nack`, `continue-from-subscription`) because `ConfigurationUtils.convertToEnum`
  matches on `toString()` case-insensitively and normalizes nothing — Flink's own
  `DeliveryGuarantee` has the same shape. Table-local `DescribedEnum` duplicates (Kafka's
  `ScanStartupMode`) were the alternative and were declined: four extra types and a conversion step
  for no gain. The one visible cost is `StartPosition.toString()`, which now reads
  `StartPosition{mode=latest}`.
  One factory class implements both directions (#136 adds `DynamicTableSourceFactory` to it), so
  `topic`/`subscription` are **not** in `requiredOptions()` — each is checked in the `create...`
  method that needs it, or a table used in only one direction would be forced to configure the
  other. Sink specifics: metadata is **not** forwarded to formats (no built-in format ships
  writable metadata, and Kafka does not forward either), so the physical prefix of a consumed row
  is exactly the table's physical columns and a reused `ProjectedRowData` hides the metadata suffix
  from the encoder; the row is written into the `PubsubMessage.Builder` directly rather than
  through the public `withAttributes`/`withOrderingKey` combinators, whose `Map<String, String>`
  extractor would allocate a map per record; a null attribute key or value **fails the write**
  rather than being dropped; `ChangelogMode.insertOnly()` because Pub/Sub cannot express a
  retraction; and an `ordering-key` column without `sink.message-ordering.enabled` is rejected in
  `applyWritableMetadata`, since the writer would otherwise fail on the first record. Credentials
  stay ADC-only (#139) and dynamic per-record topics stay out (#140) — both cut from #47
  deliberately.
  **Package layout**: `table` holds the `@PublicEvolving` options class and the factory, `table.sink`
  (and `table.source` from #136) the `@Internal` implementation — a deliberate departure from Kafka,
  which keeps its whole table layer flat. The root `CLAUDE.md` rule (public API at the package root,
  implementation beneath) decides it, and #136 is the in-prospect sibling the #119 test asks for, so
  this is #125's situation rather than Cloud Tasks'. **The factory is the only place a DDL option
  becomes a value** — `PubSubDynamicSink` takes resolved constructor arguments and has no
  configuration vocabulary at all, which is why `PublisherOptionsMapper` is `@Internal public`
  rather than package-private.
  **Source specifics** (#136): the SPI was widened to
  `deserialize(PubsubMessage, SubscriptionDestination, Collector<T>)` rather than dropping the
  `subscription` metadata column — nothing is published, so a signature change is the cheap option
  (see the repo-level stance), and `SubscriptionSplit` was already in `emitRecord`, so the call site
  was one line. That column carries the **resource name**
  `projects/<project>/subscriptions/<sub>`, not the bare id: it is the only form Pub/Sub's API
  speaks in (`Subscription.name`, every RPC's `subscription` field), it is AIP-122's canonical
  relative resource name, and Pub/Sub publishes no URL or self-link — the full resource name and the
  resource URI are string operations on it. A two-column short-id-plus-resource-name design was
  built and then dropped as redundant. Consequence to document, not to fix: the column does **not**
  equal the `subscription` option, which is a bare id.
  Format readable metadata **is** forwarded (unlike the sink's writable metadata, which no format
  ships), listed before the connector's own so the produced row stays a plain concatenation; a
  format declaring one of the connector's keys is rejected. `getChangelogMode()` delegates to the
  format, so a changelog format over Pub/Sub works.
  **`DecodingFormat.applyReadableMetadata` throws by default** and no built-in format overrides it,
  so it must only be called when the planner actually selected one of the format's keys — calling it
  unconditionally breaks every table with any metadata column. Caught by the acceptance IT, never by
  a unit test.
  **Per-key ordering is not reachable from SQL** (#143): the guarantee is per writer subtask, the
  DataStream answer is a `keyBy` before the sink, and SQL has no equivalent — `DISTRIBUTED BY` needs
  `SupportsBucketing`, which this sink does not implement. `sink.parallelism = 1` is the only correct
  configuration today; it is documented rather than enforced, because a distribution the user
  arranged upstream is legitimate and the sink cannot tell the difference
