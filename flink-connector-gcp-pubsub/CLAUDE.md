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
  was one line. That column carries the **resource name**, not the
  bare id — the argument is on the docs page; what belongs here is that a two-column
  short-id-plus-resource-name design was built and dropped as redundant, and that the column
  deliberately does **not** equal the `subscription` option, which is documented rather than fixed.
  **`DecodingFormat.applyReadableMetadata` throws by default** and no built-in format overrides it,
  so it must be guarded — and the guard is on the format *declaring* metadata, as Kafka's is, not on
  the planner having selected some: only that form can shrink the key set back, and the ability
  permits repeated calls. Calling it unconditionally breaks every table with any metadata column;
  caught by the acceptance IT, never by a unit test.
  **Per-key ordering is not reachable from SQL** (#143): the guarantee is per writer subtask, the
  DataStream answer is a `keyBy` before the sink, and SQL has no equivalent — `DISTRIBUTED BY` needs
  `SupportsBucketing`, which this sink does not implement. `sink.parallelism = 1` is the only correct
  configuration today; it is documented rather than enforced, because a distribution the user
  arranged upstream is legitimate and the sink cannot tell the difference.
  **Auto-creation and start position** (#137): three setters do not take a `ConfigOption`'s shape,
  and each resolution lives in a mapper under `table.source` rather than in the factory —
  `StartPositionMapper` and `SubscriptionCreateOptionsMapper`, joining `SubscriberOptionsMapper`.
  Start position is `scan.startup.mode` + `scan.startup.timestamp-millis`, **Kafka's spelling rather
  than the connector's own** ("start position") — weighed, and settled on what a migrating SQL user
  types without reading anything; the docs table's "Maps to" column carries the connection to
  `StartPosition`. It has no declared default, like every other option here: `PubSubSourceBuilder`
  already initialises `continueFromSubscription()`, so absent means default and the issue's "default
  `continue-from-subscription`" describes behaviour rather than a `ConfigOption` default.
  `StartPosition.of(Mode, Instant)` raises both pairing errors, so the mapper delegates; the one rule
  it owns is a **timestamp with no mode**, where `of` is never reached and the option would otherwise
  be read by nothing. Same reasoning gives "a `scan.auto-create.*` knob without
  `scan.auto-create.topic` is rejected, not ignored".
  **`expirationTtl` versus `neverExpire` has no builder backstop** — the issue assumed one, and the
  builder is in fact last-writer-wins, each setter clearing the other, which is right for a call
  sequence and meaningless for a `WITH` clause. So the table layer rejects the pair *only* here, and
  that check is load-bearing rather than a nicer message; the builder was deliberately left as it is.
  `never-expire = false` calls nothing, since the setter takes no argument and `false` is already the
  state.
  **Auto-creation requires exactly one subscription**, because settings are per destination and carry
  the topic binding: N options objects are inexpressible in a flat DDL namespace, and sharing one
  would duplicate every message with nothing reporting an error. The precondition is checked in the
  mapper and again in `PubSubDynamicSource`'s constructor, which is the code that indexes the list.
  A `scan.auto-create.topics` `mapType()` extension for N>1 is deferred to #152. The builder's
  own cross-checks (ordering under `PER_KEY`, a dead-letter policy under a policy that needs one)
  then reach SQL users unchanged, which `carriesTheCreationSettingsAndTheStartPositionIntoTheBuiltSource`
  and its sibling are what prove — the create options and the start position are otherwise invisible
  from outside the built `Source`, and a mutant that dropped the start position on the way to the
  builder survived every unit test until they read it back through
  `PubSubStreamingPullSource.getConfig()`.
  **The two directions spell resource creation differently on purpose, and this is where that was
  settled** (the question #136 left open, having counted `sink.create-disposition`, `sink.recovery.*`
  and `scan.auto-create.*` as three spellings of one feature). They are not one feature. A topic
  needs no configuration to exist, so the sink can gate creation with a `CreateDisposition` enum and
  a "create with defaults" is meaningful; a subscription without a topic binding is not a
  subscription, so the source has no disposition enum at all and **the presence of settings is the
  authorization**. `scan.create.*` was weighed and declined: sharing the word would put a uniform
  vocabulary over a difference the DataStream API makes deliberately, and this layer maps rather
  than invents. `scan.` itself is not a choice — it is Flink's read-side prefix, carried by every
  source option here and by `FactoryUtil.SOURCE_PARALLELISM` (`scan.parallelism`) — and with one
  factory serving both directions it is what tells a reader which half an option belongs to.
  **That settlement has a stated expiry**: #153 would give the sink creation *settings* for the
  topic it creates — today it has none, while a created subscription takes nine, and that asymmetry
  is the issue's opening argument. It would make the two sides more alike than they were when this
  was decided, so re-open the naming there rather than assuming this answer still holds.
  **The source never creates a topic.** There is no `createTopic` in the `source` package, so
  `scan.auto-create.topic` names a topic that must already exist, while the sink's
  `create-if-needed` does create one — the same two words meaning opposite things across one DDL,
  which is why both user-facing documents now say so outright. A sink-created topic also takes every
  `Topic` field's service default, message retention among them, so a backwards seek over it replays
  nothing that was already acknowledged (#153 again)
- **`flink-sql-connector-gcp-pubsub`, the uber-jar** (#138) — the repository's first shaded module,
  so what is decided here sets the shape every later `flink-sql-connector-gcp-*` will copy.
  **Everything bundled is relocated under `io.github.flink.gcp.connector.pubsub.shaded.`, with no
  exemption for `grpc-netty-shaded`.** The exemption is the tempting answer and was built first:
  that artifact carries native libraries whose names netty derives from its own package, and
  maven-shade does not rename native resources. It was rejected on a measurement rather than a
  preference — with `io.grpc.netty.shaded` left in place the jar cannot share a classpath with
  anything else carrying that package, failing with `ServiceConfigurationError: NettyChannelProvider
  not a subtype`, and the *first* thing to trigger that would be a second GCP SQL connector built
  the same way. The price is two path relocations renaming
  `META-INF/native/(lib)?io_grpc_netty_shaded_netty*` to the relocated prefix with dots as
  underscores; both forms are needed because Windows DLLs carry no `lib`. **That is the established
  form and it was checked, not assumed**: identical pairs are in googleapis/java-bigtable-hbase
  (citing netty#6995 and grpc-java#2485), Dataproc's gcs-connector, spark-bigquery, Beam's
  `GrpcVendoring`, and the uber-jars of both Google Flink connectors — while
  GoogleCloudDataproc/flink-bigquery-connector relocates without renaming and ships a jar whose
  tcnative and epoll can never load. `rawString` is **not** needed (maven-shade matches resource
  paths directly) and no surveyed project uses it. The replacement is constrained by netty's
  `calculateMangledPackagePrefix()`: the relocated name must remain a pure *prefix* of
  `io.netty.util.internal.NativeLibraryLoader` — Beam gets away with collapsing `io.grpc.netty
  .shaded` to its vendor root only because what remains still satisfies that — and an underscore in
  the prefix would have to be spelled `_1`, which is why the shaded prefix must not grow one.
  `PubSubSqlConnectorPackagingITCase` derives the expected string from the shaded prefix rather than
  repeating it, so config and assertion cannot drift. Untested residue, deliberately: whether the
  renamed libraries load through JNI is only exercised on Linux with epoll or tcnative, and a wrong
  rename degrades to NIO and JDK SSL *silently*. Still unrelocated are `org.conscrypt` (native
  libraries too, but a reflectively-loaded optional TLS provider gRPC does without) and four
  annotation-only packages, where a duplicate class is inert because nothing invokes it.
  Three build traps worth not rediscovering. **Declaring a Google artifact at `test` scope in the
  SQL module demotes it out of the bundle** — Maven's nearest-definition rule beats the transitive
  `compile` scope — which silently cut the jar down to guava plus a few annotation jars.
  `maven-dependency-plugin:analyze` is absent for the same reason: the scoping it would demand is
  the scoping that breaks the bundle, so the test harness uses classes that arrive transitively and
  declares none of them. **`artifactSet` is `*:*`, and the enumerated include list it replaced should not come back.**
  The list's justifications each died when measured: an unlisted new transitive does *not* fail the
  build, contrary to what #138 assumed — it is silently dropped from the jar, the worst available
  outcome — and "readable beside the NOTICE" ended when the NOTICE became generated. With the
  wildcard a new dependency is bundled automatically; what remains human is relocating a genuinely
  new package root (a real decision — conscrypt must *not* be, and commons-lang3 under
  `org.apache.commons` would arrive unrelocated because only `commons.codec` is mapped, measured),
  and the packaging tests fail with the artifact's name until it is made.
  `BundledDependenciesNoticeTest` diffs the NOTICE against the recorded runtime tree both ways. **`ApacheNoticeResourceTransformer` needs `organizationName` and `inceptionYear`,
  not just `projectName`**, or the aggregated NOTICE still reads "Copyright 2006-2026 The Apache
  Software Foundation". Relatedly, the root pom now sets `<organization>`: without it the ASF
  parent's remote-resources bundle stamped that same claim into *every* module jar this project
  builds.
  **The NOTICE's prose is hand-written; everything mechanical is generated and pinned.** The split
  is `NOTICE.template` (module root): human paragraphs plus one `{{Licence}}` placeholder per
  group, which `scripts/check-notice.py --update` fills from what license-maven-plugin resolved —
  so a wrong group, a duplicate bullet or a stale version is not a checkable mistake but an
  *inexpressible* one. `just update-notice <module>` regenerates; `just check-notice <module>`
  (CI) re-renders in memory and fails on any drift, offline. Licence *texts* come only from
  `scripts/licence-sources.json`, each entry pinned by **sha256** with its provenance recorded:
  the artifact's own jar where one ships a text (threetenbp, javax.annotation-api — best
  provenance, version-exact), otherwise a curated URL whose ref matches the bundled version and
  whose note says why (protobuf's Java 4.33.x maps to core tag v33.x; gax and google-auth live in
  *archived* — hence frozen — repositories with no tag for these versions; POM-declared URLs are
  often HTML pages or bare templates, and the script rejects HTML outright). A fetch that stops
  hashing to its pin fails: upstream changed, a human reviews. This replaced an earlier state
  where five texts had been curl'd from repository heads chosen by hand — wrong provenance, and
  the reason the pin exists. **Curating a new entry follows a fixed fallback ladder** (also printed by the failure
  message): (1) a licence file inside the artifact's own jar; (2) the publisher's repository at
  the tag matching the bundled version; (3) the publisher's repository head only when it is
  frozen (archived) or no version tag exists, with the reason in the note; and there is no rung
  4 — a generic template is not the project's text, since the copyright line is part of a BSD or
  MIT licence, so an artifact with no pinnable publisher text is a reason to question the
  dependency, not to substitute one. The curation itself is judgment; everything after the pin
  is mechanical. Measured before any of this was built: the plugin's classification matched
  the hand grouping on **all 52 artifacts**, including the two that inherit `<licenses>` from a
  parent pom (guava, animal-sniffer), the dual-licensed `javax.annotation-api`, and re2j's
  non-SPDX "Go License". `licenseMerges` in the root pom's `pluginManagement` is what makes the
  vocabulary stable — this tree alone spells Apache-2.0 six ways — and it lives there so a
  sibling SQL module inherits one vocabulary rather than inventing a second.
  **What a sibling actually costs, measured against the BigQuery module's 113-artifact tree**: the
  plugin block and one execution in its pom, its own `NOTICE.template`, a CI step, and
  `licence-sources.json` entries for its non-Apache artifacts (the file and its pins are shared, so
  overlapping dependencies cost nothing twice). No new `licenseMerges` — that list was extended
  here, once, to cover the spellings that tree adds (`Apache License V2.0`, `BSD 3-clause`,
  `MIT License`, `The MIT License`, `The BSD 2-Clause License`), and `failOnMissing` does not fire
  on it.
  **`download-licenses` must not be used for the licence texts**: it names files after the *licence*,
  so protobuf, gax, google-auth and threetenbp collapse into one BSD-3-Clause file and the last
  download wins. Measured — it left ThreeTen's copyright line standing for Google's code, and the
  copyright holder is part of a BSD or MIT text.
  **Invoke the goal through a phase, never as a bare `license:add-third-party`** — a
  CLI goal invocation selects reactor modules but does not build them, so `-pl` cannot resolve the
  connector the module bundles, not even with `-am`, unless an earlier `install` happened to leave
  it in the local repository. That failed in CI and passed locally twice for exactly that reason.
  It costs nothing to bind: the goal reads POMs Maven has already resolved and fetches nothing —
  the network-using goal is `download-licenses`, which is the one not used here.
  `BundledDependenciesNoticeTest` overlaps the script's first check deliberately: the comparison is
  a Python script rather than a Maven plugin, so it is a CI step of its own, and the test is what
  makes the same drift fail inside `just verify`
