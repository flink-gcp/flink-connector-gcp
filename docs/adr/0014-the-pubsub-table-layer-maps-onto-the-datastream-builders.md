<!--
Copyright 2026 The flink-gcp authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# ADR-0014: The Pub/Sub table layer is a mapping onto the DataStream builders

- Status: Accepted
- Date: 2026-07-26/27 ([#135], [#136], [#137], settled under [#47]); sink creation settings
  2026-07-27 ([#153]); [#140] closed as not needed 2026-08-09; explicit service-account key
  file 2026-08-12 ([#139]); SQL ordering-key routing 2026-08-12 ([#143]); multi-subscription
  auto-creation 2026-08-12 ([#152])
- Issues: [#47] (split into [#135]–[#138]), [#139], [#140], [#143], [#152], [#153]
- Modules: pubsub (`table`, `table.sink`, `table.source`)
- Current behavior: `docs/content/docs/connectors/table/pubsub.md`

## Decision

The `table` layer is a *mapping* onto the DataStream builders, never a second implementation:
one typed `ConfigOption` per builder setter, applied with `getOptional(...).ifPresent(...)` so
"absent from the DDL" and "left at the connector's default" are the same state and no default is
restated in a `ConfigOption`. A reflective test asserts the setter set and the option set match,
which is what keeps that true once the key names are grouped (`sink.batching.*`,
`sink.retry.*`) and no naming rule connects the two.

- **No `properties.*` passthrough**: Kafka's is a map its own client parses, Pub/Sub has none,
  and [#20] already decided no gax type reaches the public API. Byte knobs are `memoryType()`,
  converted at the mapper boundary so `MemorySize` never reaches the connector API.
- **The four connector enums carry their DDL spelling in `toString()`** (`create-if-needed`,
  `per-key`, `nack`, `continue-from-subscription`) because
  `ConfigurationUtils.convertToEnum` matches on `toString()` case-insensitively and normalizes
  nothing — Flink's own `DeliveryGuarantee` has the same shape. Table-local `DescribedEnum`
  duplicates (Kafka's `ScanStartupMode`) were declined: four extra types and a conversion step
  for no gain. The visible cost is `StartPosition.toString()` reading
  `StartPosition{mode=latest}`.
- One factory class implements both directions, so `topic`/`subscription` are **not** in
  `requiredOptions()` — each is checked in the `create...` method that needs it, or a table used
  in only one direction would be forced to configure the other.
- **Sink specifics**: metadata is not forwarded to formats (no built-in format ships writable
  metadata, and Kafka does not forward either), so the physical prefix of a consumed row is
  exactly the table's physical columns and a reused `ProjectedRowData` hides the metadata suffix
  from the encoder; the row is written into the `PubsubMessage.Builder` directly rather than
  through the public `withAttributes`/`withOrderingKey` combinators, whose
  `Map<String, String>` extractor would allocate a map per record; a null attribute key or value
  **fails the write** rather than being dropped; `ChangelogMode.insertOnly()` because Pub/Sub
  cannot express a retraction; and an `ordering-key` column without
  `sink.message-ordering.enabled` is rejected in `applyWritableMetadata`, since the writer would
  otherwise fail on the first record. Dynamic per-record topics stay out ([#140], closed as not
  needed: a `STATEMENT SET` of `INSERT`s covers the SQL fan-out case with topics known at plan
  time) — cut from [#47] deliberately.
- **Credentials remain a builder mapping, not a Table-only provider abstraction** ([#139]).
  ADC is still the default, including its existing `GOOGLE_APPLICATION_CREDENTIALS` path.
  `serviceAccountKeyFile(...)` and `service-account-key-file` add one explicit service-account JSON
  key path for deployments that cannot select an identity through the process environment.
  The path, rather than a parsed credentials or gax provider object, crosses Flink serialization;
  each writer, reader, or enumerator loads and scopes it when it starts, so every eligible
  JobManager and TaskManager must mount the same path.
  That runtime component shares its provider among the publisher, subscriber, topic-admin, or
  subscription-admin clients it creates.
  The emulator mode rejects it because its plaintext channel deliberately carries no credentials.
  Raw or Base64-encoded JSON, access tokens, and custom provider classes stay out: they add secret
  exposure or lifecycle semantics that this issue does not need, while attached service accounts
  and Workload Identity remain the preferred production choices.
- **Package layout**: `table` holds the `@PublicEvolving` options class and the factory,
  `table.sink`/`table.source` the `@Internal` implementation — a deliberate departure from
  Kafka, which keeps its whole table layer flat; the root layout rule (public API at the package
  root, implementation beneath) decides it. **The factory is the only place a DDL option becomes
  a value** — `PubSubDynamicSink` takes resolved constructor arguments and has no configuration
  vocabulary at all, which is why `PublisherOptionsMapper` is `@Internal public` rather than
  package-private.
- **Source specifics** ([#136]): the SPI was widened to
  `deserialize(PubsubMessage, SubscriptionDestination, Collector<T>)` rather than dropping the
  `subscription` metadata column — nothing is published, so a signature change is the cheap
  option. That column carries the **resource name**, not the bare id (argument on the docs
  page); a two-column short-id-plus-resource-name design was built and dropped as redundant, and
  the column deliberately does **not** equal the `subscription` option, documented rather than
  fixed. **`DecodingFormat.applyReadableMetadata` throws by default** and no built-in format
  overrides it, so it must be guarded — and the guard is on the format *declaring* metadata, as
  Kafka's is, not on the planner having selected some: only that form can shrink the key set
  back, and the ability permits repeated calls. Calling it unconditionally breaks every table
  with any metadata column; caught by the acceptance IT, never by a unit test.
- **Per-key ordering is routed by the table sink** ([#143]): selecting the writable
  `ordering-key` metadata column makes a `DataStreamSinkProvider` insert a keyed partition before
  the Sink V2 writer, so every non-empty key reaches one writer at any sink parallelism.
  Null and empty keys are unordered and receive distinct routing keys so they do not form one hot
  partition; parallelism one skips the unnecessary exchange.
  `SupportsBucketing` is not this contract: Flink accepts only physical distribution keys, while
  `ordering-key` is metadata, and the ability validates a requested distribution but does not
  create the runtime shuffle.
  The connector consequently exposes neither `DISTRIBUTED BY` nor `INTO n BUCKETS` semantics;
  `sink.parallelism` remains only the writer and publisher count.
  The automatic route deliberately pays a network shuffle above one writer because an upstream
  distribution is not a connector-visible guarantee, and a hot key remains limited to one writer.
- **Auto-creation and start position** ([#137]): three setters do not take a `ConfigOption`'s
  shape, and each resolution lives in a mapper under `table.source` (`StartPositionMapper`,
  `SubscriptionCreateOptionsMapper`, joining `SubscriberOptionsMapper`). Start position is
  `scan.startup.mode` + `scan.startup.timestamp-millis`, **Kafka's spelling rather than the
  connector's own** — settled on what a migrating SQL user types without reading anything; the
  docs table's "Maps to" column carries the connection to `StartPosition`. It has no declared
  default: `PubSubSourceBuilder` already initialises `continueFromSubscription()`, so absent
  means default. `StartPosition.of(Mode, Instant)` raises both pairing errors, so the mapper
  delegates; the one rule it owns is a **timestamp with no mode**, where `of` is never reached
  and the option would otherwise be read by nothing. Same reasoning gives "a
  `scan.auto-create.*` knob without `scan.auto-create.topics` is rejected, not ignored".
- **`expirationTtl` versus `neverExpire` has no builder backstop** — the issue assumed one, and
  the builder is in fact last-writer-wins, each setter clearing the other, which is right for a
  call sequence and meaningless for a `WITH` clause. So the table layer rejects the pair *only*
  here, and that check is load-bearing rather than a nicer message; the builder was deliberately
  left as it is. `never-expire = false` calls nothing, since the setter takes no argument and
  `false` is already the state.
- **Auto-creation maps each subscription to its own topic** ([#152]):
  `scan.auto-create.topics` is a `mapType()` rendered canonically as one prefixed entry per
  subscription, such as `scan.auto-create.topics.orders-sub = orders-topic`.
  Its key set must equal `subscription` exactly; all other creation options are shared across the
  resulting `SubscriptionCreateOptions` objects.
  An absent map requires every subscription to exist, while a present map authorizes creation of
  any missing subscription; topics are still never created.
  The invariant is checked in the mapper and again in `PubSubDynamicSource`'s constructor, which
  is the code that indexes the list.
  The builder's own cross-checks remain in force after the table mapping, but `FactoryUtil` wraps a
  factory failure in a `ValidationException` that adds table context and retains the original cause.
  `carriesTheCreationSettingsAndTheStartPositionIntoTheBuiltSource` and its sibling prove the
  settings reach those checks — the create options and the start position are otherwise invisible
  from outside the built `Source`, and a mutant that dropped the start position on the way to the
  builder survived every unit test until they read it back through
  `PubSubStreamingPullSource.getConfig()`.
- **The two directions spell resource creation differently on purpose** (the question [#136]
  left open). They are not one feature: a topic needs no configuration to exist, so the sink
  gates creation with a `CreateDisposition` enum and "create with defaults" is meaningful; a
  subscription without a topic binding is not a subscription, so the source has no disposition
  enum at all and **the presence of settings is the authorization**. `scan.create.*` was weighed
  and declined: sharing the word would put a uniform vocabulary over a difference the DataStream
  API makes deliberately, and this layer maps rather than invents. `scan.` itself is not a
  choice — Flink's read-side prefix, carried by every source option and
  `FactoryUtil.SOURCE_PARALLELISM` — and with one factory serving both directions it is what
  tells a reader which half an option belongs to.
- **[#153] resolved that settlement's stated expiry** by giving the sink creation settings
  (`TopicCreateOptions`: `messageRetention`, `kmsKeyName`, the storage policy). The re-opened
  naming question settled as: the *settings* vocabulary aligns — `sink.auto-create.*` beside
  `scan.auto-create.*`, one spelling where both sides carry one knob (`message-retention`) —
  while the *gates* stay different, because the gate reasoning is about what a resource needs to
  exist and [#153] changed nothing about that. Three sink-side facts: the settings are additive
  and never authorize (the disposition still does, and its default `CREATE_IF_NEEDED` means
  settings alone are meaningful — only an explicit `CREATE_NEVER` beside them is rejected, in
  the builder naming methods and in the mapper naming option keys); **one options object applies
  to every topic the sink creates**, dynamic destinations included, because unlike a
  subscription's topic binding nothing in the settings ties them to one topic — there is no
  per-topic map to express; and `schemaSettings`, `labels` and `tags` were considered and
  declined (schema validates at publish time only, re-checking what this sink serialized, and is
  invisible to subscriptions beyond the `googclient_schema*` attributes — its payoff accrues to
  GCP-managed consumers, not the Flink pipeline; its evolution model means support would not end
  at creation; labels/tags mirror the subscription side's omission) — all additive later.
  **Deliberately no follow-up issue for schema support** (decided with the user on [#153]): the
  declined record here and on the docs page is the anchor, and a future issue needs a real
  consumer-side use case.
- **The source never creates a topic.** There is no `createTopic` in the `source` package, so
  `scan.auto-create.topics` names topics that must already exist, while the sink's
  `create-if-needed` does create one — the same two words meaning opposite things across one
  DDL, which is why both user-facing documents say so outright. A sink-created topic without
  creation settings takes every `Topic` field's service default, message retention among them,
  so a backwards seek over it replays nothing already acknowledged unless `messageRetention` was
  set at creation.

## Evidence

The emulator stores all four `TopicCreateOptions` knobs verbatim and returns them on `GetTopic`
— measured in [#153] after a first measurement wrongly concluded the opposite off a one-line
grep of a multi-line proto `toString` — but validates nothing and shows no effect, so the ITs
assert the round trip and the *semantics* (real CMEK, residency, retention-driven replay) stay
with the real-GCP suite ([#82]).

[#20]: https://github.com/laughingman7743/flink-connector-gcp/issues/20
[#47]: https://github.com/laughingman7743/flink-connector-gcp/issues/47
[#82]: https://github.com/laughingman7743/flink-connector-gcp/issues/82
[#135]: https://github.com/laughingman7743/flink-connector-gcp/issues/135
[#136]: https://github.com/laughingman7743/flink-connector-gcp/issues/136
[#137]: https://github.com/laughingman7743/flink-connector-gcp/issues/137
[#138]: https://github.com/laughingman7743/flink-connector-gcp/issues/138
[#139]: https://github.com/laughingman7743/flink-connector-gcp/issues/139
[#140]: https://github.com/laughingman7743/flink-connector-gcp/issues/140
[#143]: https://github.com/laughingman7743/flink-connector-gcp/issues/143
[#152]: https://github.com/laughingman7743/flink-connector-gcp/issues/152
[#153]: https://github.com/laughingman7743/flink-connector-gcp/issues/153
