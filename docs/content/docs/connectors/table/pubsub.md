---
title: Cloud Pub/Sub
type: docs
weight: 20
---

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

# Cloud Pub/Sub SQL Connector

The `pubsub` table connector, provided by the `flink-connector-gcp-pubsub` module. It maps onto the
[DataStream sink and source]({{< relref "docs/connectors/datastream/pubsub" >}}), which is where the
behavior behind every option is documented; this page covers the option surface and the decisions specific to SQL.

Per-feature implementation status — including which directions are implemented — is tracked in the
[module README]({{< param BookRepo >}}/blob/main/flink-connector-gcp-pubsub/README.md). Most of
the option keys below are declared by `PubSubConnectorOptions` — `format`, `sink.parallelism` and
`scan.parallelism` come from Flink's `FactoryUtil` — and are applied by an internal table factory;
both it and the DataStream types the options map onto are in the
[Java API reference]({{< param ApiDocsURL >}}).

```sql
CREATE TABLE orders (
  order_id STRING,
  amount   INT,
  attrs    MAP<STRING, STRING> METADATA FROM 'attributes',
  okey     STRING             METADATA FROM 'ordering-key'
) WITH (
  'connector' = 'pubsub',
  'project'   = 'my-project',
  'topic'     = 'orders',
  'format'    = 'json',
  'sink.message-ordering.enabled' = 'true'
);

INSERT INTO orders
SELECT order_id, amount, MAP['source', 'sql'], customer_id FROM staged_orders;
```

```sql
CREATE TABLE incoming_orders (
  order_id     STRING,
  amount       INT,
  message_id   STRING          METADATA FROM 'message-id'   VIRTUAL,
  publish_time TIMESTAMP_LTZ(3) METADATA FROM 'publish-time' VIRTUAL,
  attrs        MAP<STRING, STRING> METADATA FROM 'attributes' VIRTUAL,
  WATERMARK FOR publish_time AS publish_time - INTERVAL '5' SECOND
) WITH (
  'connector'    = 'pubsub',
  'project'      = 'my-project',
  'subscription' = 'orders-sub',
  'format'       = 'json'
);

SELECT window_start, COUNT(*)
FROM TABLE(TUMBLE(TABLE incoming_orders, DESCRIPTOR(publish_time), INTERVAL '1' MINUTE))
GROUP BY window_start;
```

## Getting the connector onto the classpath

Use `flink-sql-connector-gcp-pubsub`, an uber-jar built for exactly this: put it in Flink's `lib/`
directory, or add it with `ADD JAR` in the SQL client. It bundles `flink-connector-gcp-pubsub`
together with its whole runtime tree — the Pub/Sub client, gRPC, protobuf, Guava, the Google auth
and HTTP clients — which is 51 artifacts, not a dependency list anyone wants to assemble by hand.

The plain `flink-connector-gcp-pubsub` jar works too, where the deployment already resolves
transitive dependencies. That is the right choice for a DataStream job built with Maven or Gradle.
For SQL it usually is not.

### Everything bundled is relocated

Every bundled package moves under `io.github.flink.gcp.connector.pubsub.shaded.`, so the versions of
gRPC, protobuf and Guava this connector needs cannot collide with the ones a job, another connector,
or Flink itself brings. That is the point of the artifact: without it, a Pub/Sub job that also
touches any other Google Cloud library becomes a version-alignment exercise.

Five packages are deliberately *not* relocated, and none of them can collide in a way that matters:
`org.conscrypt`, which gRPC picks up reflectively as an optional TLS provider and does without when
it is unusable; and the annotation-only `javax.annotation`, `org.jspecify`,
`org.codehaus.mojo.animal_sniffer` and `android.annotation`, where a duplicate class is inert
because nothing ever invokes it. `javax.annotation` here is jsr305's classes only —
`javax.annotation-api`, the other artifact publishing into that package, is not bundled
([#352]({{< param BookRepo >}}/issues/352)).

`io.grpc:grpc-netty-shaded` *is* relocated, which takes some care: gRPC ships it already relocated
once, having renamed its `META-INF/native/` libraries to match, because netty derives the native
library name from its own package at load time. Relocating those classes a second time therefore
means renaming the library files again in step. Leaving it alone was the obvious alternative and is
wrong — the jar would then be unable to share a classpath with anything else bundling gRPC. That
is no longer the hypothetical it was when this was written: `flink-sql-connector-gcp-bigquery`
and `flink-sql-connector-gcp-bigtable` bundle gRPC too, and all three are meant to sit in one
`lib/`.

### Licensing

`META-INF/NOTICE` inside the jar lists every bundled artifact grouped by licence, and
`META-INF/licenses/` carries the full text of each non-Apache-2.0 one — protobuf, gax, the Google
auth library, ThreeTen backport, RE2/J and animal-sniffer.

The prose of the NOTICE is human-written, in the module's `NOTICE.template`; the artifact lists are
generated into it from what Maven actually resolves, so a wrong licence grouping or a stale version
cannot be written at all. Each licence text has a pinned source — the artifact's own jar where one
ships a text, otherwise a curated URL matched to the bundled version — recorded with its sha256, so
a text that changes upstream fails the build instead of being shipped unreviewed. `just
update-notice <module>` regenerates both after a dependency change; `just check-notice <module>`
verifies, offline, that what is checked in still matches the bundle and the pins. Generic
licence-name URLs (`opensource.org`, `spdx.org`) are rejected as sources: they serve HTML pages or
bare templates, and the copyright holder is part of a BSD or MIT text.

## The payload and the rest of the message

A Pub/Sub message is a payload plus attributes and an ordering key. The payload is what `format`
encodes, from the table's physical columns; everything else is a metadata column.

Metadata columns are appended after the physical columns, so the format never sees them — a `json`
table with the DDL above publishes `{"order_id":"...","amount":...}` and carries `attrs` and `okey`
outside the payload.

### Writable

| Metadata key | Type | Notes |
|---|---|---|
| `attributes` | `MAP<STRING, STRING>` | A null column adds no attributes. A null key or a null value in the map **fails the write**: Pub/Sub attributes can represent neither, and dropping the entry would be data loss the query cannot see. Filter such entries out first |
| `ordering-key` | `STRING` | A null or empty value sets no key. Requires `sink.message-ordering.enabled` = `true`, and see the ordering caveat below |

Writable metadata is not forwarded to the format. No built-in format ships any, and the Kafka
connector does not forward either.

### Readable

| Metadata key | Type | Notes |
|---|---|---|
| `message-id` | `STRING NOT NULL` | The service-assigned id, unique within the topic |
| `publish-time` | `TIMESTAMP_LTZ(3) NOT NULL` | When the service received the message. Pub/Sub stamps nanoseconds; this is **truncated** to milliseconds, never rounded up. The natural column for `WATERMARK FOR` |
| `attributes` | `MAP<STRING, STRING> NOT NULL` | Never null; empty when the message carries none. On a subscription with a dead-letter policy the client library injects `googclient_deliveryattempt`, which is passed through rather than stripped |
| `ordering-key` | `STRING` | **`NULL` when the message has no key.** Pub/Sub represents "no key" as the empty string, which would be a wrong SQL value — an unordered message has no key rather than an empty one |
| `subscription` | `STRING NOT NULL` | The subscription's **resource name**, `projects/<project>/subscriptions/<subscription>` — **not** the bare id the `subscription` option takes, so `WHERE subscription = 'orders-sub'` matches nothing. See below for why |

Readable metadata a *format* declares is forwarded, and listed **before** the connector's own so
that the produced row is a plain concatenation whichever subset is selected. Keys are unprefixed:
Kafka's `value.` prefix exists only to disambiguate against a key format, and there is one format
here. A format declaring a key this connector also declares is **rejected**, because resolving the
collision silently would make a column's meaning depend on the format.

#### Why `subscription` is a resource name

`projects/my-project/subscriptions/orders-sub`, not `orders-sub`. That is the only form Pub/Sub's
own API speaks in: it is the value of `Subscription.name`, it is what every RPC's `subscription`
field takes, and the bare id appears nowhere on the API surface. It is also the *relative resource
name* that [AIP-122](https://google.aip.dev/122) makes canonical for API fields, so it is what joins
a stream against audit logs or Cloud Asset Inventory.

Pub/Sub publishes no URL or self-link of its own, so there is nothing else to expose. Google's two
other spellings are string operations on this one:

```sql
'//pubsub.googleapis.com/'        || subscription  -- full resource name (IAM, Asset Inventory)
'https://pubsub.googleapis.com/v1/' || subscription  -- resource URI
```

Note this does **not** equal the `subscription` option, which is the bare id resolved against
`project`. `WHERE subscription = 'orders-sub'` will not match; compare against the resource name, or
against `'projects/' || 'my-project' || '/subscriptions/orders-sub'`.

The subscription is on neither the message nor anything the SDK hands the connector — it consumes
through `Subscriber`, whose receiver callback delivers a message and an ack handle and never
surfaces the streaming-pull response. So it is threaded through
`PubSubDeserializationSchema.deserialize`, which is why that SPI takes a `SubscriptionDestination`.

## Options

`project` and `format` are always required; `topic` is required to write. The destination is not
declared as a required option because one factory serves both directions, and a table that is only
read from must not be forced to name a topic.

Topic names are bare names resolved against `project`, never resource paths — `'topic' = 'orders'`,
not `'topic' = 'projects/my-project/topics/orders'`.

### Shared

| Option | Type | Maps to |
|---|---|---|
| `project` | String, required | the project component of `TopicDestination.of(...)` / `SubscriptionDestination.of(...)` |
| `format` | String, required | format factory discovery, encoding or decoding as the direction needs |
| `service-account-key-file` | String | `serviceAccountKeyFile(...)`, a service-account JSON key-file path read when each writer, reader or enumerator starts |
| `emulator-endpoint` | String | `emulatorEndpoint(...)` as `host:port`. Parsed when the statement is planned, as everything on this page is, so a malformed value fails on the client in either direction. The rejection names `emulator-endpoint`, the key written in the DDL |

When `service-account-key-file` is absent, the connector uses application-default credentials.
That default already honors `GOOGLE_APPLICATION_CREDENTIALS`; set this option only when the job must select an explicit service-account JSON key path independently of its process environment.
The source reads the file on the JobManager for subscription administration and on each TaskManager that creates a reader, while the sink reads it on each TaskManager that creates a writer.
The same path must therefore be readable in every eligible process.
Each writer, reader or enumerator loads the file once and shares the resulting provider among the Pub/Sub clients it creates.
A read or parse failure reports neither the path nor credential material.
The DataStream page's [credential file deployment]({{< relref "docs/connectors/datastream/pubsub" >}}#credential-file-deployment) note covers Kubernetes Secret mounts, session clusters and key rotation for both APIs.

Service-account keys are long-lived secrets, so prefer an attached service account or Workload Identity where the deployment supports one.
Raw JSON, Base64-encoded JSON, access tokens, and custom credential-provider classes are not accepted by this option.
`service-account-key-file` and `emulator-endpoint` are mutually exclusive because the emulator channel deliberately uses no credentials.

### Sink

Every option maps onto one setter of `PubSubSinkBuilder`, `PubSubPublisherOptions.Builder` or
`TopicCreateOptions.Builder`, named in the last column. An option left out of the DDL leaves that
setter uncalled, so its default is whatever the connector or the SDK already uses — the default is
never restated here, and there is no third state between "configured" and "default".

The `sink.auto-create.*` options configure the topic that `sink.create-disposition` =
`create-if-needed` (the default) creates — they are additive settings, not an authorization, and
setting any of them alongside an explicit `create-never` is rejected.
`sink.auto-create.storage-policy.enforce-in-transit` requires
`sink.auto-create.storage-policy.allowed-regions`.

| Option | Type | Maps to |
|---|---|---|
| `topic` | String, required to write | `topic(...)` |
| `sink.create-disposition` | `create-if-needed` \| `create-never` | `createDisposition` |
| `sink.auto-create.message-retention` | Duration | `TopicCreateOptions` `messageRetention` |
| `sink.auto-create.kms-key-name` | String | `TopicCreateOptions` `kmsKeyName`. A blank value is refused under `sink.auto-create.kms-key-name`, the key written in the DDL |
| `sink.auto-create.storage-policy.allowed-regions` | String list | `TopicCreateOptions` `allowedPersistenceRegions`. A list that is blank, or that carries a blank entry, is refused under `sink.auto-create.storage-policy.allowed-regions`, the key written in the DDL |
| `sink.auto-create.storage-policy.enforce-in-transit` | Boolean | `TopicCreateOptions` `enforceInTransit` |
| `sink.batching.element-count-threshold` | Long | `batchElementCountThreshold` |
| `sink.batching.request-byte-threshold` | MemorySize | `batchRequestByteThreshold` |
| `sink.batching.delay-threshold` | Duration | `batchDelayThreshold` |
| `sink.retry.total-timeout` | Duration | `retryTotalTimeout` |
| `sink.retry.initial-delay` | Duration | `retryInitialDelay` |
| `sink.retry.delay-multiplier` | Double | `retryDelayMultiplier` |
| `sink.retry.max-delay` | Duration | `retryMaxDelay` |
| `sink.retry.initial-rpc-timeout` | Duration | `retryInitialRpcTimeout` |
| `sink.retry.rpc-timeout-multiplier` | Double | `retryRpcTimeoutMultiplier` |
| `sink.retry.max-rpc-timeout` | Duration | `retryMaxRpcTimeout` |
| `sink.retry.max-attempts` | Integer | `retryMaxAttempts` |
| `sink.message-ordering.enabled` | Boolean | `enableMessageOrdering` |
| `sink.in-flight.max-messages` | Integer | `maxInFlightMessages` |
| `sink.in-flight.max-bytes` | MemorySize | `maxInFlightBytes` |
| `sink.max-consecutive-rejections` | Integer | `maxConsecutiveRejections` |
| `sink.publish-progress-timeout` | Duration | `publishProgressTimeout` |
| `sink.recovery.initial-backoff` | Duration | `recoveryInitialBackoff` |
| `sink.recovery.max-backoff` | Duration | `recoveryMaxBackoff` |
| `sink.recovery.max-attempts` | Integer | `recoveryMaxAttempts` |
| `sink.shutdown-timeout` | Duration | `shutdownTimeout` |
| `sink.metrics.per-destination` | Boolean | `perDestinationMetrics` |
| `sink.parallelism` | Integer | the sink operator's parallelism |

`sink.retry.total-timeout` and `sink.retry.max-attempts` are **rejected** together with
`sink.message-ordering.enabled` = `true`, because an ordering-enabled publisher retries without
limit: neither an attempt cap nor a total timeout can bound a publish there. The `CREATE TABLE`
itself succeeds — the check runs when the table is used as a sink, so the failure lands at plan
time, before any record is published. The six other `sink.retry.*` keys are unaffected. See
[`PubSubPublisherOptions`]({{< relref "docs/reference/pubsub" >}}#pubsubpublisheroptions).

### Source

Every option maps onto one setter of `PubSubSourceBuilder` or `PubSubSubscriberOptions.Builder`,
under the same "absent means default" rule as the sink.

| Option | Type | Maps to |
|---|---|---|
| `subscription` | String list, `;`-separated, required to read | `subscriptions(...)` |
| `scan.ordering-mode` | `none` \| `per-key` | `orderingMode` |
| `scan.deserialization-failure-policy` | `fail` \| `drop` \| `nack` | `deserializationFailurePolicy` |
| `scan.flow-control.max-outstanding-element-count` | Long | `flowControlMaxOutstandingElementCount` |
| `scan.flow-control.max-outstanding-request-bytes` | MemorySize | `flowControlMaxOutstandingRequestBytes` |
| `scan.paused-split-buffer.max-messages` | Long | `pausedSplitBufferMaxMessages` |
| `scan.paused-split-buffer.max-bytes` | MemorySize | `pausedSplitBufferMaxBytes` |
| `scan.parallel-pull-count` | Integer | `parallelPullCount` |
| `scan.ack.max-extension-period` | Duration | `maxAckExtensionPeriod` |
| `scan.ack.min-duration-per-extension` | Duration | `minDurationPerAckExtension` |
| `scan.ack.max-duration-per-extension` | Duration | `maxDurationPerAckExtension` |
| `scan.ack.await-confirmation` | Duration | `awaitAckConfirmation` |
| `scan.shutdown-timeout` | Duration | `shutdownTimeout` |
| `scan.max-records-per-fetch` | Integer | `maxRecordsPerFetch` |
| `scan.first-checkpoint-timeout` | Duration | `firstCheckpointTimeout` |
| `scan.startup.mode` | `continue-from-subscription` \| `earliest-retained` \| `latest` \| `timestamp` | `StartPosition.of(mode, ...)` |
| `scan.startup.timestamp-millis` | Long, required by and only by `timestamp` | the instant of `StartPosition.of(...)` |
| `scan.auto-create.topics` | Map<String, String> | each subscription key maps to its `topic(...)` — see [Subscription auto-creation](#subscription-auto-creation-maps-every-subscription-to-its-topic) for both accepted syntaxes |
| `scan.auto-create.ack-deadline` | Duration | `ackDeadline` |
| `scan.auto-create.message-ordering.enabled` | Boolean | `enableMessageOrdering` |
| `scan.auto-create.message-retention` | Duration | `messageRetention` |
| `scan.auto-create.retain-acked-messages` | Boolean | `retainAckedMessages` |
| `scan.auto-create.expiration-ttl` | Duration | `expirationTtl` |
| `scan.auto-create.never-expire` | Boolean | `neverExpire()` |
| `scan.auto-create.dead-letter.topic` | String | `deadLetterPolicy(...)`, first argument |
| `scan.auto-create.dead-letter.max-delivery-attempts` | Integer | `deadLetterPolicy(...)`, second argument |
| `scan.auto-create.filter` | String | `filter` |
| `scan.parallelism` | Integer | the source operator's parallelism |

`scan.parallel-pull-count` and `scan.parallelism` are unrelated despite the names: the first is how
many gRPC streaming-pull connections one subscriber opens, the second is the Flink operator's
parallelism. So are `scan.flow-control.*` and the sink's `sink.in-flight.*` — the sink's are the
writer's own caps, because gax flow control could never be the byte bound an ordered sink needs
([#85]({{< param BookRepo >}}/issues/85)).

Several subscriptions are separated by `;` — `'subscription' = 'orders-sub;refunds-sub'` — and are
resolved against `project`, so a subscription in **another project cannot be named**;
`SubscriptionDestination` takes its components separately and parses no path.

Combinations the source itself refuses are not re-checked here: `scan.ordering-mode` = `per-key`
with `scan.parallel-pull-count` above 1, or a repeated subscription, both fail with the message the
DataStream builder already produces.

Byte-valued options are written the Flink way — `'sink.in-flight.max-bytes' = '64 mb'`.

### A start position seeks, and a seek is not local to your job

`scan.startup.mode` decides where the source begins. Only the default,
`continue-from-subscription`, leaves the subscription alone; every other value **seeks**.

```sql
CREATE TABLE orders (
  id STRING,
  amount INT
) WITH (
  'connector' = 'pubsub',
  'project' = 'my-project',
  'subscription' = 'orders-sub',
  'format' = 'json',
  'scan.startup.mode' = 'timestamp',
  'scan.startup.timestamp-millis' = '1735689600000'
);
```

A Pub/Sub subscription has no offset a reader resumes from: its position *is* server state, shared
by every consumer.

- **A seek rewrites state shared by every consumer of the subscription, including other jobs.** Use
  a non-default start position only on a subscription your job owns.
- **The seek runs once, at the first start of a job, and never on a restore.** The enumerator
  records that it ran in its checkpointed state, so a failover resumes rather than rewinding.
- **A redeploy without a savepoint seeks again**, because the state that remembered it is gone. So
  does a job that crash-loops before its first checkpoint completes.
- **`latest` is the one position that is not reproducible.** It resolves against the clock at the
  moment the seek runs, and it *drops* the existing backlog. Use `timestamp` when the boundary has
  to be exact.

How far back `earliest-retained` and a past `timestamp` reach is a property of the subscription, not
of this option: already-acknowledged messages are replayable only if the subscription sets
`retain-acked-messages` or its topic retains messages. Against a subscription with neither, a
backwards seek recovers only what was never acknowledged.

That is worth checking when the topic was created by `sink.create-disposition` =
`create-if-needed`, which without further options creates it with **service defaults and no
message retention** — so a backwards seek over such a topic recovers only the unacknowledged
backlog unless the subscription itself sets `scan.auto-create.retain-acked-messages`, or the
sink's table set `sink.auto-create.message-retention` when it created the topic
([#153]({{< param BookRepo >}}/issues/153)).

### Subscription auto-creation maps every subscription to its topic

Setting `scan.auto-create.topics` is what authorizes creating missing subscriptions.
Each map key is a subscription name and its value is the topic that subscription is bound to.
Without the map, every subscription named by `subscription` must already exist and the job fails at
startup if one does not.
An existing subscription is used exactly as it is configured: these settings apply to creation
only, and are neither applied to it nor compared against it.

**Only subscriptions are created. Every mapped topic must already exist.**
`scan.auto-create.topics` names topics to bind to, not topics to create, and neither they nor
`scan.auto-create.dead-letter.topic` are created on your behalf.
This is the opposite of `sink.create-disposition`, which does create a missing topic, so the two
halves of one DDL do not mean the same thing by "create": a source cannot invent a topic, because
which topic to consume is the whole question.

For one subscription, use one prefixed map entry:

```sql
CREATE TABLE orders (
  id STRING
) WITH (
  'connector' = 'pubsub',
  'project' = 'my-project',
  'subscription' = 'orders-sub',
  'format' = 'json',
  'scan.auto-create.topics.orders-sub' = 'orders',
  'scan.auto-create.ack-deadline' = '60 s',
  'scan.auto-create.retain-acked-messages' = 'true'
);
```

For several subscriptions, give each one its own entry under the same option prefix:

```sql
CREATE TABLE events (
  id STRING
) WITH (
  'connector' = 'pubsub',
  'project' = 'my-project',
  'subscription' = 'orders-sub;refunds-sub',
  'format' = 'json',
  'scan.auto-create.topics.orders-sub' = 'orders',
  'scan.auto-create.topics.refunds-sub' = 'refunds',
  'scan.auto-create.ack-deadline' = '60 s'
);
```

The prefixed form above is recommended because each DDL line names one subscription-to-topic
binding.
Flink also accepts the whole map in one option:

```sql
'subscription' = 'orders-sub;refunds-sub',
'scan.auto-create.topics' = 'orders-sub:orders,refunds-sub:refunds'
```

These options use different separators.
Flink splits the `subscription` list on `;`, but splits packed map entries on `,` and each entry's
key from its value on `:`.
For one subscription, the packed map has one pair and therefore no comma:
`'scan.auto-create.topics' = 'orders-sub:orders'`.
Do not separate packed map entries with `;`.
Use either prefixed entries or the packed map in one table; configuring both forms is rejected
because Flink otherwise gives the packed value precedence and silently ignores prefixed entries.

The map's key set must match `subscription` exactly.
A missing key would leave one subscription without a topic binding, while an unexpected key would
configure a subscription this table never consumes, so either is rejected during validation.
The other `scan.auto-create.*` settings are shared and applied to every missing subscription.

Three further rules, each because the option shape and the setter shape differ:

- `scan.auto-create.expiration-ttl` and `scan.auto-create.never-expire` = `true` are **rejected
  together**. They are alternatives, and a `WITH` clause has no ordering that could resolve the
  contradiction.
- `scan.auto-create.dead-letter.topic` and `scan.auto-create.dead-letter.max-delivery-attempts` are
  **required together**. Defaulting the attempt count would be a redelivery limit nobody chose.
- A `scan.auto-create.*` option set without `scan.auto-create.topics` is **rejected rather than
  ignored**, since nothing would read it.

Both topic names are bare names resolved against `project`, like `topic` on the sink side.

Creation is idempotent — `ALREADY_EXISTS` counts as success, so two jobs racing to create the same
subscription need no coordination. `enableExactlyOnceDelivery` is deliberately not offered: the
startup check rejects such a subscription, so the option would only let you create one the source
then refuses. Note that a subscription retains nothing published **before** it existed, so a table
that auto-creates one starts from an empty backlog whatever the topic already held.

## Delivery guarantees

At-least-once in both directions, unchanged from the DataStream connectors. The sink publishes
asynchronously and flushes at each checkpoint, so a failover republishes whatever the last completed
checkpoint did not cover.

**A source table needs checkpointing enabled.** Messages are acknowledged when a checkpoint
completes, so without one nothing is ever acknowledged and everything is redelivered forever; the
source detects that state and fails rather than stalling silently. Set
`execution.checkpointing.interval`.

The source's changelog mode is **the format's**, not a hard-coded insert-only, so a changelog format
over Pub/Sub works. The transport is still at-least-once — a redelivered `-U` is a real possibility,
which is a property of the pipeline to design around rather than one this connector can remove.

### Ordering keys are routed to one writer

When a table writes the `ordering-key` metadata column, the connector partitions its input before
the sink so every non-empty ordering key reaches one writer subtask.
This happens automatically at any sink parallelism; `DISTRIBUTED BY` and `INTO n BUCKETS` are not
part of the contract.
`sink.parallelism` controls only the number of sink writers and therefore the number of Pub/Sub
publishers.

A null or empty ordering key means that the message is unordered.
The connector spreads those rows across writers instead of routing all of them to one empty-key
hotspot.
With one sink writer no partitioning exchange is needed.

At higher parallelism the correctness guarantee has a cost: selecting the metadata column inserts
a network shuffle even when the upstream query already happens to have a compatible distribution.
One hot ordering key still goes through one writer and one Pub/Sub ordering-key sequence, so adding
writers cannot increase that key's throughput.
Choose `sink.parallelism` for the number and skew of ordering keys, publisher resource use, and the
extra shuffle rather than treating it as a bucket count.

### Inserts only

Pub/Sub has no way to express a retraction, so an updating query is rejected when the job is
planned rather than publishing its `-U` and `-D` rows as ordinary messages:

```sql
INSERT INTO orders SELECT id, COUNT(*) FROM staged GROUP BY id
-- Table sink ... doesn't support consuming update changes
```

## Design decisions

**No `properties.*` passthrough.** Kafka's `properties.*` is a string map the Kafka client parses
itself. Pub/Sub has no such surface, and the connector deliberately keeps SDK types off its public
API ([#20]({{< param BookRepo >}}/issues/20)), so a passthrough map would reintroduce exactly what
that decision keeps out. There is one typed option per builder setter, and a test asserts the two
sets match so neither can drift.

**Enum values are hyphenated because the enums spell themselves that way.** Flink resolves an enum
option by matching the configured value against `toString()` — case-insensitively, but with no
other normalization, so an underscore in the constant name would be an underscore in the DDL. The
connector's four enums (`CreateDisposition`, `OrderingMode`, `DeserializationFailurePolicy` and
`StartPosition.Mode`) therefore carry their option spelling in `toString()`, as Flink's own
`DeliveryGuarantee` does. Duplicating them as table-local enums was the alternative and was
declined: it would add four types and a conversion step for no gain.

**An ordering-key column without `sink.message-ordering.enabled` fails at plan time.** The writer
rejects any message carrying an ordering key while ordering is disabled, so the pair would
otherwise plan cleanly and fail on the first record.

**The metadata is written into the protobuf builder directly** rather than through the public
`withAttributes` / `withOrderingKey` combinators. Those take a `Map<String, String>`, so every
record would allocate an intermediate map only to copy it into the builder; the table sink already
holds the row's `MapData` and writes from it.

**Dynamic per-record topics are not exposed.** `DestinationResolver` makes them possible and Kafka
offers a `topic` metadata column, but the table sink writes to the one topic its DDL names: a SQL
job routes one source to several topics with multiple `INSERT` statements in a `STATEMENT SET`,
branching on `WHERE` clauses, which covers the fan-out case with topics known at plan time
([#140]({{< param BookRepo >}}/issues/140), closed as not needed — a use case `INSERT` branching
cannot cover gets a new issue referencing it). A topic genuinely computed from record data, unknown
at plan time, stays a [DataStream sink]({{< relref "docs/connectors/datastream/pubsub" >}}) job,
through `destinationResolver(...)`.

**Three setters do not fit one option each, and the DDL bends rather than the DataStream API.**
`startPosition(...)` takes a mode and, for one mode, an instant; `neverExpire()` takes no argument
and contradicts `expirationTtl(...)`; `deadLetterPolicy(...)` takes two. They become, respectively,
a mode-plus-timestamp pair, a boolean beside a duration with both-set rejected, and two options
required together. The alternatives were a `0` or `-1` sentinel on the duration and a string option
accepting the literal `never` — both invent a magic value, and the string one loses duration
parsing as well. Inventing a `max-delivery-attempts` default would likewise put back the third
state, between "configured" and "default", that this option design exists to remove.

**Auto-creation maps only the topic binding per subscription.**
The DataStream API keys creation settings by subscription because every subscription carries its
own topic binding.
`scan.auto-create.topics` expresses that one differing value as a map through Flink's standard
prefixed map syntax, while the remaining creation settings stay scalar and shared.
Requiring the map keys to equal the configured subscriptions prevents both an unbound subscription
and a silently unused binding without duplicating every setting for every subscription
([#152]({{< param BookRepo >}}/issues/152)).

**The two directions gate resource creation differently, and that is not an oversight.** The sink
gates topic creation with `sink.create-disposition`, an enum; the source has no disposition option
at all, and the presence of `scan.auto-create.topics` is the authorization. A topic needs no
configuration to exist, so "create with defaults" means something for it; a subscription without a
topic binding is not a subscription, so it cannot. The creation *settings*, on the other hand, are
spelled alike on purpose — `sink.auto-create.*` beside `scan.auto-create.*`
([#153]({{< param BookRepo >}}/issues/153) re-opened the naming that
[#137]({{< param BookRepo >}}/issues/137) settled for the gates): a setting is a setting whichever
side creates the resource, and where both sides carry one it keeps one key
(`message-retention` on both). The gate difference survives inside that alignment: the sink's
settings do not authorize anything — the disposition still does, it defaults to
`create-if-needed`, and combining settings with an explicit `create-never` is rejected, since
they would configure a topic the table never creates.

**The uber-jar relocates `grpc-netty-shaded` rather than exempting it.** The exemption is the
tempting answer, because that artifact carries native libraries whose names netty derives from its
own package, and maven-shade does not rename native resources. It was built that way first and
rejected on evidence: with `io.grpc.netty.shaded` left in place, the jar cannot share a classpath
with anything else carrying that package, and the failure is a `ServiceConfigurationError` reading
"NettyChannelProvider not a subtype" — a copy extending a differently relocated gRPC core. A second
GCP SQL connector built the same way would be the first thing to trigger it, so the exemption
traded a real collision for a hypothetical one. Relocating instead costs two extra relocations that
rename the native libraries in step, and the packaging test derives the expected names from the
shaded prefix so the two cannot drift.

One consequence to know before reaching for a tuning flag: relocation rewrites netty's **system
property names** along with its packages. Inside this jar the knob spelled
`io.grpc.netty.shaded.io.netty.maxDirectMemory` upstream becomes
`io.github.flink.gcp.connector.pubsub.shaded.io.grpc.netty.shaded.io.netty.maxDirectMemory`, so a
`-D` using the upstream name has no effect. This is inherent to relocating an already-relocated
gRPC and is true of every project listed below.

That pairing is the established practice rather than a local invention: the same two entries appear
in googleapis/java-bigtable-hbase, Dataproc's gcs-connector, spark-bigquery, Beam's gRPC vendoring
and the uber-jars of both Google-maintained Flink connectors. The project that skipped it,
GoogleCloudDataproc/flink-bigquery-connector, publishes a jar whose tcnative and epoll libraries can
never be found. What remains untested here is whether the renamed libraries load through JNI, which
happens only on Linux with epoll or tcnative; a wrong rename there degrades to NIO and JDK SSL
silently rather than failing.

## Testing

Unit tests cover the factory in both directions (identifier, required options, format discovery,
parallelism, and the destination mistakes each direction invites), the option-to-setter mapping —
including reflective checks that every `PubSubPublisherOptions.Builder`, `PubSubSubscriberOptions
.Builder` and `SubscriptionCreateOptions.Builder` setter has an option and vice versa, and that
every declared option is one the factory accepts — the enum spellings and their round trip through a
`ConfigOption`, the row-to-message conversion and the message-to-rows conversion. The creation
mapper's check maps a setter to a *set* of options, since two of its setters are not one option
each; every rejection it owns has a test of its own, because none of them has a DataStream backstop
to fall through to.

Four integration tests run SQL against the Pub/Sub emulator in a MiniCluster through the
production factory, with the endpoint passed as the `emulator-endpoint` option rather than through
a test-only factory. No cloud credentials are needed.

- `PubSubTableSinkITCase` — a `CREATE TABLE` with both writable metadata columns, an `INSERT`, and a
  raw pull asserting the payload, the attributes and the ordering key; plus topic auto-creation,
  `create-never`, and the two plan-time refusals.
- `PubSubTableSourceITCase` — reading messages published outside Flink, telling two subscriptions
  apart through the `subscription` column, and `drop` skipping an undecodable message. The last one
  publishes under one ordering key with `scan.ordering-mode` = `per-key`, because the SDK's receiver
  callbacks otherwise arrive out of order and the test would pass under `fail` too.
- `PubSubTableRoundTripITCase` — what SQL writes is what SQL reads back, over the same topic and
  subscription, with every metadata column asserted.
- `PubSubTableAutoCreateITCase` — the two features whose effects exist only on the service: a
  subscription the table created and then consumed, with its settings read back off the service;
  `earliest-retained` replaying a backlog that was already acknowledged elsewhere; and `timestamp`
  replaying only what was published after a cutoff. Both seek tests **acknowledge the whole backlog
  outside Flink first**, so the seek is the only thing that can produce a row — otherwise a seek
  that never happened would deliver the same rows plus older ones, and the emulator's delivery
  order would decide the result. The cutoff is the publish time the service assigned plus one
  millisecond, not `Instant.now()`: the container's clock and the test JVM's are not the same one.
  Both subscriptions are unordered, because the emulator does not support seek on an
  ordering-enabled one.

A source test's `TableEnvironment` enables checkpointing and disables restarts, and rows are drained
by **distinct** count with a deadline: the transport is at-least-once, so counting total rows would
let one redelivery crowd out an original.

The uber-jar is covered separately, in `flink-sql-connector-gcp-pubsub`.

- `PubSubSqlConnectorPackagingITCase` reads the built jar: the factory SPI file SQL discovers the
  connector through, that no class outside the shaded prefix is missing from a short documented
  allow-list, that the netty native libraries were renamed to match their relocated package, that
  the relocated gRPC service file names the relocated provider, and that the `NOTICE` claims no
  Apache provenance.
- `PubSubSqlConnectorSmokeITCase` runs a SQL round trip against the emulator **through the shaded
  classes** — the module's surefire configuration drops the connector artifact from the test
  classpath and adds the uber-jar, and the test asserts the factory really did load from there,
  because a regression in that setup would leave every other assertion about the wrong code. This
  is the only test that exercises relocation at runtime; opening a Pub/Sub channel is what puts
  relocated gRPC and its relocated netty transport together. The harness drives the emulator with
  the *stock*, unrelocated admin client, so the two coexisting on one classpath is itself part of
  what is asserted.
- `BundledDependenciesNoticeTest` diffs `META-INF/NOTICE` against the runtime dependency tree
  recorded during the build, in both directions. The `artifactSet` is `*:*`, so a new transitive is
  bundled automatically; this test is what makes it fail the build until the NOTICE is regenerated
  to record it.
