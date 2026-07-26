---
title: Cloud Pub/Sub
type: docs
weight: 20
---

<!--
Copyright 2026 laughingman7743

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
DataStream sink and source described in
{{< relref "docs/connectors/datastream/pubsub" >}}, which is where the behavior behind every option
is documented; this page covers the option surface and the decisions specific to SQL.

Per-feature implementation status — including which directions are implemented — is tracked in the
[module README]({{< param BookRepo >}}/blob/main/flink-connector-gcp-pubsub/README.md).

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
| `emulator-endpoint` | String | `emulatorEndpoint(...)` |

Credentials are not configurable: the connector uses application default credentials, exactly as
the DataStream API does. Making them configurable is tracked in
[#139]({{< param BookRepo >}}/issues/139).

### Sink

Every option maps onto one setter of `PubSubSinkBuilder` or `PubSubPublisherOptions.Builder`, named
in the last column. An option left out of the DDL leaves that setter uncalled, so its default is
whatever the connector or the SDK already uses — the default is never restated here, and there is
no third state between "configured" and "default".

| Option | Type | Maps to |
|---|---|---|
| `topic` | String, required to write | `topic(...)` |
| `sink.create-disposition` | `create-if-needed` \| `create-never` | `createDisposition` |
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
| `sink.recovery.initial-backoff` | Duration | `recoveryInitialBackoff` |
| `sink.recovery.max-backoff` | Duration | `recoveryMaxBackoff` |
| `sink.recovery.max-attempts` | Integer | `recoveryMaxAttempts` |
| `sink.parallelism` | Integer | the sink operator's parallelism |

### Source

Every option maps onto one setter of `PubSubSourceBuilder` or `PubSubSubscriberOptions.Builder`,
under the same "absent means default" rule as the sink.

| Option | Type | Maps to |
|---|---|---|
| `subscription` | String list, required to read | `subscriptions(...)` |
| `scan.ordering-mode` | `none` \| `per-key` | `orderingMode` |
| `scan.deserialization-failure-policy` | `fail` \| `drop` \| `nack` | `deserializationFailurePolicy` |
| `scan.flow-control.max-outstanding-element-count` | Long | `flowControlMaxOutstandingElementCount` |
| `scan.flow-control.max-outstanding-request-bytes` | MemorySize | `flowControlMaxOutstandingRequestBytes` |
| `scan.parallel-pull-count` | Integer | `parallelPullCount` |
| `scan.ack.max-extension-period` | Duration | `maxAckExtensionPeriod` |
| `scan.ack.min-duration-per-extension` | Duration | `minDurationPerAckExtension` |
| `scan.ack.max-duration-per-extension` | Duration | `maxDurationPerAckExtension` |
| `scan.ack.await-confirmation` | Duration | `awaitAckConfirmation` |
| `scan.shutdown-timeout` | Duration | `shutdownTimeout` |
| `scan.max-records-per-fetch` | Integer | `maxRecordsPerFetch` |
| `scan.first-checkpoint-timeout` | Duration | `firstCheckpointTimeout` |
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

### Ordering from SQL needs `sink.parallelism` = `1`

Pub/Sub orders an ordering key's messages only among publishes from one client, and the sink owns
one publisher per writer subtask. The DataStream API answers that with a `keyBy` on the ordering
key before the sink; **SQL has no equivalent**. `DISTRIBUTED BY` is rejected because this sink does
not implement `SupportsBucketing`, and nothing else keys the sink's input, so at any parallelism
above one two rows sharing an ordering key may be published by two subtasks and arrive out of
order.

Until that is addressed ([#143]({{< param BookRepo >}}/issues/143)), a table that writes the
`ordering-key` metadata column and actually depends on the order must also set
`'sink.parallelism' = '1'`. The connector does not enforce this: a single-subtask-per-key
distribution arranged upstream is legitimate, and the sink cannot tell the difference.

### Inserts only

Pub/Sub has no way to express a retraction, so an updating query is rejected when the job is
planned rather than publishing its `-U` and `-D` rows as ordinary messages:

```
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
offers a `topic` metadata column, but the table sink writes to the one topic its DDL names; see
[#140]({{< param BookRepo >}}/issues/140).

## Testing

Unit tests cover the factory in both directions (identifier, required options, format discovery,
parallelism, and the destination mistakes each direction invites), the option-to-setter mapping —
including reflective checks that every `PubSubPublisherOptions.Builder` and every
`PubSubSubscriberOptions.Builder` setter has an option and vice versa, and that every declared
option is one the factory accepts — the enum spellings and their round trip through a
`ConfigOption`, the row-to-message conversion and the message-to-rows conversion.

Three integration tests run SQL against the Pub/Sub emulator in a MiniCluster through the
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

A source test's `TableEnvironment` enables checkpointing and disables restarts, and rows are drained
by **distinct** count with a deadline: the transport is at-least-once, so counting total rows would
let one redelivery crowd out an original.
