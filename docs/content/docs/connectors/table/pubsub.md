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

## The payload and the rest of the message

A Pub/Sub message is a payload plus attributes and an ordering key. The payload is what `format`
encodes, from the table's physical columns; everything else is a metadata column.

Metadata columns are appended after the physical columns, so the format never sees them — a `json`
table with the DDL above publishes `{"order_id":"...","amount":...}` and carries `attrs` and `okey`
outside the payload.

| Metadata key | Type | Notes |
|---|---|---|
| `attributes` | `MAP<STRING, STRING>` | A null column adds no attributes. A null key or a null value in the map **fails the write**: Pub/Sub attributes can represent neither, and dropping the entry would be data loss the query cannot see. Filter such entries out first |
| `ordering-key` | `STRING` | A null or empty value sets no key. Requires `sink.message-ordering.enabled` = `true`, and see the ordering caveat below |

Metadata is not forwarded to the format. No built-in format ships writable metadata, and the Kafka
connector does not forward either.

## Options

`project` and `format` are always required; `topic` is required to write. The destination is not
declared as a required option because one factory serves both directions, and a table that is only
read from must not be forced to name a topic.

Topic names are bare names resolved against `project`, never resource paths — `'topic' = 'orders'`,
not `'topic' = 'projects/my-project/topics/orders'`.

### Shared

| Option | Type | Maps to |
|---|---|---|
| `project` | String, required | the project component of `TopicDestination.of(...)` |
| `format` | String, required | `SerializationFormatFactory` discovery |
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

Byte-valued options are written the Flink way — `'sink.in-flight.max-bytes' = '64 mb'`.

## Delivery guarantees

At-least-once, unchanged from the DataStream sink: messages are published asynchronously and
flushed at each checkpoint, and a failover republishes whatever the last completed checkpoint did
not cover.

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

Unit tests cover the factory (identifier, required options, format discovery, parallelism), the
option-to-setter mapping — including a reflective check that every `PubSubPublisherOptions.Builder`
setter has an option and vice versa — the enum spellings and their round trip through a
`ConfigOption`, and the row-to-message conversion.

`PubSubTableSinkITCase` runs SQL against the Pub/Sub emulator in a MiniCluster through the
production factory, with the emulator endpoint passed as the `emulator-endpoint` option rather than
through a test-only factory: a `CREATE TABLE` with both metadata columns, an `INSERT`, and a raw
pull asserting the payload, the attributes and the ordering key; plus topic auto-creation,
`create-never`, and the two plan-time refusals. No cloud credentials are needed.
