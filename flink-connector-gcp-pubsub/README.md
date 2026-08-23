# flink-connector-gcp-pubsub

Cloud Pub/Sub sink and source for Apache Flink, with dynamic per-record topic destinations on the
sink and multi-subscription consumption on the source.

## Sink

| Sink feature | Status |
|---|---|
| SinkV2 at-least-once sink; per-record topic resolution; per-topic publishers; checkpoint flush | Implemented ([#18](https://github.com/flink-gcp/flink-connector-gcp/issues/18)) |
| Topic auto-creation | Implemented ([#19](https://github.com/flink-gcp/flink-connector-gcp/issues/19)) |
| Attributes/ordering-key conveniences; message ordering; batching/retry options; recovery knobs; in-flight message and byte caps | Implemented ([#20](https://github.com/flink-gcp/flink-connector-gcp/issues/20), byte cap [#85](https://github.com/flink-gcp/flink-connector-gcp/issues/85)) |
| Per-message failure policy; fatal-exception classifier | Implemented ([#206](https://github.com/flink-gcp/flink-connector-gcp/issues/206)) |
| Cross-connector dead-letter queue (`PubSubDeadLetterQueue`) | Implemented ([#211](https://github.com/flink-gcp/flink-connector-gcp/issues/211)) |
| Emulator integration tests | Implemented ([#21](https://github.com/flink-gcp/flink-connector-gcp/issues/21)) |

## Source

| Source feature | Status |
|---|---|
| FLIP-27 at-least-once source; multi-subscription splits; checkpoint-bound acknowledgement; nack on close | Implemented ([#79](https://github.com/flink-gcp/flink-connector-gcp/issues/79)) |
| Ordering mode preserving per-ordering-key order | Implemented ([#79](https://github.com/flink-gcp/flink-connector-gcp/issues/79)) |
| Subscriber tuning options (flow control, ack extension, parallel pull, shutdown); missing-checkpoint detection | Implemented ([#80](https://github.com/flink-gcp/flink-connector-gcp/issues/80)) |
| Deserialization failure policy; nack on collect failure; metrics; acknowledgement confirmation | Implemented ([#80](https://github.com/flink-gcp/flink-connector-gcp/issues/80)) |
| Startup check; subscription auto-creation; start position (seek); `NACK` failure policy | Implemented ([#81](https://github.com/flink-gcp/flink-connector-gcp/issues/81)) |
| Acceptance and real-GCP integration tests | Implemented ([#82](https://github.com/flink-gcp/flink-connector-gcp/issues/82)) |

## Table API / SQL

| Table API / SQL feature | Status |
|---|---|
| `pubsub` table connector; `DynamicTableSink` with `attributes`/`ordering-key` metadata columns | Implemented ([#135](https://github.com/flink-gcp/flink-connector-gcp/issues/135)) |
| `DynamicTableSource` (scan) with `message-id`/`publish-time`/`attributes`/`ordering-key`/`subscription` metadata columns | Implemented ([#136](https://github.com/flink-gcp/flink-connector-gcp/issues/136)) |
| Subscription auto-creation, including per-subscription topic maps, and start position as table options | Implemented ([#137](https://github.com/flink-gcp/flink-connector-gcp/issues/137), [#152](https://github.com/flink-gcp/flink-connector-gcp/issues/152)) |
| `flink-sql-connector-gcp-pubsub` shaded uber-jar | Implemented ([#138](https://github.com/flink-gcp/flink-connector-gcp/issues/138)) |

<!-- readme-example file="PubSubConnectorOverview.java" tag="pubsub-connector-overview" -->
```java
Sink<MyEvent> sink =
        PubSubSink.<MyEvent>builder()
                .destinationResolver(
                        (e, ctx) -> TopicDestination.of("my-project", e.topicName()))
                .serializer(
                        PubSubSerializationSchema.payload(new MyEventSerializationSchema())
                                .withAttributes(e -> Map.of("source", e.source()))
                                .withOrderingKey(MyEvent::deviceId))
                .publisherOptions(
                        PubSubPublisherOptions.builder()
                                .enableMessageOrdering(true)
                                .batchDelayThreshold(Duration.ofMillis(10))
                                .build())
                .build();
```

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
```

## Documentation

Design, delivery guarantees, publisher options, topic auto-creation, the source's split model and
ordering semantics, error handling and the testing strategy are documented in the
[Pub/Sub connector guide](https://flink-gcp.github.io/flink-connector-gcp/docs/connectors/datastream/pubsub/).
The Table API / SQL option surface is documented in the
[Pub/Sub SQL connector guide](https://flink-gcp.github.io/flink-connector-gcp/docs/connectors/table/pubsub/).

Complete runnable jobs — sink, source and SQL — are in
[Quickstart](https://flink-gcp.github.io/flink-connector-gcp/docs/quickstart/pubsub/); dynamic
destinations, auto-creation and emulator-backed local runs are worked through in
[Examples](https://flink-gcp.github.io/flink-connector-gcp/docs/examples/pubsub/). Every option the
sink and source take, with its default, is in the
[configuration reference](https://flink-gcp.github.io/flink-connector-gcp/docs/reference/pubsub/).

## Provenance and attribution

This module began as an adaptation of the Flink connector in
[GoogleCloudPlatform/pubsub](https://github.com/GoogleCloudPlatform/pubsub) (`flink-connector/`,
Apache-2.0, Copyright 2023 Google LLC) — the sink under issue
[#17](https://github.com/flink-gcp/flink-connector-gcp/issues/17), the source reader core under
[#31](https://github.com/flink-gcp/flink-connector-gcp/issues/31) — and the adaptation is also
recorded in the repository-level `NOTICE`.

Parts of it have since been rewritten, and the design now deviates substantially on both sides:
dynamic per-record topic destinations, mailbox-based backpressure with in-flight message and
byte caps, multi-subscription splits with an ordering mode, per-split acknowledgement scoping
and a `Collector`-based deserialization contract, among others. The
[connector guide](https://flink-gcp.github.io/flink-connector-gcp/docs/connectors/datastream/pubsub/)
documents the current design; what the rewrite changed relative to the adaptation, including
the upstream defects it fixed, is recorded in
[docs/adr/0122](../docs/adr/0122-the-pubsub-internals-are-decomposed-by-concern-rather-than-inherited-from-the-adaptation.md).

**Nine files still carry the upstream licence header**: `PubSubAckTracker`,
`PubSubSourceReader`, `PayloadSerializationSchema`, `PayloadDeserializationSchema`,
`AckTracker`, `PullSubscriber`, `PubSubSerializationSchema`, `PubSubDeserializationSchema` and
`StreamingPullSubscriber`. Four others had theirs retired because nothing upstream-specific
survives in them. Which files, why, and the residue-not-percentage rule that decides are the
file-by-file audit in
[docs/adr/0123](../docs/adr/0123-a-pubsub-file-keeps-its-upstream-notice-while-it-still-carries-upstream-expression.md).

[apache/flink-connector-gcp-pubsub](https://github.com/apache/flink-connector-gcp-pubsub) is a
**design reference only** — the mailbox-based backpressure model, the idea of a fatal-exception
classifier ([#206](https://github.com/flink-gcp/flink-connector-gcp/issues/206)), and its
synchronous-pull decision record, which this source's streaming-pull design weighs and departs
from — no code has been copied from it.
