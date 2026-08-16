# flink-connector-gcp-pubsub

Cloud Pub/Sub sink and source for Apache Flink, with dynamic per-record topic destinations on the
sink and multi-subscription consumption on the source.

## Sink

| Sink feature | Status |
|---|---|
| SinkV2 at-least-once sink; per-record topic resolution; per-topic publishers; checkpoint flush | Implemented ([#18](https://github.com/laughingman7743/flink-connector-gcp/issues/18)) |
| Topic auto-creation | Implemented ([#19](https://github.com/laughingman7743/flink-connector-gcp/issues/19)) |
| Attributes/ordering-key conveniences; message ordering; batching/retry options; recovery knobs; in-flight message and byte caps | Implemented ([#20](https://github.com/laughingman7743/flink-connector-gcp/issues/20), byte cap [#85](https://github.com/laughingman7743/flink-connector-gcp/issues/85)) |
| Per-message failure policy; fatal-exception classifier | Implemented ([#206](https://github.com/laughingman7743/flink-connector-gcp/issues/206)) |
| Cross-connector dead-letter queue (`PubSubDeadLetterQueue`) | Implemented ([#211](https://github.com/laughingman7743/flink-connector-gcp/issues/211)) |
| Emulator integration tests | Implemented ([#21](https://github.com/laughingman7743/flink-connector-gcp/issues/21)) |

## Source

| Source feature | Status |
|---|---|
| FLIP-27 at-least-once source; multi-subscription splits; checkpoint-bound acknowledgement; nack on close | Implemented ([#79](https://github.com/laughingman7743/flink-connector-gcp/issues/79)) |
| Ordering mode preserving per-ordering-key order | Implemented ([#79](https://github.com/laughingman7743/flink-connector-gcp/issues/79)) |
| Subscriber tuning options (flow control, ack extension, parallel pull, shutdown); missing-checkpoint detection | Implemented ([#80](https://github.com/laughingman7743/flink-connector-gcp/issues/80)) |
| Deserialization failure policy; nack on collect failure; metrics; acknowledgement confirmation | Implemented ([#80](https://github.com/laughingman7743/flink-connector-gcp/issues/80)) |
| Startup check; subscription auto-creation; start position (seek); `NACK` failure policy | Implemented ([#81](https://github.com/laughingman7743/flink-connector-gcp/issues/81)) |
| Acceptance and real-GCP integration tests | Implemented ([#82](https://github.com/laughingman7743/flink-connector-gcp/issues/82)) |

## Table API / SQL

| Table API / SQL feature | Status |
|---|---|
| `pubsub` table connector; `DynamicTableSink` with `attributes`/`ordering-key` metadata columns | Implemented ([#135](https://github.com/laughingman7743/flink-connector-gcp/issues/135)) |
| `DynamicTableSource` (scan) with `message-id`/`publish-time`/`attributes`/`ordering-key`/`subscription` metadata columns | Implemented ([#136](https://github.com/laughingman7743/flink-connector-gcp/issues/136)) |
| Subscription auto-creation, including per-subscription topic maps, and start position as table options | Implemented ([#137](https://github.com/laughingman7743/flink-connector-gcp/issues/137), [#152](https://github.com/laughingman7743/flink-connector-gcp/issues/152)) |
| `flink-sql-connector-gcp-pubsub` shaded uber-jar | Implemented ([#138](https://github.com/laughingman7743/flink-connector-gcp/issues/138)) |

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

CREATE TABLE incoming_orders (
  order_id     STRING,
  publish_time TIMESTAMP_LTZ(3) METADATA FROM 'publish-time' VIRTUAL
) WITH (
  'connector'    = 'pubsub',
  'project'      = 'my-project',
  'subscription' = 'orders-sub',
  'format'       = 'json'
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

Parts of it have since been rewritten. **Nine files still carry the upstream licence header.** Four
because they still hold upstream's code:

- `PubSubAckTracker` — the checkpoint sweep that acknowledges everything bound at or below a
  completed checkpoint, and the state it sweeps
- `PubSubSourceReader` — its two acknowledgement-tracker calls, which are upstream's whole method
  bodies
- `PayloadSerializationSchema` / `PayloadDeserializationSchema` — the payload-only adapters,
  whose `open` (and, on the read side, `getProducedType`) forward to the wrapped Flink schema and
  have no other possible form

Four because an exact upstream declaration survives in them — `AckTracker`, `PullSubscriber`
(formerly `NotifyingPullSubscriber`), `PubSubSerializationSchema` and
`PubSubDeserializationSchema`. A method name and an empty parameter list very likely carry no
authorship, but the project does not need to be right about that, so the notice stays and the
question is not reached.

And `StreamingPullSubscriber` (formerly `PubSubNotifyingPullSubscriber`), re-measured after the
rewrite that split its teardown out ([#755](https://github.com/flink-gcp/flink-connector-gcp/issues/755)):
its message buffer is still declared exactly as upstream declares it and `receiveMessage` keeps
upstream's statement arrangement, so the notice stays. The extracted `SubscriberTeardown` carries
this repository's own header — the teardown machinery has no upstream counterpart at all.

The four files whose header was retired — `PubSubWriter`, `PubSubSplitReader`,
`PubSubRecordEmitter` and `SubscriptionSplit` — share nothing with upstream but generic Java and
declarations Flink's own SPI dictates. Which files those are was decided by comparing each against
upstream rather than by how much had changed; the audit and its method are in `docs/adr/0123`.

Deviations from upstream, sink side: dynamic per-record topic destinations with a writer-owned
per-topic publisher map (upstream: single fixed topic with a JVM-wide static publisher cache),
mailbox-based backpressure with in-flight message and byte caps and async error capture (upstream: unbounded
future list), and a hand-written builder following this repository's conventions (upstream:
AutoValue).

Deviations from upstream, source side: multi-subscription enumeration with a deterministic split
plan (upstream: one hard-coded subscription, one split per registered subtask — which supports
neither several subscriptions nor ordering, since every subtask then opens its own streaming pull
against the same subscription); an ordering mode; per-split acknowledgement scoping (upstream's
tracker is reader-wide, so closing one subscriber nacks every split's messages); a `Collector`-based
deserialization contract (upstream returns one nullable record and then collects it without a null
check); hand-written serializers (upstream: protobuf code generation); an explicit emulator endpoint
(upstream: `PUBSUB_EMULATOR_HOST` sniffing plus an `emulator:///` URI prefix, where the environment
variable silently overrides an explicitly configured endpoint); and a hand-written builder.
Rewriting the reader stack also fixed several upstream defects: a `null` user-code class loader
passed to the deserialization schema, a fresh `Configuration` in place of the job's (which made the
source reader options unreachable), draining at most one message per split per fetch, rejecting
split removal, not overriding `pauseOrResumeSplits` (breaking watermark alignment), a missing
`return` in the wake-up branch, and `shutdown()` mutating lock-guarded state without holding the
lock.

[apache/flink-connector-gcp-pubsub](https://github.com/apache/flink-connector-gcp-pubsub) is a
**design reference only** — the mailbox-based backpressure model, the idea of a fatal-exception
classifier ([#206](https://github.com/laughingman7743/flink-connector-gcp/issues/206)), and its synchronous-pull decision record, which the source weighs and departs
from above — no code has been copied from it.
