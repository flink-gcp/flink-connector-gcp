# flink-connector-gcp-cloudtasks

Cloud Tasks sink for Apache Flink — dispatching a stream as HTTP tasks that the service executes
later, paced by the queue's rate limit.

| Feature | Status |
|---|---|
| Sink design (targets, task naming/dedup, rate limits, checkpoint semantics) | Design settled ([#23](https://github.com/laughingman7743/flink-connector-gcp/issues/23)) |
| SinkV2 at-least-once sink; HTTP targets; fixed and per-record queue destinations | Implemented ([#24](https://github.com/laughingman7743/flink-connector-gcp/issues/24)) |
| Opt-in named-task deduplication | Implemented ([#24](https://github.com/laughingman7743/flink-connector-gcp/issues/24)) |
| Emulator integration tests | Implemented ([#25](https://github.com/laughingman7743/flink-connector-gcp/issues/25)) |
| Per-task failure policy (fail, drop or dead-letter) | Implemented ([#207](https://github.com/laughingman7743/flink-connector-gcp/issues/207)) |
| Table API / SQL support | Planned ([#99](https://github.com/laughingman7743/flink-connector-gcp/issues/99)) |

```java
Sink<OrderEvent> sink =
        CloudTasksSink.<OrderEvent>builder()
                .queue(QueueDestination.of("my-project", "asia-northeast1", "webhooks"))
                .serializer(
                        CloudTasksSerializationSchema
                                .httpTarget("https://api.example.com/v1/orders")
                                .withBody(new MyEventJsonSerializationSchema())
                                .withOidcToken("dispatcher@my-project.iam.gserviceaccount.com"))
                .build();
```

## Documentation

The connector documentation — what the connector is for, targets and authorization, task naming and
deduplication, delivery guarantees, why retries are the sink's responsibility here and how they are
tuned, and how queue rate limits relate to sink concurrency — is in
[docs/content/docs/connectors/datastream/cloudtasks.md](../docs/content/docs/connectors/datastream/cloudtasks.md)
(rendered on the documentation site once it is published).

A complete runnable job is in
[Quickstart](../docs/content/docs/quickstart/cloudtasks.md); sharding across queues and
emulator-backed local runs are worked through in
[Examples](../docs/content/docs/examples/cloudtasks.md). Every option the sink takes, with its
default, is in the [configuration reference](../docs/content/docs/reference/cloudtasks.md).

## Provenance and attribution

This module is an original implementation. No Flink Cloud Tasks connector exists in Apache Flink,
in [GoogleCloudPlatform/pubsub](https://github.com/GoogleCloudPlatform/pubsub) or elsewhere in open
source, so nothing is vendored here. Its design references the Pub/Sub sink in this repository (the
mailbox-based in-flight cap, the flush-on-checkpoint stateless writer, the serialization-schema
shape) and Google's Cloud Tasks documentation.

No source code has been copied into this module.
