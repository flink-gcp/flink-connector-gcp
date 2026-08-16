# flink-connector-gcp-cloudtasks

Cloud Tasks sink for Apache Flink — dispatching a stream as HTTP or App Engine tasks that the
service executes later, paced by the queue's rate limit.

| Feature | Status |
|---|---|
| Sink design (targets, task naming/dedup, rate limits, checkpoint semantics) | Design settled ([#23](https://github.com/laughingman7743/flink-connector-gcp/issues/23)) |
| SinkV2 at-least-once sink; HTTP and App Engine targets; fixed and per-record queue destinations | Implemented ([#24](https://github.com/laughingman7743/flink-connector-gcp/issues/24), [#628](https://github.com/laughingman7743/flink-connector-gcp/issues/628)) |
| Opt-in named-task deduplication | Implemented ([#24](https://github.com/laughingman7743/flink-connector-gcp/issues/24)) |
| Emulator integration tests | Implemented ([#25](https://github.com/laughingman7743/flink-connector-gcp/issues/25)) |
| Per-task failure policy (fail, drop or dead-letter) | Implemented ([#207](https://github.com/laughingman7743/flink-connector-gcp/issues/207)) |
| Table API / SQL sink, generic body formats and writable request metadata | Implemented ([#605](https://github.com/laughingman7743/flink-connector-gcp/issues/605)) |
| Form-encoded SQL body format | Implemented ([#606](https://github.com/laughingman7743/flink-connector-gcp/issues/606)) |
| Shaded SQL connector jar | Implemented ([#607](https://github.com/laughingman7743/flink-connector-gcp/issues/607)) |

<!-- readme-example file="CloudTasksReadmeOverview.java" tag="cloud-tasks-readme-overview" -->
```java
Sink<OrderEvent> sink =
        CloudTasksSink.<OrderEvent>builder()
                .queue(QueueDestination.of("my-project", "asia-northeast1", "webhooks"))
                .serializer(
                        CloudTasksSerializationSchema.httpTarget(
                                        "https://api.example.com/v1/orders")
                                .withBody(new MyEventJsonSerializationSchema())
                                .withOidcToken(
                                        "dispatcher@my-project.iam.gserviceaccount.com"))
                .build();
```

## Documentation

The connector documentation — what the connector is for, targets and authorization, task naming and
deduplication, delivery guarantees, why retries are the sink's responsibility here and how they are
tuned, and how queue rate limits relate to sink concurrency — is in the
[Cloud Tasks connector guide](https://flink-gcp.github.io/flink-connector-gcp/docs/connectors/datastream/cloudtasks/).

The SQL request model, writable metadata and `WITH` options are in the
[Table API connector page](https://flink-gcp.github.io/flink-connector-gcp/docs/connectors/table/cloudtasks/).

A complete runnable job is in
[Quickstart](https://flink-gcp.github.io/flink-connector-gcp/docs/quickstart/cloudtasks/); sharding
across queues and emulator-backed local runs are worked through in
[Examples](https://flink-gcp.github.io/flink-connector-gcp/docs/examples/cloudtasks/). Every option
the sink takes, with its default, is in the
[configuration reference](https://flink-gcp.github.io/flink-connector-gcp/docs/reference/cloudtasks/).

## Provenance and attribution

This module is an original implementation. No Flink Cloud Tasks connector exists in Apache Flink,
in [GoogleCloudPlatform/pubsub](https://github.com/GoogleCloudPlatform/pubsub) or elsewhere in open
source, so nothing is vendored here. Its design references the Pub/Sub sink in this repository (the
mailbox-based in-flight cap, the flush-on-checkpoint stateless writer, the serialization-schema
shape) and Google's Cloud Tasks documentation.

No source code has been copied into this module.
