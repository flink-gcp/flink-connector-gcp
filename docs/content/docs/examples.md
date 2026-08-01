---
title: Examples
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

# Examples

Worked examples of the four things the connector pages describe at length but never show whole:
routing each record to its own destination, configuring exactly-once, letting the connector create
what it writes to, and running the lot against an emulator.

Each starts from the jobs in the [Quickstart]({{< relref "docs/quickstart" >}}), so only the parts
that change are shown. The reasoning behind every option is on the connector's own page, linked from each
section.

## Dynamic per-record destinations

All three sinks resolve their destination per record from the same shape — a `destinationResolver`
in place of the fixed `destination` / `topic` / `queue` — so one sink instance fans out across
tables, topics or queues.

**The resolver runs once per record on the write path.** That is the constraint the examples below
are built around: it must be cheap, deterministic, and it should hand back cached destination
instances rather than allocating one per record, because destination identity is what the sinks key
their per-destination state on.

### BigQuery: a table per day

The writer context carries the record's event timestamp, which is what makes time-based routing
expressible without the record having to carry the routing key itself.

```java
public class DailyTableResolver implements DestinationResolver<OrderEvent> {

    private static final DateTimeFormatter SUFFIX = DateTimeFormatter.ofPattern("yyyyMMdd");

    private final String project;
    private final String dataset;
    private final String prefix;

    // One entry per day rather than one TableDestination per record. A plain HashMap is enough:
    // the writer is single-threaded per subtask, and this resolver is never shared across them.
    private final Map<LocalDate, TableDestination> cache = new HashMap<>();

    public DailyTableResolver(String project, String dataset, String prefix) {
        this.project = project;
        this.dataset = dataset;
        this.prefix = prefix;
    }

    @Override
    public TableDestination resolve(OrderEvent element, SinkWriter.Context context) {
        Long eventTime = context.timestamp();
        // Null when nothing assigned the record a timestamp — a processing-time job, or a source
        // with no timestamp assigner. Falling back to the record's own field keeps such a record
        // routed rather than unroutable.
        Instant instant =
                eventTime != null ? Instant.ofEpochMilli(eventTime) : element.createdAt();
        LocalDate day = instant.atZone(ZoneOffset.UTC).toLocalDate();
        return cache.computeIfAbsent(
                day,
                d ->
                        TableDestination.of(
                                project, dataset, prefix + "_" + d.format(SUFFIX)));
    }
}
```

```java
BigQuerySink.<OrderEvent>builder()
        .destinationResolver(new DailyTableResolver("my-project", "my_dataset", "orders"))
        .serializer(serializer)
        .build();
```

Two things to plan for with a resolver that keeps producing new destinations. Each one holds its
own stream writer, so `DefaultStreamOptions`' `destinationIdleTimeout` (one hour by default) is
what stops per-destination state from growing without bound in a long-lived job. And every new
table is created on its first record under the default create disposition, so
[table auto-creation](#table-auto-creation) applies to every day this produces, not only the first.

**`STORAGE_API_EXACTLY_ONCE` rejects `destinationResolver(...)`** at graph construction — that
write method takes one fixed destination. Dynamic destinations there are
[#76]({{< param BookRepo >}}/issues/76).

### Pub/Sub: a topic per record

```java
env.fromSource(source, WatermarkStrategy.noWatermarks(), "orders")
        .sinkTo(
                PubSubSink.<OrderEvent>builder()
                        .destinationResolver(
                                (element, context) ->
                                        TopicDestination.of("my-project", element.region()))
                        .serializer(
                                PubSubSerializationSchema.dataOnly(new OrderEventSchema())
                                        .withOrderingKey(OrderEvent::customerId))
                        .build());
```

A lambda is fine where the destination set is small: `TopicDestination` is pure identity, so the
allocation is a few fields. Cache as the BigQuery example does when the resolver is doing real work
to produce the name.

Each distinct topic gets its own SDK publisher, owned by the writer and closed with it. Ordering,
when enabled, is per key *within one topic* and holds per writer subtask — route same-key records
to the same subtask with `keyBy` for end-to-end order.

### Cloud Tasks: sharding across queues

```java
CloudTasksSink.<OrderEvent>builder()
        .destinationResolver(
                (element, context) ->
                        QueueDestination.of(
                                "my-project",
                                "asia-northeast1",
                                "webhooks-" + Math.floorMod(element.customerId().hashCode(), 4)))
        .serializer(
                CloudTasksSerializationSchema.httpTarget("https://api.example.com/v1/orders")
                        .withBody(new OrderEventSchema()))
        .build();
```

This one costs nothing: Cloud Tasks has no per-destination connection or stream, so a single client
serves every queue. Sharding across queues is how a pipeline exceeds the per-queue throughput
ceiling — the aggregate limits, and why they rarely matter for the workload this connector exists
for, are on the [Cloud Tasks connector]({{< relref "docs/connectors/datastream/cloudtasks" >}})
page. The queues must all exist; the sink creates none of them.

## Exactly-once

Two of BigQuery's three write methods are exactly-once, and they trade against each other rather
than one being better. Pub/Sub and Cloud Tasks are at-least-once with no exactly-once path — the
services have no transactional publish.

### Buffered streams

Rows are appended to a per-subtask Storage Write API buffered stream at explicit offsets, invisible
until a completed checkpoint makes exactly that checkpoint's rows visible.

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
// The mode must be explicit: AUTOMATIC is rejected at graph construction, because resolving to
// streaming without checkpointing would leave buffered rows invisible forever.
env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
env.enableCheckpointing(60_000);

env.fromSource(source, WatermarkStrategy.noWatermarks(), "orders")
        .sinkTo(
                BigQuerySink.<OrderEvent>builder()
                        .writeMethod(WriteMethod.STORAGE_API_EXACTLY_ONCE)
                        .destination(TableDestination.of("my-project", "my_dataset", "orders"))
                        .serializer(serializer)
                        .bufferedStreamOptions(BufferedStreamOptions.builder().build())
                        .build());

env.execute("bigquery-exactly-once");
```

`bufferedStreamOptions(...)` is required for this write method and rejected for the others, and
every knob in it is defaulted — `builder().build()` is how to say "the defaults" out loud. The
checkpoint interval is the visibility latency: rows land when the checkpoint that named them
completes.

### File loads

Rows are staged as Avro files on Cloud Storage and loaded with BigQuery load jobs, which is free of
streaming-insert cost and exactly-once in both execution modes.

```java
env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
// The checkpoint is the load trigger, and each one issues at least one load job per destination
// table against BigQuery's quota of 1,500 per table per day. 5 minutes is 288 of them; below
// 2 minutes the sink rejects the job outright.
env.enableCheckpointing(300_000);

env.fromSource(source, WatermarkStrategy.noWatermarks(), "orders")
        .sinkTo(
                BigQuerySink.<OrderEvent>builder()
                        .writeMethod(WriteMethod.FILE_LOADS)
                        .destinationResolver(
                                new DailyTableResolver("my-project", "my_dataset", "orders"))
                        .serializer(serializer)
                        .fileLoadsOptions(
                                FileLoadsOptions.builder()
                                        .stagingPath("gs://my-staging-bucket/flink-loads")
                                        .build())
                        .build());

env.execute("bigquery-file-loads");
```

Point `stagingPath` at a **dedicated bucket, separate from checkpoint and savepoint storage, with a
lifecycle rule** deleting objects after a few days, so files orphaned by a hard failure expire on
their own. Size the rule above the longest outage you intend to recover from: files a checkpoint
still references *are* the data, and a streaming job restored after the rule expired them leaves
its pending loads permanently failing.

Batch is the same builder with `RuntimeExecutionMode.BATCH` and no checkpointing — everything loads
at end of input.

### Redeploying an exactly-once job

**Never redeploy through discarded state.** The two-phase commit puts rows and source positions in
the same phase with no atomicity between them, so a writer restored with no state opens a loss
window of at most one checkpoint: rows appended but not flushed, and committables checkpointed but
not committed, stay invisible forever while the source may already have acked past them.

The sink cannot detect this — a writer restored with no state is indistinguishable from a new job —
so the guard belongs in deployment tooling:

```sh
flink stop --savepointPath gs://my-savepoints <job-id>
flink run -s gs://my-savepoints/savepoint-xxxx my-job.jar
```

With the Flink Kubernetes Operator that is `upgradeMode: savepoint` (or `last-state`), never
`stateless`. When state genuinely has to be dropped, rewind the source behind the last completed
checkpoint so a potential loss becomes a duplicate instead, and make duplicates harmless downstream
with an idempotent key plus `MERGE` or `QUALIFY ROW_NUMBER()`.

The at-least-once write method has the opposite profile — it keeps the sink strictly ahead of the
source, so discarding state can duplicate rows but cannot lose them. Neither method is uniformly
safer; the [BigQuery connector]({{< relref "docs/connectors/datastream/bigquery" >}}) page sets
their loss paths side by side.

## Table auto-creation

The default create disposition is `CREATE_IF_NEEDED`, so the first record for a missing table
creates it from the serializer's schema. `tableCreateOptions(...)` is what decides the rest of the
table's shape:

```java
BigQuerySink.<OrderEvent>builder()
        .destinationResolver(new DailyTableResolver("my-project", "my_dataset", "orders"))
        .serializer(serializer)
        .tableCreateOptions(
                TableCreateOptions.builder()
                        .timePartitioning(TimePartitioningType.DAY, "created_at")
                        .timePartitioningExpiration(Duration.ofDays(90))
                        .clusteredFields(List.of("customer_id"))
                        .build())
        .build();
```

**These apply at creation and never afterwards.** An existing table is never modified by them, so
adding partitioning to a running pipeline changes only the tables created from that point on. Use
`tableCreateOptionsProvider(...)` instead when the settings vary per destination — it receives the
`TableDestination` and returns the options for it.

Creation is idempotent across parallel subtasks (a 409 counts as success), so nothing needs
coordinating. The credentials need `bigquery.tables.create` on the dataset; `CREATE_NEVER` turns a
missing table into an immediate job failure instead, which is what to use when a missing table
means a routing bug.

Creation is also the **only** moment a `REQUIRED` column can appear — BigQuery cannot add one to an
existing table — so whichever column modes the serializer derives are decided here, durably.

## Topic and subscription auto-creation

### Topics, on the sink

The sink creates a missing topic reactively: a publish failing with `NOT_FOUND` parks its messages,
creates the topic, and republishes under a bounded backoff. An existing topic costs nothing — no
admin call is made unless a publish actually fails.

```java
PubSubSink.<OrderEvent>builder()
        .topic(TopicDestination.of("my-project", "orders"))
        .topicCreateOptions(
                TopicCreateOptions.builder()
                        // What makes messages published before a subscription exists reachable
                        // by one created later, or by a backwards seek.
                        .messageRetention(Duration.ofDays(7))
                        .build())
        .serializer(PubSubSerializationSchema.dataOnly(new OrderEventSchema()))
        .build();
```

**An auto-created topic starts with no subscriptions**, so without `messageRetention` the messages
published before one is attached are retained for nobody. That makes auto-creation without it suit
pipelines whose consumers create their own subscriptions, or attach them promptly. With dynamic
destinations one options object applies to *every* topic the sink creates.

Supplying the options is not what authorises creation here — the disposition is, because a topic
can meaningfully be created with defaults. Combining them with `CREATE_NEVER` is rejected at graph
construction rather than silently ignored.

### Subscriptions, on the source

The source is the other way round: **passing creation settings alongside a subscription is what
authorises creating it.** There is no disposition, because there is no meaningful "create with
defaults" — a subscription without a topic is not a subscription, and only you know which topic to
bind.

```java
PubSubSource.<OrderEvent>builder()
        .subscription(
                SubscriptionDestination.of("my-project", "orders-sub"),
                SubscriptionCreateOptions.builder()
                        .topic(TopicDestination.of("my-project", "orders"))
                        .ackDeadline(Duration.ofSeconds(60))
                        .build())
        // No options: this one must already exist, and the startup check says so if it does not.
        .subscription(SubscriptionDestination.of("my-project", "returns-sub"))
        .deserializationSchema(new OrderEventDeserializationSchema())
        .build();
```

**The settings are per subscription because they carry the topic binding.** One options object
shared across several would bind them all to one topic, and Pub/Sub delivers a complete copy of a
topic's stream to every subscription of it — so the source would emit each message once per
subscription, with nothing anywhere reporting an error.

A subscription only retains messages published **after** it exists, so a job that auto-creates one
starts from an empty backlog whatever was published before.

## Running against an emulator

**An emulator is a convenience for fast feedback, never evidence about the service's behaviour.**
Where the two disagree the real service decides, and every emulator below has blind spots that
matter — a green emulator run is not a green integration.

### Pub/Sub

```sh
gcloud beta emulators pubsub start --project=my-project --host-port=localhost:8085
```

```java
PubSubSink.<String>builder()
        .topic(TopicDestination.of("my-project", "orders"))
        .serializer(PubSubSerializationSchema.dataOnly(new SimpleStringSchema()))
        .emulatorEndpoint("localhost:8085")
        .build();
```

`emulatorEndpoint(...)` exists on the source too, and on both it opens a **plaintext channel with
no credentials** — so it must only ever point at an emulator, never at production Pub/Sub. Note the
source deliberately does *not* honour the `PUBSUB_EMULATOR_HOST` environment variable, unlike the
Apache connector: a stray value on a task manager would silently redirect a production job.

What the emulator cannot show: ordered delivery (per-key callback serialization in the client
library is gated on a subscription property the emulator does not set, so callbacks arrive out of
order with no Flink involved), ordered seek, dead-letter forwarding, IAM, and the *effect* of every
create-option it stores and ignores — a KMS key that does not exist is accepted.

### Cloud Tasks

Google publishes no Cloud Tasks emulator; the one the integration tests use is
[`aertje/cloud-tasks-emulator`](https://github.com/aertje/cloud-tasks-emulator) (MIT).

```sh
docker run --rm -p 8123:8123 ghcr.io/aertje/cloud-tasks-emulator:1.2.0 \
    -host 0.0.0.0 -port 8123 \
    -queue projects/my-project/locations/asia-northeast1/queues/webhooks
```

```java
CloudTasksSink.<String>builder()
        .queue(QueueDestination.of("my-project", "asia-northeast1", "webhooks"))
        .serializer(
                CloudTasksSerializationSchema.httpTarget("http://localhost:9000/orders")
                        .withBody(new SimpleStringSchema()))
        .emulatorEndpoint("localhost:8123")
        .build();
```

It dispatches over real HTTP, so a local server sees what the tasks actually carry. What it cannot
show: task-name garbage collection (so the deduplication *window* is untestable, only the
`ALREADY_EXISTS` response), queue-level `uriOverride` routing, the OAuth token path (it implements
OIDC only), failure injection, and any size limit.

### BigQuery

**There is no `emulatorEndpoint(...)` on the BigQuery sink**, and that is a decision rather than a
gap waiting to be filled. The module's own tests reach
[goccy/bigquery-emulator](https://github.com/goccy/bigquery-emulator) through a test-only appender
factory handed to a `@VisibleForTesting` overload, so the production factory never needed an
endpoint seam; adding one to the public API was considered under
[#54]({{< param BookRepo >}}/issues/54) and left unbuilt for want of a consumer. It would slot into
the production factory's constructor state cheaply — open an issue if you want it.

Develop against a real dataset meanwhile; a sandbox project with a short default table expiration
keeps it cheap. That is also less of a loss than it sounds, because of how much such a run could
never prove: the emulator supports neither `gs://` load jobs nor a Cloud Storage endpoint, so
`FILE_LOADS` could not run against it at all, and it reads `TIME`, `DATETIME`, `NUMERIC` and
`BIGNUMERIC` columns back as unrelated values.
