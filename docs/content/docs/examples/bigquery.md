---
title: BigQuery
type: docs
weight: 10
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

# BigQuery examples

Starting from the [BigQuery quickstart]({{< relref "docs/quickstart/bigquery" >}}) job.

## A table per day

The writer context carries the record's event timestamp, which is what makes time-based routing
expressible without the record having to carry the routing key itself. The
[resolver contract]({{< relref "docs/examples" >}}#dynamic-per-record-destinations-share-one-shape)
is what the caching here is for.

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

## Exactly-once

Two of BigQuery's three write methods are exactly-once, and they trade against each other rather
than one being better. (Pub/Sub and Cloud Tasks are at-least-once with no exactly-once path — those
services have no transactional publish.)

Both need streaming checkpointing in `CheckpointingMode.EXACTLY_ONCE`, which is Flink's default and
so needs no line in either job below — but a cluster setting `execution.checkpointing.mode` to
`AT_LEAST_ONCE` has the job rejected when the graph is built, rather than silently downgraded.

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
coordinating — and a subtask the per-table quota rate-limits instead of answering 409 retries the
creation within the recovery budget, so a wide parallelism costs a backoff rather than the job
(see [Losing the creation race]({{< relref "/docs/connectors/datastream/bigquery" >}}#losing-the-creation-race-costs-a-retry-not-the-job)).
The credentials need `bigquery.tables.create` on the dataset; `CREATE_NEVER` turns a
missing table into an immediate job failure instead, which is what to use when a missing table
means a routing bug.

Creation is also the **only** moment a `REQUIRED` column can appear — BigQuery cannot add one to an
existing table — so whichever column modes the serializer derives are decided here, durably.

## No emulator path

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
