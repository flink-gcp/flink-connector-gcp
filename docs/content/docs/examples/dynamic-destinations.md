---
title: Dynamic destinations
type: docs
weight: 5
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

# Dynamic destinations

One sink instance can route records to many tables, topics or queues without splitting the stream into one sink per destination.
BigQuery, Cloud Pub/Sub, Cloud Tasks and Bigtable express that choice through a `destinationResolver`, while Spanner takes the table from each serialized `Mutation`.

The examples below start from each connector's [Quickstart]({{< relref "docs/quickstart" >}}) job and replace its fixed destination with record-driven routing.
The connector example pages carry the surrounding job and the detailed options.

## The resolver contract

The four resolver-based sinks call their resolver once per record on the writer's hot path and before they invoke the serializer.
The resolver must therefore be serializable, deterministic and cheap.
The portable contract across all four sinks is that every resolver returns a non-null result, including when the serializer later skips the record by returning `null`.
Use the serializer's `null` result, not a resolver's `null` result, to skip a record.
BigQuery additionally accepts an explicit `UnroutableRecord` result for a deterministic, record-specific routing failure.
Its configured failure handler then decides whether to fail, drop or dead-letter that record; a bare resolver `null` or an unexpected resolver exception remains fatal.

The writer context exposes the record's event timestamp and the current watermark.
Time-based routing should use that timestamp or a field carried by the record rather than reading the task manager's wall clock, because a replay must make the same routing decision as the original attempt.

Destination values use their resource names for `equals` and `hashCode`, so separately constructed but equal values reach the same writer state.
Caching still avoids rebuilding a destination object for every record, and it becomes more valuable when deriving the resource name does real work.
A cache captured by a resolver travels in the job graph and is copied per sink subtask, so every captured object must be serializable.
Writer idle eviction does not remove entries from a cache owned by the resolver.
Keep that cache bounded, or construct equal destination values on demand when the destination set is unbounded.

## Resource lifetimes

Dynamic routing changes how many resources one writer subtask owns.
The resolver cache holds names, while the sink creates the client-side resource in the table below lazily when a serialized record first reaches that destination.

| Sink | Per-record route | Per-destination client-side state | Idle eviction | Auto-creation |
|---|---|---|---|---|
| BigQuery | `TableDestination` from `destinationResolver` | A Storage Write API writer on the streaming methods; conversion state and the current staging file on FILE_LOADS | Yes; FILE_LOADS also applies an active-destination capacity | Supported according to the create disposition |
| Cloud Pub/Sub | `TopicDestination` from `destinationResolver` | One SDK publisher per topic | No; publishers close with the writer | Supported according to the create disposition |
| Cloud Tasks | `QueueDestination` from `destinationResolver` | None; one client serves every queue | Not needed | Not supported |
| Bigtable | `TableDestination` from `destinationResolver` | One bulk mutation batcher per table; data clients are shared by project and instance | Yes | Opt-in with a declared table schema |
| Spanner | Table name in the serialized `Mutation` | None per table; one database client and one shared mutation batch | Not needed | Not supported |

Parallel sink subtasks own these resources independently.
For example, four subtasks that each see ten Pub/Sub topics can hold forty publishers even though every subtask uses the same resolver code.

The table does not include optional per-destination metrics.
When enabled on BigQuery's default-stream or FILE_LOADS method, Pub/Sub, Cloud Tasks or Bigtable, Flink cannot unregister those counters, so each seen destination keeps metric registry state for the task lifetime even after writer-state eviction.

BigQuery's default and buffered stream writers drop drained destination state after the configured idle period at the end of a successful non-end-of-input flush.
FILE_LOADS finishes an idle destination file after its configured timeout, finishes the least recently used file before exceeding its active-destination capacity, and finishes every remaining file at a checkpoint.

Bigtable sweeps idle table batchers after a successful non-end-of-input flush and rebuilds one transparently if the table becomes active again.
Pub/Sub deliberately keeps each topic publisher until the writer closes, while Cloud Tasks and Spanner have no per-routed-destination service client state to evict.

## BigQuery tables

This resolver caches one `TableDestination` per event day and returns `UnroutableRecord` when the record does not identify a known tenant.
The same failure policy applies under every BigQuery write method.

{{< java-snippet file="DynamicDestinationsBigQueryTables.java" tag="bigquery-tables" >}}

The default-stream and buffered-stream methods keep independent writer state per active table.
FILE_LOADS stages and commits files per table instead, so its load-job cadence and table-modification quotas replace the streaming methods' connection cost.
An `UnroutableRecord` creates none of that per-table state and never reaches the serializer.
The resolver supplies its dead-letter payload because no destination schema exists for the sink to serialize against; with the Pub/Sub queue shown above it is published with `dlq-destination=unresolved`.
The default policy is `FailureHandler.failJob()`, so omitting `failureHandler(...)` remains conservative.
The [BigQuery examples]({{< relref "docs/examples/bigquery" >}}#a-table-per-day) show event-time fallback and all three write methods.

## Pub/Sub topics

Pub/Sub adds an ordering boundary to dynamic routing because an ordering key is ordered only within one topic and one publisher stream.
The job must route every record for one customer to the same topic and the same sink subtask when it requires end-to-end per-customer order.

{{< java-snippet file="DynamicDestinationsPubSubTopics.java" tag="pubsub-topics" >}}

`keyBy` keeps one ordering key on one sink subtask, while the resolver must keep that key on one topic.
If a customer's records resolve to different topics, Pub/Sub preserves a separate sequence inside each topic rather than one sequence across them.
The [Pub/Sub examples]({{< relref "docs/examples/pubsub" >}}#a-topic-per-record) continue with topic creation and emulator limits.

## Cloud Tasks queues

Cloud Tasks can shard work across queues without creating a service client, publisher, stream or batcher per queue.
This example uses four queues to spread dispatch capacity while keeping the same customer on the same queue.

{{< java-snippet file="DynamicDestinationsCloudTasksQueues.java" tag="cloud-tasks-queues" >}}

Every queue must already exist because this sink does not create queues.
The [Cloud Tasks examples]({{< relref "docs/examples/cloudtasks" >}}#sharding-across-queues) explain the per-queue throughput reason for this routing shape.

## Bigtable tables

Bigtable places one batcher behind each active table, so the resolver's cardinality is also a resource decision.
This example shortens the idle lifetime because a date-suffixed table stops receiving records when its day ends.

{{< java-snippet file="DynamicDestinationsBigtableTables.java" tag="bigtable-tables" >}}

If the sink may create tables, one `TableCreateOptions` declaration applies to every table the resolver can name.
An unbounded routing key can therefore create an unbounded table set, so auto-creation must be chosen for the generated set rather than for the first table alone.
The [Bigtable examples]({{< relref "docs/examples/bigtable" >}}#a-table-per-day-from-the-record) cover the resolver's serialization and table-creation requirements.

## Spanner tables

Spanner fixes the sink to one database and takes the table name from the `Mutation` returned by the serializer.
There is no resolver SPI because a second table-routing decision could disagree with the mutation that the Spanner client actually writes.

{{< java-snippet file="DynamicDestinationsSpannerTables.java" tag="spanner-tables" >}}

The writer batches mutations from several tables together and reads cell weights for the whole database when it opens.
Every table must already exist, and the serializer must preserve the sink's at-least-once idempotence requirements for each mutation.
The [Spanner examples]({{< relref "docs/examples/spanner" >}}#writing-to-several-tables-from-one-sink) cover deletes, skipped records and failure routing beside the same table-selection mechanism.

## Creating generated destinations

Auto-creation applies to the complete set the resolver can produce, not only to the destination present when the job starts.
The configuration attached to the sink is reused whenever a generated destination is missing.

- **BigQuery tables**: the create disposition decides whether a missing table is created, and the serializer supplies the destination's schema.
- **Pub/Sub topics**: the create disposition decides whether a missing topic is created, and one `TopicCreateOptions` value applies to every created topic.
- **Bigtable tables**: creation is opt-in and requires one `TableCreateOptions` schema whose declared column families apply to every created table.
- **Cloud Tasks queues and Spanner tables**: the sinks do not create them, so every generated name must exist before records are routed to it.

A resolver over an unconstrained tenant ID can turn one configuration mistake into one resource per input value.
Bound the destination set before enabling creation, and use each connector page's creation section to choose the required service settings and permissions.
