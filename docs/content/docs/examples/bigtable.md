---
title: Bigtable
type: docs
weight: 40
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

# Bigtable examples

Starting from the [Bigtable quickstart]({{< relref "docs/quickstart/bigtable" >}}) job.

## Several cells, and a delete, per record

One `RowMutationEntry` may carry any number of mutations, and they apply to the row atomically —
which is the one atomicity guarantee Bigtable offers, and the reason a record that updates several
columns should build one entry rather than be split upstream:

```java
BigtableSink.<OrderEvent>builder()
        .table(TableDestination.of("my-project", "my-instance", "orders"))
        .serializer(
                (event, context) -> {
                    long timestampMicros = event.updatedAtMillis() * 1_000;
                    RowMutationEntry entry = RowMutationEntry.create("order#" + event.id());
                    entry.setCell("cf", "status", timestampMicros, event.status());
                    entry.setCell("cf", "total", timestampMicros, event.totalCents());
                    if (event.isCancelled()) {
                        // Applied together with the two cells above, in one atomic mutation.
                        entry.deleteCells("cf", "reserved_stock");
                    }
                    return entry;
                })
        .build();
```

## A table per day, from the record

The [dynamic destinations guide]({{< relref "docs/examples/dynamic-destinations" >}}#bigtable-tables) defines the shared resolver contract and compares the batcher's lifetime with the other sinks.
The resolver names the table, while the serializer still builds the whole mutation.
The resolver runs once per record, so this map avoids reconstructing equal `TableDestination` values for repeated days.
Equality rather than object identity keys the writer's batcher pool, so the cache is an allocation optimization rather than a correctness requirement.

```java
Map<LocalDate, TableDestination> byDay = new HashMap<>();

BigtableSink.<OrderEvent>builder()
        .destinationResolver(
                (event, context) ->
                        byDay.computeIfAbsent(
                                event.day(),
                                day ->
                                        TableDestination.of(
                                                "my-project", "my-instance", "orders-" + day)))
        .serializer(
                (event, context) ->
                        RowMutationEntry.create(event.id())
                                .setCell("cf", "payload", event.timestampMicros(), event.body()))
        // A day's table stops receiving records once the day rolls over, and its batcher goes with
        // it after this long. One hour is the default; this job knows its tables turn over faster.
        .writerOptions(
                BigtableWriterOptions.builder()
                        .destinationIdleTimeout(Duration.ofMinutes(15))
                        .build())
        .build();
```

The map is captured by the resolver's closure, so it has to reach the task manager: a `HashMap` built where the job is assembled travels fine, while an instance field of a class that is not serializable does not.
Idle eviction closes the writer's batcher but does not remove entries from this resolver-owned map, so the example retains one entry per observed day.
Every table the resolver can name must already exist unless the sink is opted into [auto-creation]({{< relref "docs/connectors/datastream/bigtable" >}}#table-auto-creation).
Read that section beside a resolver because one schema serves every table the sink creates, and a resolver keyed on something unbounded creates one table per value.

## Skipping records instead of filtering upstream

Returning `null` writes nothing and is not a failure, so a filter whose condition is only known
while building the mutation belongs here:

```java
(event, context) ->
        event.isHeartbeat()
                ? null
                : RowMutationEntry.create("device#" + event.deviceId())
                        .setCell("cf", "reading", event.timestampMicros(), event.value());
```

## Dropping bad rows instead of failing the job

Only two failures are droppable — a record the serializer rejects, and a mutation the service
rejects as invalid. Everything else, an outage or a missing column family included, still fails the
job; the [Bigtable connector]({{< relref "docs/connectors/datastream/bigtable" >}}#error-handling)
page sets out why that line is where it is.

```java
BigtableSink.<OrderEvent>builder()
        .table(TableDestination.of("my-project", "my-instance", "orders"))
        .serializer(new OrderEventMutations())
        .failedMutationHandler(FailureHandler.logAndDrop())
        .build();
```

A dead-letter destination is the same setter with
`FailureHandler.sendToDeadLetterQueue(...)`. What arrives there is the serialized
`MutateRowsRequest.Entry`, so a consumer replays the whole row mutation rather than reconstructing
it from a row key.

## Bounding memory on large mutations

The default byte bound is 64 MiB of unacknowledged entries. A pipeline whose rows are large — or
one running many subtasks per TaskManager — sets it explicitly, and lowering the batch element count
shortens the delay before a mutation reaches the service at low volume:

```java
BigtableSink.<OrderEvent>builder()
        .table(TableDestination.of("my-project", "my-instance", "orders"))
        .serializer(new OrderEventMutations())
        .writerOptions(
                BigtableWriterOptions.builder()
                        .maxInFlightBytes(16L * 1024 * 1024)
                        .batchElementCount(50)
                        .build())
        .build();
```

Raising `maxInFlightEntries` well past its default is the one direction that does not help: the
client's own flow controller then becomes the binding limit, and it blocks the task thread rather
than yielding to the mailbox — see
[Tuning]({{< relref "docs/connectors/datastream/bigtable" >}}#tuning).

## Reading a key range

A prefix and an explicit range are the same thing said two ways, and both are repeatable:

```java
BigtableSource.<Order>builder()
        .table(TableDestination.of("my-project", "my-instance", "orders"))
        .deserializer(new OrderRows())
        // Everything under one prefix, plus one range named outright. Overlapping ranges are
        // merged rather than rejected, so nested prefixes cost nothing but are not read twice.
        .prefix("2026-08-")
        .rowRange("archive#2025-", "archive#2026-")
        .build();
```

What a checkpoint carries is the range that is **left**: after emitting the row `2026-08-14#9`, the
split covers `(2026-08-14#9, 2026-09-)`. That is what makes a restore resume rather than replay, and
it is why the source needs no offset of its own.

## Filtering on the server

What a filter excludes never leaves Bigtable, so it is the cheapest thing a scan can carry — and it
is where every per-cell decision belongs, since the source has no separate knobs for families,
qualifiers, timestamps or versions:

```java
BigtableSource.<Order>builder()
        .table(TableDestination.of("my-project", "my-instance", "orders"))
        .deserializer(new OrderRows())
        .filter(
                Filters.FILTERS.chain()
                        .filter(Filters.FILTERS.family().exactMatch("cf"))
                        .filter(Filters.FILTERS.qualifier().exactMatch("payload"))
                        // The latest version of each cell only.
                        .filter(Filters.FILTERS.limit().cellsPerColumn(1)))
        .build();
```

## Reading through an application profile

```java
BigtableSource.<Order>builder()
        .table(TableDestination.of("my-project", "my-instance", "orders"))
        .deserializer(new OrderRows())
        .appProfileId("analytics")
        .build();
```

A [Data Boost]({{< relref "docs/connectors/datastream/bigtable" >}}#serverless-reads-with-data-boost)
profile is named here like any other. Two caveats come with one: its reads can be up to about 35
minutes stale, so a job that writes with the sink and reads back through it may not see its own
recent writes; and it is read-only, so the same profile on the sink breaks writes. This project has
not exercised Data Boost — [#248]({{< param BookRepo >}}/issues/248) is the verification.

## Running against the emulator

Google's Bigtable emulator ships with the Cloud SDK, and the sink reaches it over a plaintext
channel with no credentials:

```sh
gcloud beta emulators bigtable start --host-port=localhost:8086
```

```sh
# The admin surface works too, so the table can be created against the emulator.
BIGTABLE_EMULATOR_HOST=localhost:8086 \
    cbt -project my-project -instance my-instance createtable orders families=cf
```

```java
BigtableSink.<OrderEvent>builder()
        .table(TableDestination.of("my-project", "my-instance", "orders"))
        .serializer(new OrderEventMutations())
        .emulatorEndpoint("localhost:8086")
        .build();
```

The source reaches it the same way:

```java
BigtableSource.<Order>builder()
        .table(TableDestination.of("my-project", "my-instance", "orders"))
        .deserializer(new OrderRows())
        .emulatorEndpoint("localhost:8086")
        .build();
```

The project and instance ids are opaque path segments to the emulator; neither has to exist. It
implements `MutateRows`, `ReadRows` and the table admin surface, which is enough to develop against
— but it validates far less than the service does, so a mutation it accepts is not evidence that
Bigtable would.

One read-path difference is worth knowing while developing: the emulator models no tablets, so it
offers almost no split boundaries and a job against it runs on **one** split whatever the
parallelism. Reading in parallel is something only real Bigtable shows.
