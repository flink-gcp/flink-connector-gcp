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

{{< java-snippet file="BigtableExamplesSeveralCellsAndDelete.java" tag="bigtable-examples-several-cells-and-delete" >}}

## A table per day, from the record

The [dynamic destinations guide]({{< relref "docs/examples/dynamic-destinations" >}}#bigtable-tables) defines the shared resolver contract and compares the batcher's lifetime with the other sinks.
The resolver names the table, while the serializer still builds the whole mutation.
The resolver runs once per record, so this map avoids reconstructing equal `TableDestination` values for repeated days.
Equality rather than object identity keys the writer's batcher pool, so the cache is an allocation optimization rather than a correctness requirement.

{{< java-snippet file="BigtableExamplesTablePerDay.java" tag="bigtable-examples-table-per-day" >}}

The map is captured by the resolver's closure, so it has to reach the task manager: a `HashMap` built where the job is assembled travels fine, while an instance field of a class that is not serializable does not.
Idle eviction closes the writer's batcher but does not remove entries from this resolver-owned map, so the example retains one entry per observed day.
Every table the resolver can name must already exist unless the sink is opted into [auto-creation]({{< relref "docs/connectors/datastream/bigtable" >}}#table-auto-creation).
Read that section beside a resolver because one schema serves every table the sink creates, and a resolver keyed on something unbounded creates one table per value.

## Skipping records instead of filtering upstream

Returning `null` writes nothing and is not a failure, so a filter whose condition is only known
while building the mutation belongs here:

The following lambda omits the enclosing `serializer(...)` setter and sink builder.

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

{{< java-snippet file="BigtableExamplesDroppingBadRows.java" tag="bigtable-examples-dropping-bad-rows" >}}

A dead-letter destination is the same setter with
`FailureHandler.sendToDeadLetterQueue(...)`. What arrives there is the serialized
`MutateRowsRequest.Entry`, so a consumer replays the whole row mutation rather than reconstructing
it from a row key.

## Bounding memory on large mutations

The default byte bound is 64 MiB of unacknowledged entries. A pipeline whose rows are large — or
one running many subtasks per TaskManager — sets it explicitly, and lowering the batch element count
shortens the delay before a mutation reaches the service at low volume:

{{< java-snippet file="BigtableExamplesBoundingMemory.java" tag="bigtable-examples-bounding-memory" >}}

Raising `maxInFlightEntries` well past its default is the one direction that does not help: the
client's own flow controller then becomes the binding limit, and it blocks the task thread rather
than yielding to the mailbox — see
[Tuning]({{< relref "docs/connectors/datastream/bigtable" >}}#tuning).

## Reading a key range

A prefix and an explicit range are the same thing said two ways, and both are repeatable:

{{< java-snippet file="BigtableExamplesKeyRange.java" tag="bigtable-examples-key-range" >}}

What a checkpoint carries is the range that is **left**: after emitting the row `2026-08-14#9`, the
split covers `(2026-08-14#9, 2026-09-)`. That is what makes a restore resume rather than replay, and
it is why the source needs no offset of its own.

## Filtering on the server

What a filter excludes never leaves Bigtable, so it is the cheapest thing a scan can carry — and it
is where every per-cell decision belongs, since the source has no separate knobs for families,
qualifiers, timestamps or versions:

{{< java-snippet file="BigtableExamplesServerFilter.java" tag="bigtable-examples-server-filter" >}}

## Reading through an application profile

{{< java-snippet file="BigtableExamplesApplicationProfile.java" tag="bigtable-examples-application-profile" >}}

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

{{< java-snippet file="BigtableExamplesEmulatorSink.java" tag="bigtable-examples-emulator-sink" >}}

The source reaches it the same way:

{{< java-snippet file="BigtableExamplesEmulatorSource.java" tag="bigtable-examples-emulator-source" >}}

The project and instance ids are opaque path segments to the emulator; neither has to exist. It
implements `MutateRows`, `ReadRows` and the table admin surface, which is enough to develop against
— but it validates far less than the service does, so a mutation it accepts is not evidence that
Bigtable would.

One read-path difference is worth knowing while developing: the emulator models no tablets, so it
offers almost no split boundaries and a job against it runs on **one** split whatever the
parallelism. Reading in parallel is something only real Bigtable shows.
