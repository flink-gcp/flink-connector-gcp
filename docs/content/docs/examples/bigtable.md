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

The default byte bound is 64 MiB of unacknowledged mutations. A pipeline whose rows are large — or
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

Raising `maxInFlightMutations` well past its default is the one direction that does not help: the
client's own flow controller then becomes the binding limit, and it blocks the task thread rather
than yielding to the mailbox — see
[Tuning]({{< relref "docs/connectors/datastream/bigtable" >}}#tuning).

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

The project and instance ids are opaque path segments to the emulator; neither has to exist. It
implements `MutateRows` and the table admin surface, which is enough to develop against — but it
validates far less than the service does, so a mutation it accepts is not evidence that Bigtable
would.
