---
title: Bigtable
type: docs
weight: 40
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

# Bigtable examples

The [Bigtable quickstart]({{< relref "docs/quickstart/bigtable" >}}) owns the basic source and sink jobs.
The cases below change one direction or compose it with another connector.

## DataStream source

The [Quickstart read job]({{< relref "docs/quickstart/bigtable" >}}#read-a-table-back) is the canonical bounded source example.
The worked cases below narrow its rows, filter its cells, or route it through another application profile.

### Reading a key range

A prefix and an explicit range are the same thing said two ways, and both are repeatable:

{{< java-snippet file="BigtableExamplesKeyRange.java" tag="bigtable-examples-key-range" >}}

What a checkpoint carries is the range that is **left**: after emitting the row `2026-08-14#9`, the
split covers `(2026-08-14#9, 2026-09-)`.
That is what makes a restore resume rather than replay, and it is why the source needs no offset of its own.

### Filtering on the server

What a filter excludes never leaves Bigtable, so it is the cheapest thing a scan can carry.
Every per-cell decision belongs in that filter because the source has no separate knobs for families, qualifiers, timestamps, or versions:

{{< java-snippet file="BigtableExamplesServerFilter.java" tag="bigtable-examples-server-filter" >}}

### Reading through an application profile

{{< java-snippet file="BigtableExamplesApplicationProfile.java" tag="bigtable-examples-application-profile" >}}

A [Data Boost]({{< relref "docs/connectors/datastream/bigtable" >}}#serverless-reads-with-data-boost)
profile is named here like any other.
Its reads can be up to about 35 minutes stale, so a job that writes with the sink and reads back through it may not see its own recent writes.
The profile is read-only, so using it on the sink breaks writes.
This project has not exercised Data Boost; [#248]({{< param BookRepo >}}/issues/248) owns that verification.

## DataStream sink

The [Quickstart write job]({{< relref "docs/quickstart/bigtable" >}}#write-a-stream-of-row-mutations) is the canonical fixed-table sink example.
The worked cases below change the mutation, destination, rejection policy, or memory bounds.

### Several cells, and a delete, per record

One `RowMutationEntry` may carry any number of mutations, and they apply to the row atomically —
which is the one atomicity guarantee Bigtable offers, and the reason a record that updates several
columns should build one entry rather than be split upstream:

{{< java-snippet file="BigtableExamplesSeveralCellsAndDelete.java" tag="bigtable-examples-several-cells-and-delete" >}}

### A table per day, from the record

The [dynamic destinations guide]({{< relref "docs/examples/dynamic-destinations" >}}#bigtable-tables) defines the shared resolver contract and compares the batcher's lifetime with the other sinks.
The resolver names the table, while the serializer still builds the whole mutation.
The resolver runs once per record, so this map avoids reconstructing equal `TableDestination` values for repeated days.
Equality rather than object identity keys the writer's batcher pool, so the cache is an allocation optimization rather than a correctness requirement.

{{< java-snippet file="BigtableExamplesTablePerDay.java" tag="bigtable-examples-table-per-day" >}}

The map is captured by the resolver's closure, so it has to reach the task manager: a `HashMap` built where the job is assembled travels fine, while an instance field of a class that is not serializable does not.
Idle eviction closes the writer's batcher but does not remove entries from this resolver-owned map, so the example retains one entry per observed day.
Every table the resolver can name must already exist unless the sink is opted into [auto-creation]({{< relref "docs/connectors/datastream/bigtable" >}}#table-auto-creation).
Read that section beside a resolver because one schema serves every table the sink creates, and a resolver keyed on something unbounded creates one table per value.

### Skipping records instead of filtering upstream

Returning `null` writes nothing and is not a failure, so a filter whose condition is only known
while building the mutation belongs in the serializer:

{{< java-snippet file="BigtableExamplesSkippingRecords.java" tag="bigtable-examples-skipping-records" >}}

### Dropping bad rows instead of failing the job

Only two failures are droppable — a record the serializer rejects, and a mutation the service
rejects as invalid. Everything else, an outage or a missing column family included, still fails the
job; the [Bigtable connector]({{< relref "docs/connectors/datastream/bigtable" >}}#error-handling)
page sets out why that line is where it is.

{{< java-snippet file="BigtableExamplesDroppingBadRows.java" tag="bigtable-examples-dropping-bad-rows" >}}

A dead-letter destination is the same setter with
`FailureHandler.sendToDeadLetterQueue(...)`. What arrives there is the serialized
`MutateRowsRequest.Entry`, so a consumer replays the whole row mutation rather than reconstructing
it from a row key.

### Bounding memory on large mutations

The default byte bound is 64 MiB of unacknowledged entries. A pipeline whose rows are large — or
one running many subtasks per TaskManager — sets it explicitly, and lowering the batch element count
shortens the delay before a mutation reaches the service at low volume:

{{< java-snippet file="BigtableExamplesBoundingMemory.java" tag="bigtable-examples-bounding-memory" >}}

Raising `maxInFlightEntries` well past its default is the one direction that does not help: the
client's own flow controller then becomes the binding limit, and it blocks the task thread rather
than yielding to the mailbox — see
[Tuning]({{< relref "docs/connectors/datastream/bigtable" >}}#tuning).

## Table source

### Scanning a Bigtable table with SQL

A bounded scan treats the row key as one column and every selected column family as a nested row.
Only the families selected by the query leave Bigtable.
The quickstart creates the instance but not this table, so create the physical table and its families first:

```sh
cbt -project my-project -instance my-instance createtable profiles families=profile,usage,state
```

{{< sql-snippet file="flink/BigtableExamples.sql" tag="batch-source" >}}

The row-key bounds are pushed into the scan.
The family projection is pushed down separately.
Selecting any field from `profile` reads that whole family; the connector discards undeclared or unselected qualifiers after the row arrives.

## Table sink

### Writing a bounded aggregate as upserts

Bigtable writes are row-key upserts by construction.
Declaring `rowkey` as the primary key tells the planner that the query and destination use the same upsert key.
For a bounded aggregation, the planner can emit one final total per key and the sink overwrites the cells for that row key.

Create the destination table before submitting the statement:

```sh
cbt -project my-project -instance my-instance createtable profile-totals families=stats
```

{{< sql-snippet file="flink/BigtableExamples.sql" tag="batch-upsert" >}}

The aggregate's upsert key is `user_id`, which maps directly to the sink's `rowkey`, so Flink 2.3 can plan this statement without an `ON CONFLICT` clause.
Flink 2.3 requires an `ON CONFLICT DO DEDUPLICATE`, `DO ERROR`, or `DO NOTHING` strategy when the query key differs from the sink key or the planner cannot infer one.

The default `sink.insert-only-input-mode = upsert` exposes those conflict strategies for an insert-only input too.
An updating input remains upsert-shaped under either option value.

The writer does not order two entries for the same row key when they share a bulk-mutation batch, so a streaming aggregation that emits repeated updates for one key has no latest-input-value guarantee.
Use this bounded form only when the aggregate emits at most one final mutation per key, or separate dependent writes so one completes before the next begins.

### Keeping a plain insert portable

The table-local `insert-only` mode narrows an input containing inserts alone to an insert-only planner contract:

{{< sql-snippet file="flink/BigtableExamples.sql" tag="insert-only-sink" >}}

This mode keeps the clause-less statement portable across Flink 1.20, 2.2, and 2.3.
It does not make Bigtable insert-if-absent because a physical write still overwrites cells under an existing row key.
The option does not narrow an updating query, and the planner continues to request upserts and deletes for one.

### Writing a stable cell timestamp

Writable `timestamp` metadata applies one `TIMESTAMP_LTZ(6)` value to every cell the row writes:

{{< sql-snippet file="flink/BigtableExamples.sql" tag="cell-timestamp-sink" >}}

The explicit timestamp makes a replay address the same cell version after Flink serializes the record again.
The value above already has millisecond granularity, which Bigtable accepts without enabling `sink.cell-timestamp.truncate-to-millis`.
A missing or null metadata value leaves the client to use the TaskManager's wall clock instead.

## Lookup joins

### Joining a Bigtable lookup table

A processing-time temporal join turns each input row into a Bigtable point read.
This example reuses the `profiles` table above and adds a generated facts table with a processing-time column:

{{< sql-snippet file="flink/BigtableExamples.sql" tag="lookup-join" >}}

The equality condition must cover the single Bigtable row-key column.
Bigtable has one atomic row key, so the connector rejects composite lookup keys and nested family fields as lookup keys.
The default is a synchronous point read without a cache.

### Enriching Pub/Sub events before creating tasks

This pipeline treats Bigtable as a low-latency attribute store keyed by the user ID.
It consumes JSON events from Pub/Sub, reads the matching `profile` family, and creates a Cloud Tasks request for an external API:

{{< sql-snippet file="flink/BigtableExamples.sql" tag="attribute-enrichment-pipeline" >}}

The inner join creates no task when the lookup finds no row or when `scan.row-prefix` excludes the user ID.
The selected `profile` fields make that family the projection; an unselected family does not leave Bigtable.
The example uses asynchronous PARTIAL caching, so cache misses start non-blocking point reads and results are retained under the configured row and expiry bounds.

The lookup choices change this pipeline at distinct boundaries:

| Choice | Effect on the enrichment |
|---|---|
| `lookup.async = false` | Each lookup call is synchronous; this is the default and can be combined with NONE, PARTIAL, or FULL caching |
| `lookup.async = true` | Point reads may overlap; FULL caching rejects this setting because a FULL lookup is synchronous |
| `lookup.cache = NONE` | Every event performs a point read |
| `lookup.cache = PARTIAL` | Entries are loaded on demand and retained under at least one configured cache bound |
| `lookup.cache = FULL` | Each lookup task loads all projected rows in the configured key bounds and refreshes them periodically or at a configured time |
| `scan.row-prefix` or `scan.row-ranges` | The same bounds restrict point reads and the rows loaded by FULL caching |

The [Table connector lookup section]({{< relref "docs/connectors/table/bigtable" >}}#lookup-joins) owns the complete cache and retry behavior.
Pub/Sub source acknowledgements and Cloud Tasks creation remain independently at-least-once; the lookup join does not strengthen either endpoint's delivery guarantee.

## Change Streams

Before either example runs, enable Change Streams on the physical `profiles` table and create the single-cluster routing application profile named `single-cluster-profile`.
The SQL DDL registers a Flink table but does not provision either prerequisite.

### Consuming Change Streams with SQL

Envelope mode emits one insert row for every Bigtable mutation.
The physical columns carry the row key and ordered mutation entries, while virtual metadata columns expose mutation identity and service timestamps:

{{< sql-snippet file="flink/BigtableExamples.sql" tag="change-stream-envelope" >}}

The `entries` array preserves the service order, and `entry_index` preserves that position after `UNNEST`.
The envelope is a mutation record rather than a reconstructed current row, so deletes also arrive as inserted envelope rows.
It declares no primary key and cannot feed a keyed upsert sink without a stateful transformation that owns row reconstruction.

### Replicating a selected cell into BigQuery

Selected-cell mode interprets one configured cell as the complete logical value for its row key.
The producer must atomically delete the complete selected column or family before setting the replacement cell; arbitrary Bigtable writers do not automatically satisfy that protocol.

The source declares exactly one primary key and emits keyed `UPDATE_AFTER` or `DELETE` rows.
That changelog can feed a BigQuery table with the same primary key and CDC enabled:

{{< sql-snippet file="flink/BigtableExamples.sql" tag="selected-cell-bigquery-cdc" >}}

The example selects one source cluster so multi-cluster conflict resolution cannot reorder the selected-cell changelog.
It deliberately supplies no BigQuery [sequence metadata]({{< relref "docs/connectors/table/bigquery" >}}#change-data-capture).
BigQuery therefore resolves colliding mutations for one primary key by arrival order, and Change Streams partitions do not supply a total application order across the pipeline.
This is an analytics-replica pattern, not a strict replica guarantee.

## Local development

### Running against the emulator

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
