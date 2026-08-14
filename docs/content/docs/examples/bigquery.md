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

The writer context carries the record's event timestamp, which makes time-based routing expressible without the record carrying the routing key.
The [dynamic destinations guide]({{< relref "docs/examples/dynamic-destinations" >}}#bigquery-tables) defines the shared resolver contract and compares its resource lifetime with the other sinks.
This resolver caches one destination per UTC day and falls back to the record's own timestamp when the writer context has none.

{{< java-snippet file="BigQueryExamplesTablePerDay.java" tag="bigquery-examples-table-per-day-resolver" >}}

{{< java-snippet file="BigQueryExamplesTablePerDay.java" tag="bigquery-examples-table-per-day-sink" >}}

Two things need planning when a resolver keeps producing new destinations.
The default-stream and buffered-stream methods hold one writer per active destination, so `DefaultStreamOptions` and `BufferedStreamOptions` expose `destinationIdleTimeout` (one hour by default) to bound that local state.
FILE_LOADS has no destination idle timeout and retains each destination's conversion state until the writer closes, although it finishes the open staging file at every commit preparation.
Every new table is also created on its first record under the default create disposition, so [table auto-creation](#table-auto-creation) applies to every day this produces, not only the first.

## Exactly-once

Two of BigQuery's three write methods are exactly-once, and they trade against each other rather
than one being better. (Pub/Sub and Cloud Tasks are at-least-once with no exactly-once path — those
services have no transactional publish.)

Both need streaming checkpointing in `CheckpointingMode.EXACTLY_ONCE`, which is Flink's default and
so needs no line in either job below — but a cluster setting `execution.checkpointing.mode` to
`AT_LEAST_ONCE` has the job rejected when the graph is built, rather than silently downgraded.

### Buffered streams

Rows are appended to one Storage Write API buffered stream per (subtask, destination) at explicit
offsets, invisible until a completed checkpoint makes exactly that checkpoint's rows visible.

{{< java-snippet file="BigQueryExamplesBufferedStreams.java" tag="bigquery-examples-buffered-streams" >}}

`bufferedStreamOptions(...)` is required for this write method and rejected for the others, and
every knob in it is defaulted — `builder().build()` is how to say "the defaults" out loud. The
checkpoint interval is the visibility latency: rows land when the checkpoint that named them
completes.
Each active destination uses a dedicated connection and contributes its own stream creation and
flush calls, so high-cardinality routing should use an idle timeout appropriate to its churn and
must account for the Storage Write API's stream-creation quota.

### File loads

Rows are staged as files on Cloud Storage — Avro by default — and loaded with BigQuery load jobs, which is free of
streaming-insert cost and exactly-once in both execution modes.

{{< java-snippet file="BigQueryExamplesFileLoads.java" tag="bigquery-examples-file-loads" >}}

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

{{< java-snippet file="BigQueryExamplesTableAutoCreation.java" tag="bigquery-examples-table-auto-creation" >}}

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

## Reading one column of a large table

The two push-down knobs are applied by BigQuery when the read session is created, so what they
exclude never leaves it — and the columns you leave out are not scanned, which is what the read is
charged for.

{{< java-snippet file="BigQueryExamplesReadingOneColumn.java" tag="bigquery-examples-reading-one-column" >}}

The reader schema names only the column being read. A row's other columns are dropped by Avro's
schema resolution before the record is built — and here they never left BigQuery in the first place.

## Reading a public dataset

A read session belongs to a project, and that is the project it is billed to. Reading a table you do
not own — a public dataset, or another team's — means naming your own project as the payer:

{{< java-snippet file="BigQueryExamplesReadingPublicDataset.java" tag="bigquery-examples-reading-public-dataset" >}}

Without `parentProject` the session would be created in `bigquery-public-data`, where you have no
permission to create one.

## Reading a table as it was

`snapshotTime` reads the table as of an instant, from BigQuery's time-travel window. Two jobs given
the same instant read the same rows, whatever has been written since — which is what makes a
re-run reproducible rather than merely repeated.

{{< java-snippet file="BigQueryExamplesReadingSnapshot.java" tag="bigquery-examples-reading-snapshot" >}}

Note that a read session pins its own snapshot at creation regardless, so a job that does *not* set
this still reads one consistent view of the table — just whichever one existed when it started.

## Reading a view

A view cannot be read as a table — the Storage Read API reads storage, and a view has none. Run it
as a query instead, and the source reads the table its result lands in.

{{< java-snippet file="BigQueryExamplesReadingView.java" tag="bigquery-examples-reading-view" >}}

`parentProject` is required here rather than optional: no table is named, so nothing else says which
project runs the query job and is billed for it. By default the result goes to BigQuery's own
anonymous dataset, which expires it in about a day and charges no storage for it — nothing to create
and nothing to clean up.

Prune inside the query rather than with `selectedFields`: those are applied to the *result*, so they
cannot make the query itself cheaper, and a query source pays for both scans. The trade-offs and the
constraints of each landing place are under
[Reading a query or a view]({{< relref "docs/connectors/datastream/bigquery" >}}#reading-a-query-or-a-view).

## Reading a view without writing the query

If a job is pointed at names it does not control — a catalog where some are tables and some are
views — `materializeViews()` handles both without the job having to know which is which.

{{< java-snippet file="BigQueryExamplesMaterializingViews.java" tag="bigquery-examples-materializing-views" >}}

A view is materialized and read; an ordinary table is read directly, with nothing billed for a
query. It is off by default because it costs one metadata call per job to tell the two apart, and
because materializing bills a query nobody wrote. `selectedFields` is folded into the generated
`SELECT`, so a view is not scanned for columns that would only be discarded.

## Landing a query result in your own dataset

Name a dataset when the anonymous one will not do — because something outside the job has to read
the result, or because a cached results table is not a dependency you want to take.

{{< java-snippet file="BigQueryExamplesQueryResultDataset.java" tag="bigquery-examples-query-result-dataset" >}}

The dataset must already exist and be in the query's own location. The connector creates a table
there with a one-day expiration and does not delete it earlier: teardown also runs on a JobManager
failover, where the restored job is still reading the read session that table backs.

## Asking for more read streams

A read stream is read by one subtask at a time, and a subtask takes the next stream as soon as it
finishes one. Over-provisioning is therefore how the work spreads evenly: with as many streams as
subtasks, one slow stream leaves a subtask idle at the end.

{{< java-snippet file="BigQueryExamplesPreferredStreamCount.java" tag="bigquery-examples-preferred-stream-count" >}}

BigQuery decides the actual count and may give fewer — a small table is read by one stream however
many are asked for. The measured behaviour of both knobs is under
[Assignment and stream count]({{< relref "docs/connectors/datastream/bigquery" >}}#assignment-and-stream-count).
