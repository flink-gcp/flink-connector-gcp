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

# Bigtable options

Every option the Bigtable sink and source take. What each one is *for* is on the
[Bigtable connector]({{< relref "docs/connectors/datastream/bigtable" >}}) page; the three forms of
the Default column are explained [here]({{< relref "docs/reference" >}}#what-a-default-means).

The `WITH` options of the `bigtable` table connector are a separate surface, documented on the
[Bigtable SQL connector]({{< relref "docs/connectors/table/bigtable" >}}) page.

One thing this reference does not list, because it is not an option here. **There are no retry
knobs**: the client retries `MutateRows` per entry on a schedule of its own, so the sink owns no
retry loop — the opposite of the [Cloud Tasks]({{< relref "docs/reference/cloudtasks" >}}) sink, and
the reasoning is under
[Retries]({{< relref "docs/connectors/datastream/bigtable" >}}#retries-belong-to-the-client). The
`recovery*` knobs below are not an exception: they budget the sink-owned
[table auto-creation]({{< relref "docs/connectors/datastream/bigtable" >}}#table-auto-creation)
repair, not the client's mutation retries. The source owns no retry loop either: the client
resumes a broken `ReadRows` stream from the last key it saw.

## `BigtableSink.builder()`

| Option | Default | What it does |
|---|---|---|
| `table` | **required**, unless `destinationResolver` is set | Writes every mutation to one fixed table |
| `destinationResolver` | — | Resolves the table per record. Runs before the serializer; returning `null` fails the job |
| `serializer` | **required** | Turns a record into a `RowMutationEntry`, or into `null` to skip it |
| `appProfileId` | *unset ⇒ the instance's default profile* | The application profile the client routes through, which is what selects the routing policy and the request priority. **A Data Boost profile is read-only** — its eligible methods are `ReadRows`, `SampleRowKeys` and `PingAndWarm`, and it carries neither a request priority nor a routing policy of its own — so naming one here breaks writes. The connector cannot tell locally what kind a profile is, which is why this is documented rather than rejected at `build()` |
| `writerOptions` | [defaults](#bigtablewriteroptions) | The batch thresholds and the in-flight bounds |
| `failedMutationHandler` | `FailureHandler.failJob()` | What happens to a mutation that terminally fails. Only the two data-shaped failures reach it — see [Error handling]({{< relref "docs/connectors/datastream/bigtable" >}}#error-handling). The queue behind `sendToDeadLetterQueue(...)` has [options of its own]({{< relref "docs/reference/pubsub" >}}#pubsubdeadletterqueuebuilder) |
| `emulatorEndpoint` | — | Points the sink at an emulator over a plaintext channel with **no credentials**. Never production. Given as `host:port`, and rejected at `build()` if it is not |
| `createDisposition` | `CREATE_NEVER` | Whether a missing table or column family is created (`CREATE_IF_NEEDED`) or fails the job. `CREATE_IF_NEEDED` requires `tableCreateOptions` |
| `tableCreateOptions` | — | The [column families and rules](#tablecreateoptions) for the table the sink creates. Required with `CREATE_IF_NEEDED`, rejected with `CREATE_NEVER` |

**The mutation itself is built by the serializer, not configured here.** Row key, column families
and qualifiers, cell timestamps, deletes — every per-record decision about the *mutation* belongs to
the `BigtableSerializationSchema`, which returns the whole `RowMutationEntry`; the table it goes to
is the resolver's, never the serializer's. The one decision worth
making deliberately is the cell timestamp, because it is what decides whether a replayed record
overwrites a cell or adds a version to it; see
[Delivery guarantees]({{< relref "docs/connectors/datastream/bigtable" >}}#delivery-guarantees-and-state).

**A destination costs a batcher.** The client binds a bulk mutation batcher to one table, so the
writer holds one per table a resolver names, over a client shared by the tables of an instance; an
idle table's batcher is dropped after `destinationIdleTimeout`. The in-flight bounds below are the
writer's, summed across destinations rather than split among them. See
[Per-record destinations]({{< relref "docs/connectors/datastream/bigtable" >}}#per-record-destinations).

## `BigtableWriterOptions`

Set through `writerOptions(...)`; every knob is defaulted, so `defaults()` is equivalent to not
setting options at all.

| Option | Default | What it does |
|---|---|---|
| `batchElementCount` | *unset ⇒ 100* (the client's threshold) | How many **entries** — one per record written, whatever each carries — the client accumulates before sending a batch. **At most `19999`** |
| `batchByteSize` | *unset ⇒ 20 MiB* (the client's threshold) | How many bytes of mutations it accumulates before sending a batch. **At most `104857599`** — one byte under 100 MiB |
| `maxInFlightEntries` | 1000 | Caps unacknowledged entries. At the cap `write()` yields to the task mailbox. Above `20000` the sink logs a `WARN` — see below |
| `maxInFlightBytes` | 64 MiB | Caps their serialized size, which is the bound that actually bounds memory. Above 100 MiB the sink logs a `WARN` — see below |
| `maxConsecutiveRejections` | 100 | Fails the job once this many confirmed rejections arrive in a row with no applied mutation between them — the guardrail on a dropping policy's [isolation cost]({{< relref "docs/connectors/datastream/bigtable" >}}#error-handling). Any success resets the count; `-1` removes the bound |
| `recoveryInitialBackoff` | 500 ms | First backoff of the [table auto-creation]({{< relref "docs/connectors/datastream/bigtable" >}}#table-auto-creation) recovery: re-applying mutations after creating a missing table |
| `recoveryMaxBackoff` | 10 s | Its backoff cap; must be at least the initial backoff |
| `recoveryMaxAttempts` | 10 | Its attempt cap, after which the job fails with the incident's cause. One repair covers every table an incident left missing and shares this budget across them; a post-ensure missing-family response for an undeclared, absent family fails immediately instead |
| `destinationIdleTimeout` | 1 h | How long a table may go without mutations before the writer drops its batcher. Swept at the end of a checkpoint's flush; an evicted table rebuilds transparently. To never evict, set a very large duration — up to `Duration.ofNanos(Long.MAX_VALUE)` |
| `perDestinationMetrics` | `false` | Registers per-table `recordsSend` and `sendErrors` counters beside the writer's totals. Off by default: Flink cannot unregister a metric, so with a resolver every table the job writes to keeps a row in the registry for the task's lifetime. See [Metrics]({{< relref "docs/connectors/datastream/bigtable" >}}#metrics) |

**Every count in this table counts entries, not mutations.** An entry is one `RowMutationEntry` —
one record the serializer returned — and it carries as many mutations as the serializer put
`setCell` calls in it. Bigtable's own documented limit is on *mutations*: no more than 100,000 in a
batch. The two numbers never have to be reconciled by a job, because the client holds a batch to
that limit itself, whatever `batchElementCount` says; the measurement and what it retires are under
[Tuning]({{< relref "docs/connectors/datastream/bigtable" >}}#tuning).

**Raising `maxInFlightEntries` far above its default does not raise the effective bound; it moves
it.** The client has a flow controller of its own — 20,000 outstanding entries and 100 MiB, and it
*blocks* the calling thread when either is reached — whose limits its public API does not expose.
While the sink's own bounds are the tighter pair, a full writer yields to the task mailbox, which is
what keeps checkpoint barriers moving; past them, the task thread stalls inside the client instead.
The reasoning is under
[Tuning]({{< relref "docs/connectors/datastream/bigtable" >}}#tuning).

**Those same two budgets are where the batch thresholds' ceilings come from**, and they are the
client's rule rather than this connector's: its settings builder requires each threshold to stay
*strictly* below the matching budget, and refuses to build a client at all otherwise — on the task
manager, as the writer opens. Hence 19,999 and 100 MiB − 1: one under each.

**The in-flight bounds are warned about rather than capped at those same figures.** Setting either
above its budget is a working configuration — the client simply becomes the layer that bounds the
sink — so `build()` logs a `WARN` naming the value and what it costs instead of rejecting it. It is
not a ceiling because the budget is *per client* and this sink holds one per (project, instance):
a resolver spreading records over several instances draws on several budgets, and a
writer-global bound above one of them can be what that job means.

The two batch thresholds are left unset by default rather than restated here, so a client upgrade
that retunes them is inherited. Lowering `batchElementCount` shortens the delay before a mutation
reaches the service at low volume; the client also sends a batch after one second regardless.

## `TableCreateOptions`

The schema for the table the sink creates under `CREATE_IF_NEEDED` — its column families and, per
family, an optional garbage-collection rule. **Creation only, per family**: an existing table is
written to as it is, except that families declared here which it lacks are added, with their rules;
an existing family's rule is neither compared nor updated. What creation does and does not repair
is under
[Table auto-creation]({{< relref "docs/connectors/datastream/bigtable" >}}#table-auto-creation).

| Option | Default | What it does |
|---|---|---|
| `columnFamily` | **at least one required** | Declares a column family, optionally with a `GcRule`. Repeatable; a repeated name is last-writer-wins |

A family declared without a rule keeps Bigtable's default of collecting nothing — for this
at-least-once sink a real decision, since the garbage-collection policy is what decides whether a
replay's duplicate cell versions accumulate forever. The rule is built from four static `GcRule`
factories mirroring the admin API's shapes: `maxVersions(int)`, `maxAge(Duration)`,
`union(GcRule...)` and `intersection(GcRule...)` (each composite takes at least two rules).
`union(GcRule.maxVersions(1), GcRule.maxAge(...))` is the usual shape for keeping only the latest
cell. Validation is shape-only — positivity and arity; the service's own limits are left to
Bigtable, whose rejection names what it refused.

## `BigtableSource.builder()`

| Option | Default | What it does |
|---|---|---|
| `table` | **required** | The table to read |
| `deserializer` | **required** | Turns a row into zero or more records. Emitting nothing skips the row |
| `rowRange` | *unset ⇒ the whole table* | Adds a row-key range to read. Repeatable and additive; overlapping ranges are merged, and an empty one is rejected at `build()`. Takes a `ByteStringRange`, or an inclusive start and an exclusive end as `ByteString`s or as UTF-8 text |
| `prefix` | *unset ⇒ the whole table* | Adds every row whose key starts with a prefix — sugar for the range that prefix describes. Repeatable, and combinable with `rowRange` |
| `filter` | — | One server-side `Filters.Filter`, applied to every split. What it excludes never leaves the server. Last writer wins; a filter too large for the service is rejected at `build()` |
| `appProfileId` | *unset ⇒ the instance's default profile* | The application profile the client routes through. A [Data Boost]({{< relref "docs/connectors/datastream/bigtable" >}}#serverless-reads-with-data-boost) profile is named here like any other |
| `emulatorEndpoint` | — | Points the source at an emulator over a plaintext channel with **no credentials**. Never production. Given as `host:port`, and rejected at `build()` if it is not |

**There is no row limit, and no read-ahead or paging knobs.** A `Query.limit()` is global to a
query, so it cannot be partitioned across splits without coordination — the client library refuses
to shard a query that carries one — and the read-ahead side is a fixed internal bound rather than a
knob until a measurement asks otherwise. Both are under
[Not here yet]({{< relref "docs/connectors/datastream/bigtable" >}}#not-here-yet).

**Per-cell shaping is the filter's job, not a knob's.** Which families and qualifiers to return,
which timestamp window, how many versions of a cell — all of it is expressible through
`filter(...)`, which is why the source has no separate options for any of it.

The SQL layer's `scan.*` options map onto this builder — the
[Bigtable SQL connector]({{< relref "docs/connectors/table/bigtable" >}}) page carries that
surface, and its projection pushdown is what supplies `filter(...)` there.
