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

Every option the Bigtable sink takes. What each one is *for* is on the
[Bigtable connector]({{< relref "docs/connectors/datastream/bigtable" >}}) page; the three forms of
the Default column are explained [here]({{< relref "docs/reference" >}}#what-a-default-means).

One thing this reference does not list, because it is not an option here. **There are no retry
knobs**: the client retries `MutateRows` per entry on a schedule of its own, so the sink owns no
retry loop — the opposite of the [Cloud Tasks]({{< relref "docs/reference/cloudtasks" >}}) sink, and
the reasoning is under
[Retries]({{< relref "docs/connectors/datastream/bigtable" >}}#retries-belong-to-the-client). The
`recovery*` knobs below are not an exception: they budget the sink-owned
[table auto-creation]({{< relref "docs/connectors/datastream/bigtable" >}}#table-auto-creation)
repair, not the client's mutation retries.

## `BigtableSink.builder()`

| Option | Default | What it does |
|---|---|---|
| `table` | **required**, unless `destinationResolver` is set | Writes every mutation to one fixed table |
| `destinationResolver` | — | Resolves the table per record. Runs before the serializer; returning `null` fails the job |
| `serializer` | **required** | Turns a record into a `RowMutationEntry`, or into `null` to skip it |
| `appProfileId` | *unset ⇒ the instance's default profile* | The application profile the client routes through, which is what selects the routing policy and the request priority |
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
| `batchElementCount` | *unset ⇒ 100* (the client's threshold) | How many mutations the client accumulates before sending a batch |
| `batchByteSize` | *unset ⇒ 20 MB* (the client's threshold) | How many bytes of mutations it accumulates before sending a batch |
| `maxInFlightMutations` | 1000 | Caps unacknowledged mutations. At the cap `write()` yields to the task mailbox |
| `maxInFlightBytes` | 64 MiB | Caps their serialized size, which is the bound that actually bounds memory |
| `maxConsecutiveRejections` | 100 | Fails the job once this many confirmed rejections arrive in a row with no applied mutation between them — the guardrail on a dropping policy's [isolation cost]({{< relref "docs/connectors/datastream/bigtable" >}}#error-handling). Any success resets the count; `-1` removes the bound |
| `recoveryInitialBackoff` | 500 ms | First backoff of the [table auto-creation]({{< relref "docs/connectors/datastream/bigtable" >}}#table-auto-creation) recovery: re-applying mutations after creating a missing table |
| `recoveryMaxBackoff` | 10 s | Its backoff cap; must be at least the initial backoff |
| `recoveryMaxAttempts` | 10 | Its attempt cap, after which the job fails with the incident's cause. One repair covers every table an incident left missing, and shares this budget across them |
| `destinationIdleTimeout` | 1 h | How long a table may go without mutations before the writer drops its batcher. Swept at the end of a checkpoint's flush; an evicted table rebuilds transparently. To never evict, set a very large duration — up to `Duration.ofNanos(Long.MAX_VALUE)` |
| `perDestinationMetrics` | `false` | Registers per-table `recordsSend` and `sendErrors` counters beside the writer's totals. Off by default: Flink cannot unregister a metric, so with a resolver every table the job writes to keeps a row in the registry for the task's lifetime. See [Metrics]({{< relref "docs/connectors/datastream/bigtable" >}}#metrics) |

**Raising `maxInFlightMutations` far above its default does not raise the effective bound; it moves
it.** The client has a flow controller of its own — 1000 entries per channel and 100 MB, and it
*blocks* the calling thread when either is reached — whose limits its public API does not expose.
While the sink's own bounds are the tighter pair, a full writer yields to the task mailbox, which is
what keeps checkpoint barriers moving; past them, the task thread stalls inside the client instead.
The reasoning is under
[Tuning]({{< relref "docs/connectors/datastream/bigtable" >}}#tuning).

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
