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

Two things this reference does not list, because they are not options here. **There are no retry
knobs**: the client retries `MutateRows` per entry on a schedule of its own, so the sink owns no
retry loop — the opposite of the [Cloud Tasks]({{< relref "docs/reference/cloudtasks" >}}) sink, and
the reasoning is under
[Retries]({{< relref "docs/connectors/datastream/bigtable" >}}#retries-belong-to-the-client). And
**there is no create disposition**: the sink never creates a table or a column family.

## `BigtableSink.builder()`

| Option | Default | What it does |
|---|---|---|
| `table` | **required** | The table every mutation is written to. Fixed for the sink's lifetime |
| `serializer` | **required** | Turns a record into a `RowMutationEntry`, or into `null` to skip it |
| `appProfileId` | *unset ⇒ the instance's default profile* | The application profile the client routes through, which is what selects the routing policy and the request priority |
| `writerOptions` | [defaults](#bigtablewriteroptions) | The batch thresholds and the in-flight bounds |
| `failedMutationHandler` | `FailureHandler.failJob()` | What happens to a mutation that terminally fails. Only the two data-shaped failures reach it — see [Error handling]({{< relref "docs/connectors/datastream/bigtable" >}}#error-handling) |
| `emulatorEndpoint` | — | Points the sink at an emulator over a plaintext channel with **no credentials**. Never production |

**The mutation itself is built by the serializer, not configured here.** Row key, column families
and qualifiers, cell timestamps, deletes — every per-record decision belongs to the
`BigtableSerializationSchema`, which returns the whole `RowMutationEntry`. The one decision worth
making deliberately is the cell timestamp, because it is what decides whether a replayed record
overwrites a cell or adds a version to it; see
[Delivery guarantees]({{< relref "docs/connectors/datastream/bigtable" >}}#delivery-guarantees-and-state).

**One table per sink.** Unlike the BigQuery and Pub/Sub sinks there is no destination resolver: the
client's bulk mutation batcher is bound to one table, so per-record tables would mean a pool of
batchers sharing the in-flight budget. Writing to several tables means several sinks today.

## `BigtableWriterOptions`

Set through `writerOptions(...)`; every knob is defaulted, so `defaults()` is equivalent to not
setting options at all.

| Option | Default | What it does |
|---|---|---|
| `batchElementCount` | *unset ⇒ 100* (the client's threshold) | How many mutations the client accumulates before sending a batch |
| `batchByteSize` | *unset ⇒ 20 MB* (the client's threshold) | How many bytes of mutations it accumulates before sending a batch |
| `maxInFlightMutations` | 1000 | Caps unacknowledged mutations. At the cap `write()` yields to the task mailbox |
| `maxInFlightBytes` | 64 MiB | Caps their serialized size, which is the bound that actually bounds memory |

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
