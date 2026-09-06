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
| `serviceAccountKeyFile` | *unset ⇒ application-default credentials* | Reads a service-account JSON key when each writer starts and shares it with that writer's data and table-admin clients. Every eligible TaskManager must see the same path. Rejected beside `emulatorEndpoint`; see the [deployment note]({{< relref "docs/connectors/datastream/bigtable" >}}#credential-file-deployment) |
| `writerOptions` | [defaults](#bigtablewriteroptions) | The batch thresholds and the in-flight bounds |
| `failedMutationHandler` | `FailureHandler.failJob()` | What happens to a mutation that terminally fails. Only the two data-shaped failures reach it — see [Error handling]({{< relref "docs/connectors/datastream/bigtable" >}}#error-handling). The queue behind `sendToDeadLetterQueue(...)` has [options of its own]({{< relref "docs/reference/pubsub" >}}#pubsubdeadletterqueuebuilder) |
| `emulatorEndpoint` | — | Points the sink at an emulator over a plaintext channel with **no credentials**. Never production. Given as `host:port`, and rejected at the setter if it is not |
| `createDisposition` | `CREATE_NEVER` | Whether a missing table or column family is created (`CREATE_IF_NEEDED`) or fails the job. `CREATE_IF_NEEDED` requires `tableCreateOptions` |
| `tableCreateOptions` | — | The [column families and rules](#tablecreateoptions) for the table the sink creates. Required with `CREATE_IF_NEEDED`, rejected with `CREATE_NEVER` |

**The mutation itself is built by the serializer, not configured here.** Row key, column families
and qualifiers, cell timestamps, deletes — every per-record decision about the *mutation* belongs to
the `BigtableSerializationSchema`, which returns the whole `RowMutationEntry`; the table it goes to
is the resolver's, never the serializer's.
For `setCell`, a stable explicit timestamp targets the same version after replay, while a regenerated timestamp can add a version.
For aggregate `addToCell` and `mergeToCell`, the timestamp selects the cell but does not deduplicate a contribution; see
[Delivery guarantees]({{< relref "docs/connectors/datastream/bigtable" >}}#delivery-guarantees-and-state).

**A destination costs a batcher.** The client binds a bulk mutation batcher to one table, so the
writer holds one per table a resolver names, over a client shared by the tables of an instance; an
idle table's batcher is dropped after `destinationIdleTimeout`, and the client normally starts
closing on a daemon reaper when its last live table is gone. If the runtime refuses that handoff,
the factory closes the client on the task thread to avoid leaking it. `maxActiveInstances` bounds
open and closing clients held by each writer subtask; a physical close keeps its slot, so client
creation waits interruptibly when every slot is still occupied. The in-flight bounds below are the
writer's, summed across destinations rather than split among them. See
[Per-record destinations]({{< relref "docs/connectors/datastream/bigtable" >}}#per-record-destinations).

## `BigtableWriterOptions`

Set through `writerOptions(...)`; every knob is defaulted, so `defaults()` is equivalent to not
setting options at all.

| Option | Default | What it does |
|---|---|---|
| `batchElementCountThreshold` | *unset ⇒ 100* (the client's threshold) | How many **entries** — one per record written, whatever each carries — the client accumulates before sending a batch. **At most `19999`** |
| `batchRequestByteThreshold` | *unset ⇒ 20 MiB* (the client's threshold) | How many bytes of mutations it accumulates before sending a batch. **At most `104857599`** — one byte under 100 MiB |
| `maxInFlightEntries` | 1000 | Caps unacknowledged entries. At the cap `write()` yields to the task mailbox. Above `20000` the sink logs a `WARN` — see below |
| `maxInFlightBytes` | 64 MiB | Caps their serialized size, which is the bound that actually bounds memory. Above 100 MiB the sink logs a `WARN` — see below |
| `maxConsecutiveRejections` | 100 | Fails the job once this many confirmed rejections arrive in a row with no applied mutation between them — the guardrail on a dropping policy's [isolation cost]({{< relref "docs/connectors/datastream/bigtable" >}}#error-handling). Any success resets the count; `-1` removes the bound |
| `recoveryInitialBackoff` | 500 ms | First backoff of the [table auto-creation]({{< relref "docs/connectors/datastream/bigtable" >}}#table-auto-creation) recovery: re-applying mutations after creating a missing table |
| `recoveryMaxBackoff` | 10 s | Its backoff cap; must be at least the initial backoff |
| `recoveryMaxAttempts` | 10 | Its attempt cap, after which the job fails with the incident's cause. One repair covers every table an incident left missing and shares this budget across them; a post-ensure missing-family response for an undeclared, absent family fails immediately instead |
| `destinationIdleTimeout` | 1 h | How long a table may go without mutations before the writer drops its batcher. Swept at the end of a checkpoint's flush; an evicted table rebuilds transparently. To never evict, set a very large duration — up to `Duration.ofNanos(Long.MAX_VALUE)` |
| `maxActiveInstances` | 16 | Caps open-or-closing instance clients per writer subtask. At capacity the writer drains outstanding mutations and evicts the least recently used instance; client creation waits interruptibly until physical close frees the slot. Many tables sharing one instance consume one slot |
| `perDestinationMetrics` | `false` | Registers per-table `recordsSend` and `sendErrors` counters beside the writer's totals. Off by default: Flink cannot unregister a metric, so with a resolver every table the job writes to keeps a row in the registry for the task's lifetime. See [Metrics]({{< relref "docs/connectors/datastream/bigtable" >}}#metrics) |

**Every count in this table counts entries, not mutations.** An entry is one `RowMutationEntry` —
one record the serializer returned — and it carries as many mutations as the serializer put
`setCell` calls in it. Bigtable's own documented limit is on *mutations*: no more than 100,000 in a
batch. The two numbers never have to be reconciled by a job, because the client holds a batch to
that limit itself, whatever `batchElementCountThreshold` says; the measurement and what it retires
are under [Tuning]({{< relref "docs/connectors/datastream/bigtable" >}}#tuning).

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
that retunes them is inherited. Lowering `batchElementCountThreshold` shortens the delay before a
mutation reaches the service at low volume; the client also sends a batch after one second
regardless.

## `BigtableConditionalSink.builder()`

A key file and an emulator endpoint cannot be combined; `build()` rejects that configuration.

| Option | Default | What it does |
|---|---|---|
| `table` / `destinationResolver` | required | Fixed table or per-record resolver; the last setter wins and resolution precedes serialization |
| `serializer` | required | `ConditionalSerializationSchema`; null means skip |
| `appProfileId` | *unset ⇒ instance default profile* | Application profile requiring single-cluster routing with single-row transactions enabled |
| `requestOptions` | `BigtableRequestOptions.builder().build()` | RPC deadline, request capacity and client lifecycle |
| `emptyBranchPolicy` | `IGNORE` | Whether a successful response selecting an empty list is accepted or fails the job |
| `serviceAccountKeyFile` | *unset ⇒ application-default credentials* | Key-file path readable by each TaskManager |
| `emulatorEndpoint` | unset | Emulator endpoint; uses plaintext and no credentials |
| `failedRequestHandler` | `FailureHandler.failJob()` | Handles serialization and row-level RPC failures; ambiguous failures and empty-branch policy failures bypass it |

## `BigtableConditionalAsync.builder()`

A key file and an emulator endpoint cannot be combined; `build()` rejects that configuration.

| Option | Default | What it does |
|---|---|---|
| `table` / `destinationResolver` | required | Fixed table or per-record resolver; the last setter wins and resolution precedes serialization |
| `serializer` | required | `ConditionalSerializationSchema`; null means skip |
| `appProfileId` | *unset ⇒ instance default profile* | Application profile requiring single-cluster routing with single-row transactions enabled |
| `requestOptions` | `BigtableRequestOptions.builder().build()` | RPC deadline, request capacity and client lifecycle |
| `emptyBranchPolicy` | `IGNORE` | Whether a successful response selecting an empty list is accepted or fails the job |
| `serviceAccountKeyFile` | *unset ⇒ application-default credentials* | Key-file path readable by each TaskManager |
| `emulatorEndpoint` | unset | Emulator endpoint; uses plaintext and no credentials |

`orderedWait(input, timeout)` and `unorderedWait(input, timeout)` require an explicit Flink timeout no larger than `Duration.ofNanos(Long.MAX_VALUE)`.
After Flink truncates that value to milliseconds, it must remain greater than `requestTimeout`.
The async helper exposes no failure handler; the resolver and serializer receive a null writer context.

## `BigtableReadModifyWriteSink.builder()`

A key file and an emulator endpoint cannot be combined; `build()` rejects that configuration.

| Option | Default | What it does |
|---|---|---|
| `table` / `destinationResolver` | required | Fixed table or per-record resolver; the last setter wins and resolution precedes serialization |
| `serializer` | required | `ReadModifyWriteSerializationSchema`; null means skip |
| `appProfileId` | *unset ⇒ instance default profile* | Application profile requiring single-cluster routing with single-row transactions enabled |
| `requestOptions` | `BigtableRequestOptions.builder().build()` | RPC deadline, request capacity and client lifecycle |
| `serviceAccountKeyFile` | *unset ⇒ application-default credentials* | Key-file path readable by each TaskManager |
| `emulatorEndpoint` | unset | Emulator endpoint; uses plaintext and no credentials |
| `failedRequestHandler` | `FailureHandler.failJob()` | Handles serialization and row-level RPC failures; ambiguous outcomes bypass it |

## `BigtableReadModifyWriteAsync.builder()`

A key file and an emulator endpoint cannot be combined; `build()` rejects that configuration.

| Option | Default | What it does |
|---|---|---|
| `table` / `destinationResolver` | required | Fixed table or per-record resolver; the last setter wins and resolution precedes serialization |
| `serializer` | required | `ReadModifyWriteSerializationSchema`; null means skip |
| `appProfileId` | *unset ⇒ instance default profile* | Application profile requiring single-cluster routing with single-row transactions enabled |
| `requestOptions` | `BigtableRequestOptions.builder().build()` | RPC deadline, request capacity and client lifecycle |
| `serviceAccountKeyFile` | *unset ⇒ application-default credentials* | Key-file path readable by each TaskManager |
| `emulatorEndpoint` | unset | Emulator endpoint; uses plaintext and no credentials |

`orderedWait(input, timeout)` and `unorderedWait(input, timeout)` emit input/result pairs.
The timeout must be representable in nanoseconds and greater than `requestTimeout` after Flink truncates it to milliseconds.
The helper exposes neither retries nor a failure handler; the resolver and serializer receive a null writer context.

## `BigtableRequestOptions`

The options of the
[single-row request runtime]({{< relref "docs/connectors/datastream/bigtable" >}}#single-row-request-writes)
behind `CheckAndMutateRow` and `ReadModifyWriteRow`.
The conditional and read-modify-write sinks and async helpers consume these options.
Every knob is defaulted, and there is no shared default instance: `builder().build()` is the empty
configuration. The type is separate from `BigtableWriterOptions` because a single-row request has
no batch thresholds and no in-flight bytes — one RPC for one row, whose answer is a value — so what
it needs is a deadline and a count.

| Option | Default | What it does |
|---|---|---|
| `maxInFlightRequests` | 100 | Caps requests the sink surface keeps outstanding — accepted by the client and not yet answered. At the cap `write()` yields to the task mailbox until completions bring the count down. Both public async helpers pass this value to `AsyncDataStream` as its operator capacity; Flink enforces that bound |
| `requestTimeout` | 20 s | The deadline of one request, applied to the client as a single attempt's whole budget: no retries, and the timeout is the total. A request past it fails with `DEADLINE_EXCEEDED`, which the runtime treats as [ambiguous]({{< relref "docs/connectors/datastream/bigtable" >}}#error-handling) and counts under `requestsTimedOut`. On the async surface, Flink's operator timeout should be longer than this, so that the client's deadline fires first, and under the operator's retry mode longer than this for every attempt the strategy allows, plus the backoff. At least 1 ms |
| `destinationIdleTimeout` | 1 h | How long a table may go without requests before the runtime drops its per-table state and its lease on the instance's client. Swept at the end of each successful non-final flush on the sink surface, and as inputs arrive — at most once per idle timeout, skipping a table with a request in flight — on the async surface; an evicted table rebuilds transparently. To never evict, set a very large duration — up to `Duration.ofNanos(Long.MAX_VALUE)` |
| `maxActiveInstances` | 16 | Caps open-or-closing instance clients per subtask. At capacity the sink surface drains its outstanding requests and evicts the least recently used instance; the async surface, which cannot wait, evicts an instance with nothing in flight or fails the record naming this option. Many tables sharing one instance consume one slot |
| `perDestinationMetrics` | `false` | Registers per-table `recordsSend` and `sendErrors` counters beside the runtime's totals. Off by default: Flink cannot unregister a metric, so with a resolver every table the job writes to keeps a row in the registry for the task's lifetime. See [Metrics]({{< relref "docs/connectors/datastream/bigtable" >}}#single-row-request-metrics) |

**There are no retry knobs, by design.** The client ships both RPCs with an empty retryable-code
set — they are not idempotent, and a retry of an ambiguous failure could apply an increment twice —
and the runtime adds no loop of its own, so there is nothing to tune. `requestTimeout` is the whole
of a request's one attempt; under Flink's async retry mode, the job's own loop, each attempt is one
such request — see the
[async surface]({{< relref "docs/connectors/datastream/bigtable" >}}#single-row-request-writes).

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
| `deserializer` | **required** | Turns a row into zero or more non-null records. Emit synchronously during the call; do not retain the collector. Emitting nothing skips the row |
| `rowRange` | *unset ⇒ the whole table* | Adds a row-key range to read. Repeatable and additive; overlapping ranges are merged, and an empty one is rejected at `build()`. Takes a `ByteStringRange`, or an inclusive start and an exclusive end as `ByteString`s or as UTF-8 text |
| `prefix` | *unset ⇒ the whole table* | Adds every row whose key starts with a prefix — sugar for the range that prefix describes. Repeatable, and combinable with `rowRange` |
| `filter` | — | One server-side `Filters.Filter`, applied to every split. What it excludes never leaves the server. Last writer wins; a filter too large for the service is rejected at `build()` |
| `appProfileId` | *unset ⇒ the instance's default profile* | The application profile the client routes through. A [Data Boost]({{< relref "docs/connectors/datastream/bigtable" >}}#serverless-reads-with-data-boost) profile is named here like any other |
| `maxRowsPerFetch` | `1000` | Maximum input rows one fetch hands to Flink's element queue. The fetch returns when this count or `maxBytesPerFetch` is reached |
| `maxBytesPerFetch` | `8 MiB` | Target maximum decoded input bytes one fetch hands to Flink's element queue. One row larger than the target is handed over alone |
| `serviceAccountKeyFile` | *unset ⇒ application-default credentials* | Reads a service-account JSON key when the JobManager's enumerator or a TaskManager's reader starts. Every eligible process must see the same path. Rejected beside `emulatorEndpoint`; see the [deployment note]({{< relref "docs/connectors/datastream/bigtable" >}}#credential-file-deployment) |
| `emulatorEndpoint` | — | Points the source at an emulator over a plaintext channel with **no credentials**. Never production. Given as `host:port`, and rejected at the setter if it is not |

**The fetch bounds are memory hand-off controls, not a query row limit.**
A `Query.limit()` is global to a query, so it cannot be partitioned across splits without coordination — the client library refuses to shard a query that carries one.
The source instead reads the complete configured ranges and returns control to Flink whenever either fetch bound is reached.

**Per-cell shaping is the filter's job, not a knob's.** Which families and qualifiers to return,
which timestamp window, how many versions of a cell — all of it is expressible through
`filter(...)`, which is why the source has no separate options for any of it.

The SQL layer's `scan.*` options map onto this builder — the
[Bigtable SQL connector]({{< relref "docs/connectors/table/bigtable" >}}) page carries that
surface, and its projection pushdown is what supplies `filter(...)` there.

## `BigtableChangeStreamSource.builder()`

`@PublicEvolving`: the change-stream API may change at a minor release, announced in the release
notes.

| Option | Default | What it does |
|---|---|---|
| `table` | **required** | The change-stream-enabled table to read |
| `deserializer` | **required** | Turns each `BigtableChangeStreamMutation` into zero or more non-null records. Emit synchronously during the call; do not retain the collector |
| `appProfileId` | **required** | A single-cluster-routing application profile used by every change-stream RPC |
| `serviceAccountKeyFile` | *unset ⇒ application-default credentials* | Reads a service-account JSON key when the JobManager's coordinator or a TaskManager's reader starts. Each component shares the provider among the data, table-admin and instance-admin clients that it owns, and every eligible process must see the same path. See the [deployment note]({{< relref "docs/connectors/datastream/bigtable" >}}#credential-file-deployment) |
| `startPosition` | `StartPosition.latest()` | The position used only for a fresh job: latest, earliest, an absolute instant, or a duration ago |
| `resumeFallback` | — | Explicitly restarts an expired checkpointed partition at this position, discarding any stale token it held. Without it, an expired restore fails |
| `boundedTimestamp` | — | Stops at this instant and makes the source bounded. Without it, the source is continuous |
| `maxConcurrentStreamsPerSubtask` | `2` | Bounds open `ReadChangeStream` RPCs in each source subtask. Source parallelism multiplied by this value is configured job-wide read capacity, not a Bigtable quota |
| `familyIncludeList` | empty | Retains mutation entries whose family name fully matches at least one Java regular expression. Mutually exclusive with `familyExcludeList` |
| `familyExcludeList` | empty | Removes mutation entries whose family name fully matches any Java regular expression. Mutually exclusive with `familyIncludeList` |
| `qualifierIncludeList` | empty | Retains qualified mutation entries whose `family:qualifierBase64` identifier fully matches at least one Java regular expression. The qualifier uses canonical padded RFC 4648 standard Base64. Mutually exclusive with `qualifierExcludeList` |
| `qualifierExcludeList` | empty | Removes qualified mutation entries whose `family:qualifierBase64` identifier fully matches any Java regular expression. Family-delete entries have no qualifier and use only the family filter. Mutually exclusive with `qualifierIncludeList` |
| `skipMessagesWithoutChange` | `false` | Skips deserialization when filtering removes every entry. The default delivers the mutation with an empty entry list |

There is no emulator option because the Bigtable emulator implements neither change-stream RPC.

There is no heartbeat-interval option either, because the five-second service heartbeat is also what
paces the reader's rotation of queued partitions.
