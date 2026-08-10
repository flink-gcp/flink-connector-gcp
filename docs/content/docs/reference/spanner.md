---
title: Spanner
type: docs
weight: 50
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

# Spanner options

Every option the Spanner sink and source take. What each one is *for* is on the
[Spanner connector]({{< relref "docs/connectors/datastream/spanner" >}}) page; the three forms of
the Default column are explained [here]({{< relref "docs/reference" >}}#what-a-default-means).

Unlike the [Bigtable]({{< relref "docs/reference/bigtable" >}}) sink, this one **does** take retry
knobs, and they are not decoration: the Spanner client library does not retry the batch write RPC
at all, so the sink owns the whole retry loop. See
[Retries]({{< relref "docs/connectors/datastream/spanner" >}}#retries-belong-to-the-sink).

## `SpannerSink.builder()`

| Option | Default | What it does |
|---|---|---|
| `database` | **required** | The database every mutation is written to. Which *table* is not configured here — the mutation the serializer returns names its own |
| `serializer` | **required** | Turns a record into a `Mutation`, or into `null` to skip it |
| `writerOptions` | [defaults](#spannerwriteroptions) | The batch limits, the request scheduling and the retry budget |
| `failedMutationHandler` | `FailureHandler.failJob()` | What happens to a mutation the service terminally refused. See [Error handling]({{< relref "docs/connectors/datastream/spanner" >}}#error-handling) for the two statuses that reach it |
| `constraintViolationPolicy` | `FAIL_JOB` | What happens to a mutation refused for violating a constraint. `ROUTE_TO_FAILURE_HANDLER` hands it to `failedMutationHandler` instead, so that handler then decides between failing, dropping and dead-lettering. See [Error handling]({{< relref "docs/connectors/datastream/spanner" >}}#error-handling) |
| `emulatorEndpoint` | *unset ⇒ the real service* | `host:port` of a Spanner emulator. Setting it also stops the client looking for credentials |

## `SpannerWriterOptions`

Built with `SpannerWriterOptions.builder()`, passed to `writerOptions(...)`. Every knob is
defaulted, so `SpannerWriterOptions.defaults()` is the same as not setting options at all.

### Batch limits

A request Spanner refuses is refused as a whole, so these three bound the request the writer builds.
Each has a ceiling of its own, refused at submission — but only `maxBatchBytes`' ceiling marks a
request the service is documented to refuse; the other two are argued rather than measured, and the
connector page says how. The three are **ANDed** — a batch flushes on whichever binds first — so
raising one alone often changes nothing; **`maxBatchCells` and `maxBatchBytes` are the pair that
decides how large a request grows**. The reasoning is under
[Batching]({{< relref "docs/connectors/datastream/spanner" >}}#batching).

| Option | Default | What it does |
|---|---|---|
| `maxBatchCells` | `5000` | Caps the mutation *cells* in one request, **at most `80000`**. A written column costs one cell for the table plus one for every secondary index containing it, so this is **not** a column count — raising it toward the ceiling removes the headroom that keeps an unread schema safe |
| `maxBatchMutations` | `500` | Caps the mutations in one request, **at most `80000`** — a mutation costs at least one cell, so a batch never holds more mutations than cells, and the ceiling is `maxBatchCells`' ceiling for that reason. Set above the *configured* `maxBatchCells` it cannot take effect either, and building the options **logs a warning** to wherever the job's `main` runs. Whether a lower value binds depends on what each mutation costs in cells, not on the two knobs' order |
| `maxBatchBytes` | `1048576` (1 MiB) | Caps the *estimated* size of one request, **at most `104857600`** (100 MiB). Estimated, not measured: the client library exposes no way to size a `Mutation` as it goes on the wire, and it reads low — so the ceiling is a guard against a misconfiguration, not a value to set. It is also the looser of two readings of what a batch write request may weigh; the tighter is 10 MiB, and [#441]({{< param BookRepo >}}/issues/441) measures which one holds |

### Request scheduling

| Option | Default | What it does |
|---|---|---|
| `maxCommitDelay` | *unset ⇒ the service's own handling* | How long Spanner may delay a commit to group it with others, trading latency for throughput. Between zero and 500 ms, which is what the service accepts. Not rounded to milliseconds — the client forwards seconds and nanoseconds unchanged |
| `rpcPriority` | *unset ⇒ `HIGH`* | `LOW`, `MEDIUM` or `HIGH`. Spanner treats an unspecified priority as `HIGH`, so `MEDIUM` is a step down from the default rather than a restatement of it. `LOW` is what a backfill that must not disturb serving traffic wants |

### Retry budget

Spent on transient failures only, and on the mutations that are still undecided rather than on the
whole batch.

| Option | Default | What it does |
|---|---|---|
| `retryInitialBackoff` | `500ms` | The first backoff, at least 1 ms |
| `retryMaxBackoff` | `10s` | The backoff cap, at least `retryInitialBackoff` |
| `retryMaxAttempts` | `10` | Attempts before the job fails. Exhausting the budget fails the job — a sink cannot drop what the service never refused. Note the wall-clock worst case: the client library gives a batch write a one-hour total timeout and this sink sets no deadline of its own, so a wedged request blocks the task thread — and therefore checkpointing — for up to an hour per attempt |

## `SpannerSource.builder()`

The bounded batch source. What each option is *for*, and what Spanner decides rather than the job,
is under [Source]({{< relref "docs/connectors/datastream/spanner" >}}#source).

| Option | Default | What it does |
|---|---|---|
| `database` | **required** | The database to read |
| `readOperation` | **required** | What to read: `SpannerReadOperation.query(...)`, `.read(...)` or `.readUsingIndex(...)`. A query has to be root-partitionable |
| `deserializer` | **required** | Turns a `Struct` into a record, or into `null` to skip the row |
| `timestampBound` | `TimestampBound.strong()` | The snapshot to read at. Only `strong()`, `ofReadTimestamp` and `ofExactStaleness` are accepted; the other two modes are single-use-only and are rejected here |
| `maxPartitions` | *unset ⇒ the service decides* | How many partitions to ask for. A hint the service may ignore, and the emulator ignores outright |
| `partitionSizeBytes` | *unset ⇒ the service decides* | How much data one partition should cover. A hint, like the one above |
| `dataBoostEnabled` | `false` | Runs the read on Data Boost's independent compute. Needs `spanner.databases.useDataBoost`, is billed separately, and has a concurrency quota of its own |
| `emulatorEndpoint` | *unset ⇒ the real service* | `host:port` of a Spanner emulator. Setting it also stops the client looking for credentials |

There is no per-fetch record cap here, and no options object: the cap is a correctness floor rather
than a knob, and promoting it would need a measurement.
