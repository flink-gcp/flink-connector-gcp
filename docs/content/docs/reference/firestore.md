---
title: Firestore
type: docs
weight: 60
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

# Firestore options

Every option the Firestore source and sink take.
What each one is *for* is on the [Firestore connector]({{< relref "docs/connectors/datastream/firestore" >}}) page; the three forms of the Default column are explained [here]({{< relref "docs/reference" >}}#what-a-default-means).

There are no backoff knobs.
The Firestore client library retries a write itself, and the only part of that loop it lets a caller decide is how many attempts a write gets.
See [Retries]({{< relref "docs/connectors/datastream/firestore" >}}#retries).

## `FirestoreSource.builder()`

| Option | Default | What it does |
|---|---|---|
| `database` | **required** | The database to read, as `DatabaseDestination.of(project)` for the `(default)` database or `DatabaseDestination.of(project, databaseId)` |
| `deserializer` | **required** | Turns a `DocumentSnapshot` into zero or more records |
| `collectionGroup` | **required**, unless `query` is set | Reads every document of every collection with this id, at any depth, as a partitioned scan. One path segment: no `/`, no leading or trailing whitespace |
| `query` | **required**, unless `collectionGroup` is set | A `FirestoreQueryFactory` building the query to read, as one split. It runs on the JobManager, with the source's client, when the read is planned |
| `select` | every field | Field paths a collection-group scan reads, dot-separated. Repeatable. A query projects in its factory instead |
| `partitionCount` | the source's parallelism | How many partitions a collection-group scan asks the service for; an upper bound the service may answer below. Scan only |
| `readTime` | the service's time when the read is planned | The snapshot time every split reads at. Within the past hour, or with point-in-time recovery a whole minute within the past seven days; the default is good for an hour, so a longer read sets a whole minute. See [One snapshot for the whole read]({{< relref "docs/connectors/datastream/firestore" >}}#one-snapshot-for-the-whole-read) |
| `pageSize` | `500` | Documents one request asks for. A page is held in memory whole, plus up to one page more for each mid-stream retry the client library makes |
| `serviceAccountKeyFile` | *unset ⇒ ADC for the real service* | Service-account JSON key-file path, read by the JobManager when it creates or restores the enumerator and by each TaskManager reader. The job graph contains the path, not the credential contents. Mutually exclusive with `emulatorEndpoint`; see [Credentials]({{< relref "docs/connectors/datastream/firestore" >}}#credentials) |
| `emulatorEndpoint` | *unset ⇒ the real service* | `host:port` of a Firestore emulator, which does not partition: a scan against it needs a partition count of one. The only way the source reaches an emulator |

## `FirestoreSink.builder()`

| Option | Default | What it does |
|---|---|---|
| `database` | **required** | The database every write goes to, as `DatabaseDestination.of(project)` for the `(default)` database or `DatabaseDestination.of(project, databaseId)`. Which document is not configured here; the write the serializer returns names its own path |
| `serializer` | **required** | Turns a record into a `FirestoreWrite`, or into `null` to skip it |
| `writerOptions` | [defaults](#firestorewriteroptions) | The throttle, the retry budget and the writer's in-flight bounds |
| `failedWriteHandler` | `FailureHandler.failJob()` | What happens to a write the service terminally refused, or a record the serializer could not turn into one. See [Error handling]({{< relref "docs/connectors/datastream/firestore" >}}#error-handling) for the statuses that reach it |
| `preconditionFailurePolicy` | `FAIL_JOB` | What happens to a write whose `lastUpdateTime` precondition no longer holds. `ROUTE_TO_FAILURE_HANDLER` hands it to `failedWriteHandler` instead, so that handler then decides between failing, dropping and dead-lettering. A `FAILED_PRECONDITION` answering a write without a precondition always fails the job |
| `serviceAccountKeyFile` | *unset ⇒ ADC for the real service* | Service-account JSON key-file path read by each TaskManager writer at runtime. The job graph contains the path, not the credential contents. Mutually exclusive with `emulatorEndpoint`; see [Credentials]({{< relref "docs/connectors/datastream/firestore" >}}#credentials) |
| `emulatorEndpoint` | *unset ⇒ the real service* | `host:port` of a Firestore emulator. Setting it also stops the client looking for credentials. The only way the sink reaches an emulator; a writer warns when `FIRESTORE_EMULATOR_HOST` is set and this is not |

## `FirestoreWriterOptions`

Built with `FirestoreWriterOptions.builder()`, passed to `writerOptions(...)`.
Every knob is defaulted, so `FirestoreWriterOptions.defaults()` is the same as not setting options at all.

### Throttling and retries

The rates are per writer subtask: a sink of parallelism `p` starts at `p` times the initial rate.
The client library refuses a rate set while throttling is disabled, and an initial rate above the ceiling, so building the options refuses both.

| Option | Default | What it does |
|---|---|---|
| `throttlingEnabled` | `true` | Whether the client library throttles the writes it sends, ramping up by half every five minutes |
| `initialOpsPerSecond` | *unset ⇒ the client library's `500`* | The rate the throttle starts at, **at least `20`**, the library's batch size, below which its first batch is never sent. Only with throttling enabled |
| `maxOpsPerSecond` | *unset ⇒ no ceiling* | The rate the throttle ramps up to, **at least `20`** and at least the initial rate. Only with throttling enabled |
| `writeMaxAttempts` | `11` | How many attempts the `BulkWriter` gives a write refused with `RESOURCE_EXHAUSTED`, `UNAVAILABLE` or `ABORTED`, the first included: the client library's own ten retries. `1` disables these retries. Each attempt is one `BatchWrite` call, which the transport retries within the settings below |

### Transport retries

These set the transport's retries of each `BatchWrite` call, as the Pub/Sub sink's knobs of the same names set its publish call's.
An unset knob keeps the client library's value for `BatchWrite`.
The library applies these settings to every call the sink's client makes, and that client makes no other call.
Building the sink refuses a maximum shorter than its initial value, counting an unset one as the library's value, because gax would otherwise refuse it with a message naming no option; a zero cap therefore needs a zero initial value beside it.
It also refuses values that add up exactly to gax's own defaults, because the library would treat them as unset.

| Option | Default | What it does |
|---|---|---|
| `retryTotalTimeout` | *unset ⇒ the library's 60 s* | Total budget for a `BatchWrite` call including its retries; `0` is gax's own value for "bound retries by the attempt count instead", and beside a `0` `retryMaxAttempts` it means no retry at all |
| `retryInitialDelay` | *unset ⇒ the library's 100 ms* | Delay before the first retry, at most `retryMaxDelay`; `0` means none |
| `retryDelayMultiplier` | *unset ⇒ the library's ×1.3* | Factor the retry delay grows by |
| `retryMaxDelay` | *unset ⇒ the library's 60 s* | Cap on the delay between retries, at least `retryInitialDelay`; `0` beside a zero `retryInitialDelay` means no delay at all |
| `retryInitialRpcTimeout` | *unset ⇒ the library's 60 s* | Timeout of the first attempt, at most `retryMaxRpcTimeout`; `0` is gax's own value for "let the call run indefinitely" |
| `retryRpcTimeoutMultiplier` | *unset ⇒ the library's ×1.0* | Factor the per-attempt timeout grows by |
| `retryMaxRpcTimeout` | *unset ⇒ the library's 60 s* | Cap on an attempt's timeout, at least `retryInitialRpcTimeout`; `0` beside a zero `retryInitialRpcTimeout` lets every call run indefinitely |
| `retryMaxAttempts` | *unset ⇒ the library's 5* | Cap on a call's attempts, the first included; `0` bounds them by `retryTotalTimeout` alone, and beside a `0` `retryTotalTimeout` it means no retry at all |

### In-flight bounds

| Option | Default | What it does |
|---|---|---|
| `maxInFlightWrites` | `250` | Caps the writes handed to the client library and not yet answered, **at most `500`**: past that many the library queues further writes without bound instead of sending them. The default leaves half the library's slots for failed writes, which hold theirs until the writer replaces its `BulkWriter` |
| `maxInFlightBytes` | `64 MiB` | Caps their size in the request. This is the bound that actually bounds memory, since a document may be up to 1 MiB |
| `maxConsecutiveRejections` | `100` | How many confirmed refusals in a row, with no write applied between them, fail the job after being routed. `-1` removes the bound. `ALREADY_EXISTS` for a create and a routed `FAILED_PRECONDITION` for a conditional write do not count. Matters only beside a dropping `failedWriteHandler` |
