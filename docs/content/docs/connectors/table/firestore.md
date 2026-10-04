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

# Firestore SQL connector

The `firestore` connector writes Table API and SQL rows into one collection of a Firestore database in Native mode, one document per row, through `flink-connector-gcp-firestore`.
It maps onto the [DataStream sink]({{< relref "docs/connectors/datastream/firestore" >}}#sink), so throttling, batching, retry, delivery, metrics and failure behavior remain the same.
A table with a PRIMARY KEY writes its rows under their key as the document id; a table without one creates a new document for every row.

{{< sql-snippet file="flink/FirestoreTableReference.sql" tag="overview" >}}

The connector is a sink for now.
Scanning a collection arrives with [#1608]({{< param BookRepo >}}/issues/1608) and lookup joins with [#1609]({{< param BookRepo >}}/issues/1609); until then, reading a `firestore` table is refused when the statement is planned.
The Datastore-mode table connector is [#1545]({{< param BookRepo >}}/issues/1545).

Use `flink-sql-connector-gcp-firestore`, the relocated SQL uber-jar, for SQL deployments, and place it in Flink's `lib/` before starting the cluster.
It is not on Maven Central yet: the release that first publishes the Firestore connector is tracked by [#1547]({{< param BookRepo >}}/issues/1547), and until then the jar is built from source as [Development]({{< relref "docs/development" >}}) describes.
The artifact bundles the connector and its runtime dependencies, for both Native mode and Datastore mode, while leaving Flink APIs provided by the cluster.
Its bundled dependency implementations are relocated so it can coexist with other connector jars and with application dependencies.
DataStream applications should depend on `flink-connector-gcp-firestore` instead of the SQL uber-jar.

## Lineage

The Table sink reports its configured collection in the `gcp` facet of one logical SQL dataset, named by the table's catalog identifier.
The collection is a `firestore-collection` resource: namespace `firestore://{project}/{database}`, with the default database spelled `(default)`, and the collection path relative to the database as its name, such as `orders` or `users/alice/audit`.
It names the documents directly in that collection, not those of its subcollections.
See [Lineage]({{< relref "docs/connectors/lineage" >}}) for the class loader configuration and what Flink 1.20 and 2.x each deliver.

## Credentials

`service-account-key-file` selects one service-account JSON key for the sink.
When it and `emulator-endpoint` are absent, the sink uses Application Default Credentials.
The option stores only the path in the job graph, and each sink subtask reads the file on its TaskManager, so mount the same path in every TaskManager container.
The option is mutually exclusive with `emulator-endpoint`, because the emulator channel carries no credentials.
See the [DataStream credentials]({{< relref "docs/connectors/datastream/firestore" >}}#credentials) for the `FIRESTORE_EMULATOR_HOST` variable, which the client library reads on its own.

## Schema and type mapping

A table names one collection with `collection`: a collection id such as `orders`, or a path that alternates collection and document ids and ends with a collection id, such as `users/alice/audit`.
A path with an even number of segments names a document and is refused.
Each row is one document of that collection, and each column other than the PRIMARY KEY is one top-level field, named exactly as the column is: a column named `a.b` is a field whose name contains a dot.
A column or `ROW` field whose name starts and ends with `__` is refused when the statement is planned, because Firestore reserves such names and would refuse every document.

The PRIMARY KEY is the document id.
It must be one `STRING` column, declared `PRIMARY KEY (...) NOT ENFORCED`, and it is never stored as a field.
A row whose key is empty or contains `/` fails the job, because the path it would compose names another document, or a document in another collection.
A table without a PRIMARY KEY writes every row as a new document under an id the connector draws, 20 letters and digits, as the client library does for an added document.
The write is a `set` rather than a `create`, so a write the client library retries after its answer was lost lands again instead of failing the job as a document that already exists.

### Type mapping

| Flink type | Firestore value |
|---|---|
| `STRING`, `VARCHAR`, `CHAR` | String |
| `BIGINT` | Integer |
| `DOUBLE` | Floating-point number |
| `BOOLEAN` | Boolean |
| `BYTES`, `VARBINARY`, `BINARY` | Bytes |
| `TIMESTAMP_LTZ(p)` | Timestamp; Firestore keeps microseconds |
| `ARRAY<T>` | Array; an array directly inside an array is refused, as Firestore stores none |
| `ROW<...>` | Map, one entry per field |
| `MAP<STRING, T>` | Map |
| `ROW<latitude DOUBLE, longitude DOUBLE>` named by `geo-point-field-paths` | Geographical point |
| `STRING` named by `reference-field-paths` | Reference to the document at that path |

Every other Flink type is refused when the statement is planned, naming the field: write an `INT` as a `BIGINT` and a `FLOAT` as a `DOUBLE`.
`DECIMAL` has no Firestore counterpart and is not mapped.
A SQL `NULL` is written as a Firestore null value, so the field exists and holds null.

A geographical point and a reference share their Flink types with ordinary values, so the table names them with two markers, each a semicolon-separated list of field paths.
A path names a column, then a ROW field by `.name`; an `ARRAY` is passed through, so a path through an array names its elements, and a `MAP`'s values are `.value`.
A marker that names a path the table does not declare, a field of the wrong type, the PRIMARY KEY, or a path two fields share (a column named `a.b` beside a ROW `a` with a field `b`), is refused when the statement is planned.

{{< sql-snippet file="flink/FirestoreTableReference.sql" tag="markers" >}}

A reference's value is a document path relative to the same database, such as `staff/alice`; the connector stores it as a reference to that document in the database the table writes to, and a value that is not a document path fails the job.
A point's latitude and longitude must both be set and within range, or the row fails the job.

## Sink

### Write modes

With a PRIMARY KEY, `sink.write-mode` says what an insert or an update does to the document:

- **`set`** replaces the document with the row, creating it if it is missing; fields the table does not declare are removed.
- **`merge`** merges the row into the document, creating it if it is missing; fields the table does not declare keep their values, and a `ROW` or `MAP` column is merged key by key rather than replaced, so an empty `MAP` leaves the stored map as it is.
- **`update`** replaces the table's fields of an existing document; a missing document fails the job. An `update` table takes inserts and updates but no deletes, and an input that carries deletes is refused when the statement is planned: an update reordered after a delete of its document, or replayed after a restart, would meet a missing document and fail the job again on every restart.
  The planner can still send a delete when the query's key differs from the table's PRIMARY KEY, as in an aggregation written into a table keyed by its result, because it then materializes the upsert and turns a retraction into a delete; such a delete fails the job without deleting anything, and the message says to key the query by the PRIMARY KEY or to use `set` or `merge`.

Under `set` and `merge`, a delete deletes the document; deleting a missing document succeeds.
`sink.write-mode` is refused on a table without a PRIMARY KEY, whose rows are always created as new documents.

{{< sql-snippet file="flink/FirestoreTableReference.sql" tag="merge" >}}

A table without a PRIMARY KEY accepts inserts only, and creates one document per row:

{{< sql-snippet file="flink/FirestoreTableReference.sql" tag="append" >}}

### Changelog and ordering

A table with a PRIMARY KEY consumes an upsert changelog: inserts, updates after and, except under `update`, deletes, keyed by the document id.
On Flink 2.x a delete may carry the key alone, which is all the sink reads from it.

The sink does not keep the order of two writes to one document.
It sends writes through the client library's `BulkWriter`, which sends several requests at once and applies the writes of one request in no guaranteed order, so a later change of a key can be applied before an earlier one.
That is harmless when every key changes at most once in a job, as when the input is a table's rows written once, but an input that updates a key repeatedly, such as an aggregation, can leave an older value in the document.
The Spanner and Bigtable Table sinks share the limitation, and an opt-in mode that keeps each document's order is [#1556]({{< param BookRepo >}}/issues/1556).

### What a `WITH` clause cannot set

A refused write fails the job: a `WITH` clause cannot name a serializable failure handler, so the DataStream sink's routing of refused writes to one is not available, and neither is `sink.max-consecutive-rejections`, which only bounds a handler that drops them.
No table write carries a precondition.

## Options

| Option | Default | What it does |
|---|---|---|
| `project` | **required** | The Google Cloud project containing the database |
| `database` | *unset ⇒ `(default)`* | The id of the Firestore database in Native mode |
| `collection` | **required** | The collection path the table's documents are in, relative to the database; an odd number of `/`-separated segments, each without leading or trailing whitespace |
| `emulator-endpoint` | *unset ⇒ the real service* | `host:port` of a Firestore emulator; setting it also stops credential discovery. Parsed when a statement over the table is planned, not at `CREATE TABLE` |
| `service-account-key-file` | *unset ⇒ ADC for the real service* | Service-account JSON key-file path read by each sink subtask; rejected with `emulator-endpoint` |
| `geo-point-field-paths` | empty | Semicolon-separated field paths of `ROW<latitude DOUBLE, longitude DOUBLE>` values written as geographical points |
| `reference-field-paths` | empty | Semicolon-separated field paths of `STRING` values written as references to documents |
| `sink.write-mode` | `set` | `set`, `merge` or `update`, for a table with a PRIMARY KEY |
| `sink.throttling.enabled` | `true` | Maps to `throttlingEnabled`: whether the client library ramps the write rate up |
| `sink.throttling.initial-ops-per-second` | *unset ⇒ the client library's `500`* | Maps to `initialOpsPerSecond`, at least `20`; only with throttling enabled |
| `sink.throttling.max-ops-per-second` | *unset ⇒ no ceiling* | Maps to `maxOpsPerSecond`, at least `20` and at least the initial rate; only with throttling enabled |
| `sink.write.max-attempts` | `11` | Maps to `writeMaxAttempts`: how many attempts the client library gives a write refused with a retryable status, the first included |
| `sink.retry.total-timeout` | *unset ⇒ the library's 60 s* | Maps to `retryTotalTimeout` |
| `sink.retry.initial-delay` | *unset ⇒ the library's 100 ms* | Maps to `retryInitialDelay`, at most `sink.retry.max-delay` |
| `sink.retry.delay-multiplier` | *unset ⇒ the library's ×1.3* | Maps to `retryDelayMultiplier` |
| `sink.retry.max-delay` | *unset ⇒ the library's 60 s* | Maps to `retryMaxDelay`, at least `sink.retry.initial-delay` |
| `sink.retry.initial-rpc-timeout` | *unset ⇒ the library's 60 s* | Maps to `retryInitialRpcTimeout`, at most `sink.retry.max-rpc-timeout` |
| `sink.retry.rpc-timeout-multiplier` | *unset ⇒ the library's ×1.0* | Maps to `retryRpcTimeoutMultiplier` |
| `sink.retry.max-rpc-timeout` | *unset ⇒ the library's 60 s* | Maps to `retryMaxRpcTimeout`, at least `sink.retry.initial-rpc-timeout` |
| `sink.retry.max-attempts` | *unset ⇒ the library's 5* | Maps to `retryMaxAttempts` |
| `sink.in-flight.max-writes` | `250` | Maps to `maxInFlightWrites`, at most `500` |
| `sink.in-flight.max-bytes` | `64 mb` | Maps to `maxInFlightBytes` |
| `sink.parallelism` | *unset ⇒ the input's parallelism* | Flink's standard sink parallelism override |

The [configuration reference]({{< relref "docs/reference/firestore" >}}#firestorewriteroptions) explains each writer option, and a refusal of a `sink.*` value names the option key the `WITH` clause spells.

## Delivery guarantee

The sink is at-least-once: it waits for every write it has sent at each checkpoint, and a restart replays the records after the last completed one.
With a PRIMARY KEY, `set`, `merge` and a delete are idempotent, so a replayed row writes the same document again; an `update` replayed after something outside the job deleted the document fails the job, and fails again after each restart until the document exists.
Without a PRIMARY KEY, a replayed row creates another document under a new id, so a restart can leave duplicates.
See the [DataStream delivery guarantee]({{< relref "docs/connectors/datastream/firestore" >}}#delivery-guarantee).

## Design decisions and testing

The table mapping, the markers and the declined alternatives are recorded in [ADR-0179]({{< param BookRepo >}}/blob/main/docs/adr/0179-the-firestore-table-sink-writes-one-collection-keyed-by-document-id.md).
The emulator integration tests write every type through SQL and read the documents back with the client library; the uber-jar's tests write through its relocated classes.
