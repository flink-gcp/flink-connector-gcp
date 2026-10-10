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

The `firestore` connector reads and writes Table API and SQL rows in one collection of a Firestore database in Native mode, or reads every collection with one id, one document per row, through `flink-connector-gcp-firestore`.
It maps onto the [DataStream source and sink]({{< relref "docs/connectors/datastream/firestore" >}}), so snapshot, paging, recovery, throttling, batching, retry, delivery, metrics and failure behavior remain the same; a lookup join reads through its own client, with the retries its section describes.
A table with a PRIMARY KEY writes its rows under their key as the document id; a table without one creates a new document for every row.

{{< sql-snippet file="flink/FirestoreTableReference.sql" tag="overview" >}}

A scan reads the collection as a bounded snapshot, and a lookup join reads one document of it by id.
A database in Datastore mode uses the [`datastore` connector]({{< relref "docs/connectors/table/datastore" >}}) instead.

Use `flink-sql-connector-gcp-firestore`, the relocated SQL uber-jar, for SQL deployments, and place it in Flink's `lib/` before starting the cluster.
It is not on Maven Central yet: the release that first publishes the Firestore connector is tracked by [#1547]({{< param BookRepo >}}/issues/1547), and until then the jar is built from source as [Development]({{< relref "docs/development" >}}) describes.
The artifact bundles the connector and its runtime dependencies, for both Native mode and Datastore mode, while leaving Flink APIs provided by the cluster.
Its bundled dependency implementations are relocated so it can coexist with other connector jars and with application dependencies.
DataStream applications should depend on `flink-connector-gcp-firestore` instead of the SQL uber-jar.

## Lineage

The Table source and sink report their configured collection in the `gcp` facet of one logical SQL dataset, named by the table's catalog identifier.
A scan under `scan.collection-group` reports the group instead, as a `firestore-collection-group` resource named by the collection id.
The collection is a `firestore-collection` resource: namespace `firestore://{project}/{database}`, with the default database spelled `(default)`, and the collection path relative to the database as its name, such as `orders` or `users/alice/audit`.
It names the documents directly in that collection, not those of its subcollections.
Lookup joins are outside this extraction path and report no resource.
See [Lineage]({{< relref "docs/connectors/lineage" >}}) for the class loader configuration and what Flink 1.20 and 2.x each deliver.

## Credentials

`service-account-key-file` selects one service-account JSON key for the scan, the lookup and the sink.
When it and `emulator-endpoint` are absent, all three use Application Default Credentials.
The option stores only the path in the job graph: a scan reads the file on the JobManager, where it plans the read, and on each TaskManager that reads a split, while each lookup and sink subtask reads it on its TaskManager, so mount the same path in every container.
The option is mutually exclusive with `emulator-endpoint`, because the emulator channel carries no credentials.
See the [DataStream credentials]({{< relref "docs/connectors/datastream/firestore" >}}#credentials) for the `FIRESTORE_EMULATOR_HOST` variable, which the client library reads on its own.

## Schema and type mapping

A table names one collection with `collection`: a collection id such as `orders`, or a path that alternates collection and document ids and ends with a collection id, such as `users/alice/audit`.
A path with an even number of segments names a document and is refused.
Each row is one document of that collection (or, for a collection-group scan, of any collection with its id), and each column other than the PRIMARY KEY is one top-level field, named exactly as the column is: a column named `a.b` is a field whose name contains a dot.
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

### Reading values back

A scan reads each column from the top-level field of its name, and the PRIMARY KEY column from the document id.
A field the document does not have reads as NULL, as does a field that holds null.
An integer reads into a `DOUBLE` column only when the `DOUBLE` represents it exactly, up to `2^53` in magnitude; a 32-bit BSON integer reads only into a `BIGINT`, and a table sink writes it back as a 64-bit integer.
A timestamp reads at the column's precision, truncated.
A string or bytes read whole, whatever length a `CHAR`, `VARCHAR`, `BINARY` or `VARBINARY` column declares: the scan neither checks the length nor pads a fixed-length value, so declare `STRING` and `BYTES` for values of any length.
A reference reads as its document path into a column `reference-field-paths` names, and a geographical point into a ROW `geo-point-field-paths` names.

A collection has no schema, so a document can hold, under a column's name, a value of another type: a string where the column is `BIGINT`, a reference where the column is an unmarked `STRING`, a reference into another database, or a value outside the mapping such as bytes of a BSON subtype, a vector or one of the BSON value types an Enterprise-edition database stores.
`type-mismatch-policy` decides what such a value does.
Under `fail` the read fails, naming the document and the field's path in the schema, with a map's values as `.value`.
Under `null` the innermost nullable field around the value reads as NULL instead; a `NOT NULL` field cannot, so the nearest nullable field around it does, which also covers a `NOT NULL` nested field the document lacks.
A `NOT NULL` column has no nullable field around it, so a mismatched or missing value there fails the read under either policy.

## Source

A scan reads the documents directly in the table's collection, as one query on one split, at one read time.
With `scan.collection-group` it reads every collection whose id is the last segment of `collection`, at any depth of the database, as a collection-group scan that the service partitions; `scan.partition.max-partitions` caps the partitions it asks for.
Both read a consistent snapshot at `scan.read-time`, or at the service's time when the read is planned.

{{< sql-snippet file="flink/FirestoreTableReference.sql" tag="scan" >}}

A scan reads only the fields of the columns it produces: the declared columns, or the projected ones when the planner pushes a projection down, each named as one literal field, so a document's undeclared top-level fields never travel; a `ROW` or `MAP` column's field is read whole, with any nested keys the `ROW` does not declare.
Filters are not pushed down.

A collection-group scan reads documents of many collections, whose ids repeat across them, so its table cannot declare a PRIMARY KEY: the planner trusts a declared key to be unique and removes a `GROUP BY` or a `DISTINCT` over it, so repeated ids would come back unaggregated.
The `document-path` metadata column, which is unique, identifies a document instead.

A scan or a lookup can read four metadata columns:

| Key | Type | Meaning |
|---|---|---|
| `document-path` | `STRING NOT NULL` | The document's path relative to the database, which tells the parents of a collection-group scan's documents apart |
| `create-time` | `TIMESTAMP_LTZ(6) NOT NULL` | When the document was created |
| `update-time` | `TIMESTAMP_LTZ(6) NOT NULL` | When the document was last updated |
| `read-time` | `TIMESTAMP_LTZ(6) NOT NULL` | The time the document was read at |

{{< sql-snippet file="flink/FirestoreTableReference.sql" tag="collection-group" >}}

The [DataStream source]({{< relref "docs/connectors/datastream/firestore" >}}#source) describes the snapshot's time window, paging and recovery.

## Lookup

A lookup join reads one document of the table's collection by the PRIMARY KEY column, its document id.
It also accepts equality keys on other top-level physical scalar columns, whether a constant such as `c.tier = 'gold'` appears in `ON` or `WHERE`, or an input column supplies the comparison value.
Only the document id addresses the read; the additional keys compare against the converted Flink row before it is returned, with binary values compared by content.
Nested key paths, metadata columns, and additional `ARRAY`, `MAP`, or `ROW` keys are rejected during planning.
A NULL lookup key or a failed additional equality joins no row.
A condition that is not an equality, such as `c.tier <> 'gold'`, stays a filter applied after the lookup.
A table without a PRIMARY KEY, which a collection-group table always is, cannot be looked up.
A lookup that reaches the service reads the document as it is at that moment, and under a `PARTIAL` cache a hit returns the row as it was cached until it expires; `scan.read-time` and the other `scan.*` options do not apply to it.

{{< sql-snippet file="flink/FirestoreTableReference.sql" tag="lookup" >}}

A document that does not exist joins no row, so a `LEFT JOIN` fills its columns with NULL.
A key that cannot be a document id in the collection joins no row without a read: NULL, empty, `.`, `..`, a key matching `__.*__` (two underscores, anything, two underscores), one longer than 1,500 bytes, and any key holding a `/`, which would otherwise name another document (`a/` names `a`, and `a/sub/b` a document of a subcollection).
Every other key is sent to the service as it is.

A lookup reads only the fields of the columns the join uses, like a scan, and fills the metadata columns from the document it read; `read-time` is the time of that read.
`type-mismatch-policy` applies to a looked-up value as it does to a scanned one.

`lookup.async` runs the join as Flink's asynchronous lookup, with several reads in flight per subtask, instead of waiting for each read; both modes read through the same `BatchGetDocuments` call.
Flink's `lookup.cache` modes `NONE` and `PARTIAL` are supported, and Flink owns the partial cache; `FULL` is refused, because a full cache is loaded by a scan with a snapshot and a reload contract of its own.
The cache stores the filtered result under the complete lookup-key tuple, so different additional values for the same document id have separate entries.
An additional equality that fails follows the configured missing-key cache policy; expiry and size options keep their standard Flink meaning.
`lookup.max-retries` reads again after `UNAVAILABLE`, `INTERNAL` or `DEADLINE_EXCEEDED`, the statuses the client library already retries this call on, so it adds attempts on top of the library's rather than retrying anything the library does not; it counts retries after the first read, and every other failure fails the join at once.

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
On Flink 2.2 and 2.3, two Flink bugs fixed in 2.4.0 affect such a delete, whose non-key columns are null.
Flink's not-null enforcer checks those columns and fails the job when the table declares one of them `NOT NULL` ([FLINK-40477](https://issues.apache.org/jira/browse/FLINK-40477)); setting `table.exec.sink.not-null-enforcer` to `DROP`, as the error suggests, drops the delete instead, so the document is never deleted.
A projection between the source and the sink also evaluates its expressions over those columns, which can fail the job when the source declares one of them `NOT NULL` ([FLINK-40528](https://issues.apache.org/jira/browse/FLINK-40528)).
Declare the non-key columns nullable, in this table and in the source table, when the input's deletes carry the key alone.

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
| `service-account-key-file` | *unset ⇒ ADC for the real service* | Service-account JSON key-file path, read by the scan's JobManager and readers and by each lookup and sink subtask; rejected with `emulator-endpoint` |
| `geo-point-field-paths` | empty | Semicolon-separated field paths of `ROW<latitude DOUBLE, longitude DOUBLE>` values written as geographical points |
| `reference-field-paths` | empty | Semicolon-separated field paths of `STRING` values written as references to documents |
| `type-mismatch-policy` | `fail` | What a read does with a stored value whose type does not match its column: `fail` the read, or read the field as `null` |
| `scan.collection-group` | `false` | Scan every collection whose id is the last segment of `collection`, partitioned, rather than the one collection as a single split |
| `scan.partition.max-partitions` | *unset ⇒ the scan's parallelism* | Maps to `partitionCount`: the partitions a collection-group scan asks the service for; only with `scan.collection-group` |
| `scan.read-time` | *unset ⇒ the service's time when the read is planned* | Maps to `readTime`: an ISO-8601 instant such as `2026-10-04T00:00:00Z`, within the past hour, or a whole minute within seven days with point-in-time recovery |
| `scan.max-rows-per-fetch` | `500` | Maps to `pageSize`: the documents one request asks for |
| `scan.parallelism` | *unset ⇒ the planner's parallelism* | Flink's standard source parallelism override |
| `lookup.async` | `false` | Run the join as Flink's asynchronous lookup, with several reads in flight per subtask |
| `lookup.cache` | `NONE` | Flink's standard lookup cache mode; `NONE` and `PARTIAL` are supported |
| `lookup.max-retries` | `3` | Reads after the first one, for `UNAVAILABLE`, `INTERNAL` and `DEADLINE_EXCEEDED` only |
| `lookup.partial-cache.expire-after-access` | *unset* | Flink's standard partial-cache access expiry |
| `lookup.partial-cache.expire-after-write` | *unset* | Flink's standard partial-cache write expiry |
| `lookup.partial-cache.cache-missing-key` | `true` | Whether the partial cache records a key that found no document |
| `lookup.partial-cache.max-rows` | *unset* | Maximum rows the partial cache keeps |
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

The [configuration reference]({{< relref "docs/reference/firestore" >}}#firestorewriteroptions) explains each writer option, and a refusal of a `scan.*`, `lookup.*` or `sink.*` value names the option key the `WITH` clause spells.

## Delivery guarantee

A scan restored from a checkpoint resumes each split just after the last document it passed, at the same read time, without reading a document twice or skipping one; a job that restarts before its first checkpoint plans again, at a new read time unless `scan.read-time` is set.

The sink is at-least-once: it waits for every write it has sent at each checkpoint, and a restart replays the records after the last completed one.
With a PRIMARY KEY, `set`, `merge` and a delete are idempotent, so a replayed row writes the same document again; an `update` replayed after something outside the job deleted the document fails the job, and fails again after each restart until the document exists.
Without a PRIMARY KEY, a replayed row creates another document under a new id, so a restart can leave duplicates.
See the [DataStream delivery guarantee]({{< relref "docs/connectors/datastream/firestore" >}}#delivery-guarantee).

## Design decisions and testing

The table mapping, the markers and the declined alternatives are recorded in [ADR-0179]({{< param BookRepo >}}/blob/main/docs/adr/0179-the-firestore-table-sink-writes-one-collection-keyed-by-document-id.md).
The emulator integration tests write every type through SQL and read the documents back with the client library, read every type back through a SQL scan, and join against documents through blocking and asynchronous lookups; the uber-jar's tests write through its relocated classes.
