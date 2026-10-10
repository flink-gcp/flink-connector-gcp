---
title: Firestore in Datastore mode
type: docs
weight: 61
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

# Datastore-mode SQL connector

The `datastore` connector reads and writes Table API and SQL rows as the entities of one kind, in one namespace of a Firestore database in Datastore mode, through `flink-connector-gcp-firestore`.
It maps onto the [Datastore-mode DataStream source and sink]({{< relref "docs/connectors/datastream/firestore" >}}#datastore-mode), so splitting, snapshot, paging, recovery, batching, ramp-up, delivery, metrics and failure behavior remain the same.
A table with a PRIMARY KEY writes each row under that key's name or numeric id; a table without one writes every row as a new entity.

{{< sql-snippet file="flink/DatastoreTableReference.sql" tag="overview" >}}

A scan reads the kind as a bounded snapshot; a lookup join is [#1653]({{< param BookRepo >}}/issues/1653).
A database in Native mode uses the [`firestore` connector]({{< relref "docs/connectors/table/firestore" >}}) instead.

Use `flink-sql-connector-gcp-firestore`, the relocated SQL uber-jar, for SQL deployments, and place it in Flink's `lib/` before starting the cluster.
The same jar carries the `firestore` and `datastore` connectors; the [Firestore SQL connector]({{< relref "docs/connectors/table/firestore" >}}) page describes how to obtain it.

## Lineage

The Table source and sink report their configured kind in the `gcp` facet of one logical SQL dataset, named by the table's catalog identifier.
The kind is a `datastore-kind` resource: namespace `datastore://{project}/{database}`, with the default database spelled `(default)`, followed by `/{namespace}` outside the default namespace, and the kind as its name.
This is the resource the [Datastore-mode source]({{< relref "docs/connectors/datastream/firestore" >}}#lineage) reports for the same kind.
See [Lineage]({{< relref "docs/connectors/lineage" >}}) for the class loader configuration and what Flink 1.20 and 2.x each deliver.

## Credentials

`service-account-key-file` selects one service-account JSON key for the scan and the sink.
The option stores only the path in the job graph: a scan reads the file on the JobManager, where it plans the read, and on each TaskManager that reads a split, while each sink subtask reads it on its TaskManager, so mount the same path in every container.
Without it and without `emulator-endpoint`, both use Application Default Credentials, and the identity needs the role the [DataStream credentials]({{< relref "docs/connectors/datastream/firestore" >}}#credentials) name.
The option is mutually exclusive with `emulator-endpoint`, because the emulator channel carries no credentials.
Only `emulator-endpoint` reaches an emulator, one started with `--database-mode=datastore-mode`: the scan and the sink always give their clients a host, so `DATASTORE_EMULATOR_HOST` is never read.

## Keys and type mapping

A table names one kind with `kind`, in the namespace `namespace` names or in the default namespace without it.
Each row is one entity, and each column other than the PRIMARY KEY is one property of the entity, named exactly as the column is.
A kind or a namespace that every write would be refused for is refused when the statement is planned, for a scan as well as the sink: one of the form `__…__` with at least one character between the underscores, such as `__Stat_Kind__`, which the emulator refuses as reserved; a kind longer than 1,500 bytes; and a namespace outside the client library's grammar of at most 100 letters, digits, `.`, `_` and `-`.
So are a column or `ROW` field with such a name or one longer than 1,500 bytes, wherever it is declared, and a `project` outside the client library's grammar for a project id.
A reserved kind holds Datastore's own statistics or metadata, and the scan refuses it too: the emulator answers the `__kind__` metadata query without the per-entity cursor the scan resumes from, and reading a statistics kind has not been measured; [#1690]({{< param BookRepo >}}/issues/1690) tracks reading them.
A blank `kind` or `namespace` is refused too; leave `namespace` out for the default namespace.
Datastore's documentation reserves every kind beginning with `__`, which the emulator does not enforce, so such a kind is left to the service.

The PRIMARY KEY is the entity key's last path element, declared `PRIMARY KEY (...) NOT ENFORCED` on one column.
A `STRING` key column is the key's name and a `BIGINT` one its numeric id; it is never stored as a property.
A row whose key is an empty name or the id `0` fails the job, because neither addresses an entity, and so does a name the service refuses: one of the form `__…__` or longer than 1,500 bytes.
The key column is `NOT NULL`, so Flink's not-null enforcer handles a NULL key before the sink sees it, failing the job or, under `table.exec.sink.not-null-enforcer = 'DROP'`, dropping the row.
The sink writes keys without ancestors, which are deferred until a use asks for them; [Reading values back](#reading-values-back) says how a scan meets a key that has one.

{{< sql-snippet file="flink/DatastoreTableReference.sql" tag="id-key" >}}

| Flink type | Datastore value |
|---|---|
| `STRING`, `VARCHAR`, `CHAR` | String |
| `BIGINT` | Integer |
| `DOUBLE` | Floating-point number |
| `BOOLEAN` | Boolean |
| `BYTES`, `VARBINARY`, `BINARY` | Blob |
| `TIMESTAMP_LTZ(p)` | Timestamp from year 1 to 9999; Datastore keeps microseconds and rounds a finer value down |
| `ARRAY<T>` | Array; an array directly inside an array is refused, as Datastore stores none |
| `ROW<...>` | Embedded entity without a key, one property per field |

Every other Flink type is refused when the statement is planned, naming the field: write an `INT` as a `BIGINT` and a `FLOAT` as a `DOUBLE`.
`DECIMAL`, `DATE` and `MAP` have no mapping, and neither do a geographical point or a key value.
A SQL `NULL` is written as a Datastore null value, so the property exists and holds null.

### Reading values back

A scan reads each column from the top-level property of its name, and the PRIMARY KEY column from the entity's key: a `STRING` key column from the key's name and a `BIGINT` one from its numeric id.
An entity whose key has the other form fails the read whatever the policy below, because the key column is `NOT NULL`, and it does so whether or not the query reads the key column; a kind that holds both forms is read by a table without a PRIMARY KEY, through the `key-name` and `key-id` metadata.
A scan of a kind also returns the kind's entities whose keys have a parent, and the key column holds only the last path element, so children of two parents could read as one key, which the planner trusts to be unique.
Such an entity fails the read of a table with a PRIMARY KEY, also when the query does not read the key column, which the planner prunes from a `GROUP BY` over the key; a table without one reads it, and its `key-name` and `key-id` name the last path element alone.
A property the entity does not have reads as NULL, as does a property that holds null, and so does a field the embedded entity of a `ROW` column lacks.
An integer reads into a `DOUBLE` column only when the `DOUBLE` represents it exactly, up to `2^53` in magnitude, and a timestamp reads at the column's precision, truncated.
A string or blob reads whole, whatever length a `CHAR`, `VARCHAR`, `BINARY` or `VARBINARY` column declares: the scan neither checks the length nor pads a fixed-length value, so declare `STRING` and `BYTES` for values of any length.

A kind has no schema, so an entity can hold, under a column's name, a value of another type: a string where the column is `BIGINT`, or a value outside the mapping, such as a geographical point or a key.
`type-mismatch-policy` decides what such a value does.
Under `fail` the read fails, naming the entity's key and the property's path in the schema.
Under `null` the innermost nullable field around the value reads as NULL instead; a `NOT NULL` field cannot, so the nearest nullable field around it does.
A `NOT NULL` column has no nullable field around it, so a mismatched or missing value there fails the read under either policy.

## Unindexed columns

Datastore indexes every property value unless the value is excluded, and it refuses an indexed string or blob longer than 1,500 bytes.
`sink.unindexed-columns` names the top-level columns whose values the sink writes excluded from indexes, as a semicolon-separated list.
An excluded string or blob can be far longer, within Datastore's [limits](https://cloud.google.com/datastore/docs/concepts/limits) for an unindexed value and for the entity's 1 MiB, but a query cannot filter or sort on it.

{{< sql-snippet file="flink/DatastoreTableReference.sql" tag="unindexed" >}}

The exclusion covers everything inside the column.
An `ARRAY` column's elements are each excluded, because Datastore refuses the exclusion on an array value itself, and a `ROW` column's embedded entity is excluded with every value nested in it.
A column the table does not declare, a nested path such as `meta.note`, and the PRIMARY KEY are refused when the statement is planned.
Without the option, a value over the limit is refused by the service with `INVALID_ARGUMENT`, which fails the job, naming the entity's key and the property.
A restart replays the same row and fails again, so the remedy is to add the column to `sink.unindexed-columns` or to change the data.

## Source

A scan reads every entity of the table's kind in its namespace, as the [Datastore-mode source]({{< relref "docs/connectors/datastream/firestore" >}}#reading-in-datastore-mode) reads a kind: cut into key ranges read in parallel, at one read time.
`scan.partition.max-partitions` asks for at most that many key ranges, which the planner otherwise estimates from the kind's statistics, and `scan.read-time` fixes the read time, which is otherwise the service's when the read is planned.
Datastore takes a read time of microsecond precision within the past hour, or on a whole minute within the past seven days with point-in-time recovery; the read fails when it is planned on the JobManager otherwise, so the instant in the example below is one to replace.

{{< sql-snippet file="flink/DatastoreTableReference.sql" tag="scan" >}}

A scan reads whole entities and converts only the columns it produces.
A column projection is not sent to the service as a Datastore projection query, because such a query returns only the entities that hold an indexed value of every projected property, and one result per value of an array property: a column written through `sink.unindexed-columns` would read as no row at all.
Filters are not pushed down either.

A scan can read six metadata columns:

| Key | Type | Meaning |
|---|---|---|
| `key-name` | `STRING` | The key's name, NULL when the key has a numeric id |
| `key-id` | `BIGINT` | The key's numeric id, NULL when the key has a name |
| `version` | `BIGINT NOT NULL` | The entity's version, which grows with every change to the entity |
| `create-time` | `TIMESTAMP_LTZ(6) NOT NULL` | When the entity was created |
| `update-time` | `TIMESTAMP_LTZ(6) NOT NULL` | When the entity was last changed |
| `read-time` | `TIMESTAMP_LTZ(6) NOT NULL` | The time the entity was read at, the scan's one read time |

Each time is truncated to microseconds, the type's precision.
A query result carries the version and both times of every entity it returns, and the read fails, naming the entity's key and the metadata, if a result lacks one that a column reads.

A table without a PRIMARY KEY reads the ids the service allocated for its rows through `key-id`:

{{< sql-snippet file="flink/DatastoreTableReference.sql" tag="key-less-read" >}}

The [DataStream source]({{< relref "docs/connectors/datastream/firestore" >}}#reading-in-datastore-mode) describes the read-time window, the split count's estimate, paging and recovery.

## Sink

Every write is an `upsert` or a `delete`.
An insert or an update writes the row's entity whole, replacing whatever the key held, so properties the table does not declare are removed; a delete deletes the entity, and deleting a missing entity succeeds.
The table offers no `insert` or `update` operation: a replay after a restart would meet `ALREADY_EXISTS` for an `insert`, or `NOT_FOUND` for an `update` of an entity deleted in between, and fail the job again on every restart.

A table with a PRIMARY KEY consumes an upsert changelog: inserts, updates after and deletes, keyed by the entity key.
On Flink 2.x a delete may carry the key alone, which is all the sink reads from it.
On Flink 2.2 and 2.3, Flink's not-null enforcer also checks the other columns of such a delete and fails the job when one of them is `NOT NULL`, a Flink bug fixed in 2.4.0 ([FLINK-40477](https://issues.apache.org/jira/browse/FLINK-40477)); declare the columns besides the key nullable when the input's deletes carry the key alone.
One sink subtask applies the writes to one key in the order it receives them, because it commits one request at a time and never puts a key in a request twice; the [DataStream sink]({{< relref "docs/connectors/datastream/firestore" >}}#how-the-datastore-sink-writes) names the one exception, an attempt that timed out on the client and is applied late.
The rows of one key reach one subtask when Flink shuffles the input by the PRIMARY KEY, which it does for an upsert changelog it materializes and when the sink's parallelism differs from the input's; an insert-only input carrying several rows per key at the same parallelism is not shuffled unless `table.exec.sink.keyed-shuffle` is `FORCE`, so its rows of one key can be written by different subtasks in either order.

A table without a PRIMARY KEY accepts inserts only, and writes each row as an `upsert` under a numeric id the service allocated (`AllocateIds`).
The service does not hand out an id it allocated again, so such a row cannot replace an entity written under another allocated id; each sink subtask fetches ids `sink.id-allocation.batch-size` at a time, through the same client, credentials, request timeout and recovery budget as its commits, and an allocation the service refuses, or the budget cannot finish, fails the job.
The guarantee covers allocated ids only: Datastore does not keep an allocated id from landing on an entity stored under a numeric id someone chose, so do not write a table without a PRIMARY KEY into a kind and namespace that also hold entities under self-chosen numeric ids, such as those of a `BIGINT`-keyed table.
Allocating ids takes the `datastore.entities.allocateIds` permission, which `roles/datastore.user` carries.

{{< sql-snippet file="flink/DatastoreTableReference.sql" tag="append" >}}

A refused write fails the job: a `WITH` clause cannot name a serializable failure handler, so the DataStream sink's routing of refused writes to one is not available, and neither is `maxConsecutiveRejections`, which only bounds a handler that drops them.

## Options

| Option | Default | What it does |
|---|---|---|
| `project` | **required** | The Google Cloud project containing the database |
| `database` | *unset ⇒ the default database* | The id of the Firestore database in Datastore mode |
| `kind` | **required** | The kind of the table's entities |
| `namespace` | *unset ⇒ the default namespace* | The namespace of the table's entities |
| `emulator-endpoint` | *unset ⇒ the real service* | `host:port` of a Firestore emulator started in Datastore mode; setting it also stops credential discovery |
| `service-account-key-file` | *unset ⇒ ADC for the real service* | Service-account JSON key-file path, read by the scan's JobManager and readers and by each sink subtask; rejected with `emulator-endpoint` |
| `type-mismatch-policy` | `fail` | What a read does with a stored value whose type does not match its column: `fail` the read, or read the field as `null` |
| `scan.partition.max-partitions` | *unset ⇒ estimated from the kind's statistics, at least the larger of 12 and the scan's parallelism* | Maps to `splitCount`: the key ranges a scan asks the splitter for, at most `50000` |
| `scan.read-time` | *unset ⇒ the service's time when the read is planned* | Maps to `readTime`: an ISO-8601 instant such as `2026-10-04T00:00:00Z`, within the past hour, or a whole minute within seven days with point-in-time recovery |
| `scan.max-rows-per-fetch` | `500` | Maps to `pageSize`: the entities one request asks for |
| `scan.parallelism` | *unset ⇒ the planner's parallelism* | Flink's standard source parallelism override |
| `sink.unindexed-columns` | empty | Semicolon-separated top-level columns written excluded from indexes, with every value nested in them |
| `sink.buffer-flush.max-mutations` | `500` | Maps to `maxBatchMutations`: the mutations in one commit |
| `sink.buffer-flush.max-size` | `9000000 bytes` | Maps to `maxBatchBytes`: the protobuf size of one commit request, at most `10 mb` |
| `sink.request-timeout` | `60 s` | Maps to `requestTimeout`: the timeout of one commit attempt, or of one id allocation for a table without a PRIMARY KEY |
| `sink.recovery.initial-backoff` | `500 ms` | Maps to `recoveryInitialBackoff`, at most `sink.recovery.max-backoff` |
| `sink.recovery.max-backoff` | `10 s` | Maps to `recoveryMaxBackoff`, at least `sink.recovery.initial-backoff` |
| `sink.recovery.max-attempts` | `10` | Maps to `recoveryMaxAttempts`: the attempts at one commit, the first included |
| `sink.throttling.enabled` | `true` | Maps to `throttlingEnabled`: whether the writer follows Datastore's ramp-up guidance |
| `sink.id-allocation.batch-size` | `1000` | Maps to `idAllocationBatchSize`: the ids one `AllocateIds` call fetches, for a table without a PRIMARY KEY |
| `sink.throttling.parallelism` | *unset ⇒ the sink's parallelism* | Maps to `throttlingParallelism`: how many subtasks share the ramp-up's starting 500 operations per second |
| `sink.parallelism` | *unset ⇒ the input's parallelism* | Flink's standard sink parallelism override |

The [configuration reference]({{< relref "docs/reference/firestore" >}}#datastorewriteroptions) explains each writer option, and a refusal of a `sink.*` or `scan.*` value names the option key the `WITH` clause spells.
The pair `sink.recovery.initial-backoff` and `sink.recovery.max-backoff` is checked over the values the writer would use, so setting one compares it with the other's default.

## Delivery guarantee

A scan restored from a checkpoint resumes each split just after the last entity it passed, at the same read time; a job that restarts before its first checkpoint plans again, at a new read time unless `scan.read-time` is set.

The sink is at-least-once: it commits everything it holds at each checkpoint, and a restart replays the records after the last completed one.
With a PRIMARY KEY, an `upsert` and a `delete` are idempotent, so a replayed row writes the same entity again.
Without a PRIMARY KEY, a replayed row writes another entity under a newly allocated id, so a restart can leave duplicates; it replaces no entity written under an allocated id.
See the [DataStream delivery guarantee]({{< relref "docs/connectors/datastream/firestore" >}}#delivery-guarantee-in-datastore-mode).

## Design decisions and testing

The table mapping and the declined alternatives are recorded in [ADR-0184]({{< param BookRepo >}}/blob/main/docs/adr/0184-the-datastore-table-sink-upserts-one-kind-keyed-by-name-or-id.md).
The emulator integration tests, against the Firestore emulator in Datastore mode, write every type through SQL and read the entities back with the client library, apply an upsert changelog in order, write a numeric id into a namespace, store a timestamp to the microsecond, and contrast an over-long string in indexed and unindexed columns at the top level, as an array element and inside a `ROW`, which the emulator refuses and stores as the service documents.
They also read every type back through a SQL scan with its metadata, read the allocated ids of a table without a PRIMARY KEY, fail and read as NULL a mismatched value under each policy, keep every entity under a column projection, and read at a configured read time.
The uber-jar's tests write through its relocated classes.
