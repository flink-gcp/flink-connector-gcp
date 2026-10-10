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

The `datastore` connector writes Table API and SQL rows as the entities of one kind, in one namespace of a Firestore database in Datastore mode, through `flink-connector-gcp-firestore`.
It maps onto the [Datastore-mode DataStream sink]({{< relref "docs/connectors/datastream/firestore" >}}#datastore-mode), so batching, recovery, ramp-up, delivery, metrics and failure behavior remain the same.
A table with a PRIMARY KEY writes each row under that key's name or numeric id; a table without one writes every row as a new entity.

{{< sql-snippet file="flink/DatastoreTableReference.sql" tag="overview" >}}

The connector is a sink only for now: a scan is [#1652]({{< param BookRepo >}}/issues/1652) and a lookup join [#1653]({{< param BookRepo >}}/issues/1653).
A database in Native mode uses the [`firestore` connector]({{< relref "docs/connectors/table/firestore" >}}) instead.

Use `flink-sql-connector-gcp-firestore`, the relocated SQL uber-jar, for SQL deployments, and place it in Flink's `lib/` before starting the cluster.
The same jar carries the `firestore` and `datastore` connectors; the [Firestore SQL connector]({{< relref "docs/connectors/table/firestore" >}}) page describes how to obtain it.

## Lineage

The Table sink reports its configured kind in the `gcp` facet of one logical SQL dataset, named by the table's catalog identifier.
The kind is a `datastore-kind` resource: namespace `datastore://{project}/{database}`, with the default database spelled `(default)`, followed by `/{namespace}` outside the default namespace, and the kind as its name.
This is the resource the [Datastore-mode source]({{< relref "docs/connectors/datastream/firestore" >}}#lineage) reports for the same kind.
See [Lineage]({{< relref "docs/connectors/lineage" >}}) for the class loader configuration and what Flink 1.20 and 2.x each deliver.

## Credentials

`service-account-key-file` selects one service-account JSON key for the sink, read by each sink subtask on its TaskManager, so mount the same path in every container.
Without it and without `emulator-endpoint`, the sink uses Application Default Credentials, and the identity needs the role the [DataStream credentials]({{< relref "docs/connectors/datastream/firestore" >}}#credentials) name.
The option is mutually exclusive with `emulator-endpoint`, because the emulator channel carries no credentials.
Only `emulator-endpoint` reaches an emulator, one started with `--database-mode=datastore-mode`: the sink always gives its client a host, so `DATASTORE_EMULATOR_HOST` is never read.

## Keys and type mapping

A table names one kind with `kind`, in the namespace `namespace` names or in the default namespace without it.
Each row is one entity, and each column other than the PRIMARY KEY is one property of the entity, named exactly as the column is.
A kind or a namespace that every write would be refused for is refused when the statement is planned: one of the form `__…__` with at least one character between the underscores, such as `__Stat_Kind__`, which the emulator refuses as reserved; a kind longer than 1,500 bytes; and a namespace outside the client library's grammar of at most 100 letters, digits, `.`, `_` and `-`.
So are a column or `ROW` field with such a name or one longer than 1,500 bytes, wherever it is declared, and a `project` outside the client library's grammar for a project id.
A blank `kind` or `namespace` is refused too; leave `namespace` out for the default namespace.
Datastore's documentation reserves every kind beginning with `__`, which the emulator does not enforce, so such a kind is left to the service.

The PRIMARY KEY is the entity key's last path element, declared `PRIMARY KEY (...) NOT ENFORCED` on one column.
A `STRING` key column is the key's name and a `BIGINT` one its numeric id; it is never stored as a property.
A row whose key is an empty name or the id `0` fails the job, because neither addresses an entity, and so does a name the service refuses: one of the form `__…__` or longer than 1,500 bytes.
The key column is `NOT NULL`, so Flink's not-null enforcer handles a NULL key before the sink sees it, failing the job or, under `table.exec.sink.not-null-enforcer = 'DROP'`, dropping the row.
Keys have no ancestors, which are deferred until a use asks for them.

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
| `service-account-key-file` | *unset ⇒ ADC for the real service* | Service-account JSON key-file path, read by each sink subtask; rejected with `emulator-endpoint` |
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

The [configuration reference]({{< relref "docs/reference/firestore" >}}#datastorewriteroptions) explains each writer option, and a refusal of a `sink.*` value names the option key the `WITH` clause spells.
The pair `sink.recovery.initial-backoff` and `sink.recovery.max-backoff` is checked over the values the writer would use, so setting one compares it with the other's default.

## Delivery guarantee

The sink is at-least-once: it commits everything it holds at each checkpoint, and a restart replays the records after the last completed one.
With a PRIMARY KEY, an `upsert` and a `delete` are idempotent, so a replayed row writes the same entity again.
Without a PRIMARY KEY, a replayed row writes another entity under a newly allocated id, so a restart can leave duplicates; it replaces no entity written under an allocated id.
See the [DataStream delivery guarantee]({{< relref "docs/connectors/datastream/firestore" >}}#delivery-guarantee-in-datastore-mode).

## Design decisions and testing

The table mapping and the declined alternatives are recorded in [ADR-0184]({{< param BookRepo >}}/blob/main/docs/adr/0184-the-datastore-table-sink-upserts-one-kind-keyed-by-name-or-id.md).
The emulator integration tests, against the Firestore emulator in Datastore mode, write every type through SQL and read the entities back with the client library, apply an upsert changelog in order, write a numeric id into a namespace, store a timestamp to the microsecond, and contrast an over-long string in indexed and unindexed columns at the top level, as an array element and inside a `ROW`, which the emulator refuses and stores as the service documents.
The uber-jar's tests write through its relocated classes.
