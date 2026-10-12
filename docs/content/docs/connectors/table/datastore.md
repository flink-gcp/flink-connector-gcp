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
It maps onto the [Datastore-mode DataStream source and sink]({{< relref "docs/connectors/datastream/firestore" >}}#datastore-mode), so splitting, snapshot, paging, recovery, batching, ramp-up, delivery, metrics and failure behavior remain the same; a lookup join reads through its own client, with the retries its section describes.
A table with a PRIMARY KEY writes each row under that key's name or numeric id; a table without one writes every row as a new entity.

{{< sql-snippet file="flink/DatastoreTableReference.sql" tag="overview" >}}

A scan reads the kind as a bounded snapshot, and a lookup join reads one entity of it by key.
A database in Native mode uses the [`firestore` connector]({{< relref "docs/connectors/table/firestore" >}}) instead.

Use `flink-sql-connector-gcp-firestore`, the relocated SQL uber-jar, for SQL deployments, and place it in Flink's `lib/` before starting the cluster.
The same jar carries the `firestore` and `datastore` connectors; the [Firestore SQL connector]({{< relref "docs/connectors/table/firestore" >}}) page describes how to obtain it.

## Lineage

The Table source and sink report their configured kind in the `gcp` facet of one logical SQL dataset, named by the table's catalog identifier.
The kind is a `datastore-kind` resource: namespace `datastore://{project}/{database}`, with the default database spelled `(default)`, followed by `/{namespace}` outside the default namespace, and the kind as its name.
This is the resource the [Datastore-mode source]({{< relref "docs/connectors/datastream/firestore" >}}#lineage) reports for the same kind.
Lookup joins are outside this extraction path and report no resource.
See [Lineage]({{< relref "docs/connectors/lineage" >}}) for the class loader configuration and what Flink 1.20 and 2.x each deliver.

## Credentials

`service-account-key-file` selects one service-account JSON key for the scan, the lookup and the sink.
The option stores only the path in the job graph: a scan reads the file on the JobManager, where it plans the read, and on each TaskManager that reads a split, while each lookup and sink subtask reads it on its TaskManager, so mount the same path in every container.
Without it and without `emulator-endpoint`, all three use Application Default Credentials, and the identity needs the role the [DataStream credentials]({{< relref "docs/connectors/datastream/firestore" >}}#credentials) name.
The option is mutually exclusive with `emulator-endpoint`, because the emulator channel carries no credentials.
Only `emulator-endpoint` reaches an emulator, one started with `--database-mode=datastore-mode`: none of the three reads `DATASTORE_EMULATOR_HOST`.

## Keys and type mapping

A table names one kind with `kind`, in the namespace `namespace` names or in the default namespace without it.
Each row is one entity, and each column other than the PRIMARY KEY is one property of the entity, named exactly as the column is.
A kind or a namespace that every write would be refused for is refused when the statement is planned, for a scan as well as the sink, except that a scan reads a statistics kind: one of the form `__…__` with at least one character between the underscores, such as `__kind__`, which the emulator refuses as reserved; a kind longer than 1,500 bytes; and a namespace outside the client library's grammar of at most 100 letters, digits, `.`, `_` and `-`.
So are a column or `ROW` field with such a name or one longer than 1,500 bytes, wherever it is declared, and a `project` outside the client library's grammar for a project id.
A reserved kind holds Datastore's own statistics or metadata.
The metadata kinds `__namespace__`, `__kind__` and `__property__` are refused by the DataStream source the scan reads through as well ([Reading in Datastore mode]({{< relref "docs/connectors/datastream/firestore" >}}#reading-in-datastore-mode)).
A statistics kind, whose name begins `__Stat_`, such as `__Stat_Kind__`, is read by the scan and the lookup as the DataStream source reads it ([Reading in Datastore mode]({{< relref "docs/connectors/datastream/firestore" >}}#reading-in-datastore-mode)); declare the properties to read, named as Google's [statistics](https://cloud.google.com/datastore/docs/concepts/stats) page lists them, and a STRING key: every statistics entity measured, 28 of 12 statistics kinds across two namespaces, was keyed by a name, such as the kind an entry describes or `total_entity_usage`, and a scan with a STRING key fails on an entity keyed by an id.
A blank `kind` or `namespace` is refused too; leave `namespace` out for the default namespace.
Datastore's documentation reserves every kind beginning with `__`, which the emulator does not enforce, so such a kind is left to the service.

The PRIMARY KEY is the entity key's last path element, declared `PRIMARY KEY (...) NOT ENFORCED` on one column.
A `STRING` key column is the key's name and a `BIGINT` one its numeric id; it is never stored as a property.
A row whose key is an empty name or the id `0` fails the job, because neither addresses an entity, and so does a name the service refuses: one of the form `__…__` or longer than 1,500 bytes.
The key column is `NOT NULL`, so Flink's not-null enforcer handles a NULL key before the sink sees it, failing the job or, under `table.exec.sink.not-null-enforcer = 'DROP'`, dropping the row.
The sink writes each row's own key without ancestors, which are deferred until a use asks for them; [Reading values back](#reading-values-back) says how a scan meets a key that has one.

{{< sql-snippet file="flink/DatastoreTableReference.sql" tag="id-key" >}}

| Flink type | Datastore value |
|---|---|
| `STRING`, `VARCHAR`, `CHAR` | String |
| `BIGINT` | Integer |
| `DOUBLE` | Floating-point number |
| `BOOLEAN` | Boolean |
| `BYTES`, `VARBINARY`, `BINARY` | Blob |
| `TIMESTAMP_LTZ(p)` | Timestamp from year 1 to 9999; Datastore keeps microseconds and rounds a finer value down |
| `ARRAY<T>` | Array; an array directly inside an array is refused, because the client library neither builds nor reads one, although the service stored one sent as a raw commit |
| `ROW<...>` | Embedded entity without a key, one property per field |
| `ROW<latitude DOUBLE, longitude DOUBLE>` named by `geo-point-field-paths` | Geographical point |
| `ROW<kind STRING, name STRING, id BIGINT>` named by `reference-field-paths`, optionally followed by `ancestors ARRAY<ROW<kind STRING, name STRING, id BIGINT>>` | Key value in the table's project, database and namespace |

Every other Flink type is refused when the statement is planned, naming the field: write an `INT` as a `BIGINT` and a `FLOAT` as a `DOUBLE`.
`DECIMAL`, `DATE` and `MAP` have no mapping.
A SQL `NULL` is written as a Datastore null value, so the property exists and holds null.

A geographical point and a key value share their Flink types with an ordinary embedded entity, so the table names them with two markers, each a semicolon-separated list of field paths.
A path names a column, then a ROW field by `.name`; an `ARRAY` is passed through, so a path through an array names its elements, and an array of key values is an `ARRAY` of the key's `ROW`.
A marker that names a path the table does not declare, a field of the wrong type, the PRIMARY KEY, or a path two fields share (a column named `a.b` beside a ROW `a` with a field `b`), is refused when the statement is planned, as are both markers on one path.

In the example below, each `Delivery` entity holds its destination, a key value naming its customer and a list of key values naming its items.
The customers are entities of their own kind, so a second table reads them, and the query follows each delivery's reference by joining on the key value's name.

{{< sql-snippet file="flink/DatastoreTableReference.sql" tag="markers" >}}

A key value's `ROW` holds the key's last path element: its `kind`, and exactly one of its `name` and its numeric `id`, the other NULL.
The kind is a value of each row, not part of the table's declaration, so one column can hold keys of several kinds, which is why the query above filters on `d.customer.kind`.
Datastore has no foreign keys, and nothing checks that the entity a key value names exists: a reference to a missing customer simply joins no row.
A key whose entity has a parent carries its ancestors too, and a column that reads or writes such keys adds an `ancestors` field after the three: `ROW<kind STRING, name STRING, id BIGINT, ancestors ARRAY<ROW<kind STRING, name STRING, id BIGINT>>>`, root first; without the field, the column holds keys without a parent.
The sink builds the key in the table's own project, database and namespace.
The service refused a key value in another database than its entity's, and one naming a project that does not exist.
The namespace is the table's because the `ROW` has no field to name another, although the service stores a key value in another namespace.
A path element without a kind, with neither or both of a name and an id, with an empty name or with the id `0`, or a NULL ancestor, fails the job, and so does a point whose latitude or longitude is NULL or out of range.
On Flink 1.20, an `ARRAY` literal of key `ROW`s that hold NULL in different fields, such as `ARRAY[ROW('User', 'u1', CAST(NULL AS BIGINT)), ROW('User', CAST(NULL AS STRING), 9)]`, fails planning with "Conversion to relational algebra failed to preserve datatypes"; casting each element to the column's `ROW` type, `CAST(ROW(...) AS ROW<kind STRING, name STRING, id BIGINT>)`, plans on both supported Flink lines.

### Reading values back

A scan reads each column from the top-level property of its name, and the PRIMARY KEY column from the entity's key: a `STRING` key column from the key's name and a `BIGINT` one from its numeric id.
An entity whose key has the other form fails the read whatever the policy below, because the key column is `NOT NULL`, and it does so whether or not the query reads the key column; a kind that holds both forms is read by a table without a PRIMARY KEY, through the `key-name` and `key-id` metadata.
A scan of a kind also returns the kind's entities whose keys have a parent, and the key column holds only the last path element, so children of two parents could read as one key, which the planner trusts to be unique.
Such an entity fails the read of a table with a PRIMARY KEY, also when the query does not read the key column, which the planner prunes from a `GROUP BY` over the key; a table without one reads it, and its `key-name` and `key-id` name the last path element alone.
A property the entity does not have reads as NULL, as does a property that holds null, and so does a field the embedded entity of a `ROW` column lacks.
An integer reads into a `DOUBLE` column only when the `DOUBLE` represents it exactly, up to `2^53` in magnitude, and a timestamp reads at the column's precision, truncated.
A geographical point reads into a ROW `geo-point-field-paths` names, and a key value into a ROW `reference-field-paths` names, its `ancestors` an empty array for a key without a parent.
A string or blob reads whole, whatever length a `CHAR`, `VARCHAR`, `BINARY` or `VARBINARY` column declares: the scan neither checks the length nor pads a fixed-length value, so declare `STRING` and `BYTES` for values of any length.

A kind has no schema, so an entity can hold, under a column's name, a value of another type: a string where the column is `BIGINT`, a point or a key value where the column is an unmarked `ROW`, a key value with ancestors where the marked `ROW` declares no `ancestors` field, a key value with an id where the `ROW` declares its `name` `NOT NULL` (or the reverse), or a key value in another namespace, which the service stores although the `ROW` has no field to say so.
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
A `scan.read-time` finer than a microsecond is truncated to it: Datastore takes only a microsecond-precision read time, so a finer instant could never be read as given, and truncation picks the latest microsecond at or before it.
The Datastore API does not document the precision of an entity's update time; on the emulator and on the service it is a whole microsecond, so the truncated time sees every write made at or before the instant given.
The read time must lie within the past hour, or on a whole minute within the past seven days with point-in-time recovery; the read fails when it is planned on the JobManager otherwise, so the instant in the example below is one to replace.

{{< sql-snippet file="flink/DatastoreTableReference.sql" tag="scan" >}}

A scan reads whole entities and converts only the columns it produces.
A column projection is not sent to the service as a Datastore projection query, because such a query returns only the entities that hold an indexed value of every projected property, and one result per value of an array property: a column written through `sink.unindexed-columns` would read as no row at all.
Filters are not pushed down either.

A scan or a lookup can read six metadata columns:

| Key | Type | Meaning |
|---|---|---|
| `key-name` | `STRING` | The key's name, NULL when the key has a numeric id |
| `key-id` | `BIGINT` | The key's numeric id, NULL when the key has a name |
| `version` | `BIGINT NOT NULL` | The entity's version, which grows with every change to the entity |
| `create-time` | `TIMESTAMP_LTZ(6) NOT NULL` | When the entity was created |
| `update-time` | `TIMESTAMP_LTZ(6) NOT NULL` | When the entity was last changed |
| `read-time` | `TIMESTAMP_LTZ(6) NOT NULL` | The time the entity was read at: the scan's one read time, or the time of a lookup's read |

Each time is truncated to microseconds, the type's precision.
A query result or a lookup's answer carries the version and both times of every entity it returns, and a lookup's answer its read time; the read fails, naming the entity's key and the metadata, if an answer lacks one that a column reads.

A table without a PRIMARY KEY reads the ids the service allocated for its rows through `key-id`:

{{< sql-snippet file="flink/DatastoreTableReference.sql" tag="key-less-read" >}}

The [DataStream source]({{< relref "docs/connectors/datastream/firestore" >}}#reading-in-datastore-mode) describes the read-time window, the split count's estimate, paging and recovery.

## Lookup

A lookup join reads one entity of the table's kind, in its namespace, by the PRIMARY KEY column: the key's name for a `STRING` key column, its numeric id for a `BIGINT` one.
It also accepts equality keys on other top-level physical scalar columns, whether a constant such as `c.tier = 'gold'` appears in `ON` or `WHERE`, or an input column supplies the comparison value.
Only the entity key addresses the read; the additional keys compare against the converted Flink row before it is returned, with binary values compared by content.
Nested key paths, metadata columns, and additional `ARRAY`, `MAP`, or `ROW` keys are rejected during planning.
A NULL lookup key or a failed additional equality joins no row, and a condition that is not an equality, such as `c.tier <> 'gold'`, stays a filter applied after the lookup.
A table without a PRIMARY KEY cannot be looked up.

{{< sql-snippet file="flink/DatastoreTableReference.sql" tag="lookup" >}}

An entity that does not exist joins no row, so a `LEFT JOIN` fills its columns with NULL.
The lookup reads a key without a parent, so an entity whose key has one is never found, even when its last path element equals the lookup key.
A key that can address no entity joins no row without a read: an empty name, the id `0`, a name of the form `__…__` with at least one character between the underscores, and a name longer than 1,500 bytes.
The emulator refuses a lookup of an empty name, the id `0` or an over-long name with `INVALID_ARGUMENT`, which would fail the job for one stream value if the key were sent.
Every other key is sent to the service as it is, a negative id included: Datastore stores an entity under a negative id.

A lookup asks Datastore only for the properties of the columns the join uses, through the `Lookup` request's property mask, and for the key alone when the join uses only the key and metadata columns.
Unlike the projection query the [scan](#source) declines, the mask returns an entity whatever its indexes, and an array property whole.
The lookup fills the metadata columns from the answer: `version`, `create-time` and `update-time` from the entity it found, and `read-time` from the time of that read.
`type-mismatch-policy` applies to a looked-up value as it does to a scanned one.
A lookup reads strongly, the entity as it is when the request reaches the service, and under a `PARTIAL` cache a hit returns the row as it was cached until it expires; `scan.read-time` and the other `scan.*` options do not apply to it.

`lookup.async` runs the join as Flink's asynchronous lookup, with several reads in flight per subtask, instead of waiting for each read; both modes send one `Lookup` request per key through the same generated client, one per lookup subtask.
Flink's `lookup.cache` modes `NONE` and `PARTIAL` are supported, and Flink owns the partial cache; `FULL` is refused, because a full cache is loaded by a scan with a snapshot and a reload contract of its own.
The cache stores the filtered result under the complete lookup-key tuple, so different additional values for the same key have separate entries.
An additional equality that fails follows the configured missing-key cache policy; expiry and size options keep their standard Flink meaning.
`lookup.max-retries` reads again after `UNAVAILABLE` or `DEADLINE_EXCEEDED`, the statuses the client library already retries `Lookup` on, so it adds attempts on top of the library's rather than retrying anything the library does not; it counts retries after the first read, and every other failure fails the join at once.
Datastore can also defer a key for lack of resources and read nothing.
A deferred key is sent again at once, within the same `lookup.max-retries` budget, and a key still deferred when the budget runs out fails the join, because joining no row would hide an entity that may exist.
A lookup needs the `datastore.entities.get` permission, which `roles/datastore.user` and `roles/datastore.viewer` both carry.

## Sink

With a PRIMARY KEY, `sink.write-mode` says what an insert or an update does to the entity:

- **`merge`**, the default, merges the row into the entity, creating it if it is missing: each column replaces the property of its name, properties the table does not declare keep their values, and a `ROW` column is merged into the stored embedded entity property by property, at any depth. An `ARRAY` and a `NULL` replace the stored property, and so does a `ROW` a marker names, a geographical point or a key value being one value, and a `ROW` written where the entity holds a value of another type (measured on the emulator). The write is an `upsert` whose property mask names every value the row holds.
- **`replace`** writes the row's entity whole, replacing whatever the key held, so properties the table does not declare are removed.
- **`update`** replaces the table's properties of an existing entity, each one whole, a `ROW` column included, and properties the table does not declare keep their values; a missing entity fails the job, and nothing is created. The write is an `update` whose property mask names the row's columns other than the key.
  An `update` table takes inserts and updates but no deletes, and an input that carries deletes is refused when the statement is planned: an update replayed after a restart that meets an entity a later delete removed would fail the job again on every restart.
  The planner can still send a delete when the query's key differs from the table's PRIMARY KEY, as in an aggregation written into a table keyed by its result, because it then materializes the upsert and turns a retraction into a delete; such a delete fails the job without deleting anything, and the message says to key the query by the PRIMARY KEY or to use `merge` or `replace`.
  Flink 2.3 refuses such a statement while planning it, so nothing reaches the sink, as [Flink 2.3 may demand ON CONFLICT](#flink-23-may-demand-on-conflict) explains; with `ON CONFLICT DO DEDUPLICATE` or `table.exec.sink.require-on-conflict = false`, the delete reaches the sink and fails the job as above without deleting anything (measured on the emulator).
  A table that declares only its PRIMARY KEY has no property to update, and `update` is refused on it.

For a table `(id STRING, tier STRING, address ROW<city STRING>)` whose key `c-1` holds `{name: "Alice", tier: "silver", address: {city: "Tokyo", zip: "100"}}`, an entity's properties written here as JSON-like shorthand, the row `('c-1', 'gold', ROW('Osaka'))` leaves:

| `sink.write-mode` | The key holds afterwards |
|---|---|
| `merge` | `{name: "Alice", tier: "gold", address: {city: "Osaka", zip: "100"}}` |
| `replace` | `{tier: "gold", address: {city: "Osaka"}}` |
| `update` | `{name: "Alice", tier: "gold", address: {city: "Osaka"}}` |

`name`, which the table does not declare, survives `merge` and `update`; `zip`, inside the `ROW`, survives only `merge`.
Had `c-1` held no entity, `merge` and `replace` would create it and `update` would fail the job.

Under `merge` and `replace`, a delete deletes the entity, and deleting a missing entity succeeds.
The default keeps what other writers store in the kind: a job that maintains some properties of entities another application also writes leaves that application's properties in place, and so does a table that declares fewer properties than the entities hold.
Choose `replace` when the row is the whole entity, so that a property the row no longer carries does not stay stored.
Under `merge`, a table that declares only its PRIMARY KEY creates a missing entity, without properties, and leaves a stored one as it was.

{{< sql-snippet file="flink/DatastoreTableReference.sql" tag="replace" >}}

Choose `update` when the job must change only entities that already exist, never create one:

{{< sql-snippet file="flink/DatastoreTableReference.sql" tag="update" >}}

Under `merge`, a value the sink writes excluded from indexes (see [Unindexed columns](#unindexed-columns)) carries the mark, while a value the row does not write keeps its own: measured on the emulator, merging a `ROW` into an indexed embedded entity left the entity and its undeclared properties indexed.
The table offers no `insert` operation: a replay after a restart would meet `ALREADY_EXISTS` and fail the job again on every restart.

A table with a PRIMARY KEY consumes an upsert changelog: inserts, updates after and, except under `update`, deletes, keyed by the entity key.
On Flink 2.x a delete may carry the key alone, which is all the sink reads from it.
On Flink 2.2 and 2.3, two Flink bugs fixed in 2.4.0 affect such a delete, whose non-key columns are null.
Flink's not-null enforcer checks those columns and fails the job when the table declares one of them `NOT NULL` ([FLINK-40477](https://issues.apache.org/jira/browse/FLINK-40477)); setting `table.exec.sink.not-null-enforcer` to `DROP`, as the error suggests, drops the delete instead, so the entity is never deleted.
A projection between the source and the sink also evaluates its expressions over those columns, which can fail the job when the source declares one of them `NOT NULL` ([FLINK-40528](https://issues.apache.org/jira/browse/FLINK-40528)).
Declare the non-key columns nullable, in this table and in the source table, when the input's deletes carry the key alone.

One sink subtask applies the writes to one key in the order it receives them, because it commits one request at a time and never puts a key in a request twice; the [DataStream sink]({{< relref "docs/connectors/datastream/firestore" >}}#how-the-datastore-sink-writes) names the one exception, an attempt that timed out on the client and is applied late.
The rows of one key reach one subtask when Flink shuffles the input by the PRIMARY KEY, which it does for an upsert changelog it materializes and when the sink's parallelism differs from the input's; an insert-only input carrying several rows per key at the same parallelism is not shuffled unless `table.exec.sink.keyed-shuffle` is `FORCE`, so its rows of one key can be written by different subtasks in either order.

A table without a PRIMARY KEY accepts inserts only, and writes each row as an `upsert` of the whole entity under a numeric id the service allocated (`AllocateIds`); such an id names no stored entity, so there is nothing to merge, and `sink.write-mode` is refused on such a table.
The service does not hand out an id it allocated again, so such a row cannot replace an entity written under another allocated id; each sink subtask fetches ids `sink.id-allocation.batch-size` at a time, through the same client, credentials, request timeout and recovery budget as its commits, and an allocation the service refuses, or the budget cannot finish, fails the job.
The guarantee covers allocated ids only: Datastore does not keep an allocated id from landing on an entity stored under a numeric id someone chose, so do not write a table without a PRIMARY KEY into a kind and namespace that also hold entities under self-chosen numeric ids, such as those of a `BIGINT`-keyed table.
Allocating ids takes the `datastore.entities.allocateIds` permission, which `roles/datastore.user` carries.

{{< sql-snippet file="flink/DatastoreTableReference.sql" tag="append" >}}

A refused write fails the job: a `WITH` clause cannot name a serializable failure handler, so the DataStream sink's routing of refused writes to one is not available, and neither is `maxConsecutiveRejections`, which only bounds a handler that drops them.

### Flink 2.3 may demand ON CONFLICT

Flink 2.3 ([FLIP-558](https://cwiki.apache.org/confluence/display/FLINK/FLIP-558%3A+Improvements+to+SinkUpsertMaterializer+and+changelog+disorder)) refuses to plan an updating statement into a table with a PRIMARY KEY, under any `sink.write-mode`, unless the PRIMARY KEY contains an upsert key the planner infers for the query, or the statement carries one of the `ON CONFLICT DO DEDUPLICATE`, `DO ERROR` or `DO NOTHING` clauses that Flink 2.3 introduces.
A query keyed by other columns, such as an aggregation written into a table keyed by its result, is refused, and so is one whose upsert key the planner cannot infer.
The refusal is the planner's, not this connector's: rows with different upsert keys that map to one primary key reach the sink in no defined order, and Flink 2.3 asks the statement to say what that should mean instead of materializing silently, as Flink 2.2 does.
An aggregation grouped by the key columns plans with no clause, and so does an insert-only statement, `INSERT INTO .. VALUES` included, because the sink answers an insert-only input with an insert-only changelog.
`ON CONFLICT DO DEDUPLICATE`, or the planner-wide `table.exec.sink.require-on-conflict = false`, plans a refused statement with the materialization Flink 2.2 uses; `DO ERROR` and `DO NOTHING` plan it only when every table the statement scans declares a watermark, while a lookup join's dimension table needs none (measured on 2.3.0).
Flink 1.20 and 2.2 do not parse the clause, and ignore the setting.
All of this is streaming behaviour: in batch execution Flink 2.3 plans such a statement with no clause, and accepts an `ON CONFLICT` clause without watermarks but drops it, so the rows reach the sink with no materializer and the clause has no effect (measured on 2.3.0).

## Options

| Option | Default | What it does |
|---|---|---|
| `project` | **required** | The Google Cloud project containing the database |
| `database` | *unset ⇒ the default database* | The id of the Firestore database in Datastore mode |
| `kind` | **required** | The kind of the table's entities |
| `namespace` | *unset ⇒ the default namespace* | The namespace of the table's entities |
| `emulator-endpoint` | *unset ⇒ the real service* | `host:port` of a Firestore emulator started in Datastore mode; setting it also stops credential discovery |
| `service-account-key-file` | *unset ⇒ ADC for the real service* | Service-account JSON key-file path, read by the scan's JobManager and readers and by each lookup and sink subtask; rejected with `emulator-endpoint` |
| `geo-point-field-paths` | empty | Semicolon-separated field paths of `ROW<latitude DOUBLE, longitude DOUBLE>` values written and read as geographical points |
| `reference-field-paths` | empty | Semicolon-separated field paths of `ROW<kind STRING, name STRING, id BIGINT>` values, optionally with `ancestors`, written and read as key values in the table's namespace |
| `type-mismatch-policy` | `fail` | What a read does with a stored value whose type does not match its column: `fail` the read, or read the field as `null` |
| `scan.partition.max-partitions` | *unset ⇒ estimated from the kind's statistics, at least the larger of 12 and the scan's parallelism* | Maps to `splitCount`: the key ranges a scan asks the splitter for, at most `50000` |
| `scan.read-time` | *unset ⇒ the service's time when the read is planned* | Maps to `readTime`: an ISO-8601 instant such as `2026-10-04T00:00:00Z`, truncated to the microsecond, within the past hour, or a whole minute within seven days with point-in-time recovery |
| `scan.max-rows-per-fetch` | `500` | Maps to `pageSize`: the entities one request asks for |
| `scan.parallelism` | *unset ⇒ the planner's parallelism* | Flink's standard source parallelism override |
| `lookup.async` | `false` | Run the join as Flink's asynchronous lookup, with several reads in flight per subtask |
| `lookup.cache` | `NONE` | Flink's standard lookup cache mode; `NONE` and `PARTIAL` are supported |
| `lookup.max-retries` | `3` | Reads after the first one, for `UNAVAILABLE`, `DEADLINE_EXCEEDED` and a deferred key only |
| `lookup.partial-cache.expire-after-access` | *unset* | Flink's standard partial-cache access expiry |
| `lookup.partial-cache.expire-after-write` | *unset* | Flink's standard partial-cache write expiry |
| `lookup.partial-cache.cache-missing-key` | `true` | Whether the partial cache records a key that found no entity |
| `lookup.partial-cache.max-rows` | *unset* | Maximum rows the partial cache keeps |
| `sink.write-mode` | `merge` | `merge`, `replace` or `update`, for a table with a PRIMARY KEY |
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

The [configuration reference]({{< relref "docs/reference/firestore" >}}#datastorewriteroptions) explains each writer option, and a refusal of a `scan.*`, `lookup.*` or `sink.*` value names the option key the `WITH` clause spells.
The pair `sink.recovery.initial-backoff` and `sink.recovery.max-backoff` is checked over the values the writer would use, so setting one compares it with the other's default.

## Delivery guarantee

A scan restored from a checkpoint resumes each split just after the last entity it passed, at the same read time; a job that restarts before its first checkpoint plans again, at a new read time unless `scan.read-time` is set.

The sink is at-least-once: it commits everything it holds at each checkpoint, and a restart replays the records after the last completed one.
With a PRIMARY KEY, a `merge`, a `replace` and a delete are idempotent, so a replayed row writes the same entity again; an `update` replayed after something outside the job deleted the entity fails the job, and fails again after each restart until the entity exists.
Without a PRIMARY KEY, a replayed row writes another entity under a newly allocated id, so a restart can leave duplicates; it replaces no entity written under an allocated id.
See the [DataStream delivery guarantee]({{< relref "docs/connectors/datastream/firestore" >}}#delivery-guarantee-in-datastore-mode).

## Design decisions and testing

The table mapping and the declined alternatives are recorded in [ADR-0184]({{< param BookRepo >}}/blob/main/docs/adr/0184-the-datastore-table-sink-upserts-one-kind-keyed-by-name-or-id.md).
The emulator integration tests, against the Firestore emulator in Datastore mode, write every type through SQL and read the entities back with the client library, apply an upsert changelog in order, merge rows into stored entities, replace them and update their declared properties, NULL and mismatched stored values included, fail an update of a missing entity, refuse deletes into an `update` table when planned and fail the delete an upsert materialization still sends, write a numeric id into a namespace, store a timestamp to the microsecond, and contrast an over-long string in indexed and unindexed columns at the top level, as an array element and inside a `ROW`, which the emulator refuses and stores as the service documents.
They also read every type back through a SQL scan with its metadata, read the allocated ids of a table without a PRIMARY KEY, fail and read as NULL a mismatched value under each policy, keep every entity under a column projection, and read at a configured read time.
Blocking and asynchronous lookups join against entities by name and by numeric id, a negative one and one in a namespace included, join no row for the keys that address no entity without failing, read the metadata, filter on an additional equality key under both caches, and read the current entity whatever `scan.read-time` says; the lookup's own client is checked to return exactly the masked properties, names holding a dot, a backquote or a backslash included, and an unindexed value and an array whole.
Geographical points and key values, with ancestors and in arrays, and a point inside a `ROW` in an array, are written through SQL, read back by the client library and the scan, and looked up, a lookup join on a key value's name included; a key value in another namespace fails the scan or reads as NULL by the policy, and an incomplete one fails the job.
The gated suite runs the same write, scan and lookup against the service, and pins its answer to a key value outside the entity's partition: it stores one in another namespace and refuses with `INVALID_ARGUMENT` one in another database and one naming a project that does not exist, both of which the emulator stores.
The uber-jar's tests write through its relocated classes.
