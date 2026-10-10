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

# ADR-0184: The Datastore table sink upserts one kind keyed by name or id

- Status: Accepted
- Date: 2026-10-10 (client library facts read in google-cloud-datastore 3.7.0 and the
  `google/datastore/v1/entity.proto` of proto-google-cloud-datastore-v1 3.7.0 through
  libraries-bom 26.90.0; emulator behavior measured 2026-10-10 against the pinned
  `google-cloud-cli` emulators image in Datastore mode, one run)
- Issues: [#1651], [#1545], [#355]
- Modules: firestore (`io.github.flink.gcp.connector.datastore`, `datastore.table`, `datastore.table.sink`, `datastore.source.batch`); flink-sql-connector-gcp-firestore
- Current behavior: `docs/content/docs/connectors/table/datastore.md`

## Context

[#355]'s design comment settled the outline of the Datastore-mode Table API: a factory with the identifier `datastore`, options `kind` and `namespace`, the PRIMARY KEY column as the key's name (`STRING`) or id (`BIGINT`), ancestors and id allocation deferred, properties as columns with an embedded entity as a `ROW`, a `sink.unindexed-columns` option mapping to `excludeFromIndexes`, and a second factory in the `flink-sql-connector-gcp-firestore` service file.
[#1545] was split into [#1651] (the factory and the sink), [#1652] (the scan) and [#1653] (the lookup) on 2026-10-04.
This record covers [#1651]; the scan and the lookup extend it.
The Native-mode table connector's choices are ADR-0179, and the DataStream sink this one maps onto is ADR-0175.

## Evidence

- `entity.proto` reserves a kind, a key name, a property name and a partition dimension matching `__.*__`, limits a kind, a key name and a property name to 1,500 UTF-8 bytes, refuses an empty one, limits an indexed string or blob to 1,500 bytes, and forbids `exclude_from_indexes` on an array value. Datastore's entity documentation adds that excluding an embedded entity from indexes excludes all its subproperties.
- The client library checks a key's project against a grammar and its namespace against `[0-9A-Za-z._-]{0,100}` when the key is built (`Validator`), and refuses an empty name and the id `0` (`PathElement`). `ListValue.Builder` and `BaseEntity.Builder` accept an array value marked as excluded, which only the service refuses.
- The emulator in Datastore mode refused a 2,000-byte string in an indexed property ("longer than 1500 bytes"; the probe that first saw it reported `INVALID_ARGUMENT`), at the top level, as an array element and inside an embedded entity, each in a row of its own, and stored all three in excluded ones (`DatastoreTableSinkITCase`).
- Datastore's entities documentation: the service "will keep track of IDs that have been allocated with these methods and will avoid reusing them for another entity", and "there is nothing to prevent a Datastore mode database from assigning one of your manual numeric IDs to another entity". `datastore.proto`'s `AllocateIdsResponse` returns the request's keys "in the same order", each "completed with a newly allocated ID".
- `AllocateIds` against a real Datastore-mode database (`flink-gcp`, a temporary database in `us-central1`, 2026-10-10, from a client in Japan, three calls per size, not kept as a test): calls of 100, 500, 501, 1,000, 2,000, 5,000 and 20,000 keys all succeeded with distinct ids, so the "500 keys per `AllocateIds`" row older copies of the limits page carried no longer applies; the current page has no such row. One call took about 0.3 s up to 1,000 keys, the round trip, and grew with its size past that: about 0.8 s at 2,000, 1.7 s at 5,000 and 5 s at 20,000. The emulator also served 20,000 and refused 100,000 only because the answer exceeded the client's 4 MiB gRPC message limit.
- Writing 100,000 rows at parallelism 4 with throttling off, from the same client, one run each: a keyed table 2,743 rows/s; a key-less table 1,330 rows/s allocating 100 ids per call, 2,460 at 500 and 2,554 at 1,000, within a tenth of the keyed table. The first cut fetched 100 per call; the owner asked for a configurable batch and a larger default.
- In two emulator probes (not kept as tests): the emulator refused `__x__` as a property name at the top level, inside an embedded entity and excluded from indexes alike (`The property.name "__x__" is reserved`), as a kind, as a namespace and as a key name, and refused `_____` as a kind and a property name, and a kind or a property name over 1,500 bytes; it stored `____` and `___` and names beginning with `__` (`__foo` as a kind, a property name, a namespace and a key name). `entity.proto`'s `__.*__` matches `____`, and Datastore's entities documentation says "All kind names that begin with two underscores (`__`) are reserved and may not be used", which covers `__foo`; the service's reading of both is unmeasured until the gated suite ([#1546]) writes them.
- The emulator stored a timestamp sent with nine fractional digits with six, as `entity.proto` says the service does (`DatastoreTableSinkITCase`).
- The DataStream sink commits one request at a time per subtask and never puts a key in a request twice, so one subtask's writes to one key are applied in order, except an attempt that timed out on the client and is applied late (ADR-0175). An upsert changelog of two changes to one key left the later one on the emulator.

## Decision

- **A table is one kind in one namespace, and the PRIMARY KEY is the key's last path element.** The key is one column: `STRING` is the name and `BIGINT` the id; it is never stored as a property. An empty name and the id `0` fail the record, because neither addresses an entity; a PRIMARY KEY column is `NOT NULL`, so Flink's not-null enforcer refuses (or, under `DROP`, drops) a NULL key before the sink sees it, and the serializer's own NULL check only guards a caller outside the planner. Keys carry no ancestors ([#355]'s deferral stands).
- **Every write is an `upsert` or a `delete`** (owner's decision, 2026-10-10). An insert or an update-after is an `upsert` of the row's entity, and a delete a `delete` of its key. There is no `sink.write-mode`: an `insert` replayed after a restart is refused with `ALREADY_EXISTS`, and an `update` of an entity deleted in between with `NOT_FOUND`, and under the table's fail-job handler either fails the job again on every restart. Neither the client library's entity API nor `DatastoreMutation` expresses a partial write (the API's `Mutation.property_mask` is reachable only through the generated client), so the Native-mode `merge` has nothing to map to.
- **A table without a PRIMARY KEY is insert-only, and each row is an `upsert` under an id the service allocated** (owner's decision, 2026-10-10, lifting [#355]'s id-allocation deferral for this case). The service keeps track of the ids `AllocateIds` hands out and does not reuse them, so a row cannot silently replace an entity written under another allocated id: that guarantee, not the improbability of a collision, is the requirement. It does not cover an entity under a numeric id someone chose — Datastore's documentation says nothing keeps an automatic id from landing on one — so the docs tell users not to share a kind and namespace between a key-less table and self-chosen numeric ids. The writer allocates, through its own client, credentials, single-attempt deadline and recovery budget, retrying a transient failure as it retries a commit, and hands its allocator to the table serializer through the `@Internal` `KeyAllocatingSerializationSchema`; the writer fetches `idAllocationBatchSize` ids per call (`sink.id-allocation.batch-size`, 1,000 by default, as the Evidence measures), the serializer draws them in order and takes only the id from each answer, building the key from its own project, database, namespace and kind. An allocation the service refuses, a spent budget, a short answer or an interrupted backoff fails the job whatever the failure handler, as a commit the budget cannot finish does: it is the database's failure, not the record's. The DataStream SPI is unchanged: a user serializer still returns complete keys only (ADR-0175). The id is chosen before the mutation is buffered and travels with it, so a retry within one attempt writes the same entity; a replay after a restart allocates another, so it can duplicate a row but not replace an allocated one. The write stays an `upsert`, because an `insert` retried after a lost answer would be refused with `ALREADY_EXISTS`. The emulator serves `AllocateIds`, in a namespace too (`DatastoreTableSinkITCase`).
- **A table with a PRIMARY KEY consumes an upsert changelog**, `upsert(true)` on Flink 2.x so a delete may carry the key alone, which is all the sink reads. The independent review found, and `DatastoreKeyOnlyDeletesITCase` measured in a first version, that Flink 2.2's not-null enforcer (`NotNullConstraint`) checks every row before the sink, a delete included, so a key-only delete into a table with a `NOT NULL` column besides the key fails the job (or, under `DROP`, vanishes). That is Flink's bug, [FLINK-40477], fixed in 2.4.0 and present in both supported 2.x minors; the sink keeps declaring what it reads (owner's decision, 2026-10-10), and the docs name the workaround, nullable columns besides the key. One subtask keeps one key's write order, as the Evidence measures, unlike the Native-mode sink.
- **The type mapping is closed and checked when the statement is planned** (`DatastoreTableSchema`): `STRING`/`VARCHAR`/`CHAR`, `BIGINT`, `DOUBLE`, `BOOLEAN`, `BYTES`/`VARBINARY`/`BINARY` as a blob, `TIMESTAMP_LTZ` as a timestamp, `ARRAY` (not directly inside another), and `ROW` as an embedded entity without a key. A SQL `NULL` is a Datastore null. Every other type is refused naming the field.
- **What every write would be refused for is refused when the statement is planned** (ADR-0127's per-record class): a reserved property name or one over 1,500 bytes, at any depth; a blank kind, a reserved one or one over 1,500 bytes; a blank namespace, a reserved one, or one the client library's grammar refuses; and a project that grammar refuses. A reserved name is `__`, at least one character, then `__`, the emulator's reading: `____` and kinds beginning with `__`, which `entity.proto` and the documentation reserve more widely, are left to the service, since refusing a name the service accepts would refuse a working table, where a name it refuses only fails the job on the first write. A nested name is refused wherever it is declared, even in a `ROW` a row may leave NULL: the column's type is what declares the property. A blank kind or namespace is refused as configuration (ADR-0127's blankness), not as a service rule; the empty namespace is the default one, written by leaving the option out. The project and the namespace grammar are checked by building a probe key, so the client library's own rule applies; the library checks a kind only for emptiness, so the kind's rules and the reserved pattern are the service's, stated once in `DatastoreTableSchema`. A reserved or over-long key name is data and stays the service's, which refuses it loudly.
- **`sink.unindexed-columns` names top-level columns**, refusing an unknown one, a nested path and the key. The column's value is marked excluded from indexes, and so is every value nested in it: an array's elements carry the mark because the array value may not, and an embedded entity's values carry it beside the entity's own, which keeps the exclusion independent of how the service treats a subproperty.
- **Every writer knob maps through `OptionSetters`** under ADR-0137's keys: `sink.buffer-flush.max-mutations` and `sink.buffer-flush.max-size` for the connector-owned buffer (Spanner's spelling), `sink.request-timeout`, `sink.recovery.*` and `sink.throttling.*`. The builder's one cross-check, a backoff cap below the initial backoff, is restated in option keys over the values the builder would compare, an unset option taking its default. `maxConsecutiveRejections` has no option, because the table sink fails the job on the first refused write.
- **The sink reports a `datastore-kind` lineage resource** through the base `TableLineageSink` under the table's catalog identifier (ADR-0160), with the default database spelled `(default)`, as the DataStream source reports the same kind.
- **The factory is the module's second Table factory**, in the same service file, so `flink-sql-connector-gcp-firestore` carries it without a change to the jar's relocations; its packaging test checks that both factories are discovered.

## Consequences

- A Datastore-mode table can be written from SQL without a DataStream program, with the DataStream sink's batching, ramp-up and recovery.
- A table cannot express an ancestor path, a key value, a geographical point, a `MAP` or a `DECIMAL`; each is additive later.
- An over-long value in an indexed column fails the job, with the service's message naming the property; the docs point to `sink.unindexed-columns`.
- The scan ([#1652]) and the lookup ([#1653]) extend this connector; the type mapping is the contract they read back.

## Alternatives declined

- **Refusing a table without a PRIMARY KEY.** Proposed in planning on the ground that id allocation was deferred; the owner chose to accept key-less tables.
- **A 20-character `SecureRandom` key name per row**, ADR-0179's shape, which this sink first shipped with. About 119 bits make a collision improbable, but an `upsert` under a colliding name replaces the other entity silently, and the owner asked for a guarantee rather than a probability. The Native-mode sink has no allocator in `firestore.v1`; [#1680] tracks the same guarantee there.
- **An `insert` under a random name**, which refuses a collision instead of replacing: a commit retried after a lost answer, or one that partly applied, would then refuse the sink's own write with `ALREADY_EXISTS` and fail the job.
- **An `upsert` under an incomplete key, letting `Commit` allocate the id**, which needs no allocation call. It would put incomplete keys into the public `DatastoreMutation` (ADR-0175 keeps them out), and the writer keys its batch bookkeeping, solo confirmation and address check by complete key; and a commit retried after a lost answer, or one that partly applied, would write those rows a second time under new ids.
- **Timestamp- or sequence-prefixed key names** (UUIDv7-like). Datastore's best practices name sequential keys as a hotspot under write load, and a prefix adds no guarantee against replacement.
- **`sink.write-mode` with `insert` and `update`.** Both turn a replay into a job failure that repeats on every restart, as the Decision says; the DataStream sink offers them to a pipeline that can route the refusal.
- **Asking for full deletes when a column besides the key is `NOT NULL`**, to keep Flink 2.2 and 2.3 from failing on [FLINK-40477]. Measured in `DatastoreKeyOnlyDeletesITCase`'s first version: the planner then normalizes an input whose deletes carry the key alone, which keeps each key's last row in state and drops the delete of a key the job never saw, so an entity that predates the job is never deleted. Working around a Flink bug at that cost was declined (owner, 2026-10-10).
- **Marking an array value itself as excluded**, which the client library accepts and the service refuses.
- **Nested paths in `sink.unindexed-columns`.** Excluding a column excludes everything in it, which covers the long-text and blob cases; a path grammar can be added without breaking a table.
- **Checking a key name against the reserved pattern and the length limit in the serializer.** Only the data supplies a key name, so no check can run at planning, and the service refuses either with `INVALID_ARGUMENT`, which the sink confirms and fails the job on; a copy of the rule in the serializer would change only the message.

[#355]: https://github.com/flink-gcp/flink-connector-gcp/issues/355
[#1545]: https://github.com/flink-gcp/flink-connector-gcp/issues/1545
[#1546]: https://github.com/flink-gcp/flink-connector-gcp/issues/1546
[FLINK-40477]: https://issues.apache.org/jira/browse/FLINK-40477
[#1651]: https://github.com/flink-gcp/flink-connector-gcp/issues/1651
[#1652]: https://github.com/flink-gcp/flink-connector-gcp/issues/1652
[#1653]: https://github.com/flink-gcp/flink-connector-gcp/issues/1653
[#1680]: https://github.com/flink-gcp/flink-connector-gcp/issues/1680
