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

# ADR-0189: The Spanner sink keeps per-row order on opt-in

- Status: Accepted
- Date: 2026-10-29 (client library read in google-cloud-spanner 6.124.0; emulator behavior measured
  2026-10-28 against the pinned `gcr.io/cloud-spanner-emulator/emulator` image, three runs; service
  behavior measured 2026-10-29 on an ephemeral 100-processing-unit `STANDARD` instance in
  `regional-us-central1`), revised by [#1927] (2026-10-30) and [#1928] (2026-10-30)
- Issues: [#1619], [#1618], [#1556], [#1927], [#1928]
- Modules: spanner (`sink`, `sink.writer`, `table`, `table.sink`)
- Current behavior: `docs/content/docs/connectors/datastream/spanner.md` § Order per row;
  `docs/content/docs/connectors/table/spanner.md` § Delivery guarantee;
  `docs/content/docs/reference/spanner.md` § Order per row

## Context

[ADR-0075] puts one mutation in each `MutationGroup` of a `batchWriteAtLeastOnce` request. The
client library documents that the groups of one request "are applied non-atomically in an
unspecified order and thus, they must be independent of each other". Two mutations of one row in a
request are not independent, so the sink has documented that it does not keep the order of
successive writes to one key. [#1618] asks each keyed sink for an opt-in mode that keeps it, and
[ADR-0187] settled the spelling on the first of them, Firestore.

Spanner's case is narrower than Firestore's. The writer is synchronous: it sends one request at a
time, and it retries a request's undecided mutations before it sends the next ([ADR-0075]). Order
between requests is therefore already kept, and only a row that appears twice in one request is at
risk.

## Decision

- **`OrderingMode.PER_KEY` sends the batch before a mutation whose row the batch already holds.** No
  request then carries a row twice, which removes the dependence the client library warns about
  between two groups of one row. Groups of different rows can still depend on each other through a
  cascade, a foreign key or a unique index, and the mode does not order those. Mutations of other
  rows still share requests. This is the Datastore-mode writer's shape
  ([ADR-0175]), not Firestore's [ADR-0187] sequencer: a writer with nothing in flight between
  requests has no answer to wait for, so the flush alone orders a row's mutations.
- **A row is a table and its primary-key values, with the primary key and its column types read
  from `INFORMATION_SCHEMA` when the writer opens, in this mode only.** A `Mutation` names its
  columns but not which of them form the key. The query starts from each base table in `TABLES`
  and outer-joins its key columns from `INDEX_COLUMNS` under the index named and typed
  `PRIMARY_KEY`, placed by `ORDINAL_POSITION`, and their types from `COLUMNS.SPANNER_TYPE`, across
  every visible user schema, as the cell weights ([ADR-0077]) and the catalog ([ADR-0176]) read
  them. It starts from `TABLES` because a GoogleSQL table declared with `PRIMARY KEY ()` has no row
  in `INDEX_COLUMNS`, and on the service none in `INDEXES` either, although the emulator lists one
  there ([#1927]). The catalog's `SpannerTypeSpelling` moved to the module root so that the sink
  parses the spellings without reaching into the table layer. Under `NONE` the writer opens
  exactly as before.
- **The read covers the tables the serializer may name** ([#1928]). The DataStream serializer
  chooses a table per mutation ([ADR-0075]), so its writer reads every table. The Table sink writes
  the one table it was planned for, and its serializer declares that table by implementing the
  internal `SingleTableSerializationSchema`, so its writer reads that table alone, as it does for the cell weights
  ([ADR-0077]). The table is matched without case in both dialects, as table names compare, which
  reads one table at most: GoogleSQL names are case-insensitive, and the service refuses a
  PostgreSQL table whose name differs from another's only by case.
- **Doubt resolves toward the whole table.** Finding two rows the same when they are not costs an
  earlier flush; finding them different when they are the same costs the order. So a mutation whose
  row the writer cannot name is ordered against every mutation of its table: a delete of a key range
  or of every row, a write without a value for every key column (an insert into a table whose key
  column is generated, which Spanner computes from the row's other columns), a key value in another
  representation than its column's type, and an operation other than the four writes and the
  delete. A GoogleSQL table declared with `PRIMARY KEY ()` is one of these too, and there the
  answer is exact rather than cautious: it has no key column and holds one row at most, so its
  table is its row ([#1927]).
- **A table name the keys do not list makes the writer send its batch and read them again, once
  per name** ([#1928]). The name may be a table created after the writer opened, which the new read
  lists, so its rows are named from then on. It may also be a synonym, which addresses a listed
  table's rows under another name, or a table the writer's role cannot see; the new read does not
  list those either, and they stay ordered against the whole database, since only the whole
  database is sure to contain the rows they address. A synonym is a `TABLES` row of type
  `SYNONYM`, not `BASE TABLE`, so the query never lists it. The independent review of [#1619]
  found the synonym case: the first version ordered an unlisted name against its own name only, so
  an upsert of `Orders` and a delete through a synonym of it could share a request. A name the new
  read does not list is remembered, and never triggers another read, so the reads a writer makes
  are bounded by the distinct unlisted names it meets. A name that is neither a table nor a synonym
  fails its write with `NOT_FOUND`, which fails the job, and the restart reads the schema at open.
  The batch is sent before the read: the mutation would have sent it anyway, and sending it means
  every row the batch held was named by the keys the next rows are compared with.
- **A failed read again follows the batch write's failure policy** ([ADR-0075], [ADR-0076]).
  `ABORTED`, `UNAVAILABLE`, `DEADLINE_EXCEEDED` and `RESOURCE_EXHAUSTED` are retried under
  `recoveryInitialBackoff`, `recoveryMaxBackoff` and `recoveryMaxAttempts`, and fail the job once
  those are spent; every other status fails the job at once. The read runs outside the per-record
  guard, so its failure is never routed as a failed mutation.
- **A key value is compared as its column's type reads it, and only in the client library's
  representation of that type.** Spanner receives a mutation's values untyped and reads each by its
  column's type, so one row can be addressed in more than one spelling, such as a `UUID` key as a
  `UUID` or as a string. An `INT64` key compares as a `Long`, a `STRING` key as a `String`, a
  `NUMERIC` key by value whatever its scale, and any other representation names the whole table.
  Review found that a first version, which compared wire forms after normalizing numeric strings
  and RFC 3339 spellings, could judge one row two: it normalized a base64 `BYTES` value that looked
  numeric on one side only, it kept a `UUID` written typed and deleted as an upper-case string in
  two spellings Spanner presumably reads as one, and a numeric string with a huge exponent threw
  instead of comparing. A `FLOAT32` key, a third case review raised, cannot occur: the emulator
  refuses a `FLOAT32` primary-key column (2026-10-29), so the float-precision comparison the code
  keeps for it only merges.
- **Table names compare without case in both dialects.** The cell weights match a PostgreSQL table
  name exactly ([ADR-0077]), and whether the service does is not measured; an undercounted cell
  weight is harmless, but a table split in two here would break the order. Folding only merges, and
  never merges two tables: the service refuses a PostgreSQL table whose name differs from
  another's only by case, measured for [#1928].
- **The commit-timestamp placeholder is one value.** Groups of one request may commit in one
  transaction, and so at one timestamp, so two writes keyed by it may address one row.
- **The option is `orderingMode(OrderingMode)` on `SpannerWriterOptions` and `sink.ordering-mode` =
  `none` | `per-key` in a `WITH` clause, with `OrderingMode` in this module's `sink` package**
  ([ADR-0187]'s spelling). The options object reads a `null` field from a form serialized before the
  field existed as `NONE`, as it does for `batchWriteTimeout`.
- **No new metric.** The cost is extra requests, which `batchesSent` already counts against
  `numRecordsSend`.

## Evidence

- The client library's own contract, quoted in the Context, from `DatabaseClient` in
  google-cloud-spanner 6.124.0.
- `SpannerWriteITCase.perKeyOrderAppliesEachRowsMutationsInTheOrderReceived` also counts
  `batchesSent`: ten rounds over twenty rows send exactly ten requests, which fails when the primary
  keys read at open do not reach the writer (every mutation would then be ordered as a whole
  table).
- `SpannerWriterTest` pins the flush: without the mode one request carries a row twice; with it the
  batch is sent before the repeat, other rows keep sharing requests, a delete of every row is ordered
  against its table, and a batch's undecided mutation is retried before the row's next mutation is
  sent. `PrimaryKeysTest` pins the comparison for every key type against the `Key` part a delete would
  carry under each column type, the representations that fall back to the whole table, and each
  other fallback.
- The emulator does not reorder. `SpannerWriteITCase.perKeyOrderAppliesEachRowsMutationsInTheOrderReceived`
  writes ten rounds of upserts and deletes over twenty rows; switched to `NONE`, it still passed in
  each of three runs. The emulator test therefore holds the mode's end-to-end path, and the unit
  tests are what fail without the flush.

Measured against the service on 2026-10-29, from one writer on a workstation outside Google Cloud,
one database, three repetitions per mode after a warm-up, alternating which mode went first. The
primary-key read named the row correctly in both dialects. That run used the first version, which
read `INDEX_COLUMNS` alone; the join to `COLUMNS.SPANNER_TYPE` added on review is the shape the
catalog's gated suite already runs against the service ([ADR-0176]), and the emulator tests run it
in both dialects.

Measured against the service on 2026-10-30 for [#1927], on two ephemeral 100-processing-unit
`STANDARD` instances in `regional-us-central1`, each deleted after its run. A GoogleSQL table
declared with `PRIMARY KEY ()` is a `BASE TABLE` row in `TABLES` but has no `PRIMARY_KEY` row in
`INDEXES`, which the emulator does list, so a query starting from `INDEXES` still missed it. The
query as written returned that table once, with a null column, position and type; a two-column key
in position order; and a table in a named schema. In PostgreSQL it returned both keyed tables the
same way. A synonym and a view, which `TABLES` lists as `SYNONYM` and `VIEW` in both dialects,
were not returned.

| Workload | `NONE` | `PER_KEY` |
|---|---|---|
| 100 upserts of one row | 0.3–2.9 s; the row ended at the last write in 4 of 4 runs | 20.0–23.0 s, about 0.2 s per request; the last write stood in 3 of 3 runs |
| 2,000 upserts, each of its own row | 1.6–3.4 s | 1.6–4.6 s |
| 200 rows changed five times each, interleaved, a third of the changes deletes | 0.4–3.9 s, every row at its last change in 3 of 3 runs | 1.1–4.0 s, every row at its last change in 3 of 3 runs |

- **The service did not reorder in these runs.** Unlike Firestore ([ADR-0187]), where the hot
  document ended at an earlier write every time, one Spanner request carrying 100 writes of one row
  applied them in order four times. That is one small, new table, whose rows a 100-processing-unit
  instance presumably serves from few splits; a table spread over many splits was not measured.
  The contract does not promise the order, and the mode follows the contract rather than the
  measurement (owner's decision, 2026-10-29).
- **The cost is a request per repeat**, about 0.2 s each from this client. The distinct-row workload
  falls in the same range in both modes, and the changelog shape's spread between runs is wider than
  the difference between the modes.

- [#1928]'s firing controls, each a mutant run against the tests that should fail it with
  checkstyle skipped, all failed a test: no read again, a read for every mutation of an unlisted
  name, the raw table name as the key of the names read for, no batch sent before the read, no
  retry of a transient read failure, a narrowed query that ignores its table, a serializer's table
  not taken, and the Table sink's serializer not declaring its table. `SpannerWriteITCase.perKeyNamesTheRowsOfATableCreatedAfterTheWriterOpened`
  creates a table after the writer opened and writes three of its rows and a repeat, in both
  dialects on the emulator: two requests, where an unlisted table takes four.
  `readsOnlyTheNamedTableOutOfTheRealSchema` reads one table, spelled in another case, out of two.
- Measured against the service on 2026-10-30 for [#1928], through the writer's own database access
  on two ephemeral 100-processing-unit `STANDARD` instances in `regional-us-central1` that the
  gated harness created and deleted. A read scoped to a table spelled in another case listed that
  table alone: in GoogleSQL a default-schema table and a named-schema one, in PostgreSQL an
  unquoted table and a quoted mixed-case one. A scope naming a synonym listed nothing, and a table
  created just before a read was listed by it. The service refused to create a PostgreSQL table
  `"Orders"` beside `orders`: "Schema object names differ only in case".

## Alternatives declined

- **A per-key sequencer like [ADR-0187]'s.** It holds a write until the earlier one is answered,
  which a synchronous writer already does for every request.
- **A key extractor supplied by the user**, a `KeySelector`-shaped setter beside the serializer. It
  adds public surface, and the Table sink would have to supply one too, when the database already
  says which columns form each key.
- **Reading the primary keys in every mode.** It would change the open path of every existing job
  for a feature it does not use.
- **Ordering a parent row against its interleaved children or a foreign key's referenced row.**
  Those are different rows. Ordering them would mean reading the interleave and foreign-key graph
  and flushing across tables, a stronger guarantee than [#1618] asks for.
- **Comparing wire forms with normalizations**, the first version. See the Decision: every
  representation the normalizations missed split a row.
- **Not shipping the mode**, because the service kept the order in the measurement. The opt-in leaves
  the default unchanged, and the measured order is not a contract a sink can rely on.
- **Reading the keys again at an interval, or periodically** ([#1928]). The first plan re-read on
  an unlisted name at most once per interval, to bound a synonym written on every record. The owner
  asked why the schema must be read on a schedule at all. Remembering the names a read did not
  list bounds the reads without one, and a name that is neither a synonym nor a table fails its
  write. A synonym dropped and replaced by a table of its name, or a table the role is later
  granted, stays ordered against the whole batch until a read made for another name, or a restart,
  lists it, which costs requests but keeps the order.
- **A lookup per name, with a cache of names that expires** ([#1928]). It reads one table rather
  than the scope, but needs a rule for how a lookup by name treats a synonym, and the expiry brings
  back the schedule the item above removes. Re-reading the scope keeps one query, whose treatment
  of synonyms was measured on the service.
- **Warning and keeping the old keys when a read fails** ([#1928]), the first plan. The order would
  hold, since an unlisted name is ordered against the whole database, but a table created since
  the writer opened would stay at one request per mutation behind a job that looks healthy. The
  failures that are not transient, a lost grant or a deleted database, fail the writes as well.
- **A builder setter for the read scope** ([#1928]). Only this module's Table sink knows its table
  when the writer is built, and a public setter would let a DataStream user narrow the read below
  the tables the serializer writes. The serializer declares the scope instead.
- **Reading the cell weights again with the keys.** See [ADR-0077]: a weight the writer lacks
  undercounts inside the headroom, while a key it lacks costs a request per mutation.

## Consequences

- A row written n times in a row takes n requests under `PER_KEY`. A table whose writes the writer
  cannot name, such as one whose key column is generated and whose inserts carry no key, gets one
  request per mutation, and a table name the keys do not list even when read again sends the batch
  before and after each of its mutations.
- The order kept is the order one subtask received. The DataStream user must key the stream by
  primary key before the sink, and the Table sink gets it when Flink shuffles the input by the
  primary key, which an insert-only input at the sink's own parallelism is not unless
  `table.exec.sink.keyed-shuffle` is `FORCE`.
- One order is out of the writer's reach, as in [ADR-0175]: an attempt that timed out on the client
  can still be applied by the service after a later request.
- A table created after the writer opened costs one schema read and one flush, at its first
  mutation, and is named from then on. A synonym costs one read, the first time it appears, and is
  ordered against the whole batch at every mutation, as before.
- A table dropped and created again with another primary key is still compared by its old key
  until the writer is created again, which can split a row; the mode does not detect it, because no
  schema version fences the read against the write. The DataStream and Table pages tell the user to
  restart a `per-key` job after such a change ([#1928]).
- Rows related by an interleaved parent's `ON DELETE CASCADE`, a foreign key or a `UNIQUE` index
  are different rows and are not ordered against each other.
- `OrderingMode` is `@Experimental`, a closure stop on the `@Public` `SpannerWriterOptions`
  signatures, recorded in [ADR-0124]'s revision; Firestore's namesake is `@PublicEvolving` because
  its options class is.

[#1556]: https://github.com/flink-gcp/flink-connector-gcp/issues/1556
[#1618]: https://github.com/flink-gcp/flink-connector-gcp/issues/1618
[#1619]: https://github.com/flink-gcp/flink-connector-gcp/issues/1619
[#1927]: https://github.com/flink-gcp/flink-connector-gcp/issues/1927
[#1928]: https://github.com/flink-gcp/flink-connector-gcp/issues/1928
[ADR-0075]: 0075-the-spanner-sink-batch-writes-and-owns-the-whole-retry-loop.md
[ADR-0076]: 0076-two-spanner-statuses-are-routed-and-a-request-failure-never-is.md
[ADR-0077]: 0077-batch-limits-are-counted-in-index-aware-cells-read-once-at-open.md
[ADR-0124]: 0124-the-stability-boundary-at-1-0-0-is-a-promoted-public-entry-surface-checked-by-japicmp.md
[ADR-0175]: 0175-the-datastore-sink-commits-batches-and-confirms-a-refusal-one-write-at-a-time.md
[ADR-0176]: 0176-the-spanner-catalog-maps-an-instances-databases-and-information-schema-to-flink-tables.md
[ADR-0187]: 0187-the-firestore-sink-keeps-per-document-order-on-opt-in.md
