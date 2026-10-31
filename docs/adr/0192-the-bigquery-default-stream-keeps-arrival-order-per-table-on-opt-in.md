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

# ADR-0192: The BigQuery default stream keeps arrival order per table on opt-in

- Status: Accepted
- Date: 2026-10-31 (writer behavior reproduced with unit tests; cost measured 2026-10-31 against
  the service from a workstation, dataset in `us-central1`); refined 2026-10-31 for [#1936], which
  keeps a CDC key out of a request twice (unit tests, and the service measured the same day from
  the same workstation)
- Issues: [#1621], [#1618], [#1936]
- Modules: bigquery (`sink`, `sink.storage`, `sink.storage.writer`, `sink.tables`, `table`, `table.sink`)
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md` § Commit order;
  `docs/content/docs/connectors/table/bigquery.md` § Commit order of a table's rows;
  `docs/content/docs/reference/bigquery.md` § `DefaultStreamOptions`

## Context

[#1618] asks each keyed sink for an opt-in mode that keeps the order of successive writes to one
key, and [ADR-0187], [ADR-0189] and [ADR-0190] answered it for Firestore, Spanner and Bigtable.
[#1621] carried the BigQuery CDC sink's share of the question. A CDC write without a
`_CHANGE_SEQUENCE_NUMBER` leaves BigQuery to resolve one primary key's mutations by the time it
ingests them ([ADR-0111]), so the sink's commit order decides which mutation wins.

The `STORAGE_API_AT_LEAST_ONCE` writer does not keep that order. It keeps several append requests
of one table in flight, and a completion callback that sees a repairable failure only marks the
writer; the task thread repairs at the next `write()` or `flush()` by re-sending the failed rows on
a rebuilt stream writer ([ADR-0017], [ADR-0030]). Three paths then commit a table's rows out of the
order a subtask received them:

- later requests are sent, and may commit, before the failed one is re-sent;
- the repair collects the destination's failed requests from a hash map keyed by future and
  re-sends them in that map's order, and `flush()` sends the pending batch before it repairs;
- after a row-level rejection, the surviving rows are re-sent behind the requests that followed
  them.

None of these fails the job; the re-sends show only in the writer's metrics (`appendRetries` and
the per-status error counters) and its log.
The documentation described only BigQuery's own tie-break.

The same writer serves the append-only path, and the owner decided on 2026-10-30 that it gets the
same opt-in rather than being excluded: an append-only table has no same-key overwrite, but a reader
can depend on commit order. A query over `APPENDS` that keeps the row with the latest
`_CHANGE_TIMESTAMP` per key, a BigQuery continuous query that forwards rows as they commit, and a
reader that resumes after the highest value it has seen all observe it.

## Decision

- **`OrderingMode.PER_TABLE` sends a table's next append only after the previous one has
  committed, or after its failed rows have been re-sent or handed to the failure handler.** The
  writer records each table's latest append, awaits it before the next one, and repairs it on the
  spot when it failed. A repair re-sends every failed batch of the table and awaits each re-send
  before it returns, so nothing of the table is left in flight, and the hash-map order of the
  second path above has at most one entry. A terminal failure that a completion callback captured
  is surfaced before the next append is sent rather than at the next `write()`. Different tables
  keep their appends in flight together.
- **For a CDC write, a request ends before a row whose primary key it already holds**
  ([#1936]). BigQuery's
  [CDC documentation](https://cloud.google.com/bigquery/docs/change-data-capture#manage_custom_ordering)
  resolves a sequence-less key by ingestion time, cautions that this "might not be sufficient" for
  very frequent upserts to one key, and says nothing about the rows of one append request, so a
  key changed twice within one request would have no defined winner; the measurement below found
  that the first change wins. Two changes with equal sequences fall back to the same ingestion-time
  order, and the PostgreSQL, TiCDC and Spanner profiles can each produce equal sequences for one
  key ([ADR-0111]), so the split applies with a sequence too. The writer therefore sends
  the pending request before such a row, the shape [ADR-0189] uses for Spanner, and a key's
  changes land in successive requests, which the rest of the mode orders. The owner routed this to
  [#1936] when the first version of the mode left it open.
- **The key is the table's declared primary key, read from the serialized row.** When it opens a
  CDC destination under the mode, the writer reads the table's primary-key columns with their
  types and collation through the Tables API, after the CDC table contract has been ensured, and
  reads each row's key from the bytes the serializer wrote, by field name. Reading the serialized
  row serves the Table API and every DataStream serializer the same way, where reading the record
  before serialization would need a key accessor for each record type. The live table rather than
  `CdcTableOptions` names the key, because the options need not name one: a table that exists is
  verified against its own key when they do not. Columns are matched by name case-insensitively,
  through the `column_name` annotation of a placeholder field when the name is not a valid proto
  field name, as the Storage Write API matches them. The read is a second `tables.get` beside the
  provisioning one and is retried as that one is, so a rate limit on it delays the table's
  activation rather than failing the job.
- **A key is compared as the bytes that encode it, and every imprecision may only merge keys.**
  Two rows BigQuery would see with one key must never compare as different, since that is the
  defect the split exists to close; two different keys compared as one only cost a request. Bytes
  name one value exactly for a `string` or `bytes` field in an uncollated `STRING` column, a
  `bytes` field in a `BYTES` column, a `bool` field in a `BOOL` column, any integer field in an
  `INT64`, `DATE`, `DATETIME` or `TIMESTAMP` column, and a `bytes` (after removing redundant sign
  bytes) or integer field in a `NUMERIC` or `BIGNUMERIC` column, which covers every key type the
  Table API writes. Under a parameterized decimal type, BigQuery rounds a value with more
  fractional digits than the column's scale, so two different encoded values can meet in one key;
  such a value's row has an unreadable key. `NUMERIC(P)` declares no scale and is read as scale
  zero. Any other encoding, such as a `TIMESTAMP` or `NUMERIC` written as text, a
  floating-point field, or a collated `STRING`, has several spellings of one value or several values
  BigQuery compares as equal; its key is unreadable, an unreadable key matches every row, and every
  row of the table becomes its own request. The writer logs a warning naming the column when it
  opens such a table, and again only if a schema change alters the reason. A row whose key does
  not parse matches every row the same way, a default value is read as absent, and a key column
  the descriptor lacks is absent in every row; all three only merge.
- **An append-only write is not split.** It has no key, and the mode orders its requests as it
  orders any table's.
- **The mode orders a table, not a key.** The default stream offers nothing narrower: a failed
  request need not fail the requests behind it (a row-level rejection fails only its own), which is
  what lets a later one commit first, so the only order that holds across a failure is one request
  at a time. The CDC key split above decides where a request ends, not which requests may be in
  flight together, and an append-only table has no key to order by.
- **The constant is `PER_TABLE`, spelled `per-table`, not the umbrella's `per-key`** (owner,
  2026-10-31). A shared name diverges only to name a real difference ([ADR-0137]), and this is
  one: the mode orders a table's requests rather than a key's writes, and an append-only table has
  no key for the name to refer to. The Table option keeps the shared key, `sink.ordering-mode`, and the
  DataStream setter keeps the shared shape, `DefaultStreamOptions.Builder#orderingMode`, with a
  BigQuery `OrderingMode` enum in `bigquery.sink` ([ADR-0137]).
- **The option belongs to `STORAGE_API_AT_LEAST_ONCE` alone.** It is a member of the
  `sink.default-stream.*` family for the factory's foreign-family check although its key is not under
  that prefix, so `storage-api-exactly-once` and `file-loads` reject it with the family's message.
  Neither has the problem in this form. The buffered writer appends with offsets ([ADR-0022]), so a
  failed append cannot be overtaken, and `FILE_LOADS` makes rows visible one checkpoint commit at a
  time; the order of the load jobs inside one commit was not examined.
- **`OrderingMode` is `@Experimental`**, a closure stop on the `@Public` `DefaultStreamOptions`
  signatures, recorded in [ADR-0124]'s revision, as Spanner's and Bigtable's are.

## Evidence

- `BigQueryDefaultStreamWriterArrivalOrderTest` drives the writer against a fake service that
  answers an append when the writer awaits it, or else when the next append of the same table is
  sent. Each test asserts both modes on one input. Under `NONE`, a transient failure of the first of
  three appends lands `b, c, a`, and so does one whose re-send fails again; a row-level rejection
  in the first of three appends lands the survivor after both later batches; and twenty failed
  appends land the twenty-first batch first and the rest in hash order. Under `PER_TABLE` each
  lands in arrival order. Further tests pin that a terminal failure, an exhausted repair, and a
  terminal failure a schema refresh awaited each stop the next append; that two tables keep an
  append in flight each while a repair runs during `flush()`; that an answer arriving before the
  next record keeps the order; and that the mode reaches the writer from `DefaultStreamOptions`
  through the sink.
- Seven mutants of the mode, each removing one piece (the await, the recorded future, the captured
  failure check, the same check when there is no previous append, the two places the state a
  repair rebuilds is re-read, and the public constructor passing the mode), were each killed by
  that test class.

Measured against the service on 2026-10-31, from one writer on a workstation outside Google Cloud,
far from the dataset's region, writing to a table in `us-central1`: 50,000 rows of about 1 KB each
per run, three repetitions per configuration after a warm-up, alternating which mode went first.

| `maxAppendRequestBytes` | `NONE` | `PER_TABLE` |
|---|---|---|
| 512 KiB (default), about 97 requests | 2.7–3.0 s | 23.8–24.8 s, about 0.25 s per request |
| 8 MiB, about 7 requests | 2.8–4.5 s | 5.4–7.7 s |

- **The cost is a round trip per request.** At the default cap, `PER_TABLE` took about eight times
  as long, and the time per request matches one round trip from this client. An 8 MiB cap brought
  it to about twice `NONE`'s time, because each round trip carried sixteen times the rows. A client
  closer to the dataset's region has shorter round trips and should pay less; that was not
  measured.
- **Every run committed every row exactly once**: each table held its 50,000 rows and the warm-up
  row, all distinct. No append failed during the runs, so the measurement prices the mode and does
  not show a reordering; the unit tests above are what show one.

### The CDC key split ([#1936])

- `BigQueryDefaultStreamWriterCdcKeyOrderTest` writes through the sink's production path. Each
  test that expects a split runs `NONE` over the same input, where one request carries every row:
  under `PER_TABLE` a repeated key starts a new request, also when every row carries the same
  sequence; a row whose key cannot be read shares a request with no other row; the keys of pending
  rows survive the rebuild a schema refresh makes; a failed request is re-sent as it was, without
  merging into the pending rows behind it; and a key column whose name is not a valid proto field
  name is found through its placeholder field's annotation. Further tests pin that an append-only
  table leaves the request whole and reads no key, that the key is read after the table contract
  is ensured and again when an evicted table returns, and that an unreadable key is logged once,
  not again on a rebuild that leaves the reason unchanged. `CdcRowKeysTest` holds which encodings
  are read, that every normalization only merges, and that a decimal finer than its column's
  scale is unreadable; `BigQueryTableAdminTest` holds the scale read, `NUMERIC(P)` included.
- Of twenty-seven mutants, each removing one piece, twenty-six were killed by those classes and
  the admin tests: the key check, the unreadable-key match in either direction, the key carry-over
  on a rebuild and the reset on a send, a sequence exemption, the primary key reaching the reader
  and being read after the contract is ensured, the wire-type check, each normalization, each
  rejected encoding, the length prefix that keeps columns apart, the absent single-column key, the
  `column_name` lookup, the decimal scale check and both encoded scales, `NUMERIC(P)` read as
  scale zero, the warning and its suppression on a rebuild, and the retry and failure typing of the
  key read. The survivor drops the eviction's removal of the key from the writer's map, which
  changes no behavior, because a returning table reads its key again; the removal keeps the map
  from holding tables a dynamic-destination job no longer writes to.

Measured against the service on 2026-10-31, from the same workstation, writing to CDC tables in
`us-central1` that the sink created, with a `STRING` primary key and no maximum staleness: each
configuration three times, alternating which mode went first, each into a fresh table, and the
table's rows read back after every write had committed.

| Workload | `NONE` | `PER_TABLE` |
|---|---|---|
| 500 keys, each changed twice in a row | 1 request, 0.21–0.24 s; the first change won for all 500 keys, in every run | 501 requests, 84.8–85.5 s; the last change won for all 500 keys, in every run |
| 1 key changed 200 times | 1 request, 0.16–0.17 s; the first change won, in every run | 200 requests, 33.9–34.0 s; the last change won, in every run |

- **The service resolved one key's rows within a request to the first of them.** It did so for
  all 1,504 keys that had two or more changes in one request (the three runs of each workload and
  a warm-up of one key), so the gap this split closes is not only an undefined order in principle
  but an older value kept in practice. One reading is that the rows of one request share an
  ingestion time and the tie goes to the first row; nothing documents the tie-break, and the
  measurement does not show the mechanism.
- **Successive requests did not tie.** Under `PER_TABLE`, the two changes of each of 1,500 keys
  were in adjacent requests one round trip apart, and the later change won every time; the hot
  key's last change won over its 199 earlier ones in each run. The ingestion time the service
  assigns therefore ordered requests one round trip apart in every case measured. The documentation does not promise it, and a client much
  closer to the service has shorter round trips; that was not measured.
- **The cost is one round trip per change of a hot key**, about 0.17 s from this client, and a
  workload whose keys repeat within a request's worth of rows pays it once per repeat.

## Alternatives declined

- **Deriving a `_CHANGE_SEQUENCE_NUMBER` in the writer.** It covers CDC only, and a writer-local
  counter does not survive a restart or a rescale that moves a key to another subtask, so it would
  need a clock component, which is the timestamp-only ordering [ADR-0111] declines.
- **Keeping requests in flight and holding only the requests behind a failed one.** By the time the
  failure is seen, later requests may already have committed; nothing on the default stream
  guarantees that they fail with it.
- **A per-key sequencer like [ADR-0187]'s for CDC tables.** BigQuery commits a request as a whole,
  so the request, not the key, is the unit a failure reorders; reading the key from each row, as
  the split does, does not change that.
- **Comparing CDC keys as GoogleSQL values** ([#1936]). Parsing every text spelling BigQuery accepts
  for a `TIMESTAMP`, `DATETIME`, `DATE` or `NUMERIC` key would reproduce the service's parser, and a
  parse that disagreed with it in one case would split two spellings of one key, which is the gap
  the split exists to close. Treating those encodings as unreadable costs requests instead, and the
  Table API never writes them.
- **Returning the key from the CDC provisioning read** ([#1936]). It would save one `tables.get`
  per table activation under the mode, at the cost of widening `ensureCdcTable`, whose result
  today reports only whether a creation was requested and which every test admin implements. A
  separate read keeps the provisioning contract as it is.
- **Leaving a request whole under a sequence** ([#1936]). An unequal sequence decides the winner,
  but an equal one falls back to ingestion order. The first version of this refinement exempted
  every sequence; the independent review of its pull request found the tie, which three shipped
  profiles can produce. A writer with unique sequences has little reason to use the mode for CDC
  at all, so the split costs it little in practice.
- **Ordering across tables.** Rows of different tables have no commit order a reader can compare
  through BigQuery, and serializing them would cost every dynamic-destination job without a reader
  that benefits.
- **The same opt-in for Pub/Sub.** Ordering keys already express it, a constant key giving a
  subtask's arrival order at Pub/Sub's 1 MB/s per key, and without a key Pub/Sub delivers in no
  defined order.
- **The same opt-in for Cloud Tasks.** The service makes "no guarantees or best effort attempts" to
  execute tasks in any particular order
  ([Common pitfalls](https://cloud.google.com/tasks/docs/common-pitfalls)), so an order the sink kept
  could not reach the handler.

## Consequences

- A table has one append in flight per subtask under `PER_TABLE`, so its throughput is one round
  trip per request of up to `maxAppendRequestBytes`. Raising that cap is how a job with the mode on
  regains throughput.
- The order kept is the order one subtask received. The DataStream user keys the stream before the
  sink; the CDC Table sink gets it for one key when Flink shuffles the changelog by the primary key.
- A CDC key changed in quick succession costs one request per change, so a hot key writes at one
  round trip per change per subtask.
- A DataStream serializer that writes a key column as text, as floating point, or into a collated
  `STRING` makes every row of the table its own request; the warning at open names the column.
- The order inside one request remains undefined, but no CDC key appears in one request twice, so
  the remaining in-request question concerns rows of different keys, which do not overwrite one
  another.
- An append that timed out on the client can still be committed by the service after a later one,
  as in [ADR-0175], [ADR-0189] and [ADR-0190], and the writer cannot observe it.
- A restart re-sends the rows written since the last completed checkpoint after the rows that had
  already committed, which is the at-least-once duplicate. The re-sent rows keep the order the
  replay delivers them in, which the mode does not choose.
- `NONE` stays the default and its behavior is unchanged.

[#1618]: https://github.com/flink-gcp/flink-connector-gcp/issues/1618
[#1621]: https://github.com/flink-gcp/flink-connector-gcp/issues/1621
[#1936]: https://github.com/flink-gcp/flink-connector-gcp/issues/1936
[ADR-0017]: 0017-bigquery-error-handling-routes-row-verdicts-and-repairs-dead-writers.md
[ADR-0022]: 0022-exactly-once-uses-buffered-streams-reused-across-checkpoints-never-finalized.md
[ADR-0030]: 0030-a-missing-bigquery-table-does-not-answer-not-found.md
[ADR-0111]: 0111-the-bigquery-table-sink-maps-upsert-changelogs-and-keeps-cdc-metadata-out-of-the-schema.md
[ADR-0124]: 0124-the-stability-boundary-at-1-0-0-is-a-promoted-public-entry-surface-checked-by-japicmp.md
[ADR-0137]: 0137-a-cross-connector-name-diverges-only-to-name-a-real-difference.md
[ADR-0175]: 0175-the-datastore-sink-commits-batches-and-confirms-a-refusal-one-write-at-a-time.md
[ADR-0187]: 0187-the-firestore-sink-keeps-per-document-order-on-opt-in.md
[ADR-0189]: 0189-the-spanner-sink-keeps-per-row-order-on-opt-in.md
[ADR-0190]: 0190-the-bigtable-sinks-keep-per-row-order-on-opt-in.md
