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

# ADR-0193: BigQuery CDC writes require a change sequence number

- Status: Accepted
- Date: 2026-10-31 (the default stream measured 2026-10-31 against the service from a workstation,
  dataset in `us-central1`); rationale refined 2026-10-31 ([#1965])
- Issues: [#1945], [#1618], [#1936], [#1959], [#1965]
- Modules: bigquery (`sink.cdc`, `sink.storage.writer`, `sink.tables`, `table.sink`)
- Current behavior: `docs/content/docs/connectors/table/bigquery.md` § Change data capture;
  `docs/content/docs/connectors/datastream/bigquery.md` § Change data capture and § Commit order

## Context

[ADR-0110] and [ADR-0111] made the `_CHANGE_SEQUENCE_NUMBER` pseudocolumn optional, as the service
does.
BigQuery's [CDC documentation](https://docs.cloud.google.com/bigquery/docs/change-data-capture)
calls the pseudocolumn "an optional column" and says that without it the service orders one primary
key's mutations by "the BigQuery system time".
The rows its UPSERT and DELETE example streams carry `_CHANGE_TYPE` as their only pseudocolumn, and
each of the example's three keys changes once.
A sequence-less CDC write is therefore the vendor's documented default.
The same page's [custom ordering section](https://docs.cloud.google.com/bigquery/docs/change-data-capture#manage_custom_ordering)
warns that system-time ordering "might not be sufficient" when a key is upserted frequently in a
short window or "where the upsert order is not guaranteed", in which case "a user-supplied ordering
key might be necessary".
Apache Beam's `BigQueryIO`, the BigQuery connector a Dataflow pipeline writes through, requires one:
its [`RowMutationInformation`](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/bigquery/RowMutationInformation.html)
states that "A sequence number must always be supplied to order the updates", both of its factory
methods take a sequence, and the `String` form rejects a null or empty value.

[ADR-0192] measured the consequence inside one append request: the first of a key's changes won
for all 1,504 keys tried.
For [#1936] it therefore split a request before a repeated key under `OrderingMode.PER_TABLE`,
reading the table's primary key and each row's key from the serialized bytes.
[#1945] asked the same question of the default `OrderingMode.NONE`, under which a key changed twice
within one request's worth of rows still kept its older value, with no failure and no metric.

## Decision

- **A CDC write requires a sequence.** `CdcOptions.Builder#build()` rejects options without a
  sequence-number provider, and the Table API sink rejects `sink.cdc.enabled = true`
  during planning unless the DDL declares one of the sequence metadata columns
  (`change-sequence-number`, `debezium-source-properties`, `spanner-change-sequence`).
  The owner decided on 2026-10-31 that a CDC write without a key that orders its changes is not
  worth supporting: the measurement below shows that the sink cannot make ingestion order follow
  arrival order except by waiting a round trip, and the input the CDC path is designed for, a
  Debezium-format changelog, carries a sequence through the built-in profiles.
  The owner re-examined the decision against the vendor's sequence-less default on 2026-10-31
  ([#1965]) and kept it.
  In the measurement below, two changes of one key in one request resolved to the first, and only
  requests awaited one at a time left the last change in place, so the system-time order the
  documentation describes is not the order of rows within a request; a Flink sink puts many rows
  into one request where the vendor's example changes each key once, and Beam's connector requires
  a sequence too (Context).
  Any other source supplies, through `change-sequence-number` or a custom
  `CdcSequenceNumberProvider`, a value that never repeats for one primary key and grows with each
  of its changes, such as a nanosecond timestamp or an increasing ID.
  The value has to be taken where the change is made, as a source's commit time is: a clock read in
  the writer orders changes by when they reached it, which is the timestamp-only ordering
  [ADR-0111] declines.
  Declaring the column is not enough on its own: Flink fills a column an `INSERT` column list
  leaves out with `NULL`, which would send every row of that statement to the row-failure path.
  The sink therefore also rejects, during planning, a CDC `INSERT` whose column list does not name
  the sequence column ([#1959]).
  It reads `DynamicTableSink.Context#getTargetColumns()`, measured on Flink 1.20.4 and 2.2.1 to
  index every column of the resolved schema, computed and metadata columns included, and to be
  empty when the statement has no column list.
- **Equal sequences are resolved by arrival order only under `OrderingMode.PER_TABLE`.** BigQuery
  falls back to ingestion time for two changes of one key with equal sequences, and the writer
  cannot tell whether a provider can produce them.
  Under `NONE` such changes are in no guaranteed order.
  Under `PER_TABLE` the [ADR-0192] key split stays as it is: a request ends before a row whose key
  it already holds, so equal sequences commit in the order the subtask received them.
  Equal sequences arise in ordinary pipelines (Evidence below), so the split is not only for a
  source that repeats a value.

## Evidence

Measured on 2026-10-31 from one writer on a workstation outside Google Cloud, far from the dataset's
region, writing to CDC tables in `us-central1` that the sink created with a `STRING` primary key and
no maximum staleness.
A local prototype (not shipped) varied what `NONE` does with a repeated sequence-less key; each
split and wait configuration ran three times, the control once, each run into a fresh table, and the
table was read back after every write had committed.
The pool column is the size of the client library's connection pool after the run, from its
`Scaling up new connection` log lines.

| Variant | Workload | Requests | Pool | Keys that kept an older value | Time |
|---|---|---|---|---|---|
| Today's `NONE` (control) | 500 keys, each changed twice in a row | 1 | 1 | 500 of 500 | 0.27 s |
| Split before a repeated key, requests in flight together | 10 keys, each changed twice | 11 | 1 | 8, 5 and 9 of 10 | 0.18–0.29 s |
| The same | 500 keys, each changed twice | 501 | 20 | 472–477 of 500 | 0.59–0.73 s |
| The same | 1 key changed 200 times | 200 | 10 | kept change 191, 106 and 126 instead of the last, 199 | 0.44–0.54 s |
| Split, and await the in-flight request that holds the key | 500 keys, each changed twice | 501 | 1 | 0 of 500 | 85.2–85.5 s |
| The same | 1 key changed 200 times | 200 | 1 | 0 | 34.1–35.2 s |

- **Requests in flight together did not order a key's changes, even on one connection.** With 11
  requests and no second connection opened, 22 of the 30 keys over three runs still kept their
  first change, so the gap is
  not the connection pool spreading requests across connections, although with more requests in
  flight the pool did grow.
  The first change winning that often is consistent with requests sent back to back sharing
  an ingestion time; nothing documents the tie-break, and the measurement does not show the
  mechanism.
- **Only waiting for the earlier request made the last change win**, in every run, at one round
  trip per repeat: the same cost [ADR-0192] measured for `PER_TABLE`.
- **The client library reorders requests on one connection on its own.** A default-stream request
  that fails with a retryable in-stream status is put back at the front of the waiting queue
  without recalling the requests sent after it (`ConnectionWorker` in google-cloud-bigquerystorage
  3.36.0, which libraries-bom 26.91.0 resolves; 3.35.0 reads the same), and the writer only sees the future complete once the retry resolves.
  No such retry was logged during the runs above.
- Every table was deleted after the run; the dataset was listed empty afterwards.

Read from source and planned on 2026-10-31:

- **A delete Flink completes carries the sequence of the change it deletes.** The planner adds
  `ChangelogNormalize` after an upsert source, always on Flink 1.20 and on Flink 2.x when the
  query reads a metadata column, filters or needs update-before rows, and its
  `DeduplicateFunctionHelper#processLastRowOnChangelog` emits the last row it holds, set to
  `DELETE`, rather than the delete it received (Flink 2.2.1 source; the 1.20.4 bytecode does the
  same), and emits nothing when it holds no row; a metadata-derived sequence is part of that row.
  `explain` of the Bigtable selected-cell example on Flink 2.2.1 and 1.20.4 shows the operator
  between the source and the sink.
  The PostgreSQL, TiCDC and Spanner profiles have residual ties of their own ([ADR-0111]).
- **The Bigtable example's sequence expression** was evaluated on Flink 2.2.1 and 1.20.4 in UTC,
  Asia/Kolkata and America/New_York over six rows at five instants spanning a minute boundary, 2040 and tie
  breakers down to the `INT` minimum; both versions gave identical output in the expected order.
  `TIMESTAMPDIFF(SECOND, ...)` returned `-2085978496` for 2040, which is why minutes have a section
  of their own.

## Alternatives declined

- **Splitting requests by key under `NONE` and keeping them in flight.** Measured above to leave the
  older value for most keys, and exposed to the client library's own retry reordering.
- **Removing the per-table key split once a sequence is required.** It was removed in a first
  version of this change, on the reasoning that a never-repeating sequence leaves it nothing to
  order, and restored on the owner's review: the delete `ChangelogNormalize` completes ties with
  the change it deletes in an ordinary pipeline, and without the split the two can share a request,
  in which the first change won in every measured case.
- **Splitting and awaiting the request that holds the key, under `NONE`.** It made the last change
  win in the measurement, and both a Fable and a Codex consultation on 2026-10-31 preferred it to
  the split alone.
  It was declined because it emulates an ordering key from timing: it orders only what one subtask
  has in flight, not changes on either side of a restart or a rescale, it costs a round trip per
  repeat on the default mode, and it needs the table's key read from every serialized row.
  A sequence orders all of those cases without the writer.
- **Keeping the sequence optional and documenting the gap.** It leaves a key changed twice within
  one request silently at its older value under the default mode, which no metric shows.
  It is the vendor's documented default (Context), so the owner re-examined it on 2026-10-31
  ([#1965]) and kept the requirement on the Decision's grounds.
- **Accepting a sequence-less CDC write only under `PER_TABLE`** (owner, 2026-10-31, [#1965]).
  The split and the await already exist for that mode, and the split-and-await rows above left no
  key at an older value.
  It was declined because the await orders only what one subtask has in flight, so the gaps the
  `NONE` variant above names remain, at one round trip per repeat (34–35 s for one key changed 200
  times above), and because it would add a second documented behaviour for an input the built-in
  profiles never produce.
- **A new ordering mode for sequence-less CDC** (owner, 2026-10-31). The mode would exist to repair
  an input this decision no longer accepts; `PER_TABLE` already orders commits for CDC and
  append-only tables alike.
- **Deriving a sequence in the writer.** [ADR-0192] declines it: a writer-local counter does not
  survive a restart or a rescale, and a clock component is the timestamp-only ordering [ADR-0111]
  declines.

## Consequences

- A sequence-less CDC job that 1.2.0 accepted fails on upgrade: a DataStream job when it builds its
  `CdcOptions`, and a Table API job during planning.
  The CDC surface is `@Experimental` ([#706]), which allows the change at a minor release; the
  release notes owe it an entry.
- A catalog table (`docs/content/docs/connectors/table/bigquery.md` § Catalog) has no column to
  carry the sequence, so a CDC write to one goes through a table created `LIKE` it with a sequence metadata
  column added, rather than through an `OPTIONS` hint alone ([ADR-0169]).
- The Bigtable selected-cell example builds its sequence from the commit timestamp and tie breaker
  and sets `PER_TABLE`, because its deletes carry the sequence of the change they delete.
  It states what stays outside the order: a delete next to a Change Streams partition split or
  merge, a delete behind an append that timed out on the client and commits late, and the delete
  of a key only a backfill wrote, which `ChangelogNormalize` drops.
- Every CDC row carries `_CHANGE_SEQUENCE_NUMBER`, so the sink no longer writes rows without one
  into a table it also writes rows with one, which BigQuery documents as leaving their order unpredictable.

[#706]: https://github.com/flink-gcp/flink-connector-gcp/issues/706
[#1618]: https://github.com/flink-gcp/flink-connector-gcp/issues/1618
[#1936]: https://github.com/flink-gcp/flink-connector-gcp/issues/1936
[#1945]: https://github.com/flink-gcp/flink-connector-gcp/issues/1945
[#1959]: https://github.com/flink-gcp/flink-connector-gcp/issues/1959
[#1965]: https://github.com/flink-gcp/flink-connector-gcp/issues/1965
[ADR-0110]: 0110-bigquery-cdc-augments-default-stream-rows-without-changing-the-table-schema.md
[ADR-0111]: 0111-the-bigquery-table-sink-maps-upsert-changelogs-and-keeps-cdc-metadata-out-of-the-schema.md
[ADR-0169]: 0169-the-bigquery-catalog-maps-a-projects-datasets-and-rest-schemas-to-flink-tables.md
[ADR-0192]: 0192-the-bigquery-default-stream-keeps-arrival-order-per-table-on-opt-in.md
