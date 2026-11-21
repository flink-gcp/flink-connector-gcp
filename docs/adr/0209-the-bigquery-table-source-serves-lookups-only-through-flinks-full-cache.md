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

# ADR-0209: The BigQuery table source serves lookups only through Flink's full cache

- Status: Accepted
- Date: 2026-11-21
- Issues: [#1848](https://github.com/flink-gcp/flink-connector-gcp/issues/1848)
- Modules: bigquery, base
- Current behavior: `docs/content/docs/connectors/table/bigquery.md` § Lookup

## Context

ADR-0100 made the `bigquery` table source a bounded scan and stated that it offers no lookup,
because the Storage Read source offers no point read.
That statement still holds for a read per probe row: a lookup without a cache would create a read
session, or run a query, for every row of the stream, which neither API is made for.

A periodically reloaded copy of a small table is a different matter.
Flink's `FULL` lookup cache loads a table through an `InputFormat`, keeps one copy per lookup
function on each TaskManager, indexes it by the planner's lookup keys and swaps in a new copy on
each reload.
The Spanner, Firestore and Datastore table sources already serve `FULL` from their scans
(ADR-0098, ADR-0179, ADR-0184), and a Storage Read session gives each load a consistent snapshot.
The owner decided on 2026-11-07 to support it, with no size guard and with the per-load read cost
stated in the documentation.

## Decision

**The source implements `LookupTableSource` and answers only `lookup.cache = FULL`.**
`NONE` and `PARTIAL` are refused when a lookup join is planned, with a message naming the option.
The factory does not refuse them, because the same DDL may be scanned under any cache value.
The factory accepts `lookup.cache` and the five `lookup.full-cache.*` options and checks the reload
schedule through the shared `FullCacheReloadConfig` when the cache is `FULL`.
No `lookup.async`, `lookup.max-retries` or `lookup.partial-cache.*` option is accepted: none of
them has anything to configure under a full cache.

**A load reads a table, as the table currently is.**
A lookup is refused when the table sets `scan.query`, or `scan.materialize-views` to `true`: a
load reads a table's storage directly, and every reload would otherwise run the query, or check
for a view and materialize it, again.
A table that is not a view is looked up with the flag left at `false` and without the
`scan.query-*` and `scan.reuse-query-result-within` options, which a direct table read refuses.
It is also refused when the table sets `scan.snapshot-time`: every load would read the same
snapshot until the instant leaves the table's time travel window, and then fail.
Spanner refuses a fixed read timestamp under `FULL` for the same reason (ADR-0098).
Firestore and Datastore ignore their `scan.read-time` in a load instead, to match their point
lookups (ADR-0179); BigQuery has no point lookup to match.

**A load is the scan's read, built by the same builder.**
`BigQueryDynamicSource` builds the scan and the load through one method, so the load carries the
planner's projection and pushed filters, `scan.row-restriction`, `scan.parent-project`, the fetch
bounds, `scan.retry.max-attempts`, the credentials and the emulator endpoint exactly as the scan
does.
The load's configuration differs in two values only: it carries no snapshot time, and it asks for
one stream.

**One input split per load, one read session per split, one stream per session.**
Flink's loader runs the splits of one load on several threads that share one cache-entry
serializer, and that serializer corrupts a generic map copied on two threads at once (#2217).
This table produces `MAP` and `MULTISET` values as generic maps, so a load has one split.
One split reads its streams one after another, so a second stream would add a `ReadRows` call and
read nothing faster; the load requests `maxStreamCount = 1` and no preferred minimum.
`scan.max-stream-count` and `scan.preferred-min-stream-count` remain the scan's.

**The session is created when the split is opened, not when it is planned.**
Flink plans splits on the format it holds, then opens and closes a deserialized copy of it
(`InputFormatCacheLoader`, `InputSplitCacheLoadTask`, Flink 2.2.1 and 1.20.5).
A client opened while planning would therefore have nothing to close it.
`BigQueryFullCacheInputFormat` creates the session in `open`, closes the session-creating client
as soon as the session exists, reads the streams through the scan's `BigQuerySplitReader` and
closes that reader and its client in `close`.
A release failure after a complete read is reported as an `IOException`, which Flink's loader logs
while keeping the rows, as the other connectors' loads do.
Because no runtime context reaches the format, the reader's counters are unregistered and each
load logs its session, stream count, rows, bytes and retries instead.

**Keys may be any top-level column whose Flink equality is SQL's.**
The shared `FullCacheLookupKeys` allowlist gains `TINYINT`, `SMALLINT`, `INTEGER`, `TIME` and
`TIMESTAMP`, the types this table produces and the other tables do not.
Integers compare by value, a `TIME` by its milliseconds of the day, and a `TIMESTAMP` by its
`TimestampData`, whose `equals` and `hashCode` cover the millisecond and the nanoseconds within it,
so two stored values that the converter truncates to the same declared precision share a key, as
they would in a regular join.
`FLOAT`, `DOUBLE`, `ARRAY`, `MAP`, `MULTISET` and `ROW` stay refused.

## Evidence

Measured on 2026-11-21 against the BigQuery emulator with Flink 2.2.1
(`BigQueryTableFullCacheITCase`):

- In streaming mode, a `WHERE` condition of an inner join on the looked-up table reached the load
  as a row restriction and left out a row whose value no Flink `INT` holds; in batch mode the same
  join read that row and its load failed.
  `scan.row-restriction` narrowed the load in both modes: the batch join, which no pushed condition
  reaches, and a streaming left join whose condition `d.n IS NULL` holds only after the join and
  stays there, so row 3 was left out by the option alone.
  This matches the planner behaviour recorded for the other connectors (#2219).
- A periodic reload picked up a row changed with DML between two loads.
- The emulator fails a read of a `NULL` in a nullable column and of any `DATETIME` value
  (`cannot encode binary`), so the emulator tests hold no `NULL` dimension value and check a
  `TIMESTAMP` key at planning only.

## Consequences

- Each cache instance reads the projected table once at startup and once per reload, and BigQuery
  meters each load by the bytes its `ReadRows` calls scan, as it meters a scan.
  The documentation states that cost and the memory a reload needs for two copies.
- The loading TaskManager now creates read sessions, so its credentials need the session-creating
  permission on the parent project as well as read access to the table.
- A load that outlives its session's lifetime fails like any other failed load: Flink stops reloading
  after a failure, so the lookups fail until the job restarts and loads again with a new session.
- A failed load follows Flink's full-cache behaviour, which the other connectors already document:
  later lookups fail until the job restarts, apart from the first-load window of #2225.

## Alternatives declined

- **A point read per probe row, with or without a partial cache.**
  Storage Read has no row lookup, and a query per row costs a job per row; ADR-0100's statement
  about that case stands.
- **Ignoring `scan.snapshot-time` in a load,** as Firestore ignores `scan.read-time`.
  A load would then join different data than the DDL names without saying so.
- **Honouring `scan.max-stream-count` in a load.**
  One split reads streams serially, and several splits are ruled out by #2217.
- **Lifting Firestore's abstract `FullCacheInputFormat` into the base module.**
  The base module depends on neither `flink-connector-base` nor `google-cloud-core`, and the
  BigQuery load needs neither the shared read-time split nor the planning hook, because a read
  session supplies both its snapshot and its expiry.
