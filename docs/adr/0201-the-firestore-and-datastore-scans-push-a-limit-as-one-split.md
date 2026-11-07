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

# ADR-0201: The Firestore and Datastore scans push a limit as one split

- Status: Accepted
- Date: 2026-11-07 (planner behavior measured on Flink 2.2.1 in batch and streaming mode; emulator behavior against the pinned Firestore emulator image in both modes)
- Issues: [#1838], [#1829] (the pushdown umbrella)
- Modules: firestore (`io.github.flink.gcp.connector.firestore.table.source`, `datastore.table.source`, `firestore.table`, `datastore.table`)
- Current behavior: `docs/content/docs/connectors/table/firestore.md` § Limit pushdown, `docs/content/docs/connectors/table/datastore.md` § Limit pushdown

## Context

Neither the `firestore` nor the `datastore` scan implemented `SupportsLimitPushDown`, so a statement such as `SELECT * FROM t LIMIT 10`, a common preview of a collection or kind, read and was billed for every document or entity.
Flink's limit operator does not stop a bounded source early; the source reads to its end.
Both services bill per document or entity returned, so the cost of a preview was the cost of a full scan.

[#1838] put three questions.
A pushed limit stops the read after that many results, so it is safe only when nothing between the limit and the scan can drop a row: how does that interact with the filters [ADR-0197] and [ADR-0184] push as necessary conditions?
A collection-group scan and a Datastore kind scan read several splits: does each split take the limit, or does the scan become one split?
And the Native-mode source shrinks a query's `limit` at a checkpoint ([ADR-0173]): does a pushed limit take that path?

## Evidence

The planner was asked to explain statements over both tables with a probe implementation that recorded each `applyLimit` call, on Flink 2.2.1, in batch and in streaming mode; the two modes agreed on every statement.

- `LIMIT n` directly above the scan, with or without a projection or a later expression such as `UPPER(s)`, calls `applyLimit(n)` and keeps Flink's own `Limit` above the scan.
- `LIMIT n OFFSET m` calls `applyLimit(n + m)`; the offset stays with Flink.
- `LIMIT 0` replaces the scan with an empty `Values`; no source is planned.
- **Any `WHERE` leaves a `Calc` between the limit and the scan, and no limit is pushed**: a pushed equality (`s = 'x'`, `id = 'k'`), whose predicate stays a residual under ADR-0197's and ADR-0184's contract, and a predicate nothing pushes (`n > 1`) alike.
- `ORDER BY ... LIMIT`, `GROUP BY ... LIMIT`, `DISTINCT ... LIMIT` and a limit over `UNION ALL` push nothing.
- A filter above a limited subquery (`SELECT * FROM (SELECT * FROM t LIMIT 5) WHERE ...`) pushes the limit and keeps the filter above it, which is the statement's meaning.

On the emulator, a collection-group table with `scan.partition.max-partitions = 4` fails at planning, because the emulator does not implement `PartitionQuery`, and the same table with `LIMIT 3` reads three documents, because the limited scan is one query.
A runtime source built from a scan given `applyLimit(3)`, run without Flink's limit above it, emitted three rows from a ten-document collection, a collection group, and a ten-entity kind, with a page size of two; with `applyLimit` made a no-op, the collection and the kind emitted all ten, and the collection group failed at planning, because the scan asked for partitions.

The Datastore-mode source already reads a query carrying a `limit` as one split (`SplittableQueries`, [ADR-0177]), and its builder refuses `splitCount` for such a query.
The Native-mode builder refuses `partitionCount` for a query, which is always one split.
Both sources page through a limited query and shrink the limit by what a checkpoint has passed ([ADR-0173], [ADR-0177]).

## Decision

Both scans implement `SupportsLimitPushDown`, and a pushed limit becomes the `limit` of the query the DataStream source reads.

**A limited scan is one split.**
A single-collection Native-mode scan already is.
A collection-group scan given a limit reads the group as one collection-group query with the limit, through `FirestoreCollectionQueryFactory`, instead of asking `PartitionQuery` for partitions.
A Datastore-mode scan given a limit reads the kind as one query with the limit, which `SplittableQueries` reads as one split.
`scan.partition.max-partitions` does not apply to a limited scan; `FirestoreScanConfig.applyTo` and `DatastoreScanConfig.applyTo` leave the count unset for a scan read as one split, which their builders would otherwise refuse.
The scan therefore emits at most the limit's number of documents or entities, and while no read fails its query reads no more.
Planning adds its own one-result probes (two in Native mode without a configured read time, one otherwise, one in Datastore mode).
A retry reads more: Datastore mode repeats the failed page's request, and in Native mode the client library resumes a broken stream from the last document received with the page's limit unchanged, so each such retry can read up to one page again, which the reader cuts off ([ADR-0173]); a page holds at most the remaining limit.
The bound is therefore on what the scan emits and on a read without failures, not an absolute cap on what is billed.

**The filter question is the planner's.**
The planner pushes a limit only when nothing stands between it and the scan, and both scans keep every predicate as a residual, so a limited scan never carries a pushed filter.
Each source still drops the limit if a scan were to hold both, because a limit beside a necessary condition could stop before a row Flink's residual keeps; a unit test holds that guard.

A limit beyond `Integer.MAX_VALUE` is not pushed, because both services' `limit` is a 32-bit integer; Flink's own limit still applies.

The pushed limit is the query's own `limit`, so a checkpoint shrinks it and a restored reader reads only what remains, as for a user's limited query.

## Consequences

- A preview statement reads at most `offset + fetch` documents or entities instead of the whole collection, group or kind when no read fails; planning probes and the reads of a retry are billed on top, as for any scan.
- A limited scan reads serially: a large limit over a large kind or group loses the parallel key ranges or partitions it would otherwise read in. No option turns the push off; a statement that wants a parallel read of most of a kind can drop the `LIMIT`.
- Which documents a limit returns is unspecified, as in SQL: they are the first the service returns for the query, in whatever order it answers an unordered query.
- `FirestoreTableLimitPushDownPlanTest` and `DatastoreTableLimitPushDownPlanTest` pin the accepted and declined statement shapes in both modes, so a planner change that pushed a limit through a residual would fail them.

## Alternatives declined

- **Each split takes the limit**, keeping the parallel read with a bound of the split count times the limit. Without `scan.partition.max-partitions`, a Datastore kind scan asks for at least twelve key ranges and up to 50,000, so `LIMIT 10` could read 120 to 500,000 entities, each billed; neither public builder can express a per-split limit, so it would also need an internal path into both sources. The owner chose one split (2026-11-07).
- **Declining the push for a scan of several splits**, pushing only into the single-collection Native-mode scan. It would leave a preview of a collection group or a kind a full, billed read.
- **An option to turn the push off.** No case needs one that dropping the `LIMIT` does not cover.

[ADR-0173]: 0173-the-firestore-source-reads-partition-cursor-ranges-at-one-read-time.md
[ADR-0177]: 0177-the-datastore-source-reads-key-ranges-the-client-librarys-splitter-cuts.md
[ADR-0184]: 0184-the-datastore-table-sink-upserts-one-kind-keyed-by-name-or-id.md
[ADR-0197]: 0197-the-firestore-scan-pushes-equalities-as-necessary-conditions.md
[#1829]: https://github.com/flink-gcp/flink-connector-gcp/issues/1829
[#1838]: https://github.com/flink-gcp/flink-connector-gcp/issues/1838
