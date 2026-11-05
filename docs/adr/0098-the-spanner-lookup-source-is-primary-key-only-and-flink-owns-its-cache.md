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

# ADR-0098: The Spanner lookup source point-reads by primary key or loads the scan, and Flink owns its cache

- Status: Accepted
- Date: 2026-08-11, revised 2026-08-12, 2026-10-04 and 2026-11-05
- Issues: [#504](https://github.com/flink-gcp/flink-connector-gcp/issues/504), [#529](https://github.com/flink-gcp/flink-connector-gcp/issues/529) (under
  [#223](https://github.com/flink-gcp/flink-connector-gcp/issues/223)), [#573](https://github.com/flink-gcp/flink-connector-gcp/issues/573), [#1643](https://github.com/flink-gcp/flink-connector-gcp/issues/1643), [#1845](https://github.com/flink-gcp/flink-connector-gcp/issues/1845)
- Modules: spanner, base (`table`)
- Current behavior: `docs/content/docs/connectors/table/spanner.md`

## Context

Flink lookup joins need a source runtime that turns equality keys into zero or more rows.
Spanner has native point reads for a complete primary key, while incomplete keys require a query or range scan with different cost and latency.
Flink also defines standard lookup cache options and owns the cache lifecycle around a connector lookup function.

## Decision

The Spanner dynamic source accepts a lookup only when the planner supplies equality predicates for every declared primary-key column.
It restores the declared composite-key order before calling Spanner.
Synchronous mode uses `readRow`; asynchronous mode uses `readRowAsync`.
Both modes qualify the table with the optional dialect-specific `named-schema` value used by the sink and bounded source.

Issue #1643 permits additional top-level physical scalar equality keys, including constants from ON or WHERE and comparisons with input fields.
The complete primary key alone addresses the read.
The shared `base.table.LookupKeyFilter` compares each additional key against the converted Flink row before returning it to Flink's cache.
Binary values compare by content; schema-marked values compare in their Flink carrier types rather than native Spanner semantics.
NULL keys and NULL additional result values produce no match.
Nested key paths, metadata and additional ARRAY, MAP or ROW keys are rejected during planning.
The helper snapshots comparison values per async invocation and normalizes complete PARTIAL key tuples without taking over Flink's cache lifecycle or policy.

The connector exposes Flink's `NONE` and `PARTIAL` cache modes and delegates partial-cache storage, expiry, and missing-key behavior to Flink.
The primary-key requirement above applies to these two modes, which address Spanner by point read.

Issue #1845 adds `FULL`.
Flink's full cache never loads through the FLIP-27 `SourceProvider` the bounded scan returns: it takes an `InputFormatProvider` on both supported lines, or on Flink 1.20 also a deprecated `SourceFunctionProvider` (ADR-0095), and loads through `InputFormatCacheLoader` (checked in Flink 1.20.5 and 2.2.1).
An `InputFormatProvider` is the one shape both lines accept.
`SpannerFullCacheInputFormat` instead runs the bounded scan's own configuration, built by the same builder call as `getScanRuntimeProvider`, so a load reads what a scan of the table would: projection, pushed filters, `scan.index`, partition hints, Data Boost and RPC priority.
The snapshot and reload contract is:

- **One snapshot per load**: a load opens a new batch read-only transaction at the scan's timestamp bound and reads every partition of that plan at its timestamp; the next load plans again and reads a newer snapshot.
  A `FULL` lookup is rejected while it is planned when `scan.timestamp-bound.read-timestamp` is set, because a fixed timestamp would reload identical rows until it left the version retention period and then fail every load; a scan of the same table keeps the option, and exact staleness is accepted.
  The partitions are read sequentially at that timestamp, so a load longer than the version retention period fails; a table meant for `FULL` is expected to load well inside it.
- **One split per load**: the format returns one input split whatever count Flink asks for, which plans the read and executes its partitions one after another through one reader client, then releases the transaction's session through the planner.
  Flink clones the format for every split of every load and the clones share nothing, so one split per partition would build a client per partition and leave no clone able to release the session the others read through.
- **A failed partition fails the load**: the exception reaches Flink's loader, which does not swap in the partial copy and fails the lookups that follow until the job restarts and loads again.
  The connector adds no retry of its own; the client's batch-read retry settings apply, as for a bounded scan, and `lookup.max-retries` remains a point-read option.

Flink indexes the loaded rows by the planner's lookup keys and keeps every row of a key, so `FULL` accepts any top-level physical scalar key, primary-key columns or not, and no `LookupKeyFilter` is involved.
The index compares keys with `GenericRowData.equals`, so a key type is accepted only when that equality agrees with SQL `=`: `FLOAT` and `DOUBLE` are rejected because boxed equality separates `-0.0` from `0.0` and matches `NaN` to itself, and `ARRAY`, the one nested type the Spanner schema maps, because binary and generic nested values are not equal to each other (the Bigtable finding in ADR-0095).
A schema-marked UUID key matches as its lowercase canonical string, while a primary-key point read parses the probe in either case; the difference is documented rather than normalized, because Flink's index compares the probe as the planner produced it.
`lookup.async = true` is rejected with `FULL`, as on Bigtable, because Flink has no asynchronous full-cache provider.
The standard `lookup.full-cache.*` options are imported unchanged and validated by constructing Flink's reload trigger while the DDL is planned.
The periodic interval must also be at least one millisecond (`OptionChecks.checkAtLeastOneMilli`, ADR-0068): Flink's trigger checks only that it is positive and then schedules it in whole milliseconds, so a shorter interval would plan and then fail as the job starts.
The loader exports no client metrics to Flink, because Flink opens it without a runtime context.
The connector retries only `ABORTED`, `DEADLINE_EXCEEDED`, and `UNAVAILABLE` point reads within the configured retry budget.
Those three are the connector's own choice rather than a mirror of the client's, because `readRow` reaches `StreamingRead` rather than the unary `Read` and `SpannerStubSettings` gives that RPC an empty retryable set (google-cloud-spanner 6.120.0); of the three, only `UNAVAILABLE` is retried underneath, so `ABORTED` and `DEADLINE_EXCEEDED` would be retried by nobody without this loop.
When the planner also pushes an exact primary-key predicate, both lookup modes reject a non-matching lookup key before opening a point-read RPC.
Predicates pushed through the scan filter ability that are not exact primary-key constraints remain Flink residuals.
Additional lookup equality keys are a separate planner channel and are evaluated by the lookup helper.
The bounded-scan `scan.index` option does not change lookup access paths.

The asynchronous retry loop is shared through `base.table.AsyncLookupRetries` ([ADR-0039](0039-retry-schedules-are-shared-retry-loops-are-not-and-every-schedule-jitters.md)); the read, conversion and failure classifier stay in this connector.

## Alternatives declined

- Loading a full cache with one input split per Spanner partition, to load in parallel: each split runs in its own clone of the format, so every partition would open its own client, and no clone could release the batch transaction's session after the others finished. A table meant for `FULL` is small enough to hold in every TaskManager, so the sequential read is not expected to be the bottleneck; a measured case would reopen this.
- Keeping `scan.timestamp-bound.read-timestamp` with `FULL`, so a reload reread a fixed snapshot: the reload would do no useful work and would fail once the timestamp left version retention.
- Retrying `RESOURCE_EXHAUSTED`, which the sink's `SpannerErrorClassifier` does treat as transient. What separates the two is backoff rather than polarity: the sink retries on a `RetrySchedule` and sleeps between attempts (ADR-0075), so it can serve out the wait this status asks for, and the client honours that wait too, retrying the status on this read path precisely when the server attached a retry delay. The lookup loop has no backoff at all, so including it would re-issue at once and spend the whole budget against the wait the server asked for. That is the shape ADR-0084 records on the BigQuery read path.

## Consequences

Lookup cost is one native point read on a cache miss when no retryable failure occurs, and absent rows naturally produce an empty result.
Prefix, range, and lookups without the complete primary key remain unsupported in `NONE` and `PARTIAL`; `FULL` serves them from memory at the cost of reading the whole table once per TaskManager per load, and of holding two copies while a reload runs.
PARTIAL stores filtered hits and misses under the whole key tuple; different additional comparison values do not share entries.
Emulator tests exercise constant predicates in ON and WHERE and input-field keys with NONE and PARTIAL, sync and async, in both dialects.
Emulator tests also exercise a `FULL` load joined on the primary key and on a non-key column in both dialects, a periodic reload that observes a changed row, and the planning rejection of `lookup.async` with `FULL`.
The lookup function owns and closes its Spanner service handle.
