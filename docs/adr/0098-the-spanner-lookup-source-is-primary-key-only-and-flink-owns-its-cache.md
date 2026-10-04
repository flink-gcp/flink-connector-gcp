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

# ADR-0098: The Spanner lookup source is primary-key-only and Flink owns its cache

- Status: Accepted
- Date: 2026-08-11, revised 2026-08-12 and 2026-10-04
- Issues: [#504](https://github.com/flink-gcp/flink-connector-gcp/issues/504), [#529](https://github.com/flink-gcp/flink-connector-gcp/issues/529) (under
  [#223](https://github.com/flink-gcp/flink-connector-gcp/issues/223)), [#573](https://github.com/flink-gcp/flink-connector-gcp/issues/573), [#1643](https://github.com/flink-gcp/flink-connector-gcp/issues/1643)
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
It rejects `FULL` because a full cache would require a scan and a separately defined snapshot and reload contract.
The connector retries only `ABORTED`, `DEADLINE_EXCEEDED`, and `UNAVAILABLE` point reads within the configured retry budget.
Those three are the connector's own choice rather than a mirror of the client's, because `readRow` reaches `StreamingRead` rather than the unary `Read` and `SpannerStubSettings` gives that RPC an empty retryable set (google-cloud-spanner 6.120.0); of the three, only `UNAVAILABLE` is retried underneath, so `ABORTED` and `DEADLINE_EXCEEDED` would be retried by nobody without this loop.
When the planner also pushes an exact primary-key predicate, both lookup modes reject a non-matching lookup key before opening a point-read RPC.
Predicates pushed through the scan filter ability that are not exact primary-key constraints remain Flink residuals.
Additional lookup equality keys are a separate planner channel and are evaluated by the lookup helper.
The bounded-scan `scan.index` option does not change lookup access paths.

The asynchronous retry loop is shared through `base.table.AsyncLookupRetries` ([ADR-0039](0039-retry-schedules-are-shared-retry-loops-are-not-and-every-schedule-jitters.md)); the read, conversion and failure classifier stay in this connector.

## Alternatives declined

- Retrying `RESOURCE_EXHAUSTED`, which the sink's `SpannerErrorClassifier` does treat as transient. What separates the two is backoff rather than polarity: the sink retries on a `RetrySchedule` and sleeps between attempts (ADR-0075), so it can serve out the wait this status asks for, and the client honours that wait too, retrying the status on this read path precisely when the server attached a retry delay. The lookup loop has no backoff at all, so including it would re-issue at once and spend the whole budget against the wait the server asked for. That is the shape ADR-0084 records on the BigQuery read path.

## Consequences

Lookup cost is one native point read on a cache miss when no retryable failure occurs, and absent rows naturally produce an empty result.
Prefix, range, and lookups without the complete primary key remain unsupported.
PARTIAL stores filtered hits and misses under the whole key tuple; different additional comparison values do not share entries.
Emulator tests exercise constant predicates in ON and WHERE and input-field keys with NONE and PARTIAL, sync and async, in both dialects.
The lookup function owns and closes its Spanner service handle.
