<!--
Copyright 2026 laughingman7743

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
- Date: 2026-08-11, revised 2026-08-12
- Issues: [#504](https://github.com/laughingman7743/flink-connector-gcp/issues/504), [#529](https://github.com/laughingman7743/flink-connector-gcp/issues/529) (under
  [#223](https://github.com/laughingman7743/flink-connector-gcp/issues/223)), [#573](https://github.com/laughingman7743/flink-connector-gcp/issues/573)
- Modules: spanner
- Current behavior: `docs/content/docs/connectors/table/spanner.md`

## Context

Flink lookup joins need a source runtime that turns equality keys into zero or more rows.
Spanner has native point reads for a complete primary key, while incomplete keys require a query or range scan with different cost and latency.
Flink also defines standard lookup cache options and owns the cache lifecycle around a connector lookup function.

## Decision

The Spanner dynamic source accepts a lookup only when the planner supplies equality predicates for every declared primary-key column.
It restores the declared composite-key order before calling Spanner.
Synchronous mode uses `readRow`; asynchronous mode uses `readRowAsync`.
Both modes qualify the table with the optional dialect-specific `schema` value used by the sink and bounded source.

The connector exposes Flink's `NONE` and `PARTIAL` cache modes and delegates partial-cache storage, expiry, and missing-key behavior to Flink.
It rejects `FULL` because a full cache would require a scan and a separately defined snapshot and reload contract.
The connector retries only `ABORTED`, `DEADLINE_EXCEEDED`, and `UNAVAILABLE` point reads within the configured retry budget.
When the planner also pushes an exact primary-key predicate, both lookup modes reject a non-matching lookup key before opening a point-read RPC.
Predicates that are not exact primary-key constraints remain Flink residuals.
The bounded-scan `scan.index` option does not change lookup access paths.

## Consequences

Lookup cost is one native point read on a cache miss when no retryable failure occurs, and absent rows naturally produce an empty result.
Prefix, range, and non-key lookups remain unsupported.
The lookup function owns and closes its Spanner service handle.
