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

# ADR-0037: `base.metrics` helpers, and `numRecordsSend` counts each record once at first hand-off

- Status: Accepted
- Date: 2026-08-03; revised by [#1056] (2026-08-23)
- Issues: [#208] (first consumers Pub/Sub and Cloud Tasks [#209]), which superseded the
  [#37] design's "retries re-count"; expanded by [#1056]
- Modules: base (consumed by every connector)
- Current behavior: each connector's docs page § Metrics

## Decision

`base.metrics` is `@Internal` throughout — nothing here is implemented by a user. Three types:

- `MetricValues.elapsedMillis` returns zero for equal or future values and saturates subtraction
  overflow at `Long.MAX_VALUE`, so Bigtable and Spanner lag and health gauges share one boundary
  policy ([#1056]).
- `ErrorClassCounters` registers `errorClass.CODE.errors`, `CODE` being a gax `StatusCode.Code`
  name or `UNCLASSIFIED`; child counters are created on first use (registering ~17 rows per
  subtask for statuses a job never sees is what the laziness avoids), and **which throwable in a
  chain classifies a failure stays at the call site**, exactly as `StatusCodes.codeOf` leaves
  traversal there — the [#61] do-not-converge decision, extended to metrics.
- `DestinationMetrics` is the opt-in per-destination pair (`recordsSend`/`sendErrors`,
  `perDestinationMetrics` default false everywhere): **Flink cannot unregister a metric**, so an
  unconditional subgroup per destination would grow the registry for the task's lifetime against
  an unbounded destination set, and the same fact is why **entries are never removed** — a
  rebuilt writer state reuses its counters, since re-registering the name would be refused. It
  hands out a `Counters` handle rather than taking a destination name per record, so the name is
  composed once and a disabled instance costs two null checks. `Counters.recordsSent(long)` is
  the batching form of the same counter, added for BigQuery ([#210]).
- Both counter helpers are **task-thread only** — plain `SimpleCounter`s, valid because every sink
  increment site in this repository runs on the task thread, unlike the Pub/Sub *source*, whose
  SDK callback threads forced `ThreadSafeSimpleCounter`. A connector counting from a callback
  thread must not reuse them as they stand. `flink-test-utils` is a *test*-scope dependency here
  for `MetricListener`, so the helpers are asserted through the names they register under.

**`numRecordsSend` counts each record once, at the first hand-off, in every connector**
(superseding the [#37] design's "retries re-count"): a sink-owned retry — Pub/Sub's
topic-creation republish, Cloud Tasks' park-and-redispatch, BigQuery's re-append — must not
count the record again. The increment goes **inside the send call, guarded by a first-attempt
flag** — not at the `write()` call site, which would count a record the client rejected
synchronously, and not unguarded, which would count attempts. Bigtable already counts once
because its retries are inside the SDK batcher, so the four connectors report one quantity and a
dashboard comparing them is honest. What is given up is stated on the docs pages:
`numBytesSend` is payload volume, not wire volume. Retry volume is read from the
`errorClass.CODE.errors` counters instead, which is per status code and strictly more
informative than a re-counted send.

[#37]: https://github.com/flink-gcp/flink-connector-gcp/issues/37
[#61]: https://github.com/flink-gcp/flink-connector-gcp/issues/61
[#208]: https://github.com/flink-gcp/flink-connector-gcp/issues/208
[#209]: https://github.com/flink-gcp/flink-connector-gcp/issues/209
[#210]: https://github.com/flink-gcp/flink-connector-gcp/issues/210
[#1056]: https://github.com/flink-gcp/flink-connector-gcp/issues/1056
