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

# ADR-0109: Bigtable Change Streams estimates do not become native source watermarks

- Status: Accepted
- Date: 2026-08-13
- Issues: [#604](https://github.com/laughingman7743/flink-connector-gcp/issues/604)
- Modules: bigtable (`source.changestream`, `table.source`)
- Current behavior: [Change Streams source](../content/docs/connectors/datastream/bigtable.md#change-streams-source)

## Context

Each Bigtable Change Streams mutation and heartbeat carries an `estimatedLowWatermark` for its service partition.
The source checkpoints that value with assigned, reader-queued and enumerator-held splits, pending merges and missing partitions.
A coordinator could therefore calculate a monotonic minimum across the complete partition ledger instead of relying on the active split outputs that Flink currently sees.

That aggregation would solve only the connector-side omission.
Bigtable defines the field as an estimate that is usually no later than a future record's commit timestamp, while explicitly allowing a future record with an older timestamp.
The [Change Streams overview](https://cloud.google.com/bigtable/docs/change-streams-overview#watermarks) likewise says that the estimate does not guarantee that no data remains to arrive.
The service publishes no finite maximum for this exception.

Flink gives a source watermark a stronger meaning.
It tells downstream operators that no later element will have a timestamp at or below the watermark, which lets them fire and eventually delete event-time state.
`SupportsSourceWatermark` asks the planner to rely fully on the source's own policy rather than insert a job-defined watermark expression.

## Decision

The Bigtable Change Streams source does not emit connector-native watermarks.
Its builder gains no estimated-watermark opt-in, and its Table source does not implement `SupportsSourceWatermark`.
An opt-in would express willingness to accept late data, but it would not turn the service estimate into the non-early frontier that the interface describes.

The partition estimate remains checkpoint progress, readable Table metadata and an operational lag signal.
Those uses tolerate an estimate because they neither close an event-time window nor discard a later record.

A DataStream job may supply its own `WatermarkStrategy`, and a Table DDL may declare an ordinary watermark expression over the `commit-timestamp` metadata column.
Both are application-owned heuristics.
The application chooses the delay, idleness and late-data policy, and no configured delay is presented as a Bigtable guarantee.

Spanner does not supply the same service contract.
Its heartbeat states that the partition has returned every change at or before the heartbeat timestamp and that future records have greater commit timestamps.
The connector still has to aggregate scheduled and queued Spanner partitions before claiming a stream-wide frontier; [#635](https://github.com/laughingman7743/flink-connector-gcp/issues/635) owns that separate protocol audit.

## Evidence

- The [Bigtable data RPC reference](https://cloud.google.com/bigtable/docs/reference/data/rpc/google.bigtable.v2#heartbeat) permits a future record below a previously observed estimated low watermark.
- The official Bigtable overview says that the Beam connector timestamps output at the epoch to avoid relying on event time when partition progress stalls.
- Apache Beam's [`ChangeStreamAction`](https://github.com/apache/beam/blob/a0e27149ee119b1bf6bff12c8c08c803f6757946/sdks/java/io/google-cloud-platform/src/main/java/org/apache/beam/sdk/io/gcp/bigtable/changestreams/action/ChangeStreamAction.java) advances its internal estimator but emits mutations and heartbeats at the epoch for the same reason.
- The [Spanner Change Streams contract](https://cloud.google.com/spanner/docs/change-streams/details#heartbeat_records) gives its heartbeat the strict per-partition ordering guarantee that Bigtable omits.
- Planner tests keep `SOURCE_WATERMARK()` in a separate `WatermarkAssigner` instead of pushing it into the Bigtable source, while accepting an ordinary watermark expression over commit-time metadata.

No real-service observation can prove the missing universal guarantee.
The published service contract, rather than a finite sample of on-time records, decides this source capability.

## Alternatives declined

- **Emit the minimum of the checkpointed ledger.**
  This includes inactive partitions but cannot strengthen the service estimate behind every ledger entry.
- **Subtract a configurable safety delay.**
  Bigtable publishes no upper bound from which the connector could derive a safe finite value.
- **Expose an estimated native watermark with a warning.**
  A warning cannot restore window state or a late record that an operator has already discarded.
- **Mark slow or unopened partitions idle.**
  Ignoring the partition removes the evidence that should hold back the frontier and can make the watermark earlier rather than safer.

## Consequences

- The Table source does not advertise or accept `SOURCE_WATERMARK()` pushdown; users should declare an ordinary application-owned expression instead.
- The `estimated-low-watermark` metadata column remains available as partition data, not a rowtime frontier.
- Applications that choose heuristic event time must configure and operate their own late-data policy.
- Bigtable and Spanner retain different source-watermark answers because their service contracts differ, even if a future coordinator protocol has a similar shape.
