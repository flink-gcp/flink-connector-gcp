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

# ADR-0103: The Bigtable Change Streams reader bounds asynchronous partition reads

- Status: Accepted
- Date: 2026-08-13
- Issues: [#553](https://github.com/laughingman7743/flink-connector-gcp/issues/553)
- Modules: bigtable (`source.changestream`, `source.changestream.reader`)
- Current behavior: [Change Streams source](../content/docs/connectors/datastream/bigtable.md#change-streams-source)

## Context

ADR-0097 defines the coordinator ledger and partition-transition protocol, but its reader opens one blocking `ReadChangeStream` RPC at a time.
A stable stream can remain open indefinitely, so one active read leaves other initial, restored, or successor partitions unserved even when the source subtask has resources for them.

Bigtable controls the service partition set and does not publish a fixed partition-to-worker ratio.
Flink source parallelism controls the number of reader subtasks, while each reader also needs a separate bound for the long-lived RPCs and callback state it owns.

An asynchronous callback can receive a record before Flink's task thread emits it.
Checkpointing that delivered token would skip the handed-over record after recovery, while allowing callbacks to enqueue freely would make memory depend on service response volume.

## Decision

Each reader subtask opens at most `maxConcurrentStreamsPerSubtask` partition reads, default two.
The conservative default is the smallest value that exercises concurrent partition reads, and the
gated acceptance case measures two simultaneous live reads in each of two source subtasks.
The configured job-wide capacity is source parallelism multiplied by this value; it is a connector resource bound rather than a Bigtable quota.

The reader reports its absolute free-slot count to the enumerator.
The enumerator assigns at most that many splits and retains the remaining fresh, returned, or successor splits in its checkpointed unassigned ledger.
Restored assignments that exceed the local bound remain in the reader's checkpointed FIFO.

Each partition uses the pinned client's `readChangeStreamAsync` API with automatic inbound flow control disabled.
Each active read has at most one requested response, and the subtask-wide handover queue has the same capacity as the active-stream bound.
The task thread removes a response before it emits the record and requests another, so queued plus outstanding responses cannot exceed the number of active reads.
No partition owns a dedicated blocking thread or unbounded queue.

Every active read keeps separate delivered and emitted split positions.
The callback advances only the delivered position, while the task thread advances the emitted position after successful deserialization and coordinator-event emission.
Snapshots contain emitted active state plus every queued split.
An RPC error fails the task without removing that state or reporting a partition transition.

The client request asks for a five-second service heartbeat.
When a heartbeat reaches the task thread while another partition is queued, the reader cancels that RPC and appends its emitted split to the FIFO after the RPC terminates.
The queued head then opens, which gives every queued partition a rotation point even when no service topology change closes a stable stream.
Cancellation retains the physical stream slot until its terminal callback so a slow cancellation cannot exceed the configured bound.
Completion without a `CloseStream` is a failure; connector-initiated rotation, topology completion, and reader close are distinguished from it.

Reader metrics report opened and active RPCs, queued partitions and their oldest-position lag, missed heartbeat intervals, record variants, and the minimum checkpointed low watermark across active and queued assignments.
Enumerator metrics report initial and successor partitions, split and merge transitions, and oldest unassigned-position lag.
Lag calculations return zero for future positions and saturate on subtraction overflow.

## Evidence

Reader tests drive manual request counts, simultaneous heartbeat handover, availability-future replacement, the shared concurrency bound, delivered-versus-emitted snapshots, FIFO heartbeat rotation, bounded completion after delayed cancellation termination, and RPC failure, unexpected completion, and unrequested responses without split loss.
Enumerator tests advertise capacity before initialization, assign only that absolute count, and redistribute four restored splits across two readers in stable order without exceeding either advertised capacity.
Metric tests use a controllable clock for queue lag, heartbeat intervals, future timestamps, and overflow without wall-clock waiting.
The savepoint rescaling test drives six partitions through parallelism 1 to 3 to 1, verifies per-reader peaks of 6, then 2/2/2, then 2, and covers unbounded first and last partition boundaries.
The request test verifies the explicit five-second heartbeat in the protobuf sent by the client adapter.

The gated real-service acceptance path is the only place that can validate several live Bigtable partition reads across several Flink source subtasks.
The emulator cannot provide that evidence because it does not implement Change Streams.

## Alternatives declined

- **Raise source parallelism and keep one stream per subtask.**
  This couples Flink task allocation to the service's private partition count and gives one quiet partition an entire subtask.
- **Use one blocking thread per partition.**
  Restored or successor bursts would require either an unbounded thread count or a second scheduling bound around those threads.
- **Give every active stream its own response queue.**
  The total handed-over records would grow with both stream count and per-stream queue depth, while the source needs only one outstanding response per configured stream slot.
- **Checkpoint the delivered token.**
  A callback can deliver a record that the task thread has not emitted, so restore from that token can lose the record.
- **Wait for `CloseStream` before serving queued work.**
  A stable partition may not close, which allows queued partitions to starve indefinitely.
- **Treat the concurrency default as a Bigtable limit.**
  No cited service contract publishes that limit, and the configured value bounds connector resources regardless of service topology.

## Consequences

- Increasing the option spends one active RPC, callback observer, and one entry of handover-queue capacity per additional slot in each subtask.
- A rotation reopens from the emitted continuation token and can replay responses received after that position, preserving at-least-once delivery.
- A parallelism or bound reduction preserves excess splits in checkpoint state and trades per-partition freshness for a lower resource ceiling.
- The connector exposes capacity and lag but does not change Flink operator parallelism automatically.
- Operators must combine connector lag with Bigtable CPU-load and Change Streams log-volume metrics; Bigtable streaming-request latency measures RPC lifetime rather than change-processing delay.
