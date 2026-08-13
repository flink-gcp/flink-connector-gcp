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

# ADR-0108: A source deserializer collector forwards records directly on the task thread

- Status: Accepted
- Date: 2026-08-13
- Issues: [#587](https://github.com/laughingman7743/flink-connector-gcp/issues/587)
- Modules: base, bigquery, pubsub, bigtable, spanner
- Current behavior: each connector's DataStream source reference

## Context

Issue #587 changes the row-oriented source deserializers from one nullable return value to a
collector that may emit zero or more non-null records.
BigQuery, Pub/Sub, Bigtable scan and Change Streams, and Spanner batch and Change Streams all need
the same boundary between one input and its outputs.

The first implementation copied a lifecycle wrapper into each source path and then replaced those
copies with a shared monitor-based collector.
The shared collector rejected cross-thread calls and retained downstream failures even when the
deserializer caught them.
This made a synchronous push adapter responsible for thread ownership, lifecycle enforcement and
failure recovery.

Flink's source output remains task-thread confined.
Asynchronous transport callbacks therefore hand records to the reader before deserialization.
Spanner Change Streams uses a one-slot handover to its mailbox thread, while Flink's
`SourceReaderBase` hands split-reader records to its task thread.

Apache Kafka, Pulsar and Kinesis source emitters all deserialize on that task-thread boundary and
adapt the supplied `Collector` directly to `SourceOutput`.
They keep transport buffering and backpressure outside the collector.
Kafka and Pulsar also warn that one input should produce a relatively small number and size of
records because collection can consume memory or delay checkpoint barriers.

## Decision

**One internal `SynchronousDeserializationCollector` supplies the same direct adapter to every
source path.**
Its static `deserialize` method creates one collector for one input and forwards each non-null
record immediately to a caller-supplied function.
The function carries connector-specific timestamp or metadata behavior without adding another
collector implementation.
The Bigtable selected-cell Table adapter also uses the helper to enforce its nested value
format's exactly-one-row contract; only the accepted row reference is held in method-local state.

The helper counts a record only after its downstream call succeeds and returns the count after the
deserializer completes.
The emitter uses zero to increment `recordsSkipped` and advances source progress only after the
method returns normally.
An unhandled deserializer or downstream failure therefore prevents progress from advancing, while
a record already sent downstream is not buffered or recalled.

The helper clears the downstream function in `finally`.
This rejects use after the call and prevents a retained collector from retaining Flink's output,
an input timestamp or Table API metadata.
The helper does not synchronize, identify or support another thread.

The public deserializer contract remains synchronous and task-thread confined.
A deserializer must not retain the collector, call it from another thread or emit null.
Supporting asynchronous output would require an explicit completion protocol with bounded
capacity, backpressure and checkpoint ownership rather than a lock around this collector.

Pub/Sub keeps its existing source-specific marker for an output exception that escapes the
deserializer.
Its failure policy continues to apply only when no such marker occurs in the thrown cause chain.
The shared helper does not retain a failure that user code catches, so adopting it does not create a
new acknowledgement, nack or dead-letter path.

## Evidence

- The shared unit test verifies direct call ordering, zero-to-many counting, null rejection,
  downstream failure propagation and downstream-reference release on success and failure.
- Each connector's emitter test retains the supplied collector and verifies that later use cannot
  emit into a subsequent input.
- Reader tests verify that an unhandled deserializer or downstream failure prevents BigQuery,
  Bigtable and Spanner progress from advancing.
- Pub/Sub tests preserve its pre-existing distinction between a schema failure and an output
  failure that escapes through the marker, including a schema that wraps that marker.
- The implementation shape matches Apache Kafka, Pulsar and Kinesis record emitters: transport
  handover is bounded separately and deserialized records pass directly to `SourceOutput`.

## Alternatives declined

- **Keep one wrapper per source path.**
  Timestamp and metadata behavior differ, but those differences fit in the downstream function and
  do not justify repeating the direct adapter and count.
- **Enforce thread ownership with a monitor or lifecycle fields.**
  This adds state to an API whose contract is already synchronous and does not make asynchronous
  output safe with respect to checkpoints or source progress.
- **Buffer every input's outputs and flush after deserialization.**
  A generic record has no cheap byte size, mutable records would need a type-aware copy, and a
  downstream failure during flush still cannot recall records already emitted.
  Buffering would also change the established partial-output behavior on a schema failure.
- **Queue cross-thread output for the task thread.**
  This is an asynchronous source protocol and needs a capacity, completion signal, timeout and
  checkpoint state.
  Flink's async operator and the bounded handovers in the source readers provide the relevant
  design shape outside this collector.

## Consequences

The connectors share the same direct adapter while keeping timestamp, metadata, acknowledgement
and progress decisions local.
The collector adds no record buffer and retains no downstream reference after deserialization.
Thread-safe transport handover remains a reader responsibility, and asynchronous deserialization
would require a separate SPI and decision record.
