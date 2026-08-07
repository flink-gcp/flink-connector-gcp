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

# ADR-0001: A serializer returning `null` skips the record

- Status: Accepted
- Date: 2026-08-04
- Issues: [#230](https://github.com/laughingman7743/flink-connector-gcp/issues/230)
- Modules: all connector modules
- Current behavior: each connector's DataStream page documents `recordsSkipped` and the skip
  semantics

## Context

Every serialization SPI in this repository — `BigtableSerializationSchema`,
`CloudTasksSerializationSchema`, `PubSubSerializationSchema`, `BigQueryProtoSerializer` — can
return `null`. Before [#230](https://github.com/laughingman7743/flink-connector-gcp/issues/230)
the meaning was undecided: Bigtable (following `google/flink-connector-gcp`'s
`BaseRowMutationSerializer`) skipped, while other paths could fail with a bare NPE. The issue
offered two resolutions: define `null` as a filter, or reject it with a named message.

## Decision

`null` means **filter, and only filter** (decided with the user): the record is written nowhere,
is **not** a failure, and never reaches the `FailureHandler`. Every SPI is `@Nullable` on
`serialize`, and every writer checks it.

Three implementation rules, none re-derivable from the contract alone:

- **The check goes immediately after the serializer's `catch`, ahead of any per-destination
  state.** `PubSubWriter.stateFor(...)` opens a publisher and `FileLoadsWriter.stateFor(...)`
  opens a staging file, so a record written nowhere must not reach either. The three BigQuery
  writers already serialized before creating a stream or auto-creating a table, for the sibling
  reason their comments give.
- **A combinator over one of our own SPIs propagates the `null` unchanged, so the writer's check
  is the single decision point.** `MetadataSerializationSchema` is the case that made this a bug
  rather than a tidiness point: it used to pass the `null` through when no extractor fired and
  NPE on `message.toBuilder()` when one did, so *one* skipping serializer became a dead letter
  for some records of a stream and a silent skip for the rest, depending on the record. Its
  extractors are not called for a skipped record either — they are user code, and running them
  for a record the sink will not send would surface their failures as failures of that record.
- **A `null` from a wrapped Flink `SerializationSchema` is a serialization failure, not a skip**
  — `DataOnlySerializationSchema`, `HttpTargetSerializationSchema.withBody(...)`,
  `table.sink.RowDataSerializationSchema`. That SPI's contract has no `null` in it (checked
  against `apache/flink` master: `@return The serialized element`), so reading one as a skip
  would silently drop every record a format failed on. Each throws an `IOException` naming the
  wrapped class and pointing at the SPI-level skip; the routing is unchanged, the message is
  what is new. The Table API layer therefore cannot skip at all, which is correct — SQL has no
  way to express it.

## Evidence

- Precedent: Flink's own `KafkaWriter` takes a `@Nullable ProducerRecord` and skips silently;
  `google/flink-connector-gcp`'s `BaseRowMutationSerializer` does the same, which is why the
  Bigtable connector here already shipped skip semantics.
- What the argument does **not** rest on: Kafka's javadoc, which reads *"or null if the given
  element cannot be serialized"*. That is `null` overloaded to mean *failure*, silently dropped
  — and [#37](https://github.com/laughingman7743/flink-connector-gcp/issues/37) replaced that
  role here with a real failure channel, so the failure reading is unavailable by construction.

## Alternatives declined

- **Reject `null` with a named message** (the issue's other option): a user may legitimately
  return `null` to filter, and both precedents above define the idiom as a filter.
- **Per-destination breakdown of the skip counter** (`destination.X.skipped` under
  `perDestinationMetrics`): the serializer is handed the record alone, so its decision cannot
  depend on the destination, and the name would read as a property of X.

## Consequences

`recordsSkipped` is registered by all six writer metrics classes and is the **only** thing that
reports a skip. That is the honest cost of the contract: a serializer skipping every record
leaves an empty destination under a green job, which no failure counter sees — the
[#206](https://github.com/laughingman7743/flink-connector-gcp/issues/206) exposure in another
shape, and the reason the counter is not optional.
