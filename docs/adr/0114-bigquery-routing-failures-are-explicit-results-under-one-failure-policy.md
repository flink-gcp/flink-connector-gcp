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

# ADR-0114: BigQuery routing failures are explicit results under one failure policy

- Status: Accepted
- Date: 2026-08-14
- Issues: [#657](https://github.com/flink-gcp/flink-connector-gcp/issues/657)
- Modules: bigquery, base
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md#error-handling`

## Context

A dynamic BigQuery destination can depend on record data that cannot be validated when the sink is
built.
Treating every such case as a resolver exception forces one policy on users, while returning
`null` cannot distinguish an intentional record verdict from a resolver bug.
The sink already has one conservative, configurable failure policy for deterministic per-record
failures and a dead-letter contract over serialized bytes.

BigQuery serialization depends on the destination schema.
When routing fails, the sink cannot ask its serializer to produce a portable payload and must not
create table-specific state for a destination that does not exist.

## Decision

**Destination resolution returns one closed result hierarchy.**
`DestinationResolver.resolve(...)` returns `DestinationResolution`, whose connector-defined
variants are `TableDestination` for success and `UnroutableRecord` for an explicit,
record-specific routing failure.
The hierarchy's constructor is package-private so unsupported external variants cannot reach the
writers.
Its package-private visitor method performs double dispatch through one internal visitor contract
implemented by each writer.
Adding a connector-defined variant therefore adds a visitor method and makes every writer update
its handling at compile time, without per-record callback allocation or fallback casts.
Existing resolvers returning `TableDestination` remain source-compatible through the subtype.

**One failure policy covers both routing and post-routing failures.**
`BigQueryFailure` is the shared handler type implemented by `UnroutableRecord` and `FailedRow`.
The builder exposes `failureHandler(FailureHandler<? super BigQueryFailure>)`; its default remains
`FailureHandler.failJob()`.
The previous unpublished `failedRowHandler` name and `FailureHandler<FailedRow>` surface are
replaced rather than aliased.
This extends ADR-0017's row-only handler boundary while retaining its outage classification,
recovery and handler-lifecycle decisions.

**The resolver owns an unroutable record's payload and reason.**
`UnroutableRecord.of(payloadBytes, reason)` requires both values, reports connector `bigquery`,
destination `unresolved`, and no cause.
All three write methods route it before schema preparation, serialization, table metric lookup, or
per-destination client, stream, converter and file state.
It increments `numRecordsSendErrors` globally and creates no per-destination metric.

**Ambiguous resolver failures remain fatal.**
A resolver returning `null` fails the write with an `IOException`, and an unexpected resolver
exception propagates.
Neither reaches the failure handler or its counters.
Only the explicit result authorizes fail, drop or dead-letter behavior.

## Alternatives declined

- **Treat every resolver exception as a record failure.** An exception can represent a code bug,
  invalid configuration or dependency outage; a drop policy could then discard a backlog.
- **Treat `null` as the unroutable signal.** It carries neither replayable payload nor diagnosis
  and turns an ordinary implementation bug into intentional data loss.
- **Add a second routing-failure policy.** The existing policy already owns fail, drop, DLQ and
  lifecycle semantics; a second policy would duplicate configuration and checkpoint behavior.
- **Ask the serializer for the DLQ payload.** BigQuery serializers can choose their schema and
  descriptor by destination, which is precisely the value that could not be resolved.

## Consequences

A caller that chooses a dropping or dead-letter policy must make its routing verdict deterministic
under replay.
Dead-letter delivery retains the existing at-least-once semantics and identifies this case with
`dlq-destination=unresolved`.
Because the API is not published, the cleaner common failure type is preferred over keeping an
alias whose name excludes one of the failures it handles.
