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

# ADR-0097: The Bigtable Change Streams coordinator checkpoints the partition ledger

- Status: Accepted
- Date: 2026-08-11
- Issues: [#35](https://github.com/laughingman7743/flink-connector-gcp/issues/35), [#510](https://github.com/laughingman7743/flink-connector-gcp/issues/510)
- Modules: bigtable (`source.changestream`)
- Current behavior: `source.changestream.BigtableChangeStreamSplitEnumerator`

## Context

Bigtable Change Streams starts with `GenerateInitialChangeStreamPartitions`, but those partitions
do not stay fixed. A stream ends with `CloseStream` continuation tokens for successor partitions.
A split gives each child its own token; a merge gives the same target to several parents, and the
target cannot start until its parent-token ranges cover it. Rapid split/merge sequences can also
lose a merge close message, so the coordinator eventually needs a keyspace reconciler.

Apache Beam keeps this protocol in an external metadata table because its splittable functions have
no single coordinator. FLIP-27 already has one. Adding Beam's metadata table here would create a
second mutable recovery system beside Flink checkpoints and would make a new external resource part
of every job's correctness.

The java-bigtable methods and models used by this protocol carry `@InternalApi("Intended for use by
the BigtableIO in apache/beam only.")` in 2.80.0. They are nevertheless the client library's only
typed entry points to these RPCs. The acceptance is the same shape as ADR-0041: the annotation and
surface are checked facts, reread on a client upgrade, rather than an accidental dependency.

## Decision

The `SplitEnumerator` owns the partition topology. Its checkpoint contains unassigned and assigned
splits, pending merge targets, the resolved fresh-start time and the next split id. The reader added
by #511 will checkpoint the matching exact continuation tokens and low watermark. No external metadata
table or change-stream name exists; cross-job handoff is a Flink savepoint.

A successor is parked until the ranges named by all tokens received for it tile the target range.
Only then is one split created with the full token list. Split ids come from a checkpointed monotonic
counter rather than from ranges: a range can disappear and reappear after another topology change,
and reusing its old id would confuse a late completion with the new split.

Initial partitions are generated only for a fresh start. A restored enumerator validates every
checkpointed low watermark through ADR-0094 and never calls the initial-partition RPC. The source
requires an application profile id because Change Streams accepts only single-cluster routing. The
coordinator preflights the routing policy when the principal can read it; a data-plane-only
principal is allowed through and the reader translates the service rejection if the profile is
multi-cluster.

## Evidence

- In java-bigtable 2.80.0, `ReadChangeStreamQuery.continuationTokens` accepts a list and is mutually
  exclusive with `startTime`, which permits a merged partition to resume from all parent tokens.
- `CloseStream` carries paired successor ranges and tokens; `ChangeStreamContinuationToken`
  exposes the token's partition range, which is what proves that a merge target is complete.
- `Table.getChangeStreamRetention()` exposes the configured retention to the connector, while a
  fresh `latest()` avoids that admin read through ADR-0094's lazy lookup.
- Unit tests round-trip pending merge state, prove one token cannot release a two-parent merge,
  prove restore does not generate new initial partitions, and prove expired coordinator-held state
  fails unless fallback was opted in.

## Alternatives declined

- **Beam's metadata table.** Flink already checkpoints a coordinator and reader state; the table
  would duplicate it and add lifecycle, permissions, locks and cleanup to every job.
- **Start merged partitions on the first token.** The uncovered parent range would be silently
  dropped, which is worse than holding the merge.
- **Derive split ids from ranges.** The same range can recur after a split/merge cycle, so the id is
  not an identity.
- **Require admin metadata permission.** The routing preflight is diagnostic rather than part of
  the data protocol; denying a principal that can read changes would turn an improved message into
  a new permission requirement.

## Consequences

Enumerator state intentionally carries an assigned ledger, unlike the bounded scan source in
ADR-0080: a continuous source must know which ranges are live before it can find a missing one.
The final implementation phase adds that reconciler and checkpoints its timers in the same state.
The emulator covers none of these RPCs, so deterministic tests use hand-written seams and the final
protocol proof runs against gated real Bigtable.
