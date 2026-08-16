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

# ADR-0097: The Bigtable Change Streams coordinator checkpoints the partition ledger

- Status: Accepted
- Date: 2026-08-11 (revised 2026-08-14)
- Issues: [#35](https://github.com/laughingman7743/flink-connector-gcp/issues/35),
  [#510](https://github.com/laughingman7743/flink-connector-gcp/issues/510),
  [#532](https://github.com/laughingman7743/flink-connector-gcp/issues/532),
  [#533](https://github.com/laughingman7743/flink-connector-gcp/issues/533),
  [#586](https://github.com/laughingman7743/flink-connector-gcp/issues/586)
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

The java-bigtable methods, query and mutation records, and mutation entry types used by this
protocol carry `@InternalApi("Intended for use by the BigtableIO in apache/beam only.")` in 2.80.0;
the aggregate `Value` model is `@BetaApi`. They are nevertheless the client library's only typed
entry points to these RPCs. The acceptance is the same shape as ADR-0041: the annotations and
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

Pending merge tokens accumulate in a coordinator-thread range index.
Each arrival reevaluates at most three affected adjacency relationships; it does not copy or sort every token already received.
The index freezes to the immutable `PendingMerge` model in range order at completion, checkpoint, and reconciliation boundaries, preserving the connector-owned checkpoint format and deterministic restore behavior.

Initial partitions are generated only for a fresh start. A restored enumerator validates every
checkpointed low watermark through ADR-0094 and never calls the initial-partition RPC. The source
requires an application profile id because Change Streams accepts only single-cluster routing. The
coordinator preflights the routing policy when the principal can read it; a data-plane-only
principal is allowed through and the reader translates the service rejection if the profile is
multi-cluster.

The reader reconstructs every SDK-bound partition as an explicit closed-start/open-end range before
building either a `ReadChangeStream` request or a continuation token. Empty boundary keys mean
negative or positive infinity in this API, but their protobuf oneof cases must still be set. The
connector's general-purpose range copy normalizes an empty key to an unbounded model value, so it
remains safe for internal range algebra and is deliberately not the SDK representation.

The reader evaluates output filters against each complete SDK mutation and converts only retained
entries into the connector-owned immutable `ChangeStreamMutation` before application
deserialization.
The public model carries every mutation-level field and an ordered typed representation of the
five entry kinds and three aggregate value kinds exposed by java-bigtable 2.80.0.
Unknown SDK entry or value subtypes fail before any partial mutation reaches the application.
The connector-owned model has tagged Flink serialization and type information, so public output
does not depend on the SDK's Apache-Beam-only `@InternalApi` record model or reflective Kryo.

Family and qualifier regular expressions select SDK entries with full-match semantics.
Qualified-column expressions match `family:` plus canonical padded RFC 4648 standard Base64, and
family deletes use only the family filter because they have no qualifier.
Filtering runs after the service has delivered the complete atomic mutation and does not reduce
RPC traffic.
An empty projection is delivered by default; the explicit skip flag bypasses deserialization while
still advancing the continuation token and estimated low watermark.
When no entry filter is configured, the reader bypasses filter evaluation and its projection result
allocation, but the SDK-to-public-model conversion remains the cost of removing the unstable SDK
type from the public SPI.
When an explicit skip removes every entry, the reader advances directly from SDK metadata without
materializing the public mutation or any public entry/value/range object.

## Evidence

- In java-bigtable 2.80.0, `ReadChangeStreamQuery.continuationTokens` accepts a list and is mutually
  exclusive with `startTime`, which permits a merged partition to resume from all parent tokens.
- `CloseStream` carries paired successor ranges and tokens; `ChangeStreamContinuationToken`
  exposes the token's partition range, which is what proves that a merge target is complete.
- `Table.getChangeStreamRetention()` exposes the configured retention to the connector, while a
  fresh `latest()` avoids that admin read through ADR-0094's lazy lookup.
- The first real-service run on 2026-08-12 rejected a partition whose boundary oneofs had been
  cleared with `INVALID_ARGUMENT: partition.row_range must be the form of [start_key, end_key)`.
  java-bigtable 2.80.0 and Apache Beam both preserve `ByteStringRange.create(empty, empty)`, which
  sets the required closed/open cases while using empty values for the unbounded keyspace.
- The next real-service run reached the mutation stream and exposed the same normalized range at
  `ChangeStreamContinuationToken.create` as `IllegalStateException: Start is unbounded`; all
  connector-created SDK ranges therefore share the same boundary conversion.
- After that fix, the live stream emitted a service mutation but default Kryo copying failed with
  `UnsupportedOperationException` while rebuilding its immutable `entries`; #586 replaces that
  public SDK output with the connector-owned tagged representation.
- The following live run completed a checkpoint and entered the fixture's controlled failure. Its
  generic `RichMapFunction` then erased the schema's output type before the collect network edge;
  the gated fixture now declares that output type explicitly so recovery uses the same serializer.
- The final production-service run on 2026-08-12 used instance
  `flink-it-1786493698-968f3051`, observed all 100 seeded rows, completed a checkpoint, triggered the
  one controlled failure, recovered without loss, and completed at the bounded end time in 132.3
  seconds. The instance returned `NOT_FOUND` immediately after teardown.
- Unit tests round-trip pending merge state, prove one token cannot release a two-parent merge,
  prove restore does not generate new initial partitions, prove many parents require only bounded neighboring coverage checks, and prove expired coordinator-held state fails unless fallback was opted in.
- Deterministic tests cover all 2.80.0 entry and value kinds, full-match family and qualified-column
  filtering, default-empty and explicit-skip dispositions, source serialization, metrics, and
  progress through filtered mutations. No new service behavior is assumed, so #586 requires no
  additional real-GCP acceptance run.

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
- **Keep the raw SDK mutation as a public escape hatch.** That would preserve the unstable type
  dependency the connector-owned model removes and would make filter output semantics differ by
  deserializer choice.
- **Push the filters into the service request.** `ReadChangeStream` returns complete logical
  mutations and exposes no equivalent family or qualifier predicate in the pinned client.

## Consequences

Enumerator state intentionally carries an assigned ledger, unlike the bounded scan source in
ADR-0080: a continuous source must know which ranges are live before it can find a missing one.
The final implementation phase adds that reconciler and checkpoints its timers in the same state.
The emulator covers none of these RPCs, so deterministic tests use hand-written seams and the final
protocol proof runs against gated real Bigtable.
Output filters do not alter the partition ledger or checkpoint format.
Operators can distinguish removed entries from explicitly skipped empty projections through the
two Change Streams reader counters.
