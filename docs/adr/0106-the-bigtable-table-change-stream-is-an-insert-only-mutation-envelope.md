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

# ADR-0106: The Bigtable table change stream preserves mutations or decodes one complete cell

- Status: Accepted
- Date: 2026-08-13
- Issues: [#523](https://github.com/laughingman7743/flink-connector-gcp/issues/523), [#600](https://github.com/laughingman7743/flink-connector-gcp/issues/600), [#601](https://github.com/laughingman7743/flink-connector-gcp/issues/601), [#602](https://github.com/laughingman7743/flink-connector-gcp/issues/602), [#603](https://github.com/laughingman7743/flink-connector-gcp/issues/603)
- Modules: bigtable (`table`, `table.source`)
- Current behavior: [Change Streams](../content/docs/connectors/table/bigtable.md#change-streams)

## Context

Bigtable Change Streams report mutations rather than row images.
A mutation can set or delete cells and families, or apply aggregate operations, but it does not carry the complete row before or after the change.

Flink's row-level update and delete kinds would therefore overstate what the service supplied.
Reconstructing them requires an external row-image store with ordering, retention, and bootstrap rules that the source does not own.

The pinned Bigtable client 2.80.0 exposes five entry models with different value domains: `SetCell`, `DeleteCells`, `DeleteFamily`, `AddToCell`, and `MergeToCell`.
A table schema tied to only the first three would silently lose aggregate mutations when a table enables aggregate families.

## Decision

`scan.mode = change-stream` selects the existing DataStream Change Streams source and requires an explicit `scan.change-stream.changelog-mode`.

`envelope` emits one insert-only row per Bigtable mutation and preserves its row key plus the ordered entry list.
It declares no primary key because repeated mutations for one row are distinct log records.

The physical DDL is exact.
Each entry carries its zero-based index, kind, family, generic qualifier, generic timestamp, generic value, and delete range.
The generic value discriminates raw bytes, a raw timestamp in microseconds, and a signed 64-bit integer without coercing one domain into another.
Fields that do not apply to an entry kind are null.

The converter handles every entry and generic-value subtype in client 2.80.0.
An unknown future subtype fails the job with its class name instead of emitting a partial envelope.

The source exposes five scalar mutation fields through Flink's readable-metadata ability.
`mutation-type` is a non-null string, `source-cluster-id` is a nullable string, `commit-timestamp` and `estimated-low-watermark` are non-null `TIMESTAMP_LTZ(9)` values, and `tie-breaker` is a non-null integer.
The empty cluster identifier on a garbage-collection mutation becomes SQL null because that mutation has no source cluster.
The timestamp conversion retains all nine fractional digits carried by the SDK's `java.time.Instant`.

`selected-cell` is a separate, stateless interpretation for producers that store one complete logical row in one configured cell.
The DDL declares exactly one physical primary-key column, in any position, and at least one non-key physical column.
The mutation row key is decoded into that primary key with the HBase-compatible `CellValueCodec`; `value.format` decodes every non-key column from the selected cell.
The format must be insert-only and must emit exactly one non-null row for an upsert.
Format metadata is not exposed because a delete has no payload from which to obtain it.

The selected cell is identified by its family and canonical padded RFC 4648 Base64 qualifier, including an empty qualifier.
One configured source cluster is accepted so multi-cluster conflict resolution cannot reorder the keyed changelog.
The source emits `UPDATE_AFTER` when an atomic user mutation contains exactly one full selected-column or selected-family delete followed by exactly one selected `SetCell`.
It emits a key-only `DELETE` when that full delete has no following selected `SetCell`.
Entries for other cells and families emit nothing.

The source fails on a standalone, repeated, or out-of-order selected `SetCell`; a timestamp-bounded selected-column delete; a selected-cell garbage-collection mutation; an aggregate mutation that can affect the selected cell; a mutation from another cluster; or a value format that emits zero, multiple, or null rows.
It does not invent `INSERT`, perform a lookup, reconstruct an old value, or bootstrap a snapshot before the configured Change Streams start position.
This restriction is the state-and-bootstrap design: correctness comes from the producer replacing or deleting the complete logical value atomically, not from state hidden inside the source.

Continuation tokens and partition ranges remain internal to the FLIP-27 source protocol.
The estimated low watermark belongs to the partition that produced the mutation and is readable data, not a Flink source watermark.
A stream-wide watermark requires a coordinated frontier across active, queued, and unassigned partitions.

Change Streams options map onto the DataStream builder.
An absent startup mode retains the builder's latest-position default; a restored checkpoint position takes precedence under ADR-0094, and an expired restored position fails unless the DDL explicitly configures a resume fallback.
An end timestamp makes the source bounded, and the per-subtask stream option retains ADR-0103's builder default when absent.

Bounded-scan row ranges, lookup settings, and the emulator are rejected in Change Streams mode.
Change Streams options are rejected in bounded mode.
Selected-cell options and `value.format` are rejected in envelope mode.
This prevents a valid-looking DDL from carrying options that no selected runtime consumes.

## Alternatives declined

- **Emit row-level updates and deletes from arbitrary mutations.**
  The service does not provide before or full after images, so those row kinds would describe state the source never observed.
- **Look up the row after every mutation or retain row images in source state.**
  A lookup is not ordered atomically with the Change Streams record, while retained state still needs a snapshot bootstrap and an ownership policy for records before the configured start position.
- **Treat any selected `SetCell` as an upsert.**
  A single cell version does not prove that older versions were removed, so a replay or timestamp choice could expose a different logical value to readers.
- **Support only set and delete mutations.**
  Aggregate mutations are part of the pinned client surface and omitting them would make the same physical Bigtable table lose changes depending on its family type.
- **Flatten qualifier, timestamp, and value to byte strings.**
  Aggregate entries distinguish raw bytes, raw timestamps, and integers; flattening would either lose the discriminator or invent an encoding outside the service model.
- **Ignore options that belong to the other scan mode.**
  A typo or copied bounded-scan setting would then plan successfully while doing nothing.
- **Infer the envelope schema from arbitrary column names.**
  A fixed contract lets saved DDL, SQL queries, and downstream schemas agree on entry meaning and makes an SDK model addition fail visibly.
- **Expose continuation tokens as metadata.**
  They are checkpoint protocol state rather than mutation data, and a query must not take ownership of source resumption.
- **Treat the estimated low watermark as a Flink source watermark.**
  One mutation reports one partition's estimate, bounded reader concurrency can leave older partitions queued or unassigned, and Bigtable permits future records below a previously observed estimate.

## Consequences

- The envelope is lossless for the mutation entry models in client 2.80.0, but it is not a materialized Bigtable row.
- Consumers derive their own stateful interpretation from the ordered mutation log.
- SQL can select or cast the five readable metadata fields without changing the physical envelope.
- The source remains insert-only even when an entry deletes a cell or family.
- Selected-cell mode is a keyed upsert stream only for the documented atomic producer protocol; existing arbitrary Bigtable writers do not satisfy that contract automatically.
- A downstream keyed materialization interprets the first `UPDATE_AFTER` it sees as the current value; the source does not relabel it as `INSERT`.
- The SQL uber-jar does not bundle Flink formats, so a selected-cell deployment supplies its chosen format jar separately.
- The source deliberately omits native source watermarks because Bigtable does not guarantee that its partition estimate is non-early ([ADR-0109](0109-bigtable-change-stream-estimates-do-not-become-native-source-watermarks.md)).
- Gated real-GCP Table API acceptance uses the existing ephemeral-instance harness to exercise the
  envelope through SQL with timestamp bounds, a binary row key, binary qualifier and value, ordered
  user writes and deletes, and readable metadata.
  Service-timed garbage collection and retention expiry remain outside that acceptance because
  neither is deterministic.
