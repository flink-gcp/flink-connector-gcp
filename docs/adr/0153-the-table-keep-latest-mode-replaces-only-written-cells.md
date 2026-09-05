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

# ADR-0153: The Table keep-latest mode replaces only written cells

- Status: Accepted
- Date: 2026-09-06
- Issue: [#1177](https://github.com/flink-gcp/flink-connector-gcp/issues/1177)
- Modules: bigtable (`table.sink`)
- Current behavior: `docs/content/docs/connectors/table/bigtable.md`

## Decision

Add `keep-latest` to the single `sink.write-mode` option introduced by ADR-0152.
Its default remains `upsert`.
For every cell an INSERT or UPDATE_AFTER physically writes, append an unbounded `DeleteFromColumn` immediately followed by its `SetCell` to the same `RowMutationEntry`.
All targeted qualifiers remain in that entry, so the service applies the row's replacement atomically.
DELETE still deletes the whole row; UPDATE_BEFORE remains invalid.

The existing family/qualifier schema and nullable codec define the target cells.
A null scalar cell is written using the existing empty-byte or `null-string-literal` encoding, and its previous versions are deleted.
A null family writes nothing and deletes nothing.
Families and qualifiers absent from the DDL are untouched.
A row whose every family is null remains invalid.
A partial SQL INSERT can materialize null scalar cells; those cells are targets, unlike a null family.

ADR-0149 continues to govern timestamps: absent or null metadata uses the connector's millisecond-aligned writer clock, read per cell.
Explicit metadata and the truncation option keep their existing semantics, including negative-value rejection.
The new mode does not expose server time through SQL.
DataStream serializers continue to own their timestamps; service acceptance separately exercises delete-then-set with the SDK's explicit server-time sentinel.

The mode uses the ordinary mutation batcher, writer options and table creation/repair path.
GC settings remain independent, including the requirement for a GC rule when SQL enables table creation.
Conditional-only settings remain invalid.
The ordinary upsert changelog and ADR-0102's insert-only compatibility mode apply unchanged.
The serializer stores a boolean flag whose absent value in an older job graph is false, preserving ordinary writes on restore.

## Guarantees and costs

This follows Google's [delete-then-write recommendation](https://docs.cloud.google.com/bigtable/docs/keep-only-latest-value).
Version-based GC is asynchronous; it does not establish an immediate one-version result.
The mode leaves one replacement version per targeted cell when that entry completes, before another write or applicable GC changes it.
It does not promise immediate physical storage reclamation.

Replaying an entry replaces the targeted cells again, including when reserialization chooses different timestamps.
This bounds visible versions, not effects: timestamps can change, an old replay can overwrite a newer value, and Change Streams can observe repeated mutations.
There is no compare-and-set, event-time arbitration, ordering between separate same-row entries, or Flink exactly-once guarantee.
The batcher may run separate requests concurrently even with one entry per request (ADR-0093).

Each targeted cell consumes two mutations instead of one.
The SDK continues to enforce the per-entry mutation limit and batch mutation budget (ADR-0082); entry-count options remain entry counts.
Age-based GC can still remove a replacement whose explicit timestamp is old enough.

## Alternatives

Using `maxVersions(1)` alone cannot provide immediate logical replacement.
Deleting a whole family or row before a write would erase unrelated cells during a partial update.
A read followed by a write would add latency without making the replacement atomic.
Changing timestamp defaults or using timestamp zero would conflate retention with event timestamps and diverge from ADR-0149.

## Validation

Serialization tests assert exact delete-before-set ordering, unbounded ranges, scalar and family null semantics, timestamp behavior, job-graph restoration and SDK mutation limits.
Factory and planner tests cover writer-option propagation, incompatible options and changelog compatibility.
Emulator and credential-gated service tests exercise replacement and replay without a latest-version read filter or a GC policy that could hide old versions.
Service evidence is recorded after the gated run; emulator results alone do not establish service behavior.
