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

# ADR-0099: The Spanner Change Streams coordinator checkpoints partition lineage

- Status: Accepted
- Date: 2026-08-12; revised 2026-08-13 and by [#1141](https://github.com/flink-gcp/flink-connector-gcp/issues/1141) on 2026-08-29
- Issues: [#222](https://github.com/flink-gcp/flink-connector-gcp/issues/222),
  [#534](https://github.com/flink-gcp/flink-connector-gcp/issues/534),
  [#535](https://github.com/flink-gcp/flink-connector-gcp/issues/535),
  [#635](https://github.com/flink-gcp/flink-connector-gcp/issues/635),
  [#1141](https://github.com/flink-gcp/flink-connector-gcp/issues/1141)
- Modules: base, spanner (`source.changestream`)
- Current behavior: `source.changestream.enumerator.SpannerChangeStreamSplitEnumerator`

## Context

A Spanner change-stream query begins with a null partition token and ends after returning every child-partitions record for that partition.
A child names its parent tokens, and a merged child must not start until every parent query has finished.
Starting it earlier can reorder changes for one key across the parent-child boundary.

Apache Beam's splittable-function implementation stores the partition lifecycle in a Spanner metadata table.
FLIP-27 already has a coordinator whose state participates in Flink checkpoints.
Adding the metadata table here would create a second recovery record with separate permissions, lifecycle, and consistency failure modes.

## Decision

The split enumerator owns one checkpointed partition ledger.
Each entry derives a stable split id from its optional Spanner token and carries parent identities, start and end timestamps, heartbeat interval, current read position, lifecycle state, and watermark.
The lifecycle is `CREATED`, `SCHEDULED`, `RUNNING`, then `FINISHED`.
The initial entry has no Spanner token, and the coordinator gives its null parent a reserved identity that cannot collide with a real token-derived id.

Child discovery and parent completion are separate source events.
A parent query can return several child-partitions records with the same start timestamp, so receiving one record does not prove that the query finished.
The reader reports every child record first and reports completion only after the query ends successfully.
The coordinator deduplicates identical child reports and moves a child from `CREATED` to `SCHEDULED` only when every named parent is `FINISHED`.

The coordinator checkpoints only unfinished entries.
A finished parent leaves the live ledger immediately and remains as a compact split-id proof only while an already-discovered `CREATED` child still names it.
Successful query completion comes after every child record, so no future child can first name that parent; after all known children become schedulable, the proof is no longer needed and is removed.
Reader progress events keep the running entries' current position and watermark monotonic.
A returned split keeps that progress and moves back to `SCHEDULED`; a restored running entry waits for Flink to return its reader-owned split rather than being assigned twice.
The checkpoint records boundedness separately, so a bounded source whose final live entry has been removed can restore an empty ledger and still signal no more splits.

The coordinator also owns the source-wide event-time frontier.
It is the minimum safe watermark across every unfinished `CREATED`, `SCHEDULED`, and `RUNNING` entry in the compact ledger, including partitions that no reader currently owns.
Finished-parent proofs carry no progress and do not participate in this minimum.
The enumerator checkpoints the last emitted source frontier beside the ledger and sends it to every registered reader, including readers with no split, so restore and rescaling cannot move event time backwards.
At runtime, a counted ordered index tracks unfinished partition watermarks and is rebuilt from the ledger after restore; ordinary data progress with an unchanged partition watermark does not scan or rewrite that index.

Fresh initialization resolves `StartPosition` once and creates exactly one null-token entry.
Restore validates every unfinished entry against the one retained window from ADR-0094 and never creates an initial entry while the ledger remains valid.
The connector reads retention and partition mode through dialect-specific `CHANGE_STREAM_OPTIONS` queries behind a serializable factory and a small runtime client interface.
An absent retention row uses the configured fallback, whose connector default is seven days, while `MUTABLE_KEY_RANGE` is rejected because its partition start, end, move-in, and move-out records are outside this source's model.

Spanner resets the whole ledger when an explicit fallback recovers an expired restore.
Advancing one old partition token can skip the child-partitions record that ended its query, leaving the coordinator unable to discover the next topology.
The reset discards every stale dependency and starts one new null-token query at the resolved fallback, with a warning that names the unavailable range and duplicate boundary.
Without the explicit fallback, expiry fails the job.

Readers wait for a coordinator initialization event before opening any restored split.
The event preserves reader-owned splits after a valid restore and discards them after an explicit whole-ledger fallback.
This handshake is necessary because Flink restores reader state independently of the enumerator's asynchronous metadata check; without it an expired token can reach Spanner before the coordinator has decided whether to fail or fall back.

Both checkpoint serializers use connector-owned versioned formats.
They encode optional values and lifecycle states with explicit tags and reject unknown versions, tags, negative counts, and invalid timestamps.
Enumerator state version 3 writes unfinished entries, finished-parent proof IDs, boundedness, and the source-wide frontier.
Versions 1 and 2 remain readable as complete ledgers; restore validates their full graphs before deriving the conservative version-1 frontier and compacting finished entries into only the proofs still needed by `CREATED` children.

## Evidence

The real-GCP acceptance produced two child partitions in each 5,000-row recovery run and observed work on both reader subtasks.
It intentionally failed after a completed checkpoint, recovered every unique mutation id with the allowed inclusive-boundary duplicates, stopped with a savepoint, and consumed both a mutation written while stopped and one written after restore.
Separate stale-state savepoints verify the reader handshake and whole-ledger fallback in both dialects.
A slow serializer test migrates a version-2 chain with 100,000 historical transitions and requires version 3 to retain one live entry and no proofs.
Its 2026-08-29 local measurement reduced the checkpoint from 10,477,838 bytes to 123 bytes; the test reports checkpoint bytes, migration, snapshot and restore times, exact retained lineage-entry counts, and current-thread heap allocation during legacy and compact snapshot serialization rather than fixing environment-dependent byte or timing observations as thresholds.

## Alternatives declined

- **Use Beam's external metadata table.**
  Flink checkpoints already provide one atomic recovery record for enumerator and reader state, while a table would add resource provisioning and another consistency boundary.
- **Mark a parent finished on its first child record.**
  One query can return several child records, so this could schedule a child while its parent still has records to report.
- **Restart only each expired token.**
  The skipped interval can contain that token's terminal child record, which would strand all descendants even though the restarted query itself succeeded.
- **Accept unknown partition modes as immutable.**
  A newly added mode can introduce record types that change the lifecycle protocol, so consuming it without an explicit implementation would be partial support.

## Consequences

- Checkpoint and coordinator lineage state scale with unfinished topology and unresolved parent proofs rather than job lifetime.
- `changeStreamPartitionLedgerEntries` and `changeStreamFinishedParentProofs` expose those two bounded structures without partition-token labels.
- Version-1 and version-2 restore pays one validation and compaction pass before subsequent checkpoints use version 3.
- The public source and reader can be added independently in #536 because the coordinator-facing split and event contracts are now fixed.
- Real-service partition, checkpoint, savepoint, retention, and fallback behavior is covered by the gated acceptance in #535.
