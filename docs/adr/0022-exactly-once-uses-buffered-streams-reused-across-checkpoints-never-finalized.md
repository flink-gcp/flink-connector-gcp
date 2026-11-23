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

# ADR-0022: Exactly-once uses buffered streams reused across checkpoints, never finalized

- Status: Accepted
- Date: 2026-07-25; dynamic destinations and schema evolution added 2026-08-13 ([#76], [#77]);
  the answer to a repeated flush corrected 2026-11-23 ([#2272]); the committer's in-place retries
  and the answer to a lower flush recorded 2026-11-23 ([#2283])
- Issues: [#30], [#76], [#77]
- Modules: bigquery (`sink.storage`)
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md` § Exactly-once
  (buffered streams)

## Decision

`STORAGE_API_EXACTLY_ONCE` is buffered streams + 2PC on checkpoints.

- **One stream per (writer subtask, active destination), reused across checkpoints and tracked in
  Flink writer state** (Dataproc-connector style; stream-per-checkpoint explicitly rejected — GCP
  support told the user frequent `CreateWriteStream` churn is not intended usage).
- Writer state carries the destination; its version-2 serializer migrates destination-less
  version-1 state only when the restored sink still has the same fixed destination (validated
  against the stream's table path).
  Committables need no destination because `FlushRows` addresses the full stream resource name,
  so their version-1 format remains unchanged.
- Committable = (streamName, inclusive flushOffset, subtaskId); the committer calls `FlushRows`
  synchronously.
  Repeating a stream's last flush, to the same offset, succeeds, as the restores in the
  [exactly-once fault sequences](evidence/0204-bigquery-exactly-once-fault-sequence-findings-2148.md#where-the-faults-landed-against-the-commits)
  measured.
  A flush below the offset the stream is already flushed to answers `ALREADY_EXISTS` ("Attempted
  to flush to offset 9 when stream is already flushed to 19", measured 2026-11-23 against real
  BigQuery), and the committer takes that answer, like an `OFFSET_ALREADY_EXISTS` error, as
  already flushed, so re-committing a committable older than the stream's last flush does not fail
  the commit.
  That is the idempotent-re-commit foundation; its `ALREADY_EXISTS` handling follows Apache Beam's
  `StorageApiFlushAndFinalizeDoFn`.
  A transient failure, and under `CREATE_IF_NEEDED` a status that may mask a table not yet
  propagated, is retried in place on the recovery schedule.
  Any other failure, an exhausted retry budget, or a response offset other than the requested one
  throws and fails the commit; the restored requests are re-committed when Flink restarts the job
  or the job is restored.
  There is no deterministic-id machinery, no checkpoint stamper and no `.global()`: the committer
  runs at sink parallelism, and the pre-commit topology is identity and exists only as the
  validation hook.
- Restore: synchronous probe append at the restored offset; offset conflicts / dead stream /
  reopen failure abandon the stream for a fresh one at offset 0 (rows past the restored offset
  were never committable, so they stay invisible).
- Restore groups state by destination and adopts the highest checkpoint id independently for each
  table; equal checkpoint ids use the lexicographically first stream name as a deterministic
  tie-breaker, and unadopted sibling streams remain open because pending committables may name them.
- **Streams are never finalized anywhere** — real BigQuery rejects `FlushRows` on a finalized
  stream (verified; the batch IT caught it), so finalizing races restored-but-uncommitted
  commits.
  Open streams' unflushed tails remain invisible; whether BigQuery bills their buffered storage
  has not been established.
- Server-side row-level errors route to the `FailureHandler` with offset-recompute recovery
  (atomic request rejection → route failing rows, replay survivors + trailing batches;
  `ALREADY_EXISTS` during an offset-shifting replay is terminal).
- Dynamic destinations are supported through the same `destinationResolver` contract as the other
  write methods; each destination has independent batching, offsets, recovery, state, and commits.
- A clean destination idle past `BufferedStreamOptions.destinationIdleTimeout` is evicted after a
  successful non-end-of-input flush; the local appender closes, the remote stream is not finalized,
  and a later row creates a new stream.
- Mid-stream schema evolution drains old-descriptor rows before closing the local appender and
  reopening the same remote stream with the serializer's current descriptor.
  The stream name and next offset do not change, so writer state stays version 2 and committables
  stay version 1.
  Fingerprint changes perform this refresh proactively; schema-mismatch responses perform it
  reactively and use the shared schema propagation schedule.
  Enabled `schemaUpdateOptions(...)` runs the same additive, etag-conditioned table reconciliation
  as the default-stream writer before reconnecting.
  Dynamic destinations reconcile and reconnect independently.
  `SchemaUnifier` is the common policy across both Storage Write API methods and `FILE_LOADS`.
  The Storage writers share `StorageWriteSchemaReconciler`, while `FILE_LOADS` retains its
  load-specific orchestration because it must return the schema carried by each load job, retain
  it in the destination's commit plan and run write-disposition-specific behavior.
- BATCH is supported (commit at end of input), streaming requires EXACTLY_ONCE +
  checkpoints-after-tasks-finish; recovery and eviction knobs are builder-configurable via
  `BufferedStreamOptions` with defaults.

## Consequences

The goccy emulator keeps no flush cursor, ignores requested append offsets and can hang after a
same-stream descriptor reconnect, so exactly-once schema-evolution semantics run against
deterministic fakes and real GCP.
The normal real-service schema test pre-creates the widened table so it does not inherit metadata
propagation latency.
Connector-driven propagation has a separately gated manual probe because the default-stream path
has previously shown a rare service-controlled tail near two hours.

[#30]: https://github.com/flink-gcp/flink-connector-gcp/issues/30
[#76]: https://github.com/flink-gcp/flink-connector-gcp/issues/76
[#77]: https://github.com/flink-gcp/flink-connector-gcp/issues/77
[#2272]: https://github.com/flink-gcp/flink-connector-gcp/issues/2272
[#2283]: https://github.com/flink-gcp/flink-connector-gcp/issues/2283
