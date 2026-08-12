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

# ADR-0022: Exactly-once uses buffered streams reused across checkpoints, never finalized

- Status: Accepted
- Date: 2026-07-25; dynamic destinations added 2026-08-13 ([#76])
- Issues: [#30], [#76]
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
  synchronously, `ALREADY_EXISTS` = already flushed = success, everything else throws (restart +
  idempotent re-commit; no deterministic-id machinery, no checkpoint stamper, no `.global()` —
  the committer runs at sink parallelism, the pre-commit topology is identity and exists only as
  the validation hook).
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
- No mid-stream schema evolution (each stream's schema is pinned at creation), BATCH supported
  (commit at end of input), streaming requires EXACTLY_ONCE + checkpoints-after-tasks-finish;
  recovery and eviction knobs are builder-configurable via `BufferedStreamOptions` with defaults.

## Consequences

The goccy emulator keeps no flush cursor (re-flush duplicates), so exactly-once ITs run against
real GCP; the emulator gets a single-flush smoke test only.

[#30]: https://github.com/laughingman7743/flink-connector-gcp/issues/30
[#76]: https://github.com/laughingman7743/flink-connector-gcp/issues/76
