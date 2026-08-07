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
- Date: 2026-07-25
- Issues: [#30]
- Modules: bigquery (`sink.storage`)
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md` § Exactly-once
  (buffered streams)

## Decision

`STORAGE_API_EXACTLY_ONCE` is buffered streams + 2PC on checkpoints.

- **One stream per writer subtask, reused across checkpoints and tracked in Flink writer state**
  (Dataproc-connector style; stream-per-checkpoint explicitly rejected — GCP support told the
  user frequent `CreateWriteStream` churn is not intended usage).
- Committable = (streamName, inclusive flushOffset, subtaskId); the committer calls `FlushRows`
  synchronously, `ALREADY_EXISTS` = already flushed = success, everything else throws (restart +
  idempotent re-commit; no deterministic-id machinery, no checkpoint stamper, no `.global()` —
  the committer runs at sink parallelism, the pre-commit topology is identity and exists only as
  the validation hook).
- Restore: synchronous probe append at the restored offset; offset conflicts / dead stream /
  reopen failure abandon the stream for a fresh one at offset 0 (rows past the restored offset
  were never committable, so they stay invisible).
- **Streams are never finalized anywhere** — real BigQuery rejects `FlushRows` on a finalized
  stream (verified; the batch IT caught it), so finalizing races restored-but-uncommitted
  commits; open streams' unflushed tails are invisible and cost nothing.
- Server-side row-level errors route to the `FailureHandler` with offset-recompute recovery
  (atomic request rejection → route failing rows, replay survivors + trailing batches;
  `ALREADY_EXISTS` during an offset-shifting replay is terminal).
- v1 scope: fixed destination only (the builder rejects `destinationResolver`), no mid-stream
  schema evolution (stream schema pinned at creation), BATCH supported (commit at end of input),
  streaming requires EXACTLY_ONCE + checkpoints-after-tasks-finish; recovery knobs are
  builder-configurable via `BufferedStreamOptions` with defaults.

## Consequences

The goccy emulator keeps no flush cursor (re-flush duplicates), so exactly-once ITs run against
real GCP; the emulator gets a single-flush smoke test only.

[#30]: https://github.com/laughingman7743/flink-connector-gcp/issues/30
