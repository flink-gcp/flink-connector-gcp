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

# ADR-0018: FILE_LOADS commits deterministic load jobs in the committer, on the checkpoint

- Status: Accepted
- Date: 2026-07-19 ([#14]); load stage revised 2026-07-20 ([#69]); committer schedules
  2026-08-01 ([#198])
- Issues: [#14], [#69], [#198]
- Modules: bigquery (`sink.fileloads`)
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md` § File loads

## Decision

- Exactly-once via deterministic BigQuery job ids (hash of destination + sorted staged URIs)
  with get-then-submit re-attach. Avro-only staging in v0.1, written with the
  `google-cloud-storage` client directly (no Flink filesystem plugin dependency).
- **Load jobs run in the committer** behind a pre-commit topology (`SupportsPreCommitTopology`)
  whose trailing `.global()` routes every subtask's committables to committer subtask 0. The
  [#14] post-commit-topology design was replaced in [#69]: records emitted to a post-commit
  topology during job shutdown are not guaranteed to be processed — verified empirically, the
  final streaming batch was lost — while committer commits ride the final-checkpoint wait and
  the framework's committer state. Jobs are submitted all at once then awaited. Cleanup is
  best-effort on success only; a staging bucket lifecycle rule is the documented mitigation for
  orphans.
- **Streaming FILE_LOADS** ([#69]): same `WriteMethod.FILE_LOADS` value, allowed under explicit
  `STREAMING` + checkpointing (`AUTOMATIC` stays rejected); `WRITE_APPEND` only. The checkpoint
  is the trigger: each completed checkpoint's committables are committed synchronously (a slow
  load delays the next checkpoint = backpressure; async in-flight loads were evaluated and
  rejected — `commit()` must mean durable, or a crash after the next checkpoint strands
  submitted-but-unconfirmed loads). A `FileLoadsCheckpointStamper` pre-commit map stamps the
  checkpoint id onto committables (the `Committer` SPI cannot see it); job ids gain a visible
  `-c<checkpointId>` segment (hash material unchanged) and derive their Flink-job segment from
  the committable's originating job id (stamped by the writer) so re-commits after a new-JobID
  restore still re-attach; streaming overflow submits direct append jobs sequentially instead of
  temp-table+copy. Streaming also requires EXACTLY_ONCE checkpointing and
  checkpoints-after-tasks-finish (the final batch rides the post-finish checkpoint). Quota guard
  at graph construction: interval < `minCheckpointInterval` (default 2 min) errors, < 5 min
  warns (1,500 load jobs/table/day), plus a runtime cadence warning in the committer.
- **Committer schedules are knobs** ([#198]): `loadJobPoll*` and `schemaReconcile*` on
  `FileLoadsOptions`, mapped by `toLoadJobPollSchedule()` / `toSchemaReconcileSchedule()`. Both
  pass the [#54] workload-versus-service test that kept the default-stream schema-wait schedule
  unexposed: completion polling paces the **caller's own** `jobs.get` quota and latency (it
  covers the overflow path's copy job too), and the etag-race budget absorbs contention from
  **other writers of the same table** — a property of the deployment, not of BigQuery. **It is
  not about this job's parallelism**: `prepared.global()` means one job has exactly one
  reconciler. (The first draft said the opposite, transplanting the wording from the
  default-stream path, where the etag loop really is per writer subtask.) **The polling attempt
  cap stays unexposed and hardcoded to `Integer.MAX_VALUE`**: a batch load may legitimately run
  for hours, so any bound a user could set would fail loads that were progressing normally, and
  the Flink job's own timeouts are the right ceiling. Exposing it "for symmetry" is the mistake
  to avoid. `BigQueryLoadJobRunner` takes its schedule as a constructor argument rather than
  reading the options — it implements the `LoadJobRunner` SPI and must not depend on the
  FILE_LOADS options type.

[#14]: https://github.com/laughingman7743/flink-connector-gcp/issues/14
[#54]: https://github.com/laughingman7743/flink-connector-gcp/issues/54
[#69]: https://github.com/laughingman7743/flink-connector-gcp/issues/69
[#198]: https://github.com/laughingman7743/flink-connector-gcp/issues/198
