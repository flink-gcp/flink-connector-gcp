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
  2026-08-01 ([#198]); revised by [#337] (2026-08-08); conflict handler revised by [#380]
  (2026-08-08)
- Issues: [#14], [#69], [#198], [#337], [#380]
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
- **Completion polling re-fetches the job through `BigQuery#getJob`, never `Job#reload()`**
  ([#337]). Measured against `google-cloud-bigquery` 2.68.0 (2026-08-08): `Job#reload` is
  `bigquery.getJob(getJobId())` plus a throw — it raises `BigQueryException` as soon as the job it
  fetched carries an error, whatever that job's state. Every load that failed *while being polled*
  therefore left `awaitJob` as an unchecked exception, past the `IOException` the `LoadJobRunner`
  contract promises and past the message the runner composes from the error and its execution
  errors; only a job that was **already** failed when the runner took hold of it reached that
  message. Two doors hand over such a job: `create`'s own HTTP-409 handler — closed by the
  [#380] revision below — and the SDK absorber's statusless answer, which nothing can judge
  until the first poll resolves it. Same request, without the throw; do not simplify it back. Two facts the same measurement turned up,
  both about `BigQueryImpl.create` rather than about this connector: it absorbs an already-exists
  error itself for a non-random job id, re-fetching with `JobOption.fields(STATISTICS)` and
  returning that job when it was created within 24 hours — so the runner's own 409 handler is
  reached only past that window, or when the SDK's regex does not match, or when it never consults
  the regex at all (a null cause or cause message); and the job it absorbs comes back with **no
  status**, which the polling loop treats as not-yet-done and resolves on the next fetch. What the
  operator gains: `Job#reload`'s throw built a `BigQueryException(List<BigQueryError>)`, whose
  message is the *first execution error* and which names **no job id** — and which the SQL uber-jar
  reports under its relocated class name; the composed `IOException` names the job id that actually
  ran, which after a probe is not the one the caller passed.
- **Every `jobs.get` the runner makes answers a failure with `IOException`, naming the job**
  ([#337]). The three lookups — the probe, the poll, and the one `create` makes after a conflict —
  went through the client unguarded, so a lookup that failed past the SDK's own retries left
  `submitOrAttach` and `awaitJob` as an unchecked `BigQueryException`: the same contract hole the
  polling change closes, reached by a different door, and with the job id in neither the message
  nor the code. Nothing about the commit's fate changes — both types fail it identically, and the
  SDK has already retried by the time either is thrown — but the type the SPI declares is now true
  of every path through it. The conflict lookup keeps the 409 as a **suppressed** exception on the
  failure it reports, because the conflict is the half that says the id is already taken.
- **The HTTP-409 handler judges the job it finds before attaching to it** ([#380]). A failed job
  id cannot be reused, and `submitOrAttach` spends up to five `-rN` probes making sure it never
  attaches to a failed job — yet the conflict handler attached to whatever the losing race left
  behind, so a zombie that had already failed handed the commit a failure predating the attempt
  and cost one Flink restart (the next attempt's probe found the failed id and moved on; measured
  self-healing, which is why this waited for [#380] rather than riding [#337]). The handler now
  hands the conflicting job back to the probe loop, which applies the same finished-with-an-error
  verdict it applies to a job the probe finds: a live or statusless job is attached to, a failed
  one is probed past, feeding the same `-rN` warning and the same give-up message. The statusless
  half is load-bearing: the job the SDK's own already-exists absorber returns carries no status
  (above), so a null status must read as attachable or every absorbed conflict would burn a
  retry id. Which is also the revision's limit — a failed zombie behind a statusless answer is
  still attached to, its stored failure surfacing from the first poll at the same one-restart
  cost, because no verdict exists to read at attach time.

[#14]: https://github.com/laughingman7743/flink-connector-gcp/issues/14
[#54]: https://github.com/laughingman7743/flink-connector-gcp/issues/54
[#69]: https://github.com/laughingman7743/flink-connector-gcp/issues/69
[#198]: https://github.com/laughingman7743/flink-connector-gcp/issues/198
[#337]: https://github.com/laughingman7743/flink-connector-gcp/issues/337
[#380]: https://github.com/laughingman7743/flink-connector-gcp/issues/380
