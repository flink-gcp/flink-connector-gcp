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

# ADR-0018: FILE_LOADS commits deterministic load jobs in the committer, on the checkpoint

- Status: Accepted
- Date: 2026-07-19 ([#14]); load stage revised 2026-07-20 ([#69]); committer schedules
  2026-08-01 ([#198]); revised by [#337] (2026-08-08); conflict handler revised by [#380]
  (2026-08-08); load-job grouping refined by [#284] (2026-08-08); job locations revised by
  [#491] (2026-08-10); streaming overflow revised by [#72] (2026-08-13); hierarchical overflow
  revised by [#598] (2026-08-13); data-only replacement revised by [#646] (2026-08-14);
  writer lifecycle revised by [#1128] (2026-08-28); destination commit concurrency revised by
  [#1129] (2026-08-29); writer checkpoint finalization concurrency revised by [#1164]
  (2026-08-30); laid-out temporary tables revised by [#1671] (2026-10-09); staging upload revised
  by [#2021] (2026-11-04); a job refused as it is submitted revised by [#2181] (2026-11-17); job project
  revised by [#2178] (2026-11-18, ADR-0206); retry ids uncapped by [#2181] (2026-11-19);
  temporary-table names derived from staged files by [#2289] (2026-11-23)
- Issues: [#14], [#69], [#72], [#198], [#337], [#380], [#284], [#491], [#598], [#646], [#1128], [#1129], [#1164], [#1671], [#2021], [#2178], [#2181], [#2289]
- Modules: bigquery (`sink.fileloads`)
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md` § File loads

## Decision

- Exactly-once via deterministic BigQuery job ids (hash of destination + sorted staged URIs)
  with get-then-submit re-attach. Written with the `google-cloud-storage` client directly (no
  Flink filesystem plugin dependency).
- **A load job is one destination in one staging format** (refined by [#284]). The format travels
  in the committable rather than being read from configuration at commit time, because a
  committable recovered from state has to be loaded as the format its file was *actually* written
  in — and a load job carries exactly one. So `CommitPlanner` keys on `(destination,
  format)`, which changes nothing for a commit whose committables share a format.
  The transitional commit after the format changes can contain both old and new formats.
  `WRITE_APPEND` and `WRITE_EMPTY` keep separate direct loads when both groups fit, preserving their
  previous behavior.
  `WRITE_TRUNCATE` and `WRITE_TRUNCATE_DATA` instead load both groups into temporary tables and use
  one combined final replacement, so a direct truncate cannot erase the other format's rows.
  Draining the old format first was rejected — the writer cannot see what is in committer state —
  as was refusing the mix, which would wedge the restart that produced it.
  The job ids need no format segment: they already hash the source URI list, and the two formats'
  files are distinct objects. The committable serializer **migrates** version 2 — the layout `main` has
  produced since [#69], all Avro by construction — where it still rejects version 1, which predates
  [#69] and never survived a job. Version 3 added the format; version 4, current since [#2178], adds
  the configured job project, and versions 2 and 3 migrate with none (ADR-0206).
- **Load jobs run in the committer** behind a pre-commit topology (`SupportsPreCommitTopology`)
  whose trailing `.global()` routes every subtask's committables to committer subtask 0. The
  [#14] post-commit-topology design was replaced in [#69]: records emitted to a post-commit
  topology during job shutdown are not guaranteed to be processed — verified empirically, the
  final streaming batch was lost — while committer commits ride the final-checkpoint wait and
  the framework's committer state. Independent jobs are submitted and awaited in deterministic
  waves of at most 50,000, keeping one connector run within BigQuery's per-project, per-region
  pending-job limit.
  The `WRITE_TRUNCATE_DATA` terminal queries are interactive and use waves of at most 1,000,
  matching their narrower queued-query limit.
  Cleanup is best-effort on success only; a staging bucket lifecycle rule is the documented
  mitigation for orphans.
- **Independent destinations commit concurrently under a fixed bound** ([#1129]).
  The complete plan remains global and side-effect-free, then the single committer dispatches at
  most `maxConcurrentDestinations` destination actions at once.
  The default is 8 and the accepted range is 1 through 64.
  Before the pool has been needed, one destination stays on the calling thread, avoiding a pool
  hand-off for the fixed-table case; later singleton phases reuse the initialized pool.
  Each pool thread owns one BigQuery REST client, load-job runner and retrying table admin; REST
  clients and their mutable caches stay on their owner thread.
  The workers share only a concurrent submitted-job registry, so a job submitted by one worker can
  be awaited after the global submission barrier by whichever worker receives that destination
  action in the wait phase.
  A bounded reconciliation phase reconciles each destination once, including any reads needed for
  create-after-miss or lost-etag retries.
  Each following load, copy-level and final-action wave is submitted across all destinations before
  its jobs are awaited, so client calls are bounded without reducing BigQuery's server-side job
  parallelism.
  The wave sizes hold the combined pending-job count at 50,000 and the interactive-query subset at
  1,000, so destination concurrency does not multiply the service quota waves.
  The first observed ordinary failure stops new destination dispatch, but every job already
  submitted in the wave is awaited before `commit()` returns.
  A JVM-fatal failure aborts the current worker action immediately; the coordinator interrupts its
  peers, drains them for at most 30 seconds and then propagates the fatal failure so an unbounded
  peer cannot hide it from Flink.
  If the coordinator itself is interrupted, it uses the same bounded cancellation and drain.
  An interrupt-ignoring daemon worker may outlive `commit()`, and a later JVM-fatal failure is sent
  first to Flink's fatal-exit handler, with a connector-log fallback if that handler returns.
  Failures are then ordered by destination plan position: the first is primary and later failures
  are suppressed, except that a JVM-fatal failure remains primary so Flink can act on it.
  Temporary-table and staging-object cleanup starts only after every destination succeeds.
  On 2026-08-29, seven-sample local runs measured 50 destination tasks with a 5 ms fake service
  delay at 359 ms serial and 48 ms with 8 workers; 200 tasks measured 1,411 ms and 179 ms, and 50
  tasks with a 50 ms delay measured 2,796 ms and 390 ms.
  The one-destination median remained 7 ms for every configured bound because it uses the inline
  path.
  A bounded real-GCP acceptance run the same day used two writer subtasks, two default Avro/Zstandard
  files per destination and a native `JSON` column.
  It exercised 10 destinations at concurrency 1, 4, the unset default 8 and 16, then 50
  destinations at the unset default 8 and 16: 140 load jobs and 280 staged files in total.
  Every table contained its two distinct rows and matching JSON payload, and the post-run sweep
  found no target table or staging object.
  End-to-end arm times, including MiniCluster startup, uploads, the validation query and resource
  deletion, were 49.917, 26.537, 22.295 and 23.464 seconds for the four 10-destination arms, then
  84.194 and 80.574 seconds for the two 50-destination arms.
  Those single observations validate correctness and bounded service operation rather than a
  throughput target: network and service variance plus serial harness work make them unsuitable as
  an SLA or a precise concurrency ratio.
  Values above 8 remain an operator opt-in because every active destination can add a thread, a
  REST client and pending service work.
- **Overflow keeps one deterministic copy hierarchy before its final action** ([#598]). Each load
  partition first lands in an idempotent leaf temporary table. Up to 1,200 leaves feed the final
  copy unchanged. A larger set is grouped in source order into copy jobs of at most 1,200 sources;
  each group of two or more becomes an intermediate temporary table, while a final singleton is
  carried forward without spending a copy job. A level is fully awaited before the next level is
  submitted, and only the final level writes the destination. Every intermediate uses
  `CREATE_IF_NEEDED` plus `WRITE_TRUNCATE`, so the existing deterministic job re-attachment also
  makes a retry idempotent; a laid-out destination's are created ahead with an expiration and filled
  under `CREATE_NEVER` by jobs that also name the table's creation time (ADR-0183). Names and ids hash their ordered inputs and include the hierarchy level,
  group and checkpoint attribution. A destination whose copy needs a layout has its temporary
  tables renamed, and the jobs filling them salted, with that layout after reconciliation, and its
  final copy takes a fixed marker and an equivalent id
  ([ADR-0183](0183-file-loads-temporary-tables-take-the-destinations-live-layout.md)).
  Leaf and intermediate tables remain until the final action succeeds, then join the existing
  best-effort cleanup.
- **`WRITE_TRUNCATE_DATA` is a batch-only data replacement that preserves destination metadata**
  ([#646]).
  BigQuery load and query jobs support this disposition, but copy jobs do not.
  A destination that fits one load job therefore uses it directly.
  Overflow and mixed-format replacement first use only `WRITE_TRUNCATE` loads and copies to build a
  deterministic aggregate temporary table, then run one standard-SQL `SELECT *` query into the
  existing destination with `CREATE_NEVER` and `WRITE_TRUNCATE_DATA`.
  The query preserves the destination schema and constraints atomically; enabled native
  `ALLOW_FIELD_ADDITION` and `ALLOW_FIELD_RELAXATION` options still travel on both direct loads and
  the terminal query.
  A terminal failure retains staged objects and every temporary table for deterministic re-attach.
  The query adds no new connector-specific IAM permission beyond the FILE_LOADS read/write/job
  permissions, but it scans all aggregate columns and therefore consumes query bytes or capacity
  slots, queued-query quota, and one destination-table modification.
  The connector deliberately sets no `maximumBytesBilled`; project query quotas remain the
  operator's spending boundary.
- **The complete job graph is bounded and validated before side effects** ([#598]). One commit may
  plan at most 100,000 load jobs and 100,000 copy jobs, matching BigQuery's project-wide daily
  quotas. The planner constructs every copy with at most 1,200 sources before schema reconciliation,
  table creation or job submission begins. The planner is `CommitPlanner`, and its fields are the
  sink configuration, the FILE_LOADS options, the Flink job id, the checkpoint id and the limits —
  no runner, no table admin, no staging storage. So "before side effects" is a property of what
  planning can reach rather than a rule each review has to re-apply; `LoadJobOrchestrator` receives
  the finished `CommitPlan` and is the only half that can act on it. Reconciliation may rewrite a
  laid-out destination's names and ids and drop its loads and intermediate copies once its final
  copy succeeded (ADR-0183), which never raises a count.
  This does not reserve shared quota: other workloads and
  failed attempts still consume the project's daily allowance. The cap is deliberately not a
  public option; a plan large enough to spend a whole project's default daily quota should be
  reduced with larger staging files or smaller commits instead.
- **Streaming FILE_LOADS** ([#69]): same `WriteMethod.FILE_LOADS` value, allowed under explicit
  `STREAMING` + checkpointing (`AUTOMATIC` stays rejected); `WRITE_APPEND` only. The checkpoint
  is the trigger: each completed checkpoint's committables are committed synchronously (a slow
  load delays the next checkpoint = backpressure; async in-flight loads were evaluated and
  rejected — `commit()` must mean durable, or a crash after the next checkpoint strands
  submitted-but-unconfirmed loads). A `FileLoadsCheckpointStamper` pre-commit map stamps the
  checkpoint id onto committables (the `Committer` SPI cannot see it); job ids gain a visible
  `-c<checkpointId>` segment (hash material unchanged) and derive their Flink-job segment from
  the committable's originating job id (stamped by the writer) so re-commits after a new-JobID
  restore still re-attach. On overflow, batch and streaming both load idempotent partitions into
  temporary tables, reduce more than 1,200 leaves through intermediate copy levels, and append only
  the final level to the destination with one atomic copy job; streaming temporary-table names
  include the checkpoint id, and leaf digests also distinguish staging formats that coexist during
  a transition. Streaming also requires EXACTLY_ONCE checkpointing and
  checkpoints-after-tasks-finish (the final batch rides the post-finish checkpoint). Quota guard
  at graph construction: interval < `minCheckpointInterval` (default 2 min) errors, < 5 min
  warns (1,500 load jobs/table/day, with an overflow copy still consuming one destination-table
  modification), plus a runtime cadence warning in the committer.
- **Writer resources follow a bounded active-destination lifecycle** ([#1128]).
  Each writer subtask keeps descriptor, Avro schema, converter and staging resources only for
  destinations with an open file.
  `maxOpenDestinations` defaults to 16 and finishes the least recently used file before opening a
  new destination; `destinationIdleTimeout` defaults to one minute and finishes inactive files on
  the mailbox task thread; a checkpoint finishes and clears every remaining active state.
  Files finished by eviction or rolling remain as small committables until that checkpoint;
  `maxPendingFiles` defaults to 10,000, counts finished and open files together, and fails the
  task before opening another file when reached.
  This second bound prevents LRU churn from replacing bounded active buffers with unbounded
  committable metadata; `pendingFiles` exposes proximity to it.
  Conversion of a new destination's first row precedes capacity eviction, so a poison row cannot
  churn a healthy file.
  Reopening rebuilds conversion state and uses a writer-global monotonic sequence under the
  attempt-specific random prefix, avoiding URI reuse across eviction and checkpoints.
  `maxSerializedRowBytes` defaults to 15,000,000 bytes and routes a larger serialized protobuf row
  to the failure policy before parsing and Avro conversion.
  The active limit is tunable because heap availability and destination locality are workload
  properties. The GCS upload chunk (`stagingUploadChunkBytes`, 4 MiB by default) is a separate
  knob since [#2021]; every open file holds one, so the writer's upload buffers are the chunk times
  the active limit.
- **Checkpoint finalization is serial by default and bounded when enabled** ([#1164],
  [ADR-0146](0146-file-loads-bounds-writer-checkpoint-finalization-concurrency.md)).
  `maxConcurrentCheckpointFinalizations` defaults to 1 and accepts 1 through 8 per writer.
  At a checkpoint or end of input, workers close independent open files while the task thread
  records committables, metrics and failures in input order. Every submitted close drains before
  return on success, ordinary failure, or interruption. A JVM-fatal worker failure instead wakes
  the task thread and cancels its peers so an earlier stalled close cannot hide the fatal failure.
  Appends, size rolls, capacity evictions and idle closes remain serial, and one open file gains no
  concurrency. They wait on the task thread unless the writer runs the background staging uploader, on by default
  ([#2021], [ADR-0195](0195-file-loads-offers-an-opt-in-background-staging-uploader.md)), whose single
  thread runs upload operations, sending full chunks and finalizing rolled, evicted and idle files,
  in submission order.
  The bound comes from real-GCS measurements across 1, 10 and 50 objects on both sides of the
  client's default 4 MiB upload chunk; ADR-0146 keeps the measurements, declined full-upload lanes and
  in-region confirmation.
- **Committer schedules are knobs** ([#198]): `loadJobPoll*` and `schemaReconcile*` on
  `FileLoadsOptions`, mapped by `toLoadJobPollSchedule()` / `toSchemaReconcileSchedule()`. Both
  pass the [#54] workload-versus-service test that kept the default-stream schema-wait schedule
  unexposed: completion polling paces the **caller's own** `jobs.get` quota and latency (it
  covers every overflow copy level too), and the etag-race budget absorbs contention from
  **other writers of the same table** — a property of the deployment, not of BigQuery. **It is
  not about this job's destination parallelism**: the single committer creates one task and one
  reconciler per distinct table, never two for the same table. (The first draft said the opposite,
  transplanting the wording from the
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
- **Every job id names its location, derived from the destination dataset when `location()` is
  unset** ([#491]). BigQuery scopes a job to (project, location, id), and a `jobs.get` naming no
  location resolves against the US multi-region only. Measured 2026-08-10 with standalone SDK
  calls against a us-central1 dataset: a load submitted under a location-less `JobId` succeeds —
  the server infers the location from the destination dataset — but the same location-less id
  then answers `null` to `jobs.get`, and resubmitting it throws the SDK's own
  `NullPointerException` out of the duplicate-id absorber's location-less re-fetch
  (google-cloud-bigquery 2.68.0; upstream
  [googleapis/google-cloud-java#14025](https://github.com/googleapis/google-cloud-java/issues/14025),
  fix PR [googleapis/google-cloud-java#14026](https://github.com/googleapis/google-cloud-java/pull/14026)
  — which only renames the failure here: with the NPE guarded, the retry still finds nothing to
  attach to). Normal runs never noticed, because ids are fresh per attempt and `awaitJob` polls
  the server-returned `JobId`, which carries the server-filled location — so the gated suite's
  green against its regional dataset was no evidence about re-attach. A commit retry or failover
  recovery against anything but the US multi-region was permanently stuck, with BigQuery holding
  the id for six months. The runner now derives each job's location from its destination
  dataset's metadata — the dataset decides, because BigQuery runs a load job in the location of
  the dataset it writes, so the derived value is exactly where the previous attempt's job lives —
  one `datasets.get` per dataset, memoized for the runner's lifetime (a dataset's location is
  immutable), failures converted to the SPI's `IOException` like every other lookup ([#337]
  above). The derivation is a new permission for a location-less FILE_LOADS deployment —
  `bigquery.datasets.get` on the destination datasets — and its not-found message offers both
  readings, because a 404 does not establish non-existence: GCP's disclosure convention can
  answer it for a resource the caller may not see, and the wrong-location measurement below
  answers it for a dataset that demonstrably exists. Setting `location()` remains the escape
  that makes no metadata call. A configured `location()` wins unchanged. Multi-region datasets need no special case:
  `datasets.get` reports `US`/`EU`, valid job locations — and `US` pins exactly what the
  location-less lookup already resolved to, which is why only US-multi-region deployments were
  ever unaffected. The declined alternative was requiring `location()` at `build()` under
  `FILE_LOADS`, mirroring the query source's reuse knob (ADR-0089): FILE_LOADS supports dynamic
  destinations across datasets, where one value cannot cover cross-region routing — an explicit
  location that disagrees with a job's destination dataset fails the submission (measured
  2026-08-10: `jobs.insert` under `EU` against the us-central1 dataset answers `404 Not found:
  Dataset`) — and the
  requirement would have taxed every FILE_LOADS user, including the unaffected US ones, for a
  recovery path most runs never take. This answers, for load jobs, the `location()` granularity
  question ADR-0016 deferred: per destination, derived. The gated
  `BigQueryLoadJobRunnerRealGcpITCase` holds the re-attach against the suite's regional dataset
  with no location configured — the only coverage of the path a fresh-id happy run cannot reach.

## Revision: a job refused as it is submitted fails the attempt (2026-11-17)

The `-rN` probes exist to step past a failed job a previous attempt left under an id, and the
[#380] handler extended them to a failed zombie behind a conflict.
`submitOrAttach` also probed past a job that its own `jobs.insert` returned already failed, which
a refused request does.
Measured on 2026-11-17 ([#2181]): a load under `CREATE_NEVER` into a table dropped since the
commit reconciled it came back from `jobs.insert` done, with `notFound`, under the base id and every
retry id in turn, so one attempt spent all six ids, and the retried commit, whose reconciliation had
created the table again, found every id failed and could never load.

A job the runner's own submission created and that is already failed now fails the attempt with its
error, naming the job.
The id is spent, as any failed id is, and the next attempt's probe steps past it to the next one,
so a request refused for a reason the retry removes costs one restart and one id.
A refusal no retry removes fails the commit on every restart, one id each.
A failed job found by the probe or behind a conflict is still probed past within the attempt; the
SDK's own already-exists absorber returns a job with no status (above), which is attached to as
before.
The change applies to every job the runner submits.
Only the load's refusal at submission was measured; the final copy that
`BigQueryFileLoadsTempTableLayoutRealGcpITCase` drives into a destination with an incompatible
layout was refused while it ran, not at submission, in the same run, so that path is unchanged.

## Revision: the retry ids have no cap (2026-11-19)

The walk past failed ids stopped after five retry ids and then failed the commit for good, with
every restart from that checkpoint failing the same way.
The number was chosen when the runner was written, and no measurement or service limit stood
behind it ([#2181]).
Every id the walk passes holds a job some attempt created, so the first unused id ends it, and
`submitOrAttach`, `copySucceeded` and the equivalent copy's look-up now walk without a cap.
How often a failing commit is retried is the Flink restart strategy's decision, and each restart
reports the service's own error rather than a give-up message.

The cap also turned a lost response into a lasting failure.
When the response to a `jobs.insert` is lost and the SDK's retry is answered 409, `create`'s
conflict look-up hands back this submission's own job, which the runner cannot tell from a
previous attempt's zombie; refused, it is probed past within the attempt like one.
Under the cap each such loss, like each failed job behind a genuine conflict, spent an id toward
the give-up; without it, the walk moves on to the next id.
A run of such conflicts would keep an attempt submitting while it lasted; none has been observed,
and no handling is added for it.
The refusal at submission still fails the attempt: with no cap, probing past it would submit one
refused job after another for as long as the attempt lasted.
The query source's reuse walk had the same cap for the same reason and loses it too (ADR-0089).

## Revision: temporary tables are named from their staged files (2026-11-23)

The Decision's idempotency key is a hash of the staged URIs, and direct loads have always used it.
The temporary-table path did not.
A leaf temporary table's name hashed the Flink job id, the checkpoint id, the destination and the
staging format, and every intermediate table, copy and terminal query hashed the tables it read, so
none of them depended on the files ([#2289]).
The path assumed that a Flink job id and checkpoint id never repeat, and they do.
Flink's application mode with high availability fixes the job id from the cluster id, so a redeploy
without state reuses it and numbers checkpoints from 1 again; restoring an older checkpoint repeats
the ids that followed it; and two `FILE_LOADS` sinks writing one table in one job share both.

Measured on 2026-11-23 against BigQuery, a second commit of other files under the same ids found the
first commit's final copy done and re-attached to it, and its own rows never reached the
destination.
Nothing failed and nothing was logged above INFO.
The plain, laid-out and batch cases each held 3 rows where 6 were written, while the same commit
under the next checkpoint id, under another job id, or on the direct-load path held 6.
Under `WRITE_TRUNCATE_DATA`, which never asks whether its final copy succeeded, the final copy and
terminal query re-attach by their ids alone in the same way.

A leaf temporary table's name now also hashes the partition's sorted staged URIs.
Each writer instance puts a whole random UUID (122 random bits) in its object names, so two runs'
URIs differ where the Flink job id, subtask and attempt repeat, and every downstream name and id
becomes this commit's own through the table paths it hashes.
The writer used only the UUID's first eight hexadecimal digits before; with names now derived
from the URIs, those 32 bits were all that told two runs' files apart, and the whole UUID costs
24 more characters in each object name.
The id of the job that fills a leaf now also hashes that table, because a re-attach compares ids,
never the table a job filled.
A retry over the same committables still derives the same base names and ids, to which ADR-0183's
layout salt, table incarnation and final-copy marker apply as before.

The dataset of a destination's temporary tables travels in the committable too, as the run that
staged the file resolved it: the configured `tempDataset`, or the destination's own dataset.
Every temporary name and every copy id hashes table paths, which include the dataset, so a commit
restored under another configured `tempDataset` derived other ids from the same files.
Found by reading the code in the independent review of [#2289], not by a run: when an attempt completed one
destination's final copy and failed on another destination, the restore under another dataset
missed that copy, loaded the retained files again and copied them a second time; once a completed
commit had deleted its files, it failed on every restart instead.
The recorded dataset gives the restored commit the tables and ids its attempt used, whatever the
restored job configures, so a change of `tempDataset` applies to files staged after the restore.
The resolved dataset is recorded, not the configured option as ADR-0206 records the job project.
A run with no `tempDataset` would then record nothing, which could not be told from a committable an
older version staged, and a restore after setting the option would copy that commit again.

The record's absence also marks a file a version before #2289 staged, whose commit keeps that
version's names (`FileLoadsCommittable#isStagedByOlderVersion`).
A successful commit deletes its staged files, and every restore commits the last checkpoint's
committables again, so a committable restored from state an older version wrote must be planned
under the names its attempt used.
Under the new names it would re-attach to loads that filled tables the copy no longer reads, or,
with new load ids too, load files already deleted, and fail on every restart.
The committable serializer's version 5 records the dataset; versions 2 to 4 read without one, and a
version-5 committable written back from such state keeps that absence.
The marker is persisted rather than derived from the serializer version, because a restored
committable that has not committed is written again in the current version first: a failed commit,
or a retained unaligned checkpoint whose channel state carries the older version's committables to
the committer as new elements.
Every file this version stages records a dataset: the writer hands the resolved dataset to each file
as it opens, the public constructors require one, and in main code only the serializer can create
a committable without it, through a package-private factory.
The committer refuses a batch that mixes recorded and unrecorded committables; restored and new
committables never share a checkpoint, so a mix means an invariant broke.
Recorded datasets may differ, one per destination, so the check compares presence, not value.

Two alternatives were declined in favour of the presence rule.
A separate naming-scheme enum duplicated the record: every committable the writer emits records a
dataset, so the enum's older value marked exactly the committables without one, and it named a
value no new data would ever carry.
Deriving the marker from the serializer version alone loses it in the write-back above.

Two further alternatives were declined.
Planning restored committables under the new names would stall the ordinary upgrade, a restore from
a stop-with-savepoint, until the state was discarded, which is worse than the rows a collision can
lose.
Proving from job metadata that an old-scheme copy read this commit's rows cannot work: a load job
records its URIs and its table, while a copy records only its source tables, so a copy that ran
after this commit's loads may still have read rows another commit wrote into the same tables
before it.

What remains is the old behaviour for committables an older version wrote.
A commit of such committables that repeats an earlier commit's ids can still re-attach to that
commit's copy.
Such a commit also records no temporary dataset, so it is planned in the configured one, and a
redeploy that changes `tempDataset` can make it fail on every restart or copy its rows a second
time, depending on whether its leaves are laid out and its staged files remain.
Both are what the older release would have done with them, whenever after the upgrade such a
commit is planned; files the upgraded writer stages are exposed to neither.
Every restore of state an older release wrote should therefore keep its `tempDataset` and its
effective job project (ADR-0206), since those committables record neither.

The older names can be removed once no state an older release staged is restored.
The retirement test is the record's absence, not the serializer version, because version-5 state
can carry committables written back from older state.
Removing them changes the compatibility ADR-0200 promises for a committable's serialized form: the
serializer would reject a committable without a dataset with a message naming the upgrade path
(restore once on a release that reads it, let those commits finish, and take a new checkpoint or
savepoint), as it rejects version 1 today, and `isStagedByOlderVersion`, the planner's older-name
branch and the nullable dataset would go.

## Design references

Moved here from the module README's provenance section ([#1096]), because the designs credited
are the ones this record's decisions rest on:

- [Apache Beam](https://github.com/apache/beam) `BigQueryIO`'s FILE_LOADS design — the
  `BatchLoads`/`WritePartition`/`WriteTables`/`WriteRename` decomposition: per-destination
  bin-packing against load-job limits, the temp-table + copy-job overflow path, updating the
  final table schema before the copy (copy jobs support no schema update options), GC of staged
  files only after the final load, copy, or query completes, and the streaming
  `triggeringFrequency` model, whose trigger role the Flink checkpoint takes here.
- [GoogleCloudDataproc/flink-bigquery-connector](https://github.com/GoogleCloudDataproc/flink-bigquery-connector)'s
  `BigQueryIndirectSink` / `BigQueryLoadJobOperator` — a SinkV2 post-commit topology on a single
  non-parallel operator, deterministic BigQuery job ids with get-then-submit re-attach for
  exactly-once retries, 1.5 GB size-based file rolling, and best-effort cleanup. The post-commit
  half is what [#69] replaced with the committer; the deterministic-id half is what the Decision
  keeps.
- [Apache Flink](https://github.com/apache/flink)'s SinkV2 committer/committable machinery
  (`CommitterOperator`, `GlobalCommitterOperator`) and the `FileSink` pre-commit-topology
  precedent, studied under [#69] to establish that streaming commits must run in the committer;
  the stamper and committer here are independent implementations over the public SinkV2 API.

[#14]: https://github.com/flink-gcp/flink-connector-gcp/issues/14
[#54]: https://github.com/flink-gcp/flink-connector-gcp/issues/54
[#69]: https://github.com/flink-gcp/flink-connector-gcp/issues/69
[#72]: https://github.com/flink-gcp/flink-connector-gcp/issues/72
[#198]: https://github.com/flink-gcp/flink-connector-gcp/issues/198
[#337]: https://github.com/flink-gcp/flink-connector-gcp/issues/337
[#284]: https://github.com/flink-gcp/flink-connector-gcp/issues/284
[#380]: https://github.com/flink-gcp/flink-connector-gcp/issues/380
[#491]: https://github.com/flink-gcp/flink-connector-gcp/issues/491
[#598]: https://github.com/flink-gcp/flink-connector-gcp/issues/598
[#646]: https://github.com/flink-gcp/flink-connector-gcp/issues/646
[#1096]: https://github.com/flink-gcp/flink-connector-gcp/issues/1096
[#1128]: https://github.com/flink-gcp/flink-connector-gcp/issues/1128
[#1129]: https://github.com/flink-gcp/flink-connector-gcp/issues/1129
[#1164]: https://github.com/flink-gcp/flink-connector-gcp/issues/1164
[#2021]: https://github.com/flink-gcp/flink-connector-gcp/issues/2021
[#2178]: https://github.com/flink-gcp/flink-connector-gcp/issues/2178
[#1671]: https://github.com/flink-gcp/flink-connector-gcp/issues/1671
[#2181]: https://github.com/flink-gcp/flink-connector-gcp/issues/2181
[#2289]: https://github.com/flink-gcp/flink-connector-gcp/issues/2289
