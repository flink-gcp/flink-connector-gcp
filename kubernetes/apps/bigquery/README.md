# BigQuery Tier-3 recovery application

This internal application supplies the finite workload for [issue #1312](https://github.com/flink-gcp/flink-connector-gcp/issues/1312).
It uses the production BigQuery sinks on Flink 2.2.1 and Java 17: the two Storage Write sinks, and the FILE_LOADS sink that [issue #1549](https://github.com/flink-gcp/flink-connector-gcp/issues/1549) added for the [#1313](https://github.com/flink-gcp/flink-connector-gcp/issues/1313) finalization scenario.
The opt-in `tier3-bigquery` profile builds it separately from the published connector artifacts.

## Build and local validation

From the repository root:

```sh
mise x -- just tier3-bigquery-verify
```

The recipe builds `target/bigquery-recovery.jar` and copies runtime dependencies, including the Parquet and Hadoop runtime the FILE_LOADS `PARQUET` staging format needs, with their original license and notice files, into `target/image-lib/`.
The Dockerfile places these JARs in `/opt/flink/usrlib/` over the reviewed Flink base image and enables its bundled GCS filesystem plugin.
The entry point is `io.github.flink.gcp.connector.tier3.bigquery.BigQueryRecoveryJob`; the job URI is `local:///opt/flink/usrlib/bigquery-recovery.jar`.
No dependency download is needed at Pod startup.
The [manual publication workflow](../../../.github/workflows/tier3-images.yaml) verifies and builds this payload as the `bigquery-recovery` GAR package.
The [trial publication](../../images/README.md#bigquery-trial-publication) built commit `8bfe56d5154d40b2ccef81f16129ea873ead43a5` with the appender observations for the Storage Write trials, and has since been deleted.
The [FILE_LOADS trial publication](../../images/README.md#bigquery-file_loads-trial-publication) built commit `ef183e0c63d273e59b05599d064caf79e79dfc22` with the FILE_LOADS mode; its image refuses the `--parallelism` argument every delivery renders since [#1782](https://github.com/flink-gcp/flink-connector-gcp/issues/1782).
The [fault trial publication](../../images/README.md#bigquery-fault-trial-publication) built commit `25260e67823edc4efd60cc12a0db1f4d12912161`, the first image that accepts it, and records its digest and retention; dispatch names the digest through `application_digest` and verifies it live, and it is not pinned.
The application CI lane builds the Dockerfile against the same public Flink 2.2.1 digest without publishing or using GCP credentials.

Unit tests cover argument bounds, destination identities, serialized row sizes, Java serialization, input gaps and checkpoint state incompatibility.
Observation tests check original futures and exceptions, exact rows and offsets, failed opens/closes, reopening identities and log-output failures.
Sink tests serialize both observed sinks, construct their job graphs, create production writers, and round-trip a buffered writer state through restore and snapshot; the committer receives no appender observer.
Local MiniCluster tests restore the source and input identity operator from a checkpoint and a savepoint using a discard sink.
Separate local graph tests verify that every writer reaches every destination for every mode and destination count at parallelism two, and that the one writer does at parallelism one in the EO and FILE_LOADS modes.
Emulator tests create the application's production default-stream writer for both destination counts, write each destination sequentially, check the open/append/close observations, then query every table.
The application graph is tested separately: concurrent appends caused SQLite lock errors and RPC retries inside the pinned emulator in CI.
The pinned emulator assigns buffered offsets across streams and can hang on multi-stream flush, so it cannot exercise the production EO writer for this workload.
Existing connector tests cover that writer with deterministic service doubles, and the deployed EO trials of [#1427](https://github.com/flink-gcp/flink-connector-gcp/issues/1427) ran it against real BigQuery, one run at each destination count, with no duplicate or missing sequence across a savepoint upgrade and a JobManager failover, as their [findings](../../../docs/adr/evidence/0165-bigquery-trial-findings-1312.md) record.
The ALO emulator fixtures use one append of two rows per destination because follow-up appends are not reliably bound to their stream.
They establish local wiring and routing, not BigQuery exactly-once recovery, GCS checkpoint permissions or deployed Operator behavior.

FILE_LOADS has no emulator coverage: the connector refuses emulator endpoints for that write method, because the pinned emulator runs no load jobs and serves no Cloud Storage.
Local tests build the FILE_LOADS graph at the deployed interval, which the connector's streaming checks accept, and confirm that the Storage Write interval would be refused.
The gated `RecoveryFileLoadsITCase` runs the application graph in a MiniCluster against real BigQuery and Cloud Storage, once with Avro and once with Parquet staging.
It stops the job with a savepoint after a load has committed and before the input ends, restarts it as the upgrade phase, and requires every sequence in exactly one row of its destination table with no staged file left behind.
It writes run-unique tables into `BQ_IT_DATASET` and stages under `BQ_IT_GCS_BUCKET`, with a two-second interval and a matching `minCheckpointInterval` that only this test may set; `just e2e` runs it with the connector suites.

## Trial inputs

| Argument | Contract |
| --- | --- |
| `--run-id` | Required Tier-3 run label, at most 40 characters; reuse only when restoring the same trial |
| `--mode` | Required `ALO` (default stream), `EO` (buffered stream) or `FILE_LOADS` (staged files and load jobs) |
| `--destinations` | Required `10` or `50` |
| `--records` | Finite count covering every destination on each of two writers (at least twice the destination count) and at most 2 GiB of serialized input; defaults to 30 minutes at 1 MiB/s |
| `--bytes-per-second` | Datagen offered rate in serialized row bytes, from 1 through 1,048,576; defaults to 1,048,576 |
| `--parallelism` | The writers' parallelism, `1` or `2`; defaults to `2`. It is not part of the checkpointed input identity, so a savepoint upgrade can rescale |
| `--phase` | `initial` by default, or `upgrade` |
| `--require-restored` | `false` by default; `upgrade` requires `true` |
| `--staging-format` | FILE_LOADS only: `AVRO` by default, or `PARQUET` |
| `--max-concurrent-checkpoint-finalizations` | FILE_LOADS only: the connector's default, 1 |
| `--max-concurrent-destinations` | FILE_LOADS only: the connector's default, 8 |
| `--max-staging-file-bytes` | FILE_LOADS only: the connector's default, 16,777,216 |
| `--max-open-destinations` | FILE_LOADS only: the connector's default, 16 |

The Storage Write modes refuse the five FILE_LOADS arguments, and the connector's `FileLoadsOptions` builder checks their ranges as the arguments are parsed.
The application leaves `maxPendingFiles` at the connector's 10,000, which the builder requires to be at least `--max-open-destinations`.
They are part of the checkpointed input identity in FILE_LOADS mode, so an upgrade cannot change them.

ALO rows are exactly 65,536 serialized protobuf bytes, and EO and FILE_LOADS rows exactly 1,024 bytes.
The default counts are 28,800 and 1,843,200 respectively: each offers 1,887,436,800 bytes before replay and RPC framing.
FILE_LOADS converts each protobuf row into the staged file's format, so its staged bytes differ from these serialized bytes.
This is a logical input ceiling, not a billable-byte, retry, elapsed-time or query-cost ceiling.
Source pacing and serialization overhead must be observed on the execution host before interpreting throughput.

The source emits sequence numbers from zero through `records - 1` at parallelism one.
A checkpointed identity operator checks each next sequence before forwarding it.
The sink runs at the phase's `--parallelism` with maximum parallelism 128, keeping its UID, so a savepoint upgrade can restore it at another parallelism; deterministic partitioning by `(sequence / destinations) % parallelism` sends a complete destination cycle to each writer in turn.
Every writer reaches every destination, while each row retains `sequence % destinations` as its table identity.
The job enables checkpoints every 30 seconds in the Storage Write modes and every 120 seconds in FILE_LOADS mode, with at most one concurrent checkpoint.
FILE_LOADS refuses an interval below its `minCheckpointInterval`, two minutes by default, because each checkpoint issues a load job per destination table against a daily per-table modification quota.
Writer and buffered-stream options retain their production defaults; this workload does not search capacity or tune those defaults.
The FILE_LOADS sink appends to the provisioned tables and stages under `gs://flink-gcp-tier3-bigquery/runs/<run-id>/staging`, inside the run prefix that the state IAM condition, the bucket lifecycle rule and cleanup already cover.
Staged files therefore also count against the 10,000 objects cleanup lists under the run prefix.
At the 1 MiB/s offered rate and the 120-second interval a checkpoint stages roughly 120 MiB before conversion, and its commit deletes those files; the `fl-10` trial's [findings](../../../docs/adr/evidence/0165-bigquery-fileloads-findings-1313.md) record no commit backlog and no peak state-prefix size.
Its five arguments above default to the connector's values.
The FILE_LOADS sink also names the dataset's location, `us-central1`, so its committer never reads dataset metadata: without a location it looks each destination dataset up to place its load jobs, which would need `bigquery.datasets.get`.
The Storage Write modes leave the location unset, as before.
Submitting the load jobs needs project-wide `bigquery.jobs.create`, which the workload identity holds through a custom role added under [issue #1550](https://github.com/flink-gcp/flink-connector-gcp/issues/1550); as their creator it can read them without `bigquery.jobs.get`.
The gated integration test runs under the E2E identity; the `fl-10` trial's 190 load jobs, submitted by the deployed workload, show that the deployed identity holds the grant.

The FILE_LOADS sink is not decorated for observation.
It has no appender, and the connector already reports what a trial reads of it: an INFO line per writer checkpoint with the files staged, one per submitted and completed load job, and one per committed checkpoint with its row count, plus the writer's `filesStaged`, `pendingFiles` and `openDestinations` metrics and the committer's `loadJobsSubmitted`.
The committer runs on a separate vertex, and only its subtask 0 commits.
The default Avro staging holds one 4 MiB upload chunk per open destination, about 64 MiB per writer at the default 16 open destinations; Parquet staging also buffers a row group per open file.
The `fl-10` trial ran Avro staging at 10 open destinations with TaskManager heap peaking at 474 MiB of 644 MiB, about twice the EO trials', as its [findings](../../../docs/adr/evidence/0165-bigquery-fileloads-findings-1313.md) record; the staging buffers' share was not isolated, and Parquet staging has not run on these TaskManagers.

## Deployment definition

[`pkg/bigquery.#Application`](../../pkg/bigquery/application.cue) defines one finite trial as a FlinkDeployment for the pinned Operator 1.15.0 schema and Flink 2.2.1 application image.
A delivery supplies `run.id`, `run.image`, `run.mode` (`ALO`, `EO` or `FILE_LOADS`), `run.destinations` (`10` or `50`) and `run.parallelism` (`1` or `2`); `run.phase` defaults to `initial` and `run.records` defaults to the mode's 30-minute-equivalent count above.
A FILE_LOADS delivery may also set `run.fileLoads` (`stagingFormat`, `maxConcurrentCheckpointFinalizations`, `maxConcurrentDestinations`, `maxStagingFileBytes`, `maxOpenDestinations`), whose defaults and ranges are the connector's, with `maxOpenDestinations` capped at those 10,000 pending files; the package renders all five as arguments, and refuses an out-of-range or unknown knob or any knob in a Storage Write mode.
The lifecycle delivery's `bigquery_mode` tag selects the mode and leaves the knobs at their defaults, and its `bigquery_fault` tag selects each phase's parallelism, as the [fault trials](#offline-execution-proposal) list it.
Explicit record counts retain the application's minimum of twice the destination count and 2 GiB serialized-input ceiling.
The offered rate is fixed at 1 MiB/s.
The image must be a lowercase SHA-256 digest from the fixed `bigquery-recovery` GAR package; this checks its form, while publication provenance and registry retention still require verification before admission.

Use this package from a delivery under `runs/` with `run.namespace: "tier3-bigquery"` and the usual run ID, image and expiry inputs.
The package sets the namespace and the existing `bigquery` ServiceAccount; the run hierarchy supplies resource labels, expiry annotation and AMD64 selectors on the common and manager-specific Pod templates, with Spot on the TaskManager alone.
[`tests/fixtures/bigquery.cue`](../../tests/fixtures/bigquery.cue) shows the package composition used by the disposable render tests.
No concrete run, image digest or execution window is selected by this package.

The definition requests one JobManager and one TaskManager per writer, each with 1 CPU, 2 GiB memory and 1 GiB ephemeral storage; container requests equal limits.
At two writers the application total is 3 CPUs, 6 GiB memory and 3 GiB ephemeral storage, excluding the Operator and supervisor.
Each TaskManager has one slot, job parallelism is the writers' and autoscaling is disabled; the Java graph retains its source parallelism of one and takes the sink's from `--parallelism`.
These resource choices follow the smoke Pod shape and still need approval in the complete trial budget; they do not establish sufficient heap or a runtime Pod ceiling.
No PVC or application volume is requested.

Checkpoints run every 30 seconds in the Storage Write modes and every 120 seconds in FILE_LOADS mode, matching the interval the application enables, with at most one concurrent checkpoint, a 120-second timeout and two retained checkpoints.
The hashmap backend uses filesystem checkpoint storage, with checkpoints, savepoints and Kubernetes HA state below `gs://flink-gcp-tier3-bigquery/runs/<run-id>/` in separate `checkpoints`, `savepoints` and `ha` prefixes.
The fixed-delay restart strategy permits three attempts with a ten-second delay; the external supervisor must bound wall-clock runtime and repeated process incarnations.

For the upgrade, render the same inputs with `run.phase: "upgrade"` and update the same deployment.
Both phases use `upgradeMode: savepoint` and `allowNonRestoredState: false`; only the phase argument and `--require-restored` change, with the latter becoming `true` on upgrade.
A rescale trial's upgrade also changes `--parallelism`, the job parallelism and the TaskManager count together.
The definition also sets `kubernetes.operator.job.upgrade.last-state-fallback.enabled=false` so an unavailable savepoint cannot be replaced by last-state recovery, and `kubernetes.operator.snapshot.resource.enabled=false` to use Operator 1.15.0's status-based savepoint reporting instead of separate FlinkStateSnapshot resources.
These settings follow the generic recovery exercise that [#1311](https://github.com/flink-gcp/flink-connector-gcp/issues/1311) ran; an Operator upgrade must revisit this version-specific reporting choice.
The running application's checkpointed identity also rejects changed input parameters on restore.
The package describes that transition; it does not wait for baseline observations, prove a completed savepoint, apply the upgrade or delete the JobManager.

`just tier3-check` renders both phases for all six mode/destination combinations and each fault's sizes against the pinned schemas and checks input bounds, namespace policy, Pod resources, state paths and that the phases differ only in phase, restoration and a rescale's sizes.
Those static tests use synthetic digests and do not establish image availability, server admission, task placement or GCS restore permissions.
The [production dispatch](#production-dispatch) path must bind the exact rendered manifests to the approved trial, provision owned tables, admit the complete workload budget, collect observations, run the query oracle and clean all owned resources.

## Offline execution proposal

`flink-tier3 render --scenario bigquery-recovery` combines one trial's proposed limits, initial/upgrade applications and supervisor delivery into a JSON bundle.
A proposal is not an approval: dispatch admits this scenario only through the [production dispatch](#production-dispatch) below.
The bundle contains `approved: false` and an empty `approval.json`; rendering never grants execution permission or calls a cloud API.
It may download pinned public CUE schema dependencies when the local cache is cold.

Name the trial with `--trial`: `alo-10`, `eo-10`, `alo-50`, `eo-50` or `fl-10`, one delivery method at one destination count.
`fl-10` is the one FILE_LOADS trial, at 10 destinations with the five sink inputs at the connector's defaults: it asks whether finalization survives the two recoveries, not how it scales.
These run a savepoint upgrade and a JobManager failover, with the writers at parallelism two in both phases.

The `eo-10` and `fl-10` cells also name a fault that takes the place of one of those recoveries ([#1779](https://github.com/flink-gcp/flink-connector-gcp/issues/1779)), as the [shared recovery exercise](../../../docs/adr/0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md#shared-recovery-exercise-fault-kinds) runs it:

| Trial | Upgrade | Failover | Writers and TaskManagers, initial then upgrade |
| --- | --- | --- | --- |
| `<cell>-tm-replacement` | Savepoint upgrade | The TaskManager that reported the latest input | 2, 2 |
| `<cell>-rescale-out` | Savepoint rescale out | JobManager | 1, 2 |
| `<cell>-rescale-in` | Savepoint rescale in | JobManager | 2, 1 |

A rescale stays inside the namespace's three-Pod quota, the one the estimate charges for every trial, so the estimate is the same; the three `eo-10` ones are preregistered as [Production dispatch](#production-dispatch) lists.
That quota has no slot for a replacement, so a TaskManager replacement's new Pod is refused until the deleted one finishes terminating, and Flink retries its creation; the recovery's 300-second deadline includes that wait.
Every trial now renders `--parallelism`, which a `bigquery-recovery` image built before [#1782](https://github.com/flink-gcp/flink-connector-gcp/issues/1782) refuses as an unknown argument at job start, so each needs an image published after it.
One bundle describes one trial, not a campaign authorization; the renderer does not allocate unique run IDs/nonces or prove that other trials ran.
The record count is fixed to the mode's 30-minute-equivalent input above.
Every trial has the scenario's query budget: 12 slots, each billing at most 4 GiB with a 60-second timeout.
Every visibility retry that submits a new job must consume another slot.

For example, render from the repository root with a synthetic image digest for local review:

```sh
mise x cue uv -- uv run --locked --package flink-tier3 --no-dev flink-tier3 render \
  --scenario bigquery-recovery --trial eo-10 \
  --run-id example-1312 --nonce aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --started-at 2026-09-21T00:00:00Z --expires-at 2026-09-21T01:30:00Z \
  --active-seconds 5220 --revision "$(git rev-parse HEAD)" \
  --application-image us-central1-docker.pkg.dev/flink-gcp/flink-tier3/bigquery-recovery@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  > /tmp/bigquery-proposal.json
```

The intended window is exactly 90 minutes, reserving its last 15 minutes for cleanup; the proposed supervisor deadline leaves the final three minutes for external settlement.
Dates must be whole-second UTC values.
The example date and synthetic digest are illustrative, not a runnable delivery.
The initial and upgrade manifests use the same input identity, and the only upgrade changes are the phase and restoration arguments.
The proposal embeds the `ResourcePlan` and the application/upgrade/supervisor hashes.
The ConfigMap carries that same proposal as `proposal.json`, separately from the empty approval document.
The declared revision is a requested source identity: the renderer does not prove checkout cleanliness, GitHub main ancestry, image publication/provenance, registry retention or a live namespace/Operator baseline.
Those checks and final approval belong to [production dispatch](#production-dispatch).

The planning estimate, USD 2.35 for every trial, is what the owner approves before a dispatch; nothing at run time compares spend against it.
It charges all five running Pods for the full window at the existing conservative CPU/memory/ephemeral-storage rates, adds all reserved query bytes at USD 6.25/TiB, and adds USD 1 for other incremental costs.
It does not separately price occupancy of the shared policy's extra Operator replacement slot.
The reviewed sources are [GKE Autopilot pricing](https://cloud.google.com/kubernetes-engine/pricing) and [BigQuery on-demand pricing](https://cloud.google.com/bigquery/pricing), checked on 2026-09-20; the proposal records that date as the estimate's basis.
It assumes on-demand query billing and takes no free-tier, Spot or commitment discount.
The reserve is an allowance, not a measured bound on storage, ingestion, retries, networking or telemetry; existing cluster standing charges and unexpected cleanup overruns are outside the estimate.
The generated observation plan records three minutes of warm-up, ten minutes each of baseline and post-recovery observation, ten-minute startup/visibility limits and a five-minute limit per recovery.
[Internal recovery execution](#internal-recovery-execution) describes the clocks the exercise enforces from them, and [production dispatch](#production-dispatch) is the only path that runs it.

## Table and row identity

Every destination is in the fixed `flink-gcp.flink_gcp_tier3_bigquery` dataset.
Its name is `bq_<run-id with hyphens replaced by underscores>_d<destination>`, with zero-based decimal destination numbers and no zero padding.
Run IDs do not allow underscores, so this conversion preserves distinct run identities.
The external runner must pre-create exactly those empty tables before admission.
The sink uses `CREATE_NEVER`; the workload creates no tables or datasets.

Rows contain nullable `run_id STRING`, `sequence INTEGER`, `destination INTEGER` and `payload BYTES` columns, with a value supplied for every field.
Padding is deterministic pseudorandom data derived from run ID and sequence, avoiding an all-zero fixture.
Replay serializes the same sequence into the same bytes and table.
The deployed query oracle must require every expected sequence in its correct table; EO and FILE_LOADS additionally require exact uniqueness, while ALO records duplicate multiplicities.
The [offline query oracle](#offline-query-oracle) below supplies the aggregate data check.
The resource adapter and controller below provide internal query, table creation and deletion operations.
The internal admission path connects those operations to deployment creation, and [production dispatch](#production-dispatch) reaches it through the actor factories.

## State and observations

The source, input identity operator and sink have fixed UIDs.
Input operator state retains a lineage UUID, the next sequence and the run's identity: run ID, mode, destination count, record count and offered byte rate.
A changed identity, missing state, multiple state entries or an out-of-range position fails restore.
Only phase and the restoration requirement may change across an upgrade.
This workload has no rescaling contract.

Logs emit `bigquery-progress` on the first record of each attempt, approximately every 30 seconds of offered input and the final input record.
The fields include run ID, phase, lineage, restoration status, processed count and sequence.
`bigquery-snapshot` records checkpoint ID and processed count; `bigquery-checkpoint-complete` records the completion notification.
These are source-side observations, not sink acknowledgements or query-visible row counts.
An abrupt attempt failure can lose logs; the later collector must correlate these observations with Flink checkpoint history and query results.

### Appender call observations

The application decorates the default-stream appender factory and the buffered writer's service factory through internal sink hooks.
The production writers retain their options, timers, retry logic, writer-state serializers, pre-commit topology and committer implementation.
The factory hooks run for each new or restored writer; the buffered committer uses its original service factory.
No observation object or live client travels in the serialized job graph.

At INFO level, `bigquery-appender v=1` emits a writer-registration line and one result line for each appender open, append invocation and close invocation.
Each writer incarnation receives a fresh UUID, including after failover or upgrade.
Appender IDs increase within that incarnation; reopening the same stream receives a new ID.
Use the run ID, phase, mode, subtask, attempt and writer UUID together when correlating logs across TaskManagers.

| Field | Meaning |
| --- | --- |
| `sequence` | Monotonic observation sequence within one writer incarnation, starting at one |
| `appender`, `stream` | Local appender ID and buffered stream name or synthesized default-stream label; zero/`none` for writer registration |
| `operation`, `outcome` | `writer/created`, or `open`, `append`, `close` with `returned` or `threw` |
| `rows`, `protoRowsBytes` | Rows and serialized `ProtoRows` bytes supplied to this append invocation, including connector re-appends and synchronous rejections; zero for other operations |
| `offset` | Supplied buffered append offset; `-1` for default-stream calls and non-append events |
| `callNanos` | Time until the delegate returned or threw; for append this ends at future hand-off |
| `elapsedNanos` | Elapsed monotonic time since this writer's observer was created; compare only within that incarnation |
| `openAppenders` | Successful observed opens minus first successful closes; a failed close leaves the handle unresolved |
| `lostEvents` | Cumulative observer-output RuntimeExceptions before this event; backend drops are not detectable here |
| `failure` | Exception class for a synchronous failure, otherwise `none`; messages and row payloads are omitted |

For ALO, `stream` is synthesized as `<table path>/streams/_default` before opening the appender; it is not read back from the SDK.
The production factory passes the short `<table path>/_default` name to the SDK, while the emulator factory uses the longer form.
Normalize these labels when correlating logs; an open failure still carries the intended destination label.

The appender returns the identical `ApiFuture`; the observer adds no callback, retry, wait or cancellation behavior.
An append's `returned` outcome establishes only that the delegate returned a future, even if that future is already failed.
It establishes neither a physical network attempt nor server acknowledgement or query visibility.
SDK-internal retries are opaque; connector-issued re-appends are separate observed invocations.
`protoRowsBytes` includes the `ProtoRows` envelope, but excludes the full RPC framing and SDK retries; it is not a billable-byte measure.
The logged open count is an appender-handle observation, not a connection count or a proof that a failed close retained its resource.

Logs are best-effort and synchronous, so their cost can affect throughput and must be accounted for in the deployed characterization.
An observer-output RuntimeException leaves the delegate's result unchanged and increments `lostEvents`; later sequence gaps expose those missed events.
Disabled INFO logging, transport loss or abrupt process termination can also lose evidence without a detectable final gap.
The external collector must retain writer registrations and correlate these incomplete observations with Flink metrics, checkpoint history and query oracles; missing events are not evidence of zero activity.
The observer holds counters and one handle per wrapped appender, and retains no row, response future or unbounded event collection.

### Offline query oracle

The `flink-tier3 bigquery` commands prepare the final data check without contacting GCP.
They require the exact run ID, mode, destination count and finite input record count from the approved trial; they do not infer these from observed output.
For example, from the repository root:

```sh
mise x uv -- uv run --locked --package flink-tier3 --no-dev flink-tier3 bigquery query \
  --run-id example-1312 --mode EO --destinations 10 --records 200 > oracle.sql
mise x uv -- uv run --locked --package flink-tier3 --no-dev flink-tier3 bigquery assess \
  --run-id example-1312 --mode EO --destinations 10 --records 200 --result aggregate.json > oracle-report.json
```

The first command writes GoogleSQL; it submits no query.
The second requires an already collected JSON array of aggregate objects and prints one JSON report to standard output; the example redirects it to `oracle-report.json`.
These commands submit and export nothing; a deployed run's queries go through the [query handoff](#query-requests-and-runner-release).
An installed CLI can run either command outside the checkout.
The array must contain every result row, with the generated field names; BigQuery INT64 strings and JSON integers are accepted.
Missing, repeated or extra destinations, a changed input identity, inconsistent counts, duplicate JSON keys or evidence over 64 KiB are rejected.
Exit code zero means the query-result check passed, one means a data mismatch, and two means invalid input or unreadable evidence.

Each object in `aggregate.json` has this shape; this example shows only destination zero, while the complete array must contain all 10 or 50 destinations:

```json
{
  "run_id": "example-1312",
  "mode": "EO",
  "expected_records": "200",
  "destinations": "10",
  "destination": "0",
  "total_rows": "20",
  "valid_rows": "20",
  "distinct_sequences": "20"
}
```

The result collector must flatten each query-result row into these named fields using the returned schema and retain all pages.
A raw REST `rows[].f[].v` envelope or job-metadata wrapper is not this format.
The internal resource adapter supplies this collector; a deployed run reaches it only through the [query handoff](#query-requests-and-runner-release), and no CLI subcommand submits or exports a query on its own.

The query reads exactly the run's 10 or 50 named tables and emits an aggregate even for an empty table.
It counts every row, including rows with another run ID, null identity fields, an out-of-range sequence, or a mismatch between physical table, declared destination and sequence modulo destination count.
Only rows satisfying all those conditions contribute to `valid_rows` and exact `distinct_sequences`.
It uses [exact aggregate functions](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions), without an approximate distinct count or a WHERE clause that could hide invalid rows.
The expected sequence count for each table includes the remainder when the input count is not divisible by the destination count.
A matching distinct count over the valid finite sequence domain proves completeness for that table; a matching total count alone would let a missing row and a duplicate cancel.

| Report field | Meaning |
| --- | --- |
| `invalid_rows` | All observed rows minus rows with valid identity, range and routing |
| `missing_sequences` | Expected sequences minus distinct valid sequences, summed across tables |
| `duplicate_rows` | Valid rows minus distinct valid sequences; extra copies, not the number of duplicated identities |
| `verdict` | `pass` only with no invalid or missing rows, and also no duplicate rows in EO or FILE_LOADS mode |

ALO reports duplicate copies while permitting them; EO and FILE_LOADS reject them.
FILE_LOADS derives each load job's id from the staged files it loads, so a retried commit re-attaches to its job instead of loading the files twice, and the connector claims exactly-once for it.
The report includes per-destination counts and totals, with `scope=query-result` to distinguish this check from a deployed recovery verdict.
It does not inspect payload bytes, prove table ownership, authenticate evidence, establish a checkpoint/fault boundary or prove that no later writes occur.
Identity fields in the aggregate bind the input parameters for accidental-mismatch detection; they are query literals, not a service attestation.
A substituted or edited aggregate can pass this offline check.
In particular, generating and assessing an EO trial with `--mode ALO` can permit duplicates when that input count also fits the ALO limit.
Rows contain no mode field, so this check cannot discover that mistake; the executor must take the mode from the approved application configuration, and the report must be interpreted with its recorded mode.

The [resource adapter](#resource-adapter) creates the tables and binds a completed query job's result pages to a trial.
The internal resource controller persists intent, query slots and query evidence as described below.
The executor must supply the approved cumulative retry/visibility budget and run the final check after the workload has stopped writing.
The local tests execute the generated SQL unchanged on synthetic SQLite tables to exercise row, NULL, empty-table and duplicate semantics.
Their SQLite build must provide the `MOD` math function.
That coverage does not establish BigQuery query acceptance or streaming visibility; the deployed trials ran the generated SQL against BigQuery, as their [Storage Write](../../../docs/adr/evidence/0165-bigquery-trial-findings-1312.md) and [FILE_LOADS](../../../docs/adr/evidence/0165-bigquery-fileloads-findings-1313.md) findings record.

### Resource adapter

[`flink_tier3.bigquery.resources`](../../../tools/tier3/src/flink_tier3/bigquery/resources.py) provides internal REST v2 table and query operations for the BigQuery actors.
It has no CLI route and does not extend lifecycle admission.
Its synthetic HTTP tests cover request construction, lost responses, create conflicts, cleanup and result pagination; the deployed trials of [#1427](https://github.com/flink-gcp/flink-connector-gcp/issues/1427) and [#1552](https://github.com/flink-gcp/flink-connector-gcp/issues/1552) used it against the real service to provision, query and delete their tables, and neither record reports an ownership conflict; the FILE_LOADS trial found no temporary table to delete.

The caller must persist one `ResourcePlan` before any write: the approved `Trial`, a fresh 32-character hexadecimal nonce, an absolute table expiration, the number of query slots, maximum bytes billed per slot and job timeout.
These values have no operational defaults or implied approval.
Reuse that intent unchanged across restarts, reserve each slot durably before submission and retain the table creation receipts and returned query evidence.
The plan permits only the trial's 10 or 50 table names in the fixed dataset and query slots numbered from zero through `query_slots - 1` in `us-central1`.
Slot count times maximum bytes billed is the planned query-byte ceiling when all actors use that unchanged plan and the same submitting identity; approval must also account for storage, writes, cluster resources and other costs.

Table creation sets the application's four nullable fields, explicit expiration within the next 24 hours, and labels binding run ID, nonce and the complete trial identity.
An existing table of the same name, found before the create or after a create conflict, is taken as the one a lost create response left.
Neither its metadata nor its creation time is compared with the plan: each run has its own table names, one run holds the environment lock at a time, and every table expires within 24 hours, so another table of that name cannot exist ([#1726](https://github.com/flink-gcp/flink-connector-gcp/issues/1726)).
Taking the table does not prove it empty: admission still requires exclusive access and a quiescent data check because streaming metadata can lag.
Deletion deletes the planned table name, and an absent table is already gone.
Before cleanup, the executor must stop and fence all creators/writers and hold exclusive lifecycle access through deletion.
Only a persisted creation intent authorizes deletion, and expiration is only a fallback.

Each query slot has a deterministic job ID that does not change after a timeout or restart.
Submission returns an existing job of that ID, or the one a create conflict names, without comparing its configuration with the plan, for the same reason as tables.
There are no automatic retries, fresh retry IDs or polling loops in the adapter.
A new visibility observation must consume another durably reserved slot; reusing a slot reads the same job's result.
The [REST job configuration](https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfiguration) bounds billed query bytes, but `jobTimeoutMs` is a best-effort service cancellation request.
The adapter checks the caller's absolute deadline before requests, after response headers and at streamed-body boundaries, caps each response at 1 MiB and bounds request timeouts by the remaining time.
An operation-specific adapter view shares the session, plan and clock while taking the earlier of the operation deadline and the original adapter deadline; it never mutates or extends the original deadline.
These checks do not establish a hard wall-clock stop or cancel an already submitted job.
Cleanup requests cancellation of that slot's job, and must poll its status separately until `DONE` before treating cancellation as complete.

Result collection requires a successful `DONE` job and billed-byte statistics within its limit.
It checks every page's job reference, completion, total row count and scalar schema, follows page tokens with a destination-count-plus-one page ceiling, and refuses repeated tokens, missing/duplicate destinations and aggregates over 64 KiB.
It flattens the returned `f`/`v` cells using the schema, runs the offline oracle, and returns the full job metadata/statistics, rows and `scope=query-result` report for the caller to persist.
It does not establish checkpoint provenance, table quiescence or a deployed recovery verdict.
Collect results as the submitting identity: [anonymous result tables are private to their creator](https://docs.cloud.google.com/bigquery/docs/cached-results#how_cached_results_are_stored), so the supervisor's project-level job get/update grants do not by themselves authorize reading the runner's result table.
The resource controller below supplies durable intent, query evidence and a cleanup pass.
The internal handoff and lifecycle loops supply shared deadlines and cleanup accounting; actor construction below supplies the HTTP session; [production dispatch](#production-dispatch) constructs the runner and the in-cluster entrypoint the supervisor, and dispatch verifies idle afterwards.
Provisioning passes the earlier of the approved start plus 600 seconds and the query window's end to all of its REST operations.
Query submission, job status and every result page share the requested observation's fixed deadline.
Common cleanup passes its cleanup deadline separately, allowing cancellation and table deletion after the query window closes without reusing an expired query deadline.
An earlier original adapter deadline still wins, so each actor's adapter must cover its intended operation window.
Expiry rejects late responses, including a 404 or an empty successful body; a failed creating operation retains its unresolved handoff marker, while a returned read/collection failure can release its marker under the existing protocol.

### Durable resource controller

[`flink_tier3.bigquery.lifecycle`](../../../tools/tier3/src/flink_tier3/bigquery/lifecycle.py) composes the adapter with generation-checked lifecycle records.
The internal runner and supervisor use this controller only with explicitly supplied handoffs.
Both execution entrypoints reach it only through the actor factories.
The caller must authorize the resource plan, retain exclusive environment ownership and provide a resource adapter with the appropriate operation or cleanup deadline.
The controller binds the complete service plan, run ID, nonce and trial arguments to the approved initial application hash.
[Production dispatch](#production-dispatch) validates the approval once and performs deployment admission.

Initialization stores the unchanged plan in `RunRecord.bigquery`.
Provisioning records a creation intent, then creates that table or takes the existing one and stores its creation receipt.
A table with a receipt is never created again, even when it has disappeared.
The runner reserves a named observation's slot through a conditional control update before submission; retries and process restarts retain the same slot and job ID, and exhaustion refuses another observation.
Query submission requires all table receipts and a running, unstopped record.
Concurrent late submission responses cannot overwrite a collected observation's state.
The BigQuery portion of the control record is bounded at 256 KiB.

Only the runner creates resources or collects results.
Collection writes the adapter's job metadata, rows and oracle report to a create-only `runs/<run-id>/bigquery/queries/<slot>.json` object before recording its generation and SHA-256.
An existing object must equal the freshly collected artifact when recovering a lost pointer write; changed metadata or conflicting contents fail closed.
Once recorded, collection verifies that generation and digest and reads the retained result without querying again.
These objects are protected against accidental overwrite by the controller, not by a bucket retention lock.
The caller still owns the total evidence budget and preservation/export policy.
A query-result report does not establish final visibility or absence of future writes.

Cleanup first persists the component's stop flag, then requires the caller's quiescence barrier.
By then common cleanup has removed the workload, and the barrier confirms on each pass that none of the run's writer-capable objects remains; merely setting the flag is insufficient.
The barrier holds only when it is called and cannot observe a Storage Write append already in flight, as [Quiescence barrier](#quiescence-barrier) records; FILE_LOADS cleanup also waits for the [connector-issued jobs](#connector-issued-jobs) created since the run started.
The controller does not implement or independently verify this barrier.
After it succeeds, cleanup requests cancellation of every possibly submitted slot and returns incomplete while any job is absent or not `DONE`.
An absent job after an uncertain submission remains unresolved rather than proving cleanup.
A slot recorded only as reserved has no submission intent and needs no cancellation.
Only after these checks does cleanup delete every table with a persisted creation intent, including one whose create response was lost, and confirm absence.
It never deletes an unrecorded table name except the temporary tables FILE_LOADS cleanup lists and records first, as [Connector-issued jobs](#connector-issued-jobs) describes.
The supervisor may perform this cleanup without collecting the runner's private query results.
A completed pass marks only the BigQuery component clean; it does not set run success, remove Kubernetes/GCS state, release the environment lock or declare the environment idle.

Synthetic tests cover conditional-update conflicts, restarts, lost responses, stop races, evidence generations, pending queries and receipt-based cleanup, including composition with the REST adapter.
They do not establish a functioning barrier or real-service acceptance.

### Query requests and runner release

[`flink_tier3.bigquery.handoff`](../../../tools/tier3/src/flink_tier3/bigquery/handoff.py) adds an internal protocol between the submitting runner and the observing supervisor.
The common runner settlement and supervisor cleanup paths accept this protocol through explicit constructor arguments.
The common model validates BigQuery approval inputs, and the [production dispatch](#production-dispatch) constructs these actor bindings through the actor factories.
The caller must allocate a query evidence budget and deadline, and give exactly one submitting process a fixed runner token.
The supervisor is bound without it: its Pod starts before the runner writes the binding, so it binds the fields its own approval fixes and adopts the token from the first binding it reads, refusing a replacement thereafter.
The token binds records; it is not an authentication credential or a lease that another process may take over.
After initializing this protocol on an empty resource intent, all runner provisioning and query calls must use it rather than the resource controller directly.

The supervisor persists one observation request at a time with an immutable name and deadline.
The runner reserves a deterministic query slot, submits and polls that job, and collects results with its own identity.
Each new observation consumes another slot; repeated polls do not submit a completed or pending job again.
The supervisor reads the resulting evidence object and checks its recorded generation, hash, intent, observation name and slot without calling the private query-result API.
Query evidence bytes are divided equally among the reserved slots and checked on the complete serialized artifact before writing and when reading it.
This bounds those query artifacts, not telemetry or the whole run's evidence; other evidence has no byte budget, as [Approval and shared resource policy](#approval-and-shared-resource-policy) records.
Request deadlines must fit inside the shared query window and table expiry.
An expired uncollected request ends observation processing for the trial; it cannot be abandoned to skip a required baseline or reuse its slot.
The caller must stop, release and clean up rather than retry a later observation.
The protocol checks deadlines before admitting each query operation; adapter request deadlines remain the caller's responsibility, and neither check is a hard service-side stop.

A conditional record update marks each runner call in flight before it starts.
Stopping sets the run-global stop flag as well as the BigQuery component flag; release also stops admission for the whole run.
This closes new requests and calls but does not acknowledge runner release.
Release is permanent and requires no in-flight or unresolved call; cleanup additionally requires the quiescence barrier and terminal query checks.
If a creating operation raises, its marker remains unresolved even when some of its work is known to have completed.
This conservatively blocks release and cleanup, including after a lost response, process failure or partial provisioning interrupted by stop.
Neither elapsed time nor a replacement process automatically clears the marker; incident recovery remains external work.
After a completed call or a failed read/collection, the runner attempts to clear its marker, since no BigQuery table or job creation remains unresolved by that call.
It retries only this conditional acknowledgement, with at most three attempts including the initial attempt, using a unique invocation ID so a lost response cannot clear a later call.
An already clear marker is accepted as acknowledged; a different active invocation aborts immediately without another attempt.
Persistent control-storage failures can still leave the marker unresolved and require external recovery; no resource operation is retried by this mechanism.
A completed collection may finish after stop, and the supervisor may read retained evidence after release.
Release does not prove Flink termination, settle unknown service operations, exclude other lifecycle processes or authorize deletion by itself.

Synthetic tests interleave the two actors through shared generation-checked storage, including stop during provisioning, submission and collection, lost responses, acknowledgement failures, invocation identity, expired requests, actor roles, pending jobs, replaced evidence and byte limits.
They do not establish workload fencing, real-service acceptance or a deployed recovery verdict.

### Common lifecycle loop integration

An admitting caller passes its process-local handoff to `Runner(env, bigquery=handoff)`.
`Runner.start()` initializes and provisions it after supervisor and Operator readiness, before admitting the application.
Only after admission has returned may that runner enter `settle()`.
While waiting for the supervisor, settlement services pending query requests until stop, evidence failure, the execution deadline or supervisor termination.
A query failure stops further polling and prevents settlement from preserving a successful trial verdict.
The runner attempts permanent release before leaving the wait, including on timeout; an unresolved call prevents release and leaves cleanup incomplete.
A recovery process must not reconstruct the original submitting process's token to manufacture this acknowledgement.
Repeated release attempts can recover a transient control failure; settlement emits a blocked-release diagnostic only when its cause changes.

The supervisor receives its own environment-bound handoff and a mandatory `quiesce` callback, which the supervisor factory supplies as the [quiescence barrier](#quiescence-barrier).
Supplying the callback without a handoff is rejected at construction.
Its `query(name, deadline=...)` method requests an observation and waits for archived evidence, maintaining heartbeats and auditing resources until the deadline or cancellation.
It returns the verified result without interpreting an incomplete baseline as a failed trial or treating a query report as a deployed recovery verdict.
The internal recovery exercise below chooses final-query boundaries and row acceptance criteria, and decides the deployed measurement verdict from them together with the observation coverage.

Common cleanup closes run admission and removes the owned Kubernetes workload before waiting for runner release.
For failed admission with recorded BigQuery intent, the supervisor first waits for runner release before entering common cleanup, so admission can finish recording any confirmed application creation.
Release alone does not resolve an unknown creation outcome: settlement still has to reconcile the persisted application intent, and service deletion still requires the creator/writer barrier.
Failure to persist the stop request marks evidence incomplete but does not prevent workload teardown.
The cleanup-phase transition is attempted independently of that stop write.
If no BigQuery intent was ever recorded, cleanup skips the attached handoff and continues ordinary settlement.
Once released, it waits for the quiescence callback to return exactly `true`, then invokes handoff cleanup, which rechecks that same barrier before any service deletion.
It waits for terminal queries and table deletion within its cleanup window; a barrier that fails its recheck aborts that pass.
Pod disappearance observed outside that callback is not a substitute for it.

## Quiescence barrier

[`flink_tier3.bigquery.quiesce`](../../../tools/tier3/src/flink_tier3/bigquery/quiesce.py) builds that callback from the run's own identity, inside the supervisor factory, so no caller supplies one: a `callable` check, the only check a supplied callback admits, cannot tell a proof from a constant such as `lambda: True`.

On every call it lists the nine kinds that can run or restore a writer, in the application namespace alone rather than through the four-namespace inventory, and refuses while any object owned by this run's application roots is still among them.
It is scoped on both axes: the supervisor asking the question is itself a Pod under a root in `tier3-system`, so seeding the ownership closure from every root would make it wait for itself, and `observed` is not namespace-scoped, so the namespace filter is load-bearing on its own.
Controllers are listed before Pods, because the reverse order loses a Pod created by a controller that is collected between the two pages — absent from the Pod listing, its owner absent from the controller listing.
That set is deliberately not the one `verify_idle` refuses after deletion: it adds `Deployment` and `ReplicaSet`, which restore a writer but which the idle check tolerates from the Operator, and it omits `PersistentVolumeClaim`, which the idle check refuses but which writes nothing.
Re-listing rather than trusting a snapshot is what catches a Pod a controller puts back between two calls, and deletes on this path are graceful, so a Pod leaving the API means its containers terminated.

A read that says nothing about the cluster — a transport error or a 408, 429, 500, 502, 503 or 504 — is retried inside the call rather than answered, bounded by a count of consecutive unreadable attempts. The cleanup window cannot bound it: `Cleanup.run` computes its own deadline through `cleanup_window(now)`, while `env.schedule.cleanup_end` is the static expiry the run has usually already reached when cleanup starts, so reading it here would spend the retry before the first attempt.
The barrier never reports "unknown", because it cannot: the cleanup predicate asks twice per poll and the second asker turns anything but `true` into a failure that ends the pass, so a single bad response would otherwise strand the shared environment lock on the path that runs on every successful cleanup.
A read that does say something about the cluster still raises.

It does not prove that no append is still in flight server-side, and it does not prove that no buffered write stream remains open. Neither is observable: the Storage Write API has no call that lists a table's streams, this connector deliberately never finalizes them, and their server-assigned names never leave Flink's own state. Beyond the listing, exclusivity is by grant rather than by observation — the only principal holding `bigquery.tables.updateData` on the dataset is the workload service account, reachable only through the Kubernetes service account in the namespace the listing just showed empty.

The measurement does not rest on this barrier. The query oracle reads only after the job reached `FINISHED` and its post-recovery window closed, which is where a settled read is established. What rests on the barrier is that cleanup does not delete a table while a Pod that could write to it is still alive, and the window between the barrier returning `true` and `tables.delete` completing remains outside what this component guarantees.
If release, the barrier or query termination does not complete, cleanup retains the control record and environment lock and does not scale down the Operator or claim idle.
A runner without a cleanup-capable supervisor can remove the workload but cannot impersonate the supervisor to finish BigQuery cleanup.
The runner writes its resource intent and handoff binding in one record change, so initialization cannot leave an intent without a binding.
An intent without one, which only a bare controller writes, is never adopted: the runner's initialization refuses it and common cleanup cannot invent proof of runner release, so such state and unresolved creating calls require externally reviewed incident recovery rather than automatic adoption by a replacement process.

Recorded BigQuery state also gates Operator shutdown, the `CLEANED` transition, settlement, idle verification and finalization, even when no handoff is attached.
The final receipt retains the complete BigQuery control snapshot, including table receipts and query evidence pointers.
A receipt of another run, or a control change after the receipt was derived, leaves the control record and lock in place.
When finalization deleted the control record but did not release the lock, recovery writes a minimal cleaned record and the retry keeps the receipt, as the [lifecycle runbook](../../lifecycle/README.md#stranded-environment-lock) describes.
Synthetic tests cover these settlement paths with fake actors and services.
The internal recovery schedule is described below.

The deployed exercise runs only through [production dispatch](#production-dispatch): its runner runs in CI under the runner's workload identity and the supervisor in the cluster under its own, and dispatch verifies the published image digest live.
The application itself does not scale the Operator, admit Pods, inject JobManager failure, submit queries or clean cloud resources.
The conditional GCS grants, including the `.inprogress/` grant [#1498](https://github.com/flink-gcp/flink-connector-gcp/issues/1498) added, are in the [BigQuery trial foundation](../../../opentofu/README.md#bigquery-trial-foundation); the deployed trials wrote and restored checkpoints and savepoints through them.

### Approval and shared resource policy

[Production dispatch](#production-dispatch) builds a `bigquery-recovery` approval and validates it once, before it takes the lock; the internal integration and cleanup tests construct one directly.
It binds `bigquery_trial` to the trial's delivery method and destination count, the only fields a trial chooses.
The approval also pins the initial and upgrade manifest hashes, the rig commit, runtime image digests, run/lock identity, and the observed namespace, Operator and idle-quota identities.
Nothing validates it again: admission checks only its window, and recovery reads it as written, so a later policy change cannot strand a held run's recovery.
A valid document is not execution permission on its own: dispatch still requires the environment lock, and the in-cluster entrypoint builds its supervisor only through the actor factory.
Internal runner admission requires an explicit environment-bound handoff; internal supervision additionally requires the approved upgrade and a quiescence callback.

The approval requires a 90-minute window with the final 15 minutes reserved for cleanup.
The application quota covers one JobManager and two TaskManagers, each using the existing 1 CPU, 2 GiB memory and 1 GiB ephemeral-storage shape.
The control quota covers the Operator, supervisor and one Operator replacement while the previous Pod terminates; five Pods run in the steady state.
The quotas, not a count in the approval, bound the Pods and PVCs; the approval carries no cost ceiling, because spend is approved from the estimate before dispatch.
Every trial retains its mode's fixed finite input size; the approval model derives the resource plan rather than accepting another independently editable copy.
The resource controller refuses a plan that differs from this approval before reading or creating service resources.

Common creation/adoption uses the approved application namespace.
Inventory covers every Tier-3 namespace and verifies owner UIDs, image digests and effective resources.
BigQuery cleanup records the tables a pass deleted in one control write, not one per table: trial `bq1312-alo-50-a1` wrote once per table, exceeded Cloud Storage's per-object mutation rate and crashed with most of its fifty tables left.
Provisioning likewise writes every missing create intent in one control write before the first create, and the receipts in one write after the creates: trial `bq1312-alo-50-a2` wrote twice per table and stopped halfway through fifty.
A supervisor that stops before admission completes records its reason as an `admission-stopped` event; that trial's supervisor stopped and left none.
State cleanup lists and generation-deletes only `runs/<run-id>/` in `flink-gcp-tier3-bigquery` and `.inprogress/flink-gcp-tier3-bigquery/runs/<run-id>/`, where Flink's GCS writer stages uploads before composing them into place; same-named objects in the smoke or evidence buckets and other BigQuery run prefixes remain outside that operation.
Synthetic tests cover these boundaries and compose the real approval, environment, handoff and common cleanup with fake Kubernetes/storage/BigQuery services.
They do not establish a functioning quiescence barrier or live service acceptance.

The actor factories, writer fencing, query-evidence accounting, measurement collection and the verdict are connected ([#1419](https://github.com/flink-gcp/flink-connector-gcp/issues/1419)–[#1424](https://github.com/flink-gcp/flink-connector-gcp/issues/1424)), and both execution entrypoints admit this scenario.
The query artifacts share 10 MiB, `QUERY_EVIDENCE_BYTES`, divided equally among the scenario's 12 query slots; the receipts and run documents have no byte budget, because the window bounds what a run writes and the evidence bucket's 30-day expiry bounds what it retains.

The collected queries' billed bytes are summed into the retained control state as `billed_bytes`.
Each query was already refused above its own `maximum_bytes_billed`, and the slot count bounds the worst case, but nothing added them up, so a finished trial could not state what it had actually spent.
A re-collected observation returns on the evidence pointer it already holds and is not counted again, and the total is restated from each query's recorded figure rather than accumulated into, because a conflicting write re-applies the same edit against a record that already carries it.

The shared final receipt records the BigQuery scenario and approved trial, with `success` taken from the execution verdict under *Deployed verdict* below.
A finalization retry keeps the original receipt and still requires the current plans to be empty for the approved nonce and all three roots.

## Approval-bound delivery

Dispatch binds the delivery its proposal rendered and checked: it embeds the approval in the immutable ConfigMap and reduces the supervisor Job's relative deadline to the time remaining when it binds.
All other rendered delivery fields are retained, and the complete ConfigMap data must fit Kubernetes' 1 MiB limit.
Nothing renders the delivery a second time to compare it with the first, and nothing checks the checkout's revision: the workflow checked the dispatched commit out fresh, and the approval's `sha` names it.

## Connector-issued jobs

A FILE_LOADS trial adds writes the Pod listing above cannot see: the committer submits load jobs as the workload, and a job keeps running server-side after the Pod that submitted it is gone.
They carry no labels, and their ids hash the staged files they load, so the supervisor finds them by listing the project's jobs that are pending or running since the run started, then reading each one whose id has the connector's `flink-bq-` shape.
A load belongs to the run when every source is a staged file under its `runs/<run-id>/staging/` prefix, and a copy or query when it writes one of the run's tables; a load of the run's files into any other table fails the listing.
The supervisor holds `bigquery.jobs.list` and `bigquery.jobs.listAll` for this, since without `listAll` the listing replaces every other principal's job id with `<REDACTED>`, the workload's included. It reads each candidate whole through its existing `bigquery.jobs.get`; a listed job whose id is not a job id, such as that placeholder, fails the listing rather than counting as no job, and a connector-shaped job that is listed but cannot then be read counts as unfinished, which it stops being once it is `DONE` and leaves the listing.

Cleanup runs this after the Pod barrier and the query checks, and before any table is deleted: while one of the run's jobs is not `DONE`, the pass returns without deleting, and the ids it waited for stay in the control record, because a job that outlived the workload is a finding.
The tables are `CREATE_NEVER` destinations, so a load that reached the service only after a table was deleted would fail; the wait is what keeps one that is already running from racing the deletion.
Once none remains, cleanup deletes any temporary table the overflow path created in the dataset since the run started, recording each before deleting it, so a pass that stops between the two loses nothing the next pass cannot list again.
The workload holds no `bigquery.tables.create`, so the overflow loads that would create one fail first; the check exists so that an unexpected one is recorded and removed rather than left to the dataset's one-day expiry.
The dataset holds only Tier-3 trial tables and one trial runs at a time under the environment lock, which is what lets a temporary table created since the run started be read as this run's.
After the run's tables are deleted, the write that records cleanup also records the objects left under the staging prefix, with their bytes and the first 20 names, before state cleanup deletes the run prefix that holds them.
A listing that fails is recorded as unreadable rather than raised, because this account must not keep the run's tables alive, and a later pass keeps the first count rather than reading a prefix state cleanup may have emptied.
A pass that settles therefore writes the control record twice, the stop request and its result, as a Storage Write cleanup does; only a pass that records a newly seen job or deletes a temporary table adds one write.
The receipt carries all of this as the `file_loads` part of its `bigquery` record.

## Production dispatch

The [run workflow](../../../.github/workflows/tier3-run.yaml) admits `bigquery-recovery` with two inputs beyond the common ones.
The campaign, its estimate, stop conditions and cleanup checks are preregistered in the [BigQuery trial preregistration](../../../docs/adr/evidence/0165-bigquery-trial-preregistration-1312.md), and its results, every attempt included, are in the [BigQuery trial findings](../../../docs/adr/evidence/0165-bigquery-trial-findings-1312.md).
The FILE_LOADS trial `fl-10` has its own records, the [BigQuery FILE_LOADS trial preregistration](../../../docs/adr/evidence/0165-bigquery-fileloads-preregistration-1313.md) and the [BigQuery FILE_LOADS trial findings](../../../docs/adr/evidence/0165-bigquery-fileloads-findings-1313.md); the [in-flight failover findings](../../../docs/adr/evidence/0165-bigquery-fileloads-inflight-findings-1685.md) record the trial of [#1685](https://github.com/flink-gcp/flink-connector-gcp/issues/1685).
The `eo-10` fault trials of [#1779](https://github.com/flink-gcp/flink-connector-gcp/issues/1779) are preregistered in the [fault trial preregistration](../../../docs/adr/evidence/0165-bigquery-fault-trials-preregistration-1779.md).

| Input | Contract |
| --- | --- |
| `trial` | `alo-10`, `eo-10`, `alo-50`, `eo-50` or `fl-10`, or one of the `eo-10` and `fl-10` fault trials above; the default `none` is refused |
| `application_digest` | The published `bigquery-recovery` GAR digest, verified live at dispatch and never pinned |

The owner approves the run's estimate before dispatching it; dispatching is the approval.
The window starts when dispatch admits the run, on the whole second, and lasts exactly 90 minutes, so queueing before the job starts costs the run none of its startup budget.

The checks that need neither the cluster nor the lock run first: the trial and digest, the dispatch from `main`, the rig commit (the dispatched `main` commit unless `rig_sha` names another), the run ID and an existing run's evidence.
Dispatch then snapshots the foundation, renders and verifies the proposal, takes live image receipts, builds and validates the approval and binds the delivery with the approval embedded, so a ConfigMap size refusal also arrives before the lock.
It then acquires the environment lock, checks the foundation again, writes the run documents and runs the runner inside its session, minting the runner token in this process.
If the actor cannot be built after the lock is taken, dispatch settles the run through the plain runner and reports the environment idle before it fails, so the workflow's finalization still proves the plans empty, writes an unsuccessful receipt and releases the lock.

The in-cluster entrypoint verifies the mounted application and upgrade against the mounted approval's pins, refusing a missing document by name, then builds the supervisor with its quiescence barrier and exercise and holds that session open for the whole supervision.
It is constructed without the runner token and adopts it from the binding, as [Query requests and runner release](#query-requests-and-runner-release) describes.

The workflow job runs for up to 120 minutes for this scenario, because the 90-minute window's serial budget is 114 minutes.
Enabling the scenario does not approve a run.

## Internal recovery execution

The common `Runner.start()` and `Supervisor.supervise()` paths support an explicitly attached BigQuery handoff.
They are internal integration points: the caller must bind the approved delivery, retain exclusive environment ownership and provide the writer/creator quiescence barrier and appropriate HTTP operation deadlines.
The production dispatch and in-cluster entrypoints reach these paths only through the actor factories, and a bare `Runner` or `Supervisor` without its handoff refuses; passing these tests does not approve a paid run.
A replacement runner must not reconstruct the submitting process's token.

Admission validates the handoff's environment, approved resource plan, query-evidence allocation and absolute query deadline before any mutation.
The deadline equals the approved cleanup start, and admission must complete within 600 seconds of the approved start.
After the supervisor heartbeats and the Operator becomes ready, the runner initializes resource intent and provisions every table before creating the FlinkDeployment.
Failure before that readiness handoff leaves no BigQuery resource intent.
If workload admission subsequently fails, the supervisor requests stop and waits for the original runner to return from `start()` and acknowledge release through `settle()`.
Only then does it inventory and remove the workload and run BigQuery cleanup; a missing release retains the control record and resources for incident recovery.
A failed or ambiguous provisioning call prevents workload admission and retains the existing unresolved-call cleanup guard.

The BigQuery exercise shares the smoke exercise's UID-scoped mutations and checkpoint/savepoint proof checks, using the BigQuery namespace, state bucket, input sizes, and the JobManager and the initial phase's TaskManagers as its topology.
Within 1200 seconds of the approved start, it requires input progress, a completed checkpoint and all of those Pods running before starting 180 seconds of warmup followed by a 600-second baseline window.
It then requires a checkpoint triggered after that window and fresh, unfinished input before applying the approved upgrade.
Each recovery has its own 300-second deadline, capped by the absolute cleanup start.
The upgrade must restore its new savepoint, preserve the input lineage, advance source progress and complete another checkpoint before a single JobManager deletion.
The replacement JobManager must restore the checkpointed job and advance input and checkpoint evidence again.
A fault trial changes one of the two as the table in [Offline execution proposal](#offline-execution-proposal) names: a rescale's restored job must also checkpoint more or fewer subtasks than before it, and a TaskManager replacement deletes the TaskManager that reported the latest input, keeps the JobManager and waits for the deleted Pod to go.

In FILE_LOADS mode the JobManager, or a fault trial's TaskManager, is deleted only once a listing shows one of the run's load jobs `RUNNING`, so that the failover is issued while a commit's load runs server-side ([#1685](https://github.com/flink-gcp/flink-connector-gcp/issues/1685)).
A commit takes 5 to 11 seconds of each 120-second checkpoint interval at the trial's sizes and a load 1 to 6 seconds, so a deletion left to the 15-second poll rarely lands inside one.
Once the upgrade is proved, the supervisor starts a listing of the run's [connector-issued jobs](#connector-issued-jobs) once a second (`in_flight_poll_seconds`), or as soon as the previous one returns when it took longer, and deletes the JobManager as soon as a load is `RUNNING`; a `PENDING` load, or a copy or query, does not count.
The wait blocks the supervisor loop for at most `in_flight_seconds`, 130 seconds or one checkpoint interval and a margin, which replaces the upgrade's recovery deadline.
Input keeps flowing meanwhile and must still last the 600-second post-recovery window: `bq1313-fl-10-a1`'s input completed 232 seconds after that window ended, and would have completed about 172 seconds after it had its upgrade waited a whole interval for the first checkpoint after the baseline, so a longer wait would put the run at risk of failing on input that ran out too early.
A stop is checked once a poll during the wait, a listing that fails in transport, including the control and lock reads it makes first, counts as a listing that saw nothing, and a run in which no load runs by the end fails without deleting the JobManager.
The loads the wait saw are written into the `failover_in_flight` outcome before the deletion, so the failover record holds them even when the recovery then fails.
Right after the delete request returns, the supervisor lists again: a load listed `RUNNING` on both sides was running when the deletion was issued, since a job's state only moves forward, and the intersection is recorded as `running_through`; an empty one records a failover issued just as its load finished, and a relisting that fails in transport is recorded as unknown rather than failing the run.
The record is also emitted as a `bigquery-failover-in-flight` event; the verdict does not read it.
It does not show that the load outlived the JobManager's exit or the committer's cancellation, which follow the delete request by a moment (0.8 seconds for the committer in `bq1313-fl-10-a1`): that is read afterwards from each load's end time and the TaskManager log.
The restored committer re-attaches to the commit's jobs about 40 seconds after the deletion, by when a 1 to 6 second load has finished, so the trial cannot show a re-attachment to a job still running.

After Flink reports `FINISHED` with complete input and both recovery proofs, the supervisor requests final queries through the existing runner handoff.
It recomputes the oracle from the archived rows and compares the archived report.
Missing rows may consume another approved query slot, with at most 600 seconds for the whole visibility phase and no deadline extension between requests.
In FILE_LOADS mode the supervisor first lists the run's [connector-issued jobs](#connector-issued-jobs), and refuses to read while one is not `DONE`: the committer commits synchronously, so a finished application has no job left by the connector's own account, and waiting for one would let rows that arrive after `FINISHED` land and pass.
It records the jobs it found as a `bigquery-unfinished-connector-jobs` evidence record and fails the run.
Invalid routing or EO and FILE_LOADS duplicates abort immediately; ALO duplicates remain reported observations.
Query errors, slot exhaustion, cancellation or deadline expiry cannot record recovery completion.
The `complete` recovery record includes both restore proofs and the passing query observation; common cleanup still requires runner release and the quiescence barrier before deleting tables.
The final receipt retains this recovery record, and its overall BigQuery `success` is the verdict the exercise decided when it completed — not the generic completion flag, which a trial must not be authorized by.

## Deployed verdict

A run is `usable` when the instrument worked: both recoveries proved, the query oracle passed, and each observation family read in the `baseline` window and again in the `finishing` window — the sink's task metrics, the network metrics, the connector's own gauges and the TaskManager memory.
A FILE_LOADS run must also have read two more families in both windows: the committer vertex's metrics, and a subtask's end-to-end, synchronous and asynchronous checkpoint durations for both the writer and the committer vertex.
The trial's mode decides which families are required, and the analyzer takes it from the receipt's trial.
Anything short of that is `inconclusive`, the same distinction the Cloud Tasks analyzer draws: a run that happened and cannot carry the claim.
The verdict carries its reasons, naming the family and the window it was missing from, so an inconclusive run says what it lacked.

Each window is required on its own, because a post-recovery figure with no baseline beside it is not the comparison the measurement exists to make, and a reading credited to the baseline that was actually taken mid-failover is not a steady state.
Coverage is recorded against the stage the sample was taken in, so the `baseline` window is the warm-up and steady stage together and the `finishing` window is the stage after the second recovery; the recovery stages hold their own coverage and satisfy neither.
Coverage within a window is monotonic: a family read in one baseline sample stays read when a later baseline sample loses it to a restart, because the question is whether that window observed it at all.
A window that recorded no sample observed nothing, whatever its coverage claims.
A family counts as observed only where a reading returned it. Discovery lists a metric id once and never drops it, so a request naming a gauge the operator no longer publishes still succeeds with that gauge simply absent from the answer, and crediting the request would report a name as a measurement.

It reports no measured value, and it must not. A throughput or a memory figure from this trial is a finding to publish, not a threshold to pass; #1312 forbids reusing component throughput as a production target, and a verdict that encoded one would be exactly that.
The verdict is decided and written with the transition that completes the run, so it reaches the `recovery-complete` evidence record and not only the final receipt.

`just tier3-analyze LOCAL_EVIDENCE_DIRECTORY` then recomputes it offline from the exported evidence rather than reading the receipt's answer, and reports each run's recomputed verdict, the verdict the run stored, its reasons, sample count and problems.
It rebuilds the coverage from the readings themselves. Each sample was exported as its own record beside the completed one, so the same fold the Pod ran is run again over those readings, and the verdict is decided over what they support rather than over the `coverage` the record claims.
A stored verdict, or stored reasons, other than the recomputed ones is reported as `verdict-differs-from-the-record`; the receipt's other claims decide nothing.
An evidence set missing the run's account of itself has no verdict, and neither has one holding a sample record the fold cannot read or place, one holding fewer samples in a window than the record counted there (`missing-samples`), or one whose record says the run completed with no completion evidence beside it: the lost record would lower the rebuilt coverage for what is only a truncated download, so fetch the evidence again.
A run that did not complete exported its account of itself, which says so, and is `inconclusive`.

The report also times each recovery the run proved, from its stage's start to Flink's restore and to the acknowledgement of the checkpoint that proved it, for findings rather than for the verdict; [ADR-0165](../../../docs/adr/0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md#recovery-timing-from-flinks-timestamps) records what each figure bounds.
A recovery whose proof or start the export does not hold readably is reported as unreadable.

The analysis does not classify a disagreement as an edited record or as an inconsistent export ([#1726](https://github.com/flink-gcp/flink-connector-gcp/issues/1726)).
The readings are files the same owner writes, so such a classification would raise the cost of a forged record without excluding one, and the owner who runs a trial is also the one who reads it.
The recomputation uses the rule set as it stands, so evidence re-analyzed after the required families or the sampled windows change can disagree with its own stored verdict for that reason alone; archived evidence is read with the revision that produced it.

Synthetic controller/service tests exercise both Storage Write modes at both destination counts and FILE_LOADS at 10, including a load job that outlives the Pods and one still running when the oracle would read, plus failed recovery proofs, early completion, delayed visibility, cancellation, budget exhaustion, provisioning failure and an unresolved quiescence barrier.
They establish the internal sequencing and refusal behavior, not actual Operator recovery, BigQuery visibility latency or a working writer fence.

## Internal actors

[`flink_tier3.bigquery.actors`](../../../tools/tier3/src/flink_tier3/bigquery/actors.py) constructs internal actors with a BigQuery HTTP session.
Its `runner` context manager takes the application dispatch rendered and approved; its `supervisor` context manager takes the manifests the entrypoint verified against the mounted approval.
Neither validates the approval or the installed source again: dispatch validated the one, and the dispatched commit is the other.
Both check the current environment lock and actor role before they return the actor.
The runner also checks admission, including its window and 600-second startup limit.
The handoff uses the approved resource plan, cleanup start as the query deadline and the fixed 10 MiB query-evidence budget.
The runner's caller supplies the original process token; the supervisor takes none and adopts it from the binding the runner writes.
Each caller keeps its context open through admission and settlement, or supervision and cleanup.
The supervisor Job replaces no Pod, so the cleanup window runs inside the context its one supervisor opened, and nothing refuses constructing a supervisor inside that window.
Exiting closes the HTTP session; it does not acknowledge runner release or initiate cleanup automatically.

The session is the authorized session the evidence Storage client and the image workflow already use, over `google-auth` application default credentials requesting the `cloud-platform` and `userinfo.email` scopes, unless the caller provides credentials.
It follows no redirect, ignores environment-derived proxies and does not refresh a token again after a 401.
The actors' copy also returns a redirect unread, which the adapter refuses as a non-success status: without that, requests reads a 3xx body whole before the adapter's 1 MiB cap applies.
Each request takes the resource adapter's remaining operation time as its timeout, and the adapter enforces the absolute operation deadline while consuming the response.
The two actors reach Google differently: the supervisor runs as `tier3-system/tier3-supervisor` bound to its Google service account, and the runner authenticates through workload identity federation in CI.
Constructing an actor sends no BigQuery request and does not check which principal the credentials name, although discovering the default credentials may ask the metadata server: each actor takes the credentials of the environment it runs in, the runner's workload identity federation in CI and the supervisor's Kubernetes service account, so a wrong identity needs a misconfigured environment, and IAM then refuses the first call that identity has no permission for ([#1726](https://github.com/flink-gcp/flink-connector-gcp/issues/1726)).

The factories do not authenticate the supplied Kubernetes/storage collaborators or the approval's origin, establish GitHub main ancestry or image provenance, or fence another creator/writer.
The supervisor factory builds the [quiescence barrier](#quiescence-barrier) from the run's own identity; constructing a session does not prove that barrier works.
Synthetic tests exercise approval binding, admission and lock loss, and that construction sends nothing.
The deployed trials ran both actors under the deployed WIF and GKE credentials with live IAM access, the quiescence barrier, the upgrade and failover recoveries, and cleanup; these synthetic tests establish none of that.
The overall BigQuery receipt reports success only for the [deployed verdict](#deployed-verdict)'s `usable`.
