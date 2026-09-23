# BigQuery Tier-3 recovery application

This internal application supplies the finite workload for [issue #1312](https://github.com/flink-gcp/flink-connector-gcp/issues/1312).
It uses the production BigQuery Storage Write sinks on Flink 2.2.1 and Java 17.
The opt-in `tier3-bigquery` profile builds it separately from the published connector artifacts.

## Build and local validation

From the repository root:

```sh
mise x -- just tier3-bigquery-verify
```

The recipe builds `target/bigquery-recovery.jar` and copies runtime dependencies, with their original license and notice files, into `target/image-lib/`.
The Dockerfile places these JARs in `/opt/flink/usrlib/` over the reviewed Flink base image and enables its bundled GCS filesystem plugin.
The entry point is `io.github.flink.gcp.connector.tier3.bigquery.BigQueryRecoveryJob`; the job URI is `local:///opt/flink/usrlib/bigquery-recovery.jar`.
No dependency download is needed at Pod startup.
The [manual publication workflow](../../../.github/workflows/tier3-images.yaml) verifies and builds this payload as the `bigquery-recovery` GAR package.
The [first publication](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/35489876881) built commit `116f2b8d9f992ecca7282d6320468db2b4a5c196` before appender observations were added.
This changed payload needs another reviewed publication and digest before workload admission.
The application CI lane builds the Dockerfile against the same public Flink 2.2.1 digest without publishing or using GCP credentials.

Unit tests cover argument bounds, destination identities, serialized row sizes, Java serialization, input gaps and checkpoint state incompatibility.
Observation tests check original futures and exceptions, exact rows and offsets, failed opens/closes, reopening identities and log-output failures.
Sink tests serialize both observed sinks, construct their job graphs, create production writers, and round-trip a buffered writer state through restore and snapshot; the committer receives no appender observer.
Local MiniCluster tests restore the source and input identity operator from a checkpoint and a savepoint using a discard sink.
Separate local graph tests verify both writers reach every destination for both modes and destination counts.
Emulator tests create the application's production default-stream writer for both destination counts, write each destination sequentially, check the open/append/close observations, then query every table.
The application graph is tested separately: concurrent appends caused SQLite lock errors and RPC retries inside the pinned emulator in CI.
The pinned emulator assigns buffered offsets across streams and can hang on multi-stream flush, so it cannot exercise the production EO writer for this workload.
Existing connector tests cover that writer with deterministic service doubles; this application still needs real BigQuery EO validation.
The ALO emulator fixtures use one append of two rows per destination because follow-up appends are not reliably bound to their stream.
They establish local wiring and routing, not BigQuery exactly-once recovery, GCS checkpoint permissions or deployed Operator behavior.

## Trial inputs

| Argument | Contract |
| --- | --- |
| `--run-id` | Required Tier-3 run label, at most 40 characters; reuse only when restoring the same trial |
| `--mode` | Required `ALO` (default stream) or `EO` (buffered stream) |
| `--destinations` | Required `10` or `50` |
| `--records` | Finite count covering every destination on both writers (at least twice the destination count) and at most 2 GiB of serialized input; defaults to 30 minutes at 1 MiB/s |
| `--bytes-per-second` | Datagen offered rate in serialized row bytes, from 1 through 1,048,576; defaults to 1,048,576 |
| `--phase` | `initial` by default, or `upgrade` |
| `--require-restored` | `false` by default; `upgrade` requires `true` |

ALO rows are exactly 65,536 serialized protobuf bytes and EO rows exactly 1,024 bytes.
The default counts are 28,800 and 1,843,200 respectively: each offers 1,887,436,800 bytes before replay and RPC framing.
This is a logical input ceiling, not a billable-byte, retry, elapsed-time or query-cost ceiling.
Source pacing and serialization overhead must be observed on the execution host before interpreting throughput.

The source emits sequence numbers from zero through `records - 1` at parallelism one.
A checkpointed identity operator checks each next sequence before forwarding it.
The sink runs at parallelism two with maximum parallelism 128; deterministic partitioning by `(sequence / destinations) % 2` sends a complete destination cycle to each writer in turn.
Both writers reach every destination, while each row retains `sequence % destinations` as its table identity.
The job enables checkpoints every 30 seconds with at most one concurrent checkpoint.
Writer and buffered-stream options retain their production defaults; this workload does not search capacity or tune those defaults.

## Deployment definition

[`pkg/bigquery.#Application`](../../pkg/bigquery/application.cue) defines one finite trial as a FlinkDeployment for the pinned Operator 1.15.0 schema and Flink 2.2.1 application image.
A delivery supplies `run.id`, `run.image`, `run.mode` (`ALO` or `EO`) and `run.destinations` (`10` or `50`); `run.phase` defaults to `initial` and `run.records` defaults to the mode's 30-minute-equivalent count above.
Explicit record counts retain the application's minimum of twice the destination count and 2 GiB serialized-input ceiling.
The offered rate is fixed at 1 MiB/s.
The image must be a lowercase SHA-256 digest from the fixed `bigquery-recovery` GAR package; this checks its form, while publication provenance and registry retention still require verification before admission.

Use this package from a delivery under `runs/` with `run.namespace: "tier3-bigquery"` and the usual run ID, image and expiry inputs.
The package sets the namespace and the existing `bigquery` ServiceAccount; the run hierarchy supplies resource labels, expiry annotation and AMD64 selectors on the common and manager-specific Pod templates, with Spot on the TaskManager alone.
[`tests/fixtures/bigquery.cue`](../../tests/fixtures/bigquery.cue) shows the package composition used by the disposable render tests.
No concrete run, image digest or execution window is selected by this package.

The definition requests one JobManager and two TaskManagers, each with 1 CPU, 2 GiB memory and 1 GiB ephemeral storage; container requests equal limits.
The application total is 3 CPUs, 6 GiB memory and 3 GiB ephemeral storage, excluding the Operator and supervisor.
Each TaskManager has one slot, job parallelism is two and autoscaling is disabled; the Java graph retains its source parallelism of one and sink parallelism of two.
These resource choices follow the smoke Pod shape and still need approval in the complete trial budget; they do not establish sufficient heap or a runtime Pod ceiling.
No PVC or application volume is requested.

Checkpoints run every 30 seconds with at most one concurrent checkpoint, a 120-second timeout and two retained checkpoints.
The hashmap backend uses filesystem checkpoint storage, with checkpoints, savepoints and Kubernetes HA state below `gs://flink-gcp-tier3-bigquery/runs/<run-id>/` in separate `checkpoints`, `savepoints` and `ha` prefixes.
The fixed-delay restart strategy permits three attempts with a ten-second delay; the external supervisor must bound wall-clock runtime and repeated process incarnations.

For the upgrade, render the same inputs with `run.phase: "upgrade"` and update the same deployment.
Both phases use `upgradeMode: savepoint` and `allowNonRestoredState: false`; only the phase argument and `--require-restored` change, with the latter becoming `true` on upgrade.
The definition also sets `kubernetes.operator.job.upgrade.last-state-fallback.enabled=false` so an unavailable savepoint cannot be replaced by last-state recovery, and `kubernetes.operator.snapshot.resource.enabled=false` to use Operator 1.15.0's status-based savepoint reporting instead of separate FlinkStateSnapshot resources.
These settings follow the [generic recovery exercise](../../lifecycle/README.md#generic-recovery-exercise); an Operator upgrade must revisit this version-specific reporting choice.
The running application's checkpointed identity also rejects changed input parameters on restore.
The package describes that transition; it does not wait for baseline observations, prove a completed savepoint, apply the upgrade or delete the JobManager.

`just tier3-check` renders both phases for all four mode/destination combinations against the pinned schemas and checks input bounds, namespace policy, Pod resources, state paths and the phase-only argument change.
Those static tests use synthetic digests and do not establish image availability, server admission, task placement or GCS restore permissions.
The [production dispatch](#production-dispatch) path must bind the exact rendered manifests to the approved trial, provision owned tables, admit the complete workload budget, collect observations, run the query oracle and clean all owned resources.

## Offline execution proposal

`flink-tier3 render --scenario bigquery-recovery` combines one trial's proposed limits, initial/upgrade applications and supervisor delivery into a JSON bundle.
A proposal is not an approval: dispatch admits this scenario only through the [production dispatch](#production-dispatch) below, under its own version 4 approval contract.
The bundle contains `approved: false` and an empty `approval.json`; rendering never grants execution permission or calls a cloud API.
It may download pinned public CUE schema dependencies when the local cache is cold.

Name the trial with `--trial`: `alo-10`, `eo-10`, `alo-50` or `eo-50`, one delivery method at one destination count.
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
The proposal embeds the `ResourcePlan`, application/upgrade/supervisor hashes and installed runtime-source hash.
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
These values are proposals, not clocks enforced by a running executor.
The internal runner/supervisor loops now coordinate provisioning, recovery and final queries as described below.
Authenticated dispatch, creator/writer fencing, complete evidence accounting and live cleanup acceptance remain required before any paid dispatch.

## Table and row identity

Every destination is in the fixed `flink-gcp.flink_gcp_tier3_bigquery` dataset.
Its name is `bq_<run-id with hyphens replaced by underscores>_d<destination>`, with zero-based decimal destination numbers and no zero padding.
Run IDs do not allow underscores, so this conversion preserves distinct run identities.
The external runner must pre-create exactly those empty tables and verify their ownership and schema before admission.
The sink uses `CREATE_NEVER`; the workload creates no tables or datasets.

Rows contain nullable `run_id STRING`, `sequence INTEGER`, `destination INTEGER` and `payload BYTES` columns, with a value supplied for every field.
Padding is deterministic pseudorandom data derived from run ID and sequence, avoiding an all-zero fixture.
Replay serializes the same sequence into the same bytes and table.
The deployed query oracle must require every expected sequence in its correct table; EO additionally requires exact uniqueness, while ALO records duplicate multiplicities.
The [offline query oracle](#offline-query-oracle) below supplies the aggregate data check.
The resource adapter and controller below provide internal query, ownership and deletion operations.
The internal admission path connects those operations to deployment creation; authenticated production dispatch remains subsequent lifecycle work.

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
These commands do not provide the missing query submission/export step.
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
The internal resource adapter supplies this collector; its authorized CLI submission/export path remains subsequent implementation.

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
| `verdict` | `pass` only with no invalid or missing rows, and also no duplicate rows in EO mode |

ALO reports duplicate copies while permitting them; EO rejects them.
The report includes per-destination counts and totals, with `scope=query-result` to distinguish this check from a deployed recovery verdict.
It does not inspect payload bytes, prove table ownership, authenticate evidence, establish a checkpoint/fault boundary or prove that no later writes occur.
Identity fields in the aggregate bind the input parameters for accidental-mismatch detection; they are query literals, not a service attestation.
A substituted or edited aggregate can pass this offline check.
In particular, generating and assessing an EO trial with `--mode ALO` can permit duplicates when that input count also fits the ALO limit.
Rows contain no mode field, so this check cannot discover that mistake; the executor must take the mode from the approved application configuration, and the report must be interpreted with its recorded mode.

The [resource adapter](#resource-adapter) supplies table ownership/schema checks and binds a completed query job's SQL and all result pages to a trial.
The internal resource controller persists intent, query slots and query evidence as described below.
The executor must supply the approved cumulative retry/visibility budget and run the final check after the workload has stopped writing.
The local tests execute the generated SQL unchanged on synthetic SQLite tables to exercise row, NULL, empty-table and duplicate semantics.
Their SQLite build must provide the `MOD` math function.
That coverage does not establish BigQuery query acceptance or streaming visibility; an authorized service run remains an acceptance gate.

### Resource adapter

[`flink_tier3.bigquery_resources`](../../../tools/tier3/src/flink_tier3/bigquery_resources.py) provides internal REST v2 table and query operations for the BigQuery actors.
It has no CLI route and does not extend lifecycle admission.
Its synthetic HTTP tests cover request construction, lost responses, ownership conflicts, cleanup and result pagination; real-service acceptance remains pending.

The caller must persist one `ResourcePlan` before any write: the approved `Trial`, a fresh 32-character hexadecimal nonce, an absolute table expiration, the number of query slots, maximum bytes billed per slot and job timeout.
These values have no operational defaults or implied approval.
Reuse that intent unchanged across restarts, reserve each slot durably before submission and retain the table creation receipts and returned query evidence.
The plan permits only the trial's 10 or 50 table names in the fixed dataset and query slots numbered from zero through `query_slots - 1` in `us-central1`.
Slot count times maximum bytes billed is the planned query-byte ceiling when all actors use that unchanged plan and the same submitting identity; approval must also account for storage, writes, cluster resources and other costs.

Table creation sets the application's four nullable fields, explicit expiration within the next 24 hours, and labels binding run ID, nonce and the complete trial identity.
A matching existing table can reconcile a lost create response; a same-name table with different ownership, schema or expiration is refused.
This metadata check does not prove emptiness: admission still requires exclusive access, absence checks and a quiescent data check because streaming metadata can lag.
Deletion requires a matching receipt and a fresh read with the same creation timestamp.
The [documented `tables.delete` API](https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/tables/delete) exposes no generation precondition, so this read and delete are not atomic.
Before cleanup, the executor must stop and fence all creators/writers and hold exclusive lifecycle access through deletion; an external actor replacing a table between those calls remains outside that guarantee.
Missing ownership evidence refuses deletion, and expiration is only a fallback.

Each query slot has a deterministic job ID that does not change after a timeout or restart.
Submission checks an existing job or reconciles a create conflict against the exact generated SQL, identity labels, region, byte ceiling, timeout and disabled legacy SQL/query cache.
There are no automatic retries, fresh retry IDs or polling loops in the adapter.
A new visibility observation must consume another durably reserved slot; reusing a slot reads the same job's result.
The [REST job configuration](https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfiguration) bounds billed query bytes, but `jobTimeoutMs` is a best-effort service cancellation request.
The adapter checks the caller's absolute deadline before requests, after response headers and at streamed-body boundaries, caps each response at 1 MiB and bounds request timeouts by the remaining time.
An operation-specific adapter view shares the session, plan and clock while taking the earlier of the operation deadline and the original adapter deadline; it never mutates or extends the original deadline.
These checks do not establish a hard wall-clock stop or cancel an already submitted job.
The authenticated session described below also budgets credential HTTP exchanges and identity checks; it cannot forcibly interrupt synchronous credential code or a blocked socket read.
Cleanup can request cancellation only after validating that slot's job, and must poll its status separately until `DONE` before treating cancellation as complete.

Result collection requires a successful `DONE` job and billed-byte statistics within its limit.
It checks every page's job reference, completion, total row count and scalar schema, follows page tokens with a destination-count-plus-one page ceiling, and refuses repeated tokens, missing/duplicate destinations and aggregates over 64 KiB.
It flattens the returned `f`/`v` cells using the schema, runs the offline oracle, and returns the full job metadata/statistics, rows and `scope=query-result` report for the caller to persist.
It does not establish checkpoint provenance, table quiescence or a deployed recovery verdict.
Collect results as the submitting identity: [anonymous result tables are private to their creator](https://docs.cloud.google.com/bigquery/docs/cached-results#how_cached_results_are_stored), so the supervisor's project-level job get/update grants do not by themselves authorize reading the runner's result table.
The resource controller below supplies durable intent, query evidence and a cleanup pass.
The internal handoff and lifecycle loops supply shared deadlines and cleanup accounting; actor construction below supplies the BigQuery session, while production dispatch and live idle verification remain executor work.
Provisioning passes the earlier of the approved start plus 600 seconds and the query window's end to all of its REST operations.
Query submission, job status and every result page share the requested observation's fixed deadline.
Common cleanup passes its cleanup deadline separately, allowing cancellation and table deletion after the query window closes without reusing an expired query deadline.
An earlier original adapter deadline still wins, so each actor's adapter must cover its intended operation window.
Expiry rejects late responses, including a 404 or an empty successful body; a failed creating operation retains its unresolved handoff marker, while a returned read/collection failure can release its marker under the existing protocol.

### Durable resource controller

[`flink_tier3.bigquery_lifecycle`](../../../tools/tier3/src/flink_tier3/bigquery_lifecycle.py) composes the adapter with generation-checked lifecycle records.
The internal runner and supervisor use this controller only with explicitly supplied handoffs.
Neither production CLI entrypoint admits a BigQuery scenario yet.
The caller must authorize the resource plan, authenticate the actor, retain exclusive environment ownership and provide a resource adapter with the appropriate operation or cleanup deadline.
The controller binds the complete service plan, run ID, nonce and trial arguments to the approved initial application hash.
The common model validates the version 4 approval; complete deployment admission remains executor work.

Initialization stores the unchanged plan in `RunRecord.bigquery`.
Provisioning first checks absence, records a creation intent, then creates or reconciles that table and stores its creation receipt.
A table found before its creation intent is refused even when its labels match.
A receipt whose table disappeared or was replaced cannot authorize recreation.
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
That barrier must stop and join every creator and writer, settle server-side in-flight creation requests and exclude replacement throughout cleanup; merely stopping Flink Pods or setting the flag is insufficient.
The controller does not implement or independently verify this external barrier.
After it succeeds, cleanup requests cancellation of every possibly submitted slot and returns incomplete while any job is absent or not `DONE`.
An absent job after an uncertain submission remains unresolved rather than proving cleanup.
A slot recorded only as reserved has no submission intent and needs no cancellation.
Only after these checks does cleanup reconcile lost table receipts from persisted creation intents, delete owned tables and confirm absence.
It never enumerates or deletes unrecorded table names.
The supervisor may perform this cleanup without collecting the runner's private query results.
A completed pass marks only the BigQuery component clean; it does not set run success, remove Kubernetes/GCS state, release the environment lock or declare the environment idle.

Synthetic tests cover conditional-update conflicts, restarts, lost responses, stop races, evidence generations, pending queries and receipt-based cleanup, including composition with the REST adapter.
They do not establish authenticated handoff, a functioning external barrier or real-service acceptance.

### Query requests and runner release

[`flink_tier3.bigquery_handoff`](../../../tools/tier3/src/flink_tier3/bigquery_handoff.py) adds an internal protocol between the submitting runner and the observing supervisor.
The common runner settlement and supervisor cleanup paths accept this protocol through explicit constructor arguments.
The common model validates BigQuery approval inputs, and the [production dispatch](#production-dispatch) constructs these actor bindings through the authenticated actor factories.
The caller must authenticate both actors, allocate a query evidence budget and deadline, and give exactly one submitting process a fixed runner token.
The supervisor is bound without it: its Pod starts before the runner writes the binding, so it binds the fields its own approval fixes and adopts the token from the first binding it reads, refusing a replacement thereafter.
The token binds records; it is not an authentication credential or a lease that another process may take over.
After initializing this protocol on an empty resource intent, all runner provisioning and query calls must use it rather than the resource controller directly.

The supervisor persists one observation request at a time with an immutable name and deadline.
The runner reserves a deterministic query slot, submits and polls that job, and collects results with its own identity.
Each new observation consumes another slot; repeated polls do not submit a completed or pending job again.
The supervisor reads the resulting evidence object and checks its recorded generation, hash, intent, observation name and slot without calling the private query-result API.
Query evidence bytes are divided equally among the reserved slots and checked on the complete serialized artifact before writing and when reading it.
This bounds those query artifacts, not telemetry or the whole run's evidence; the executor must allocate the other evidence separately.
Request deadlines must fit inside the shared query window and table expiry.
An expired uncollected request ends observation processing for the trial; it cannot be abandoned to skip a required baseline or reuse its slot.
The caller must stop, release and clean up rather than retry a later observation.
The protocol checks deadlines before admitting each query operation; adapter request deadlines remain the caller's responsibility, and neither check is a hard service-side stop.

A conditional record update marks each runner call in flight before it starts.
Stopping sets the run-global stop flag as well as the BigQuery component flag; release also stops admission for the whole run.
This closes new requests and calls but does not acknowledge runner release.
Release is permanent and requires no in-flight or unresolved call; cleanup additionally requires the resource controller's external quiescence barrier and terminal query checks.
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
They do not establish authenticated handoff, workload fencing, real-service acceptance or a deployed recovery verdict.

### Common lifecycle loop integration

An admitting caller passes its process-local handoff to `Runner(env, bigquery=handoff)`.
`Runner.start()` initializes and provisions it after supervisor and Operator readiness, before admitting the application.
Only after admission has returned may that runner enter `settle()`.
While waiting for the supervisor, settlement services pending query requests until stop, evidence failure, the execution deadline or supervisor termination.
A query failure stops further polling and prevents settlement from preserving a successful trial verdict.
The runner attempts permanent release before leaving the wait, including on timeout; an unresolved call prevents release and leaves cleanup incomplete.
A recovery process must not reconstruct the original submitting process's token to manufacture this acknowledgement.
Repeated release attempts can recover a transient control failure; settlement emits a blocked-release diagnostic only when its cause changes.

The supervisor receives its own environment-bound handoff and a mandatory external `quiesce` callback.
Supplying the callback without a handoff is rejected at construction.
Its `query(name, deadline=...)` method requests an observation and waits for archived evidence, maintaining heartbeats and auditing resources until the deadline or cancellation.
It returns the verified result without interpreting an incomplete baseline as a failed trial or treating a query report as a deployed recovery verdict.
The internal recovery exercise below chooses final-query boundaries and row acceptance criteria, and decides the deployed measurement verdict from them together with the observation coverage.

Common cleanup closes run admission and removes the owned Kubernetes workload before waiting for runner release.
For failed admission with recorded BigQuery intent, the supervisor first waits for runner release before entering common cleanup, so admission can finish recording any confirmed application creation.
Release alone does not resolve an unknown creation outcome: settlement still has to reconcile the persisted application intent, and service deletion still requires the external creator/writer barrier.
Failure to persist the stop request marks evidence incomplete but does not prevent workload teardown.
The cleanup-phase transition is attempted independently of that stop write.
If no BigQuery intent was ever recorded, cleanup skips the attached handoff and continues ordinary settlement.
Once released, it waits for the external quiescence callback to return exactly `true`, then invokes handoff cleanup, which rechecks that same barrier before any service deletion.
It waits for terminal queries and table deletion within its cleanup window; a barrier that fails its recheck aborts that pass.
Observed Pod disappearance is not a substitute for that callback's creator/writer and in-flight service guarantees described above.

## Quiescence barrier

[`flink_tier3.bigquery_quiesce`](../../../tools/tier3/src/flink_tier3/bigquery_quiesce.py) builds that callback from the run's own identity, inside the authenticated supervisor factory, so no caller supplies one: a `callable` check cannot tell a proof from a constant, and `lambda: True` satisfied every check the factory previously made.

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
A conflicting receipt or a concurrent control change leaves the control record and lock in place.
These tests use synthetic actors and services; authenticated admission, the external quiescence implementation, image publication and live acceptance remain subsequent work for issue #1312.
The internal recovery schedule is described below.

The deployed exercise still needs authenticated admission, updated image publication, measurement collection and complete cleanup acceptance.
The application itself does not scale the Operator, admit Pods, inject JobManager failure, submit queries or clean cloud resources.
The fixed dataset and namespace grants already exist, but checkpoint/savepoint restore through the conditional GCS grants remains a live acceptance gate.

### Approval and shared resource policy

The common model accepts a version 4 `bigquery-recovery` approval, which [production dispatch](#production-dispatch) builds and the internal integration and cleanup tests construct directly.
It binds `bigquery_trial` to the trial's delivery method and destination count, the only fields a trial chooses.
The approval also pins the initial and upgrade manifest hashes, source hash, runtime image digests, run/lock identity, and the observed namespace, Operator and idle-quota identities.
A valid document is not authenticated execution permission on its own: dispatch still requires the typed phrase, the environment lock and a verified bundle, and the in-cluster entrypoint builds its supervisor only through the authenticated actor factory.
Internal runner admission requires an explicit environment-bound handoff; internal supervision additionally requires the approved upgrade and a quiescence callback.

The approval requires a 90-minute window with the final 15 minutes reserved for cleanup, at most six Pods and no PVCs.
The application quota covers one JobManager and two TaskManagers, each using the existing 1 CPU, 2 GiB memory and 1 GiB ephemeral-storage shape.
The control quota covers the Operator, supervisor and one Operator replacement while the previous Pod terminates; five Pods run in the steady state.
State and log limits remain 1 GiB of state, 10,000 state objects and 100 MiB of logs; the approval carries no cost ceiling, because spend is approved from the estimate before dispatch.
The two trial modes retain their fixed finite input sizes; the approval model derives the resource plan rather than accepting another independently editable copy.
The resource controller refuses a plan that differs from this approval before reading or creating service resources.

Common creation/adoption uses the approved application namespace.
Inventory includes `tier3-bigquery` for BigQuery approvals, counts its Pods against the six-Pod ceiling, and verifies owner UIDs, image digests and effective resources.
Recovery of version 1–3 approvals keeps the namespace scope of their saved baseline.
State cleanup lists and generation-deletes only `runs/<run-id>/` in `flink-gcp-tier3-bigquery`; same-named objects in the smoke or evidence buckets and other BigQuery run prefixes remain outside that operation.
Synthetic tests cover these boundaries and compose the real approval, environment, handoff and common cleanup with fake Kubernetes/storage/BigQuery services.
They do not establish a functioning external quiescence barrier or live service acceptance.

Authenticated approval delivery, measurement collection, complete evidence accounting and writer fencing must be connected before enabling either execution entrypoint.
The internal loops require a fixed 10 MiB allocation for query artifacts, reducing supervisor receipts to 80 MiB and runner receipts to 8 MiB for version 4 approvals.
The four allowances are now entries under `[bigquery_ceilings]` rather than literals spread across modules, and they sum to the `evidence_bytes` they divide: 80 MiB of supervisor receipts, 8 MiB of runner receipts, 10 MiB of query artifacts and 2 MiB for the immutable run documents.
Those documents — the approval, the application and upgrade manifests, the image receipts, the session file and the final result — are weighed against that last allowance before each write, where previously they were written unweighed and nothing proved the run stayed inside what it was approved to retain.
Only the documents directly under the run prefix count against it; the per-actor receipts and the query artifacts live in subdirectories and answer to their own entries.
The count comes from a fixed roster of those document names rather than from listing the prefix: the prefix also holds every receipt, whose object count reaches the listing's twenty-thousand ceiling long before its byte ceiling, and this accounting runs when the final result is written, so a listing here would fail a run at its last step.

The collected queries' billed bytes are summed into the retained control state as `billed_bytes`.
Each query was already refused above its own `maximum_bytes_billed`, and the slot count bounds the worst case, but nothing added them up, so a finished trial could not state what it had actually spent.
A re-collected observation returns on the evidence pointer it already holds and is not counted again, and the total is restated from each query's recorded figure rather than accumulated into, because a conflicting write re-applies the same edit against a record that already carries it.

The shared final receipt records the BigQuery scenario and approved trial, with `success` taken from the execution verdict under *Deployed verdict* below.
A generic recovery completion flag cannot satisfy that verdict.
A BigQuery finalization retry compares all receipt fields except the refreshed plans' observation time (`plans.at`); it retains the original receipt and still requires the current plans to be empty for the approved nonce and all three roots.

## Approval-bound delivery bundle

`flink-tier3 bigquery-bundle` prepares the delivery from a separately supplied version 4 approval and verifies it by re-rendering against that approval.
The checkout HEAD must match the approval SHA, with no tracked Kubernetes input changes or untracked CUE inputs, including ignored files.
Repository checks ignore ambient `GIT_*` overrides and bound each Git invocation to 60 seconds.
The installed package runtime hash, both application hashes and the supervisor image must also match the approval.
It embeds the approval in the immutable ConfigMap and reduces the supervisor Job's relative deadline to the time remaining at the specified preparation instant.
All other rendered delivery fields are retained, and the complete ConfigMap data must fit Kubernetes' 1 MiB limit.
The original proposal stays visibly unapproved; the enclosing bundle reports `admission_enabled: false`, because the bundle alone authorizes nothing — dispatch admits it only under the typed phrase and the environment lock.

Run from the repository root with the managed CUE and Python tools:

```sh
mise x cue uv -- uv run --locked --package flink-tier3 --no-dev flink-tier3 bigquery-bundle prepare \
  --approval-file approval.json \
  --prepared-at 2026-09-21T00:10:00Z > bundle.json
mise x cue uv -- uv run --locked --package flink-tier3 --no-dev flink-tier3 bigquery-bundle verify \
  --approval-file approval.json --bundle-file bundle.json
```

The preparation instant must be a whole second between the approved start and cleanup deadline.
An installed CLI can select the checkout with `flink-tier3 --repository /path/to/checkout bigquery-bundle ...`; CUE must be on its executable path.
Verification compares the complete bundle, including its embedded source, command, images, deadlines and trial metadata; it requires the external approval file even when the bundle already contains an approval.
Both input files reject duplicate JSON object fields.
The commands perform local generation and comparison, including CUE's pinned public dependency downloads when needed, and make no cluster or GCP service calls.
A matching file does not authenticate its approval or authorize execution.
The future executor must independently authenticate the approval, verify GitHub main ancestry and image publication/provenance/retention, prove current lock/namespace ownership, prepare for the actual admission time, and enforce absolute deadlines, actor fencing and aggregate evidence budgets.
A previously prepared relative Job deadline is not permission to start that Job later.
The [production dispatch](#production-dispatch) is that executor, for the checks it can make; image publication and retention remain a live check at dispatch.

## Production dispatch

The [run workflow](../../../.github/workflows/tier3-run.yaml) admits `bigquery-recovery` with four inputs beyond the common ones.
The campaign, its estimate, stop conditions and cleanup checks are preregistered in the [BigQuery trial preregistration](../../../docs/adr/evidence/0165-bigquery-trial-preregistration-1312.md).

| Input | Contract |
| --- | --- |
| `trial` | `alo-10`, `eo-10`, `alo-50` or `eo-50`; the default `none` is refused |
| `application_digest` | The published `bigquery-recovery` GAR digest, verified live at dispatch and never pinned |
| `expires_at` | 90 to 100 minutes ahead; the latest the run may end |
| `approval` | `APPROVE ONE BIGQUERY TRIAL: 6 PODS, 90 MINUTES`; it confirms the run, and spend is approved beforehand from the estimate |

The window starts when dispatch admits the run, on the whole second, and lasts exactly 90 minutes, so queueing before the job starts costs the run none of its startup budget.
The typed expiry bounds it: dispatch refuses an expiry earlier than the window's end, or more than ten minutes after it.

The checks that need neither the cluster nor the lock run first: the trial and digest, the phrase, the exact main commit, the run ID, an existing run's evidence and the window.
Dispatch then snapshots the foundation, renders and verifies the proposal, takes live image receipts, builds and validates the version 4 approval and prepares the bundle with the approval embedded, so a bundle refusal also arrives before the lock.
It then acquires the environment lock, checks the foundation again, writes the run documents and runs the authenticated runner inside its session, minting the runner token in this process.
If the authenticated session cannot be built after the lock is taken, dispatch settles the run through the plain runner and reports the environment idle before it fails, so the workflow's finalization still proves the plans empty, writes an unsuccessful receipt and releases the lock.

The in-cluster entrypoint verifies the mounted approval, application and upgrade against their pins, refusing a missing document by name, then builds the authenticated supervisor with its quiescence barrier and exercise and holds that session open for the whole supervision.
It is constructed without the runner token and adopts it from the binding, as [Query requests and runner release](#query-requests-and-runner-release) describes.

The workflow job runs for up to 120 minutes for this scenario, because the 90-minute window's serial budget is 114 minutes.
The published application image predates the appender observations; enabling the scenario does not approve a run.

## Internal recovery execution

The common `Runner.start()` and `Supervisor.supervise()` paths now support an explicitly attached BigQuery handoff.
They are internal integration points: the caller must verify the approval-bound bundle, authenticate each actor, retain exclusive environment ownership and provide the external writer/creator quiescence barrier and appropriate HTTP operation deadlines.
The production dispatch and in-cluster entrypoints reach these paths only through the actor factories, and a bare `Runner` or `Supervisor` without its handoff still refuses; passing these tests does not approve a paid run.
A replacement runner must not reconstruct the submitting process's token.

Admission validates the handoff's environment, approved resource plan, query-evidence allocation and absolute query deadline before any mutation.
The deadline equals the approved cleanup start, and admission must complete within 600 seconds of the approved start.
After the supervisor heartbeats and the Operator becomes ready, the runner initializes resource intent and provisions every table before creating the FlinkDeployment.
Failure before that readiness handoff leaves no BigQuery resource intent.
If workload admission subsequently fails, the supervisor requests stop and waits for the original runner to return from `start()` and acknowledge release through `settle()`.
Only then does it inventory and remove the workload and run BigQuery cleanup; a missing release retains the control record and resources for incident recovery.
A failed or ambiguous provisioning call prevents workload admission and retains the existing unresolved-call cleanup guard.

The BigQuery exercise shares the smoke exercise's UID-scoped mutations and checkpoint/savepoint proof checks, using the BigQuery namespace, state bucket, input sizes and three-Pod topology.
Within the initial 600-second deadline, it requires input progress, a completed checkpoint and all three running Pods before starting 180 seconds of warmup followed by a 600-second baseline window.
It then requires a checkpoint triggered after that window and fresh, unfinished input before applying the approved upgrade.
Each recovery has its own 300-second deadline, capped by the absolute cleanup start.
The upgrade must restore its new savepoint, preserve the input lineage, advance source progress and complete another checkpoint before a single JobManager deletion.
The replacement JobManager must restore the checkpointed job and advance input and checkpoint evidence again.
Input completion must be observed at least 600 seconds after that second recovery proof.
Inside those windows the sink is sampled through the job's own REST service, at most once per `measure_seconds` rather than once per poll, and the samples are emitted as `bigquery-measurement` records carrying the stage they were taken in. One sample is six REST reads, and at the transport's timeout that would let a slow endpoint spend two minutes of a window that is three; a memory or GC trend does not need a reading every fifteen seconds to be readable.
What is read: the sink and source vertices' task metrics — throughput, backpressure, busy and idle time, and the buffer-pool and byte-rate metrics that say whether the network was the limit; every TaskManager's memory, including non-heap, direct, mapped and Flink managed memory rather than heap alone, because this sink appends through native buffers; and the connector's own gauges, which are the active-writer observation — open destinations, in-flight appends and batches, append retries, destination activations and commit durations.
A connector metric's id carries its operator name, so the ids are discovered and then asked for by id; the discovered set is unioned across samples rather than frozen on the first answer, because a task registers its own metrics when it deploys and the sink's operators theirs when they open — freezing a listing taken between those two moments would silently drop the active-writer gauges for the rest of the job's life.
The source vertex is identified by its name rather than by its position in the plan, and a plan that does not hold exactly one of each is reported unavailable rather than guessed at.
No sample is taken on a poll where the loop resolved no Service: the job is between states, and a reading taken through the last one would be attributed to a job that is not the one running.
An absent reading is recorded as `unavailable` with its cause rather than dropped: a missing sample and a zero reading mean opposite things when the question is whether the sink was the limit.
Checkpoint duration and state come from the checkpoints the recovery loop already reads.
What the verdict below reads from these is whether each was obtained, never what it read.

After Flink reports `FINISHED` with complete input and both recovery proofs, the supervisor requests final queries through the existing runner handoff.
It recomputes the oracle from the archived rows and compares the archived report.
Missing rows may consume another approved query slot, with at most 600 seconds for the whole visibility phase and no deadline extension between requests.
Invalid routing or EO duplicates abort immediately; ALO duplicates remain reported observations.
Query errors, slot exhaustion, cancellation or deadline expiry cannot record recovery completion.
The `complete` recovery record includes both restore proofs and the passing query observation; common cleanup still requires runner release and the external barrier before deleting tables.
The final receipt retains this recovery record, and its overall BigQuery `success` is the verdict the exercise decided when it completed — not the generic completion flag, which a trial must not be authorized by.

## Deployed verdict

A run is `usable` when the instrument worked: both recoveries proved, the query oracle passed, and each observation family read in the `baseline` window and again in the `finishing` window — the sink's task metrics, the network metrics, the connector's own gauges and the TaskManager memory.
Anything short of that is `inconclusive`, the same distinction the Cloud Tasks analyzer draws: a run that happened and cannot carry the claim.
The verdict carries its reasons, naming the family and the window it was missing from, so an inconclusive run says what it lacked.

Each window is required on its own, because a post-recovery figure with no baseline beside it is not the comparison the measurement exists to make, and a reading credited to the baseline that was actually taken mid-failover is not a steady state.
Coverage is recorded against the stage the sample was taken in, so the `baseline` window is the warm-up and steady stage together and the `finishing` window is the stage after the second recovery; the recovery stages hold their own coverage and satisfy neither.
Coverage within a window is monotonic: a family read in one baseline sample stays read when a later baseline sample loses it to a restart, because the question is whether that window observed it at all.
A window that recorded no sample observed nothing, whatever its coverage claims.
A family counts as observed only where a reading returned it. Discovery lists a metric id once and never drops it, so a request naming a gauge the operator no longer publishes still succeeds with that gauge simply absent from the answer, and crediting the request would report a name as a measurement.

It reports no measured value, and it must not. A throughput or a memory figure from this trial is a finding to publish, not a threshold to pass; #1312 forbids reusing component throughput as a production target, and a verdict that encoded one would be exactly that.
The verdict is decided and written with the transition that completes the run, so it reaches the `recovery-complete` evidence record and not only the final receipt.

`just tier3-analyze LOCAL_EVIDENCE_DIRECTORY` then recomputes it offline from the exported evidence rather than reading the receipt's answer, and reports each run's status, verdict, reasons, sample count and problems.
It rebuilds the coverage from the readings themselves. Each sample was exported as its own record beside the completed one, so the same fold the Pod ran is run again over those readings, and the verdict is decided over what they support rather than over the `coverage` the record claims — recomputing from a field the same hand could edit would only restate the record to itself.
Only a claim the evidence denies is `tampered`: a record claiming an attempt or a family its readings do not support, a verdict of `usable` they do not support, or a receipt claiming `success`, which the runner computes from a usable verdict and nothing else. The claim can be made in either document, so both are read. Every other disagreement is `inconsistent` — a receipt at odds with the evidence beside it, a completed record accounting for fewer readings than the run emitted, a stored verdict more conservative than the readings warrant, or two completion records. Under-reporting is not forgery.
An evidence set missing the run's account of itself is `unexported`, and so is one holding a sample record the fold cannot read or place, or one whose record says the run completed with no completion evidence beside it — an absent object is named only by the record it should have accompanied.
That last shape is also what a wholly fabricated receipt looks like, and the two are told apart only by the readings: a missing object on its own is `unexported` and re-fetchable, while the same record or receipt claiming coverage, a verdict or a success the readings do not support stays `tampered`. Where the readings are gone too, nothing distinguishes a truncated export from a fabrication, and the severe label is reported; fetch the evidence again and ask once more. That is a truncated download, not a dishonest run: the lost reading lowers the rebuilt coverage, so continuing would report the record as claiming more than its readings support. Fetch the evidence again.

What this does not do is make the evidence unforgeable. The readings are files like any other, so a forger who fabricates one per window alongside the edited record still passes; what the rebuild removes is the single-field edit, and what it costs is one file per sample instead.
Those are the excluded statuses the Cloud Tasks analysis uses and none of them is a measurement — but a run that simply did not complete is not one of them: it exported its account of itself, and that account is `inconclusive`.
The receipt's `success` is checked in one direction only. It is a conjunction the runner may refuse for reasons of its own, so `false` beside a usable verdict is the runner obeying its rules; `true` beside a verdict that is not usable is the forgery.
The recomputation uses the rule set as it stands, so evidence re-analyzed after the required families or the sampled windows change can disagree with its own stored verdict for that reason alone; archived evidence is read with the revision that produced it.

Synthetic controller/service tests exercise both delivery modes and both destination counts, plus failed recovery proofs, early completion, delayed visibility, cancellation, budget exhaustion, provisioning failure and an unresolved external barrier.
They establish the internal sequencing and refusal behavior, not actual Operator recovery, BigQuery visibility latency or a working writer fence.

## Authenticated internal actors

[`flink_tier3.bigquery_actors`](../../../tools/tier3/src/flink_tier3/bigquery_actors.py) constructs internal actors with a role-specific BigQuery HTTP session.
Its `runner` context manager compares the complete bundle with the independently supplied environment approval, including local source, Git revision and CUE rendering checks.
Its `supervisor` context manager instead binds the initial and upgrade manifests and installed source to that environment approval; it does not run Git or CUE inside the runtime image.
Both validate the approval, current environment lock and actor role before Google authentication, then recheck ownership before returning the actor.
Each checks the source pin it can reproduce: the runner compares `runtime_sha256` against its complete installation, and the supervisor compares `delivery_sha256` against the mounted subset it actually runs, which is all it has.
The runner also checks admission before and after authentication, including its 600-second startup limit.
The handoff uses the approved resource plan, cleanup start as the query deadline and the fixed 10 MiB query-evidence budget.
The runner's caller supplies the original process token; the supervisor takes none and adopts it from the binding the runner writes.
Each caller keeps its context open through admission and settlement, or supervision and cleanup.
Because both factories validate the approval against the current clock, neither actor can be constructed once admission closes at the cleanup start, so the cleanup window runs inside a context opened before it.
Replacing a supervisor inside that window is therefore not a recovery path, and the production entrypoints do not make it one.
Exiting closes the HTTP session; it does not acknowledge runner release or initiate cleanup automatically.

[`BigQuerySession`](../../../tools/tier3/src/flink_tier3/bigquery_auth.py) uses `google-auth` application default credentials, requesting `cloud-platform` and `userinfo.email` scopes, unless the caller provides credentials with suitable scopes.
Before the first BigQuery request with each distinct bearer header, it calls Google's [OAuth2 userinfo endpoint](https://developers.google.com/resources/api-libraries/documentation/oauth2/v2/python/latest/oauth2_v2.userinfo.html) using that same header.
It requires a verified email equal to `tier3-runner@flink-gcp.iam.gserviceaccount.com` or `tier3-supervisor@flink-gcp.iam.gserviceaccount.com`, according to the actor role.
It does not infer the authenticated principal from credential configuration or email metadata.
The identity response is capped at 16 KiB; malformed, unverified, mismatched and unsuccessful responses refuse the actor or resource request.

Credential HTTP requests made through the supplied auth adapter, identity checks and the resource request share one monotonic budget capped at 20 seconds and the caller's remaining operation time.
Each subsequent request receives the remaining budget; a shorter credential-supplied timeout is preserved.
Nonblocking credential refresh is rejected, redirects and environment-derived proxies are disabled, and BigQuery/identity requests are not retried or replayed on 401.
Credential-library retries may still occur within that library, but each HTTP attempt through the adapter must fit the remaining budget.
The adapter covers every token refresh and the in-cluster metadata credential; it does not cover credential discovery that builds its own transport.
The pinned `google-auth` forwards the caller's request object only to its Compute Engine checker, so an external-account credential file resolves its project with that library's own timeout, proxy handling and redirect policy rather than this session's.
The two actors reach Google differently: the supervisor runs as `tier3-system/tier3-supervisor` bound to its Google service account, and the runner authenticates through workload identity federation in CI, which is the external-account form.
Measured against the pinned library, discovery issues no request when the environment supplies a project and enters the credential exchange on the library's own transport when it does not; that part of construction is then bounded by the caller's deadline check rather than by this budget.
These checks do not forcibly interrupt credential discovery, synchronous credential code, blocked reads or server-side execution.
The resource adapter still enforces the absolute operation deadline while consuming BigQuery responses.

The factories do not authenticate the supplied Kubernetes/storage collaborators or the approval's origin, establish GitHub main ancestry or image provenance, fence another creator/writer, or implement the immutable artifact allowance.
The supervisor still requires an explicit external quiescence callback; constructing a session does not prove that callback works.
Synthetic tests exercise the pinned Google auth request adapter, token refresh and rotation, identity refusal, shared time budgets, approval binding and lock loss.
Acceptance of the deployed WIF/GKE credentials by userinfo remains unmeasured, as do live IAM access, fencing and full recovery/cleanup.
The overall BigQuery receipt reports success only for the [deployed verdict](#deployed-verdict)'s `usable`.
