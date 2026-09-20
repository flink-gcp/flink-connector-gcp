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
The package sets the namespace and the existing `bigquery` ServiceAccount; the run hierarchy supplies resource labels, expiry annotation and AMD64 Spot selectors on the common and manager-specific Pod templates.
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
A later executor must bind the exact rendered manifests to the approved trial, provision owned tables, admit the complete workload budget, collect observations, run the query oracle and clean all owned resources.

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
Its deployed execution, temporary-table deletion and ownership checks still belong to the subsequent lifecycle implementation.

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
The collector and its authorized submission/export path remain subsequent implementation.

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

The later executor must verify table ownership/schema and the completed query job, bind its exact SQL and all result pages to the approved trial, and retain query statistics and immutable evidence.
It must enforce query deadlines, maximum bytes billed and a cumulative retry/visibility budget before submitting anything, and run the final check after the workload has stopped writing.
The local tests execute the generated SQL unchanged on synthetic SQLite tables to exercise row, NULL, empty-table and duplicate semantics.
Their SQLite build must provide the `MOD` math function.
That coverage does not establish BigQuery query acceptance or streaming visibility; an authorized service run remains an acceptance gate.

The deployed exercise still needs approved manifests, updated image publication, numeric resource/cost limits, startup and recovery deadlines, a supervisor and complete cleanup.
The application itself does not scale the Operator, admit Pods, inject JobManager failure, submit queries or clean cloud resources.
The fixed dataset and namespace grants already exist, but checkpoint/savepoint restore through the conditional GCS grants remains a live acceptance gate.
