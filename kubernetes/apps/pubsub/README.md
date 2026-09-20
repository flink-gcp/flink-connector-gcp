# Pub/Sub Tier-3 DataStream recovery application

This internal Flink 2.2.1 / Java 17 application prepares the DataStream part of [issue #1361](https://github.com/flink-gcp/flink-connector-gcp/issues/1361).
It relays two pre-created subscriptions through the production Pub/Sub source and publisher sink into a pre-created output topic.
It is an unbounded streaming job over a bounded logical input domain: receiving every expected ID does not stop the job, Pods or Operator.
An independently supervised lifecycle must bound execution and perform cleanup before deployment is admitted.

## Build and local validation

```sh
mise x -- just tier3-pubsub-verify
```

The opt-in `tier3-pubsub` Maven profile keeps the application outside published connector artifacts.
The build produces `target/pubsub-recovery.jar` and copies runtime dependencies with their original licenses/notices into `target/image-lib/`.
The Dockerfile adds these files to `/opt/flink/usrlib/` over the reviewed Flink image and enables its bundled GCS filesystem plugin.
The entry point is `io.github.flink.gcp.connector.tier3.pubsub.PubSubRecoveryJob`.
The [image publication workflow](../../../.github/workflows/tier3-images.yaml) packages it as `pubsub-recovery` on the fixed Flink 2.2.1 / Java 17 AMD64 base.
The application CI lane builds the Dockerfile without publishing; its context admits only the Dockerfile, application JAR and packaged runtime dependency JARs.
The [first publication](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/35494622116) built main commit `cba9043faee0ceb067cba23b56fe3e0abe7f0542`; a GAR read confirmed its application digest.
The reusable deployment definition below consumes a supplied digest; it does not select or admit an active run.

Unit tests cover argument and input bounds, serialization, missing/foreign restored state, contradictory evidence, missing IDs and duplicate classification.
MiniCluster tests use production source/sink RPCs against the Pub/Sub emulator, replace TaskManagers after a completed checkpoint and restore savepoints at parallelism 1→2 and 2→1.
They publish new input to both subscriptions after the disruption and require output from restored attempts.
These are local wiring and state-continuity checks; they do not measure real-service redelivery, ACK deadlines, lease expiry, ordering, GCS access, GKE scheduling or Operator reconciliation.

## Deployment definition

[`pkg/pubsub.#Application`](../../pkg/pubsub/application.cue) defines the DataStream relay on Flink 2.2.1 / Java 17.
A delivery below `runs/` supplies `run.id`, `run.expiresAt`, `run.namespace: "tier3-pubsub"` and the application's `id`, `image`, `phase`, `recordsPerSubscription` and `parallelism` inputs.
The [synthetic fixture](../../tests/fixtures/pubsub.cue) demonstrates that binding; tests supply disposable values and render every initial/upgrade and parallelism 1/2 combination.
There is no committed Pub/Sub run delivery or lifecycle CLI scenario yet.

The first verified image reference is:

```text
us-central1-docker.pkg.dev/flink-gcp/flink-tier3/pubsub-recovery@sha256:d56245c72fb69d3b1a68b05319cd4ecbdf74a3080995433ed9d5fe12d509e938
```

This records publication, not current availability.
GAR versions become deletion-eligible seven days after creation; admission must verify existence and retention through the run and cleanup window.
The package requires this GAR package with a lowercase SHA-256 digest and rejects tags and other application packages.

The deployment uses the `tier3-pubsub/pubsub` KSA and native workload ADC.
It retains checkpoints on cancellation and separates checkpoints, savepoints and Kubernetes HA data beneath `gs://flink-gcp-tier3-pubsub/runs/<run-id>/`.
Objects become deletion-eligible after one day, with soft delete and versioning disabled.
Complete recovery trials within that storage lifetime; the state bucket is not durable evidence storage.
One JobManager and one or two single-slot TaskManagers each request and limit 1 CPU, 2 GiB memory and 1 GiB ephemeral storage on AMD64 Spot nodes.
At parallelism two, the application alone therefore uses three Pods; later admission must additionally budget the Operator and independent supervisor.
Namespace quotas remain idle until a separately approved lifecycle admits the complete workload.

Savepoint upgrades preserve state and refuse unclaimed state.
The rendered job arguments use the application's `--name=value` syntax, match Flink parallelism and require restored identity in the `upgrade` phase.
That phase does not locate a savepoint or prove a checkpoint boundary: the later controller must retain and select recovery state, keep the run/input domain fixed and observe successful restoration.
The logical input domain and restart count do not bound elapsed execution or billable service operations.

Before deployment, implement the owned-resource provisioner and scoped service grants described below, independent stop/cleanup supervision, concrete execution limits and external fault/evidence collection.
Table entry points and deployed recovery acceptance remain on [#1361](https://github.com/flink-gcp/flink-connector-gcp/issues/1361).
The offline manifest check is `mise x -- just tier3-check`; it does not create resources or establish service settings.

## Run identity and service resources

| Argument | Contract |
| --- | --- |
| `--run-id` | Required Tier-3 label of at most 40 lowercase letters/digits/hyphens, beginning and ending with a letter or digit |
| `--records-per-subscription` | Logical sequence domain per input, 1–10,000; defaults to 1,000 |
| `--parallelism` | 1 or 2; defaults to 1 |
| `--phase` | `initial` by default, or `upgrade` |
| `--require-restored` | `false` by default; `upgrade` requires `true` |

Arguments use `--name=value`; unknown or repeated arguments fail.
The fixed project is `flink-gcp`.
Input index `i` is 0 or 1; its topic and subscription both have ID `t3-<run-id>-in-<i>`.
The output topic and independent observer subscription both have ID `t3-<run-id>-out`.
All three subscriptions must exist before any corresponding publication, including the output observer before starting the relay.
The job uses ADC, provides no key-file option, never creates a subscription and sets the sink to `CREATE_NEVER`.
The emulator endpoint is a package-private test seam, absent from the deployed CLI.

The external provisioner must reserve a fresh run ID, refuse existing same-name resources and retain an immutable manifest of the exact resource names, project, run ID and ownership nonce.
It must attach run/nonce ownership labels, verify topic bindings and service settings before admission, and refuse adoption or deletion when the recorded ownership is missing or inconsistent.
Pub/Sub resource names can be reused after deletion; names or label checks alone do not make deletion atomic with replacement.
The shared environment lock and exclusive control of run resources must cover provisioning through cleanup; unexpected replacement is an incident to reconcile, not permission to delete by prefix.
These provisioner and cleanup operations are requirements for the later lifecycle implementation, not implemented features of this application.

The workload requires input subscription metadata/consume access and output publication access; it needs no topic/subscription creation, deletion or IAM authority.
The later provisioner owns creation, topic attachment and scoped grant installation; the input publisher and output observer receive their own data permissions, and the independent cleanup identity receives only the required metadata/deletion operations.
Exact IAM policy installation and service-setting enforcement remain prerequisites before a real run; the existing Pub/Sub GSA currently has state-bucket access only.
That lifecycle must explicitly fix retention, acknowledgement deadline, ordering, exactly-once-delivery and expiry settings.
The connector rejects service-side exactly-once-delivery subscriptions; this relay makes only an at-least-once contract.

## Payload and restoration

Each UTF-8 input payload is `v1|<run-id>|<input-index>|<sequence>` with canonical decimal numbers and sequence in `[0, records-per-subscription)`.
The deserializer rejects foreign runs, wrong subscriptions, invalid UTF-8, payloads over 128 bytes and absent service message IDs.
It does not discard or deduplicate replayed input.
The logical count is not a cap on service deliveries, retries, bytes billed, output publications or elapsed time.
Those require separately approved execution ceilings and an independent stop path.

The source, observer and sink have fixed UIDs `pubsub-input-v1`, `pubsub-observer-v1` and `pubsub-output-v1`.
The main entry point fixes maximum parallelism at 128 and enables checkpoints every 30 seconds with at most one in flight.
The observer keeps version/project/run ID/input-domain identity in union operator state, accepting rescaling but refusing missing or incompatible restored identity.
Only phase, parallelism and the restoration requirement may change between phases of the same run.
The source's enumerator owns subscription reassignment; its readers checkpoint neither payloads nor split ownership, so unacknowledged delivery is recovered through the service.
The publisher flushes on checkpoints and remains at-least-once.

Each output payload contains nine pipe-delimited fields:
`v1|<run-id>|<input-index>|<sequence>|<base64url-input-message-id>|<attempt-uuid>|<observation-uuid>|<phase>|<restored>`.
Base64url encoding is UTF-8 without padding.
The attempt UUID is generated during each observer initialization; the observation UUID is new for every processing call, including repeated input.
Neither is a deterministic deduplication key for the logical input.
The app logs `pubsub-attempt`, `pubsub-snapshot` and `pubsub-checkpoint-complete` with run, phase and attempt identity; checkpoint events also name the checkpoint ID.
A snapshot is not completion, and the observer's completion callback does not prove that the Pub/Sub service has accepted an ACK.
Abrupt failure can lose logs, so the deployed lifecycle must export observations outside the task JVM and correlate them with Flink checkpoint history, faults and service ACK/lease observations.

## Offline output reconciliation

An independent output subscriber must record every observed output service message ID and its full payload before acknowledgement.
The offline tool reads one record per UTF-8 line: base64url output message ID, a tab, then base64url payload, both without padding.
Use the built application and copied dependencies from its directory:

```sh
java -cp 'target/pubsub-recovery.jar:target/image-lib/*' \
  io.github.flink.gcp.connector.tier3.pubsub.PubSubRecoveryReport \
  /tmp/pubsub-output.tsv --run-id=example --records-per-subscription=1000
```

The tool rejects files over 64 MiB, more than 200,000 observations, oversized records, conflicting identity reuse, foreign runs and missing logical input IDs.
Its counts describe the supplied evidence, not a complete service inventory or future deliveries after collection stops.

| Counter | Counted population |
| --- | --- |
| `logical_inputs` | Distinct input-index/sequence pairs; must equal twice the configured per-subscription count |
| `input_publication_duplicates` | Extra input service message IDs for the same logical input, scoped by input topic |
| `repeated_input_processing` | Extra processing observation UUIDs for the same input service message ID |
| `output_publication_duplicates` | Extra output service message IDs carrying the same processing observation UUID |
| `repeated_output_delivery` | Repeated observations of the same output service message ID |

The collector must preserve output message IDs: discarding them would conflate its own redelivery with duplicate sink publication.
Completeness alone does not establish recovery, exactly-once processing/output, strict replay ordering or a checkpoint-confirmed fault boundary.
The later trial controller must retain a completed checkpoint, publish a separately identified post-checkpoint cohort, prove no later checkpoint completed before the fault, and require that cohort to reappear with preserved input message IDs and new processing observations after restore.
It must also observe continued progress and later completed checkpoints, and retain both missing-ID and replay-population negative controls.
Table source/sink entry points, JM replacement, real-service replay and the complete deployment/cleanup workflow remain on the parent issue.
