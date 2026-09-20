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

Before deployment, integrate the owned-resource operations described below with scoped service grants, independent stop/cleanup supervision, concrete execution limits and external fault/evidence collection.
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

### Owned resource operations

[`flink_tier3.pubsub`](../../../tools/tier3/src/flink_tier3/pubsub.py) supplies internal `ResourcePlan` and `Resources` helpers for the later lifecycle controller.
These helpers are not a CLI scenario and do not admit a workload.
A plan requires a fresh run ID and a caller-generated ownership nonce of 32 lowercase hexadecimal digits.
The caller supplies the shared authorized HTTP session, GCS `Storage` adapter and a mandatory `before_operation(phase, method, name)` guard; construction performs no authentication or I/O.
Use the shared evidence bucket for the control record, as with the other lifecycle records.

`provision()` refuses an existing control record or any of the six existing service names, including resources with matching labels.
It then writes the version 2 plan, including the fixed data-role bindings described below, to `_control/pubsub/<run-id>.json` with GCS generation-match zero before creating the three topics followed by the three subscriptions.
Every service resource carries `tier3-run` and `tier3-nonce` labels.
The helper re-reads the matching manifest before every service write and validates all six service resources after creation; `inspect()` repeats that readback validation.
An ambiguous create stops without retrying or adopting the resource; the retained manifest permits cleanup of matching partial creation.
Re-running `provision()` against that manifest is refused.
The retained intent lives outside `_control/runs/`, whose contents block new shared environment locks.
The later lifecycle must separately maintain its active run control record before provisioning until owned cleanup and final evidence complete; the Pub/Sub intent is not that active-run record.

| Service setting | Required value |
| --- | --- |
| Topic persistence region | `us-central1`, without in-transit region enforcement |
| Topic retention, schema, KMS key, ingestion or message transforms | Unset |
| Subscription delivery | Pull, with no filter, dead-letter policy, retry policy or transforms |
| Acknowledgement deadline | 30 seconds |
| Unacknowledged message retention | 1 day |
| Subscription inactivity expiration | 1 day |
| Retain acknowledged messages, ordering, exactly-once delivery | Disabled |

The service may omit false scalar fields and return integral durations with fractional zeros; readback accepts those representations.
The [subscription API](https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions) defines these settings; an unset retry policy uses immediate redelivery, and inactivity expiration is not a run deadline.
Subscriptions can expire after one day without subscriber activity; provision within the approved admission window and revalidate presence and settings immediately before admitting the workload.
The [topic API](https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/projects.topics) defines the persistence policy and optional topic features.
The relay makes an at-least-once contract; the connector rejects service-side exactly-once-delivery subscriptions.

`cleanup()` requires the matching manifest, verifies resource labels and subscription topic bindings, and deletes subscriptions before topics.
For cleanup only, it also accepts the service's `_deleted-topic_` marker on an owned subscription, so a topic deleted earlier does not prevent removing that subscription; a different live topic binding still refuses deletion.
Inspection requires the expected live binding and identifies absent resources by name.
It confirms absence after each deletion and retains the control record for repeated cleanup and reconciliation.
Settings drift does not erase ownership, but missing or inconsistent recorded identity refuses deletion.
Failures stop that call, including stopping before topic deletion when subscription cleanup fails.
No listing, prefix deletion, same-name adoption or Pub/Sub request retry is performed.

Pub/Sub resource names can be reused after deletion, and the [subscription](https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions/delete) and [topic](https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/projects.topics/delete) delete APIs accept no generation precondition.
Names and ownership labels therefore do not make deletion atomic with replacement.
The caller's shared environment lock and exclusive control of run resources must cover provisioning through cleanup, including each read/delete gap; unexpected replacement is an incident to reconcile.
The guard runs before each Pub/Sub request or logical storage adapter call and must prove current approval, exclusive ownership and the appropriate admission or cleanup budget.
For a control-record `GET`, the shared adapter reads metadata and then generation-matched data, repeating on a generation race at most five times: reserve up to ten GCS data requests for that single callback.
A control-record `PUT` callback denotes the logical create upload, whose GCS HTTP method is `POST`.
The caller must reserve the whole adapter call's request and time budget; callback counts are not HTTP-request counts, and credential refresh and the guard's own I/O require additional caller accounting.
The helper delegates those checks to the caller; it does not implement durable operation counters, the lock, deadlines or independent supervision.
Pub/Sub HTTP requests use the shared 20-second timeout with redirects disabled and no automatic retry; this is a per-request transport limit, not a total elapsed-time or cost ceiling.

### Permissions and remaining integration

The [persistent IAM foundation](../../../opentofu/README.md#pubsub-lifecycle-authority) defines project-wide resource control for the runner and metadata/deletion authority for the supervisor.
The runner can change policies on any topic or subscription in the project, including granting itself data access; the runtime's ownership checks are not an IAM restriction on that identity.
The workload GSA receives no project-level Pub/Sub grant.
The custom `projects/flink-gcp/roles/tier3PubSubConsumer` role contains only `pubsub.subscriptions.get` and `pubsub.subscriptions.consume` and is defined without a project binding.

The version 2 ownership manifest freezes these explicit resource policies before creation:

| Owned resource | Member at `flink-gcp.iam.gserviceaccount.com` | Role |
| --- | --- | --- |
| Both input topics | `tier3-runner` | `roles/pubsub.publisher` |
| Both input subscriptions | `tier3-pubsub` | `tier3PubSubConsumer` |
| Output topic | `tier3-pubsub` | `roles/pubsub.publisher` |
| Output subscription | `tier3-supervisor` | `tier3PubSubConsumer` |

`install_grants()` validates all resources and requires all six explicit policies to be empty before the first write.
It requests policy version 3 to expose conditional bindings, then revalidates each resource and empty policy before setting exactly its recorded binding.
Every write preserves the returned nonempty etag; a missing etag refuses installation, even on an otherwise empty policy.
The [Pub/Sub policy contract](https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/Policy) describes that read/modify/write concurrency precondition.
The [GET](https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/projects.topics/getIamPolicy) and [POST](https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/projects.topics/setIamPolicy) operations use the same guarded transport and storage budget as the resource helpers.
IAM requests use `grant` or `inspect-grants` phases and the resource name with its IAM method suffix (including the version query on reads).
Both methods also call `inspect()`, whose guard callbacks use the `inspect` phase; installation ends by calling `inspect_grants()`.
Unknown policy fields, extra or conditional bindings, ownership drift, non-success responses and ambiguous writes stop installation without retry or policy merging.
Partial installation can be inspected through service readback and cleaned by deleting the owned resources; installation is not resumable and refuses even matching pre-existing grants.
`inspect_grants()` validates the six resource settings and exact explicit bindings again and returns their named policies for external evidence.
It fails when any policy is incomplete; it is not a partial-installation recovery command.

A successful call has the following operation budget, measured with the shared helper and synthetic transport:

| Method | Guard callbacks by phase | Pub/Sub requests | Logical GCS reads | Maximum GCS data requests |
| --- | --- | --- | --- | --- |
| `install_grants()` | `inspect`: 14; `grant`: 42; `inspect-grants`: 12 | 42 | 26 | 260 |
| `inspect_grants()` | `inspect`: 7; `inspect-grants`: 12 | 12 | 7 | 70 |

Reserve these bounds before entering the method and still enforce the guard before each operation.
These totals exclude initial `provision()`, cleanup, credential refresh and the guard's own I/O; total elapsed time needs its own deadline.
Policy reads deliberately re-read ownership, so they use more storage reads than the resource inspection that checks its manifest once.

Policy readback does not prove effective permissions, propagation or the absence of inherited grants.
During separately approved provisioning, retain fresh-resource version-3 policy responses and verify that empty policies return nonempty etags before installation.
If that service behavior is absent, stop and clean the owned resources rather than attempting an unconditional policy write.
Before admission, the later controller must verify access using each participating identity, retain the observations, and account for IAM propagation within its approved deadline and operation ceilings.
The output subscription belongs to the supervisor's independent observation path; runner and workload are not granted consume access there by this plan.
The [Pub/Sub access-control reference](https://docs.cloud.google.com/pubsub/docs/access-control#required_permissions) lists the permissions for each API operation.
The workload uses input metadata/consume and output publish access, with no resource creation, deletion or policy mutation.
An unsupported manifest version is reported explicitly and refused rather than upgraded or adopted; no version 1 live resource execution was performed in the preparation stage.

Synthetic tests exercise resource and policy request grammar, collisions, manifest replacement, etag conflicts, ambiguous and partial writes, settings drift and repeated cleanup without credentials or service calls.
A composition test uses the production GCS adapter with a fake client to exercise all five generation-read attempts and the guard's separate adapter-call budget.
These tests do not establish live API behavior, effective IAM access or cross-actor lifecycle safety.
The later controller must integrate resource and policy operations with shared admission and cleanup, collect external service and access evidence, and reserve separately approved numeric execution ceilings before starting the relay.

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
