# Pub/Sub Tier-3 recovery application

This internal Flink 2.2.1 / Java 17 application prepares the DataStream and Table entry points of [issue #1361](https://github.com/flink-gcp/flink-connector-gcp/issues/1361).
It relays two pre-created subscriptions through the production Pub/Sub source and publisher sink into a pre-created output topic.
The `--entry-point` argument selects whether the relay reaches them through the DataStream API or through the Table connector.
It is an unbounded streaming job over a bounded logical input domain: receiving every expected ID does not stop the job, Pods or Operator.
An independently supervised lifecycle must bound execution and perform cleanup before deployment is admitted.

## Build and local validation

```sh
mise x -- just tier3-pubsub-verify
```

The opt-in `tier3-pubsub` Maven profile keeps the application outside published connector artifacts.
The build produces `target/pubsub-recovery.jar` and copies runtime dependencies with their original licenses/notices into `target/image-lib/`.
The Dockerfile adds these files to `/opt/flink/usrlib/` over the reviewed Flink image and enables its bundled GCS filesystem plugin.
The Table entry point uses the Table API, planner loader and Table runtime in the image's `lib/` rather than packaging its own, and the Dockerfile refuses a base image without them.
The entry point is `io.github.flink.gcp.connector.tier3.pubsub.PubSubRecoveryJob`.
The [image publication workflow](../../../.github/workflows/tier3-images.yaml) packages it as `pubsub-recovery` on the fixed Flink 2.2.1 / Java 17 AMD64 base.
The application CI lane builds the Dockerfile without publishing; its context admits only the Dockerfile, application JAR and packaged runtime dependency JARs.
The [first publication](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/35494622116) built main commit `cba9043faee0ceb067cba23b56fe3e0abe7f0542`; a GAR read confirmed its application digest.
The reusable deployment definition below consumes a supplied digest; it does not select or admit an active run.

Unit tests cover argument and input bounds, serialization, missing/foreign restored state, contradictory evidence, missing IDs and duplicate classification.
They also pin each entry point's operator UIDs, the Table ones across repeated translations and both parallelisms.
MiniCluster tests run each entry point with production source/sink RPCs against the Pub/Sub emulator, replace TaskManagers after a completed checkpoint and restore savepoints at parallelism 1→2 and 2→1.
They also refuse a savepoint of one entry point in the other and check that neither sink creates a missing output topic.
They publish new input to both subscriptions after the disruption and require output from restored attempts.
These are local wiring and state-continuity checks; they do not measure real-service redelivery, ACK deadlines, lease expiry, ordering, GCS access, GKE scheduling or Operator reconciliation.

## Deployment definition

[`pkg/pubsub.#Application`](../../pkg/pubsub/application.cue) defines the relay on Flink 2.2.1 / Java 17.
A delivery below `runs/` supplies `run.id`, `run.expiresAt`, `run.namespace: "tier3-pubsub"` and the application's `id`, `image`, `phase`, `recordsPerSubscription`, `parallelism` and `entryPoint` inputs.
The [synthetic fixture](../../tests/fixtures/pubsub.cue) demonstrates that binding; tests supply disposable values and render every initial/upgrade, parallelism 1/2 and entry-point combination.
The lifecycle CLI renders an [offline trial proposal](#offline-trial-proposal), and [dispatch](#approval-dispatch) runs one of the four [reviewed trials](../../lifecycle/README.md#reviewed-pubsub-trials) through the runner's [admission](#admission-and-effective-access) and the supervisor's [recovery exercise](#recovery-exercise).

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
One JobManager and one or two single-slot TaskManagers each request and limit 1 CPU, 2 GiB memory and 1 GiB ephemeral storage on AMD64 nodes; the TaskManagers run on Spot and the JobManager on normal capacity.
At parallelism two, the application alone therefore uses three Pods; later admission must additionally budget the Operator and independent supervisor.
Namespace quotas remain idle until a separately approved lifecycle admits the complete workload.

Savepoint upgrades preserve state and refuse unclaimed state.
The definition disables last-state fallback, so an unavailable savepoint cannot be replaced by last-state recovery, and sets `kubernetes.operator.snapshot.resource.enabled=false`, so Operator 1.15.0 reports the upgrade savepoint in the application's status rather than in a separate FlinkStateSnapshot, which is where the recovery exercise reads it, as the BigQuery application does.
The rendered job arguments use the application's `--name=value` syntax, match Flink parallelism and require restored identity in the `upgrade` phase.
That phase does not locate a savepoint or prove a checkpoint boundary: the supervisor's [recovery exercise](#recovery-exercise) selects and checks the recovery state and observes the restoration.
The logical input domain and restart count do not bound elapsed execution or billable service operations.

Before deployment, integrate the owned-resource operations described below with scoped service grants, independent stop/cleanup supervision, concrete execution limits and external fault/evidence collection.
The deployed recovery trials of [#1361](https://github.com/flink-gcp/flink-connector-gcp/issues/1361) ran on the DataStream entry point, one of each kind, and lost no message; their [findings](../../../docs/adr/evidence/0165-pubsub-trial-findings-1361.md) record the verdicts, every attempt and what they leave unobserved; the replacement reruns of [#1720](https://github.com/flink-gcp/flink-connector-gcp/issues/1720) observed the redelivery those trials did not, as their [findings](../../../docs/adr/evidence/0165-pubsub-redelivery-findings-1720.md) record.
The Table entry point has no deployed trial.
The offline manifest check is `mise x -- just tier3-check`; it does not create resources or establish service settings.

## Offline trial proposal

Render one unapproved trial with the existing application and lifecycle delivery packages:

```bash
cat > /tmp/pubsub-trial.json <<'JSON'
{
  "version": 5,
  "trial": "rescale-out",
  "entry_point": "datastream",
  "records_per_subscription": 1000
}
JSON

mise x cue uv -- uv run --locked --package flink-tier3 --no-dev flink-tier3 render \
  --scenario pubsub-recovery \
  --run-id proposal-1361 --nonce aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --started-at 2026-09-21T00:00:00Z --expires-at 2026-09-21T01:00:00Z \
  --active-seconds 3420 \
  --revision bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \
  --application-image us-central1-docker.pkg.dev/flink-gcp/flink-tier3/pubsub-recovery@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc \
  --trial-file /tmp/pubsub-trial.json > /tmp/pubsub-proposal.json
```

Run from the repository root, or set the CLI's global `--repository` argument before `render`.
The example uses synthetic revision/image identities and dates; rendering performs no credential lookup, image availability check or cloud operation.
The output contains `proposal`, `application`, `recovery_application` and `delivery` (ConfigMap and supervisor Job).
The proposal records the supplied source revision, exact application/supervisor hashes, the application and supervisor images, service settings and grants.
It does not prove that the installed package, checkout or application image came from the supplied revision.
`approved` is always false and the ConfigMap's `approval.json` remains empty; neither output is an execution approval.

| `trial` | Initial → recovery parallelism | Planned operation |
| --- | --- | --- |
| `jm-replacement` | 2 → 2 | Replace the JM after an externally observed completed checkpoint |
| `tm-replacement` | 2 → 2 | Replace an active TM after an externally observed completed checkpoint |
| `rescale-out` | 1 → 2 | Stop with a savepoint and restore the upgrade phase |
| `rescale-in` | 2 → 1 | Stop with a savepoint and restore the upgrade phase |

JM/TM proposals keep both manifests identical: recovery is performed by the existing deployment, with no upgrade submission.
For rescaling, `recovery_application` is also stored under the shared delivery key `upgrade-application.json` and requires restored state.
Each plan covers one trial and two input subscriptions with 3–10,000 logical records per subscription.
Each subscription's domain splits into three disjoint, nonempty cohorts at `floor(records / 3)` and `floor(2 * records / 3)`, published in this order:

| Cohort | Published |
| --- | --- |
| `before_checkpoint` | At admission, before the application exists |
| `after_checkpoint` | When the supervisor asks, after a retained completed checkpoint; a replacement trial's replay population |
| `after_recovery` | When the supervisor asks, after observed recovery |

Each range is published in ascending batches of at most 1,000, the publish API's per-request maximum, without crossing the range boundary: the first by admission, the others through the [cohort requests](#cohort-requests) below.
The output records exact logical messages, payload bytes and required publication calls.
A cohort range alone does not prove that its messages were processed but not checkpoint-confirmed: the [recovery exercise](#recovery-exercise) times the cohort and proves the boundary.

The input file requires exactly the four fields shown in the example and rejects duplicate JSON keys, unknown fields and inputs larger than 8 KiB.
`entry_point` is `datastream` or `table` and applies to both manifests, so a trial never changes the entry point between phases.
Version 5 removed version 4's `traffic_limits`: no runtime ceiling meters the trial's messages, calls, requests or evidence bytes.
Those requests are nearly free, and the trial's cost is bounded by its input domain, its window and its Pod quota, as [ADR-0165](../../../docs/adr/0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md) records.

The proposal fixes a one-hour window with the last 15 minutes reserved for cleanup and a 3,420-second supervisor Job deadline.
Admission's temporary quotas hold seven Pods: three steady Flink Pods plus one Flink replacement allowance in `tier3-pubsub`, and the Operator, supervisor and one Operator replacement allowance in `tier3-system`; PVCs are zero.
Terminating Pods can overlap their replacements and remain [charged to namespace quota](https://kubernetes.io/docs/concepts/policy/resource-quotas/#quota-on-object-count) until their phase is terminal, which is what the allowances are for.
Each Flink Pod uses the existing one-vCPU, 2-GiB shape and the shared AMD64 constraint; only the TaskManagers select Spot.
The proposal's `cost` is a planning estimate, USD 1.987 for every trial; the owner approves it before a dispatch, and nothing at run time compares spend against it.
It charges all seven Pods for the whole hour at the conservative CPU, memory and ephemeral-storage rates, the four application slots at the Flink shape and the three control slots at the Operator's, with no Spot or commitment discount, and adds a USD 1.00 reserve.
The reserve covers what the input domain and the window bound, each a fraction of it at the largest domain of 10,000 records per subscription and at the [recovery exercise](#recovery-exercise)'s pulls, at most five batches of 100 per 15-second poll until cleanup: Pub/Sub throughput at USD 40 per TiB with a 1 KB minimum per request, Cloud Storage operations at USD 0.005 per 1,000 and storage, Cloud Logging ingestion, and the logs and evidence leaving Google Cloud at USD 0.12 per GiB.
The reviewed sources are [Pub/Sub pricing](https://cloud.google.com/pubsub/pricing), [Cloud Storage pricing](https://cloud.google.com/storage/pricing), [Google Cloud Observability pricing](https://cloud.google.com/products/observability/pricing), [network pricing](https://cloud.google.com/vpc/network-pricing) and [GKE pricing](https://cloud.google.com/kubernetes-engine/pricing), checked on 2026-10-10, when the Pod rates still exceeded the published Iowa Autopilot prices; the proposal records that date as the estimate's basis.
A runnable approval also needs live image/provenance checks, stop enforcement, effective-access checks and independent cleanup supervision.
A passed deadline, evidence failure, lost ownership, uncertain actor quiescence and expiry must stop a later trial rather than produce a success verdict.
The [recovery exercise](#recovery-exercise) injects the faults, orchestrates the savepoint and decides the trial's [verdict](#verdict), which the [offline analysis](#offline-recomputation) recomputes from the exported evidence.

## Run identity and service resources

| Argument | Contract |
| --- | --- |
| `--run-id` | Required Tier-3 label of at most 40 lowercase letters/digits/hyphens, beginning and ending with a letter or digit |
| `--records-per-subscription` | Logical sequence domain per input, 1–10,000; defaults to 1,000 |
| `--parallelism` | 1 or 2; defaults to 1 |
| `--phase` | `initial` by default, or `upgrade` |
| `--require-restored` | `false` by default; `upgrade` requires `true` |
| `--entry-point` | `datastream` by default, or `table`; fixed for every phase of a run |

Arguments use `--name=value`; unknown or repeated arguments fail.
The fixed project is `flink-gcp`.
Input index `i` is 0 or 1; its topic and subscription both have ID `t3-<run-id>-in-<i>`.
The output topic and independent observer subscription both have ID `t3-<run-id>-out`.
All three subscriptions must exist before any corresponding publication, including the output observer before starting the relay.
The job uses ADC, provides no key-file option, never creates a subscription and sets the sink to `CREATE_NEVER` (`'sink.create-disposition' = 'create-never'` on the Table entry point).
The emulator endpoint is a package-private test seam, absent from the deployed CLI.

### Owned resource operations

[`flink_tier3.pubsub.resources`](../../../tools/tier3/src/flink_tier3/pubsub/resources.py) supplies internal `ResourcePlan` and `Resources` helpers for the internal durable lifecycle controller.
These helpers are not a CLI scenario and do not admit a workload.
A plan requires a fresh run ID and a caller-generated ownership nonce of 32 lowercase hexadecimal digits.
The caller supplies the shared authorized HTTP session, GCS `Storage` adapter and a mandatory `before_operation(phase, method, name)` guard; construction performs no I/O.
Use the shared evidence bucket for the control record, as with the other lifecycle records.

`provision()` refuses an existing control record or any of the six existing service names, including resources with matching labels.
It then writes the version 2 plan, including the fixed data-role bindings described below, to `_control/pubsub/<run-id>.json` with GCS generation-match zero before creating the three topics followed by the three subscriptions.
Every service resource carries `tier3-run` and `tier3-nonce` labels.
The helper re-reads the matching manifest before every service write and validates all six service resources after creation; `inspect()` repeats that readback validation.
An ambiguous create stops without retrying or adopting the resource; the retained manifest permits cleanup of matching partial creation.
Re-running `provision()` against that manifest is refused.
The retained intent lives outside `_control/runs/`, whose contents block new shared environment locks.
The durable controller below maintains separate active run control before provisioning; the retained Pub/Sub manifest is not that active-run record.

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
A subscription reads back `state: ACTIVE`, which readback requires; a topic without an ingestion source reads back no `state` at all, as measured on 2026-10-10, so a topic's state is checked only when present.
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
The guard runs before each Pub/Sub request or logical storage adapter call; the production guard holds each Pub/Sub request to its deadline, and the caller keeps exclusive ownership.
For a control-record `GET`, the shared adapter reads metadata and then generation-matched data, restarting after a jittered wait on a generation race, in at most twenty attempts: up to forty GCS data requests for that single callback.
A control-record `PUT` callback denotes the logical create upload, whose GCS HTTP method is `POST`.
Callback counts are not HTTP-request counts.
The helper delegates those checks to the caller; it does not implement the lock, deadlines or independent supervision.
Pub/Sub HTTP requests use the shared 20-second timeout with redirects disabled and no automatic retry; this is a per-request transport limit, not a total elapsed-time or cost ceiling.
Responses are read streamed and refused once their body passes 1 MiB, the same local cap the message helper applies; a body that fails while arriving is a transport failure.
The actor sessions treat no response as a redirect, because requests otherwise reads a redirect's whole body before any cap, even when it does not follow it; a 3xx to a resource, message or identity request therefore returns unread and is refused as a non-success status.
Credential refresh goes through google-auth's own request, which does not stream, so a token endpoint's response body is still read whole.

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

A successful call makes the following operations, measured with the shared helper and synthetic transport:

| Method | Guard callbacks by phase | Pub/Sub requests | Logical GCS reads | Maximum GCS data requests |
| --- | --- | --- | --- | --- |
| `install_grants()` | `inspect`: 14; `grant`: 42; `inspect-grants`: 12 | 42 | 26 | 260 |
| `inspect_grants()` | `inspect`: 7; `inspect-grants`: 12 | 12 | 7 | 70 |
| `provision()` | `provision`: 20; `inspect`: 7 | 18 | 8, plus one create | 80, plus one upload |
| `cleanup_or_confirm_absent()`, manifest present | `cleanup`: 26 | 18 | 8 | 80 |
| `cleanup_or_confirm_absent()`, manifest absent | `cleanup`: 7 | 6 | 1 | 10 |

Nothing reserves these counts: they size a call, and the production guard holds each request to its deadline instead.
These totals exclude credential refresh; total elapsed time needs its own deadline.
Policy reads deliberately re-read ownership, so they use more storage reads than the resource inspection that checks its manifest once.

Policy readback does not prove effective permissions, propagation or the absence of inherited grants.
During separately approved provisioning, retain fresh-resource version-3 policy responses and verify that empty policies return nonempty etags before installation.
If that service behavior is absent, stop and clean the owned resources rather than attempting an unconditional policy write.
[Admission](#admission-and-effective-access) verifies access with each participating identity, retains the observations and waits for IAM propagation within its deadline.
The output subscription belongs to the supervisor's independent observation path; runner and workload are not granted consume access there by this plan.
The [Pub/Sub access-control reference](https://docs.cloud.google.com/pubsub/docs/access-control#required_permissions) lists the permissions for each API operation.
The workload uses input metadata/consume and output publish access, with no resource creation, deletion or policy mutation.
An unsupported manifest version is reported explicitly and refused rather than upgraded or adopted; no version 1 live resource execution was performed in the preparation stage.

Synthetic tests exercise resource and policy request grammar, collisions, manifest replacement, etag conflicts, ambiguous and partial writes, settings drift and repeated cleanup without credentials or service calls.
A composition test uses the production GCS adapter with a fake client to exercise a read that settles after four lost generation races, and that the guard sees one callback per adapter call however many data requests it makes.
These resource-helper tests do not establish live API behavior, effective IAM access or deployed cross-actor lifecycle safety.
A deployed trial starts the relay only after the access probes have passed and the owner has approved the dispatch, as [approval dispatch](#approval-dispatch) describes.

`Resources.cleanup_or_confirm_absent()` selects strict owned cleanup or absence confirmation through one additional guarded manifest read.
A present manifest delegates to `cleanup()` and its ownership checks; an absent manifest permits only six guarded name reads, refuses any existing resource, and never authorizes deletion or adoption.
Its caller must prove quiescence first.
The durable controller below uses this entry point; the original `cleanup()` still refuses a missing manifest.

### Durable preparation and cleanup

[`PubSubLifecycle`](../../../tools/tier3/src/flink_tier3/pubsub/lifecycle.py) connects the resource helper to the existing generation-checked active run record.
It is an internal caller contract for `pubsub-recovery`; [dispatch](#approval-dispatch) builds and validates the approval, whose trial binds the inputs, but does not admit execution through it.
The caller supplies the lifecycle actor's session, the shared ownership lock, exclusive resource control and deadline guard.
The [IAM apply](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/35504842276) succeeded; subsequent role/binding readback matched the three custom roles and two project bindings, and a local refreshed GCP plan was empty.
These foundation observations do not establish per-resource data access or a recovery result.

`initialize()` saves immutable resource/application intent in `_control/runs/<run-id>.json` before preparation.
Only the submitting runner may initialize or prepare.
`prepare()` claims one attempt through a conditional transition from `initialized` to `preparing`; a competing or restarted runner cannot repeat it, even after a lost response.
Before the helper writes its resource manifest, the controller records `creation_intent: true` in the active run.
Successful resource-settings readback and the final explicit-policy readback are retained in that record, outside the workload JVM, before the stage becomes `prepared`.
The Pub/Sub portion of control is limited to 256 KiB; a failed observation write leaves its partial service work attributable for cleanup, without permitting preparation to resume.
This is bounded control evidence, not the later output-message evidence stream or proof of effective permissions.
`verify_prepared()`, which an actor reaches through its handoff's `verify()`, repeats `inspect_grants()` for either actor while the run is still `APPROVED` or `READY` and the stage is `prepared`, so settings, ownership or policy drift since preparation, including an expired subscription, refuses before admission without writing anything; it checks the run and the stage once, at its start.

Every resource request passes the caller's guard; the controller's own control reads and writes do not.
Only this rig's own code writes the record, the run's two actors and the lifecycle's [recovery](../../lifecycle/README.md#stranded-environment-lock), so the controller reads it as written rather than validating its shape again.
Preparation checks for a shared stop, an evidence failure or a run phase outside approved/ready at its transitions: the claim, the creation intent, the step from creating resources to installing their grants, and the `prepared` mark.
It does not re-read the record before each resource request, so a stop during a step lets that step's remaining requests run and then refuses the next step, which leaves the partial work cleanable.
Verification checks the same conditions once, at its start, and an access probe before each round of tests and each pull, because its wait for grants can last most of the admission window.
There is no numeric execution policy in this controller.

`cleanup(quiesce)` is available to the runner or independent supervisor.
It first persists the shared stop and `cleaning` stage, then requires the caller's barrier to return exactly `True` after proving all creators, writers, replacements and their in-flight service calls quiescent.
The stop flag alone cannot prove that an earlier API request has stopped at the server.
After the barrier, the controller re-reads the record, requires every bound actor's release and delegates deletion to the strict resource helper, whose requests it holds to no deadline, because cleanup must finish whenever it runs.
If creation intent was never recorded, cleanup leaves any colliding service names untouched because this attempt authorized no resource writes.
If creation intent was recorded but the resource manifest is absent, cleanup checks all six planned service names with guarded reads and succeeds only when every name is absent.
This covers a lost creation-intent write response or a crash before the manifest write without reconstructing ownership or deleting anything.
An existing resource, an uncertain absence read or a replaced manifest blocks cleanup.
A barrier failure, uncertain ownership, lost deletion response or failed control write retains active run control; cleanup can be retried, and successful retries recheck owned service absence.
A retry from `cleaned` first returns to `cleaning`, so an uncertain fresh check closes settlement again; a previous absence observation does not override a failed recheck.
Missing-manifest cases with remaining resources require separate investigation; preparation cannot repair or adopt them.

The controller marks only service cleanup complete; it does not set run success, remove Flink state, shut down the Operator, delete active control or release the environment lock.
Shared Operator shutdown and final receipt/lock release refuse a present Pub/Sub record unless its stage is `cleaned` and the shared stop is set.
Final settlement copies the cleaned Pub/Sub control portion, including its intent and retained observations, into `runs/<run-id>/result.json` before deleting active control.
Finalization deletes active control at the generation it derived the receipt from, so a concurrent cleanup or evidence update retains the record and lock for retry, and a retry keeps the receipt it finds.
When control deletion succeeded and lock release did not, the [recovery workflow](../../../.github/workflows/tier3-recover.yaml) writes a minimal cleaned record and finalizes again, keeping the receipt that still carries the cleaned Pub/Sub portion, as the [lifecycle runbook](../../lifecycle/README.md#stranded-environment-lock) describes.
The caller must keep exclusive control and writer quiescence through settlement; a stored cleanup marker does not detect a resource recreated afterward by another administrator.
Synthetic tests compose production record and resource adapters with fake transports for concurrent claims, restart, stop drift, partial mutations, the control-record size limit and shared settlement gates.
[Admission](#admission-and-effective-access) composes this controller with the actors and the access probes, and the supervisor's [recovery exercise](#recovery-exercise) follows it.

### Actor construction and the request guard

[`pubsub_actors`](../../../tools/tier3/src/flink_tier3/pubsub/actors.py) is the only production path that binds a Pub/Sub handoff to a runner or supervisor.
Neither checks the manifests again: dispatch rendered them through `pubsub_plan.prepare()`, which requires through `require_trial_jobs()` that each `spec.job` runs exactly what the trial derives, and the approval pins their digests, which the supervisor entrypoint checks against its mount.
A digest pins a manifest, and only that rendering check says the manifest runs the approved trial.
Each actor sends its requests through a [`PubSubSession`](../../../tools/tier3/src/flink_tier3/pubsub/session.py) over the BigQuery actors' [session](../bigquery/README.md#internal-actors), which returns a redirect unread, and its own workload's credentials, and a supervisor takes a fresh process token each time it is constructed.
Constructing an actor sends no Pub/Sub request and does not check which principal the credentials name, although discovering the default credentials may ask the metadata server: each actor takes the credentials of the environment it runs in, the runner's workload identity federation in CI and the supervisor's Kubernetes service account, so a wrong identity needs a misconfigured environment, and IAM then refuses the first call that identity has no permission for ([#1726](https://github.com/flink-gcp/flink-connector-gcp/issues/1726)).
The supervisor's cleanup barrier, [`pubsub_quiesce.barrier`](../../../tools/tier3/src/flink_tier3/pubsub/quiesce.py), is built from the run rather than supplied, and waits until no object of any writer kind remains in `tier3-pubsub`, whoever owns it: the namespace's `pubsub` service account can create Pods and Deployments, and any Pod running as it holds the workload's data grants.
It proves only that no such object remains after graceful deletion, not that a request a terminated Pod sent has finished at the service; the actors' unsettled-call markers cover only their own requests.
Outside a run the namespace is expected to hold no such object; dispatch does not refuse one, and one left there keeps the barrier from passing, so cleanup keeps the lock until it is removed.
`Runner.start` and `Supervisor.supervise` refuse `pubsub-recovery` without these actors, and the supervisor entrypoint builds its own through this factory.

Each actor's [`PubSubGuard`](../../../tools/tier3/src/flink_tier3/pubsub/guard.py) is its `before_operation`, and it checks only Pub/Sub service requests.
It refuses to start one less than 20 seconds, the request's timeout, before its deadline.
It runs as the last step before the request is sent, after any control write of the call, such as its begin marker.
That timeout bounds connecting and each read rather than the whole request, a token refresh before the request takes the same timeout, and reading a streamed body is bounded only by the per-read timeout and the helpers' 1 MiB response cap, so a request can still end after the deadline.
The deadline is the admission deadline, the earlier of 900 seconds after the window's start and `cleanup_at`, unless a caller holds another through the controller's `until()`: `publish` and `collect` hold `cleanup_at`, because later cohorts and collection belong to the exercise, a caller publishing during admission passes the admission deadline, and cleanup holds none.
Control and storage operations are not held to it, so a stop, a cleared marker or the evidence of a request already sent can still be written; the handoff enforces stop and evidence failure where it admits new work.
No per-method operation bound and no aggregate request ceiling exists, as the [offline trial proposal](#offline-trial-proposal) explains.

### Admission and effective access

`Runner.start(config, job, application, probe)` admits a `pubsub-recovery` run only with a handoff that carries its `PubSubGuard` and settled-write check, as `pubsub_actors.runner()` builds.
The manifests and the probe Pod it submits are the ones dispatch rendered and bound to the approval; that rendering checked both jobs against the trial and the probe Pod against the plan it tests and the program it runs, so admission does not check them again.
After the supervisor Job and the Operator are ready, and while the run is `READY`, [`pubsub_admission`](../../../tools/tier3/src/flink_tier3/pubsub/admission.py) runs these steps in order:

1. Bind the runner and the resource intent in one control update.
2. Create the six resources, then install their grants: installing a policy reads the resource it belongs to, so creation comes first.
3. Probe the runner's own access.
4. Open the application quota, run the workload's probe Pod, read what it saw, delete it, and wait until it is gone, so that no other Pod of the run stands beside the application.
5. Wait for the supervisor to join and to record its own access.
6. Re-read every resource and policy.
7. Publish the first cohort, so that the application finds its input waiting.

It then creates the application and sets `RUNNING`.
Every step must finish within 900 seconds of the window's start: `admission_open()` refuses after that, and the guard refuses to start a Pub/Sub request too close to it.
A grant is not taken as proof of access.
Each identity is tested on all six resources for the one permission the run's bindings decide there, publish on a topic and consume on a subscription, so every resource is a positive or a negative control for each identity.
A permission test answers only for the identity that sends it, so the runner and the supervisor each probe themselves.
The workload's service account is reachable only through the `pubsub` Kubernetes service account, so the runner creates a Pod named `<run>-access-probe` that runs as it, through a runner-only Role in `tier3-pubsub`.
The Pod runs the lifecycle tools image with [`pubsub/probe.py`](../../../tools/tier3/src/flink_tier3/pubsub/probe.py) passed inline, because a Pod cannot mount the control ConfigMap from another namespace; CUE renders it at dispatch.
The runner keeps the Pod's log, at most 64 KiB, as `pubsub-workload-probe` evidence and requires the Pod to succeed: the program checks its own identity and every expectation, and exits non-zero on any refusal, so its exit status is the result.
The program refreshes its own token before each request and then starts the request only while its 20-second timeout still fits before the deadline; it uses a plain HTTP session, so nothing replays a request answered 401 after that check.
When a request no longer fits, the program logs a refusal and fails.
As on the runner's side, a slowly arriving response body is bounded only by the per-read timeout and the 1 MiB cap.
The runner records its intent before it creates the Pod, so cleanup deletes a probe Pod the runner left behind, including one whose creation the runner did not survive to record.
A create whose response was lost can land after cleanup began, even after the workload is gone, so cleanup looks for that Pod on every pass of its deletion loop and again at each poll of its namespace-barrier wait.
It adopts the Pod only when it matches the intent, as the runner's own adoption requires, and deletes it even when it cannot record it.
A Pod that lands between a poll's barrier check and the handoff's own second check ends that cleanup pass with the lock kept, as any other writer appearing there would.
A grant that is missing may still be propagating, which IAM documents as typically two minutes and potentially seven or longer, so the six tests are repeated every 15 seconds while a whole further round, one test at a time, still fits before the deadline; a local stop, or one recorded in the run control, ends the wait.
A grant present where it must not be is refused at once: the resource policies were empty before installation, so the run's own writes cannot explain it.
Each consumer then pulls its subscriptions once, without acknowledging, while nothing is published yet; a 403 is retried in the same way, and a received message is refused.
A pull that gets no definite answer marks the session's writes unsettled, but the access probe runs outside the handoff's call markers, so it keeps no marker of its own; nothing is published yet, so it cannot lease a message.
The probe program silences library logging, so the log kept as evidence holds only its own JSON lines.
The actors' observations are recorded under the run control's `pubsub.access`, one entry each for the runner and the supervisor, and reach the final receipt with the rest of the Pub/Sub state; the workload's are the probe Pod's log.

The supervisor's entrypoint builds its actor through `pubsub_actors.supervisor()`.
Once preparation completes it joins, probes its own access, including an empty pull of the output subscription, and waits for admission to finish.
It then runs the [recovery exercise](#recovery-exercise), and cleanup deletes the run's resources after it.
Only a supervisor deletes Pub/Sub resources, and the Job replaces no supervisor Pod.
A supervisor stopped once the run has Pub/Sub state waits for the runner to settle and release, then cleans up, which must fit its grace period.
A supervisor whose cleanup outlasts its grace period, or whose Pod is gone before cleanup, leaves the resources and the lock for an operator.

When admission fails after resources exist, for example through settings or policy drift found by a read, a probe refused or still missing access at the deadline, or a supervisor that never joins, `start` raises with no call in flight, the runner's settlement releases it, and the supervisor, joined or not, deletes the six resources.
Ownership drift, a replaced manifest or a resource whose labels or live topic binding no longer match, stops the run without completing deletion and keeps the lock, because cleanup cannot prove the resources are this run's.
A write without a definite answer in a handoff call keeps its marker and the lock, as the [handoff](#actor-ownership-and-cleanup-handoff) describes.

## Recovery exercise

[`PubSubExercise`](../../../tools/tier3/src/flink_tier3/pubsub/exercise.py) is the supervisor's exercise for a `pubsub-recovery` run; it runs in the supervisor's shared poll loop, which audits the namespace, reads the Pods' logs and the job's checkpoint history through its REST Service every 15 seconds, and it drives one approved trial.
On each poll it first pulls the output subscription through the [output collector](#output-collector), at most five batches and at least one, and once a minute records a `pubsub-measurement` sample.
A sample holds each job vertex's backpressure and its Pub/Sub connector metrics, aggregated over subtasks, and a backlog derived from the supervisor's own requests and observations; the [verdict](#verdict) lists which metrics it reads.
A connector metric's id carries its operator's name, which differs between the entry points, so each sample lists a vertex's metrics and asks for the ones it recognizes; a read that fails with an API or transport error, answers more than its read ceiling or answers no JSON, or a listing that names none of them, is recorded as unavailable, and the trial goes on.
`pendingAcks` is what the source holds leased, received or emitted and not yet acknowledged, which is the population a fault returns to the service.
The `backlog` counts the requested cohorts' inputs not yet seen on the output subscription, from the supervisor's request rather than the publication, so it includes the runner's publication delay, and it reads no service metric.
Its first stage starts when the loop sees the application admitted, and it expects the JobManager and as many TaskManagers as the application's `taskManager.replicas`.
Each stage below is recorded before its operation, as `recovery-<stage>` evidence and in the run control's `recovery` record, together with the outcomes gathered so far.

| Stage | Waits for | Then | Deadline from its start |
| --- | --- | --- | --- |
| `baseline` | `before_checkpoint` observed on both inputs | On the next poll, notes the highest checkpoint id the job has reported | 900 s from admission |
| `checkpoint` | A completed checkpoint with a higher id, under the run's state prefix | Retains it and requests `after_checkpoint` | 300 s |
| `boundary` | `after_checkpoint` published and observed on both inputs | Injects the trial's fault | 180 s |
| `recovering` | The restoration, restored attempts, and the displaced population redelivered | Requests `after_recovery` | 600 s |
| `after` | `after_recovery` observed on both inputs by the attempts serving after the fault, then another completed checkpoint with a higher id | Records `complete` | 420 s |

Each deadline is also capped at `cleanup_at`, and a cohort's request leaves the runner 90 seconds to start publishing it; a passed deadline stops the trial and cleanup follows.
A stage replaces its deadline only while the outgoing one still holds, checked again at the transition and after the poll's pulls and readings, which can take their own transport timeouts, so an expired stage is never carried into the next one's deadline.
Checkpoint ids grow with their trigger, so a checkpoint whose id exceeds every id the job reported on the poll after a cohort was fully observed was triggered after that observation, on no clock but the job's own; the history is read before a poll's pulls, which is why the next poll's is taken.
Flink 2.2.1's REST API caches checkpoint statistics for `web.refresh-interval`, three seconds by default, and the mark is read at least one 15-second poll after the observing pull; a checkpoint triggered after an output observation covers the observed messages, because the source emitted them before its barrier.
The retained checkpoint was seen completed before the supervisor wrote the replay cohort's request, and the runner [starts a cohort](#cohort-requests) only after reading that request, so no checkpoint completed by then can cover the replay cohort.

| `trial` | Fault | Displaced attempts | Expected replay |
| --- | --- | --- | --- |
| `jm-replacement` | Deletes the JobManager Pod | Every attempt before the fault | The replay cohort's observations |
| `tm-replacement` | Deletes the TaskManager whose attempts processed most of the replay cohort | The attempts that Pod announced | The replay cohort's observations by those attempts |
| `rescale-out`, `rescale-in` | Patches the job arguments, job parallelism and TaskManager replicas to the recovery manifest's, a savepoint upgrade | Every attempt before the fault | None: the savepoint completes, and acknowledges what it covers, before the job stops |

Each attempt is mapped to its Pod through the `pubsub-attempt` line the application logs when it initializes; an attempt announced by two Pods, or by another run, stops the trial.
A TaskManager's loss may restart only the failover region on that Pod: in the DataStream topology each subtask's source, observer and sink form one region, so the other TaskManager's attempt keeps running without restoring anything, and its part of the replay cohort is acknowledged by the next checkpoint instead of redelivered.
The expected replay population is therefore the cohort's observations by the displaced attempts; a wider restart redelivers more, which the recovery outcome counts as `extra_replay` rather than as a failure.

Recovery is proven by the job's restoration after the fault, read from its checkpoint history, and by restored attempts announcing themselves in the logs; output is not required yet, because after a savepoint nothing is expected again and the last cohort is only requested once recovery is proven.
A replacement must restore a checkpoint under the run's state prefix no older than the retained one, with a new JobManager or with the deleted TaskManager gone.
A rescale must restore the savepoint its upgrade took, under the run's savepoint prefix, once the Operator reports the new generation deployed and the application matches the recovery manifest.
When a replacement restores exactly the retained checkpoint, no checkpoint completed between the replay cohort's processing and the fault, the boundary held, and every expected observation must reappear from a new attempt within the stage's deadline; otherwise the trial stops.
When it restores a later one, the boundary was lost, and the outcome records the replay as `unobserved` without requiring it.

The `recovery` outcome records the restored checkpoint, whether the boundary held, the expected, replayed and extra replay counts, whether each replayed observation kept its input message ID, the first output by a restored attempt seen by the time recovery is proven, which a rescale rarely has and the restored attempts.
The `fault` outcome records the retained checkpoint, the latest completed checkpoint when the fault was decided, the replay cohort's publication marks, the deleted Pod or the upgraded generation, and the attempts before the fault and those it displaced.
The redelivery's timing is read from these marks and from the output observations; shutdown is the last output observed before the fault.

An eviction, preemption, node shutdown or container restart at any stage, or a Pod replacement outside `recovering`, stops the trial as inconclusive, as in the other recovery exercises, so a Spot TaskManager reclaimed mid-trial ends it.
The supervisor records the exercise's completion as its own success; the final receipt reports success only when the exercise's [verdict](#verdict) is `usable`.
Synthetic tests run each trial through the supervisor's loop over a simulated relay, Operator and service, including a lost boundary, a population that never returns, a replay under new input message IDs, foreign output, connector metrics that never appear, a checkpoint outside the run's state, an unplanned replacement and a wrong savepoint; they do not establish real-service redelivery, failover scope or timing.

### Verdict

When the last stage completes, the exercise decides the trial's verdict over the `complete` record it is about to write, so the verdict reaches the `recovery-complete` evidence as well as the run control the final receipt reads.
The record adds the output oracle's account of every collected line, the observation coverage per window and the Pod each attempt was announced by.
[`pubsub.verdict`](../../../tools/tier3/src/flink_tier3/pubsub/verdict.py) returns `usable` with no reasons only when all of the following hold, and `inconclusive` with a reason for each shortfall otherwise.

| Condition | Reason when it fails |
| --- | --- |
| The stage is `complete`, with the `checkpoint`, `fault`, `recovery` and `after` outcomes | `recovery-incomplete`, `missing-<outcome>` |
| The [oracle](#offline-output-reconciliation) accepted every collected line | `oracle-missing`, `oracle-rejected` |
| The oracle found every logical input of both subscriptions | `oracle-incomplete` |
| A replacement restored the retained checkpoint and saw its whole expected replay, which is not empty, from new attempts | `replay-unobserved`, `replay-empty`, `replay-incomplete` |
| Each replayed observation came under an input message ID the input was processed under before the fault, asked when recovery is proven and again over everything collected by the end | `replay-ids-not-preserved` |
| A rescale's recovery outcome expects no replay | `replay-unexpected` |
| A rescale saw no replay-cohort output from the attempts it started, by the end | `replay-after-savepoint` |
| The fault names one of the four trials | `unknown-trial` |
| Each family below was read in a sample before the fault and in one after recovery | `unsampled-<window>`, `unobserved-<family>-in-<window>` |

A lost boundary completes the trial, and its verdict is `inconclusive` with `replay-unobserved`: the job restored a checkpoint that already covered the replay cohort, so the trial exercised no redelivery, which is the claim a replacement exists to carry.
A replay under new input message IDs came from a duplicate publication, not from the service redelivering what the source held leased.
The `after` outcome's `replay_by_new_attempts` counts every replay-cohort input the fault's new attempts had processed by the end, redelivered or republished alike, where recovery counted only the expected ones at its first chance.
A rescale expects none, because its savepoint acknowledges what it covers; whether the service redelivers some of it anyway, when not every acknowledgement reached it, is unmeasured, so a rescale that saw any is `inconclusive` with `replay-after-savepoint`; the deployed campaign ([#1435](https://github.com/flink-gcp/flink-connector-gcp/issues/1435)) saw none in one rescale of each direction, as its [findings](../../../docs/adr/evidence/0165-pubsub-trial-findings-1361.md#redelivery-after-a-savepoint) record, and the rule stays.
That count stops at completion, which can come before the old subscriber's extended leases expire, so a rescale found `usable` saw no such output by then, not none at all.
An input published twice may have been processed under both of its IDs before the fault, and a redelivery under either is a redelivery.

The `before` window takes the samples of `baseline`, `checkpoint` and `boundary`, because the first of them can end within a poll of the job starting, before its operators register anything; the `after` window takes those of `after`, and samples taken while recovering count toward neither.
A family counts as read only where a sample's answer returned it, a vertex's backpressure level or a finite number for a connector metric, not where it was listed or requested.
Every earlier transition also carries the coverage so far, so a trial that stops before completing still records what it read.

| Family | Read from |
| --- | --- |
| `backpressure` | A vertex's backpressure level |
| `source` | The source reader's `messagesReceived`, `messagesAcked`, `messagesNacked`, `pendingAcks`, `pendingCheckpoints`, `bufferedMessages`, `fetcherBufferedMessages`, `subscriberShutdownsAbandoned` and `subscriberFailuresUnreported` |
| `sink` | The sink writer's `inFlightMessages`, `inFlightBytes`, `activePublishers` and `publisherShutdownsAbandoned` |

The oracle's duplicate counters are reported in the record beside the verdict and decide nothing.
At-least-once delivery owes them, and neither unique Pub/Sub message IDs nor a deduplicated count can establish exactly-once output, so their absence would prove nothing either.
A `usable` verdict says nothing about ordering across the replay: the oracle is a completeness check over a set of identities.

### Offline recomputation

`flink-tier3 analyze --evidence <directory>` reads a downloaded run directory, as `gcloud storage cp --recursive` leaves `runs/<run-id>/`, and recomputes a `pubsub-recovery` run's verdict beside the BigQuery section; it contacts no service.
[`pubsub.analyze`](../../../tools/tier3/src/flink_tier3/pubsub/analyze.py) rebuilds from the evidence alone:

- the output oracle, from every batch in batch order, each appended once, with its lines derived again from the saved pull response by the function the collector used, so the batch's `observations.json` is not read;
- the input identities, from the runner's publication receipts, which bind the returned IDs to request order within the interval their path names, so each output's input message ID is checked against what was published for its input and sequence;
- the observation coverage, from the `pubsub-measurement` events;
- `replay_by_new_attempts`, from the observations and the record's fault, through the same predicate the exercise uses.

Whether the boundary held and which observations were collected before the fault are read from the `recovery-complete` record, because rebuilding them would re-run the exercise offline over the exported Pod logs and checkpoint statistics.
The verdict is recomputed with the rebuilt values, and the report places it beside the verdict the run stored, through steps the two sections [share](../../../tools/tier3/src/flink_tier3/recovery_analysis.py).

| Problem | Meaning |
| --- | --- |
| `verdict-differs-from-the-record` | The stored verdict or its reasons are not the recomputed ones |
| `unpublished-input-ids` | An output names its input under an ID no publication receipt of that input holds; the verdict is still recomputed |
| `approval-without-pubsub-trial`, `missing-result.json`, `malformed-evidence`, `malformed-message-evidence`, `missing-recovery-complete` | The evidence the verdict is decided from cannot be read; the run has no verdict |
| `missing-samples`, `missing-output-batches`, `missing-output-lines`, `missing-input-receipts` | A partial download; the run has no verdict |

A partial download is found by fewer samples in a window than the record counted there, by a gap in a collector's batch counter, by fewer exported lines than the completed record counted, or by an output of a completed run whose input has no publication receipt.
An oracle rebuilt from it would lower the verdict for what the export lost, so the run gets none; fetch the evidence again.
The analysis does not classify a disagreement as an edited record or as an inconsistent export, as for BigQuery ([#1726](https://github.com/flink-gcp/flink-connector-gcp/issues/1726)).
An unreadable `supervisor/` or `result.json` document stops the analyzer for the whole directory, as for the other sections.
The section reports the oracle's duplicate counters, and the runner's receipts beyond one per input as extra publications, apart from the verdict, and states that a usable verdict establishes neither ordering across the replay nor exactly-once output.
Tests export the evidence a simulated run writes through the real supervisor, collector and message helpers and recompute each trial, a lost boundary, an interrupted run and a redelivery after a rescale from it; edited copies show which edits the recomputation reads and which it does not.

## Input publication and output collection

[`Messages`](../../../tools/tier3/src/flink_tier3/pubsub/messages.py) supplies internal data helpers; [admission](#admission-and-effective-access) and the runner's [cohort requests](#cohort-requests) publish through them, and the supervisor's [output collector](#output-collector) pulls through them.
Construct it with the existing `ResourcePlan`, the frozen `records_per_subscription` domain, the actor's role, the shared authorized HTTP session/GCS adapter and a mandatory `before_operation(phase, method, name)` guard.
The role parameter checks routing only; the caller must bind the plan and input domain to current approval.
The guard runs before every operation; the caller verifies prepared resources and effective access, the active run, exclusive actor authority and stop conditions before each call.
A storage `PUT` callback denotes a create-only logical upload (GCS HTTP `POST`).
The guard receives phase `publish` for input intent, publication and response storage, `collect` for output intent, pull and response/observation storage, and `acknowledge` for the ACK request and receipt.
Method `PUT` always denotes storage, while `POST` always denotes a Pub/Sub data request in this helper.
The [actor handoff](#actor-ownership-and-cleanup-handoff) composes these helpers with prepared run control; the helpers alone neither hand off actors nor implement the cleanup quiescence barrier.

The runner calls `publish(input_index, start, count)` for one interval of at most 1,000 logical inputs on one of the two planned input topics.
It stores the exact request and run/nonce/domain binding in `runs/<run-id>/pubsub/messages/<nonce>/input/<index>/<start>-<count>/intent.json` with generation-match zero before publishing.
An existing intent refuses that interval, including after a restart, a lost intent-write response or an ambiguous publication response.
A successful service response is saved as `response.json` before returning its IDs; the [publish API](https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/projects.topics/publish) binds returned IDs to request order.
The helper rejects missing, empty or repeated IDs within the batch.
A missing receipt means the outcome is unknown, not that the service accepted no messages.
The caller must freeze disjoint phase cohorts, because overlapping intervals would publish duplicate logical inputs; the [cohort ranges](#offline-trial-proposal) are disjoint by construction.

The supervisor calls `collect(batch_id, max_messages=100)` for one pull from the planned output subscription.
The pull sets `returnImmediately`, so an idle subscription answers at once: the service may otherwise hold the request until messages arrive, past the 20-second transport timeout, and a pull without a definite answer would keep the call's marker and the lock.
The service may then answer empty even while messages wait, which costs a poll's delay and nothing else.
It creates an intent before the pull and saves the decoded response before interpreting its received-message envelopes.
It then writes `observations.json`, containing every output message ID and full payload as the offline oracle's unpadded base64url TSV, before sending any acknowledgement.
Append each batch's `tsv` value once when assembling the oracle input; preserve repeated lines and repeated deliveries across different batches.
It decodes both standard and URL-safe Base64, with or without padding, following the [ProtoJSON bytes format](https://protobuf.dev/programming-guides/json/#representation-of-each-type), and emits canonical unpadded base64url in the TSV.
The collector preserves foreign, empty and binary payloads for the oracle to reject; it does not validate relay semantics, filter logical identities or deduplicate messages.
Malformed envelopes, invalid encoding or exceeded local limits retain the response and stop without acknowledgement.
Malformed JSON, transport failure or a response over the transport cap leaves the intent but may have no response receipt.

An evidence-write failure, including a committed write with a lost response, prevents the following acknowledgement.
An ambiguous ACK retains the observations; another pull requires a new batch ID and may record the same output again.
A successful ACK response is followed by a create-only `acknowledged.json` receipt; its absence leaves ACK outcome unknown.
The [acknowledge API](https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions/acknowledge) permits expired IDs and repeated acknowledgements, so this receipt records a successful API response without promising no redelivery.
An [empty pull](https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions/pull) is saved without sending ACKs and is not evidence of a drained subscription.
No API operation or evidence upload is retried by the helper, and no existing evidence is overwritten or removed.
The caller must preserve intents and evidence through assessment and must not reuse a run's batch paths after deleting them.

A publication admits at most 1,000 messages and a collection at most 100, each with a streamed response of 1 MiB, an output payload of 4096 decoded bytes, a message ID of 1024 UTF-8 bytes and an ACK ID of 4096 UTF-8 bytes.
These are local collection limits, not Pub/Sub service limits; decoded response JSON and serialized GCS evidence can be larger than the wire representation.
Each publication makes two logical GCS writes and one Pub/Sub request, and each nonempty collection four logical GCS writes and two Pub/Sub requests (an empty collection three writes and one request).
The shared 20-second HTTP timeout applies to transport waits, not total trial elapsed time; the response byte cap does not bound service billing or an integrated trial's storage and request counts.
Before cleanup, the caller must stop and fence both actors and prove their in-flight requests quiescent, even if a helper has returned a timeout.
Synthetic tests establish request/evidence ordering and refusal behavior; they do not establish effective IAM access, service ACK behavior or deployed recovery acceptance.

### Cohort requests

Only the runner holds the publisher grant on the input topics, so the [cohorts](#offline-trial-proposal) after the first are published by the runner at the supervisor's request, through [`PubSubHandoff`](../../../tools/tier3/src/flink_tier3/pubsub/handoff.py).
The run control's Pub/Sub portion keeps one entry per cohort under `cohorts`, with its deadline and the times it was requested, started and published.

- **`request_cohort(name, deadline=…)`**: the supervisor asks for `after_checkpoint` or `after_recovery`, only once the cohort before it is published, while the run is `RUNNING` and its message traffic is admitted, which lasts until `cleanup_at` rather than the 900-second admission window, and with a deadline after now and no later than `cleanup_at`. Repeating a recorded request with the same deadline changes nothing, even after that deadline or a stop, so a retry after a lost response confirms it; another deadline is refused.
- **`serve()`**: the runner publishes the first requested cohort that has not started, and returns its name, or `None` when nothing waits. `Runner.settle()` calls it on each poll.
- **`publish_cohort(name, deadline=…)`**: the runner publishes one cohort to both input topics in batches of at most 1,000. Admission calls it for `before_checkpoint` with the admission deadline; `serve()` calls it for a requested cohort, which takes no deadline argument and uses the recorded one.

A cohort's start is recorded before its first publication request, so `started_at` precedes every message of the cohort, and a cohort that started is never started again.
A batch that fails stops the run through the handoff's usual failure path, leaving the cohort started but unpublished.
The start is refused, and nothing recorded, once less than the guard's 20-second request budget is left before the deadline, because the guard would refuse every batch after it; each batch's own request keeps that budget too.
The request and the two marks are one control update each, and each batch is a `publish` call with its own markers.
The start and end marks are read from the runner's clock and the request time from the supervisor's, so an ordering across the two holds only up to the skew between those hosts.

### Output collector

[`OutputCollector`](../../../tools/tier3/src/flink_tier3/pubsub/output.py) pulls the output subscription through the supervisor's handoff, so every pull is a marked call and leaves the evidence `collect` writes.
`pull()` takes one batch of up to 100 under the next batch ID: `out-`, eight random hexadecimal digits fixed for the collector, and a counter, so that two collectors name the same create-only evidence only if those digits collide.
`drain(max_pulls)` repeats it until a batch comes back empty or the pulls are spent; an empty batch ends the round without proving the subscription empty.
A short batch does not end it, because an immediate pull returns what the service has ready at once rather than the backlog; the exercise drains at most 20 pulls a poll, so that at the batch sizes measured so far one poll can collect the replay cohort's output before the next checkpoint, as [ADR-0165](../../../docs/adr/0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md) records under [#1720](https://github.com/flink-gcp/flink-connector-gcp/issues/1720).
Each collected line whose payload is this run's relay output, with the nine fields of [the payload](#payload-and-restoration) in canonical form, becomes an observation: input index and sequence, input message ID, attempt, observation ID, phase and whether the attempt restored state, with the output message ID, batch and collection time.
Any other line, including another run's, is kept apart by batch and stays in the evidence, and the [verdict](#verdict)'s oracle refuses it.
Every collected line is also kept in collection order, repeats included, as the oracle's input for the [verdict](#verdict).
The observations live in the supervisor's process for the exercise.

## Actor ownership and cleanup handoff

[`PubSubHandoff`](../../../tools/tier3/src/flink_tier3/pubsub/handoff.py) wraps the resource controller and the message helpers in a durable actor protocol.
The original runner constructs it from its `PubSubLifecycle` and a fresh 32-character lowercase hexadecimal process token, then calls `initialize()` before resource preparation.
Initialize through `PubSubHandoff.initialize()` without first calling the underlying resource controller's initializer.
This handoff entry point writes resource intent and runner authority together in one generation-checked control update; a crash cannot commit resource intent without its actor binding.
If the write did not commit, common cleanup sees no Pub/Sub state; if it committed but the acknowledgement was lost, the original process may repeat initialization with its same token while admission remains open.
If that process died, its binding stays unreleased, and common cleanup keeps the resources and the lock for an operator.
Competing initializations and shared stop are rechecked after a generation conflict; neither a retry nor cleanup adopts a different runner token.
`prepare()` records one runner invocation before creating resources and clears it once the stage is `prepared`.
After preparation and transition to `READY`, the supervisor constructs its own instance with a distinct fresh token and calls `join()` before collection; joining during `RUNNING` is also permitted while admission is open.
The submitting process may repeat its binding call idempotently while that admission window remains open, but another process must not reconstruct or transfer its recorded token.
Tokens identify process ownership; exclusive resource control remains the caller's responsibility.
Use the wrapper from the first initialization call onward: direct resource initialization bypasses atomic actor binding, and direct preparation or message calls bypass its invocation records.

The wrapper's `publish()` and `collect()` use the existing message helpers, with the records-per-subscription domain from the approved trial.
Before each preparation, publication or collection, a conditional update records a fresh invocation ID under the actor's token.
For a publication or a collection, the same update checks that message traffic is still admitted: prepared resources, no shared or local stop or evidence failure, a time before `cleanup_at`, and `RUNNING`, or for the runner `READY` too, so admission can publish the first cohort before the application exists and the run becomes `RUNNING` with its input waiting.
That begin marker and the call's completion are its only control updates on the normal path, two per publication or pull, which cuts the contention on the shared record against Cloud Storage's rate of about one mutation a second on one object; a test holds that count.
Neither actor spaces its writes, so a burst can still exceed that rate, and the conditional update's retries remain.
A call already under way finishes after a stop: it keeps its response and observations and acknowledges what it has durably observed, and only a new call is refused.
An evidence upload that fails, including a committed write whose response was lost, latches a local evidence failure and records the shared one, which refuses new calls by either actor, and the call acknowledges nothing it had not durably observed.
If that failure cannot be recorded, the wrapper raises an explicit restart-barrier error; the caller must stop both actors independently before retrying or restarting, because another process cannot observe an unpersisted failure.
A retry of an existing interval or batch ID collides with its create-only intent, which is such an evidence failure.
One actor cannot start another call while its marker is present; runner and supervisor calls may overlap.
A successful call clears only its own marker, making up to three attempts to persist the conditional completion acknowledgement without repeating the service operation.
If the acknowledgement committed but its response was lost, the retry cannot clear a newer invocation's marker.
A failed call requests shared stop, while a failed control write may prevent that stop from reaching the other actor.
It keeps its marker unless the handoff was given a settled-write check that answers true: the production actors pass their session's, which is true while every Pub/Sub request other than a GET or a permission test that the actor's session ever sent received a status below 500 rather than a transport failure.
The record is the session's, not the call's, so one unsettled write keeps every later marker too.
Storage uploads go through another client and are not tracked; a lost upload response can at most leave its object to land after settlement, never a service resource.
Such a call then clears its own marker, so a refusal before any request, or drift found by a read, leaves the run cleanable through ordinary release; a clearing failure keeps the marker.
A begin write can also land with its response lost; the call then fails before sending anything, so it reads the record and clears that marker too, rather than leaving the lock to an operator's investigation the call does not need.
A write that left without a status, or was answered 5xx, may still complete at the service, and its marker and the lock stay for an operator, as below.
The caller must independently stop actors when shared failure/stop persistence is unavailable.
If requesting shared stop also fails, that control error propagates with the original operation error as its Python exception context; callers that record only the outer message lose that original diagnostic.
The marker does not expire, and a timeout does not prove the service operation has ended.
An original process may retry a lost successful release acknowledgement with its existing token, but an unresolved call keeps the lock for an operator.
Even one transient write failure can therefore stop both actors and keep the lock for an operator; the wrapper does not retry the failed service call.

`stop()` closes admission without declaring quiescence.
Each bound actor calls `release()` to stop admission and permanently surrender its authority once it has no unresolved call.
Normal supervisor `cleanup(quiesce)` requires all bound actors released, then requires the external barrier to prove creators, workload writers and their in-flight requests quiescent.
A supervisor that never joined may call `release()` to stop admission without recording a fictitious actor release; it has no data authority, and stop prevents it joining later.
`released()` observes whether every bound actor has released, without proving external quiescence.
When a handoff is present, the underlying resource controller also refuses service deletion before actor release, and shared Operator shutdown/final settlement enforce the same gate.
Older resource records without a handoff keep their existing external-barrier contract.

An abandoned or unresolved actor has no in-protocol reclamation: the supervisor Job replaces no Pod, so no other process exists to reclaim it.
The record is read as this rig's own code wrote it, and there is no in-protocol repair or reset for corrupt control.
Retain its evidence for investigation and a separately reviewed recovery procedure.
The complete handoff and retained invocation markers survive into the final result receipt.

Synthetic tests exercise concurrent claims, stop/release races, lost acknowledgements, partial preparation, conservative failure handling and receipt preservation.
The deployed trials of [#1361](https://github.com/flink-gcp/flink-connector-gcp/issues/1361) ran through these actors and this cleanup; their [findings](../../../docs/adr/evidence/0165-pubsub-trial-findings-1361.md) record the outcome.

## Shared settlement integration

The internal caller may attach its original runner handoff with `Runner(env, pubsub=handoff)` and its supervisor handoff with `Supervisor(env, pubsub=handoff, quiesce=barrier)`; in production, [`pubsub_actors`](#actor-construction-and-the-request-guard) makes both attachments and builds the barrier from the run.
Each handoff must belong to that exact actor environment; a supervisor attachment requires the external barrier, and a lifecycle cannot attach both BigQuery and Pub/Sub handoffs.
Admission initializes, prepares and publishes the first cohort, and the supervisor joins, through `PubSubHandoff`; the runner publishes the later cohorts from `Runner.settle()`, as [cohort requests](#cohort-requests) describes.

A runner that settles with `request_stop=True`, or that is already stopping, releases before it waits, as before.
Otherwise it keeps its authority while it waits, serving cohort requests on each poll, and releases once a stop or evidence failure is recorded, the supervisor's Job ends or is lost, the run leaves `RUNNING`, `cleanup_at` passes or a publication fails; the last is recorded as `pubsub-publication-failed`.
A stop that lands while a requested cohort is still being published refuses its next batch, which is such a failure and withholds success; a stop that lands before the runner has recorded a requested cohort's start, including one between its poll and that record, only closes serving, so the cohort stays unpublished without a failure, and the run's verdict is the supervisor's.
Release closes shared admission, so the supervisor collects no further output after it.
There is no circular wait with the supervisor's cleanup: that cleanup records the shared stop before it waits for the runner's release, and the runner releases on the next poll that sees the stop.
A supervisor whose Job ends before it cleaned also releases the runner, whose settlement then finds the Pub/Sub resources undeleted and keeps the lock, because only a supervisor deletes them.
It retries an unsuccessful release during subsequent settlement polls and once after the wait.
`pubsub-release-blocked` records each changed failure cause; a release failure latches local stop and prevents that runner's settlement from recording success even if a later acknowledgement succeeds.
No retry repeats publication or reclaims an unresolved invocation.
Failure to persist stop or evidence still requires the independent stop path described above.

Common cleanup first stops admission and removes the owned Flink workload while the Operator is running.
It then releases the supervisor, waits for all bound releases and an exactly-`True` external barrier, and rechecks the barrier inside resource cleanup before deletion.
Only recorded service cleanup permits checkpoint/state deletion, Operator shutdown and shared completion.
While service cleanup is unfinished, an unresolved call, missing supervisor attachment or failed service deletion keeps common cleanup from deleting temporary state or stopping the Operator.
Common cleanup never takes over another process's actor.

Direct `Records.set_phase(CLEANED)`, `Records.settled()` and `verify_idle()` now enforce the same Pub/Sub clean-state gate as final receipt creation.
Records without Pub/Sub state retain their prior behavior.
Cleanup routes Pub/Sub resources to `tier3-pubsub` and temporary state to `flink-gcp-tier3-pubsub`; existing smoke, Cloud Tasks and BigQuery inventory scopes remain unchanged.
The production attachments are built by [`pubsub_actors`](#actor-construction-and-the-request-guard), and [admission](#admission-and-effective-access) uses them.
A successful synthetic cleanup is not a recovery verdict; the [verdict](#verdict) is.

## Payload and restoration

Each UTF-8 input payload is `v1|<run-id>|<input-index>|<sequence>` with canonical decimal numbers and sequence in `[0, records-per-subscription)`.
Both entry points apply one input check, which rejects foreign runs, wrong subscriptions, invalid UTF-8, payloads over 128 bytes and absent service message IDs.
It does not discard or deduplicate replayed input.
The logical count is not a cap on service deliveries, retries, bytes billed, output publications or elapsed time.
Those require separately approved execution ceilings and an independent stop path.

On the DataStream entry point, the source, observer and sink have fixed UIDs `pubsub-input-v1`, `pubsub-observer-v1` and `pubsub-output-v1`.
The `main` method fixes maximum parallelism at 128 and enables checkpoints every 120 seconds with at most one in flight; the CUE configuration sets the same interval.
The interval leaves the [recovery exercise](#recovery-exercise) time to publish, process and observe a cohort between two checkpoints, because the supervisor and the runner each act once per 15-second poll.

The Table entry point reads both subscriptions through one `pubsub` table with the `raw` format and the `message-id` and `subscription` metadata columns.
It converts that table to a DataStream for the input check (`pubsub-table-tagger-v1`) and the observer (`pubsub-table-observer-v1`), then writes the observations through the Table sink into a single `STRING` column, so both entry points publish the same payload bytes.
Outside a persisted compiled plan, the planner assigns explicit UIDs to its own operators only under `table.exec.uid.generation: ALWAYS`, and its default UID format numbers them from a JVM-wide counter, which differs between translations.
The entry point therefore sets `ALWAYS` with a fixed format for each of its two translations, `pubsub-table-input-v1-<type>-<transformation>` and `pubsub-table-output-v1-<type>-<transformation>`.
A repeated UID within one translation fails job graph generation with a hash collision rather than sharing state.

The observer keeps version/project/run ID/input-domain identity in union operator state, accepting rescaling but refusing missing or incompatible restored identity.
The two entry points share no operator UID, so with `allowNonRestoredState: false` a savepoint written by one entry point does not restore the other: Flink refuses its unclaimed state before either observer runs.
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
It reconciles either entry point's output, since both write the same payload.

| Counter | Counted population |
| --- | --- |
| `logical_inputs` | Distinct input-index/sequence pairs; must equal twice the configured per-subscription count |
| `input_publication_duplicates` | Extra input service message IDs for the same logical input, scoped by input topic |
| `repeated_input_processing` | Extra processing observation UUIDs for the same input service message ID |
| `output_publication_duplicates` | Extra output service message IDs carrying the same processing observation UUID |
| `repeated_output_delivery` | Repeated observations of the same output service message ID |

The collector must preserve output message IDs: discarding them would conflate its own redelivery with duplicate sink publication.
Completeness alone does not establish recovery, exactly-once processing/output, strict replay ordering or a checkpoint-confirmed fault boundary.
The [recovery exercise](#recovery-exercise) retains a completed checkpoint, publishes a separately identified post-checkpoint cohort, proves that no later checkpoint completed before the fault, and, when the boundary held, requires the share of that cohort the fault displaced to reappear from new attempts after restore, recording whether each replay kept its input message ID; it also observes continued progress and a later completed checkpoint.
The exercise's [verdict](#verdict) applies these rules through a Python port, because the supervisor's image carries no JVM; the emulator integration test keeps using `PubSubRecoveryReport`.
The port returns the same five counters, and both test suites decide the cases in [`oracle-cases.txt`](src/test/resources/oracle-cases.txt), so a rule changed on one side only fails the other's run of them.
Those cases pin the report's own rejection messages; where the JDK writes the message instead, as for an unparseable number, UUID or Base64 field, the port words it differently, and the cases require only that both refuse.
The Java tool's 64 MiB file bound and each side's line-reader checks are outside the shared cases.
Instead of refusing a run with missing inputs, the port reports how many are missing, so that the verdict can tell a refused line from an incomplete set.
The [offline recomputation](#offline-recomputation) uses the same port over the exported batches; deployed trials of either entry point remain on the parent issue.

### Internal approval contract

[Dispatch](#approval-dispatch) builds a `pubsub-recovery` approval and validates it once, before it takes the lock.
Its `pubsub_trial` is the reviewed trial file's, in the [offline proposal schema](#offline-trial-proposal), version 5, from which the input plan derives the trial's input domain.
The approval pins the run/nonce, the rig commit, initial and recovery manifest hashes, exactly the Operator, lifecycle-tools and Pub/Sub application image roles, and observed idle namespace/quota UIDs for `tier3-pubsub` and `tier3-system`.
It requires an integral one-hour window with fifteen minutes reserved for cleanup.
Nothing validates it again: admission checks only its window, and recovery reads it as written, so a later policy change cannot strand a held run's recovery.
These checks do not verify live provenance or establish a recovery verdict.

Admission's temporary quotas admit four equal Flink shapes in `tier3-pubsub` and three control shapes in `tier3-system`, and no PVCs.
The fourth application slot covers a replacement while its predecessor terminates; the third control slot covers an Operator replacement.
The rig counts neither Pods nor state bytes beside those quotas; cleanup lists at most 10,000 state objects and restores the recorded idle quotas.
Message evidence has no separate cap; its size follows from the input domain and the exercise's pulls.

`Approval.pubsub_plan` and `PubSubLifecycle` derive service names and grants from the approval's identity.
Final settlement preserves `pubsub_trial` and the recovery observations even before service intent exists, and derives success only from the exercise's `usable` [verdict](#verdict).
A finalization retry keeps the original receipt; the current plans must still prove the same nonce, all three foundation roots and an empty result.

The [proposal's estimate](#offline-trial-proposal) is what the owner approves before dispatch.
The runner admits the trial and the supervisor joins it, as [admission](#admission-and-effective-access) describes, the supervisor runs the [recovery exercise](#recovery-exercise), and [dispatch](#approval-dispatch) starts it.

## Approval dispatch

The [run workflow](../../../.github/workflows/tier3-run.yaml) accepts `pubsub-recovery`, builds its approval and runs the trial.
The scenario takes two inputs beyond the common ones.

| Input | Contract |
| --- | --- |
| `pubsub_trial` | A reviewed trial file under [`kubernetes/lifecycle/pubsub-trials/`](../../lifecycle/README.md#reviewed-pubsub-trials), named without `.toml`; an empty name is refused |
| `application_digest` | The published `pubsub-recovery` GAR digest, verified live at dispatch and never pinned |

A reviewed trial file holds the [offline proposal schema](#offline-trial-proposal) as TOML beside its licence header, so the file the dispatch names is the one reviewed at the dispatched commit, which the workflow checks out fresh.
The offline renderer keeps its JSON `--trial-file` input for proposals that nobody has approved.
The owner approves the trial's estimate before dispatching it; dispatching is the approval.
The four reviewed trials are the campaign [#1361](https://github.com/flink-gcp/flink-connector-gcp/issues/1361) runs; the example the tests use lives under `tools/tier3/tests/fixtures/`, where no dispatch can name it.

The window starts when dispatch admits the run, on the whole second, and lasts exactly one hour, so queueing before the job starts costs the run none of its startup budget.

The checks that need neither the cluster nor the lock run first: the trial file and digest, the dispatch from `main`, the rig commit (the dispatched `main` commit unless `rig_sha` names another), the run ID and an existing run's evidence.
Dispatch then snapshots the idle foundation for `tier3-pubsub`, renders and verifies the proposal, takes live image receipts, assembles and validates the approval and binds the rendered delivery with the approval embedded.
The proposal names its second manifest the recovery application; the approval pins that manifest as `upgrade_application_sha256`, the name every service scenario shares, and takes the supervisor image from the proposal's `images`, as a BigQuery approval does.

Dispatch also confirms that it may create Pods in `tier3-pubsub`, which admission's access probe needs.
It then acquires the environment lock, rechecks the idle foundation, stores the approval, both manifests and the image receipts as the run's documents, writes the control record and builds the runner through `pubsub_actors.runner()`, as a BigQuery dispatch does.
The runner starts the run with the bound ConfigMap, supervisor Job, application and access probe, and settles it; a runner that cannot be built leaves the lock to a plain runner's settlement.
The workflow job keeps the default 85-minute limit, because the window is one hour, as for smoke.
Enabling the scenario does not approve a run.
