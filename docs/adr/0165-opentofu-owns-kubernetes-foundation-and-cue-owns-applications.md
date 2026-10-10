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

# ADR-0165: OpenTofu owns Kubernetes foundation and CUE owns applications

- Status: Accepted
- Date: 2026-09-11; revised by [#1312](https://github.com/flink-gcp/flink-connector-gcp/issues/1312) (2026-09-20)
- Updated: 2026-09-12 (CI ownership, initial CRD apply recovery and idle Helm root)
- Updated: 2026-09-13 (digest-pinned GAR publication and seven-day image expiry)
- Updated: 2026-09-14 (generic stateful smoke artifact and workload storage identity)
- Updated: 2026-09-14 (published smoke digest and concrete initial/upgrade deliveries)
- Updated: 2026-09-14 (idle lifecycle identities, evidence storage and Operator resource limits)
- Updated: 2026-09-14 (bounded lifecycle, shared environment coordination and recovery)
- Updated: 2026-09-15 (runner admission, typed lifecycle package, reviewed policy and SDK transport simplification)
- Updated: 2026-09-15 (uv workspace member, installed CLI and package source delivery)
- Updated: 2026-09-16 (one bounded generic recovery exercise and phase-specific oracles)
- Updated: 2026-09-17 (idle Cloud Tasks namespace and workload identity)
- Updated: 2026-09-18 (Cloud Tasks lifecycle control grants and Operator watch set)
- Updated: 2026-09-19 (Cloud Tasks session admission, queue lifecycle and storage-backed evidence rows)
- Updated: 2026-09-19 (session evidence export, per-poll observations and offline analysis)
- Updated: 2026-09-20 (idle BigQuery foundation, Operator watch set, finite recovery application, publication wiring, observations, offline oracle, deployment package, resource adapter, durable resource controller and offline execution proposal)
- Updated: 2026-09-20 (Pub/Sub recovery identity and isolated state storage)
- Updated: 2026-09-20 (idle Pub/Sub namespace, workload identity and Operator watch set)
- Updated: 2026-09-20 (Pub/Sub application image publication wiring)
- Updated: 2026-09-20 (Pub/Sub deployment package and completed publication)
- Updated: 2026-09-20 (owned Pub/Sub resource operations and partial-creation cleanup)
- Updated: 2026-09-20 (Pub/Sub lifecycle authority and recorded resource policies)
- Updated: 2026-09-20 (durable Pub/Sub preparation and service-cleanup settlement gates)
- Updated: 2026-09-21 (Pub/Sub actor release connected to common settlement)
- Updated: 2026-09-21 (offline Pub/Sub trial proposals)
- Updated: 2026-09-23 (spend approved from the pre-run estimate; run-time cost gates removed)
- Updated: 2026-09-23 (BigQuery trial preregistration)
- Updated: 2026-09-24 (dispatch of a chosen rig commit; capacity gate)
- Updated: 2026-09-25 (one-node capacity gate removed)
- Updated: 2026-09-25 (BigQuery deployed trial findings)
- Updated: 2026-10-03 (Pub/Sub admission building blocks: actor construction, per-method operation bounds, settled failures)
- Updated: 2026-10-03 (Pub/Sub runner admission, effective-access probes and supervisor participation)
- Updated: 2026-10-03 (Pub/Sub cohorts the supervisor requests and the runner publishes, and output collection)
- Updated: 2026-10-04 (supervised Pub/Sub recovery exercise and 120-second application checkpoints)
- Updated: 2026-10-04 (checkout-only modules kept out of the delivered package)
- Updated: 2026-10-04 (Pub/Sub recovery verdict decided by the supervisor)
- Updated: 2026-10-04 (uniform lifecycle scenario files and reviewed-trial instructions)
- Updated: 2026-10-04 (service subpackages and nested source delivery)
- Updated: 2026-10-04 (Pub/Sub recovery verdict recomputed from exported evidence)
- Updated: 2026-10-10 (BigQuery FILE_LOADS trial preregistration; unredacted supervisor job listing)
- Updated: 2026-10-10 (BigQuery FILE_LOADS deployed trial findings)
- Updated: 2026-10-10 (generic exercise evidence recorded; table deletion granted to the supervisor alone)
- Issues: [#38](https://github.com/flink-gcp/flink-connector-gcp/issues/38), [#1307](https://github.com/flink-gcp/flink-connector-gcp/issues/1307), [#1308](https://github.com/flink-gcp/flink-connector-gcp/issues/1308), [#1246](https://github.com/flink-gcp/flink-connector-gcp/issues/1246), [#1312](https://github.com/flink-gcp/flink-connector-gcp/issues/1312), [#1313](https://github.com/flink-gcp/flink-connector-gcp/issues/1313), [#1549](https://github.com/flink-gcp/flink-connector-gcp/issues/1549), [#1550](https://github.com/flink-gcp/flink-connector-gcp/issues/1550), [#1551](https://github.com/flink-gcp/flink-connector-gcp/issues/1551), [#1552](https://github.com/flink-gcp/flink-connector-gcp/issues/1552), [#1691](https://github.com/flink-gcp/flink-connector-gcp/issues/1691)
- Evidence: [BigQuery trial preregistration](evidence/0165-bigquery-trial-preregistration-1312.md), [BigQuery trial findings](evidence/0165-bigquery-trial-findings-1312.md), [BigQuery FILE_LOADS trial preregistration](evidence/0165-bigquery-fileloads-preregistration-1313.md), [BigQuery FILE_LOADS trial findings](evidence/0165-bigquery-fileloads-findings-1313.md)
- Modules: opentofu, kubernetes, CI
- Supersedes: the CUE ownership of persistent Kubernetes resources in [ADR-0063](0063-persistent-gcp-infrastructure-is-one-tofu-root-module-applied-by-tfaction-over-wif.md#cue-manifest-management)
- Current behavior: [Bootstrap runbook](../../opentofu/tier3-bootstrap/README.md), [Operator runbook](../../opentofu/tier3-operator/README.md), [application manifests](../../kubernetes/README.md)

## Context

The static CUE foundation initially included CRD installation data and assigned every non-Helm Kubernetes object to CUE.
During authentication/bootstrap implementation, the project chose to focus CUE on application management and use OpenTofu for persistent Kubernetes resources, including Namespace, KSA and RBAC.
Initial administrator access had already created fourteen namespace, quota and installer-RBAC objects, so adoption must preserve those objects rather than recreate them.

## Decision

Keep the existing GCP root and its state responsible for GKE, GAR, GSA, IAM and GitHub WIF.
Add a CI-managed `opentofu/tier3-bootstrap` root with its own GCS state prefix for namespaces, idle quotas, installer RBAC, CRDs and persistent application KSA/RBAC.
Use standard Kubernetes provider resources for standard objects and `kubernetes_manifest` for the four CRDs.
Import the initial namespace, quota and access objects, including the additional bootstrap writer grants, inspect the first plan for metadata changes and preserve their identities.
Protect namespaces, quotas and CRDs from planned destruction and wait for CRDs to become Established before the dependent smoke identity.
State import alone does not establish that server-side field ownership has been safely updated; inspect the first apply and its refreshed plan.

An administrator grants initial access before CI depends on it; the root then imports those objects and owns subsequent changes.
Its tfaction marker selects it for PR plan comments and saved-plan apply on merge to main, followed by idle and no-drift checks.
The plan identity remains read-only, while the main-push apply identity manages quotas in the owned namespaces and named cluster-scoped foundation objects.
Create grants cover Namespace, CRD, ClusterRole and ClusterRoleBinding kinds because Kubernetes cannot constrain create by resource name; existing-object updates remain name-scoped.
RBAC escalation checks remain enabled.
A new authorization boundary can still require an administrator grant, but routine changes within the established boundary use CI.
CUE and schema-generation checks remain credential-free, while OpenTofu validation, formatting and TFLint run for each selected root within `tofu-plan`, after initialization and before planning.
The existing tfaction test action performs those checks with automatic fixes under the App token; a checking-only step runs when that token is unavailable.
OpenTofu checks are not added to other workflows or general lint.
Actual WIF identity/permission checks run within the bootstrap and Operator plan and apply jobs before OpenTofu executes.
Keep the manual access diagnostic command, but do not add a separate access workflow: it duplicates those checks and makes application or shared-tool changes depend on an idle cluster.
Both runners construct dedicated kubeconfigs and a temporary OpenTofu wrapper that isolates provider environment settings; no plan-time token or kubeconfig path is saved in the provider configuration.
Both identities can list CRD schemas because Kubernetes provider 3.2.1 requires that discovery read during manifest planning.
Existing GCP permissions remain additive and are not reduced by namespace RBAC.

The initial CI apply created all four CRDs, but the provider rejected API responses that omitted the chart's zero printer-column priorities and marked those instances tainted.
The CRD resource declares only the zero-priority leaf paths as computed, alongside the provider's default labels and annotations; schemas and nonzero priorities remain enforced and the distributed chart payload stays intact.
For [#1306](https://github.com/flink-gcp/flink-connector-gcp/issues/1306), permit one state-only administrator repair after backing up state and verifying live identity, Established conditions, schemas, field ownership and idle quotas.
Untaint only the four verified CRD instances with state locking enabled, then generate a fresh CI plan for the recovery PR.
Keep destruction protection and conflict detection enabled; resource changes still require reviewed CI apply after merge.
This incident does not establish automatic untaint or a local apply recovery path.

A separate `tier3-operator` root follows after bootstrap; tfaction manages its Helm release using the existing plan/apply GSAs and its own state prefix.
Helm owns Operator Deployment/configuration/KSA/RBAC and release metadata, with replicas zero, webhook disabled, skip_crds true and create_namespace false.
Its job ServiceAccount/RBAC creation is disabled because the persistent application identity belongs to bootstrap.
The chart's explicit Operator privileges remain on the installer before it creates/binds chart Roles, avoiding unrestricted bind or escalate.

The Helm root pins the distributed 1.15.0 chart independently of the CRD/schema pin, so updating bootstrap does not simultaneously advance Helm.
Both runners verify the archive SHA-512 and render the same local archive that the provider consumes.
The initial inventory is seven namespaced resources plus Helm release Secrets; the chart creates no job identity, webhook, certificate or hook workload.
The chart's default image tag is a commit abbreviation; the idle Deployment selects the published GAR digest for Operator 1.15.0.
GAR publication and digest selection remain prerequisites to later Pod admission.
Helm provider 3.3.0 reads its kubeconfig from the execution-time environment, using the shared wrapper and a fixed context, with no saved token or runner path.
The plan job retains the rendered resources for review; after saved-plan apply, CI checks the deployed release, live configuration/RBAC, idle inventory and an empty refreshed Helm plan.
Do not use replicas drift suppression or local apply to implement deliberate scale-up in this stage.

CUE owns application deliveries: FlinkDeployments and application-specific configuration, Services, Deployments and Jobs.
Generated application definitions remain in gen/, standard packages in pkg/, and delivery.resources is rendered by cli_tool.cue in Kind/key order.
CI renders each ordinary `runs/` delivery separately with real source inputs and no placeholder values; `ci.cue` remains absent.
The parameterized lifecycle delivery receives synthetic dispatch inputs in the same discovery check.
Complete CRD installation YAML moves to the bootstrap root; the same checksum-pinned chart produces both that YAML and CUE validation definitions.
CRD upgrades precede Helm upgrades and ordinary run cleanup preserves the foundation.

Future GCP-using applications receive a dedicated workload identity: the KSA annotation belongs to bootstrap, while its GSA impersonation/data grants belong to the GCP root.
The generic smoke KSA is annotated for its dedicated `tier3-smoke` GSA, whose bucket grant and KSA impersonation trust belong to the GCP root.
Installer identities must not become application identities.

The generic application for [#1309](https://github.com/flink-gcp/flink-connector-gcp/issues/1309) uses a deterministic, rate-limited sequence and four keyed counters with stable operator UIDs.
Its operator state preserves the run ID, lineage and progress so later recovery checks can distinguish a restored job from a fresh restart.
It fixes parallelism at one; connector and rescaling scenarios remain separate.
The opt-in Maven profile builds a thin application JAR and retains Datagen as an unchanged Apache JAR in the image.
The connector release reactor does not include this application.

Its STANDARD GCS bucket is regional, uses uniform access and public access prevention, and disables soft delete and versioning.
The workload receives `roles/storage.objectUser` on that bucket, which permits obsolete checkpoint deletion without granting object IAM or retention changes.
Checkpoint, savepoint and Kubernetes HA paths are separated beneath each run ID.
One-day object expiry bounds retained state after explicit cleanup; it neither preserves permanent evidence nor stops Pods.
The application definition retains Spot placement for the TaskManager, one JM/TM with bounded resources and savepoint upgrades that reject discarded state.
The committed `runs/generic-smoke/initial` and `runs/generic-smoke/upgrade` deliveries target the same resource sequentially and consume the published `images.smoke` digest.
Their shared inputs select a concrete run ID and expiry; each directory renders independently without injected tags.
The [application runbook](../../kubernetes/apps/smoke/README.md#deployment-and-storage) records the execution window and the required review of fresh inputs when execution is postponed.
These definitions do not admit, schedule or terminate workloads; lifecycle supervision and GKE execution remain separate work.

### Image publication

The GCP root owns a separate Tier-3 image publisher with repository-scoped Artifact Registry Writer access.
Its WIF binding selects the exact main-only manual publication workflow and repository ID; the provider condition checks the repository and owner IDs.
The workflow copies fixed AMD64 Operator and Flink images with crane and builds the Python/kubectl runtime with Docker's BuildKit actions.
It also builds the smoke application over the reviewed Flink base, with its JARs and GCS plugin available before Pod startup.
The workflow and Dockerfile hold the source pins; metadata-action supplies the built image's tags and labels.
Registry tools handle digest-addressed content, so publication needs no custom content inspector, intermediate JSON receipts or capacity-admission implementation.
The fixed image inventory, serialized manual workflow and 30-minute timeout define the publication scope as recorded in the [image runbook](../../kubernetes/images/README.md).
The supervisor environment does not implement workload lifecycle behavior; that remains the lifecycle issue's responsibility.

All GAR versions, including the currently selected digests, become deletion-eligible seven days after creation.
There is no keep rule, immutable tags remain disabled and vulnerability scanning is explicitly disabled to preserve the cost boundary.
This resting state was chosen over retaining current and previous versions because the rig runs intermittently.
The publisher cannot delete image versions or change infrastructure.
The cleanup service runs asynchronously, so deletion eligibility is not a precise deletion deadline or a storage quota.
Repeated publication of an existing digest does not establish a renewed retention period.
The later lifecycle preflight must check live image existence and sufficient remaining retention for the run and cleanup margin.

Publication code and IAM land before the first manual publication.
The successful workflow's job summary supplies digest references for a reviewed Helm/CUE pin change.
The first publication completed on 2026-09-13 and supplied the initial Operator, Flink and lifecycle-tools references.
The first smoke publication subsequently built main commit `053e23835782059830f871ca53f90fba43efa324` after the workload identity applies and empty refreshed plans; a GAR read confirmed the selected application digest.
The [SDK publication](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/34844507450) subsequently built main commit `abf35f5a16e50be11432788b602bdf8357f9e8ad` on 2026-09-14.
A GAR read confirmed its lifecycle-tools digest, which the CUE images package selected until the Cloud Tasks measurement publication; the Operator, Flink and smoke pins remained unchanged by that adoption.
The [Cloud Tasks measurement publication](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/35474834488) then built main commit `339d0a90675dffee74305cebe021093c6a1f9b4b` on 2026-09-19 UTC and republished the lifecycle-tools and smoke images alongside the measurement applications, so the images package selected that pair until later publications replaced it, as the [image runbook](../../kubernetes/images/README.md) records each one, and the Operator and Flink pins remained unchanged.
The idle Helm release retains zero replicas and quotas while adopting the GAR reference.
Publication records GAR image references without starting workload Pods; it does not establish GKE runtime behavior.

## Consequences

### Lifecycle foundation

Prepare lifecycle admission in two stages for [#1310](https://github.com/flink-gcp/flink-connector-gcp/issues/1310).
The first stage establishes persistent identities, storage and Operator resource requests while replicas and Pod/PVC quotas remain zero.
Apply it through the three existing CI-managed roots and require idle inventory and empty refreshed plans before implementing the dependent runner and supervisor.

Use a dedicated main-workflow runner GSA and a separate GKE supervisor GSA/KSA.
Place the supervisor KSA, Job and source ConfigMap in `tier3-system`.
The smoke KSA creates Pods and Deployments only in `tier3-smoke`; keeping the supervisor in the other namespace prevents those workloads from selecting its more privileged identity.
Their namespaced lifecycle Roles permit smoke admission and owned workload cleanup, while system writes cover supervisor Jobs/ConfigMaps, the existing Operator scale target and quota, plus Pod deletion for cleanup.
Accept the runner and supervisor as trusted Operator administrators: a Job they create in `tier3-system` can select the chart's `flink-operator` KSA and reach its smoke-namespace permissions, including Secret access and Pod creation.
The direct lifecycle Role grants therefore do not isolate those identities from the Operator's authority; the namespace boundary isolates the smoke workload from these management identities.
RBAC cannot constrain dynamically named Pod deletion by label or owner UID; the later lifecycle implementation must enforce that ownership check.
The existing bootstrap reader binding supplies only shared foundation metadata reads.
The installer receives its new Job, log/proxy and system cleanup/scale permissions through a reviewed additive administrator grant before it delegates the Roles in CI.
Do not add unrestricted bind, escalate or impersonate.

Store durable evidence in a separate regional STANDARD bucket.
Runtime identities can append and read `runs/` evidence but cannot overwrite or delete it; those objects become deletion-eligible after 30 days.
Keep mutable lifecycle records in `_control/`, outside automatic expiry.
The plan identity's additional object-write permission names only the environment lock, extending its existing state-lock exception without adding infrastructure mutation rights.
That identity and the runtime control writers can still update or delete the lock; its absence is not proof of a clean environment.
Admission must check durable run records and live inventory before acquiring a new lock.
The later runner must remove completed control records and coordinate infrastructure operations through conditional object updates.
The pinned actionlint rejects GitHub's newer `concurrency.queue` field, so that syntax is not the coordination mechanism.

The Operator container receives explicit requests and limits of 1 CPU, 2 GiB memory and 1 GiB ephemeral storage in its idle Helm values.
The [foundation runbook](../../opentofu/tier3-bootstrap/README.md#lifecycle-foundation) records the exact grants and apply boundary.
This stage does not implement or validate workload admission, supervision, shared locking, teardown or the separately approved [#1311](https://github.com/flink-gcp/flink-connector-gcp/issues/1311) execution.

### Bounded lifecycle implementation

The foundation was merged and applied before the dependent runtime implementation, with idle inventory and empty refreshed plans across the three roots.
The [lifecycle runbook](../../kubernetes/lifecycle/README.md) defines the fixed resource/cost approval, explicit main dispatch, ownership checks and recovery procedure.
Keep shared approval, records, transport and cleanup in the `flink_tier3` package under `tools/tier3/src/`, with fixed environment/resource/cost values in its reviewed `policy.toml`.
Organize service tooling beneath `bigquery/`, `pubsub/`, and `cloudtasks/`, with service tests in corresponding directories.
Keep shared modules at the parent and use explicit imports from service modules rather than compatibility files at the old paths.
Read all package modules and policy through Python package resources and pass them to CUE as JSON input.
Collect nested source recursively and close the supervisor delivery under imports, package initializers, deferred actor imports, and referenced package data.
Encode source path separators as dots in ConfigMap keys and retain the original relative paths in volume projection.
Project them through one immutable system-namespace ConfigMap; the Pod runs the package CLI without startup installation.
This removes CUE's manually maintained source-file inventory and the script loader's file-path/import-order dependency.
The approval's `runtime_sha256` now hashes the complete bundle's relative paths and file hashes, preserving source/configuration identity after the requested module split.
The layout change alters source hashes; retain historical approvals and measurement records without rewriting their pins, and require a new approval for execution from the new layout.
Use the official Kubernetes, Cloud Storage and Google Auth Python clients for API transport and authentication, with dependencies installed in the digest-pinned lifecycle image before publication.
This replaces the initial standard-library-only transport choice at the owner's request; policy-only source changes still do not require rebuilding the image.
Manage Tier-3 as a buildable uv workspace member with its own runtime dependencies and `flink-tier3` console entry point, sharing the root `uv.lock`.
Move the bootstrap/schema helpers and their tests into that member and remove their old script entry points.
Repository checks and release helpers remain in `scripts/` until their planned `tools/checks` and `tools/release` member migrations; shell programs remain there.
The installed CLI requires an explicit checkout or the repository root as its current directory for CUE/OpenTofu files; the supervisor does not require a checkout.
Retain explicit UID/generation preconditions and timeouts; disable automatic SDK API retries and optional background bucket-metadata lookups.
Refined after pilot `bq1312-alo-10-a6`, whose runner exited on one `503 NOT_SERVING` namespace read after both recoveries had passed: the Kubernetes adapter repeats a read answered 500, 502, 503 or 504 up to three times, after 1, 2 and 4 seconds, and repeats no mutation, no timed-out read, whose timeout is already spent against the caller's deadline, and no proxied Flink REST read, whose 503 is the job's answer that only the recovery stages tolerate.
Restart a complete GCS read up to five times when the observed generation disappears or changes between metadata and download; distinguish that concurrency from an initially absent object.
Trial `bq1312-alo-50-a1`'s BigQuery cleanup rewrote its control record once per table, exceeded Cloud Storage's rate of about one mutation a second per object and crashed; that cleanup now writes once per pass.
Provisioning then stopped trial `bq1312-alo-50-a2` halfway through fifty tables by the same burst, two control writes per table; it now writes all missing create intents before the first create and the receipts after the creates, one write each, and a lost receipt is still recovered from its persisted intent at cleanup.
Declined: repeating a GCS write refused with 429, because a repeated write can land after its caller's deadline, and a BigQuery creation marker persisted that way is retained as unresolved and blocks cleanup.
Use native SDK downloads and pools instead of custom response-size wrappers; generic API bodies no longer have an 8 MiB memory cap, while inventory count limits and Pod log byte limits remain.
A standard authorized session is still injected into GCS to preserve disabled proxies, redirects and rejected-request refresh retries; public Client options do not expose those settings.
GCS uploads disable the SDK's checksum-triggered automatic deletion path so every deletion remains an explicit generation-checked lifecycle operation.

The runner owns admission after observing a ready supervisor: it opens quota, scales the Operator to one, waits for readiness, and creates the application.
The ordinary smoke supervisor only observes and writes toward idle, so recovery can use the same UID-checked cleanup concurrently after admission's final write.
The separately selected generic recovery exercise permits the two additional, object-conditional operations described below.
Publish `running` only after the runner finishes admission and records the application UID.
Before that phase, a supervisor failure requests stop and leaves settlement to the runner or completed-execution recovery; cleaning sooner could stop the Operator ahead of an in-flight application create.
This uses the existing phase boundary rather than adding another claim or termination-proof protocol.
Since [#1396](https://github.com/flink-gcp/flink-connector-gcp/issues/1396) a signal before supervision starts is the exception, leaving the run to the Job's replacement, and a claim in the control record fences the Pod a replacement displaces; the refinement further below records both.
Admission retries revalidate phase/stop state, and settlement reconciles actual state even after an earlier `cleaned` record.
This revises the initial supervisor-only admission decision, which required a Pod claim, persisted container termination proofs and a quarantine delay.
Those protocols are removed; Job completion is used to retain final logs when possible, not as permission to begin cleanup.
A failed or unavailable final log remains an evidence failure while shutdown continues.
A Job deadline requests termination independently of TaskManager survival on Spot or of JobManager survival on normal capacity.
Before foundation writes, read the Kubernetes resource version and then verify the exact lock owner; conflicts re-read both before retrying.

Keep `runtime.py` as the supervisor command implementation and external admission/settlement in `runner.py`, behind the package CLI with separate Google/Kubernetes modules.
Use composition through `Environment` for `Supervisor`, `Runner` and `Cleanup`, immutable approval/schedule dataclasses, a typed control record, and explicit phase transitions.
`Schedule` centralizes deadline arithmetic and reserves an 84-minute serial allowance within the workflow's 85-minute timeout; all six OpenTofu commands share a nine-minute deadline rather than separate per-command limits.
A slow individual refresh can consume more of that shared allowance; exhaustion retains the lock and requires investigation before another attempt.
CUE composes the lifecycle application through `runs/common.cue`, and delivery discovery renders it with synthetic dispatch inputs in CI.

Use a conditional-generation GCS environment lock across the three CI plan/apply roots and runtime.
Retain the per-root state locks and the tracked resting state in OpenTofu.
Do not expire or automatically steal the environment lock.
Temporary quotas and Operator scale are restored before the run finalizer requires all three empty refreshed plans and releases the lock.
Completed CI executions automatically trigger recovery of their retained plan lock; unrelated completion events do not replace pending recovery for another source.
Recovery of an interrupted infrastructure operation permits idle Operator image drift, reports it through refreshed plans and may release its idle infrastructure lock so a follow-up PR can repair it; it does not apply the plan.

Persist transitive workload UID observations before parent deletion; never authorize forced cleanup from labels alone.
Flink's HA metadata has no ownerReference, so rely on the Operator's normal finalizer for those ConfigMaps and retain a failed run lock if they remain.
This deliberately leaves uncertain ownership for an administrator's evidence-backed repair instead of widening the lifecycle deletion policy.
Evidence failure stops admission and marks failure while paid workload shutdown continues wherever namespace/UID identity and API access remain verifiable.
A cloud outage can prevent verified termination, so resource/time/cost approval is a bounded execution policy rather than a billing cutoff guarantee.

Synthetic tests cover admission refusal, cancellation, lost creation responses, UID replacement, evidence failure, normal/forced and concurrent cleanup, supervisor readiness, lock generations and final receipt requirements.
The separately approved [#1311](https://github.com/flink-gcp/flink-connector-gcp/issues/1311) exercise supplied one successful live run of the generic recovery path on 2026-09-17, one savepoint upgrade and one JobManager deletion, as its [closing comment](https://github.com/flink-gcp/flink-connector-gcp/issues/1311#issuecomment-5715015774) records; that run exercised the success path, and the failure paths above remain synthetic apart from the deployed-trial failures the [BigQuery trial findings](evidence/0165-bigquery-trial-findings-1312.md) record.

### Generic recovery exercise

Extend the existing workflow with an explicit `generic-recovery` scenario for [#1311](https://github.com/flink-gcp/flink-connector-gcp/issues/1311).
Use one integrated trial with 12,000 records at ten per second, one savepoint upgrade and one JM Pod deletion; preserve the existing four-Pod, one-hour and USD 1 bounds.
The initial checkpoint must be observed within ten minutes of admission start, each recovery within five minutes of its operation, and full completion before the final fifteen-minute cleanup reserve.
Stop an unsuccessful trial without automatically scheduling another one.

Keep the runner's admission handoff and the existing cleanup implementation.
The supervisor becomes the sole exercise writer after that handoff, recording operation intents before an approved application-argument patch and an owned JM Pod delete.
These operations cannot create a new root resource: the patch tests UID/resourceVersion, and deletion tests the old Pod UID.
Stop/expiry/lock checks prevent further operations, while object preconditions protect concurrent cleanup from delayed API calls.
Lost responses require verification of the requested outcome; completed-execution recovery only cleans and does not resume the exercise.
This refines the previous supervisor-only-writes-toward-idle premise without introducing another active writer or a new workload-admission path.

Recovery approvals use version 2 and pin the scenario policy and both CUE-rendered manifests; existing version-1 approvals remain valid for ordinary completion and cleanup.
The manifests differ only in the phase and required-restoration arguments.
Disable last-state fallback to make the savepoint trial unambiguous.
Operator 1.15.0 defaults to creating FlinkStateSnapshot resources; disable that option for this exercise and use its supported status-based savepoint reporting to retain the existing cleanup graph.
An Operator version change must revisit this choice against its upstream behavior.

Correlate restored state identity and timestamps with fresh progress, a different JM UID and a later completed checkpoint for each disruption separately.
A savepoint upgrade may change the Flink job ID; a JM failover must restore the upgraded job ID and a checkpoint at least as recent as the observed nonzero-progress checkpoint.
Operator 1.15.0 reports the old job as `FINISHED` after stop-with-savepoint; tolerate it during the upgrade stage, without treating it as final input completion.
Refined after trial `bq1312-alo-50-a1`, whose supervisor polled after the Operator had assigned the upgraded job's ID but before the stopped job's `FINISHED` state cleared, and stopped the run: the tolerance no longer requires the old job ID or `UPGRADING`, since an upgraded job that really finished never proves recovery and the upgrade's own deadline stops the run.
After both recoveries and observed complete input, normal idle-Pod removal may precede the final job status; permit only a shrinking stable Pod set, excluding Pods already terminating when that set was recorded.
No earlier phase's success flag or completed-checkpoint counter substitutes for this evidence.
Limit REST unavailability tolerance to the two recovery windows, retain scheduling/interruption observations, and classify observed unplanned interruption as inconclusive.
Keep resource/evidence ceilings, identity checks and final empty-plan verification active throughout the exercise.
Synthetic tests and rendered manifests establish these control paths; they do not establish live GKE recovery or service permissions.

### Cloud Tasks namespace foundation

For [#1246](https://github.com/flink-gcp/flink-connector-gcp/issues/1246), extend bootstrap ownership to `tier3-cloudtasks` with zero Pod/PVC quotas, installer access and the `cloudtasks-benchmark` KSA/job Role/RoleBinding.
The GCP root already owns the matching GSA, Cloud Tasks permissions, isolated storage and KSA impersonation trust.
Reuse the application lifecycle Role for the runner and supervisor in the new namespace.
The Cloud Tasks job Role grants no permissions in `tier3-system`.
The common bootstrap/Operator preflight keeps its existing namespace list until this foundation has been applied and the runner's new read permissions verified.
A cancelled apply can leave the shared environment lock held before the new lifecycle RoleBinding exists; extending preflight earlier would deny the runner the reads needed for recovery.
Acceptance checks the new namespace's observed zero quotas and empty inventory separately during this transition.
The Helm watch set remains smoke-only until that acceptance completes.

The new namespace extends the installer's authorization boundary.
An administrator first establishes its six importable prerequisites and extends the named namespace rules in the existing bootstrap reader/writer ClusterRoles, preserving their identities.
The [bootstrap runbook](../../opentofu/tier3-bootstrap/README.md#administrator-prerequisites-for-the-namespace-extension) specifies the source inventory and ordering.
CI adopts those objects and creates the workload/lifecycle identities through the ordinary PR plan and post-merge apply.
This change starts no workload and establishes no Cloud Tasks performance result.
Operator watch configuration, application publication, bounded admission and numeric resource/cost approval remain subsequent steps in #1246.

### Cloud Tasks lifecycle control foundation

The [namespace foundation apply](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/35234958991) completed successfully with an empty refreshed plan.
The new namespace remained idle and the runner's namespace reads were verified before extending the shared helper.
At that stage, the helper inspected three namespaces, and the idle Operator watched smoke and Cloud Tasks.
Helm added only its Operator Role and RoleBinding in `tier3-cloudtasks`, bringing the rendered inventory to nine objects.
The already-applied bootstrap installer grants cover these permissions.
The runner and supervisor can select the Operator KSA through Jobs in `tier3-system`, so that extension gave them trusted Operator-administrator reach into both smoke and Cloud Tasks.

The existing lifecycle GSAs also need to stop queues and remove checkpoint state after worker failure.
Two custom roles enumerate queue get/pause/delete and task get/list; only the runner additionally receives queue create.
Neither role grants queue resume/update/purge/IAM changes, task creation/deletion/run or task fullView.
Project-level bindings do not enforce a queue prefix: the later runtime must check the exact approved queue and run ownership.
The apply identity receives project-wide Role Admin to manage custom roles, including names beyond these two; custom-role creation depends on that grant.

Both lifecycle identities receive bucket-wide Object Viewer and Object User conditioned on the benchmark bucket's `runs/` object prefix.
This allows writes and deletes across all run prefixes and does not provide per-run isolation.
The worker's existing editor role is unchanged.
The expected GCP change is nine additions, separate from the in-place idle Helm release update.
Review those counts against CI plans before merge, then collect successful apply, idle inventory and empty refreshed plans.
These persistent grants prepare a separately reviewed runtime; they start no queue or workload and authorize no paid measurement.

### Cloud Tasks session admission

The third lifecycle scenario admits one Cloud Tasks measurement session: an ordered cell list from a reviewed session file, executed as one FlinkDeployment at a time in `tier3-cloudtasks` under one environment lock of at most five hours.
One approval per cell would have needed several hundred dispatches for the preregistered 420-cell assessment, so the session is the approval unit and a campaign ledger in `_control/campaigns/` records every cell's outcome across sessions and refuses to run a completed or running cell again.
The session ceilings, JobManager/TaskManager shapes per parallelism class, queue prefix and synthetic target live in `policy.toml` beside the smoke values and are embedded byte for byte in a version 3 approval; the smoke tables and cost constant are untouched because their tests pin them.
The published application digest is a dispatch input verified live against Artifact Registry, not a pin in `images/pins.cue`, so a stand-in digest cannot look like a runnable delivery.
Both lines were published from `339d0a90675dffee74305cebe021093c6a1f9b4b` on 2026-09-19 UTC, by [the 2.2.1 run](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/35474382654) and [the 1.20.4 run](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/35474834488); their digests are recorded in the calibration preregistration rather than pinned here.

The runner owns queue admission because only it holds `cloudtasks.queues.create`: it proves the run's queue absent, creates it in one request with a paused-queue configuration modelled on the accepted #1245 harness (one-hour tombstone, one dispatch per second, a single attempt), pauses it and reads back the paused state and zero dispatch counts.
That readback waits for the queue to become readable and paused, because a newly created queue answers a transient status, nothing, or its pre-pause state for as much as a minute; a wrong name, an unapproved configuration, any dispatch or a state outside that pre-pause one still refuses the run at once, and the wait never outlives the admission budget.
The supervisor re-reads the queue on every poll and treats any deviation as a stop; cleanup deletes the queue and a remaining queue blocks the idle receipt.
Every Cloud Tasks and Flink REST read is metered per actor against the session read ceiling, administrative queue writes against a ceiling of six, and storage listings remain bounded by their per-call maxima and the evidence budgets.
The task-creation ceiling and cost use a planning bound rather than the expected count: the application caps attempts per creator incarnation, so a cell may create its attempt limit times its subtasks times the four incarnations one JobMaster's restart strategy allows, and the manifest check pins that strategy; a JobManager failover resets the budget, so the cell deadline and the paused queue remain the real caps.
Cleanup deletes the queue only after the run persisted its intent to create it, and it adopts a cell whose create landed after the stop, because the runner's settlement can otherwise race the supervisor's last create; both decisions read the cached control record so a storage failure cannot stop cleanup.

The measurement application no longer prints its per-attempt rows to stdout: the supervisor's bounded log reads cannot capture hundreds of rows per second, so rows are written as gzip parts through the Flink filesystem next to the receipts, and Pod logs carry only Flink logs.
A truncated Pod log is therefore recorded rather than fatal for this scenario, while the smoke scenarios keep failing on truncation.
Cleanup deletes only the cells' checkpoint state under the one-day benchmark bucket and records the retained rows and receipts; exporting them to durable evidence and reconciling them against the receipts is the next change, which must run inside the same workflow execution.

That next change followed: the supervisor reconciles each cell's rows against its receipts after the cell, rewrites them into the evidence bucket with per-object hashes and a marker, releases the benchmark prefix only after the marker exists, and records one observation per poll from the Flink REST API, the TaskManagers, the queue and the Pods.
The offline analyzer and the protocol pin ship in the same installed package, so `runtime_sha256` covers the rules that will judge a run before it starts; the supervisor's own delivery does not carry them, and `delivery_sha256` accordingly does not pin them. A rule a scenario applies inside the Pod is a different case: the BigQuery verdict is decided there, so it is in the delivery and both digests pin it, while the offline recomputation of that same rule is not.
The environment lock's scan of `runs/` moved from listing every object to a delimiter listing with two reads per run: a calibration session exports several hundred row parts and a large main session a few thousand, so the previous hundred-thousand-object budget would have been spent after tens of sessions rather than at a knowable point.
Declined: exporting rows through the supervisor's own memory (parts stream through gzip and are hashed on the way; nothing is buffered whole) and a separate analysis dependency (the analysis module imports no cloud client and the command needs no checkout, although the installed package's own imports still load them).

Declined: extending the 60-minute smoke window to sessions (the workflow timeout rises to six hours only for this scenario, and the session's serial budget is proven to fit inside it); pinning a placeholder application digest; letting the connector or the supervisor create queues (only the runner may, and only the approved name).

### BigQuery namespace and data foundation

For [#1312](https://github.com/flink-gcp/flink-connector-gcp/issues/1312), retain Flink 2.2.1 and Operator 1.15.0 for the Storage Write trials.
The GCP root owns a dedicated persistent dataset with a 24-hour table expiration, a one-day run-state bucket, and the `tier3-bigquery` GSA trusted by `tier3-bigquery/bigquery`.
Trials own their temporary tables and state, and must delete them explicitly; expiry is a fallback.
The dataset remains empty between trials, is protected from destruction, and cannot automatically delete its contents during destruction.

Use dataset-scoped custom roles rather than project-wide BigQuery data roles.
The writer can read table metadata and append rows, but cannot create tables or query their contents.
The runner pre-creates matching tables and both lifecycle actors can inspect and query them; the supervisor cannot create tables, and only the supervisor's role carries table deletion, because only its cleanup path deletes a trial's tables ([#1691](https://github.com/flink-gcp/flink-connector-gcp/issues/1691)).
Both lifecycle actors receive project-wide query create/get/update permissions so recovery can inspect and cancel the other actor's outstanding query.
Those permissions reach other jobs in the project: exact query-ID ownership and query budgets belong to the runtime, not to these IAM bindings.
State-bucket listing and reads cover the whole bucket, and Object User covers all `runs/` objects.
Neither dataset nor storage IAM provides per-run isolation.

Bootstrap adds the `tier3-bigquery` namespace, zero quotas, installer access, workload KSA/job RBAC and the existing application lifecycle Role.
An administrator first establishes the six importable namespace/quota/installer objects and extends the existing named namespace rules, preserving existing identities.
The [BigQuery bootstrap procedure](../../opentofu/tier3-bootstrap/README.md#administrator-prerequisites-for-bigquery) requires review and approval of the concrete mutations before this step.
CI then imports those prerequisites and applies the remaining foundation through the ordinary saved-plan workflow.
The [foundation apply](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/35482844068) completed the six imports, five bootstrap additions and 19 GCP additions.
The refreshed bootstrap plan in CI and the separate refreshed GCP plan were empty.
Runner-impersonated quota and workload reads verified the applied lifecycle RoleBinding against observed zero quotas and an empty namespace.
After this acceptance, extend common preflight to all four namespaces and the idle Operator watch set to smoke, Cloud Tasks and BigQuery.
Helm adds the BigQuery Operator Role and RoleBinding, bringing the rendered inventory to eleven resources while preserving zero replicas.
Before Operator plan/apply, the helper also requires the BigQuery job identity and RoleBinding.
The runner and supervisor remain trusted Operator administrators; selecting its KSA through system Jobs now reaches BigQuery Secret access and Pod creation as well.
Existing lifecycle scenarios still admit only their own applications; the BigQuery application and runtime remain subsequent work.
Application publication and bounded execution follow separately; these persistent grants provide no deployed BigQuery result and authorize no paid trial.

Refined under [#1550](https://github.com/flink-gcp/flink-connector-gcp/issues/1550) for the [#1313](https://github.com/flink-gcp/flink-connector-gcp/issues/1313) FILE_LOADS scenario: the writer also receives a project-wide custom role holding `bigquery.jobs.create` alone, because its committer submits load jobs and jobs are project resources.
Grant nothing else on that path, each omission for a measured or documented reason; the application behaviour cited is that of its FILE_LOADS mode, added under [#1549](https://github.com/flink-gcp/flink-connector-gcp/issues/1549):

- No `bigquery.jobs.get` or `bigquery.jobs.update`: BigQuery accepts `bigquery.jobs.create` from a job's creator for reading and cancelling it.
- No `bigquery.datasets.get`: the application sets the sink's location, and the committer reads a dataset only to find that location.
- No table creation or deletion: the overflow path that needs them requires more than 10,000 files or 11 TiB for one destination in one commit, or a truncating disposition. The application appends, and its two writers each fail rather than hold more than the 10,000 pending files it leaves as the default, spread evenly over at least 10 destinations, so one destination's commit stays near 2,000 files whatever the staging file size.
- No `storage.buckets.get`: the existing bucket-wide Object Viewer grant lets load jobs read staged files, and the E2E identity loads with Object Admin alone.

Staging under `runs/<run-id>/staging/` falls inside the existing conditioned Object User grant and the one-day lifecycle rule.
The predefined `roles/bigquery.jobUser` was declined, because it adds Dataform repository creation and project listing to the same permission.
`bigquery.jobs.create` cannot be narrowed to load jobs: the workload can also submit queries, which read no Tier-3 table without `bigquery.tables.getData` but run and bill in the project.
This grant does not fence load jobs: one keeps writing after the workload's Pods are gone, which the rig's load-job barrier covers, through the supervisor's `bigquery.jobs.list` and `bigquery.jobs.listAll` ([#1551](https://github.com/flink-gcp/flink-connector-gcp/issues/1551), below).

### BigQuery recovery application

The opt-in `tier3-bigquery` Maven profile builds an internal Flink 2.2.1 workload with the production default-stream and buffered-stream sinks.
Keep finite checkpointed input at parallelism one and the sink at parallelism two, with fixed UIDs and no rescaling contract.
Each record routes by sequence modulo 10 or 50 destinations in the dedicated dataset; the external runner must pre-create and prove ownership of those tables because the sink uses `CREATE_NEVER`.
Use deterministic protobuf rows of exactly 64 KiB for ALO and 1 KiB for EO, a default offered rate of 1 MiB/s, and a finite 30-minute-equivalent input below the 2 GiB application ceiling.
These limits count serialized input before replay and RPC framing; they neither authorize a trial nor bound billed work.

Checkpointed input identity rejects another run or any changed input/mode configuration on restore.
Log source progress, snapshot positions and checkpoint notifications, and correlate them with external checkpoint history and query oracles in the subsequent lifecycle implementation.
Local source recovery tests use a discard sink; graph tests verify every writer reaches all destinations for both modes.
Emulator tests cover production ALO writer wiring and routing with one sequential append per destination.
Concurrent appends caused SQLite lock errors and repeated RPC retries in CI; parallel graph coverage therefore uses the local capture sink.
The pinned emulator assigns buffered offsets across streams and can hang on multi-stream flush, so this application requires real-service EO validation; the connector retains its deterministic buffered-service tests.
Neither establishes deployed exactly-once recovery or the conditional GCS grant's checkpoint/restore behavior.
The manual image workflow verifies and packages this application as `bigquery-recovery` on the fixed Flink 2.2.1 AMD64 base, independently of the Cloud Tasks runtime selection.
Its restricted Docker context carries only the Dockerfile and packaged JARs; the application CI lane builds that image from the same public base without pushing it.
The authorized publication path uses the existing GAR-scoped publisher identity and includes the image and base digests in its summary.
The [first application publication](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/35489876881) built `116f2b8d9f992ecca7282d6320468db2b4a5c196`; its GAR tag and digest were verified before the appender observations below were added.
The application now observes appender creation, append invocation and close through two protected factory hooks on the internal storage sinks.
Create the observer per new or restored writer; preserve the production writer, processing-time service, options, retry logic, state serializers, topology and committer.
The buffered hook decorates writer services only; the committer retains its original factory.
This keeps observation code out of the connector's metric inventories and avoids copying writer construction and failure-handler cleanup into the application.
Log writer incarnation, subtask/attempt, local appender identity, stream, rows, serialized `ProtoRows` bytes, offset, hand-off timing and synchronous outcome without retaining payloads or futures.
Return the original future with no callbacks: observed invocations include connector retries, but neither count SDK-internal attempts nor prove server acknowledgement or query visibility.
Observation output is best-effort; sequence gaps and recorded output failures are evidence limits, and abrupt termination can lose the tail without a final marker.
Account for synchronous logging overhead in the deployed characterization rather than treating these observations as a capacity measurement.
The lifecycle package now generates the run's exact-table aggregate SQL and checks complete downloaded aggregate arrays offline.
Count invalid rows separately, use exact distinct counts within the valid finite sequence domain, and compare each physical destination against its expected share including the remainder.
Reject missing or invalid rows in both modes; permit and report duplicate copies only for ALO.
Bind the requested input parameters in every aggregate row, require all destinations and bounded integer counts, and refuse malformed or oversized evidence rather than turning it into a data verdict.
These literals detect accidental mismatches but do not authenticate results; the resource adapter below supplies ownership/schema and exact-query result checks, while the later executor still owes durable admission/evidence, quiescent final observation and the approved cumulative budget.
Synthetic SQLite execution checks the generated SQL's relational semantics, not BigQuery service acceptance or streaming visibility.
The BigQuery CUE package now describes each trial in the dedicated namespace with the existing workload identity, one JobManager and two one-slot TaskManagers, job parallelism two and autoscaling disabled.
Use the smoke Pod shape for all three managers: 1 CPU, 2 GiB memory and 1 GiB ephemeral storage with equal requests and limits, pending approval of the complete run budget.
Keep the application image input restricted to the fixed GAR package and a digest; this is format validation, not provenance verification or pin adoption.
Both phases retain the same input identity, state paths under the run's BigQuery bucket prefix, 30-second checkpoints and savepoint upgrade mode.
The upgrade changes only the phase and requires restored state; validate both manifests and preserve the input contract before applying either.
Disable last-state fallback and FlinkStateSnapshot resource creation in both phases, as in the generic recovery exercise, so the savepoint trial and its status-based reporting retain that exercise's recovery and cleanup assumptions.
Revisit the reporting choice when changing Operator versions.
Synthetic renders cover all four mode/destination combinations and reject conflicting fixed settings.
A reusable package avoids committing synthetic digests or an unapproved concrete run as a delivery, and does not extend lifecycle scenario admission.
Prepare the BigQuery REST operations as an internal adapter with an injected authorized HTTP session, without extending scenario admission or adding a client-library dependency.
Require a caller-persisted trial/nonce/expiration intent and a finite set of deterministic query slots with explicit per-job byte and timeout limits; retries keep the same job ID, and new visibility observations consume new slots.
Match owned table metadata and creation receipts before cleanup, and bind successful query metadata, all result pages and billed-byte statistics to the existing oracle.
REST `tables.delete` offers no documented generation precondition, so ownership readback cannot substitute for the executor's exclusive access and creator/writer fencing.
Job timeout and cancellation are best-effort; the executor must confirm completion and retain ownership evidence through cleanup.
Anonymous result tables remain private to the submitting identity; supervisor query inspection/cancellation does not imply permission to collect another identity's result rows.
Synthetic HTTP tests establish these adapter contracts, not service acceptance, metadata freshness, a cumulative cost guarantee without durable slot accounting, or deployed recovery.
Compose the adapter with generation-checked run records before enabling scenario admission.
Persist each table creation intent before its write, retain creation receipts, and refuse adoption of a table that predates its recorded intent.
Allocate query slots durably by observation name, retain their deterministic IDs across restarts, and store result artifacts before their generation/hash pointers.
A stop flag closes subsequent admission but cannot fence an in-flight service request.
Require the quiescence barrier before cancellation and deletion; this component does not implement it, and the barrier cannot observe a server-side request already in flight, as [BigQuery quiescence barrier](#bigquery-quiescence-barrier) records.
Pending or unreadable submitted jobs prevent table deletion, and component cleanup does not assert whole-environment idle state.
Synthetic generation-conflict and restart tests hold these controller contracts; BigQuery approval validation, authenticated actor integration and evidence budget enforcement remain caller work.
Before wiring the actors, render one unapproved execution proposal from explicit trial inputs.
Bind the resource plan, repeated-trial ordinal, fixed finite input, proposed windows/cost and initial/upgrade/supervisor hashes in the ConfigMap alongside an empty approval document.
The CUE delivery reuses the BigQuery package for both phases and retains the phase-only savepoint transition.
The offline renderer accepts this scenario, while lifecycle execution entrypoints continue to reject it.
A declared revision and digest-shaped image are inputs to later provenance checks, not publication or execution evidence.
Keep the cost calculation a planning estimate with an explicit reserve; it cannot bound service bills or replace final resource and cost approval.
Updated image publication, digest adoption, bounded admission/query execution and complete cleanup remain separate preparation and acceptance steps for [#1312](https://github.com/flink-gcp/flink-connector-gcp/issues/1312).

Refined under [#1549](https://github.com/flink-gcp/flink-connector-gcp/issues/1549), the first step of the [#1313](https://github.com/flink-gcp/flink-connector-gcp/issues/1313) FILE_LOADS finalization scenario: the application and its CUE package gain a third mode, `FILE_LOADS`, that builds the production FILE_LOADS sink with `WRITE_APPEND` and 1 KiB rows.
Stage under `runs/<run-id>/staging` in the run's state bucket, inside the existing run-state IAM condition, bucket lifecycle rule and cleanup prefix.
Checkpoint every 120 seconds in this mode, in both the Java graph and the rendered configuration: the connector refuses an interval below its two-minute `minCheckpointInterval` default at graph construction, and the Java interval overrides the rendered one.
The Storage Write modes keep their 30-second interval, and their rendered manifests are unchanged byte for byte.
Take the five sink inputs a trial varies (`stagingFormat`, `maxConcurrentCheckpointFinalizations`, `maxConcurrentDestinations`, `maxStagingFileBytes`, `maxOpenDestinations`) as an explicit argument allowlist at the connector's defaults, refused in the Storage Write modes and bound into the checkpointed input identity.
Bundle the Parquet and Hadoop runtime the connector leaves to a deployment, so `PARQUET` staging is selectable rather than refused at graph construction.
Do not decorate the FILE_LOADS sink: it has no appender, and the connector's per-checkpoint staging, per-job submission and per-commit row-count log lines with its writer and committer metrics already report what a trial reads, beside the query oracle.
The CUE package renders the knobs from a flag table and re-unifies them with their schema where they are read, because a struct a delivery passed by reference rendered an out-of-range value through the plain path on cue v0.17.1; an out-of-range or unknown knob, or any knob in a Storage Write mode, fails the render, and `maxOpenDestinations` is capped at the connector's default `maxPendingFiles`, which the application does not expose.
The emulator cannot cover this mode: the connector refuses emulator endpoints for FILE_LOADS, because the pinned emulator runs no load jobs and serves no Cloud Storage.
A gated real-GCP ITCase therefore takes the savepoint-restore coverage, and the gated discovery and the `just e2e` runner now include Tier-3 applications under `kubernetes/apps`, each built behind its `tier3-<name>` profile after its connector is installed from the same tree.
Trial selection, the proposal, dispatch and the oracle refused the mode until [#1551](https://github.com/flink-gcp/flink-connector-gcp/issues/1551), below; this change granted no permission and ran nothing on the deployed rig.
Name the dataset's location on the FILE_LOADS sink, so the committer never reads dataset metadata to place its jobs and the workload needs no `bigquery.datasets.get`; the Storage Write modes keep no location.
The workload then needs only project-wide `bigquery.jobs.create` for this mode, granted under [#1550](https://github.com/flink-gcp/flink-connector-gcp/issues/1550); the gated test runs under the E2E identity and cannot show whether the deployed identity holds it.

Refined under [#1551](https://github.com/flink-gcp/flink-connector-gcp/issues/1551): the rig admits one FILE_LOADS trial, `fl-10`, at 10 destinations with the sink inputs at their defaults, under a version 5 approval; its oracle refuses duplicates as EO's does.
Find the committer's jobs by listing, not by label: the connector labels none, and their ids hash the staged files they load, so the issue's premise that they carry the run's labels measured false.
The supervisor alone receives project-wide `bigquery.jobs.list` and `bigquery.jobs.listAll` through its own custom role, and reads each candidate through the `bigquery.jobs.get` it already held; the runner lists no job.
`listAll` was first declined on the premise that a redacted listing would keep each job's id or drop it, and [#1552](https://github.com/flink-gcp/flink-connector-gcp/issues/1552) measured it false before the first trial: a caller holding `bigquery.jobs.list` alone gets every other principal's job with its id replaced by `<REDACTED>`, which matches no connector pattern, so the workload's own jobs would have read as foreign and both the oracle's check and cleanup's wait would have passed over them, with nothing recorded; a caller that also holds `listAll` gets the real ids.
A listed id outside the job-id grammar now fails the listing, so losing `listAll` fails closed; losing the whole binding would leave the supervisor listing only its own jobs, which the preregistration's pre-dispatch read-back covers.
A load is the run's when every source is under its staging prefix, and a copy or query when it writes one of its tables; `_verify_job` still refuses every job but the run's own query slots.
Cleanup waits for every such job to be `DONE` after the Pod barrier and before deleting a table, deletes any temporary table created in the dataset since the run started, and, with the write that records cleanup, the objects left under the staging prefix before state cleanup deletes them; a failed staging listing is recorded rather than raised, so it cannot keep the tables alive.
The oracle does not wait: the committer commits synchronously, so a job still running after `FINISHED` contradicts the connector's own account, and waiting would let its rows land and pass; it records the job and fails the run instead.
The FILE_LOADS committer is a separate vertex, so observation accepts a writer and a committer for that mode, samples the committer's metrics and each sink subtask's checkpoint statistics, and the verdict requires both families in both windows for that mode.
The issue placed finalization in the subtasks' sync and async durations, which measured false against Flink 2.2.1: the writer closes its staged files in the pre-barrier step, before the synchronous timer starts, so that time shows in the writer's end-to-end duration and the committer's start delay, and the commit's own time is the committer's `lastCommitDurationMillis`.

### Pub/Sub GCP preparation

For [#1361](https://github.com/flink-gcp/flink-connector-gcp/issues/1361), prepare a dedicated `tier3-pubsub` GSA and `flink-gcp-tier3-pubsub` state bucket in the GCP root before extending the Kubernetes foundation.
Use the generic smoke bucket's regional STANDARD storage, uniform access, public access prevention, disabled versioning/soft delete and one-day expiry policy.
The workload receives bucket-scoped `roles/storage.objectUser` and trusts the future `tier3-pubsub/pubsub` KSA.
The existing lifecycle runner and supervisor receive object reads/listing and `runs/`-restricted object mutation for state cleanup, following the smoke state policy.
The workload receives no Pub/Sub permissions until the application and owned-resource design identifies their required scope.

The separate bucket keeps recovery state and cleanup permissions apart from the smoke, Cloud Tasks and routine E2E fixtures.
A shared workload identity or reuse of another scenario's state bucket would couple their grants and retained-state cleanup.
The GCP preparation introduces no namespace, Operator watch change, topic/subscription or running workload.
Verify its reviewed CI apply and empty refreshed plan before the bootstrap stage adds the KSA, idle quotas and namespaced RBAC.
Application delivery, ownership-aware run lifecycle and separately approved execution remain dependent stages; this decision does not claim deployed recovery acceptance.

### Pub/Sub namespace foundation

Extend bootstrap to `tier3-pubsub` after the GCP preparation has applied and its refreshed plan is empty.
The namespace retains zero Pod/PVC quotas, the `pubsub` KSA selects the prepared GSA, and the job Role grants the existing native Flink permissions only within this namespace.
The runner and supervisor receive the existing application lifecycle Role.

An administrator establishes the six importable namespace/quota/installer prerequisites and extends only the named namespace rules in the two existing bootstrap ClusterRoles.
The [Pub/Sub procedure](../../opentofu/tier3-bootstrap/README.md#administrator-prerequisites-for-pubsub) requires approval of the concrete mutations, collision checks and preservation of existing object identities.
CI adopts those prerequisites and creates workload/lifecycle identities through its reviewed saved plan after merge.
Keep the common preflight and Helm watch set unchanged until successful apply, idle inventory, runner reads and an empty refreshed plan establish the new foundation.
This preserves recovery of an interrupted apply before the new lifecycle binding is available.
Pub/Sub service grants, application delivery and separately approved recovery execution remain subsequent stages of [#1361](https://github.com/flink-gcp/flink-connector-gcp/issues/1361).

The [Pub/Sub bootstrap apply](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/35485438834) succeeded, imported the six prerequisites and created five workload/lifecycle objects, then produced an empty refreshed plan.
Separate reads confirmed the namespace identity, job KSA/RBAC, observed zero Pod/PVC quotas and empty workload inventory; runner-impersonated reads verified its quota and inventory access.
These observations permit the common helper to include the fifth namespace and the idle Operator to watch Pub/Sub alongside smoke, Cloud Tasks and BigQuery.
Helm adds only the Pub/Sub Operator Role and RoleBinding, bringing its rendered inventory to thirteen resources while retaining zero replicas and the existing image/chart pins.
The runner and supervisor retain their trusted Operator-administrator role, now reaching Pub/Sub namespace Secrets and Pod creation through the Operator KSA.
Application admission and service permissions remain separate from this idle scope extension.

### Pub/Sub recovery application preparation

The [recovery application](../../kubernetes/apps/pubsub/README.md) follows the applied namespace and idle Operator foundation for [#1361](https://github.com/flink-gcp/flink-connector-gcp/issues/1361).
Its opt-in build targets the Flink 2.2.1 / Java 17 runtime and relays two pre-created input subscriptions into one pre-created output topic with production source/sink builders and workload ADC.
The job has no resource-administration authority; the external provisioner must record exclusive run ownership and settings before admission, and cleanup must refuse unproved ownership.
Service permissions, workload admission and independent lifecycle supervision remain subsequent implementation.

Preserve four distinct identities: logical input, input Pub/Sub message, observer processing call and output Pub/Sub message.
A fresh observation UUID per processing call distinguishes repeated input processing from repeated publication of one already serialized observation; retaining output message IDs distinguishes either from the collector's own redelivery.
The offline oracle checks completeness and reports all four duplicate populations without treating deduplicated completeness as exactly-once behavior.
Its evidence limits bound local analysis, not service cost or workload execution.

The checkpointed union-state guard fixes the run and logical input domain while permitting parallelism one/two rescaling with stable operator UIDs.
Local emulator/MiniCluster tests exercise the source and sink RPC paths of both entry points, TaskManager-loss checkpoint recovery and savepoint restoration in both directions.
They establish neither service replay timing nor deployed recovery; a later independently observed trial must prove the fault's completed-checkpoint boundary and expected replay cohort.
Actual service-resource lifecycle remains separate acceptance work on the parent.

The Table entry point ([#1428](https://github.com/flink-gcp/flink-connector-gcp/issues/1428)) reaches the same subscriptions and topic through the production Pub/Sub Table source and sink.
The `--entry-point` argument selects it, and the two entry points share no operator UID, so with `allowNonRestoredState: false` a savepoint written by one entry point cannot restore the other.
The source table is converted to a DataStream so that the same input check and observer apply, and the sink writes the same payload bytes, so one oracle reconciles either entry point.
A SQL-only pipeline from a persisted compiled plan was declined, because the observer's union-state guard and its restored-state observation have no SQL equivalent.
The planner's default UID format numbers operators from a JVM-wide counter, so the entry point sets `table.exec.uid.generation: ALWAYS` with a fixed UID format for each of its two translations.
A unit test pins the resulting UIDs across repeated translations and both parallelisms.

The manual image workflow verifies and packages the relay as `pubsub-recovery` on the fixed Flink 2.2.1 AMD64 base, independently of the Cloud Tasks runtime selection.
Its Docker context admits only the Dockerfile and packaged JARs; the application CI lane builds the same Dockerfile from the public base without publishing.
The publication path uses the existing GAR-scoped publisher and reports application and base digests.
The [first Pub/Sub publication](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/35494622116) completed for main `cba9043faee0ceb067cba23b56fe3e0abe7f0542`; a GAR read verified the application digest recorded in its runbook.
The reusable `pkg/pubsub` package requires the dedicated application image by digest, namespace/KSA, native Flink 2.2 runtime and isolated checkpoint/savepoint/HA paths.
Use one slot per TaskManager and one or two TaskManagers so rescaling and active-Pod replacement can be observed independently in the later trial.
The initial/upgrade arguments preserve run identity and logical input bounds, with savepoint upgrades and restored-state checks.
Synthetic deliveries exercise the package and shared run policy without committing an executable run; the offline proposal below adds rendering without runnable admission.
Actual admission still requires owned service resources and grants, independent supervision, a reviewed total Pod budget, live image retention and separately approved execution limits.

### Pub/Sub owned resource preparation

The Pub/Sub resource helper fixes two input topic/subscription pairs and one output pair under the approved run identity.
It refuses existing resources, records all creation intent in a create-only GCS control object before service writes, and labels each created resource with the run and ownership nonce.
Service readback must match the frozen settings in the [application runbook](../../kubernetes/apps/pubsub/README.md#owned-resource-operations) before a later controller may admit the workload.
An ambiguous create stops; matching partial creation can be cleaned using the retained manifest, but provisioning cannot adopt or resume it.
Cleanup checks the manifest, labels and topic bindings, deletes subscriptions before topics, and confirms absence while retaining the record.
An owned subscription with the service's `_deleted-topic_` marker remains cleanable, while inspection still requires its expected live binding.
Keep the retained intent under `_control/pubsub/`, outside the `_control/runs/` prefix that blocks shared lock acquisition.
The lifecycle still owns the separate active-run record and removes it only after final evidence and owned cleanup complete.

Pub/Sub deletion has no generation precondition, so this record is not a substitute for exclusive control of the resources.
A mandatory caller guard must enforce approval, shared environment ownership and the appropriate admission or cleanup budget before each Pub/Sub request or logical storage call, with exclusive control spanning read/delete gaps.
The caller reserves the whole storage call's request/time budget, including the shared adapter's generation-read repetitions; a guard callback is not an individual HTTP-request counter.
The helper uses the existing authorized HTTP session without automatic retries and does not implement that cross-actor lifecycle.
It remains preparation for separately approved execution; synthetic operation tests establish neither live service behavior nor deployed recovery acceptance.

### Durable Pub/Sub preparation control

Compose the resource helper with active run control before adding runnable Pub/Sub admission.
Bind the resource plan to the approved application digest, run identity and nonce; allow only the submitting runner to claim one preparation attempt through a generation-checked transition.
Persist creation intent before the helper's manifest write, retain successful settings and explicit-policy observations outside the workload, and refuse preparation replay after ambiguity.
A separate supervisor can stop and reclaim partial work, provided its caller proves all creators, writers and in-flight requests quiescent first.
Without creation intent, preserve any existing service names.
With creation intent but no resource manifest, permit only guarded absence checks of all six names; an existing resource or uncertain read blocks settlement without authorizing deletion.
The resource helper owns this branch through `cleanup_or_confirm_absent()`; strict `cleanup()` still requires its manifest.
A replaced manifest still refuses cleanup.

Keep the active record when cleanup is uncertain, and guard Operator shutdown and final settlement until recorded Pub/Sub service cleanup is complete and the shared stop is set.
Final settlement preserves cleaned Pub/Sub intent and observations in the result receipt; when either snapshot carries Pub/Sub, reuse requires the complete computed result to match, including its success verdict and plans, except the refreshed plans' observation time for version 5.
This refuses presence/absence mismatches and stale success receipts after concurrent evidence failures.
A fresh cleanup attempt returns to `cleaning` even after prior success, so uncertain rechecks close settlement again.
Before deleting active Pub/Sub control, compare the complete current record with the receipt snapshot and use its observed generation; concurrent changes retain control and the lock.
This gate preserves existing behavior for records without Pub/Sub state.
The controller limits its control portion to 256 KiB and adds guarded logical control accesses; helper-only operation counts do not bound an integrated lifecycle.
The [runbook](../../kubernetes/apps/pubsub/README.md#durable-preparation-and-cleanup) states the caller's budget, exclusive-control and external quiescence obligations.
Numeric execution ceilings, CLI admission, deployed actor handoff and recovery evidence remain subsequent work under [#1361](https://github.com/flink-gcp/flink-connector-gcp/issues/1361).

### Pub/Sub message evidence

Keep the runner's input publisher separate from the supervisor's output subscriber, using the fixed resource grants and names.
Before each bounded input batch, save a create-only intent containing its exact payloads and identity; refuse an existing intent rather than retry an ambiguous publication.
Retain the ordered service response so later assessment can correlate logical inputs with input publication IDs.
Before acknowledging output, retain every output service message ID and payload outside the job JVM in the existing oracle's TSV encoding.
Keep collector redelivery visible by retaining every batch independently, without deduplication or logical-payload filtering.
An empty pull does not prove drain, and an ACK response does not prove the absence of future redelivery.

The internal message helper bounds one call's messages and response bytes; the caller still owns authenticated actor binding, prepared-resource checks, exclusive run authority, durable aggregate budgets, deadlines and quiescence before cleanup.
A guard runs before every service call and logical evidence upload; helper calls perform no automatic retries or evidence replacement.
Use retained intents to attribute ambiguous outcomes, and retain malformed decoded responses without acknowledging them.
The [runbook](../../kubernetes/apps/pubsub/README.md#input-publication-and-output-collection) defines the operation counts, local caps and remaining integration obligations.
This stage adds no runnable scenario or real-service recovery claim under [#1361](https://github.com/flink-gcp/flink-connector-gcp/issues/1361).

### Pub/Sub shared traffic reservations

Compose message helpers with the prepared resource controller before enabling runnable admission.
Freeze explicit traffic limits and the digest-bound application's logical input domain in the shared record before the first data operation.
Reserve publication calls, input messages/payload bytes, requested output deliveries, pull calls and data requests through conditional updates before the corresponding external work.
Reserve exact serialized message-evidence bytes before every upload, across both actors; preserve all charges after failed or ambiguous outcomes.
Process-local counters and refunds would let concurrent actors or a restarted process exceed the common budget.
Keep the binding and counters through service cleanup and final receipt settlement.
Latch failed evidence reservations/uploads locally and persist shared evidence failure through guarded control; if that write is unavailable, require the caller to stop both actors independently before restarting.

Admission requires current ownership, prepared resources, an active run and no stop or evidence failure before its deadline.
Permit already admitted operations to retain received evidence during cleanup until approval expiry, but treat a following ACK as new communication.
The wrapper does not adopt prior direct-helper work, cancel in-flight requests or authenticate actor strings.
Full execution approval, effective access, exclusive resource control, total credential/control/storage operation costs and the external quiescence barrier remain caller obligations.
The [reservation runbook](../../kubernetes/apps/pubsub/README.md#shared-traffic-reservations) distinguishes the internal numeric ceilings from trial authorization and total service billing.
CLI admission remains disabled.

### Pub/Sub actor release before cleanup

Bind one process token per actor before its first preparation or message call, and serialize that actor's calls through a durable invocation ID.
Create resource control and the runner binding in the same conditional write.
A synthetic crash between the former two writes left resource intent without an actor binding, which neither cooperative release nor reclamation could settle.
Atomic initialization leaves no Pub/Sub state before commit, or a complete actor binding after commit; a lost acknowledgement permits only the original token's admission-checked retry, while a dead actor requires the existing external reclamation proof.
The two actors may overlap while sharing the existing traffic reservations.
A completion acknowledgement clears only the invocation it names; retries never repeat the service operation or clear a later call.
Retain ambiguous invocations rather than interpreting client timeout as service quiescence; a failed call whose actor session saw every write answered releases its marker, as refined under [#1580](https://github.com/flink-gcp/flink-connector-gcp/issues/1580) below.
Process tokens are identities in the protocol, not authentication or transferable leases.

Require cooperative release of every bound actor before normal service cleanup, followed by the caller's external workload and in-flight-operation barrier.
Apply the release gate to the resource controller and shared settlement whenever a handoff is present; preserve the prior contract for older records without one.
A replacement supervisor cannot adopt the former actor's data authority.
It may explicitly reclaim after an external quiescence proof for the exact actor snapshot; a concurrent change refuses that proof, and unresolved invocation identities remain in the final receipt as fenced calls.
The helper records the caller's assertion without measuring process termination or service completion itself.
This avoids automatic expiry or token takeover, either of which could admit cleanup while an old request still executes.
The [handoff runbook](../../kubernetes/apps/pubsub/README.md#actor-ownership-and-cleanup-handoff) defines the required external fencing and failure-persistence obligations.
Runnable admission and deployed recovery remain subsequent work under [#1361](https://github.com/flink-gcp/flink-connector-gcp/issues/1361).

### Pub/Sub shared settlement

Attach the original Pub/Sub runner to common settlement and the supervisor to common cleanup with an explicit external quiescence barrier.
The caller must finish data operations by both actors, including supervisor output collection, before entering terminal settlement.
Release the runner before waiting for the supervisor, because the supervisor's service cleanup itself waits for every bound release.
That release closes shared admission even without an explicit stop request; waiting cannot overlap further output collection.
A runner that is not stopping now keeps its authority while it waits, to publish the cohorts the supervisor requests, as refined under [#1601](https://github.com/flink-gcp/flink-connector-gcp/issues/1601) below.
Keep failed-release diagnostics and a failed outcome even when a later acknowledgement settles.
An unbound supervisor may stop admission without fabricating a release record; a replacement cannot release a former actor.

Perform service cleanup after owned workload teardown and before temporary state deletion or Operator shutdown.
Extend the Pub/Sub clean-state gate to direct shared phase/idle observations as well as final receipt creation.
Do not automatically reclaim unresolved calls, infer quiescence from workload disappearance or hide missing actor wiring behind the shared cleanup path.
Route Pub/Sub cleanup to its existing namespace and state bucket while preserving earlier scenarios' inventory scopes.
Keep runner admission and supervisor execution disabled until the separately reviewed numeric execution contract and orchestration are complete.
The internal version 5 schema below refines the initial serialized-approval refusal without enabling execution.
The [shared settlement runbook](../../kubernetes/apps/pubsub/README.md#shared-settlement-integration) states the internal attachment and failure contracts.

### Offline Pub/Sub trial proposals

Before enabling runnable Pub/Sub admission, render one unapproved trial with the existing CUE application and supervisor delivery.
The trial names one entry point and applies it to both manifests.
Keep JM and active-TM replacement separate, with unchanged manifests at parallelism two; savepoint rescaling changes one to two or two to one and requires restored state in the upgrade phase.
Freeze the run/nonce, supplied source/image identities, installed runtime digest, exact manifests, service settings and grants.
Divide each subscription's finite sequence domain into disjoint pre-recovery and post-recovery cohorts, using at most 100 messages per publication.
The cohorts define publication intent, not a measured uncheckpointed replay population.
Three cohorts replace the two, so that the replay population is published as its own cohort, as refined under [#1601](https://github.com/flink-gcp/flink-connector-gcp/issues/1601) below.

Require explicit traffic counters within the existing helper ceilings and check that they can cover at least one complete input/output pass.
Propose a one-hour window, fifteen-minute cleanup reserve, seven total Pods and no PVCs, with separate total-request and additional-cost caps.
Reserve four application Pods (three steady plus one replacement) and three control Pods (Operator, supervisor and one replacement); later admission must account for termination overlap within these namespace budgets.
The request cap is at most 100,000 and the cost cap at most USD 10; neither is wired to full execution accounting, and cost has no estimate yet.
Message-helper reservations do not include connector SDK, provisioning, control, credential or storage requests.
Later admission must establish complete operation/state/evidence budgets, current pricing and live provenance/access before enforcing those limits.
Keep the offline delivery approval empty and reject Pub/Sub execution until that contract, external fault-boundary observations and independent orchestration are reviewed.
The [offline runbook](../../kubernetes/apps/pubsub/README.md#offline-trial-proposal) records the schema, synthetic example and outstanding execution requirements.

### Pub/Sub internal approval and shared resource policy

Use approval version 5 to serialize one Pub/Sub trial for internal lifecycle composition.
Reuse the offline trial schema and input feasibility calculation, then derive resource identities and helper counters from the validated approval.
Version 5 controllers validate the serialized trial; traffic construction must match its record count, all counters and cleanup deadline.
Retain the earlier caller-owned low-level contract for internal fixtures without version 5, without accepting them as serialized approvals.

Require the proposal's integral one-hour window, fifteen-minute cleanup reserve, fixed shared state/log/evidence limits, exact application/control namespace identities and digest-pinned runtime image roles.
Use seven shared Pod slots with a four-Pod application quota and three-Pod control quota so a terminating Flink or Operator Pod does not consume its own replacement's slot.
The per-namespace quotas carry the matching resource sums and restore the recorded idle values.
Preserve earlier scenarios' ceilings and inventory scopes.
Include the trial and recovery observations in final receipts, including runs stopped before service initialization; compare the receipt and current control snapshot before deleting active control.
For version 5, a finalization retry compares all receipt fields except the refreshed plans' observation time (`plans.at`); it retains the original receipt and still requires current empty plans for the same nonce and all three foundation roots.
A local injected control-deletion failure reproduced refusal of a valid retry when only that timestamp changed; the repaired tests cover failures before control deletion and after deletion but before lock release.
Recovery's placeholder control record finalizes a run that never reached service intent, and a measured retry proved it cannot finalize one whose cleaned Pub/Sub portion it does not carry; the BigQuery trial has the same shape, and its receipt can also carry a success verdict the placeholder does not.
Refined under [#1437](https://github.com/flink-gcp/flink-connector-gcp/issues/1437): when control is missing and a BigQuery or Pub/Sub receipt exists, recovery restores the deleted snapshot from the receipt, which still carries the service portion, the recovery observations and the verdict, and only if finalization's own receipt derivation reproduces that receipt; anything else is refused and retains the lock.
The [lifecycle runbook](../../kubernetes/lifecycle/README.md#stranded-environment-lock) holds the exact conditions.
Declined: a separate operator command for the restoration, because the existing recovery dispatch already carries the lock-holder authentication a restoration needs; and restoring for every scenario, because only the two service trials compare the whole receipt.
Earlier internal Pub/Sub fixtures keep their strict full-receipt contract.
Refined under [#1565](https://github.com/flink-gcp/flink-connector-gcp/issues/1565): a success receipt fixes the verdict, so a later settlement's own failures no longer lower it.
Recovery settles again before every finalization retry, and a transient failure of its own evidence writes or state cleanup used to reach the record as `success: false` or an evidence failure; the record never regains success once cleaned, so the retry's derived receipt conflicted with the stored one and the lock stayed retained after a successful trial.
Failures are therefore separated by where they are recorded: one already on the control record, whoever wrote it, still lowers the derived verdict and is refused as a conflict, while one of this settlement's own writes only keeps finalization from running until a retried recovery settles cleanly.
The [lifecycle runbook](../../kubernetes/lifecycle/README.md#stranded-environment-lock) holds the exact behaviour.
Declined: relaxing the receipt comparison, which would also accept a receipt that a recorded evidence failure invalidated; a separate control field for settlement failures, because each recovery settles from scratch and the failed settlement already keeps finalization from running; blocking over every receipt, because an unsuccessful receipt has no verdict to protect, and blocking would newly fail an inconclusive or Pub/Sub recovery whose own evidence failure previously finalized; and keeping the verdict when the receipt cannot be read, because before any receipt exists a settlement's failures are the run's own, and keeping them off the record could let a clean later settlement finalize a success they should have lowered.
Keep the Pub/Sub success verdict false until fault/recovery evidence and its oracle are implemented; the verdict refined under [#1624](https://github.com/flink-gcp/flink-connector-gcp/issues/1624) below meets that condition.

This refines the previous blanket refusal to deserialize Pub/Sub, not the prohibition on paid execution.
The selected dollar cap remains unestimated, the total-request cap is not an aggregate meter, and shared schema validation does not authenticate approval.
Keep runner and supervisor entrypoints disabled and the offline delivery approval empty until complete accounting, approval-bound delivery, external fault observations and independent orchestration are reviewed.
The [internal approval runbook](../../kubernetes/apps/pubsub/README.md#internal-approval-contract) records the exact limits and remaining boundaries.

Refined under [#1429](https://github.com/flink-gcp/flink-connector-gcp/issues/1429): the run workflow and the lifecycle CLI accept `pubsub-recovery` and build the version 5 approval, and admission is still refused.
A dispatch names a reviewed trial file under `kubernetes/lifecycle/pubsub-trials/`, containing the offline schema as TOML beside its licence header, and the published application digest.
Refined under [#1655](https://github.com/flink-gcp/flink-connector-gcp/issues/1655), the lifecycle delivery keeps shared resources in `delivery.cue` and each scenario in a sibling CUE file in the same package.
The [reviewed-trial instructions](../../kubernetes/lifecycle/README.md#reviewed-pubsub-trials) live in the lifecycle runbook; the directory is created with its first reviewed trial file.
Its phrase carries the policy's Pod count and window and the trial's own record and total-request ceilings, so a phrase typed for one trial does not approve another trial with different numbers.
The window starts at admission on the whole second and lasts exactly the approval's hour; the typed expiry may lie at most ten minutes beyond it, as for BigQuery.
Dispatch assembles the approval from the proposal it rendered and verified, mapping the proposal's recovery-application digest to the approval's `upgrade_application_sha256`, and prepares the approval-bound bundle, which re-renders from the approval alone.
The approval's window and source checks, the approved-checkout check, the approval embedding and the ConfigMap size limit move from the BigQuery bundle into one shared module, so the two service deliveries share them; each keeps its own re-render and digest comparison.
The approved-checkout check now also refuses untracked TOML files under `kubernetes/`, ignored ones included, so a dispatch naming a trial file the approved commit lacks is refused at bundle preparation, before the lock; this applies to the BigQuery bundle as well.
Dispatch then refuses before the environment lock, writing no lock, evidence, control record or run document, and runner admission remains [#1430](https://github.com/flink-gcp/flink-connector-gcp/issues/1430).
Declined: workflow choice inputs for the trial kind and entry point with policy-fixed numbers, because the numbers differ between the campaign's trials and are preregistered per trial; a JSON trial file, which apache-rat would reject without a licence header; and continuing to the runner's existing refusal after the lock, because a run that cannot execute would then take the lock, write evidence and depend on settlement to release it.

Refined under [#1580](https://github.com/flink-gcp/flink-connector-gcp/issues/1580), the first half of [#1430](https://github.com/flink-gcp/flink-connector-gcp/issues/1430): the parts admission composes exist, and admission is still refused.
A Pub/Sub handoff is bound to the runner and the supervisor only through one factory each, which authenticates the actor's own service account, checks its source pin and both manifests' digests, and requires both manifests to run exactly the job the approval's trial derives: parallelism, phase, record count, restore requirement and entry point.
The digests alone do not establish that: the runner's bundle re-render already compared the rendered jobs with the trial, but a supervisor's mounted manifests, which nothing re-renders, met the trial only through the record count the traffic wrapper compares.
Each actor's production guard reserves a method's whole operation bound before the method starts, refuses an operation outside the reservation, its phases or its bound, and refuses to start a Pub/Sub request within the session's 20-second request budget of its deadline, or one that an approval no longer validating would issue.
The deadline is the admission deadline, except for message traffic, which belongs to the exercise and defaults to `cleanup_at`.
The controller and the traffic wrapper consult the guard after their own control I/O, so the deadline is checked immediately before the request rather than before a control read that could outlast it; the request budget covers the response's status and headers, while a streamed body is bounded only by the transport's per-read timeout and the 1 MiB response cap that the resource helper now applies as the message helper already did.
A bound is the runbook's measured helper count plus the worst case of five-attempt conditional control updates and of a call's own markers.
The deadline applies to service requests only: a stop, a released marker or the evidence of a request already sent must still be written after it.
Admission is to create the application within 900 seconds of the window's start, because it also creates and grants six resources, waits for the grants to take effect for each identity and publishes the first cohort; until [#1581](https://github.com/flink-gcp/flink-connector-gcp/issues/1581) composes admission, the guard enforces that deadline only on the admitting methods' Pub/Sub requests.
The bounds are per method and held in process; the aggregate request ceiling stays with [#1433](https://github.com/flink-gcp/flink-connector-gcp/issues/1433).
A failed call now releases its marker while the actor's session has seen every Pub/Sub write it sent answered below 500, so read-detected drift stops the run and leaves it cleanable; a write that left without a status, or was answered 5xx, keeps that marker and every later one, and so the lock.
The runner may publish from `READY`, so the first cohort is waiting before the application exists, while the supervisor still collects only from `RUNNING`.
Cleanup's barrier waits for every writer in the application namespace, not only this run's, because the namespace's service account can create Pods that hold the workload's data grants; it is accepted as the workload-writer proof the cleanup barrier owes, and it still does not prove that a request a terminated Pod sent has finished at the service.
The tests' example trial moved out of the reviewed directory, which holds no trial until [#1434](https://github.com/flink-gcp/flink-connector-gcp/issues/1434) preregisters them, so no dispatch can name an example once admission opens.
Declined: refusing the example by a reserved name at admission, because the approval carries the trial's content rather than its file name, and either adding the name to the version 5 schema or matching content would guard a file that has no reason to be dispatchable; and keeping every failure's marker, because a refusal before any request, or a drifted read, would then need the external reclamation proof to clean a run that sent nothing ambiguous.

Refined under [#1581](https://github.com/flink-gcp/flink-connector-gcp/issues/1581), the second half of [#1430](https://github.com/flink-gcp/flink-connector-gcp/issues/1430): the runner admits a version 5 Pub/Sub run, and dispatch still refuses before the environment lock until the supervised exercise ([#1431](https://github.com/flink-gcp/flink-connector-gcp/issues/1431)) and execution accounting ([#1433](https://github.com/flink-gcp/flink-connector-gcp/issues/1433)) exist.
After the supervisor and the Operator are ready, admission binds the runner, creates the six resources and then installs their grants, probes each identity's effective access, waits for the supervisor to join, re-reads every resource and policy, and publishes the first cohort, all within 900 seconds of the window's start, before it creates the application.
The issue's order of grants before creation cannot hold, because installing a policy reads its resource, and the supervisor can join only prepared resources.
Effective access is tested per identity on all six resources for the one permission the run's bindings decide there, publish on a topic and consume on a subscription, because the runner's and the supervisor's project roles grant neither: every resource is then a positive or a negative control.
The runner and the supervisor test themselves; the workload's service account is reachable only through its Kubernetes service account, so the runner creates one CUE-rendered probe Pod that runs as it, requires it to succeed, re-derives the verdict from its log rather than trusting the exit status alone, and deletes it before the application is created.
A missing grant is retested every 15 seconds while a whole further round of tests still fits before the deadline, because IAM documents propagation as typically two minutes and potentially seven or longer; an unexpected grant is refused at once, because the resource policies were empty before installation, so the run's own writes cannot explain it; each consumer then pulls once without acknowledging, while nothing is published.
A runner-only Role lets the runner create Pods in `tier3-pubsub`; the runner could already reach the same identity through a FlinkDeployment, so this adds no reachable authority.
The supervisor joins, probes itself and stops at the exercise boundary, which the exercise refined under [#1602](https://github.com/flink-gcp/flink-connector-gcp/issues/1602) below replaces.
Once the run has Pub/Sub state, a stopped supervisor is not left to a replacement, joined or not, because only a supervisor deletes Pub/Sub resources and no other process can release a joined one's authority; a replacement that finds such a binding stops the run and reclaims only after the runner released, the former supervisor Pod ended or is gone, and the namespace barrier passed, and a supervisor whose cleanup outlasts its grace period keeps the lock for an operator.
Declined: a probe that reads a ConfigMap in `tier3-pubsub`, which would need the runner to create ConfigMaps there as well; a Python-built probe manifest, which would leave the only unrendered resource outside CUE's ownership of manifests; having the Operator's service account create the probe, which would borrow another identity's authority; treating a permission test as proof of data access, which the pull adds; publishing only after `RUNNING`, which would race the supervisor's start of supervision; and letting a replacement claim a run its joined predecessor already stopped, which would reopen the claim fence of [#1396](https://github.com/flink-gcp/flink-connector-gcp/issues/1396).

Refined under [#1601](https://github.com/flink-gcp/flink-connector-gcp/issues/1601), the first half of the supervised exercise [#1431](https://github.com/flink-gcp/flink-connector-gcp/issues/1431): the input domain splits into three cohorts per subscription, and the runner publishes the later two when the supervisor asks.
The first cohort is published at admission, the second after a retained completed checkpoint, so that a replacement trial has a population that was processed but that no completed checkpoint covers, and the third after observed recovery.
Only the runner holds the publisher grant on the input topics, so the supervisor records a request in the run control and the runner serves it from its settlement wait; each cohort is published once, in order and before its deadline, and its start is durable before its first request, so that start bounds from below when any of its messages can exist.
The runner therefore no longer releases its authority as settlement begins: it keeps it while the run is `RUNNING` and nothing stops it, and releases once a stop or evidence failure is recorded, the supervisor's Job ends or is lost, the run leaves `RUNNING`, `cleanup_at` passes or a publication fails.
Supervisor cleanup stops admission before it waits for the runner's release, so the runner, which sees that stop on its next poll, never waits on a supervisor that waits on it.
The supervisor's output collection pulls the independent subscription through its own reservations and keeps the parsed relay observations for the exercise; the batch evidence stays as the message helper writes it.
Declined: granting the supervisor publish access on the input topics, which would remove the separation between the identity that drives input and the one that observes output; and a second cohort taken from the halves the plan had, which would leave the replay population indistinguishable from input published before the retained checkpoint.

Refined under [#1602](https://github.com/flink-gcp/flink-connector-gcp/issues/1602), the second half of [#1431](https://github.com/flink-gcp/flink-connector-gcp/issues/1431): after admission the supervisor runs one approved trial, and dispatch still refuses before the environment lock until execution accounting ([#1433](https://github.com/flink-gcp/flink-connector-gcp/issues/1433)) exists.
The exercise observes the first cohort on the output subscription, retains a checkpoint triggered after that observation, requests the replay cohort, and injects the trial's fault once that cohort is processed: it deletes the JobManager or the TaskManager whose attempts processed most of the cohort, or patches the application to the recovery manifest for a savepoint rescale.
It then proves the restoration, requires what the fault displaced to reappear from new attempts, requests the last cohort and requires it processed on both inputs, followed by another completed checkpoint.
Checkpoint ids order the checkpoints against the observations, rather than a clock: an id above every id the job reported on the poll after a cohort was fully observed belongs to a checkpoint triggered after that observation; the REST statistics lag by at most the three-second refresh interval, against a mark read a 15-second poll later, and a checkpoint triggered after an output observation covers the observed messages, because the source emitted them before its barrier.
The boundary needs no clock either: the retained checkpoint was seen completed before the supervisor requested the replay cohort, and the runner starts a cohort only after reading its request, so restoring exactly that checkpoint proves that no checkpoint covering the cohort had completed before the fault; restoring a later one records the boundary as lost and the replay as unobserved rather than failing.
The expected replay population is the cohort's observations by the attempts the fault displaced, because a TaskManager's loss may restart only the failover region on that Pod, whose surviving neighbour's share the next checkpoint acknowledges; a savepoint acknowledges everything it covers, so a rescale expects none.
The application's checkpoint interval rises from 30 to 120 seconds, in the CUE configuration and in the job's `main`, because the supervisor and the runner each act once per 15-second poll: publishing, processing and observing the replay cohort can take three polls, about 45 seconds, so with 30-second checkpoints the next checkpoint would usually cover the cohort before the fault, and the replay population would rarely exist.
The Pub/Sub application now disables last-state fallback and FlinkStateSnapshot resources, as the BigQuery application does, so Operator 1.15.0 reports the rescale's upgrade savepoint in the application's status, where the exercise reads it.
The exercise pulls the output subscription at least once per poll, so the proposal's traffic check now also requires 180 pulls, their output reservations and their requests, one per 15-second poll in the 2,700 seconds before `cleanup_at`.
Those pulls set `returnImmediately`, because the service may otherwise hold an idle pull past the 20-second transport timeout, and a pull without a definite answer keeps its marker and so the lock; the service may answer such a pull empty while messages wait, which delays a cohort's observation by a poll.
The exercise records its completion as the supervisor's success, as BigQuery's does, while the final receipt derives no success for `pubsub-recovery` until a verdict exists, which [#1624](https://github.com/flink-gcp/flink-connector-gcp/issues/1624) below supplies.
Declined: keeping 30-second checkpoints and polling every second around the replay cohort, which would multiply control reads and pulls and still race the next checkpoint; requiring output from restored attempts before requesting the last cohort, which a savepoint rescale cannot meet because nothing is expected again after it, so restored attempts are proven by their own log announcements instead; and counting every pre-fault attempt as displaced on a TaskManager's loss, which would expect a replay that region failover rightly does not perform.

Refined under [#1624](https://github.com/flink-gcp/flink-connector-gcp/issues/1624), the first half of [#1432](https://github.com/flink-gcp/flink-connector-gcp/issues/1432): the exercise decides the trial's verdict when it completes, and the final receipt reports success for `pubsub-recovery` only when that verdict is `usable`, as for BigQuery.
The verdict requires the completed outcomes, the output oracle's acceptance of every collected line with every logical input of both subscriptions present, a replacement's whole non-empty expected replay seen from new attempts under an input message ID it was processed under before the fault, and backpressure, source and sink connector metrics each read in a sample before the fault and in one after recovery; anything short of that is `inconclusive` with its reasons.
A lost boundary completes the trial as `inconclusive` with `replay-unobserved`, because a trial that restored a checkpoint already covering the replay cohort exercised no redelivery, which is what a replacement trial exists to show.
Whether the service redelivers after a rescale's savepoint, when not every acknowledgement reached it, is unmeasured, so a rescale that saw replay-cohort output from the attempts it started, by completion, is `inconclusive` with `replay-after-savepoint` until the deployed campaign ([#1435](https://github.com/flink-gcp/flink-connector-gcp/issues/1435)) measures it; counting it as a harmless duplicate was proposed and withdrawn, because nothing yet shows it is one, and a later redelivery past completion, which extended leases allow, is outside what the trial observes.
The oracle's duplicate counters travel beside the verdict and decide nothing, because at-least-once delivery owes them and neither unique message IDs nor a deduplicated count establishes exactly-once output; nor does the verdict say anything about ordering across the replay.
The oracle is a Python port of the relay's Java report, because the supervisor's image carries no JVM, and the Java report stays for the emulator integration test; both decide one shared case file, which pins the report's own rejection messages and, where the JDK writes the message, only the refusal, so a rule changed on one side only fails the other's tests.
The coverage fold, its reasons, the subtask metric query and coverage on every transition are shared with the BigQuery verdict, in `metrics` and the exercise base, so those parts cannot drift; each verdict still checks its own outcomes and credits a reading by its own rule, because BigQuery's TaskManager metrics answer strings where Pub/Sub's connector aggregates answer numbers. A `True` sample count, which BigQuery's copy had read as one, now reads as none for both, and a metric read whose answer is too large, not JSON or not UTF-8 is recorded as unavailable rather than stopping the run, which the Cloud Tasks observer, sharing the same `sample`, now does too.
The samples add the connector metrics and a backlog derived from the supervisor's requests and observations; the checkpoint history with its durations, the job identity, the Pod inventory and the attempts' log announcements were already exported every poll.
Declined: reading the subscription backlog from Cloud Monitoring, which would need a new grant, a new request kind under the reservations and an approval change for a figure the source's `pendingAcks` and the output observations already bound; running the Java report in the supervisor, which would put a JVM into the lifecycle image for one computation; and counting a lost boundary as usable with a note, which would let a receipt claim a recovery trial that redelivered nothing.
The verdict's recomputation from the exported evidence is [#1625](https://github.com/flink-gcp/flink-connector-gcp/issues/1625)'s, refined below.

Refined under [#1625](https://github.com/flink-gcp/flink-connector-gcp/issues/1625), the second half of [#1432](https://github.com/flink-gcp/flink-connector-gcp/issues/1432): the offline analyzer recomputes a Pub/Sub run's verdict from its exported evidence, as it does a BigQuery run's, and classifies the run `usable`, `inconclusive`, `inconsistent`, `tampered` or `unexported` by the same rules, from steps the two sections now share so they cannot drift.
It rebuilds what the evidence alone can show: the output oracle from every collected batch, its lines derived again from the saved pull response by the collector's own function; the input identities from the runner's publication receipts, which check each output's input message ID against what was published for its input and sequence; the observation coverage from the measurement events; and the replay cohort's output from new attempts, through the predicate the exercise uses.
Whether the boundary held and which observations were collected before the fault are read from the completion record, because rebuilding them would re-run the exercise offline over the exported Pod logs and checkpoint statistics; they are held instead to the earlier transition records, which wrote the same outcomes as the trial reached them, to the approved trial, and to the observations, which show whether an attempt the fault names as running before it was the initial job's and whether a replay came under an ID no pre-fault attempt processed.
A partial download, a gap in a collector's batch counter, fewer lines than the record counted, or a completed run's output with no publication receipt, stops the comparisons that need what is missing rather than charging the record with it, after the comparisons that do not have run and after the verdict is asked over the record's own oracle, so a partial download cannot excuse a record that overstates itself.
Declined: recomputing the replay outcomes in full from the exported Pod logs and checkpoint statistics, which would re-run the exercise offline for a check the earlier records, the approval and the observations already bound.

### Pub/Sub lifecycle IAM preparation

Define the persistent custom roles and project bindings in OpenTofu, and keep per-run topic/subscription policies in the guarded runtime helper.
The runner needs project-level creation plus topic attachment, metadata, resource policy installation/readback and deletion.
The supervisor needs metadata, policy readback and deletion independently of runner progress.
These project grants are not prefix- or ownership-scoped; in particular, the runner can change any Pub/Sub topic/subscription policy and grant itself data access.
Do not describe runtime ownership checks as restricting that IAM authority.
This trades broader authority in the trusted provisioner for narrower data access in the workload; it is not a reduction in the combined authority of all identities.
A project-level workload publisher/consumer grant would avoid resource-policy writes, but would let a compromised job publish to or consume other runs' resources.
The selected design confines that job's data grants and trusts the separate runner with project-wide policy administration, including the ability to expose or revoke access on unrelated Pub/Sub resources.
This refines the Cloud Tasks lifecycle's no-IAM-write choice: its workload already has queue data grants, whereas the Pub/Sub plan installs distinct input/output grants on resources created per run.

A project binding with a resource-name condition was considered.
Google's [IAM attribute support table](https://docs.cloud.google.com/iam/docs/conditions-attribute-reference#resource.name) lists Pub/Sub Lite for resource-name conditions, but not Pub/Sub topics/subscriptions.
This design therefore does not rely on a name-prefix condition to constrain the runner; re-evaluate that choice only with verified service support.

The ownership manifest advances to version 2 and freezes data bindings before any resource creation.
The runner publishes to the two input topics; the workload reads metadata and consumes the input subscriptions and publishes to the output topic; the supervisor consumes the output subscription for external observation.
Define the metadata/consume custom role persistently without binding it on the project.
Install only on revalidated owned resources with empty explicit policies, require the returned etag on every policy write, and request policy version 3 on reads so conditional bindings cannot be silently overwritten.
Refuse unknown policy content, matching pre-existing grants, stale ownership, missing etags and ambiguous responses; do not merge policies, retry writes or resume partial installation.
Retain partial work for ownership-checked resource cleanup and re-read every installed policy before later admission.
The exclusive environment control must span resource/policy read-write gaps as well as deletion gaps.

The [application runbook](../../kubernetes/apps/pubsub/README.md#permissions-and-remaining-integration) records the exact grant table and API sources.
Explicit policy readback does not establish effective access or exclude inherited privileges; identity-specific probes and propagation handling remain mandatory before separately approved execution, and admission performs them as refined under [#1581](https://github.com/flink-gcp/flink-connector-gcp/issues/1581) above.
Version 1 manifests are not adopted or migrated; the earlier helper was not executed against live resources.
This stage prepares authority and internal operations, leaving shared lifecycle integration and deployed recovery acceptance to subsequent work.

### Ownership boundaries

There are separate state and application boundaries for GCP infrastructure, Kubernetes bootstrap, Helm and application runs.
A bootstrap-only or Operator-only PR receives OpenTofu checks and a visible tfaction plan within the plan job, with an access preflight.
After merge, CI applies the reviewed artifact and verifies an empty refreshed plan.
Failed applies use the existing follow-up PR workflow; a local pre-apply or stale-artifact retry is not the recovery path.
The earlier manual-bootstrap/CI-Helm draft split was revised because it left routine foundation changes outside PR plan/apply review.
The three idle quotas forbid Pods and PVCs throughout the foundation stage.
The bootstrap inventory is a preflight, not an exhaustive controller audit or lifecycle supervisor.
Image publication, runtime pin selection and bounded lifecycle tooling are established; a generic smoke execution still requires separate approval and live validation.
Cloud Tasks implementation/benchmarks, BigQuery-specific verification and Pub/Sub recovery trials remain outside this foundation change.

### Official Python SDK image dependencies

Bundle google-auth, google-cloud-storage and the Kubernetes Python client in the lifecycle tools image for the dependent lifecycle implementation.
Define runtime dependencies in the `flink-tier3` workspace member and resolve them in the root `uv.lock`.
Export the member's third-party dependencies with hashes into an ignored `target/requirements.txt` build input; omit workspace packages and development dependencies.
The image still installs only SDKs; the wheel is buildable but its source is delivered by ConfigMap, so code changes do not require another image publication.
The CLI selects that member, and the root test group includes it.
Install wheels at build time and validate imports before publication.
The shared runner and supervisor retain lifecycle policy; official SDKs supply authentication and API transport.
Publish through the existing reviewed-main workflow before adopting its GAR digest in the runtime change.
This image preparation does not change the selected runtime digest or admit a workload.

### BigQuery query handoff and release

Keep result collection on the submitting runner and communicate observation requests and evidence pointers through the existing conditional run record.
Bind the handoff to one runner token, explicit query evidence bytes and an absolute query deadline before any resource provisioning.
The caller authenticates actors and excludes token reuse by another process; the token is record identity, not authorization.
Serialize observation requests, reserve a new deterministic slot for each snapshot, and divide query artifact bytes equally among the slots.
The supervisor validates the archived object's generation, hash and trial/observation identity without relying on access to the runner's private result table.

Persist an in-flight marker before each runner operation and require an explicit permanent runner release before cleanup can invoke its external workload barrier.
Keep a creating operation's marker after any exception: a lost response or partially completed multi-table provisioning does not prove that every service operation has settled.
This deliberately sacrifices automatic cleanup after such failures; a process restart, elapsed deadline or matching token does not grant takeover or clear uncertainty.
Read and collection failures may clear their marker after returning because those operations do not create BigQuery tables or jobs.
Make at most three conditional completion-acknowledgement attempts, including the initial attempt, with an invocation ID that refuses to clear a later call after a lost write response.
Accept an already clear marker as acknowledged, but abort immediately on a different active invocation; retrying that invariant violation could hide it after the later call finishes.
Persistent control-storage failure still requires external recovery; never retry the resource operation as part of this acknowledgement.
An expired observation aborts further observations for the trial but still allows release and cleanup; skipping a required baseline is not a recovery strategy.
Stop and release set the run-global stop flag, so they terminate admission for the whole run.
The existing barrier still must fence writers, other creators and server-side work, and cleanup still waits for terminal query jobs.
Declined: treating the stop flag or an expired heartbeat as proof that the submitting actor can no longer create resources.

The common runner settlement loop now services an explicitly attached handoff while the supervisor runs, stops polling on failure or execution closure, and attempts permanent release before leaving its wait.
The supervisor can request archived query evidence while maintaining heartbeats and resource audits; the scenario supplies observation boundaries and interprets the result.
Common cleanup removes owned Kubernetes workloads before waiting for runner release and invoking service cleanup with a mandatory external quiescence callback.
An expired wait or unresolved call retains the control record and lock; a replacement runner cannot impersonate the original submitting actor or the cleanup supervisor.
Recorded BigQuery state gates Operator shutdown, shared completion transitions, idle verification and finalization even without an attached handoff.
Retain that entire state in the final receipt and refuse a conflicting receipt or a concurrent control change before deleting the control record.

These common-loop paths are covered by synthetic tests, including composition with the real handoff protocol.
Authenticated entrypoints, observation and recovery scheduling, and Kubernetes workload fencing remain subsequent integration work.
It changes no deployed image, grants or paid-trial authorization.

### BigQuery approval and shared resource policy

Use approval version 4 for the internal BigQuery execution contract.
Reuse the offline proposal's exact trial schema and derive the service resource plan from it, the approved run/nonce and table expiry.
The resource controller compares that whole plan before using the service adapter, so a valid but different query-slot count, byte limit, timeout or expiration cannot replace the approved one.
Retain the recorded-intent comparison as a separate guard against a replacement approval after initialization.

Require the proposal's 90-minute window and 15-minute cleanup reserve, with three equal Flink Pod shapes in `tier3-bigquery` and two running control Pods in `tier3-system`.
The shared replacement policy raises the total ceiling to six Pods by reserving an extra Operator slot in `tier3-system` while its previous Pod terminates.
Keep smoke and Cloud Tasks approvals and ceilings unchanged.
Include BigQuery in shared inventory only for BigQuery approvals; preserve the original smoke, Cloud Tasks and control namespace scope for versions 1–3 so recovery can reuse their saved baselines.
Use the BigQuery application image role and dedicated state bucket for audit and cleanup.
Generation-checked state deletion remains confined to the approved run prefix, and pending service cleanup still blocks shared completion.

Schema validation does not authenticate an approval or enable paid execution.
Keep the dispatch scenario excluded and add explicit runner/supervisor start refusals while authenticated delivery, cumulative query-evidence accounting, recovery scheduling and external writer fencing remain incomplete.
This permits real model/environment/controller composition in synthetic tests without silently falling through the smoke execution path.
The shared final receipt retains the BigQuery scenario and approved trial; at this stage its success stayed false even with a generic recovery completion flag present, because that flag is not this scenario's claim — the deployed verdict below is.
A BigQuery finalization retry compares all receipt fields except the refreshed plans' observation time (`plans.at`); it retains the original receipt and still requires the current plans to be empty for the approved nonce and all three roots.

### BigQuery approval-bound delivery

Generate a delivery from the version 4 approval using the existing proposal renderer.
Require its approved repository HEAD and unchanged Kubernetes inputs, rejecting additional CUE files even when Git ignores them.
Refuse source, application/upgrade or supervisor-image drift before embedding that approval.
Only the embedded approval and remaining relative Job deadline change from the proposed delivery; retain its source projection, identities and immutable manifests.
Require the ConfigMap data to remain within 1 MiB after adding the approval.
Verify by re-rendering against an independently supplied approval rather than trusting the bundle's embedded copy.
This is an offline binding check, not authentication, image availability, live resource admission or an absolute start fence.
Keep both execution entrypoints disabled until the executor supplies those checks and the actor/evidence protocol.

### BigQuery internal execution loops

Connect explicitly supplied BigQuery handoffs to the common runner admission and supervisor exercise paths while keeping production CLI admission disabled.
The caller still owns authenticated actor construction, approval-bundle verification and the external creator/writer barrier; this internal API cannot establish those facts from a token or a callback's existence.
The actor factories below now supply the first two for a caller that uses them; the barrier remains outstanding.
Initialize resource intent and provision tables only after supervisor and Operator readiness and before FlinkDeployment creation.
On failed workload admission, keep that original supervisor available until the submitting runner returns from admission and permanently releases its handoff through settlement.
Wait for this acknowledgement before workload inventory and teardown so admission can finish recording confirmed application creation; missing release remains an incident-recovery condition.
The acknowledgement does not settle an unknown application creation outcome: the runner must still reconcile its persisted intent, and the external creator/writer barrier remains required before service deletion.
A failure before readiness must not leave a BigQuery resource intent.
Require admission within the approved start's 600-second startup window.
A failed creating call retains its unresolved marker and requires external incident recovery under the existing protocol.

Reuse the generic recovery mechanism's UID/version preconditions and restore proofs through scenario attributes for namespace, bucket, input size, progress event, timing and Pod count.
Retain its smoke defaults.
The BigQuery subclass adds the proposal's warmup, baseline and post-recovery windows, validates its input lineage and queries final visibility only after both recoveries and finished input.
Recompute each archived oracle, retry missing visibility with a new approved slot under one absolute deadline, and never retry routing or EO uniqueness violations as visibility delay.
This preserves the distinction between recovery evidence and the complete measurement verdict.
Retain the recovery evidence in the final receipt but keep its overall BigQuery success false until measurement collection and the deployed verdict are implemented.

Require the fixed 10 MiB query-artifact allocation before admission, rejecting both larger allocations and smaller ones that may be unusable.
Reserve it by reducing version 4 supervisor and runner receipt budgets to 80 MiB and 8 MiB respectively.
Admission and supervision reject a handoff with a different query budget, plan or environment, or a query deadline other than the approved cleanup start.
The remaining 2 MiB under the 100 MiB policy is for immutable run artifacts; the future authenticated admission path must account for those artifacts explicitly.
Declined: adding the query budget above the existing receipt allowances, admitting the CLI with a placeholder writer fence, or treating passing synthetic recovery as issue acceptance.

### BigQuery operation deadlines

Carry the lifecycle operation's deadline through the handoff and controller to the REST adapter.
Provisioning uses the earlier of the approved start plus 600 seconds and the query window's end; submission, status and result pagination use the requested observation deadline.
Cleanup receives its own deadline from the common cleanup window, so it can cancel queries and delete tables after query admission closes.
Use separate adapter views over the same session, plan and clock, capped by the original adapter deadline, rather than mutating that shared deadline or resetting a timeout for each page.
Keep observation timing in the shared policy module so handoff construction does not import the offline rendering workflow.

Check expiry after receiving response headers and at streamed-body boundaries, including empty bodies and absent resources.
A late response cannot establish a successful resource operation; retain the existing unresolved-create and returned-read failure rules.
These checks bound request admission and the timeout passed to the transport, not credential refresh, a blocked read or server-side execution by themselves.
The authenticated session below supplies credential/identity request budgets; the external creator/writer barrier and live acceptance remain required before CLI admission.

### Delivered package subset

Deliver the modules the supervisor entrypoint can import, closed under their own imports, and `policy.toml`; not the whole installed package.
The ConfigMap has one consumer and one fixed command, while every module any scenario adds was carried by all of them against a shared Kubernetes data ceiling that a Cloud Tasks session had reached.
Compute the set by walking the package's own imports from the entrypoints, and declare the CLI's computed command dispatch as the one edge that walk cannot see.

Keep the approval's `runtime_sha256` over the complete installed package, which the runner verifies at dispatch, and add a second pin over the delivered set, which is the only thing the supervisor can verify from its mount.
One digest over the delivered set was declined once measured: it would have changed `runtime_sha256` for an already approved preregistration and made that document's protocol-coverage claim false, to save a field.
Because the delivered set is closed under imports, the runner computes the delivery pin from its complete installation and the supervisor reproduces it from the mount, so a truncated delivery fails that comparison instead of running.
A module nothing delivered imports is neither delivered nor pinned by the delivery digest, and becomes both as soon as a delivered module imports it; `runtime_sha256` covers it either way.
Follow a delivered module to the package data it names by literal: a delivery whose data stayed behind is the one incompleteness both sides would compute the same delivery digest over, so no comparison would catch it.
Declined: per-scenario delivery sets, which the measurement showed are not needed to clear the ceiling and would make the delivered set vary by scenario.
Refined under [#1628](https://github.com/flink-gcp/flink-connector-gcp/issues/1628): two imports carried modules only the checkout commands use into every delivery, and the 32-cell Cloud Tasks ConfigMap data measured 774,052 bytes against its 786,432-byte bound.
The CLI, a walk entrypoint, imported the four modules whose repository roots it set from `--repository`, which delivered `lifecycle` and `schemas`; and `workflow`, which `bigquery_plan` and `pubsub_plan` import for rendering, imported `bootstrap` for the cluster client and the root plans, which only the lifecycle commands call.
The repository root now lives once, in `repository`, which the CLI sets and every reader consults when it uses it, and the cluster client and the root plans moved to `lifecycle`, their only caller, so `bootstrap` again imports no other module of the package but `repository`.
The delivered source falls from 666,817 to 594,782 bytes and the same ConfigMap data to 702,017 bytes.
Tests require that no delivered module imports `bootstrap`, `lifecycle` or `schemas`, that `repository` is the only module binding the name `ROOT` or reading the working directory at import and nothing imports the name, which would bind the working directory before the CLI sets it, and that every lifecycle command still reaches its handler.

### BigQuery authenticated actor construction

Provide internal context managers that bind a caller's independent environment approval to the runner bundle or supervisor manifests, installed source, role and current lock owner.
The runner performs the existing full bundle verification; the in-image supervisor checks approval-bound source and manifest hashes without requiring Git or CUE.
Check the source pin each actor can reproduce from the tree it runs on: `runtime_sha256` for the runner's complete installation, and `delivery_sha256` for the supervisor's mounted subset, which is a strict subset of what the first covers.
Neither authenticates the approval's origin or the supplied Kubernetes/storage clients.
Retain the caller-supplied original runner token and mandatory external supervisor barrier; a credential session cannot establish process quiescence.
Close the session on context exit without synthesizing release or cleanup.

Authenticate the exact OAuth bearer header against Google's userinfo endpoint and require the fixed role-specific service account's verified email.
Recheck when the bearer header changes rather than trusting a credential object's configured email.
Use a synchronous `google-auth` request adapter whose credential exchanges, identity checks and BigQuery request share the caller's remaining timeout, capped at 20 seconds.
Refuse nonblocking refresh, redirects, identity mismatches and caller transport overrides; disable environment-derived proxy configuration and do not replay BigQuery calls after a 401.
Credential-library retries use the same adapter budget, but synchronous discovery/code and blocked reads are not forcibly interrupted by this mechanism.
The adapter reaches every token refresh and the in-cluster metadata path, and does not reach credential discovery that builds its own transport: in the pinned `google-auth`, `default` forwards the adapter only to its Compute Engine checker, so an external-account credential file resolves its project through a library-constructed transport carrying that library's timeout, proxy and redirect behaviour.
The two actors do not share one identity path: the supervisor is a Kubernetes service account bound to its Google service account, while the runner authenticates through workload identity federation in CI and therefore takes the external-account path.
Measured against the pinned library: discovery makes no request at all when the environment supplies a project, and enters the credential exchange on that library-constructed transport when it does not.
State that limit rather than claiming a budget the discovery path does not honour.
Keep service-account/WIF/GKE acceptance, authenticated dispatch, the actual writer fence, immutable artifact accounting and deployed measurements outstanding; synthetic SDK/HTTP tests do not establish those facts.

Both factories validate the approval against the current clock, so neither actor can be constructed once the admission window closes at the cleanup start.
The cleanup window therefore runs inside a context opened before it; replacing a supervisor inside that window is not a recovery path, and the entrypoint work owns whether to make it one.
Keep the production entrypoints disabled and the final success verdict false until the remaining contracts are implemented and accepted.
Declined: reusing the shared `GoogleToken` transport that Cloud Tasks, the image workflow and the Kubernetes transport already use, because its adapter timeout and its eager discovery are both fixed at construction and every one of those callers would inherit a change to either; this leaves BigQuery the one scenario with its own session, deliberately. Also declined: narrowing the session's request signature so the overrides become unexpressible, which would move the refusal out of the layer that owns the destination.

### BigQuery quiescence barrier

Build the cleanup barrier from the run's own identity inside the authenticated supervisor factory, rather than accepting a callable from the caller: a `callable` check cannot distinguish a proof from a constant.
Prove, from a listing taken on every call, that no object owned by this run's application roots, of a kind that runs or restores a writer, remains in the application namespace; include the controllers, because an empty Pod list at one instant is not the same as no writer, and re-list rather than trust a snapshot, because a replacement between two calls is exactly what a snapshot would miss.
Scope the seed to the application roots and the listing to the application namespace, both: the supervisor asking the question runs under a root in the control namespace, and the observed set is not namespace-scoped.
List controllers before Pods, so a Pod created by a controller collected between the two pages is still caught.
Retry an unreadable cluster inside the call rather than reporting it, bounded by a count of consecutive attempts rather than by the cleanup window, which is recomputed per pass and whose static field the run has usually already passed: the predicate asks twice per poll and the second asker converts anything but true into a failure, so "unknown" is not expressible and a single transient read would otherwise strand the environment lock.

State what it does not prove, because nothing available can: an in-flight server-side append, and an open buffered write stream.
The Storage Write API exposes no call that lists a table's streams, the connector deliberately never finalizes them, and their server-assigned names never leave Flink's state, so this clause cannot be discharged by observation with this client, transport and grants.
Rest exclusivity beyond the listing on the grant instead: the sole principal holding `bigquery.tables.updateData` is the workload service account, reachable only through the service account in the namespace just shown empty.
Keep the measurement independent of it — the query oracle already reads after the job reached `FINISHED` and its post-recovery window closed — so the barrier's job is that cleanup does not delete a table while a Pod that could write to it is alive, and the window to `tables.delete` remains recorded as outside this component.
Declined: inferring quiescence from Pod disappearance alone, which the runbook already forbids; reading `streamingBuffer`, `numRows` or `numBytes` as an idleness signal, all eventually consistent and, for the buffered path, blind to the unflushed tail by construction; treating appender logs as proof, since they are best-effort and unreadable once the Pod is gone; and revoking the writer's grant, which no lifecycle actor is granted and whose propagation would not prove an in-flight token had stopped.

### BigQuery evidence partition and query spend

Hold the four allowances that divide the BigQuery evidence ceiling as policy entries rather than as literals in the modules that enforce them, so the partition is visible as a sum: supervisor receipts, runner receipts, query artifacts and the immutable run documents.
Weigh each immutable document against its own allowance before writing it, counting only the documents directly under the run prefix; the receipts and query artifacts live below it and answer to their own entries.
Count from a fixed roster of document names rather than by listing that prefix, which also holds every receipt: the listing refuses past twenty thousand objects, a supervisor's receipts reach that long before their byte ceiling, and this runs as the final result is written.
A run that would exceed the allowance fails at the write rather than retaining more than it was approved to.

Sum the collected queries' billed bytes into the retained state.
Each query is already refused above its own byte limit and the slot count bounds the worst case, but a bound is not a measurement: without the sum a finished trial cannot state what it spent, which the preregistration's cost row is meant to be checked against.
Record each observation's own figure and restate the total from them, rather than accumulating into it.
A conflicting write re-reads and re-applies the same edit, so accumulation would bill one query once per attempt; assignment is the idiom the rest of this controller already uses for exactly that reason.
Declined: deriving the cost from the plan's ceiling rather than the jobs' reported bytes, which would restate the approval instead of measuring the run.

### BigQuery deployed measurements

Sample the sink from inside the exercise's existing observation windows, at its own interval rather than once per poll — one sample is six REST reads, and at the transport's timeout a slow endpoint would otherwise spend a large part of a window — through the REST service the recovery loop has already resolved and proved owned; an exercise that resolved its own could disagree with that proof, so the loop hands it over through a generic seam rather than the BigQuery exercise reaching for it.
Read what the issue's second acceptance item names and what nothing in this repository collected: TaskManager memory beyond the heap, because this sink appends through native buffers; the task-level buffer-pool and byte-rate metrics, so throughput can be read against whether the network was the limit; and the connector's own gauges, which are what "active writers" means here.
Discover the sink's metric ids by listing and union them across samples rather than freezing the first answer: a task registers its metrics at deploy and the sink's operators theirs at open, so a listing taken between those moments holds the task names and none of the connector gauges, and freezing it would drop the active-writer observation silently.
Identify the source vertex by name rather than by position, and report a plan that does not hold one of each rather than guessing.
Take no sample on a poll where the loop resolved no Service: a reading through the previous one would be attributed to a job that is not the one running.
Record an absent reading as unavailable with its cause instead of dropping it: a missing sample and a zero reading answer the question differently.
Keep the shared metric primitives in their own module, so a second scenario can sample without importing the Cloud Tasks session observer, which imports the supervisor.
Declined: extending the cell-shaped session hooks to a scenario that has no cells, and re-resolving the REST service inside the exercise.

### BigQuery deployed verdict

Decide the run's verdict from the recovery proofs, the query oracle and the observation coverage together, and make the receipt's BigQuery success that verdict rather than the generic completion flag.
`usable` says the instrument worked; `inconclusive` is a run that happened and cannot carry the claim, which is the distinction the Cloud Tasks analyzer already draws, so the labels and the excluded statuses move to the shared module rather than being restated — the analyzer is not part of what the supervisor mounts, and importing it into a delivered module would carry it and its 117 KB protocol table there.
Require every observation family in the steady window and again in the post-recovery window, each window standing on its own, and carry the reasons naming the family and the window so an inconclusive run states what it lacked.
Credit a family only where a reading returned it: discovery never drops a metric id, so a request for a gauge the operator no longer publishes still succeeds, and crediting the request would report a name as a measurement. A window that recorded no sample observed nothing, whatever its coverage claims.
Report no measured value in the verdict: a throughput or memory figure is a finding to publish, and encoding one as a threshold is what #1312 forbids.
Keep coverage monotonic within a window and never across one, and decide the verdict with the transition that completes the run so it reaches the evidence record rather than only the receipt.
Recompute that verdict offline from the exported readings instead of reading the receipt's answer, so a reader holding only the evidence can tell a run that earned its verdict from one whose receipt asserts it.
Rebuild the coverage by folding the exported readings again rather than reading the record's own `coverage` field, and decide over what they support: recomputing from a field the same hand could edit restates the record to itself and refutes nothing.
Separate the two accusations, in one direction only: a record claiming an attempt, a family or a `usable` verdict its readings deny, or a receipt claiming a `success` the runner computes from that verdict alone, is overstating itself, which is tampering, and every other disagreement — a conservative stored verdict, a completed record short of its own readings, two completion records, a receipt at odds with the evidence — is inconsistency. Under-reporting is not forgery.
Treat a sample record the fold cannot read or place as an incomplete export rather than dropping it or accusing the run, because the lost reading lowers the rebuilt coverage and would read as the first accusation; a truncated download is re-fetched, not reported. Name a record claiming the completed stage with no completion evidence beside it, because an absent object is named only by the record it should have accompanied — but as a problem rather than a refusal to look further, since a fabricated receipt has that same shape and only the readings tell the two apart. Report the severe label when both are gone, and accept that a truncated export is then indistinguishable from a fabrication: re-fetching clears the one case, and calling a forgery benign clears nothing.
Accept that this raises the cost of a forgery rather than closing it: the readings are files too, so fabricating one per window still passes. What it removes is the single-field edit that the earlier recomputation could not see at all.
Exclude only evidence problems: a run that did not complete exported its account of itself and is inconclusive, not unexported. Check the receipt's `success` in one direction only, because it is a conjunction the runner may refuse for reasons of its own.
Accept that the recomputation uses the rule set as it stands and cannot tell rule drift from forgery, and read archived evidence with the revision that produced it, rather than pinning a rule version the same forger could edit.
Declined: passing on a completed recovery alone, which an earlier revision of this ADR already refused for this scenario; and treating the query report as the measurement verdict, which says nothing about whether the run was observed.

### BigQuery production entrypoints

Admit `bigquery-recovery` at the dispatch boundary and build its supervisor in the in-cluster entrypoint, so that a run is reachable through the reviewed workflow rather than only through actors a test constructs.
This meets the condition the approval-bound delivery section set — the executor now supplies the authenticated actors, the verified bundle and the actor/evidence protocol — and revises the one the authenticated actor construction section set, which kept the entrypoints closed until the remaining contracts were *accepted*: enabling them is not that acceptance and not an execution approval.
A paid run still needs a reviewed trial file, a republished application image, the environment lock and the typed phrase, and the first run is itself the acceptance evidence.
It also lifts the exclusion the approval section kept, and the one the internal execution loops kept, while authenticated delivery, query-evidence accounting, recovery scheduling and the external writer fence were outstanding: the delivery and authenticated actor sections supply the first, the evidence partition section the second, the internal execution loops the third and the quiescence barrier the last, and the bare `Runner` and `Supervisor` refusals the approval section added remain.

Mint the runner token in the dispatching process and nowhere else, and construct the supervisor without it.
The supervisor's Pod starts before the runner writes the binding, and every channel that could deliver the token to it — the approval, the ConfigMap — is one the recovery workflow builds its actors from or reads, so a token there would let that workflow match the binding without meaning to and act as the runner whose release lets cleanup delete tables.
The token guards against that accidental match, not a deliberate one: the binding is written to the control record, which anything that can read the evidence bucket can read, and the recovery workflow constructs no BigQuery handoff.
The supervisor binds the fields its own approval fixes and adopts the token from the first binding it reads; `initialize` writes that binding once and refuses a different one, so the first binding is the submitting runner's, and a replacement is refused from then on.
Write the resource intent and the handoff binding in one record change.
They were two writes, and a stop or crash between them left an intent that release could not clear and cleanup would not delete, holding the environment lock until incident recovery; an intent without a binding now arises only from a bare controller, the runner refuses to adopt one, and it still requires that recovery.
Declined: reusing the approval's `nonce` as the token, because the recovery workflow constructs its runner from that approval and would hold a matching binding without trying; and delivering a dedicated token through the ConfigMap, which fixes it earlier but publishes it to the cluster and changes the delivery contract for a check the adoption already makes.

Start the window at admission, on the whole second, and treat the typed expiry as the latest the run may end: dispatch admits an expiry 90 to 100 minutes after admission.
Declined: deriving the start from the typed expiry, which the approval's exact 90 minutes and the bundle's start both allow, because the queueing it would absorb comes out of the 600-second startup budget every deadline counts from.
Bind the approval phrase to the trial file's own cost rather than the scenario's maximum, so the operator types the number that will bind the run.
Give the workflow job a 120-minute timeout for this scenario: the 90-minute window's serial budget is 114 minutes, and the 85 minutes it would otherwise have fallen into would kill the job mid-trial; a test holds the budget against the timeout.
Keep the bare `Runner` and `Supervisor` refusing without their authenticated handoff, so the only path in is the actor factories.

### Spend is approved before dispatch

The owner approves a run's spend before it is dispatched, from its estimate, and then dispatches it or does not; nothing at run time manages the budget.
So no scenario gates execution on cost: approvals carry no `additional_cost_usd` ceiling, admission compares no estimate against one, the confirmation phrases name the run without an amount, and neither `environment.pricing_reviewed` nor `bigquery_plan.REVIEWED_AT` closes admission when it ages.
Those review dates stay as the recorded basis of an estimate, and `estimated_cost`, `estimated_session_cost` and `bigquery_plan.estimate` stay as the numbers the owner approves.
This replaces the reviewed trial file and the phrase bound to its cost in the production entrypoints section above, the Pub/Sub proposal's separate cost cap, and the 30-day pricing refusal the approval model applied to every scenario.

A BigQuery trial therefore chooses only what differs between trials, its delivery method and destination count, and dispatch takes it as a workflow choice (`alo-10`, `eo-10`, `alo-50`, `eo-50`, and `fl-10` since [#1551](https://github.com/flink-gcp/flink-connector-gcp/issues/1551)) rather than as a reviewed file; the query budget is the scenario's, fixed in code.
A reviewed file per dispatch existed chiefly to carry the per-trial cost, and keeping those numbers beside a preregistration record needed a test to hold the two together.
The proposal also drops the repeated-trial ordinal the recovery application section binds: nothing consumed it, and a repetition is a separate run ID.
The estimates the owner approves are USD 0.81 for a smoke or generic-recovery hour (`estimated_cost`), the session's `estimated_session_cost`, which its preregistration states, and USD 2.35 per BigQuery trial (`bigquery_plan.estimate`), which the rendered proposal carries.
The BigQuery campaign, its estimate, stop conditions and cleanup checks are preregistered in the [BigQuery trial preregistration](evidence/0165-bigquery-trial-preregistration-1312.md).
The FILE_LOADS trial of [#1552](https://github.com/flink-gcp/flink-connector-gcp/issues/1552) has its own, the [BigQuery FILE_LOADS trial preregistration](evidence/0165-bigquery-fileloads-preregistration-1313.md), which also prices what that mode adds outside `bigquery_plan.estimate` — free load jobs and short-lived staged objects, expected to fit the estimate's reserve.

### BigQuery deployed trial findings

All four preregistered trials reached a `usable` verdict on 2026-09-25, as the [BigQuery trial findings](evidence/0165-bigquery-trial-findings-1312.md) record: ALO and EO at 10 and 50 destinations each passed a savepoint upgrade and a JobManager failover while the sink was active, and the query oracle found every expected sequence exactly once and routed to its own table.
The deployed rig is therefore accepted as the instrument for these trials, and its measured values are findings, not targets.
The campaign took fourteen admitted attempts: nine exposed rig or environment defects, each repaired in its own change before the next dispatch, one was lost to a Spot reclamation, and two needed manual repair of the environment lock.
The fixes the deployment required are recorded in their paragraphs of this ADR: the startup and input budgets, the staged-upload grant, the read retry, the upgrade transition, the per-pass control writes and the removed capacity gate.
The regional `SSD_TOTAL_GB` quota is also a precondition: an upgrade needs replacement nodes while the old ones drain, and at 500 GB a 50-destination trial could not provision them.

An approval written before this change still carries the removed ceiling, and a BigQuery one the old trial schema, so the model refuses it and recovery could not settle it; the change is therefore merged only while no run holds the environment lock, rather than carrying a reader for a shape no future run writes.
Keep everything that stops and cleans up a run, which is where a failure costs more than the run: the environment lock, idle and cleanup verification, recovery, the three empty plans and image retention.
Declined: keeping a USD 10 scenario ceiling as a backstop, because the owner already approves the estimate itself, and a second number beside it only decides which approved runs the rig refuses; and keeping the pricing refusal as a freshness prompt, because it turned a stale rate table into a dispatch deadline without anyone checking the rates.

### BigQuery FILE_LOADS deployed trial findings

The one preregistered FILE_LOADS trial, `fl-10`, reached a `usable` verdict on its first attempt on 2026-10-10, as the [BigQuery FILE_LOADS trial findings](evidence/0165-bigquery-fileloads-findings-1313.md) record: across a savepoint upgrade and a JobManager failover the query oracle found every one of 1,843,200 sequences exactly once in its own table, and the run left no unfinished job, temporary table or staged object.
The failover landed inside a commit whose load jobs had finished server-side, and the restored committer completed it by re-attaching to them, as the upgrade's restored committer re-attached to the savepoint commit's jobs; nothing was resubmitted.
That is what this trial establishes for the correctness half of [#1313](https://github.com/flink-gcp/flink-connector-gcp/issues/1313); no load job was still running at either recovery, and [#1685](https://github.com/flink-gcp/flink-connector-gcp/issues/1685) owns that case.
The writer's end-to-end checkpoint duration exceeded its barrier delay and synchronous and asynchronous parts by 1.2–3.1 s, an upper bound on finalization, and the commits a sample caught took 5.3–11.5 s of a 120-second interval; finalization concurrency, staging-file size and the format comparison remain with [#1553](https://github.com/flink-gcp/flink-connector-gcp/issues/1553) and [#1554](https://github.com/flink-gcp/flink-connector-gcp/issues/1554).
The attempt's receipt says `success: false`, because the supervisor's evidence-failure flag was set during cleanup for a cause nothing retained; it was accepted under the preregistration, whose receipt and `just tier3-analyze` agree on `usable`, and [#1683](https://github.com/flink-gcp/flink-connector-gcp/issues/1683) is filed to retain such a cause.

### A run may check out a chosen rig commit

The run workflow stays dispatched on `main`, so the runner's Workload Identity binding and recovery are unchanged, and may check out another rig commit through `rig_sha`.
Confirming a rig fix against the service no longer waits for its merge: on 2026-09-23 two such defects each cost a full review and merge before one dispatch could confirm them.
The commit must equal the approved `reviewed_sha` and head a branch of this repository, which `main`'s own workflow verifies before any rig source is checked out; a check inside the rig would be the rig vouching for itself, and a fork's commit is reachable through its pull request ref.
The lock owner records the rig commit as `rig_sha` beside the workflow commit that recovery verifies, and the approval binds the rig commit.
This runs unreviewed code with the runner's identity, which the owner accepted for speed; whether a run from an unmerged commit counts as a measurement is recorded with the run.
Declined: letting the workflow itself run from a branch, which would widen the runner's Workload Identity condition from `main` to any ref.

Dispatch no longer checks the cluster's node count.
The one-node refusal assumed an idle cluster settles at zero nodes; on 2026-09-24 one settled at one node carrying only system Pods for over three hours, the refusal held every attempt for two hours, and only a hand-made placeholder Pod that forced a second node let pilot `bq1312-alo-10-a6` dispatch.
The risk it guarded remains: on a one-node cluster kube-dns, which is system-critical and outranks any PriorityClass a workload may set, preempted the supervisor of pilot `bq1312-alo-10-a2` and four in [#1246](https://github.com/flink-gcp/flink-connector-gcp/issues/1246).
In each of those five cases kube-dns took the supervisor about 30 seconds after it started, while the cluster scaled up and before application admission, so the run stopped with no BigQuery query spent and an immediate redispatch found the cluster already scaled up; a preemption later in a run would stop that run like any other supervisor loss.
Declined: a required anti-affinity keeping the supervisor off kube-dns's node, because the scheduler may still preempt the lower-priority supervisor to place a later kube-dns replica on the supervisor's node, so it does not guarantee the protection; and a rig-created placeholder Pod forcing a second node, for the RBAC, cleanup and deadline handling it would add.
The node listing and its one-permission `tier3NodeReader` custom IAM role are removed with the refusal.

A supervisor Pod the infrastructure takes away is replaced, and the replacement may carry the run until supervision starts ([#1396](https://github.com/flink-gcp/flink-connector-gcp/issues/1396)).
A `podFailurePolicy` counts a Pod with the `DisruptionTarget` condition, which preemption, eviction and Pod garbage collection set, against a `backoffLimit` of two, and fails the Job on any other non-zero exit, so a crash still ends the run at once.
With that policy the Job's `podReplacementPolicy` is `Failed`: a replacement starts only after the Pod it replaces is terminal, when the system quota's one supervisor slot is free again.
Each supervisor Pod claims the run in the control record, and every control write it makes after the claim checks it inside the record's compare-and-set, so a write from a Pod that has lost the claim is refused against the very generation it would have replaced.
That refusal is deliberately not a lifecycle failure: cleanup records a failed control write and keeps deleting, which would let a displaced Pod delete the holder's workload and queue, whereas the refusal leaves the process before cleanup acts.
For the same reason a supervisor about to clean up a run still open for replacement first closes it with a stop request, and stops without cleaning if that write fails.
The takeover window closes when a stop, an evidence failure or cleanup is recorded, or when the heartbeat that sees admission complete records that supervision has started; that heartbeat is fenced like any other write, so of a holder and a racing replacement exactly one proceeds.
A Pod displaced before then stops at its next write.
A Pod signalled before then leaves the run open instead of requesting a stop, because the signal is the infrastructure taking the Pod rather than a verdict on the run; the exception is a run with provisioned BigQuery resources, which only the supervisor's handoff can clean, so that Pod still stops and cleans.
The runner stops admitting once the supervisor Job has ended, and settles it as before if no replacement comes.
A Job waiting to replace a Pod is not finished, and a replacement may stay unscheduled, so the runner also settles once no live Pod holds the claim: at once after supervision has started, when no replacement could claim the run, and before that once the later of the claim and the last heartbeat is older than the readiness allowance a first supervisor gets.
That decision is itself a stop request written only if the Pod the runner saw still holds the claim, so a replacement that claims in between keeps the run, and none can claim it after the stop lands.
The runner and recovery never claim, so their writes are not fenced, and the runner reads a supervisor as ready only when the Pod holding the claim is running.
Every cell's ledger claim comes after supervision starts, so only the holder can take a cell.
The Pod ceiling does not count a supervisor attempt the Job retains once it is terminal, since it holds no capacity and the backoff limit bounds how many there are.
A preemption after supervision starts still ends the run, because the cells, recovery stages and polling state it holds live only in its process.
Declined: resuming a session between cells, which would have to persist the per-cell meter, the outcomes and any cell in flight; and a plain `backoffLimit`, which retries a crash as readily as a preemption and starts the replacement while the old Pod terminates, where the system quota refuses it.
The measured cause is unchanged and the supervisor's whole-node request still addresses it; this change makes a preemption survivable, not rarer.

The BigQuery exercise waits 1200 seconds from the approved start for input to flow; admission keeps its 600-second startup window.
Pilot `bq1312-alo-10-a4` started from no nodes and was admitted four minutes in, after which Autopilot provisioned a node for the JobManager, following a zonal quota refusal, and then one for the TaskManagers; its first input progress was logged 16 seconds before the 600-second budget expired, and the next supervisor poll found the budget spent, so the run stopped at its baseline stage with no query spent.
The phases still fit the window: 1200 seconds to input, 180 of warmup, 600 of baseline, two 300-second recoveries, 600 after recovery and 600 for visibility take 3780 of the 4500 seconds before cleanup.

The BigQuery state grant also covers `.inprogress/flink-gcp-tier3-bigquery/runs/`, and BigQuery cleanup deletes the run's objects there.
Flink's GCS recoverable writer stages each upload under `.inprogress/<bucket>/<object>/` in the same bucket before composing it into place, so a `runs/`-only condition refused pilot `bq1312-alo-10-a5`'s first checkpoint `_metadata` with 403; the job restarted without a checkpoint, and the supervisor stopped the run on the fresh input lineage.
The staging prefix repeats the run path, so the grant stays run-scoped; the other scenarios' workloads hold unconditional Object User on their buckets and never met the condition.
