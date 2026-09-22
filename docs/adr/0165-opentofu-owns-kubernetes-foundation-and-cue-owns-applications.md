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
- Issues: [#38](https://github.com/flink-gcp/flink-connector-gcp/issues/38), [#1307](https://github.com/flink-gcp/flink-connector-gcp/issues/1307), [#1308](https://github.com/flink-gcp/flink-connector-gcp/issues/1308), [#1246](https://github.com/flink-gcp/flink-connector-gcp/issues/1246), [#1312](https://github.com/flink-gcp/flink-connector-gcp/issues/1312)
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
The [Cloud Tasks measurement publication](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/35474834488) then built main commit `339d0a90675dffee74305cebe021093c6a1f9b4b` on 2026-09-19 UTC and republished the lifecycle-tools and smoke images alongside the measurement applications, so the images package now selects that pair and the Operator and Flink pins remain unchanged.
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
Read all package modules and policy through Python package resources and pass them to CUE as JSON input.
Project them through one immutable system-namespace ConfigMap; the Pod runs the package CLI without startup installation.
This removes CUE's manually maintained source-file inventory and the script loader's file-path/import-order dependency.
The approval's `runtime_sha256` now hashes the complete bundle's relative paths and file hashes, preserving source/configuration identity after the requested module split.
Use the official Kubernetes, Cloud Storage and Google Auth Python clients for API transport and authentication, with dependencies installed in the digest-pinned lifecycle image before publication.
This replaces the initial standard-library-only transport choice at the owner's request; policy-only source changes still do not require rebuilding the image.
Manage Tier-3 as a buildable uv workspace member with its own runtime dependencies and `flink-tier3` console entry point, sharing the root `uv.lock`.
Move the bootstrap/schema helpers and their tests into that member and remove their old script entry points.
Repository checks and release helpers remain in `scripts/` until their planned `tools/checks` and `tools/release` member migrations; shell programs remain there.
The installed CLI requires an explicit checkout or the repository root as its current directory for CUE/OpenTofu files; the supervisor does not require a checkout.
Retain explicit UID/generation preconditions and timeouts; disable automatic SDK API retries and optional background bucket-metadata lookups.
Restart a complete GCS read up to five times when the observed generation disappears or changes between metadata and download; distinguish that concurrency from an initially absent object.
Use native SDK downloads and pools instead of custom response-size wrappers; generic API bodies no longer have an 8 MiB memory cap, while inventory count limits and Pod log byte limits remain.
A standard authorized session is still injected into GCS to preserve disabled proxies, redirects and rejected-request refresh retries; public Client options do not expose those settings.
GCS uploads disable the SDK's checksum-triggered automatic deletion path so every deletion remains an explicit generation-checked lifecycle operation.

The runner owns admission after observing a ready supervisor: it opens quota, scales the Operator to one, waits for readiness, and creates the application.
The ordinary smoke supervisor only observes and writes toward idle, so recovery can use the same UID-checked cleanup concurrently after admission's final write.
The separately selected generic recovery exercise permits the two additional, object-conditional operations described below.
Publish `running` only after the runner finishes admission and records the application UID.
Before that phase, a supervisor failure requests stop and leaves settlement to the runner or completed-execution recovery; cleaning sooner could stop the Operator ahead of an in-flight application create.
This uses the existing phase boundary rather than adding another claim or termination-proof protocol.
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
The separate #1311 approval and live exercise still owe evidence of actual GKE behavior.

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
Operator 1.15.0 reports the old job as `FINISHED` after stop-with-savepoint; tolerate it only during that job's `UPGRADING` reconciliation, without treating it as final input completion.
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
The runner pre-creates matching tables and both lifecycle actors can inspect, query and delete them; the supervisor cannot create tables.
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
Require an external barrier covering all creator/writer processes and server-side creation requests before cancellation and deletion; this component does not implement that barrier.
Pending or unreadable submitted jobs prevent table deletion, and component cleanup does not assert whole-environment idle state.
Synthetic generation-conflict and restart tests hold these controller contracts; BigQuery approval validation, authenticated actor integration and evidence budget enforcement remain caller work.
Before wiring the actors, render one unapproved execution proposal from explicit trial inputs.
Bind the resource plan, repeated-trial ordinal, fixed finite input, proposed windows/cost and initial/upgrade/supervisor hashes in the ConfigMap alongside an empty approval document.
The CUE delivery reuses the BigQuery package for both phases and retains the phase-only savepoint transition.
The offline renderer accepts this scenario, while lifecycle execution entrypoints continue to reject it.
A declared revision and digest-shaped image are inputs to later provenance checks, not publication or execution evidence.
Keep the cost calculation a planning estimate with an explicit reserve; it cannot bound service bills or replace final resource and cost approval.
Updated image publication, digest adoption, bounded admission/query execution and complete cleanup remain separate preparation and acceptance steps for [#1312](https://github.com/flink-gcp/flink-connector-gcp/issues/1312).

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

### Pub/Sub DataStream application preparation

The [DataStream recovery application](../../kubernetes/apps/pubsub/README.md) follows the applied namespace and idle Operator foundation for [#1361](https://github.com/flink-gcp/flink-connector-gcp/issues/1361).
Its opt-in build targets the Flink 2.2.1 / Java 17 runtime and relays two pre-created input subscriptions into one pre-created output topic with production source/sink builders and workload ADC.
The job has no resource-administration authority; the external provisioner must record exclusive run ownership and settings before admission, and cleanup must refuse unproved ownership.
Service permissions, workload admission and independent lifecycle supervision remain subsequent implementation.

Preserve four distinct identities: logical input, input Pub/Sub message, observer processing call and output Pub/Sub message.
A fresh observation UUID per processing call distinguishes repeated input processing from repeated publication of one already serialized observation; retaining output message IDs distinguishes either from the collector's own redelivery.
The offline oracle checks completeness and reports all four duplicate populations without treating deduplicated completeness as exactly-once behavior.
Its evidence limits bound local analysis, not service cost or workload execution.

The checkpointed union-state guard fixes the run and logical input domain while permitting parallelism one/two rescaling with stable operator UIDs.
Local emulator/MiniCluster tests exercise both DataStream RPC paths, TaskManager-loss checkpoint recovery and savepoint restoration in both directions.
They establish neither service replay timing nor deployed recovery; a later independently observed trial must prove the fault's completed-checkpoint boundary and expected replay cohort.
Table entry points and actual service-resource lifecycle remain separate acceptance work on the parent.

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
Retain failed or ambiguous invocations rather than interpreting client timeout as service quiescence.
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

Before enabling runnable Pub/Sub admission, render one unapproved DataStream trial with the existing CUE application and supervisor delivery.
Keep JM and active-TM replacement separate, with unchanged manifests at parallelism two; savepoint rescaling changes one to two or two to one and requires restored state in the upgrade phase.
Freeze the run/nonce, supplied source/image identities, installed runtime digest, exact manifests, service settings and grants.
Divide each subscription's finite sequence domain into disjoint pre-recovery and post-recovery cohorts, using at most 100 messages per publication.
The cohorts define publication intent, not a measured uncheckpointed replay population.

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
Recovery's placeholder control record finalizes a run that never reached service intent, and a measured retry proves it cannot finalize one whose cleaned Pub/Sub portion it does not carry.
That run's retry needs the verified snapshot restored first, which no implemented path rebuilds from the receipt that still carries it.
Earlier internal Pub/Sub fixtures keep their strict full-receipt contract.
Keep the Pub/Sub success verdict false until fault/recovery evidence and its oracle are implemented.

This refines the previous blanket refusal to deserialize Pub/Sub, not the prohibition on paid execution.
The selected dollar cap remains unestimated, the total-request cap is not an aggregate meter, and shared schema validation does not authenticate approval.
Keep runner and supervisor entrypoints disabled and the offline delivery approval empty until complete accounting, approval-bound delivery, external fault observations, a control-snapshot recovery procedure and independent orchestration are reviewed.
The [internal approval runbook](../../kubernetes/apps/pubsub/README.md#internal-approval-contract) records the exact limits and remaining boundaries.

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
Explicit policy readback does not establish effective access or exclude inherited privileges; identity-specific probes and propagation handling remain mandatory before separately approved execution.
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
