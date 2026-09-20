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
- Updated: 2026-09-20 (idle BigQuery namespace, data containers, workload identity and Operator watch set)
- Updated: 2026-09-20 (Pub/Sub recovery identity and isolated state storage)
- Updated: 2026-09-20 (idle Pub/Sub namespace, workload identity and Operator watch set)
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
The application definition retains Spot placement, one JM/TM with bounded resources and savepoint upgrades that reject discarded state.
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
A Job deadline requests termination independently of Spot JobManager or TaskManager survival.
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
The supervisor re-reads the queue on every poll and treats any deviation as a stop; cleanup deletes the queue and a remaining queue blocks the idle receipt.
Every Cloud Tasks and Flink REST read is metered per actor against the session read ceiling, administrative queue writes against a ceiling of six, and storage listings remain bounded by their per-call maxima and the evidence budgets.
The task-creation ceiling and cost use a planning bound rather than the expected count: the application caps attempts per creator incarnation, so a cell may create its attempt limit times its subtasks times the four incarnations one JobMaster's restart strategy allows, and the manifest check pins that strategy; a JobManager failover resets the budget, so the cell deadline and the paused queue remain the real caps.
Cleanup deletes the queue only after the run persisted its intent to create it, and it adopts a cell whose create landed after the stop, because the runner's settlement can otherwise race the supervisor's last create; both decisions read the cached control record so a storage failure cannot stop cleanup.

The measurement application no longer prints its per-attempt rows to stdout: the supervisor's bounded log reads cannot capture hundreds of rows per second, so rows are written as gzip parts through the Flink filesystem next to the receipts, and Pod logs carry only Flink logs.
A truncated Pod log is therefore recorded rather than fatal for this scenario, while the smoke scenarios keep failing on truncation.
Cleanup deletes only the cells' checkpoint state under the one-day benchmark bucket and records the retained rows and receipts; exporting them to durable evidence and reconciling them against the receipts is the next change, which must run inside the same workflow execution.

That next change followed: the supervisor reconciles each cell's rows against its receipts after the cell, rewrites them into the evidence bucket with per-object hashes and a marker, releases the benchmark prefix only after the marker exists, and records one observation per poll from the Flink REST API, the TaskManagers, the queue and the Pods.
The offline analyzer and the protocol pin ship in the same source bundle, so `runtime_sha256` covers the rules that will judge a run before it starts.
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
