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
- Date: 2026-09-11
- Updated: 2026-09-12 (CI ownership, initial CRD apply recovery and idle Helm root)
- Updated: 2026-09-13 (digest-pinned GAR publication and seven-day image expiry)
- Issues: [#38](https://github.com/flink-gcp/flink-connector-gcp/issues/38), [#1307](https://github.com/flink-gcp/flink-connector-gcp/issues/1307), [#1308](https://github.com/flink-gcp/flink-connector-gcp/issues/1308)
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
The chart's default image tag is a commit abbreviation, so the idle Deployment explicitly names Operator 1.15.0.
GAR publication and digest selection remain prerequisites to later Pod admission.
Helm provider 3.3.0 reads its kubeconfig from the execution-time environment, using the shared wrapper and a fixed context, with no saved token or runner path.
The plan job retains the rendered resources for review; after saved-plan apply, CI checks the deployed release, live configuration/RBAC, idle inventory and an empty refreshed Helm plan.
Do not use replicas drift suppression or local apply to implement deliberate scale-up in this stage.

CUE owns application deliveries: FlinkDeployments and application-specific configuration, Services, Deployments and Jobs.
Generated application definitions remain in gen/, standard packages in pkg/, and delivery.resources is rendered by cli_tool.cue in Kind/key order.
CI renders each application delivery separately with real source inputs; ci.cue and CI placeholder values remain absent.
Complete CRD installation YAML moves to the bootstrap root; the same checksum-pinned chart produces both that YAML and CUE validation definitions.
CRD upgrades precede Helm upgrades and ordinary run cleanup preserves the foundation.

Future GCP-using applications receive a dedicated workload identity: the KSA annotation belongs to bootstrap, while its GSA impersonation/data grants belong to the GCP root.
The generic smoke KSA currently has no GCP annotation or data grants.
Installer identities must not become application identities.

### Image publication

The GCP root owns a separate Tier-3 image publisher with repository-scoped Artifact Registry Writer access.
Its WIF binding selects the exact main-only manual publication workflow and repository ID; the provider condition checks the repository and owner IDs.
The workflow copies fixed AMD64 Operator and Flink images with crane and builds the Python/kubectl runtime with Docker's BuildKit actions.
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
The successful workflow's job summary supplies digest references for a separate reviewed Helm/CUE pin change; no placeholder GAR output digest is committed in the publication-foundation PR.
The idle Helm release continues to use its existing image reference until that follow-up lands, retaining zero replicas and quotas.
Publication records GAR image references without starting workload Pods; it does not establish GKE runtime behavior.

## Consequences

There are separate state and application boundaries for GCP infrastructure, Kubernetes bootstrap, Helm and application runs.
A bootstrap-only or Operator-only PR receives OpenTofu checks and a visible tfaction plan within the plan job, with an access preflight.
After merge, CI applies the reviewed artifact and verifies an empty refreshed plan.
Failed applies use the existing follow-up PR workflow; a local pre-apply or stale-artifact retry is not the recovery path.
The earlier manual-bootstrap/CI-Helm draft split was revised because it left routine foundation changes outside PR plan/apply review.
The two idle quotas forbid Pods and PVCs throughout this foundation stage.
The bootstrap inventory is a preflight, not an exhaustive controller audit or lifecycle supervisor.
Image publication, lifecycle tooling and a bounded generic smoke run still require later work; paid execution needs separate limits, stop conditions and approval.
Cloud Tasks implementation/benchmarks and BigQuery-specific verification remain outside this foundation change.
