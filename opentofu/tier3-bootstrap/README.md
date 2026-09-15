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

# Tier-3 Kubernetes bootstrap

This CI-managed OpenTofu root owns the persistent Kubernetes prerequisites for [#38](https://github.com/flink-gcp/flink-connector-gcp/issues/38).
It reuses the existing `flink-tier3` cluster and the state bucket `flink-gcp-opentofu`, with state prefix `tier3-bootstrap`.
Its `tfaction.yaml` enrolls it in PR plans and saved-plan apply after merge to main.
The existing GCP root retains GKE, GAR, GSA and IAM ownership; the separate [Operator root](../tier3-operator/README.md) owns the Helm release through tfaction.
[CUE](../../kubernetes/README.md) owns application deliveries.
[ADR-0165](../../docs/adr/0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md) records this ownership refinement.

## Resources and identities

This root owns `tier3-system`, `tier3-smoke`, their idle quotas, installer RBAC, the four Flink CRDs and the persistent `tier3-smoke/smoke` ServiceAccount and job RBAC.
The Helm release owns its Operator Deployment, configuration, ServiceAccount, Roles/RoleBindings and release Secrets.
Its chart-managed job identity/RBAC, namespace creation and CRD installation are disabled.
No resource in this root starts a Pod or allocates a PVC.
The [lifecycle foundation](#lifecycle-foundation) also owns the dedicated supervisor KSA and scoped runtime Roles/RoleBindings; its one-time installer permission additions precede the foundation apply.

| Identity | Added Kubernetes permissions |
| --- | --- |
| `opentofu-plan` | Read both namespaces' configuration and workload inventory, read Helm Secrets in `tier3-system`, and get the named namespaces, CRDs and bootstrap ClusterRoles/ClusterRoleBindings |
| `opentofu` | Manage bootstrap and Helm objects in the two namespaces; create Namespace/CRD/ClusterRole/ClusterRoleBinding objects and update the named cluster-scoped foundation objects |
| `tier3-smoke/smoke` | The chart's job permissions for Pods, ConfigMaps and Deployments in `tier3-smoke` |
| `tier3-runner` and `tier3-system/tier3-supervisor` | Lifecycle Roles for smoke admission/cleanup, system supervisor Jobs/configuration and the named Operator scale/quota controls; shared bootstrap metadata reads |

The installer holds the explicit chart permission inventory so Kubernetes permits creating and binding its Roles without unrestricted `bind`, `escalate` or `impersonate` grants.
The existing GCP IAM permissions remain effective: additive Kubernetes RBAC does not narrow them.
The apply identity can manage quotas in the two namespaces and the declared cluster-scoped foundation.
Kubernetes RBAC cannot restrict `create` by resource name: create grants cover those four resource kinds, while get/update/patch grants on existing cluster-scoped objects name the owned resources.
No cluster-scoped delete, unrestricted bind/escalate or workload permissions in other namespaces are added.
Expanding the owned namespace/CRD authorization boundary may require an administrator to grant the new permissions first; ordinary changes within it run entirely through CI.
Both identities can list CRD schemas because Kubernetes provider 3.2.1 requires that discovery read during manifest planning.
Plan/apply identities are the existing GitHub WIF service accounts, not application service accounts.
The smoke KSA is annotated for `tier3-smoke@flink-gcp.iam.gserviceaccount.com`.
The GCP root owns that GSA, its bucket-scoped object grant and the impersonation trust for this KSA; the [application runbook](../../kubernetes/apps/smoke/README.md) describes the storage and runtime boundary.

Both namespaces keep `tier3-idle` with `pods: "0"` and `persistentvolumeclaims: "0"`.
A quota does not terminate existing Pods, and scaling down the Operator does not stop Flink jobs.
Paid workload admission uses the [bounded lifecycle](../../kubernetes/lifecycle/README.md) and needs separate execution approval with concrete limits and stop conditions.

## PR plan and merge apply

Routine changes follow the same tfaction workflow as the GCP root:

1. Change this root and open a PR. CI authenticates through WIF as `opentofu-plan`, verifies the dedicated GKE target and idle inventory, and posts the OpenTofu plan on the PR.
2. Review the plan. Initial adoption must preserve existing objects, with no replacement or deletion.
3. Merge the reviewed PR. The main-push workflow authenticates as `opentofu`, downloads that PR's saved plan and applies it.
4. CI checks the idle inventory and runs a fresh plan with `-detailed-exitcode`; success requires no changes.

Each runner prepares its own absolute kubeconfig with context `gke_flink-gcp_us-central1_flink-tier3`, using the GKE DNS endpoint verified against the GKE API.
The GKE plugin resolves ADC/WIF credentials at execution time.
A temporary `tofu` wrapper removes inherited `KUBE_*` settings and supplies only the verified `KUBE_CONFIG_PATH`; neither that runner path nor a static access token is serialized into the provider configuration or plan.
The wrapper is generated under the runner's temporary directory and forwards arguments and exit codes to the installed OpenTofu binary.
The GCP root uses its existing execution path.

The initial administrator step establishes CI's permissions before CI depends on them.
Sixteen existing objects are then adopted through import blocks: two namespaces, two quotas, four installer Roles, four RoleBindings, two ClusterRoles and two ClusterRoleBindings.
The first CI apply imports them and creates four CRDs plus the smoke KSA/Role/RoleBinding.
Import records identity in state; it does not by itself prove a successful metadata or field-ownership update.
Do not pre-apply this reviewed plan locally: changing state makes the PR artifact stale.
Failed applies are recovered through a new PR and a fresh plan, using the existing tfaction follow-up flow.
The plan summary is published on the PR and the saved plan is held in GitHub Actions artifacts by tfaction; state and local credentials never belong in source control or PR comments.

For local diagnosis, use ADC configured by the uncommitted environment; a new worktree first runs `just worktree-env`:

```sh
mise x -- just tier3-auth /tmp/tier3-kubeconfig
mise x -- just tier3-bootstrap /tmp/tier3-kubeconfig init -input=false
mise x -- just tier3-bootstrap /tmp/tier3-kubeconfig plan -detailed-exitcode
```

The helper refuses the default kubeconfig and checks the target, observed zero quotas and idle inventory before local OpenTofu commands.
CI performs the same checks before plan/apply.
Pods, PVCs, Deployments, StatefulSets, Jobs, CronJobs and Flink run objects stop the operation.
The expected `tier3-system/flink-kubernetes-operator` Deployment at replicas zero is the only exception.
This is a bounded bootstrap inventory, not an exhaustive controller audit or later lifecycle enforcement.
If the baseline namespaces or quotas are missing, restore that baseline through an explicitly reviewed administrator recovery before retrying.

The root keeps namespaces, quotas, CRDs, the bootstrap writer ClusterRole and RoleBindings (including ClusterRoleBindings) behind `prevent_destroy` while those resource declarations remain present.
Never remove their configuration to bypass that protection.
The CRD manager uses server-side apply without forcing conflicts and waits for `Established` before creating the smoke KSA.
An immutable RoleBinding change or ownership conflict needs explicit resolution, not automatic deletion or forced takeover.
OpenTofu may have persisted earlier successful changes when an apply fails; inspect state and make a fresh plan before retrying.
Do not rerun a stale saved plan.

### Recovering the initial CRD apply

The [initial apply](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/34670486161) created all four CRDs, but provider 3.2.1 rejected the API response because it omitted the chart's eight `additionalPrinterColumns[].priority: 0` values.
Kubernetes serializes this optional integer with `omitempty`.
The CRD resource accepts API-returned values only for those zero-priority leaf fields, in addition to the provider's default computed labels and annotations.
Nonzero priorities and CRD schemas remain checked against the manifest; the generated chart payload is unchanged.

The failed creation marked all four state instances as tainted, so the follow-up plan requested replacements and stopped at `prevent_destroy`.
The user-authorized, one-time state repair tracked by [issue #1306](https://github.com/flink-gcp/flink-connector-gcp/issues/1306) is limited to these four instances:

1. Prepare the configuration repair and verify it locally before changing state. Coordinate with other bootstrap changes so no plan/apply overlaps the repair.
2. Back up the current remote state outside source control with restricted permissions. Record its lineage and serial, the four exact resource addresses, and the live CRD UIDs. The failed state has no populated CRD UIDs, so it cannot establish a UID match by itself.
3. Use the dedicated kubeconfig and explicit `gke_flink-gcp_us-central1_flink-tier3` context to verify the target and idle quotas. Check each CRD's identity, `Established=True`, complete schema and server-side field ownership. The expected spec owner is `tier3-bootstrap`; investigate any mismatch before proceeding.
4. Run `tofu untaint` through the bootstrap helper for each verified instance, keeping state locking enabled. The addresses are `kubernetes_manifest.crd["flinkdeployments.flink.apache.org"]`, `kubernetes_manifest.crd["flinksessionjobs.flink.apache.org"]`, `kubernetes_manifest.crd["flinkstatesnapshots.flink.apache.org"]` and `kubernetes_manifest.crd["flinkbluegreendeployments.flink.apache.org"]`. This removes the state failure markers without changing the live objects. Verify that no other state content changed apart from the serial.
5. Generate and review a fresh CI plan in the recovery PR. Require zero replacements/deletions, unchanged CRD schemas and zero Pod/PVC quotas. The remaining smoke ServiceAccount and RoleBinding are created by CI after merge; the smoke Role already exists.
6. After CI apply, verify all four CRDs are Established with their recorded UIDs, the smoke identity/RBAC is present, the inventory is idle and the refreshed plan has no changes.

This exception does not authorize local apply, resource deletion, forced field ownership, or automatic untaint on later failures.
Keep `prevent_destroy` enabled and use a new reviewed recovery plan if another apply fails.

## CI and static checks

OpenTofu checks run in `tofu-plan` for the selected root, after initialization and before planning.
The existing tfaction `test` action validates configuration and runs formatting and TFLint with automatic fixes when an App token is available.
Without that token, the same job runs `validate`, `fmt -check -recursive` and TFLint without fixing files.
These checks share the plan job's authentication and initialization prerequisites; they are not a separate credential-free job.
The lock file carries Linux and macOS provider checksums.
`just tier3-schemas check` regenerates the four CRD YAML files and CUE application schemas from the checksum-pinned chart.
The generated CRD payload preserves the distributed chart's complete schemas, subresources and extension fields.
Those YAML files are source data for OpenTofu, not CUE render output.

The bootstrap plan and apply jobs authenticate as `opentofu-plan` and `opentofu`, respectively.
Their preflight checks the actual username, allowed and denied operations, observed zero quotas and the idle inventory before OpenTofu runs.
Non-persisted SelfSubjectAccessReviews specify API groups/resources explicitly, so the permission checks work before CRDs exist.
A denial reason is accepted; transport failures or authorization evaluation errors fail the check.
Authenticated checks run when tfaction selects the bootstrap target; CUE and schema-generation checks remain credential-free.
There is no separate access workflow requiring a live, idle cluster for application-only or unrelated tool configuration changes.
For manual diagnosis, run the same access check with a dedicated kubeconfig:

```sh
mise x -- just tier3-access /tmp/tier3-kubeconfig plan
```

That command requires credentials which actually authenticate as the selected identity.
An administrator's `kubectl --as` probe measures impersonated RBAC only; CI supplies the actual WIF evidence.
No local service-account impersonation grant is required.

## Next stage

After bootstrap is applied and its plan is empty, the [separate Helm root](../tier3-operator/README.md) installs the idle release with `replicas = 0`, `webhook.create = false`, `skip_crds = true` and `create_namespace = false`.
The Operator initially watches `tier3-smoke` only.
CRD upgrades precede Helm upgrades; ordinary application cleanup preserves the foundation.
The [image publication path](../../kubernetes/images/README.md) supplies GAR runtime pins.
The [bounded lifecycle](../../kubernetes/lifecycle/README.md) supplies admission and cleanup; a generic smoke run requires separate execution approval.
Cloud Tasks implementation/benchmarks and BigQuery verification are outside this bootstrap change.

## Lifecycle foundation

This foundation prepares the persistent permissions and storage for [issue #1310](https://github.com/flink-gcp/flink-connector-gcp/issues/1310).
It keeps both Pod/PVC quotas and the Operator replica count at zero.
The [runtime runbook](../../kubernetes/lifecycle/README.md) describes the implemented runner, supervisor, shared lock, admission checks and cleanup automation.
The foundation apply keeps workloads stopped; a lifecycle dispatch requires separate execution approval.

### Identities and storage

The GCP root owns the following new resources and grants.

| Resource | Purpose and boundary |
| --- | --- |
| `tier3-runner@flink-gcp.iam.gserviceaccount.com` | External lifecycle operations; Cluster Viewer for GKE discovery/DNS access, Reader on the existing Tier-3 GAR repository, and the storage grants below |
| `tier3-supervisor@flink-gcp.iam.gserviceaccount.com` | Storage access from the dedicated supervisor KSA; no GKE administration or image publication |
| `gs://flink-gcp-tier3-evidence` | `us-central1`, STANDARD, uniform access, public access prevention, no soft delete or versioning |
| Evidence `runs/` prefix | Both runtime identities can create and read objects; only the bucket lifecycle removes them, starting at age 30 days |
| Evidence `_control/` prefix | Both runtime identities can update control records; the plan identity can mutate only `_control/environment.json` |
| Existing smoke bucket | Both runtime identities can list/read objects and have Object User access under `runs/` for cleanup; no bucket administration |

Object User includes object creation and update as well as deletion.
The conditional smoke-bucket grant restricts that role to run state; the separate Object Viewer grant supports listing, whose authorization is evaluated at the bucket rather than an individual object.
IAM does not select the active run within those prefixes: the lifecycle implementation must verify the approved run ID, object generation and resource ownership before changing or deleting anything.
The runtime identities have no evidence-deletion permission under `runs/`, and cannot change bucket IAM, expiry or retention.
The existing infrastructure apply identity retains its administrative permissions.

The runner's WIF bindings select this repository's immutable ID and the exact main workflow reference, event and ref.
They reserve `workflow_dispatch` for `tier3-run.yaml` and `tier3-recover.yaml`, and `workflow_run` for `tier3-recover.yaml`.
The recovery workflow must separately validate its triggering workflow and the saved run approval; its WIF binding does not authenticate that triggering run.
The supervisor trust selects only `tier3-system/tier3-supervisor` through Workload Identity Federation for GKE.
Neither identity reuses the installer, image publisher or smoke application account.

The bootstrap root creates the supervisor KSA in `tier3-system`, annotates it for its GSA and binds both runtime identities to `tier3-lifecycle` Roles in the two namespaces.
The supervisor Job and source ConfigMap also belong in `tier3-system`.
The smoke KSA can create Pods and Deployments only in `tier3-smoke`, so it cannot select the supervisor KSA through a workload it creates.
In `tier3-smoke`, the lifecycle Roles allow FlinkDeployment admission/finalizer cleanup, workload deletion and bounded observation through Pod logs and the Service proxy.
In `tier3-system`, writes cover supervisor Jobs/ConfigMaps, deletion of Pods, scale changes on `flink-kubernetes-operator` and updates to `tier3-idle`.
The runtime must verify ownership before changing or deleting a Job or ConfigMap, including the chart-owned Operator configuration in that namespace.
Quota writes in `tier3-smoke` also name only `tier3-idle`.
Dynamic Pod names cannot be constrained to a run using RBAC: the runtime must check UID and ownership before deletion.
The shared bootstrap reader binding supplies read access to the two namespace identities, the four CRDs and the named bootstrap RBAC objects.
The lifecycle Roles do not directly grant Secret access, identity/RBAC writes, namespace/CRD writes, or unrestricted bind/escalate/impersonate.
They do allow Jobs in `tier3-system` to select the chart's `flink-operator` KSA and thereby use its permissions in `tier3-smoke`, including Secret access and Pod creation.
Treat the runner and supervisor as trusted Operator administrators; the direct Role inventory is not a boundary on their reachable permissions.
This trust does not extend to the smoke workload identity, which cannot create workloads in `tier3-system`.

### Initial permission grant

CI's apply identity must hold every permission before it can delegate the new lifecycle Roles.
Prepare and review an additive administrator patch for the two existing `tier3-helm-apply` Roles before merging the foundation PR.
The patch adds only the rules in `local.lifecycle_installer_smoke` and `local.lifecycle_installer_system`; preserve every existing rule and object identity.

| Namespace | Additional rule |
| --- | --- |
| `tier3-system` | `batch/jobs`: get, list, watch, create, update, patch, delete |
| `tier3-smoke` | core `pods/log` and `services/proxy`: get |
| `tier3-system` | core `pods`: delete |
| `tier3-system` | core `pods/log`: get |
| `tier3-system` | `apps/deployments/scale`: get, update, patch, only `flink-kubernetes-operator` |

Use the dedicated kubeconfig and explicit `gke_flink-gcp_us-central1_flink-tier3` context to verify the target, idle quotas and zero Pods before the administrator patch.
First probe the added operations with the apply identity; already effective permissions need no repeated grant.
Do not grant these rules to the plan identity, and do not disable Kubernetes escalation checks.
After the one-time grant, CI applies the tracked Roles through the normal reviewed saved-plan path.
This grant does not authorize quota changes, Operator scale-up or application execution.

### Acceptance and next stage

Review all three root plans before merge.
The GCP plan adds two service accounts, one bucket and the scoped grants; it updates the existing WIF provider without changing its repository/owner condition.
The bootstrap plan adds one ServiceAccount, two Roles and two RoleBindings, extends the two installer Roles and adds the runtime subjects to the existing reader binding.
It must preserve namespaces, quotas, CRDs and existing identities.
The Operator plan updates its Helm values in place, keeping zero replicas, one container and the same seven chart-owned objects.
That container now explicitly requests and limits 1 CPU, 2 GiB memory and 1 GiB ephemeral storage; the chart and post-apply checks verify those quantities.

After CI apply, require idle inventory and empty refreshed plans for all three roots before developing the dependent lifecycle implementation.
Actual WIF, supervisor and deletion behavior remains untested until the later stages; static IAM/RBAC configuration is not execution evidence.
The [lifecycle implementation](../../kubernetes/lifecycle/README.md) enforces one run, up to a 45-minute admission/test window, 15 minutes for cleanup, a $1 additional-cost budget, and 100 MiB of durable evidence per run.
Its shared cleanup keeps the Operator running until run-object and owned workload deletion completes, then restores the original quotas and zero replicas.
The separately approved GKE lifecycle/recovery exercise belongs to [issue #1311](https://github.com/flink-gcp/flink-connector-gcp/issues/1311).
