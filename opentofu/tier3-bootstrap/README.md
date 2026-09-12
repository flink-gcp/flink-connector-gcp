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
The existing GCP root retains GKE, GAR, GSA and IAM ownership; a later `tier3-operator` root will own the Helm release through tfaction.
[CUE](../../kubernetes/README.md) owns application deliveries.
[ADR-0165](../../docs/adr/0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md) records this ownership refinement.

## Resources and identities

This root owns `tier3-system`, `tier3-smoke`, their idle quotas, installer RBAC, the four Flink CRDs and the persistent `tier3-smoke/smoke` ServiceAccount and job RBAC.
The Helm release will own its Operator Deployment, configuration, ServiceAccount, Roles/RoleBindings and release Secrets.
Its chart-managed job identity/RBAC, namespace creation and CRD installation will be disabled.
No resource in this root starts a Pod or allocates a PVC.

| Identity | Added Kubernetes permissions |
| --- | --- |
| `opentofu-plan` | Read both namespaces' configuration and workload inventory, read Helm Secrets in `tier3-system`, and get the named namespaces, CRDs and bootstrap ClusterRoles/ClusterRoleBindings |
| `opentofu` | Manage bootstrap and Helm objects in the two namespaces; create Namespace/CRD/ClusterRole/ClusterRoleBinding objects and update the named cluster-scoped foundation objects |
| `tier3-smoke/smoke` | The chart's job permissions for Pods, ConfigMaps and Deployments in `tier3-smoke` |

The installer holds the explicit chart permission inventory so Kubernetes permits creating and binding its Roles without unrestricted `bind`, `escalate` or `impersonate` grants.
The existing GCP IAM permissions remain effective: additive Kubernetes RBAC does not narrow them.
The apply identity can manage quotas in the two namespaces and the declared cluster-scoped foundation.
Kubernetes RBAC cannot restrict `create` by resource name: create grants cover those four resource kinds, while get/update/patch grants on existing cluster-scoped objects name the owned resources.
No cluster-scoped delete, unrestricted bind/escalate or workload permissions in other namespaces are added.
Expanding the owned namespace/CRD authorization boundary may require an administrator to grant the new permissions first; ordinary changes within it run entirely through CI.
Both identities can list CRD schemas because Kubernetes provider 3.2.1 requires that discovery read during manifest planning.
Plan/apply identities are the existing GitHub WIF service accounts, not application service accounts.
The smoke KSA has no GSA annotation or GCP data grants.
When an application needs GCP APIs, its KSA annotation belongs here and the corresponding GSA/IAM grants belong to the GCP root; those grants follow that application's design.

Both namespaces keep `tier3-idle` with `pods: "0"` and `persistentvolumeclaims: "0"`.
A quota does not terminate existing Pods, and scaling down the Operator does not stop Flink jobs.
Paid workload admission needs a separate lifecycle design and approval with concrete limits and stop conditions.

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

After bootstrap is applied and its plan is empty, the separate Helm root installs the idle release with `replicas = 0`, `webhook.create = false`, `skip_crds = true` and `create_namespace = false`.
The Operator initially watches `tier3-smoke` only.
CRD upgrades precede Helm upgrades; ordinary application cleanup preserves the foundation.
Image publication, lifecycle tooling and a bounded generic smoke run follow separately.
Cloud Tasks implementation/benchmarks and BigQuery verification are outside this bootstrap change.
