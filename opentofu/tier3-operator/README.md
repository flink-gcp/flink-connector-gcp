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

# Tier-3 idle Operator release

This OpenTofu root installs the Flink Kubernetes Operator for [issue #1307](https://github.com/flink-gcp/flink-connector-gcp/issues/1307) with zero replicas.
It reuses the existing cluster, the `flink-gcp-opentofu` state bucket and the established plan/apply WIF identities, with its own `tier3-operator` state prefix.
The [bootstrap root](../tier3-bootstrap/README.md) must have applied successfully and produced an empty refreshed plan first.
The [bootstrap recovery apply](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/34674345325) met those conditions for the initial installation.

## Versions and ownership

| Component | Pin |
| --- | --- |
| OpenTofu | 1.12.5 |
| Helm provider | 3.3.0, with committed Linux amd64 and macOS arm64 checksums |
| Helm rendering CLI | 3.20.2, matching the provider's Helm library |
| Distributed chart and Operator | 1.15.0 |
| Planned smoke runtime | Flink 2.2.1 on Java 17, API enum `v2_2` |

The root's `upstream.yaml` fixes the archive URL, version and SHA-512.
Both runners download and verify that archive into `.terraform/operator-chart.tgz`; the resource uses this checkout-relative path and `values.yaml`.
The helper checks the chart's version and appVersion and renders the same archive before OpenTofu runs.
The distributed chart defaults to `ghcr.io/apache/flink-kubernetes-operator:79d730b`.
The idle values deliberately use the Docker Hub release-style reference `apache/flink-kubernetes-operator:1.15.0` to record the intended Operator version.
[Image publication](https://github.com/flink-gcp/flink-connector-gcp/issues/1308) must replace it with a verified GAR digest before any Pod starts; this installation does not establish runtime image availability.

Helm owns seven rendered resources: a Deployment, ConfigMap and Operator ServiceAccount in `tier3-system`, and an Operator Role and RoleBinding in each of `tier3-system` and `tier3-smoke`.
The release also stores its metadata in Secrets in `tier3-system`, retaining up to five revisions.
The configuration watches only `tier3-smoke`; the Role in the release namespace supports leases and FlinkStateSnapshot discovery.
The installer already holds these explicit permissions through bootstrap, without unrestricted bind or escalate grants.

`replicas: 0`, `webhook.create: false`, `skip_crds = true` and `create_namespace = false` preserve the idle boundary.
The chart's job ServiceAccount, job Role and job RoleBinding creation are all disabled.
Bootstrap owns namespaces, CRDs, zero Pod/PVC quotas and the persistent smoke identity/RBAC; the GCP root owns cloud resources and IAM, and CUE owns application deliveries.
No cert-manager or workload is installed.
[ADR-0165](../../docs/adr/0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md) records these boundaries.

## Plan and apply

The tfaction marker selects this root in the existing tofu-list, plan and apply workflows.
The plan identity is `opentofu-plan`; only the main-push apply job uses `opentofu`.
Each runner constructs a dedicated DNS kubeconfig for `gke_flink-gcp_us-central1_flink-tier3` and authenticates through the GKE plugin at execution time.
The provider contains only the fixed context and Secret storage driver.
Its wrapper clears inherited Kubernetes overrides and supplies that runner's verified kubeconfig, preserving ADC/WIF environment settings.
Neither an expiring token nor the plan runner's kubeconfig path is serialized into the provider configuration.

Before plan/apply, the helper checks the actual principal, allowed/denied operations, observed zero quotas, idle inventory, four Established CRDs and the smoke identity/RoleBinding.
It checks that the rendered chart contains precisely the seven owned resources, zero replicas, the pinned image, the expected watched namespace and Operator RoleBinding subjects, with no hooks, extra containers or PVC mounts.
This is a bounded idle inventory check, not an exhaustive audit of every Kubernetes controller or the later lifecycle supervisor.
The helper pins its own PyYAML dependency in PEP 723 metadata and runs through `uv run --no-project`.
OpenTofu validation, formatting and TFLint run inside the selected plan job after initialization.
No OpenTofu checks or cluster access are added to general lint or a separate workflow.

Review the PR's OpenTofu plan together with the `tier3-operator-manifest` artifact from the same CI run.
The first plan must contain one Helm release addition and no changes or deletions.
After merge, CI applies that saved plan using a freshly verified copy of the pinned chart.
It reads the release status and versions through `helm get metadata`, then verifies live configuration/RBAC, zero Deployment replicas, zero Pods/PVCs in both owned namespaces and unchanged idle quotas.
Post-apply verification rechecks the runner-local archive's checksum and renders it again, without depending on another download.
A refreshed plan with `-detailed-exitcode` must exit zero.
The installation is complete only after those post-apply checks succeed.

## Local diagnosis and recovery

Use the same dedicated kubeconfig and root selection as CI:

```sh
mise x -- just worktree-env
mise x -- just tier3-auth /tmp/tier3-kubeconfig
mise x -- just tier3-operator /tmp/tier3-kubeconfig init -input=false
mise x -- just tier3-operator /tmp/tier3-kubeconfig plan -detailed-exitcode
mise x kubectl helm uv -- uv run --no-project scripts/tier3-bootstrap.py \
  --kubeconfig /tmp/tier3-kubeconfig --root operator verify
```

The existing bootstrap commands retain their root and behavior.
Do not apply locally before the merge: changing state invalidates the reviewed plan.
If apply partially fails, inspect release/state and use a follow-up PR touching this root to generate a fresh plan.
Do not rerun a stale saved plan or delete release Secrets to hide a failed installation.

For an upgrade, update and apply bootstrap CRDs first and require an empty refreshed bootstrap plan.
Then update this root's independent chart/image pins and review the rendered RBAC against the installer's existing grants.
Do not automatically advance Helm when the shared CRD/schema pin changes.
A new ownership or authorization boundary requires an explicit bootstrap change before the Helm change.

Deliberate scale-up, numeric workload limits, image publication and lifecycle cleanup remain separate work.
Both idle quotas continue to forbid Pods and PVCs.
Scaling down the Operator would not stop an existing Flink job, and an idle installation is not an unconditional zero-cost guarantee.
