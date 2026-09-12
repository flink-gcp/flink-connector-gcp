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

# Tier-3 Kubernetes manifests

This CUE module supplies the static manifest layer for [issue #38](https://github.com/flink-gcp/flink-connector-gcp/issues/38).
It reuses the [existing GKE foundation](../opentofu/README.md#tier-3-kubernetes-environment).
The render and schema commands remain credential-free.
The [OpenTofu bootstrap runbook](../opentofu/tier3-bootstrap/README.md) covers persistent prerequisites and authentication.

## Packages and deliveries

```text
kubernetes/
  cue.mod/module.cue             # Module identity and pinned dependencies
  gen/flink/v1beta1/             # Generated CRD validation definitions
  schemas/objects.cue            # Supported Kubernetes kinds
  pkg/flink/application.cue      # Standard Flink application defaults
  common.cue                     # Cluster identity, resource types, labels and order
  cli_tool.cue                   # The render command
  runs/common.cue                # Run inputs and environment constraints
  tests/                         # CI delivery discovery and synthetic regression cases
  upstream.toml                  # Operator chart version, URL and SHA-512
```

`gen/` contains generated sources; `pkg/` contains our reusable packages.
Both are ordinary directories in this module, separate from the deprecated `cue.mod/{gen,pkg,usr}` layout.
The native Kubernetes definitions are a versioned module dependency.

A delivery is one output file's worth of Kubernetes resources.
Each delivery directory contains a `delivery.cue` file and can split its remaining definitions across other CUE files.
The root and delivery directories use the same `package tier3`.
Loading a delivery directory also loads that package from its ancestors up to the module root.
The root partially defines `delivery.resources: [string]: ...`; descendants add named resources to that same struct.
Sibling delivery directories remain separate instances.
Concrete deliveries must be leaves: CI rejects a `delivery.cue` below another delivery directory.
Intermediate directories can hold shared `common.cue` definitions without becoming separate output units.

`delivery.resources` is a map of internal keys to Kubernetes objects.
The keys organize the source and break output-order ties; they are not Kubernetes names.
The CLI prints only the resources, leaving `cluster`, `run`, `delivery.order` and other configuration out of the YAML.
It prints a YAML document stream with `---` separators, suitable for redirecting to one file.

`delivery.order` assigns a priority to each supported Kind, with smaller values first.
ConfigMaps precede Services and workloads.
ConfigMaps precede FlinkDeployments even when their internal keys sort in the opposite order.
Equal priorities sort by internal resource key.
The shared root defines these priorities for all deliveries.
Ordering the YAML does not wait for workload readiness; that belongs to the later apply/lifecycle implementation.

## Standard Flink applications

`pkg/flink.#Application` embeds the generated `#FlinkDeployment` type and supplies common defaults.
An application imports that package and specifies its image, identity, JAR and other differences:

```cue
package tier3

import flink "github.com/flink-gcp/flink-connector-gcp/kubernetes/pkg/flink"

delivery: resources: app: flink.#Application & {
    metadata: name: run.id
    spec: {
        image: run.image
        serviceAccount: "smoke"
        job: jarURI: "local:///opt/flink/usrlib/example.jar"
    }
}
```

This example is exercised as a disposable test delivery; its JAR and image are not provided by this static layer.
For a real application, put its definitions below `runs/`, name one file `delivery.cue`, and supply its actual JAR and ServiceAccount.
Define the concrete `run.id`, `run.expiresAt` and `run.image` used by this example in that directory or shared ancestor definitions before committing the delivery.
No Python test needs to be added for each application: CI discovers delivery directories automatically.

| Field | Package default |
| --- | --- |
| `spec.flinkVersion` | `v2_2` |
| `spec.imagePullPolicy` | `IfNotPresent` |
| JobManager and TaskManager `resource` | 1 CPU, `2Gi` memory each |
| `spec.job.parallelism` | 1 |
| `spec.job.state` | `running` |
| `spec.job.upgradeMode` | `stateless` |
| `spec.job.allowNonRestoredState` | `false` |

These are CUE defaults, so an application can supply another value accepted by the generated type and its environment policy.
The package deliberately leaves image, ServiceAccount and application-specific fields to the delivery.
An upgrade requiring state preservation must explicitly select its upgrade mode and recovery inputs.

The root supplies common project labels.
`runs/common.cue` adds the run ID, RFC 3339 expiry and `tier3-smoke` namespace to resource metadata.
For FlinkDeployments it constrains `spec.image` to a GAR digest and requires a nonempty ServiceAccount, parallelism from one to two, `v2_2` and `allowNonRestoredState: false`.
Images in extra Pod-template containers and generic Jobs/Deployments are not constrained by this policy; the image/lifecycle stage must supply and verify those images before execution.
The common and any manager-specific Flink Pod templates select AMD64 Spot nodes and carry the run label.
These environment constraints apply even when a delivery changes a package default.
Services in runs are ClusterIP-only; persistent identities, quotas, RBAC and cluster-scoped resources belong to OpenTofu.
A future supervisor Job will have its own non-Spot Pod policy.

## Render and validate

Install the versions in `mise.toml`, then run from the repository root:

```sh
mise x -- just tier3-check
mise x -- just tier3-schemas check
```

`tier3-render` calls the `render` command in `cli_tool.cue`.
For an application whose run inputs are not already concrete, invoke it with CUE tags:

```sh
mise x cue -- cue -C kubernetes cmd render ./runs/example \
  -t run_id="$RUN_ID" -t expires_at="$EXPIRES_AT" -t image="$IMAGE_DIGEST" \
  > /tmp/example.yaml
```

Here `RUN_ID`, `EXPIRES_AT` and `IMAGE_DIGEST` are the chosen run identifier, deadline and full GAR image URL including its digest.
The example delivery path is illustrative; this stage has no committed run delivery.
Render one directory per invocation so one delivery maps to one output file.
Output redirection may create an empty destination file when validation fails; require a successful command exit before consuming it.
Rendered YAML is build output and must stay outside source control.

`tier3-check` checks CUE formatting and runs the Python validation suite.
That suite finds every `delivery.cue` under `runs/` and invokes the same `cue cmd render` command separately in each directory.
CI supplies no tags or placeholder inputs.
Committed deliveries must define all values needed to render their resources; missing values fail validation just as they do in the CLI.
CI discards the rendered output and never applies it.
Directories containing only shared definitions have no `delivery.cue` and are not rendered by this check.
The remaining synthetic cases test our package defaults, policy, inheritance, output ordering and discovery behaviour.

CUE is pinned to 0.17.1, and native Kubernetes definitions to `cue.dev/x/k8s.io@v0.6.0` in `cue.mod/module.cue`.
The first load can download that module from the public CUE registry.
These static checks do not implement Kubernetes admission, all CRD CEL rules or the disabled Operator webhook's semantic checks.
Server dry-run and a bounded lifecycle smoke test remain necessary before a real workload is accepted.
An expiry annotation records a deadline; enforcement requires the later supervisor and cleanup implementation.

The `lint.yaml` Tier-3 job runs these checks and verifies schema regeneration using public downloads only.
It participates in the required `CI passed` result on every pull request.
A manifest-only change runs the root Maven licence check instead of connector tests.
Ordinary `just lint` also checks the Python tests and this README.

## Refresh upstream schemas

The schema generator declares its own pinned PyYAML dependency in PEP 723 metadata and runs through `uv run --no-project`.
It can download that Python dependency independently of the root test environment.
`upstream.toml` pins the distributed Apache Helm chart and its SHA-512.
Treat that archive as the CRD source of truth: the source repository's release tag and its distributed chart can differ.
The 1.15.0 archive's FlinkVersion enum ends at `v2_2`; the planned runtime is Flink 2.2.1 on Java 17.

`just tier3-schemas refresh` verifies the archive hash and chart version, then runs `cue get crd` over all four CRD documents.
It also writes complete CRD YAML to `opentofu/tier3-bootstrap/crds/` for the Kubernetes provider.
The validation definitions alone are insufficient for applying CRDs: the YAML preserves the original schemas, subresources and Kubernetes extension fields.
The generated Apache-derived files retain an Apache licence header; attribution is in the repository [NOTICE](../NOTICE) and the [Apache-2.0 licence](../LICENSE) applies.

For an update, review the new archive and its published checksum, change the pin, run `just tier3-schemas refresh`, and inspect the generated application packages and CRD YAML.
Then run `just tier3-check` and `just tier3-schemas check`.
Check mode regenerates into a temporary directory and fails on missing, changed or stale generated CUE or CRD YAML files.
It does not modify tracked sources.
A previously downloaded archive can be checked without downloading it again:

```sh
mise x cue uv -- uv run --no-project scripts/tier3-schemas.py check --chart /path/to/chart.tgz
```

## Ownership and remaining stages

[ADR-0165](../docs/adr/0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md) separates application management from the persistent Kubernetes foundation.
[The OpenTofu bootstrap root](../opentofu/tier3-bootstrap/README.md) owns namespaces, CRDs, persistent workload identities/RBAC and quotas.
The separate Helm root will own the Operator release with `skip_crds = true`, `create_namespace = false`, normal `replicas = 0` and `webhook.create = false`.
CUE owns Flink applications and application-specific ConfigMaps, Services, Deployments and Jobs.
It cannot declare the bootstrap resource kinds, which prevents accidental ownership overlap.

The bootstrap runbook documents credentials, RBAC, initial administrator permission grants, adoption of existing resources and CI plan/apply.
The idle Helm release, GAR image and lifecycle tooling, and a separately approved generic smoke run follow.
The Operator namespace is `tier3-system`; initially it watches only `tier3-smoke`.
The Cloud Tasks namespace and benchmark remain later connector work.
Use a dedicated kubeconfig and explicitly select `gke_flink-gcp_us-central1_flink-tier3` whenever a later command contacts the cluster.
