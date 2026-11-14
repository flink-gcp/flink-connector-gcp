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

# Tier-3 on Kubernetes

Tier-3 runs each connector's Flink job on the GKE Autopilot cluster `flink-tier3`, injects faults on purpose, and then reads back what the job wrote.
It asks whether the connector keeps working through faults on real Kubernetes without losing data; it is neither a load test nor a test of long-running behavior.
[ADR-0204](../docs/adr/0204-tier3-recovery-trials-tolerate-faults-and-fail-only-on-lost-data.md) sets the rules a trial is judged by, and a scenario that has not moved to it yet keeps the rules of [ADR-0165](../docs/adr/0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md) until its issue under [#2143](https://github.com/flink-gcp/flink-connector-gcp/issues/2143) moves it.
ADR-0165 also divides ownership: OpenTofu owns the persistent foundation, and this directory owns the applications and what each run deploys.

This directory holds the Flink applications, their CUE manifests and the image pins.
The rig that dispatches, supervises and judges a run is the `flink-tier3` Python package in [`tools/tier3`](../tools/README.md#tier-3), and the [lifecycle runbook](lifecycle/README.md) describes how a run is dispatched, bounded and cleaned up.
Rendering and schema checks contact no cluster; the [OpenTofu bootstrap runbook](../opentofu/tier3-bootstrap/README.md) covers the persistent prerequisites and authentication.

## Scenarios

The `scenario` input of the `Tier-3 run` workflow selects one scenario per run, and each runs in a namespace of its own.
Every connector has one; `smoke` runs a generic stateful application without a connector, to check the environment itself.

| Scenario | Namespace | Runbook | Rules | Findings |
| --- | --- | --- | --- | --- |
| `smoke` | `tier3-smoke` | [Smoke](apps/smoke/README.md) | | |
| `bigquery-recovery` | `tier3-bigquery` | [BigQuery](apps/bigquery/README.md) | ADR-0165 until [#2150](https://github.com/flink-gcp/flink-connector-gcp/issues/2150) | [#1312](../docs/adr/evidence/0165-bigquery-trial-findings-1312.md), [#1313](../docs/adr/evidence/0165-bigquery-fileloads-findings-1313.md), [#1685](../docs/adr/evidence/0165-bigquery-fileloads-inflight-findings-1685.md), [#1779](../docs/adr/evidence/0165-bigquery-fault-trials-findings-1779.md), [#2034](../docs/adr/evidence/0195-fileloads-uploader-recovery-2034.md) |
| `pubsub-recovery` | `tier3-pubsub` | [Pub/Sub](apps/pubsub/README.md) | ADR-0165 until [#2151](https://github.com/flink-gcp/flink-connector-gcp/issues/2151) | [#1361](../docs/adr/evidence/0165-pubsub-trial-findings-1361.md), [#1720](../docs/adr/evidence/0165-pubsub-redelivery-findings-1720.md), [#2056](../docs/adr/evidence/0198-pubsub-reactive-restore-deduplication-findings-2056.md), [#2127](../docs/adr/evidence/0198-pubsub-restore-deduplication-trial-findings-2127.md) |
| `cloudtasks-recovery` | `tier3-cloudtasks` | [Cloud Tasks](apps/cloudtasks/README.md) | ADR-0165 until [#2152](https://github.com/flink-gcp/flink-connector-gcp/issues/2152) | [#1362](../docs/adr/evidence/0165-cloudtasks-recovery-findings-1362.md), [#1778](../docs/adr/evidence/0165-cloudtasks-fault-trials-findings-1778.md), [#1799](../docs/adr/evidence/0165-cloudtasks-rescale-out-findings-1799.md) |
| `bigtable-recovery` | `tier3-bigtable` | [Bigtable](apps/bigtable/README.md) | ADR-0165 until [#2153](https://github.com/flink-gcp/flink-connector-gcp/issues/2153) | [#1855](../docs/adr/evidence/0165-bigtable-recovery-findings-1855.md) |
| `spanner-recovery` | `tier3-spanner` | [Spanner](apps/spanner/README.md) | ADR-0165 until [#2154](https://github.com/flink-gcp/flink-connector-gcp/issues/2154) | [#1878](../docs/adr/evidence/0165-spanner-recovery-findings-1878.md) |
| `firestore-recovery` | `tier3-firestore` | [Firestore](apps/firestore/README.md) | ADR-0204 for the `sequence` kind; ADR-0165 for the single-fault kinds | [#2147](../docs/adr/evidence/0204-firestore-fault-sequence-findings-2147.md) |

Each findings link names the issue whose trials it reports.
The exactly-once scenario of [#2148](https://github.com/flink-gcp/flink-connector-gcp/issues/2148) is not designed yet.

## Layout per connector

Every connector's scenario is laid out the same way, so a change to one connector touches that connector's paths and, beyond them, only the entries that name the connector in shared files.
In the table, `<c>` is the connector's directory name, `bigquery`, `pubsub`, `cloudtasks`, `bigtable`, `spanner` or `firestore`.

| Path | What it holds |
| --- | --- |
| `kubernetes/apps/<c>/` | The Flink application: a Maven module that only the `tier3-<c>` profile builds, through `just tier3-<c>-verify`; its `Dockerfile` and `.dockerignore`; and its `README.md`, the scenario's runbook |
| `kubernetes/pkg/<c>/application.cue` | The application's FlinkDeployment contract |
| `kubernetes/lifecycle/<c>.cue` | The scenario's dispatch inputs and the applications the lifecycle delivery renders for it |
| `kubernetes/tests/fixtures/<c>.cue`, `kubernetes/tests/test_<c>_manifests.py` | The scenario's manifest tests |
| `tools/tier3/src/flink_tier3/<c>/` | The scenario's part of the rig |
| `tools/tier3/tests/<c>/` | The tests of that part |
| `opentofu/flink-gcp/tier3-<c>.tf`, `opentofu/tier3-bootstrap/<c>.tf` | The scenario's GCP grants and resources, and its namespace's Kubernetes identity and permissions |
| `docs/adr/evidence/NNNN-<c>-<topic>-<issue>.md` | A preregistration or findings record, numbered for the ADR it supports |

At run time a connector's scenario is `<c>-recovery` in namespace `tier3-<c>`, under ServiceAccount `<c>`, with its image from GAR package `<c>-recovery`.
The smoke application uses the same Kubernetes, manifest-test and OpenTofu paths as `smoke` and keeps its static deliveries in `runs/generic-smoke/`; it has no rig subpackage, rig tests or recovery trial, and its scenario, ServiceAccount and GAR package are all `smoke`.
The shared files that name every connector are the tables a scenario is registered in, such as the run workflow's inputs, the scenario list and windows in `lifecycle/delivery.cue`, the namespace policy in `runs/common.cue`, `flink_tier3/policy.toml`, the Operator's watch set, and the rig modules that choose a scenario's plan, actors and analysis.

A recovery runbook opens with what the application is for and then has these sections in this order: `Build and local validation`; the layout of what the run writes to, such as `Instance layout`; the graph and what each destination records; `Arguments`; `Deployment definition`, where the runbook has one; and `Recovery trial`, whose subsections cover dispatch, the lifecycle of the run's resources, the exercise and its oracles, and the verdict and analysis.
Code that two scenarios share belongs to the shared layers below or to the package root of `flink_tier3` and its tests, not to one connector's directory for another to import.

Some paths predate these rules. Each scenario's rework under [#2143](https://github.com/flink-gcp/flink-connector-gcp/issues/2143) brings its own paths in line, and code another connector imports moves to the shared layers before the rework that changes it:

- `kubernetes/lifecycle/pubsub-trials/` holds Pub/Sub's reviewed trial files, which its dispatch names instead of a trial input ([#2151](https://github.com/flink-gcp/flink-connector-gcp/issues/2151));
- the rig tests of other connectors, the shared exercise tests and `test_records.py` and `test_rest.py` take fixtures from `tools/tier3/tests/bigquery/` ([#2150](https://github.com/flink-gcp/flink-connector-gcp/issues/2150)) and `tools/tier3/tests/cloudtasks/` ([#2152](https://github.com/flink-gcp/flink-connector-gcp/issues/2152));
- Firestore's oracles build on `flink_tier3.spanner.oracle`, and its lifecycle and exercise on `flink_tier3.spanner.instance` ([#2154](https://github.com/flink-gcp/flink-connector-gcp/issues/2154));
- the BigQuery and Pub/Sub runbooks do not follow the section order ([#2150](https://github.com/flink-gcp/flink-connector-gcp/issues/2150), [#2151](https://github.com/flink-gcp/flink-connector-gcp/issues/2151)), and BigQuery's uploader findings keep the name `0195-fileloads-uploader-recovery-2034.md` they were published under.

## Shared layers

Everything outside the connector paths is shared by every scenario:

```text
kubernetes/
  cue.mod/module.cue             # Module identity and pinned dependencies
  gen/flink/v1beta1/             # Generated CRD validation definitions
  schemas/objects.cue            # Supported Kubernetes kinds
  pkg/flink/                     # Standard Flink application defaults
  common.cue                     # Cluster identity, resource types, labels and order
  cli_tool.cue                   # The render command
  runs/common.cue                # Run inputs and the environment policy every delivery applies
  lifecycle/delivery.cue         # Lifecycle inputs, ConfigMap and supervisor Job
  lifecycle/faults.cue           # Fault kinds and each phase's sizes
  images/                        # Image pins and the lifecycle-tools image
  tests/fixtures/application.cue # The standard application the shared tests render
  tests/test_manifests.py        # Tests of the shared layers, and the helpers and `module` fixture the scenario tests import
  upstream.toml                  # Operator chart version, mirror URLs and SHA-512
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
Ordering the YAML does not wait for workload readiness; the bounded lifecycle runner performs that check before Flink admission.

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
The lifecycle delivery uses the same run constraints and is rendered with synthetic dispatch inputs during that check.

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
`runs/common.cue` adds the run ID, RFC 3339 expiry and application namespace (`tier3-smoke` by default, `tier3-bigquery`, `tier3-pubsub`, `tier3-cloudtasks`, `tier3-bigtable`, `tier3-spanner` or `tier3-firestore`) to resource metadata.
For FlinkDeployments it constrains `spec.image` to a GAR digest and requires a nonempty ServiceAccount and `allowNonRestoredState: false`; in `tier3-smoke` it also requires parallelism from one to two and `v2_2`.
In `tier3-bigquery` it requires the `bigquery` ServiceAccount, the `bigquery-recovery` GAR package, parallelism one or two and `v2_2`.
In `tier3-cloudtasks` it requires the `cloudtasks` ServiceAccount, the `cloudtasks-recovery` GAR package, parallelism one or two and `v2_2`.
The `tier3-pubsub` namespace requires the `pubsub` ServiceAccount, parallelism one to three and `v2_2`; three is for the restore-deduplication trials.
In `tier3-bigtable` it requires the `bigtable` ServiceAccount, the `bigtable-recovery` GAR package, parallelism one or two and `v2_2`.
In `tier3-spanner` it requires the `spanner` ServiceAccount, the `spanner-recovery` GAR package, parallelism one or two and `v2_2`.
In `tier3-firestore` it requires the `firestore` ServiceAccount, the `firestore-recovery` GAR package, parallelism one or two and `v2_2`.
Images in extra Pod-template containers and generic Jobs/Deployments are not constrained by this policy; the lifecycle must supply and verify those images before execution.
Each delivery supplies its application's complete image; the [image runbook](images/README.md) describes publication and the [pins](images/pins.cue) of the Flink base, supervisor and smoke images.
The smoke application's committed `runs/generic-smoke/initial` and `runs/generic-smoke/upgrade` deliveries share concrete run inputs and the `images.smoke` pin, and its [runbook](apps/smoke/README.md#deployment-and-storage) gives their render commands.
The common and manager-specific Flink Pod templates select AMD64 and carry the run label; the TaskManager template adds Spot, and the JobManager template selects no capacity class, which is how a Pod asks Autopilot for normal capacity.
These environment constraints apply even when a delivery changes a package default.
Services in runs are ClusterIP-only; persistent identities, quotas, RBAC and cluster-scoped resources belong to OpenTofu.
The [lifecycle supervisor Job](lifecycle/README.md) has its own non-Spot Pod policy.

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
The example delivery path is illustrative; the committed generic smoke deliveries already define their inputs and render without tags.
Render one directory per invocation so one delivery maps to one output file.
Output redirection may create an empty destination file when validation fails; require a successful command exit before consuming it.
Rendered YAML is build output and must stay outside source control.

`tier3-check` checks CUE formatting and runs the Python validation suite.
That suite finds every `delivery.cue` under `runs/` and `lifecycle/` and renders each directory separately.
Run deliveries use `cue cmd render`; lifecycle uses `cue export` with installed package-source JSON, followed by YAML serialization.
Ordinary `runs/` deliveries receive no tags or placeholder inputs and must define all values needed to render their resources.
The lifecycle delivery instead receives synthetic dispatch inputs to validate its parameterized bundle; missing required values fail validation just as they do in the CLI.
CI discards the rendered output and never applies it.
Directories containing only shared definitions have no `delivery.cue` and are not rendered by this check.
The remaining synthetic cases test our package defaults, policy, inheritance, output ordering and discovery behaviour.

CUE is pinned to 0.17.1, and native Kubernetes definitions to `cue.dev/x/k8s.io@v0.6.0` in `cue.mod/module.cue`.
The first load can download that module from the public CUE registry.
These static checks do not implement Kubernetes admission, all CRD CEL rules or the disabled Operator webhook's semantic checks.
Server dry-run and a bounded lifecycle smoke test remain necessary before a real workload is accepted.
An expiry annotation records a deadline; the [bounded lifecycle](lifecycle/README.md) enforces it for its explicitly approved delivery.

The `lint.yaml` Tier-3 job runs these checks and verifies schema regeneration using public downloads only.
It participates in the required `CI passed` result on every pull request.
A manifest-only change runs the root Maven licence check instead of connector tests.
Ordinary `just lint` also checks the Python tests and this README.

## Refresh upstream schemas

The schema generator belongs to the `flink-tier3` uv workspace member and runs through its installed CLI.
It shares the root `uv.lock` with the bootstrap helper, lifecycle runtime and tests.
`upstream.toml` pins the distributed Apache Helm chart and its SHA-512.
Treat that archive as the CRD source of truth: the source repository's release tag and its distributed chart can differ.
Its `urls` array lists the mirrors in the order the download tries them, and only bytes that match the SHA-512 are accepted, so a mirror can be added without weakening the pin.
The operator bootstrap's `opentofu/tier3-operator/upstream.yaml` is a separate pin with its own `urls`.
The pin lists `dlcdn.apache.org` before the permanent `archive.apache.org` copy.
The CDN serves only the releases the Flink project still distributes; once the project removes the pinned release from distribution, only the archive serves it.
`just tier3-schemas fetch --chart <path>` writes the verified chart, and `check` or `refresh` with `--chart <path>` reads it without downloading.
The Tier-3 lint job caches that file under a key derived from `upstream.toml` and downloads the chart only when no cache entry for that key is available.
The 1.16.1 archive's FlinkVersion enum ends at `v2_4`; the runtime is Flink 2.2.1 on Java 17, enum `v2_2`.

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
mise x cue uv -- uv run --locked --package flink-tier3 --no-dev flink-tier3 schemas check --chart /path/to/chart.tgz
```

## Ownership

[ADR-0165](../docs/adr/0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md) separates application management from the persistent Kubernetes foundation.
[The OpenTofu bootstrap root](../opentofu/tier3-bootstrap/README.md) owns namespaces, CRDs, persistent workload identities/RBAC and quotas.
The [separate Helm root](../opentofu/tier3-operator/README.md) owns the Operator release with `skip_crds = true`, `create_namespace = false`, normal `replicas = 0` and `webhook.create = false`.
CUE owns Flink applications and application-specific ConfigMaps, Services, Deployments and Jobs.
It cannot declare the bootstrap resource kinds, which prevents accidental ownership overlap.

The bootstrap runbook documents credentials, RBAC, initial administrator permission grants, adoption of existing resources and CI plan/apply.
The Operator namespace is `tier3-system`, and the Operator watches the seven namespaces of the [scenario table](#scenarios).
Each of them keeps zero Pod and PVC quotas except while a run holds the environment lock.
Use a dedicated kubeconfig and explicitly select `gke_flink-gcp_us-central1_flink-tier3` whenever a command contacts the cluster.
