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

# Python workspace

The repository root owns one uv workspace and one `uv.lock`.
Each tool package declares its runtime dependencies and console entry points in its own `pyproject.toml`.

| Directory | Responsibility | Migration |
| --- | --- | --- |
| `tier3/` | Foundation authentication, schemas and bounded workload lifecycle | Packaged |
| `checks/` | Repository checks and shared source analysis | Not created; existing Python checks remain in `scripts/` |
| `release/` | Release validation and publication helpers | Not created; existing helpers remain in `scripts/` |

Add the latter workspace members when their callers and tests move; empty placeholder packages are unnecessary.
Shell programs remain in `scripts/`.
The existing `just` recipes remain the local command interface.
`just test-scripts` runs both the remaining script tests and the Tier-3 package tests.

## Tier-3

From the repository root:

```sh
mise x uv -- uv run --locked --package flink-tier3 --no-dev flink-tier3 --help
mise x uv -- uv run --locked --package flink-tier3 --no-dev flink-tier3 lifecycle --help
mise x uv -- uv build --package flink-tier3 --out-dir tools/tier3/target/dist
```

The package uses a `src/` layout and ordinary imports; its CLI, bootstrap helper and schema generator no longer require file-path imports or script-specific dependency pins.
The wheel includes all Python modules and the reviewed `policy.toml`.
It is a local build artifact; this change adds no package-registry publication.

Commands that read CUE or OpenTofu inputs require a repository checkout.
The current directory is the default root; an installed CLI can select another checkout with `flink-tier3 --repository /path/to/checkout COMMAND ...`.
Other relative arguments, including kubeconfig and evidence paths, remain relative to the caller's current directory.
The in-cluster `supervisor` command needs only the projected package and approval/application data.
The offline `bigquery query` and `bigquery assess` commands also work outside the checkout and perform no cloud operations; the [BigQuery application runbook](../kubernetes/apps/bigquery/README.md#offline-query-oracle) defines their aggregate contract and evidence limits.
The internal [BigQuery resource adapter](../kubernetes/apps/bigquery/README.md#resource-adapter) prepares owned table and bounded query operations for a future executor.
The [resource controller](../kubernetes/apps/bigquery/README.md#durable-resource-controller) adds durable intents, query slots, evidence pointers and cleanup after the [quiescence barrier](../kubernetes/apps/bigquery/README.md#quiescence-barrier), which the supervisor factory now builds from the run's own identity rather than taking from its caller; the internal BigQuery runner and supervisor use explicit actor handoffs, while production entrypoints remain disabled.
The [authenticated actor factories](../kubernetes/apps/bigquery/README.md#authenticated-internal-actors) bind external approvals and role-specific BigQuery sessions to those internal loops; they do not enable production dispatch.
The render-only [BigQuery execution proposal](../kubernetes/apps/bigquery/README.md#offline-execution-proposal) binds the resource plan and both application phases to a reviewable supervisor bundle without creating an approval.
The checkout-dependent [BigQuery approval bundle](../kubernetes/apps/bigquery/README.md#approval-bound-delivery-bundle) prepares and verifies delivery against a separately supplied approval without admitting execution.

The internal [BigQuery recovery exercise](../kubernetes/apps/bigquery/README.md#internal-recovery-execution) coordinates table provisioning, state-preserving recovery and bounded final queries.

The [lifecycle runbook](../kubernetes/lifecycle/README.md) defines execution approval and cleanup.
Third-party dependencies remain installed in the pinned image, while the reviewed package source the supervisor entrypoint can import is projected through an immutable ConfigMap.
There is no package installation during Pod startup.
