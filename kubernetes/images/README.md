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

# Tier-3 image publication

Private Tier-3 nodes have no Cloud NAT path to public registries.
The [publication workflow](../../.github/workflows/tier3-images.yaml) supplies their images in `us-central1-docker.pkg.dev/flink-gcp/flink-tier3` for [#1308](https://github.com/flink-gcp/flink-connector-gcp/issues/1308).

## Images

| GAR package | Source and bundled dependencies | Publication |
| --- | --- | --- |
| `operator` | [Flink Kubernetes Operator 1.15.0](https://github.com/apache/flink-kubernetes-operator/tree/release-1.15.0), Java and Operator/standalone JARs | Copy the AMD64 digest pinned in the workflow |
| `flink` | [Flink 2.2.1, Scala 2.12, Java 17](https://github.com/apache/flink-docker/tree/983be3455636eb12cd1d3dee1efc8e32c4b875db/2.2/scala_2.12-java17-ubuntu), including `opt/flink-gs-fs-hadoop-2.2.1.jar` | Copy the AMD64 digest pinned in the workflow |
| `lifecycle-tools` | [Python 3.12.14 slim-bookworm](https://github.com/docker-library/python/tree/688a0b86bb44289df16a363e9f41d90514c1a5f9/3.12/slim-bookworm), standard library and CA certificates, plus kubectl 1.35.7 | Build [the Dockerfile](lifecycle/Dockerfile) for AMD64 with its pinned base and kubectl checksum |

Operator and Flink use `crane copy`; they are not rebuilt.
Digest-addressed content and transfer integrity are handled by the registry tools.
The lifecycle Dockerfile adds kubectl and runs as UID/GID 65532.
Docker's setup-buildx, metadata and build-push actions build it, apply a full-commit `sha-...` tag and OCI labels, and publish it.
The build context is only `kubernetes/images/lifecycle`.

Source images retain their upstream notices, licenses and OS package metadata.
Flink and Operator use Apache-2.0, Python uses PSF-2.0, and kubectl uses Apache-2.0; the base distributions retain their component licenses.

The idle Operator configuration requires no init container, sidecar, webhook or certificate image.
The application JAR belongs to [#1309](https://github.com/flink-gcp/flink-connector-gcp/issues/1309).
Its application image must include the JAR locally and enable the bundled GCS plugin, for example through `ENABLE_BUILT_IN_PLUGINS=flink-gs-fs-hadoop-2.2.1.jar`.
Supervisor logic and its Kubernetes permissions belong to [#1310](https://github.com/flink-gcp/flink-connector-gcp/issues/1310); `lifecycle-tools` supplies only that program's runtime tools.
Required packages and JARs are present in images before admission, so startup does not download them from public endpoints.

## Publish

The GCP OpenTofu root owns `tier3-image-publisher@flink-gcp.iam.gserviceaccount.com`.
It has Writer access to the Tier-3 GAR repository only.
Its WIF binding selects this repository's exact `tier3-images.yaml` workflow, `workflow_dispatch` and `refs/heads/main`.
Publication does not use infrastructure or workload credentials.

Review and merge the infrastructure PR, let normal CI apply finish, and require an empty refreshed plan before the first publication.
The new publisher and first publication require the agreed execution approval; the existing foundation does not require creation approval again.

Dispatch the workflow against the reviewed main commit:

```sh
gh workflow run tier3-images.yaml --repo flink-gcp/flink-connector-gcp \
  --ref main -f expected_sha="$REVIEWED_MAIN_SHA"
```

The workflow authenticates with WIF and Docker's login action, copies Operator and Flink, then builds and pushes the lifecycle tools with BuildKit.
It uses one standard Ubuntu runner, permits one publication at a time and has a 30-minute job timeout.
The fixed source digests define what is transferred; there is no custom image-content inspector, capacity quota or intermediate receipt format.
Docker's login action logs out when the job ends.

A successful run lists all three GAR digest references in the GitHub Actions job summary.
Copy those references into the follow-up Helm/CUE pin PR.
The build action also supplies its standard build summary.
A failed run can leave already-published images in GAR; check the failed step and rerun the reviewed workflow as needed.
Do not adopt image pins from an incomplete run.

## Retention and acceptance

GAR makes every version eligible for deletion after seven days, including tagged or currently selected versions.
There is no keep exception; immutable tags and vulnerability scanning are disabled.
[Cleanup runs asynchronously](https://docs.cloud.google.com/artifact-registry/docs/repositories/cleanup-policy), so seven days is eligibility, not an exact deletion deadline.
Adding a tag or copying an existing digest does not establish a renewed retention period.
Before a workload run, the lifecycle preflight must check image availability and enough remaining retention for the run end plus 24 hours.
If a required digest is near expiry, wait for cleanup and republish.

This PR supplies publication and IAM; it does not yet commit real GAR output pins.
The follow-up must cover Operator, Flink and every auxiliary Pod-template image, retain zero Operator replicas and zero Pod/PVC quotas, and verify the idle release after apply.
Publication does not start workload Pods or prove GKE runtime behavior.
[#1308](https://github.com/flink-gcp/flink-connector-gcp/issues/1308) remains open until successful publication and the pin adoption are complete.
