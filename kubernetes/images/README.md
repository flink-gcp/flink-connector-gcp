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
The [publication workflow](../../.github/workflows/tier3-images.yaml) builds their images, and copies the public ones they need, into `us-central1-docker.pkg.dev/flink-gcp/flink-tier3`.

## Images

| GAR package | Source and bundled dependencies | Publication |
| --- | --- | --- |
| `operator` | [Flink Kubernetes Operator 1.16.1](https://github.com/apache/flink-kubernetes-operator/tree/release-1.16.1), Java and Operator/standalone JARs | Copy the AMD64 digest pinned in the workflow |
| `flink` | [Flink 2.2.1, Scala 2.12, Java 17](https://github.com/apache/flink-docker/tree/983be3455636eb12cd1d3dee1efc8e32c4b875db/2.2/scala_2.12-java17-ubuntu), including `opt/flink-gs-fs-hadoop-2.2.1.jar` | Copy the AMD64 digest pinned in the workflow |
| `lifecycle-tools` | [Python 3.12.14 slim-bookworm](https://github.com/docker-library/python/tree/688a0b86bb44289df16a363e9f41d90514c1a5f9/3.12/slim-bookworm), CA certificates, kubectl 1.35.7 and the [locked official Python SDKs](../../tools/tier3/pyproject.toml) | Export the Tier-3 member dependencies, then build [the Dockerfile](lifecycle/Dockerfile) for AMD64 with its pinned base and kubectl checksum |
| `bigquery-recovery` | Reviewed Flink 2.2.1/Java 17 base, the [BigQuery recovery application](../apps/bigquery/README.md) and unmodified runtime dependency JARs | Verify the application reactor, then build its Dockerfile for AMD64; publication requires separate approval |
| `pubsub-recovery` | Reviewed Flink 2.2.1/Java 17 base, the [Pub/Sub recovery application](../apps/pubsub/README.md) and unmodified runtime dependency JARs | Verify the application reactor, then build its Dockerfile for AMD64; publication requires separate approval |
| `cloudtasks-recovery` | Reviewed Flink 2.2.1/Java 17 base, the [Cloud Tasks recovery application](../apps/cloudtasks/README.md) and unmodified runtime dependency JARs | Verify the application reactor, then build its Dockerfile for AMD64; publication requires separate approval |
| `bigtable-recovery` | Reviewed Flink 2.2.1/Java 17 base, the [Bigtable recovery application](../apps/bigtable/README.md) and unmodified runtime dependency JARs | Verify the application reactor, then build its Dockerfile for AMD64; publication requires separate approval |
| `spanner-recovery` | Reviewed Flink 2.2.1/Java 17 base, the [Spanner recovery application](../apps/spanner/README.md) and unmodified runtime dependency JARs | Verify the application reactor, then build its Dockerfile for AMD64; publication requires separate approval |
| `firestore-recovery` | Reviewed Flink 2.2.1/Java 17 base, the [Firestore recovery application](../apps/firestore/README.md) and unmodified runtime dependency JARs | Verify the application reactor, then build its Dockerfile for AMD64; publication requires separate approval |
| `smoke` | Reviewed Flink base pin, the [generic smoke application](../apps/smoke/README.md) and the unchanged Apache Datagen 2.2.1 JAR | Build the application payload with Maven, then its Dockerfile for AMD64 |

Operator and Flink use `crane copy`; they are not rebuilt.
Digest-addressed content and transfer integrity are handled by the registry tools.
The lifecycle Dockerfile adds kubectl, google-auth, google-cloud-storage and the Kubernetes Python client, then runs as UID/GID 65532.
The `flink-tier3` workspace member in [pyproject.toml](../../tools/tier3/pyproject.toml) defines the CLI/image dependencies; [uv.lock](../../uv.lock) pins their transitive versions and artifact hashes.
The root test dependency group includes this workspace member.
Before the image build, `uv export --locked --package flink-tier3 --no-dev --no-emit-workspace` generates `lifecycle/target/requirements.txt`, including hashes and platform markers, without pytest or the project's Java parsers.
This generated file is an ignored build input; it is never edited or locked separately.
Installation permits only binary distributions and checks dependency consistency and SDK imports during the image build.
The installed distributions retain their bundled license and notice files, including certifi's MPL-2.0 certificate bundle.
SDK dependencies are installed before publication, so supervisor startup needs no PyPI access.
After editing the member dependencies, update the root lock and regenerate the image input from the repository root:

```sh
mise x uv -- uv lock
mise x -- just tier3-lifecycle-requirements
```

Run the export recipe before a local Docker build as well; the publication workflow runs it before cloud authentication.
The locked export fails when dependency declarations and `uv.lock` disagree.
Docker's setup-buildx, metadata and build-push actions build it, apply a full-commit `sha-...` tag and OCI labels, and publish it.
The BigQuery, Pub/Sub, Cloud Tasks, Bigtable, Spanner and Firestore contexts are `kubernetes/apps/bigquery`, `kubernetes/apps/pubsub`, `kubernetes/apps/cloudtasks`, `kubernetes/apps/bigtable`, `kubernetes/apps/spanner` and `kubernetes/apps/firestore`; their `.dockerignore` files admit only each Dockerfile, application JAR and packaged runtime dependency JARs.
Their CI lanes also build the Dockerfiles without publishing, using the public digest that the publication workflow mirrors into GAR.
The lifecycle build context is only `kubernetes/images/lifecycle`; the smoke context is `kubernetes/apps/smoke`, with `.dockerignore` allowing only the Dockerfile and two packaged JARs.

Source images retain their upstream notices, licenses and OS package metadata.
Flink and Operator use Apache-2.0, Python uses PSF-2.0, and kubectl uses Apache-2.0; the base distributions retain their component licenses.

The idle Operator configuration requires no init container, sidecar, webhook or certificate image.
Each application image includes its JAR, which its `just tier3-<connector>-verify` recipe builds, and enables the bundled GCS plugin through `ENABLE_BUILT_IN_PLUGINS=flink-gs-fs-hadoop-2.2.1.jar`.
`lifecycle-tools` supplies only the supervisor's runtime tools; its program arrives through the [lifecycle](../lifecycle/README.md) ConfigMap.
Required packages and JARs are present in images before admission, so startup does not download them from public endpoints.

## Publish

The GCP OpenTofu root owns `tier3-image-publisher@flink-gcp.iam.gserviceaccount.com`.
It has Writer access to the Tier-3 GAR repository only.
Its WIF binding selects this repository's exact `tier3-images.yaml` workflow, `workflow_dispatch` and `refs/heads/main`.
Publication does not use infrastructure or workload credentials.

Dispatch the workflow against the reviewed main commit, naming the application to publish:

```sh
gh workflow run tier3-images.yaml --repo flink-gcp/flink-connector-gcp \
  --ref main -f expected_sha="$REVIEWED_MAIN_SHA" -f application=firestore
```

The `application` input is `all`, the default, or one of `smoke`, `bigquery`, `pubsub`, `cloudtasks`, `bigtable`, `spanner` and `firestore`.
A dispatch verifies the named application with its `just tier3-<application>-verify` recipe, or all seven for `all`, before authenticating with WIF and Docker's login action, copies Operator and Flink, then builds and pushes the lifecycle-tools image and that application's image, or all seven, with BuildKit.
Every dispatch builds the lifecycle-tools image, about 20 seconds, because a trial runs the supervisor image that [pins.cue](pins.cue) pins, the preflight refuses one that becomes deletion-eligible within a day of the run's expiry, and renewing the pin takes a new image and a reviewed change to the pin.
BigQuery, Pub/Sub, Cloud Tasks, Bigtable, Spanner and Firestore publication require separate approval before dispatch; adding this path does not authorize a publication or trial.
It uses one standard Ubuntu runner, permits one publication at a time and has a 45-minute job timeout.
The fixed source digests define what is transferred; there is no custom image-content inspector, capacity quota or intermediate receipt format.
Docker's login action logs out when the job ends.

A successful run names its `application` input in the GitHub Actions job summary, and lists the Operator and Flink digest references and the digest of each image it built: lifecycle tools, smoke and the recovery applications.
Update the Operator digest in `opentofu/tier3-operator/values.yaml` and the Flink/lifecycle-tools references in [pins.cue](pins.cue) through a reviewed PR.
The `smoke` reference in the same package supplies the complete application image to the committed [generic smoke deliveries](../runs/generic-smoke/common.cue).
The build action also supplies its standard build summary.
A failed run can leave already-published images in GAR; check the failed step and rerun the reviewed workflow as needed.
Do not adopt image pins from an incomplete run.
Workload admission must recheck that digest's availability and retention window.
Publication admits no workload.

## Retention and acceptance

GAR makes every version eligible for deletion after seven days, including tagged or currently selected versions.
There is no keep exception; immutable tags and vulnerability scanning are disabled.
[Cleanup runs asynchronously](https://docs.cloud.google.com/artifact-registry/docs/repositories/cleanup-policy), so seven days is eligibility, not an exact deletion deadline.
Adding a tag or copying an existing digest does not establish a renewed retention period.
Before a workload run, the lifecycle preflight must check image availability and enough remaining retention for the run end plus 24 hours.
A rebuilt image gets a new digest and so a new creation time, but the `operator` and `flink` mirrors keep their source digests, and a copy onto a version still in GAR leaves its creation time as it was.
When a mirror is near expiry, do not wait for cleanup: while no Tier-3 run, recovery or publication is in progress, delete the mirror's version yourself, then dispatch the publication, which recreates it with a fresh creation time.
The publisher has Writer access only and cannot delete, so this takes an administrator's credentials:

```sh
gcloud artifacts docker images delete --delete-tags --quiet \
  "us-central1-docker.pkg.dev/flink-gcp/flink-tier3/operator@$OPERATOR_DIGEST"
gcloud artifacts docker images delete --delete-tags --quiet \
  "us-central1-docker.pkg.dev/flink-gcp/flink-tier3/flink@$FLINK_DIGEST"
```

Between the deletion and the end of the publication, no Pod can pull either image, which is why nothing may be running.
The digests stay the same, so the Helm pin and the `flink` pin stand.

## Pins and the publication record

[pins.cue](pins.cue) is the `github.com/flink-gcp/flink-connector-gcp/kubernetes/images` CUE package.
Its `flink` field is the base runtime the application images build on and holds no application JAR, `lifecycleTools` is the supervisor's Python and kubectl runtime, and `smoke` is the complete smoke application image.
A comment beside each pin names the publication run that created its GAR version, and the Operator pin is in [Helm values](../../opentofu/tier3-operator/values.yaml).
A recovery application image's digest is a dispatch input that the run verifies live, so no file pins one.
A trial's findings record names the publication and the digests it ran; the [publication record](../../docs/adr/evidence/0165-tier3-image-publications.md) keeps those of the trials up to 2026-11-12.

For a shell or Docker build argument, read a pin with:

```sh
mise x cue -- cue -C kubernetes export ./images -e flink --out text
```

Pin updates retain zero Operator replicas and zero Pod/PVC quotas and require the existing post-apply idle-release verification and empty refreshed Helm plan.
Every future auxiliary Pod-template image must also be available in GAR before admission.
Publication does not start workload Pods or prove GKE runtime behavior.
