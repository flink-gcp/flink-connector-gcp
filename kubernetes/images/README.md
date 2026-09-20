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
| `lifecycle-tools` | [Python 3.12.14 slim-bookworm](https://github.com/docker-library/python/tree/688a0b86bb44289df16a363e9f41d90514c1a5f9/3.12/slim-bookworm), CA certificates, kubectl 1.35.7 and the [locked official Python SDKs](../../tools/tier3/pyproject.toml) | Export the Tier-3 member dependencies, then build [the Dockerfile](lifecycle/Dockerfile) for AMD64 with its pinned base and kubectl checksum |
| `cloudtasks-measurement` | Reviewed Flink 2.2.1 base, the [Cloud Tasks measurement application](../apps/cloudtasks/README.md) and unmodified runtime dependency JARs | Verify the selected reactor, then build its Dockerfile for AMD64 |
| `cloudtasks-measurement-flink120` | Reviewed Flink 1.20.4/Java 17 base and the same application compiled with the Flink 1.x compatibility source root | Select `cloudtasks_flink_version=1.20.4`; build separately from the 2.2.1 package |
| `bigquery-recovery` | Reviewed Flink 2.2.1/Java 17 base, the [BigQuery recovery application](../apps/bigquery/README.md) and unmodified runtime dependency JARs | Verify the application reactor, then build its Dockerfile for AMD64; publication requires separate approval |
| `pubsub-recovery` | Reviewed Flink 2.2.1/Java 17 base, the [Pub/Sub recovery application](../apps/pubsub/README.md) and unmodified runtime dependency JARs | Verify the application reactor, then build its Dockerfile for AMD64; publication requires separate approval |
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
The Cloud Tasks, BigQuery and Pub/Sub contexts are `kubernetes/apps/cloudtasks`, `kubernetes/apps/bigquery` and `kubernetes/apps/pubsub`; their `.dockerignore` files admit only each Dockerfile, application JAR and packaged runtime dependency JARs.
The BigQuery and Pub/Sub CI lanes also build their Dockerfiles without publishing, using the public digest that the publication workflow mirrors into GAR.
The lifecycle build context is only `kubernetes/images/lifecycle`; the smoke context is `kubernetes/apps/smoke`, with `.dockerignore` allowing only the Dockerfile and two packaged JARs.

Source images retain their upstream notices, licenses and OS package metadata.
Flink and Operator use Apache-2.0, Python uses PSF-2.0, and kubectl uses Apache-2.0; the base distributions retain their component licenses.

The idle Operator configuration requires no init container, sidecar, webhook or certificate image.
The application JAR is built by `just tier3-smoke-verify` for [#1309](https://github.com/flink-gcp/flink-connector-gcp/issues/1309).
Its image includes the JAR locally and enables the bundled GCS plugin through `ENABLE_BUILT_IN_PLUGINS=flink-gs-fs-hadoop-2.2.1.jar`.
Supervisor logic and its Kubernetes permissions belong to [#1310](https://github.com/flink-gcp/flink-connector-gcp/issues/1310); `lifecycle-tools` supplies only that program's runtime tools.
Required packages and JARs are present in images before admission, so startup does not download them from public endpoints.

## Publish

The GCP OpenTofu root owns `tier3-image-publisher@flink-gcp.iam.gserviceaccount.com`.
It has Writer access to the Tier-3 GAR repository only.
Its WIF binding selects this repository's exact `tier3-images.yaml` workflow, `workflow_dispatch` and `refs/heads/main`.
Publication does not use infrastructure or workload credentials.

Review and merge the infrastructure PR, let normal CI apply finish, and require an empty refreshed plan before the first publication.
The first publication was authorized and completed on 2026-09-13 after an empty refreshed GCP plan.

Dispatch the workflow against the reviewed main commit:

```sh
gh workflow run tier3-images.yaml --repo flink-gcp/flink-connector-gcp \
  --ref main -f expected_sha="$REVIEWED_MAIN_SHA"
```

The `cloudtasks_flink_version` choice defaults to `2.2.1`; select `1.20.4` in a separate dispatch for that measurement runtime.
The selected Cloud Tasks base is a fixed official AMD64 digest, and the Dockerfile requires its matching GCS plugin.
The 1.20.4 dispatch also mirrors that base into the `flink` package; smoke, BigQuery and Pub/Sub keep their reviewed 2.2.1 bases and payloads.
Each dispatch publishes only the selected Cloud Tasks variant, and both variants need successful publication before a two-line assessment.

Every dispatch verifies smoke, BigQuery, Pub/Sub and the selected Cloud Tasks application before authenticating with WIF and Docker's login action, copies Operator and Flink, then builds and pushes lifecycle tools and all four application images with BuildKit.
The BigQuery and Pub/Sub payloads are verified before the selected Cloud Tasks build; a later Flink 1.20.4 build cleans shared dependencies but does not rewrite their already-packaged applications or copied JARs.
BigQuery and Pub/Sub publication require separate approval before dispatch; adding this path does not authorize a publication or trial.
It uses one standard Ubuntu runner, permits one publication at a time and has a 30-minute job timeout.
The fixed source digests define what is transferred; there is no custom image-content inspector, capacity quota or intermediate receipt format.
Docker's login action logs out when the job ends.

A successful run lists the Operator, Flink, lifecycle tools, smoke and selected Cloud Tasks base/application digest references in the GitHub Actions job summary.
It also lists the shared BigQuery/Pub/Sub base once, followed by both recovery application digests.
Update the Operator digest in `opentofu/tier3-operator/values.yaml` and the Flink/lifecycle-tools references in [pins.cue](pins.cue) through a reviewed PR.
The `smoke` reference in the same package supplies the complete application image to the committed [generic smoke deliveries](../runs/generic-smoke/common.cue).
The build action also supplies its standard build summary.
A failed run can leave already-published images in GAR; check the failed step and rerun the reviewed workflow as needed.
Do not adopt image pins from an incomplete run.
The Cloud Tasks application needs its first authorized publication before a session dispatch can name its digest; that digest is a dispatch input verified live against the registry, not a pin in this package.
The [first BigQuery publication](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/35489876881) built `116f2b8d9f992ecca7282d6320468db2b4a5c196`, before the application added appender observations.
The updated BigQuery application needs another authorized publication and a separately reviewed digest before workload admission.
The Pub/Sub application needs its first authorized publication and a separately reviewed digest before workload admission.
Publication supplies no Cloud Tasks, BigQuery or Pub/Sub workload admission or service-measurement approval.

## Retention and acceptance

GAR makes every version eligible for deletion after seven days, including tagged or currently selected versions.
There is no keep exception; immutable tags and vulnerability scanning are disabled.
[Cleanup runs asynchronously](https://docs.cloud.google.com/artifact-registry/docs/repositories/cleanup-policy), so seven days is eligibility, not an exact deletion deadline.
Adding a tag or copying an existing digest does not establish a renewed retention period.
Before a workload run, the lifecycle preflight must check image availability and enough remaining retention for the run end plus 24 hours.
If a required digest is near expiry, wait for cleanup and republish.

The [first successful publication](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/34760632704) used main commit `78e96a9a0d765412ee5f7ef2b66b63cababd19a0` on 2026-09-13.
GAR reads confirmed all three references; the mirrored digests match their sources.
The Operator pin is in [Helm values](../../opentofu/tier3-operator/values.yaml), and CUE deliveries can import [pins.cue](pins.cue) as `github.com/flink-gcp/flink-connector-gcp/kubernetes/images`.
The `flink` field supplies the base runtime for the application-image work in [#1309](https://github.com/flink-gcp/flink-connector-gcp/issues/1309); it contains no application JAR.
The `lifecycleTools` field supplies the Python/kubectl runtime for [#1310](https://github.com/flink-gcp/flink-connector-gcp/issues/1310).
The [first successful smoke publication](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/34768916308) built main commit `053e23835782059830f871ca53f90fba43efa324` on 2026-09-14 JST, after the GCP/bootstrap applies succeeded and their refreshed plans were empty.
A GAR read of its full-commit tag confirmed `sha256:29cc0533b2e1a984343a51315cdfe4110aa028a6c040d2275a103c3cfa591a3d`, which `images.smoke` then selected until the Cloud Tasks measurement publication below.
That smoke adoption retained the earlier Operator, Flink and lifecycle-tools pins.

The [SDK image publication](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/34844507450) built main commit `abf35f5a16e50be11432788b602bdf8357f9e8ad` on 2026-09-14.
A GAR read of its full-commit tag confirmed the lifecycle-tools digest that [pins.cue](pins.cue) selected until the Cloud Tasks measurement publication below.
The pulled image started the shared runtime offline as UID 65532, and its requirements export matched the root uv lock export.
This lifecycle-tools adoption retains the Operator, Flink and smoke pins.

The Cloud Tasks measurement publication built main commit `339d0a90675dffee74305cebe021093c6a1f9b4b` on 2026-09-19 UTC, once for [the 2.2.1 line](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/35474382654) and once for [the 1.20.4 line](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/35474834488).
Each run also republishes the smoke and lifecycle-tools images, so the same commit produced two equivalent builds of each; the pins take the later pair, which is what the commit's tag now resolves to.
GAR reads confirmed all four digests against that tag, and the seven-day eligibility clock restarted at 2026-09-19T23:07Z for the lifecycle-tools and smoke pins.
The application digests stay out of this file: they are dispatch inputs verified live, so the two measurement packages are never pinned.
This adoption retains the Operator and Flink pins.

For a shell or Docker build argument, read a pin with:

```sh
mise x cue -- cue -C kubernetes export ./images -e flink --out text
```

Pin updates retain zero Operator replicas and zero Pod/PVC quotas and require the existing post-apply idle-release verification and empty refreshed Helm plan.
Every future auxiliary Pod-template image must also be available in GAR before admission.
Publication does not start workload Pods or prove GKE runtime behavior.
