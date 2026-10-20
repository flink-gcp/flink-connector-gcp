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
| `bigquery-recovery` | Reviewed Flink 2.2.1/Java 17 base, the [BigQuery recovery application](../apps/bigquery/README.md) and unmodified runtime dependency JARs | Verify the application reactor, then build its Dockerfile for AMD64; publication requires separate approval |
| `pubsub-recovery` | Reviewed Flink 2.2.1/Java 17 base, the [Pub/Sub recovery application](../apps/pubsub/README.md) and unmodified runtime dependency JARs | Verify the application reactor, then build its Dockerfile for AMD64; publication requires separate approval |
| `cloudtasks-recovery` | Reviewed Flink 2.2.1/Java 17 base, the [Cloud Tasks recovery application](../apps/cloudtasks/README.md#recovery-application) and unmodified runtime dependency JARs | Verify the application reactor, then build its Dockerfile for AMD64; publication requires separate approval |
| `bigtable-recovery` | Reviewed Flink 2.2.1/Java 17 base, the [Bigtable recovery application](../apps/bigtable/README.md) and unmodified runtime dependency JARs | Verify the application reactor, then build its Dockerfile for AMD64; publication requires separate approval |
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
The BigQuery, Pub/Sub, Cloud Tasks and Bigtable contexts are `kubernetes/apps/bigquery`, `kubernetes/apps/pubsub`, `kubernetes/apps/cloudtasks` and `kubernetes/apps/bigtable`; their `.dockerignore` files admit only each Dockerfile, application JAR and packaged runtime dependency JARs.
Their CI lanes also build the Dockerfiles without publishing, using the public digest that the publication workflow mirrors into GAR.
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

The workflow no longer publishes the Cloud Tasks measurement images (`cloudtasks-measurement` and `cloudtasks-measurement-flink120`): only the removed cluster session ran them, and the single-host rig runs the application from its build ([#1723](https://github.com/flink-gcp/flink-connector-gcp/issues/1723)).
Versions already published expire under the repository's seven-day cleanup policy.
It publishes the Cloud Tasks recovery application as `cloudtasks-recovery` instead, for the cluster recovery trial of [#1362](https://github.com/flink-gcp/flink-connector-gcp/issues/1362), on the Flink 2.2.1 line only.

Every dispatch verifies smoke, BigQuery, Pub/Sub, Cloud Tasks and Bigtable before authenticating with WIF and Docker's login action, copies Operator and Flink, then builds and pushes lifecycle tools and all five application images with BuildKit.
BigQuery, Pub/Sub, Cloud Tasks and Bigtable publication require separate approval before dispatch; adding this path does not authorize a publication or trial.
It uses one standard Ubuntu runner, permits one publication at a time and has a 30-minute job timeout.
The fixed source digests define what is transferred; there is no custom image-content inspector, capacity quota or intermediate receipt format.
Docker's login action logs out when the job ends.

A successful run lists the Operator, Flink, lifecycle tools and smoke digest references in the GitHub Actions job summary.
It also lists the shared recovery application base once, followed by the BigQuery, Pub/Sub, Cloud Tasks and Bigtable recovery application digests.
Update the Operator digest in `opentofu/tier3-operator/values.yaml` and the Flink/lifecycle-tools references in [pins.cue](pins.cue) through a reviewed PR.
The `smoke` reference in the same package supplies the complete application image to the committed [generic smoke deliveries](../runs/generic-smoke/common.cue).
The build action also supplies its standard build summary.
A failed run can leave already-published images in GAR; check the failed step and rerun the reviewed workflow as needed.
Do not adopt image pins from an incomplete run.
The [first BigQuery publication](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/35489876881) built `116f2b8d9f992ecca7282d6320468db2b4a5c196`, before the application added appender observations; the [BigQuery trial publication](#bigquery-trial-publication) below supersedes it.
The [first Pub/Sub publication](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/35494622116) built `cba9043faee0ceb067cba23b56fe3e0abe7f0542`; its [application runbook](../apps/pubsub/README.md#deployment-definition) records the GAR-verified digest.
Workload admission must recheck that digest's availability and retention window.
Publication supplies no BigQuery, Pub/Sub or Cloud Tasks workload admission or service-measurement approval.

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
GAR reads confirmed all four digests against that tag, and the seven-day eligibility clock restarted at 2026-09-19T23:07Z for the lifecycle-tools and smoke pins, which [pins.cue](pins.cue) selected until the [BigQuery trial publication](#bigquery-trial-publication) below.
The application digests stay out of this file: they are dispatch inputs verified live, so the two measurement packages are never pinned.
This adoption retains the Operator and Flink pins.

### BigQuery trial publication

The [BigQuery trial publication](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/35869627208) built main commit `8bfe56d5154d40b2ccef81f16129ea873ead43a5` on 2026-09-23 for the trials [#1427](https://github.com/flink-gcp/flink-connector-gcp/issues/1427) dispatches.
It supersedes two earlier `bigquery-recovery` publications from 2026-09-20 UTC: 04:54Z from `116f2b8d9`, before the application's appender observations, and 06:50Z from `cba9043fa` by the [first Pub/Sub publication](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/35494622116), which carried them but was never adopted; both become deletion-eligible on 2026-09-27, before a campaign could finish.
Before it ran, the `operator` version `sha256:2c667a1c…` created 2026-09-20T03:52Z was deleted by hand: the workflow copies that same digest from Docker Hub, and a copy of a digest the registry still holds keeps its creation time, so without the deletion the mirror would have become deletion-eligible three days into the campaign.

| Package | Digest | Created (UTC) | Deletion-eligible (UTC) |
| --- | --- | --- | --- |
| `operator` | `sha256:2c667a1c14bdb7365f16811dd98a576d6e4993ccbcc369dc53fb75081116ee78` | 2026-09-23T14:02:14Z | 2026-09-30T14:02:14Z |
| `lifecycle-tools` | `sha256:107e3096d0a4d09f625b8ce61eeae1f961297db2848a5650957739af538940ca` | 2026-09-23T14:02:34Z | 2026-09-30T14:02:34Z |
| `smoke` | `sha256:8e4803c7330b68a80326fdb2542721f40859ee85bcba0450560ad5ccb99ffe45` | 2026-09-23T14:02:49Z | 2026-09-30T14:02:49Z |
| `bigquery-recovery` | `sha256:6fd22da4a44de73d2d5c15e3383c2a7b106987731b569bfb0fa73cfdcb4e6c2d` | 2026-09-23T14:03:04Z | 2026-09-30T14:03:04Z |

The digests are the ones the `sha-8bfe56d5…` tag resolves to, and `operator` the one its `1.15.0` tag resolves to, read from GAR after the run; the Operator digest is unchanged, so its Helm pin stands.
[pins.cue](pins.cue) adopted the lifecycle-tools and smoke digests until the [BigQuery FILE_LOADS trial publication](#bigquery-file_loads-trial-publication) below; the `bigquery-recovery` digest stays out of it as a dispatch input verified live, like the other scenario packages.
Dispatch refuses an image unless the trial's window ends a day before eligibility, so the last BigQuery trial window from this publication must end before 2026-09-29T14:02:14Z, and it starts at most 90 minutes earlier.
The `flink` base mirror kept its 2026-09-20 creation time; a trial does not pull it, and a later build recopies it once cleanup has removed it.

### BigQuery FILE_LOADS trial publication

The [BigQuery FILE_LOADS trial publication](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/37956928238) built main commit `ef183e0c63d273e59b05599d064caf79e79dfc22` on 2026-10-09 UTC for the `fl-10` trial of [#1552](https://github.com/flink-gcp/flink-connector-gcp/issues/1552); its `bigquery-recovery` image is the first to carry the FILE_LOADS mode.
Every earlier version had passed its seven-day eligibility and been deleted, so the registry held none when it ran, and the `operator` and `flink` mirrors took fresh creation times without a manual deletion.

| Package | Digest | Created (UTC) | Deletion-eligible (UTC) |
| --- | --- | --- | --- |
| `operator` | `sha256:2c667a1c14bdb7365f16811dd98a576d6e4993ccbcc369dc53fb75081116ee78` | 2026-10-09T16:22:01Z | 2026-10-16T16:22:01Z |
| `flink` | `sha256:a093c60a9ab038f8821a3bfce4c3236ce37c2ac8e2a94f741f49cb1689baa3cd` | 2026-10-09T16:22:28Z | 2026-10-16T16:22:28Z |
| `lifecycle-tools` | `sha256:69d4424cc81dae202ee9aa0d1ae68969c7af48622c3b5f545d58b272c5fec71f` | 2026-10-09T16:22:50Z | 2026-10-16T16:22:50Z |
| `smoke` | `sha256:8e1d7e18fe882b36abb6ceea78240ee4482b2ba4ff708fc3b463f17662fa1f63` | 2026-10-09T16:23:06Z | 2026-10-16T16:23:06Z |
| `bigquery-recovery` | `sha256:a8d130362f2a3af35262bd63efa64106f6cc3effd677518ad3d4671904f2ea0f` | 2026-10-09T16:23:22Z | 2026-10-16T16:23:22Z |

The digests are the ones the `sha-ef183e0c…` tag resolves to, and `operator` and `flink` the ones their `1.15.0` and `2.2.1` tags resolve to, read from GAR after the run; they match the run log, and the Operator and Flink digests are unchanged, so the Helm pin and the `flink` pin stand.
[pins.cue](pins.cue) adopted the lifecycle-tools and smoke digests and cited this run for the recreated `flink` version until the [BigQuery fault trial publication](#bigquery-fault-trial-publication) below; the `bigquery-recovery` digest stays a dispatch input verified live.
Dispatch refused an image unless the trial's window ended a day before eligibility, so a `fl-10` attempt from this publication had to be admitted so that its 90-minute window ended before 2026-10-15T16:22:01Z.
Its `bigquery-recovery` image refuses the `--parallelism` argument every BigQuery delivery renders since [#1782](https://github.com/flink-gcp/flink-connector-gcp/issues/1782), so it can no longer run a trial.
The same run published Cloud Tasks and Pub/Sub application images from that commit, which nothing adopts.

### Cloud Tasks recovery trial publication

The [Cloud Tasks recovery trial publication](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38243294462) built main commit `dbfc852734913ff698cb719665ea318e94891d93` on 2026-10-12 UTC, right after the recovery application merged, for the trial of [#1362](https://github.com/flink-gcp/flink-connector-gcp/issues/1362); its `cloudtasks-recovery` image is the first of that package.

| Package | Digest | Created (UTC) | Deletion-eligible (UTC) |
| --- | --- | --- | --- |
| `operator` | `sha256:2c667a1c14bdb7365f16811dd98a576d6e4993ccbcc369dc53fb75081116ee78` | 2026-10-09T16:22:01Z | 2026-10-16T16:22:01Z |
| `flink` | `sha256:a093c60a9ab038f8821a3bfce4c3236ce37c2ac8e2a94f741f49cb1689baa3cd` | 2026-10-09T16:22:28Z | 2026-10-16T16:22:28Z |
| `cloudtasks-recovery` | `sha256:cc66ba8465ce7295fc1f91e1ac95ccf078236b863d8f09a6c2187c3638d667c9` | 2026-10-12T11:43:49Z | 2026-10-19T11:43:49Z |

The digests are the ones the `sha-dbfc8527…` tag resolves to, and `operator` and `flink` the ones their `1.15.0` and `2.2.1` tags resolve to, read from GAR after the run.
The `operator` and `flink` copies kept the creation times of the [FILE_LOADS trial publication](#bigquery-file_loads-trial-publication), because the registry still held those digests.
Dispatch refuses an image unless the trial's window ends a day before eligibility, so the trial had to be admitted so that its 90-minute window ended before 2026-10-15T16:22:01Z; the `operator` version was deleted by hand and republished by the [BigQuery fault trial publication](#bigquery-fault-trial-publication) below.
Its `cloudtasks-recovery` image refuses the `--sink-parallelism` argument every Cloud Tasks delivery renders since [#1777](https://github.com/flink-gcp/flink-connector-gcp/issues/1777), so it can no longer run a trial.
The same run republished the lifecycle-tools, smoke, BigQuery and Pub/Sub images, which nothing adopts; [pins.cue](pins.cue) kept the FILE_LOADS publication's lifecycle-tools pin until the [BigQuery fault trial publication](#bigquery-fault-trial-publication) below.

### BigQuery fault trial publication

The [BigQuery fault trial publication](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38561068834) built main commit `25260e67823edc4efd60cc12a0db1f4d12912161` on 2026-10-14 UTC for the trials of [#1779](https://github.com/flink-gcp/flink-connector-gcp/issues/1779); its `bigquery-recovery` image is the first that accepts the `--parallelism` argument every BigQuery delivery renders since [#1782](https://github.com/flink-gcp/flink-connector-gcp/issues/1782).
Before it ran, the `operator` and `flink` versions created 2026-10-09T16:22Z were deleted by hand with the owner's approval, as the [BigQuery trial publication](#bigquery-trial-publication) did for `operator`, so both mirrors took fresh creation times rather than keeping ones that become deletion-eligible on 2026-10-16.

| Package | Digest | Created (UTC) | Deletion-eligible (UTC) |
| --- | --- | --- | --- |
| `operator` | `sha256:2c667a1c14bdb7365f16811dd98a576d6e4993ccbcc369dc53fb75081116ee78` | 2026-10-14T12:17:04Z | 2026-10-21T12:17:04Z |
| `flink` | `sha256:a093c60a9ab038f8821a3bfce4c3236ce37c2ac8e2a94f741f49cb1689baa3cd` | 2026-10-14T12:17:05Z | 2026-10-21T12:17:05Z |
| `lifecycle-tools` | `sha256:4f5ec406125a5eccddf8e95bd5f1a6cc355d7609f7322fdb4539434e5a003c2c` | 2026-10-14T12:17:28Z | 2026-10-21T12:17:28Z |
| `smoke` | `sha256:e4cfa8e10332ad603e9464c5acb6642649820544d128f1a6e3fe20383a54d7a2` | 2026-10-14T12:17:48Z | 2026-10-21T12:17:48Z |
| `bigquery-recovery` | `sha256:4295846cdd2187e84d7e2f7ddd7ba340934af63d4df40186bf380768ae362147` | 2026-10-14T12:17:56Z | 2026-10-21T12:17:56Z |
| `pubsub-recovery` | `sha256:4c91ccd5e270ac4974623f27ed866e0709b3a7d403121bfb215644a13d092a1f` | 2026-10-14T12:18:04Z | 2026-10-21T12:18:04Z |
| `cloudtasks-recovery` | `sha256:d2b32bdbcf112e5409e3674fdedd3188eea718f711c4ee2b54166f97a1422a65` | 2026-10-14T12:18:11Z | 2026-10-21T12:18:11Z |

The digests are the ones the `sha-25260e67…` tag resolves to, and `operator` and `flink` the ones their `1.15.0` and `2.2.1` tags resolve to, read from GAR after the run; they match the run log, and the Operator and Flink digests are unchanged, so the Helm pin and the `flink` pin stand.
[pins.cue](pins.cue) adopted the lifecycle-tools and smoke digests and cited this run for the recreated `flink` version until the [Bigtable rescale-in rerun publication](#bigtable-rescale-in-rerun-publication) below; the `bigquery-recovery` digest stays a dispatch input verified live.
Dispatch refuses an image unless the trial's window ends a day before eligibility, so a trial from this publication had to be admitted so that its 90-minute window ended before 2026-10-20T12:17:04Z.
The `pubsub-recovery` and `cloudtasks-recovery` images were built from the same commit, which nothing adopts; the Cloud Tasks changes of [#1777](https://github.com/flink-gcp/flink-connector-gcp/issues/1777) are not in this commit, so its `cloudtasks-recovery` image refuses their `--sink-parallelism` argument, and a Cloud Tasks trial needs the [Cloud Tasks fault trial publication](#cloud-tasks-fault-trial-publication) below.

### Cloud Tasks fault trial publication

The [Cloud Tasks fault trial publication](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38757542507) built main commit `3546b6ed8874f293e7d02de5ea8132e3f07746b7` on 2026-10-15 UTC for the trials of [#1778](https://github.com/flink-gcp/flink-connector-gcp/issues/1778); its `cloudtasks-recovery` image is the first that accepts the `--sink-parallelism` argument every Cloud Tasks delivery renders since [#1777](https://github.com/flink-gcp/flink-connector-gcp/issues/1777).

| Package | Digest | Created (UTC) | Deletion-eligible (UTC) |
| --- | --- | --- | --- |
| `operator` | `sha256:2c667a1c14bdb7365f16811dd98a576d6e4993ccbcc369dc53fb75081116ee78` | 2026-10-14T12:17:04Z | 2026-10-21T12:17:04Z |
| `flink` | `sha256:a093c60a9ab038f8821a3bfce4c3236ce37c2ac8e2a94f741f49cb1689baa3cd` | 2026-10-14T12:17:05Z | 2026-10-21T12:17:05Z |
| `lifecycle-tools` | `sha256:65a1b08a0f831607e9232d9d95a263a6fbbfb9fd48f1dbdf61eeee4343114219` | 2026-10-15T15:20:32Z | 2026-10-22T15:20:32Z |
| `smoke` | `sha256:74e7e49723e880abeb680e621dea231c5112f631a3efd98d94876e0ec3339a1d` | 2026-10-15T15:20:53Z | 2026-10-22T15:20:53Z |
| `bigquery-recovery` | `sha256:27bb3714f8813720ceb1c69d793661acbc5038cbc655bc9718c2eacda2746b5c` | 2026-10-15T15:21:02Z | 2026-10-22T15:21:02Z |
| `pubsub-recovery` | `sha256:a9a6d2d001216773575fb3d10ea43a9c471c5c8b0c3f035bb3f9a8c8ac734dbb` | 2026-10-15T15:21:12Z | 2026-10-22T15:21:12Z |
| `cloudtasks-recovery` | `sha256:0d6c75675aedf383eb8c0056f44bbf87f0e58bc7305c8285b5a1d3daa9d56f9e` | 2026-10-15T15:21:19Z | 2026-10-22T15:21:19Z |

The digests are the ones the `sha-3546b6ed…` tag resolves to, and `operator` and `flink` the ones their `1.15.0` and `2.2.1` tags resolve to, read from GAR after the run; they match the run log, and the Operator and Flink digests are unchanged, so the Helm pin and the `flink` pin stand.
The `operator` and `flink` copies kept the creation times of the [BigQuery fault trial publication](#bigquery-fault-trial-publication), because the registry still held those digests.
The same run republished the lifecycle-tools, smoke, BigQuery and Pub/Sub images, which nothing adopts: [pins.cue](pins.cue) kept that publication's lifecycle-tools and smoke pins, because the lifecycle image's inputs (its Dockerfile, the Tier-3 member dependencies and `uv.lock`) are unchanged between the two commits, no Cloud Tasks trial pulls `smoke`, and both earlier versions outlived the `operator` mirror; the `cloudtasks-recovery` digest stays a dispatch input verified live.
Dispatch refuses an image unless the trial's window ends a day before eligibility, so a trial from this publication had to be admitted so that its 90-minute window ended before 2026-10-20T12:17:04Z.
Its `cloudtasks-recovery` image fails a staged commit on a single `INTERNAL` from `CreateTask`, which [#1794](https://github.com/flink-gcp/flink-connector-gcp/issues/1794) made transient; the [Cloud Tasks INTERNAL retry publication](#cloud-tasks-internal-retry-publication) below carries that change.

### Cloud Tasks INTERNAL retry publication

The [Cloud Tasks INTERNAL retry publication](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/38921184987) built main commit `3a5832b8865142a3bbf96b466f7ee3aadbe52d76` on 2026-10-16 UTC for the remaining trials of [#1778](https://github.com/flink-gcp/flink-connector-gcp/issues/1778); its `cloudtasks-recovery` image is the first that retries a single `INTERNAL` from `CreateTask`, which [#1794](https://github.com/flink-gcp/flink-connector-gcp/issues/1794) made transient.

| Package | Digest | Created (UTC) | Deletion-eligible (UTC) |
| --- | --- | --- | --- |
| `operator` | `sha256:2c667a1c14bdb7365f16811dd98a576d6e4993ccbcc369dc53fb75081116ee78` | 2026-10-14T12:17:04Z | 2026-10-21T12:17:04Z |
| `flink` | `sha256:a093c60a9ab038f8821a3bfce4c3236ce37c2ac8e2a94f741f49cb1689baa3cd` | 2026-10-14T12:17:05Z | 2026-10-21T12:17:05Z |
| `lifecycle-tools` | `sha256:cd7187bb4c1ca1e2ba537564dca88a21b1ea959b70a0b02a780347bb4efedd37` | 2026-10-16T15:17:34Z | 2026-10-23T15:17:34Z |
| `smoke` | `sha256:0f1722783ebd2945be7802730935725c7e04e42e6a5be9ffc6b7f91e8ab28c32` | 2026-10-16T15:17:50Z | 2026-10-23T15:17:50Z |
| `bigquery-recovery` | `sha256:a8ab4128c44097f432012d2ba4a041be520fce60438b0954d45e6e73052054a5` | 2026-10-16T15:17:58Z | 2026-10-23T15:17:58Z |
| `pubsub-recovery` | `sha256:26ef5451deb3259db3ddc39a1e2c675d7d733091808beffe335a7c08d587bbaf` | 2026-10-16T15:18:04Z | 2026-10-23T15:18:04Z |
| `cloudtasks-recovery` | `sha256:e71790df17d5fe9383b1000cac6cf0e4fd007c17f7bef349ae4911a3a42ebfc0` | 2026-10-16T15:18:09Z | 2026-10-23T15:18:09Z |

The digests are the ones the `sha-3a5832b8…` tag resolves to, and `operator` and `flink` the ones their `1.15.0` and `2.2.1` tags resolve to, read from GAR after the run; they match the run log, and the Operator and Flink digests are unchanged, so the Helm pin and the `flink` pin stand.
The `operator` and `flink` copies kept the creation times of the [BigQuery fault trial publication](#bigquery-fault-trial-publication), because the registry still held those digests.
The same run republished the lifecycle-tools, smoke, BigQuery and Pub/Sub images, which nothing adopts: [pins.cue](pins.cue) kept the BigQuery fault trial publication's lifecycle-tools and smoke pins, because the lifecycle image's inputs are also unchanged between `25260e678` and `3a5832b88` and no Cloud Tasks trial pulls `smoke`; the `cloudtasks-recovery` digest stays a dispatch input verified live.
Dispatch refuses an image unless the trial's window ends a day before eligibility, so a trial from this publication had to be admitted so that its 90-minute window ended before 2026-10-20T12:17:04Z.
Its `cloudtasks-recovery` image fails a staged commit on a single `CANCELLED` from `CreateTask`, which [#1796](https://github.com/flink-gcp/flink-connector-gcp/issues/1796) made transient; the [Cloud Tasks CANCELLED retry publication](#cloud-tasks-cancelled-retry-publication) below carries that change.

### Cloud Tasks CANCELLED retry publication

The [Cloud Tasks CANCELLED retry publication](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/39022929316) built main commit `526a186f0e3d4550dfd838e812a5665b9dec0232` on 2026-10-17 UTC for the rescale out rerun of [#1799](https://github.com/flink-gcp/flink-connector-gcp/issues/1799); its `cloudtasks-recovery` image is the first that retries a single `CANCELLED` from `CreateTask`, which [#1796](https://github.com/flink-gcp/flink-connector-gcp/issues/1796) made transient.

| Package | Digest | Created (UTC) | Deletion-eligible (UTC) |
| --- | --- | --- | --- |
| `operator` | `sha256:2c667a1c14bdb7365f16811dd98a576d6e4993ccbcc369dc53fb75081116ee78` | 2026-10-14T12:17:04Z | 2026-10-21T12:17:04Z |
| `flink` | `sha256:a093c60a9ab038f8821a3bfce4c3236ce37c2ac8e2a94f741f49cb1689baa3cd` | 2026-10-14T12:17:05Z | 2026-10-21T12:17:05Z |
| `lifecycle-tools` | `sha256:6f6d1a08fe2e1489339d73a8af3b086952b1b50ace35a2a4b26e407d3282ede8` | 2026-10-17T10:08:13Z | 2026-10-24T10:08:13Z |
| `smoke` | `sha256:e17c9e02a5c80d8a6873a50af527a2451272a279d5184c25c591b980de43d553` | 2026-10-17T10:08:33Z | 2026-10-24T10:08:33Z |
| `bigquery-recovery` | `sha256:9ba9295a33eb2a187a3bc897190d7bd32214e4012cb4f4d2c0aa314b34fa8635` | 2026-10-17T10:08:42Z | 2026-10-24T10:08:42Z |
| `pubsub-recovery` | `sha256:fe3ae64eda2a5b7ecda0c65f3435bed8572bc7687f438d1984872db603d33f7f` | 2026-10-17T10:08:47Z | 2026-10-24T10:08:47Z |
| `cloudtasks-recovery` | `sha256:60e821b5ce9531255eb039154b24ca59b031a25d16e27a7c742a94741c87a10a` | 2026-10-17T10:08:53Z | 2026-10-24T10:08:53Z |

The digests are the ones the `sha-526a186f…` tag resolves to, and `operator` and `flink` the ones their `1.15.0` and `2.2.1` tags resolve to, read from GAR after the run; the built digests match the run log, and the Operator and Flink digests are unchanged, so the Helm pin and the `flink` pin stand.
The `operator` and `flink` copies kept the creation times of the [BigQuery fault trial publication](#bigquery-fault-trial-publication), because the registry still held those digests.
The same run republished the lifecycle-tools, smoke, BigQuery and Pub/Sub images, which nothing adopts: [pins.cue](pins.cue) kept the BigQuery fault trial publication's lifecycle-tools and smoke pins, because the lifecycle image's inputs are also unchanged between `25260e678` and `526a186f0` and no Cloud Tasks trial pulls `smoke`; the `cloudtasks-recovery` digest stays a dispatch input verified live.
Dispatch refuses an image unless the trial's window ends a day before eligibility, so a trial from this publication had to be admitted so that its 90-minute window ended before 2026-10-20T12:17:04Z.

### Bigtable recovery publication

The [Bigtable recovery publication](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/39202375380) built main commit `ed422edd40e617b04bfe6ca6af8a6eb7e99e6a8d` on 2026-10-19 UTC, the first after the Bigtable scenario merged, for the trials of [#1855](https://github.com/flink-gcp/flink-connector-gcp/issues/1855); its `bigtable-recovery` image is the one every one of those trials names as a dispatch input.

| Package | Digest | Created (UTC) | Deletion-eligible (UTC) |
| --- | --- | --- | --- |
| `lifecycle-tools` | `sha256:4602184875c3fa8c2de7a4a609670dc3dc3b6ee851211039c17e718fd1e020bf` | 2026-10-19T05:18:32Z | 2026-10-26T05:18:32Z |
| `smoke` | `sha256:d3cf553cb06023b97fef07ea128fe0ee603085b265f3154c5819a1da5cb915ae` | 2026-10-19T05:18:54Z | 2026-10-26T05:18:54Z |
| `bigquery-recovery` | `sha256:a7118fab0f19cf599f183a96c285960d7f79e5ea45281b06dcc36b5976053265` | 2026-10-19T05:19:02Z | 2026-10-26T05:19:02Z |
| `pubsub-recovery` | `sha256:6ac404667118a23621fd9f5d490531d71894665b031ef306e6a3127f41f5ee0b` | 2026-10-19T05:19:08Z | 2026-10-26T05:19:08Z |
| `cloudtasks-recovery` | `sha256:bd1e5d2917b3252d7ad49f11b69fb0b7a55e5d3ec516f971026303d40cc15097` | 2026-10-19T05:19:14Z | 2026-10-26T05:19:14Z |
| `bigtable-recovery` | `sha256:f041f5486adbbe505d98ed112a7bf270f506863c5ad1cafefbcfbfac6cadf2bf` | 2026-10-19T05:19:20Z | 2026-10-26T05:19:20Z |

The digests are the ones the `sha-ed422edd…` tag resolves to, read from GAR afterwards, and match the run log.
The `operator` and `flink` copies kept the creation times of the [BigQuery fault trial publication](#bigquery-fault-trial-publication), because the registry still held those digests.
The same run republished the lifecycle-tools, smoke, BigQuery, Pub/Sub and Cloud Tasks images, which nothing adopts, so [pins.cue](pins.cue) kept the BigQuery fault trial publication's lifecycle-tools and smoke pins, and no trial could end after 2026-10-20T12:17:04Z; the `bigtable-recovery` digest stays a dispatch input verified live.

### Bigtable rescale-in rerun publication

The [Bigtable rescale-in rerun publication](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/39449756339) built main commit `5b505786ebcdfd5d7300468e8f1e62edbb816017` on 2026-10-20 UTC for the rescale-in rerun of [#1855](https://github.com/flink-gcp/flink-connector-gcp/issues/1855).
Before it ran, the `operator` and `flink` versions created 2026-10-14T12:17Z were deleted by hand with the owner's approval, as the [BigQuery fault trial publication](#bigquery-fault-trial-publication) did, so both mirrors took fresh creation times.

| Package | Digest | Created (UTC) | Deletion-eligible (UTC) |
| --- | --- | --- | --- |
| `operator` | `sha256:2c667a1c14bdb7365f16811dd98a576d6e4993ccbcc369dc53fb75081116ee78` | 2026-10-20T15:54:54Z | 2026-10-27T15:54:54Z |
| `flink` | `sha256:a093c60a9ab038f8821a3bfce4c3236ce37c2ac8e2a94f741f49cb1689baa3cd` | 2026-10-20T15:54:56Z | 2026-10-27T15:54:56Z |
| `lifecycle-tools` | `sha256:2298529af059233d04adfc17ca7c2ad21f63113737c496a589c3f4c522ef91fc` | 2026-10-20T15:55:16Z | 2026-10-27T15:55:16Z |
| `smoke` | `sha256:774b98d2f0fab8ae960fcb44126437443f82f8448d45dfe602b9c515a3b188dc` | 2026-10-20T15:55:40Z | 2026-10-27T15:55:40Z |
| `bigquery-recovery` | `sha256:84d974f80d9f8d1a993f7e549629a927b600febbb81df79c95a042e540901c61` | 2026-10-20T15:55:50Z | 2026-10-27T15:55:50Z |
| `pubsub-recovery` | `sha256:a50736308c8003345cab86a8378356a56e089c4e417e575ac55ac9e0d0649c34` | 2026-10-20T15:55:56Z | 2026-10-27T15:55:56Z |
| `cloudtasks-recovery` | `sha256:a165ca956fd4dd43b206c3fb6975d08e3a1e5ffed4fb37f432d21521f184c664` | 2026-10-20T15:56:03Z | 2026-10-27T15:56:03Z |
| `bigtable-recovery` | `sha256:ded86d4c19c7ba15e04040e912fd0ec7a508d65425160140137fe56f1e7575dd` | 2026-10-20T15:56:10Z | 2026-10-27T15:56:10Z |

The digests are the ones the `sha-5b505786…` tag resolves to, and `operator` and `flink` the ones their `1.15.0` and `2.2.1` tags resolve to, read from GAR after the run; the built digests match the run log, and the Operator and Flink digests are unchanged, so the Helm pin and the `flink` pin stand.
[pins.cue](pins.cue) adopts the lifecycle-tools and smoke digests and cites this run for the recreated `flink` version; the lifecycle image's inputs are unchanged since `25260e678`.
Nothing adopts this run's recovery application images: the Bigtable rerun keeps `f041f548…`, so all four Bigtable trials run one application image.
Dispatch refuses an image unless the run's window ends a day before eligibility, so every run, which checks the `operator` mirror, the lifecycle-tools pin and its application or smoke image, must end before 2026-10-26T15:54:54Z, and one whose application image is older before that image's own bound: a Bigtable trial, which pulls `f041f548…`, must end before 2026-10-25T05:19:20Z.

For a shell or Docker build argument, read a pin with:

```sh
mise x cue -- cue -C kubernetes export ./images -e flink --out text
```

Pin updates retain zero Operator replicas and zero Pod/PVC quotas and require the existing post-apply idle-release verification and empty refreshed Helm plan.
Every future auxiliary Pod-template image must also be available in GAR before admission.
Publication does not start workload Pods or prove GKE runtime behavior.
