---
title: Quickstart
bookCollapseSection: true
weight: 10
---

<!--
Copyright 2026 laughingman7743

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

# Quickstart

Setup once, then one complete job per connector. This page is everything the three that follow
have in common — getting the artifacts, and getting credentials in front of them.

| | |
|---|---|
| [BigQuery]({{< relref "docs/quickstart/bigquery" >}}) | Write a stream of JSON documents into a table |
| [Cloud Pub/Sub]({{< relref "docs/quickstart/pubsub" >}}) | Publish to a topic, consume from a subscription, and the same in SQL |
| [Cloud Tasks]({{< relref "docs/quickstart/cloudtasks" >}}) | Dispatch a stream as HTTP tasks the queue paces |

The connector pages document *what each option does*; these are the shortest path to a job that
runs. Everything here writes to real Google Cloud — to run without touching a project, see
[running against an emulator]({{< relref "docs/examples" >}}).

## Before you start

- **JDK 17 or 21.** Java 11 is not supported even though Flink 2.x declares it; the build targets
  bytecode 17.
- **Apache Flink 2.2 or 2.3**, or 1.20 (the 1.x LTS) — see
  [Supported versions]({{< relref "/" >}}#supported-versions) for how the range is verified. The
  jobs run on the embedded MiniCluster that `StreamExecutionEnvironment` starts from an IDE, so no
  cluster is needed to try them.
- **A Google Cloud project** with the APIs of the services you use enabled. BigQuery tables and
  Pub/Sub topics are created for you by default; a Pub/Sub subscription and a Cloud Tasks queue
  must already exist. Each job below says which of those it is.

## Getting the connector onto the classpath

**Nothing is published yet.** Maven Central publishing arrives with
[#39]({{< param BookRepo >}}/issues/39), so until then the artifacts come from a local build.

```sh
git clone https://github.com/laughingman7743/flink-connector-gcp.git
cd flink-connector-gcp
./mvnw install -DskipTests
```

That installs `0.1.0-SNAPSHOT` into `~/.m2`, from where an ordinary dependency resolves:

```xml
<dependency>
  <groupId>io.github.flink-gcp</groupId>
  <artifactId>flink-connector-gcp-bigquery</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>

<!-- The connectors declare their Flink dependencies as `provided`, so a job brings its own.
     `compile` scope runs the jobs below from an IDE on the embedded MiniCluster; switch both to
     `provided` for a jar submitted to a cluster, which has them already. -->
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-streaming-java</artifactId>
  <version>2.2.1</version>
</dependency>
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-clients</artifactId>
  <version>2.2.1</version>
</dependency>
```

The other connector artifact ids are `flink-connector-gcp-pubsub` and
`flink-connector-gcp-cloudtasks`. All three are SNAPSHOTs of an unreleased project: the coordinates
and the API behind them change without notice, and this section is rewritten when there is
something published to point at. The Flink version above is the floor the connectors are compiled
against; any version in the supported range works, since one build covers the whole of it.

**Building for Flink 1.20** means selecting the compatibility source root along with the version,
which one command does both halves of:

```sh
./mvnw install -DskipTests -Dflink.version=1.20.4 -Dflink.compat=flink1
```

**For SQL**, use `flink-sql-connector-gcp-pubsub` instead — an uber-jar that bundles the connector
with its whole runtime tree, built to be dropped into Flink's `lib/` or added with `ADD JAR`. Why
that rather than the plain jar, and what it relocates, is on the
[Pub/Sub SQL connector]({{< relref "docs/connectors/table/pubsub" >}}) page.

## Credentials

Every connector authenticates with **application default credentials** and nothing else — there is
no credentials option on any builder. Locally:

```sh
gcloud auth application-default login
gcloud config set project my-project
```

On GKE, Dataproc or Compute Engine the workload's own service account is picked up with no
configuration. Service account key files work through `GOOGLE_APPLICATION_CREDENTIALS`, but
workload identity is the better answer wherever it is available.

Two environment facts a first run trips over:

- **`GOOGLE_CLOUD_PROJECT` (or a gcloud config the client library can see) must resolve a default
  project** for BigQuery's `FILE_LOADS` write method, whose clients are built with
  `getDefaultInstance()`. Without it the load-job committer fails with *"A project ID is required
  for this service"* — the project named in the destination is not consulted for this.
- **The BigQuery sink and the Pub/Sub sink need permission to *create* their destination**, since
  that is what they do by default. `createDisposition(CREATE_NEVER)` turns both into
  write-only jobs. The Pub/Sub source creates a subscription only when given creation settings,
  and the Cloud Tasks sink never creates a queue at all.

What each connector asks for:

| Connector | Permissions |
|---|---|
| BigQuery | `bigquery.tables.create` on the dataset under the default create disposition; `bigquery.tables.get` and `bigquery.tables.update` when schema updates or `FILE_LOADS` are enabled, plus BigQuery data-editor and job-user access, and Cloud Storage read/write on the staging bucket for `FILE_LOADS` |
| Pub/Sub sink | `pubsub.topics.publish`, plus `pubsub.topics.create` (roles/pubsub.editor) when topic auto-creation may trigger |
| Pub/Sub source | `pubsub.subscriptions.get` (roles/pubsub.viewer) on every configured subscription for the startup check, plus `create` when auto-creating and `update` when seeking — roles/pubsub.editor covers all three |
| Cloud Tasks | `cloudtasks.tasks.create` ([roles/cloudtasks.enqueuer](https://cloud.google.com/tasks/docs/secure-queue-configuration)), which binds to a single queue as well as to the project. The sink never creates a queue |

## Then

Pick a connector above. Afterwards, [Examples]({{< relref "docs/examples" >}}) covers dynamic
per-record destinations, exactly-once, auto-creation and emulator-backed local runs, and
[Connectors]({{< relref "docs/connectors" >}}) documents what every option does and why. Every type
named on these pages is in the [Java API reference]({{< param ApiDocsURL >}}).
