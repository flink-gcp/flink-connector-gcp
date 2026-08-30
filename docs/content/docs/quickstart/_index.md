---
title: Quickstart
bookCollapseSection: true
weight: 10
---

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

# Quickstart

Setup once, then one complete job per connector. This page is everything the five that follow
have in common — getting the artifacts, and getting credentials in front of them.

| | |
|---|---|
| [BigQuery]({{< relref "docs/quickstart/bigquery" >}}) | Write a stream of JSON documents into a table |
| [Cloud Pub/Sub]({{< relref "docs/quickstart/pubsub" >}}) | Publish to a topic, consume from a subscription, and the same in SQL |
| [Cloud Tasks]({{< relref "docs/quickstart/cloudtasks" >}}) | Dispatch a stream as HTTP tasks the queue paces |
| [Bigtable]({{< relref "docs/quickstart/bigtable" >}}) | Write a stream of row mutations into a table, and read a table back |
| [Spanner]({{< relref "docs/quickstart/spanner" >}}) | Write a stream of mutations into a database's tables |

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

The artifacts are on Maven Central under the `io.github.flink-gcp` group, in two version lines
per release: `1.0.0` is compiled against the supported Flink 2.x floor, and `1.0.0-1.20` is the
same code compiled for the Flink 1.20 LTS. An ordinary dependency resolves the 2.x line:

```xml
<dependency>
  <groupId>io.github.flink-gcp</groupId>
  <artifactId>flink-connector-gcp-bigquery</artifactId>
  <version>1.0.0</version>
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

The other connector artifact ids are `flink-connector-gcp-pubsub`,
`flink-connector-gcp-cloudtasks`, `flink-connector-gcp-bigtable` and
`flink-connector-gcp-spanner`. The Flink version above is the floor the connectors are compiled
against, and one artifact covers the whole 2.x range — a job on 2.3 needs no different artifact.
**Flink 1.20 is the exception**: that claim spans 2.x only, so a 1.20 job depends on the `-1.20`
line instead — the same coordinates with `<version>1.0.0-1.20</version>`. Building either line
from source is covered by [Development]({{< relref "docs/development" >}}).

**For SQL**, use the corresponding
`flink-sql-connector-gcp-{bigquery,pubsub,cloudtasks,bigtable,spanner}`
uber-jar instead. Each bundles one connector with its runtime tree and can be dropped into Flink's
`lib/` or added with `ADD JAR`; the jars are on Maven Central and attached to the
[GitHub releases]({{< param BookRepo >}}/releases). The `ADD JAR` examples throughout these docs
name the 2.x jar — a Flink 1.20 job loads the `-1.20` jar instead. The connector-specific Table
API pages document the artifact and installation details. The jars can share one `lib/`, which is why each relocates the linked
third-party Java packages that could conflict; only documented annotation-only packages and
optional native-library carriers remain unrelocated.

## Credentials

Every connector uses **application default credentials** by default. Locally:

```sh
gcloud auth application-default login
gcloud config set project my-project
```

On supported Google Cloud runtimes, ADC needs no connector credential configuration after the
platform identity and its IAM access are configured.
For GKE, configure Workload Identity Federation for GKE; for Dataproc or Compute Engine, attach
the intended service account.
Service account key files work through `GOOGLE_APPLICATION_CREDENTIALS`, but workload identity is
the better answer wherever it is available.
Pub/Sub and Cloud Tasks also accept an explicit service-account key-file path on their DataStream
builders; Pub/Sub exposes the same setting as a Table option.
Their deployment notes cover the process, Kubernetes and rotation requirements for
[Pub/Sub]({{< relref "docs/connectors/datastream/pubsub" >}}#credential-file-deployment) and
[Cloud Tasks]({{< relref "docs/connectors/datastream/cloudtasks" >}}#credential-file-deployment).

Two environment facts a first run trips over:

- **`GOOGLE_CLOUD_PROJECT` (or a gcloud config the client library can see) must resolve a default
  project** for BigQuery's `FILE_LOADS` write method, whose clients are built with
  `getDefaultInstance()`. Without it the load-job committer fails with *"A project ID is required
  for this service"* — the project named in the destination is not consulted for this.
- **The BigQuery sink and the Pub/Sub sink need permission to *create* their destination**, since
  that is what they do by default. `createDisposition(CREATE_NEVER)` drops the requirement on
  both, at the price of a missing destination failing the job instead of being created. The
  Pub/Sub source creates a subscription only when given creation settings, the Bigtable sink
  creates its table only under `CREATE_IF_NEEDED` with a declared schema, and the Cloud Tasks
  and Spanner sinks never create their destinations at all.

What each connector asks for:

| Connector | Permissions |
|---|---|
| BigQuery | `bigquery.tables.create` on the dataset under the default create disposition; `bigquery.tables.get` and `bigquery.tables.update` when schema updates or `FILE_LOADS` are enabled, plus BigQuery data-editor and job-user access, and Cloud Storage read/write on the staging bucket for `FILE_LOADS` |
| Pub/Sub sink | `pubsub.topics.publish`, plus `pubsub.topics.create` (roles/pubsub.editor) when topic auto-creation may trigger |
| Pub/Sub source | The JobManager needs `pubsub.subscriptions.get` for the startup check and `pubsub.subscriptions.consume` for every non-default start position's timestamp seek. Auto-creation on the JobManager additionally needs `pubsub.subscriptions.create` on the containing project and `pubsub.topics.attachSubscription` on the requested topic. TaskManager readers need `pubsub.subscriptions.consume` for pulling and acknowledgement handling. `roles/pubsub.viewer` plus `roles/pubsub.subscriber` cover an existing subscription; `roles/pubsub.editor` covers the full create and consume path |
| Cloud Tasks | `cloudtasks.tasks.create` ([roles/cloudtasks.enqueuer](https://cloud.google.com/tasks/docs/secure-queue-configuration)), which binds to a single queue as well as to the project. The sink never creates a queue |
| Bigtable | `bigtable.tables.mutateRows` ([roles/bigtable.user](https://cloud.google.com/bigtable/docs/access-control)), which binds to a single table as well as to the instance. `createDisposition(CREATE_IF_NEEDED)` additionally needs `bigtable.tables.create` and `bigtable.tables.update`; under the default `CREATE_NEVER` the sink creates neither the table nor its column families. The source needs `bigtable.tables.readRows` and `bigtable.tables.sampleRowKeys` ([roles/bigtable.reader](https://cloud.google.com/bigtable/docs/access-control)) and creates nothing |
| Spanner | `spanner.databases.write` for the mutations, plus `spanner.databases.select` and the read-only-transaction and session permissions the schema read goes through — the sink reads `INFORMATION_SCHEMA` at start-up to weigh mutations against Spanner's per-request limit. [roles/spanner.databaseUser](https://cloud.google.com/spanner/docs/iam) covers all of them |

## Then

Pick a connector above. Afterwards, [Examples]({{< relref "docs/examples" >}}) covers dynamic
per-record destinations, exactly-once, auto-creation and emulator-backed local runs, and
[Connectors]({{< relref "docs/connectors" >}}) documents what every option does and why. Every type
named on these pages is in the [Java API reference]({{< param ApiDocsURL >}}).
