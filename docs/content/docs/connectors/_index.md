---
title: Connectors
bookCollapseSection: true
weight: 30
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

# Connectors

Google Cloud connectors for Apache Flink.

## Choosing an API

The Table connectors map onto the DataStream connectors with the same names, but the two APIs
describe jobs at different levels.

| API | Choose it when |
|-----|----------------|
| [DataStream API]({{< relref "docs/connectors/datastream" >}}) | A Java job needs connector builders, connector-specific serialization or deserialization schemas, per-record destinations, or a connector feature with no SQL option |
| [Table API and SQL]({{< relref "docs/connectors/table" >}}) | A job is expressed as relational tables and needs DDL, SQL type mappings, or metadata columns |

The Table page for a connector documents its DDL, SQL types, metadata columns, and planner-specific
restrictions.
The corresponding DataStream page documents the underlying runtime behavior and builder-only
features.

Start with [Delivery guarantees]({{< relref "docs/connectors/delivery-guarantees" >}}) when
checkpoint durability, replay behavior, or exactly-once delivery determines which sink method to
use.

See [Lineage]({{< relref "docs/connectors/lineage" >}}) for configured physical-resource metadata,
SQL catalog identities, listener deployment and Flink-version limits.

The connector types named on these pages are documented in the
[Java API reference]({{< api-docs-url >}}).

## What a builder checks

A setter rejects a value when doing so tells you more than the service's own refusal would:

| Rejected when you set it | Which values |
|---|---|
| Missing, or `null` | Every required option |
| Empty, or nothing but whitespace | Every configured name, id or file path. Row-key values are the exception: an empty `prefix` means "scan the whole table" |
| A `/`, a whole value of `.` or `..`, or leading or trailing whitespace | A component the connector concatenates into a resource path: `project`, `dataset`, `table`, `instance`, `database`, `databaseId`, `topic`, `subscription`, `location`, `queue`, `parentProject`, `queryResultDataset`, `tempDataset` |
| Not matching the grammar the connector will read it by | A value it parses itself: an emulator endpoint's `host:port`, a Spanner identifier's quoting, a row-range or row-key literal, a `gs://` staging path, a Cloud Tasks relative URI, an additional field's protobuf name, a Firestore document path |

The `/` rule is about addressing rather than spelling. A component with a `/` in it does not fail —
it silently names a *different* resource, and the service then answers accurately about something
you never typed. A component that is exactly `.` or `..` is path navigation for the same reason,
while a dot inside a name is the service's to judge. A value that genuinely *is* a full path, such as Pub/Sub's `kmsKeyName`, is
exempt for the same reason.

Two more checks exist because of where the service's own answer would land. A Cloud Tasks target URL
must be absolute, checked at the builder for a fixed URL and again per record for one an extractor
produced, so the rejection names the URL rather than the request that carried it. And a reserved App
Engine header such as `Host` is refused at the setter because it is
[owned by Cloud Tasks]({{< relref "docs/connectors/datastream/cloudtasks" >}}) and cannot take
effect, which is worth learning where you set it.

Everything else about a name is the service's answer, including whether the resource exists and
whether the name is one that service accepts. Its rejection names the resource it refused. A copy
of those naming rules kept here would go stale in the direction that hurts, refusing a name the
service would have taken.

## Upgrading the connector

Upgrade the connector by restoring the job's state into a job graph that the new connector release
builds.
Stop the job with a savepoint and resubmit it with `-s`, or, with the Flink Kubernetes Operator,
deploy the application with `upgradeMode: savepoint` or `last-state`.
Each of these builds a new job graph from the new jar.
The connectors' checkpoint state is versioned, so the new release reads the state the old one
wrote, and `upgradeMode: stateless` is never needed for a connector upgrade.
Discarding state is also unsafe for some sinks, such as the
[BigQuery exactly-once sink]({{< relref "docs/connectors/datastream/bigquery#delivery-guarantees-and-state" >}}).

A JobManager that recovers its persisted job graph after the connector under it has changed is not
supported.
That graph was serialized by the old release and is read by the new one.
Examples are `kubectl set image` on the JobManager deployment, recreating a cluster under the same
cluster id with its high-availability ConfigMaps kept, and changing the image of an Operator session
cluster while jobs run on it.
With high availability enabled, the Operator redeploys a session cluster with its
high-availability data kept, so each running job resumes from its old graph.
Stop each job on a session cluster with a savepoint before changing the connector in its image or
`lib/`, then resubmit the jobs.
A JobManager in this state may fail to deserialize the graph.
It may also run the job with a setting the old release did not have read as `false` or `0` rather
than its documented default.
Resubmitting from a savepoint or the latest retained checkpoint recovers the job.

Run the same connector release wherever a job graph is built and run.
Outside application mode, the CLI, SQL Client or SQL Gateway builds the job graph, and the cluster
deserializes it.
When both load the connector from their own `lib/` directories, Flink does not check that the two
copies are the same release.
A mismatch has the effects above, and a cluster older than the client also ignores any setting it
does not have.
Shipping the connector with the job instead of placing it in `lib/` (inside the job jar, with the
SQL Client's `--jar`, or with `ADD JAR`), or running in application mode, leaves one copy, so the
question does not arise.

## gRPC threads and job shutdown

When the connector ships with the job, its classes, the gRPC transport it bundles among them, belong
to the job's user-code class loader.
Flink closes that loader as soon as the job leaves a TaskManager, whether it finished, was
cancelled or lost its JobManager, and on a session cluster's JobManager when the job ends; a
failover inside the job keeps it open.
gRPC's own Netty event-loop group is destroyed one second after its last channel closes, which can
fall after that point.
The destroy then fails with `NoClassDefFoundError`, and under
`cluster.uncaught-exception-handling: FAIL` the process exits: a TaskManager after a BigQuery
writer or reader, and a session cluster's JobManager, taking its other jobs with it, after a
BigQuery source is cancelled.

The BigQuery and Cloud Tasks connectors therefore run their gRPC clients on an event-loop group of
their own, whose threads are named `flink-gcp-grpc-elg-*`.
There is one group for each copy of the connectors' shared classes in a class loader, so each SQL
connector JAR, which relocates its own copy, has its own.
The last reader, writer, committer or enumerator using a group to close shuts it down and waits
for its threads.
On a TaskManager, Flink closes writers, committers and readers before it closes the loader, so the
group is gone first.
On the JobManager, Flink does not wait for a source's enumerator to close before it releases the
loader, so BigQuery's enumerator does not wait for its close either: it closes its client, and
releases its hold on the group, as soon as the read session exists.
A loader released while that session is being created can still see the group torn down after
it closed: the job ends in any way, a stop with a savepoint included, or the JobManager
shuts down, when a table read starts or when a query read's query has finished.
Closing the enumerator then asks for the creation to be cancelled rather than waiting for its
answer.
Any other client a connector closes itself gets 5 seconds for its calls to finish before they
are cancelled.
A Storage Write API default-stream writer is closed by the client library instead, whose own waits
can take minutes when it closes a connection's last writer.
The last close also waits about 10 seconds at most for the group.
If a job's own code uses the Storage Write API's connection pool in the same class loader as the
connector, with the same location and credentials, the two share the pool's connections, and the
connector stops the group when its last BigQuery client closes even if the job's writers are still
using them.

The Pub/Sub, Bigtable, Spanner and Firestore connectors still use gRPC's group
([#2243]({{< param BookRepo >}}/issues/2243)).
Under `FAIL`, placing them in Flink's `lib/` avoids the exit, because the parent class loader never
closes.
