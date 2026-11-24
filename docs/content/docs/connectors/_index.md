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

The BigQuery, Bigtable, Cloud Tasks, Pub/Sub and Spanner connectors therefore run their gRPC clients
on an event-loop group of their own, whose threads are named `flink-gcp-grpc-elg-*`.
There is one group for each copy of the connectors' shared classes in a class loader, so each SQL
connector JAR, which relocates its own copy, has its own.
The last reader, writer, committer, lookup function, enumerator or catalog using a group to close
shuts it down and waits for its threads.
On a TaskManager, Flink closes writers, committers and readers before it closes the loader, so the
group is gone first.
On the JobManager, Flink does not wait for a source's enumerator to close before it releases the
loader, so an enumerator does not wait for its close either: it closes each client, and releases
its hold on the group, as soon as the call it opened the client for returns.
BigQuery's enumerator does that once the read session exists, Pub/Sub's after each check,
creation or seek of a subscription when the source starts, Spanner's batch enumerator once it has
planned the read's partitions, Spanner's Change Streams enumerator once it has read the stream's
configuration, Bigtable's scan enumerator once it has sampled the table's row keys, and Bigtable's
Change Streams enumerator after each check of the application profile, read of the table's
retention and listing of its partitions, including the partition scan it repeats every 10 seconds.
A loader released while such a call is in flight can still see the group torn down after it
closed: the job ends in any way, a stop with a savepoint included, or the JobManager shuts down,
when a BigQuery table read starts, when a BigQuery query read's query has finished, while a
Pub/Sub or Spanner source starts, or while a Bigtable enumerator is waiting for one of the calls
above.
Closing the enumerator then asks for the call to be cancelled rather than waiting for its answer.
A Spanner Change Streams enumerator, or the enumerator of a Spanner Table API scan that reads
through `scan.index` or whose primary-key filter depends on the key's sort order, closed before
its client has created its first session waits for the client's own shutdown instead, which took
about 20 seconds for the Change Streams enumerator against an endpoint that never answered.
A catalog keeps its client until it is closed.
The SQL Gateway and the SQL Client close a session's catalogs before its class loader, so a
catalog's hold on the group is released before that loader closes.
In a program's own `TableEnvironment`, the client stays open until the catalog instance is closed,
whoever closes it: the program itself, or Flink when it drops the catalog or, during an
`ALTER CATALOG`, before it opens the replacement; a catalog left open keeps its client for as long
as the process runs.
Pub/Sub's publishers and subscribers keep the shutdown budgets of their own options and release
their hold on the group when that shutdown finishes or its budget runs out.
A Spanner client waits up to 10 seconds for each of its four stubs in turn as it closes.
Any other client a connector closes itself gets 5 seconds for its calls to finish before they
are cancelled, except Bigtable's.
A Bigtable data client's own close can wait minutes for calls still in flight, up to six for its
session pool and six more for its other channels, and the admin clients' close does not wait at
all.
The connector closes a Bigtable client once the calls it started have ended or been cancelled
where it can cancel them, a full lookup cache load's scan among them; one it cannot, such as a
point read (a lookup's, or one a full lookup cache load makes for a range whose endpoints are equal
and closed) or a mutation an interrupted writer teardown left running, can hold an uninterrupted
close for that long.
A Storage Write API default-stream writer is closed by the client library instead, whose own waits
can take minutes when it closes a connection's last writer.
The last close also waits up to 11 seconds for the group.
When Flink interrupts a BigQuery, Bigtable or Spanner full lookup cache load, which it gives 10
seconds to end, that wait ends after its first second.
If an event loop is still busy then, the group's threads can outlive the job's class loader and
fail as described above.
In a thread dump, `flink-gcp-grpc-elg-*` threads left after every job using the connector has
ended mean a client was not closed, and a `NoClassDefFoundError` from one of them after a job
ended means a close outlasted the class loader.
If a job's own code uses the Storage Write API's connection pool in the same class loader as the
connector, with the same location and credentials, the two share the pool's connections.
So when the job's own class loader defines the connectors' shared classes, as it does by default
for a connector shipped with the job, a BigQuery default-stream writer keeps the group running
until Flink releases that class loader on the TaskManager, rather than until the last client
closes.
The group then stops before the loader closes, and the TaskManager waits for that, up to 11
seconds, instead of for the last close; a group thread still busy after that can outlive the
loader.
Once a client has started the group, its threads stay up while the writer has no destination
open, and the clients of any other of these connectors the job ships unshaded run on it too, so
`flink-gcp-grpc-elg-*` threads are expected for as long as such a job runs on the TaskManager.
When the connector comes from a class loader the job shares, such as Flink's `lib/` directory,
the group is shared by every job using it and stops when its last client closes, even if a job's
own writers are still using the pool's connections.
A writer that cannot tell which class loader defined those classes, because a class loader
refuses the lookup, goes on without keeping the group itself; the group then stops when its last
client closes, unless another writer of the job is keeping it.

Two kinds of channel in the Spanner connector stay on gRPC's group, because the client library
builds them itself.
The built-in Cloud Monitoring export, on by default against the service, is started by the first
Spanner client in the client library's class loader that keeps it on, and runs until the process
exits.
It is absent only when no Spanner client sharing that class loader keeps it on: each sets
`client-metrics.export-to-cloud-monitoring` to `false`, or the process sets
`SPANNER_DISABLE_BUILTIN_METRICS=true`, the client library's default and the only switch a Spanner
catalog has, and no client turns the export back on.
With direct access enabled (`GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS`), the DirectPath control-plane
channels are the other, and the ALTS handshaker runs on an event-loop group of its own.

A Bigtable client's built-in export of its metrics to Cloud Monitoring is the exception within the
Bigtable connector.
The client library builds that export's own client from nothing but an endpoint and credentials,
so its channel stays on gRPC's group, and while the export is on, which it is by default for the
readers', writers' and lookups' clients, the exit can still follow a job that used one.
This was reproduced outside Flink and reported to the client library
([#2297]({{< param BookRepo >}}/issues/2297)).
Turning the export off with `exportClientMetricsToCloudMonitoring(false)` or the
`client-metrics.export-to-cloud-monitoring` Table option removes that channel.
The clients the Bigtable enumerators open on the JobManager never export to Cloud Monitoring,
whatever the option says, for this reason and because a client opened for each call would report
each time under a new client id.
Under DirectPath on Google Cloud, gRPC's own xDS and ALTS control channels are another such
channel, also unmeasured.

The Firestore connector still uses gRPC's group
([#2243]({{< param BookRepo >}}/issues/2243)).
Under `FAIL`, placing it in Flink's `lib/` avoids the exit, because the parent class loader never
closes; the same holds for the Bigtable and Spanner channels above.

## Memory a finished job leaves on a session cluster

A job that ships the connector brings its own copy of the gRPC transport, and once it opens a gRPC
channel, its own pooled Netty buffer allocator.
When the job ends, what those pools still hold is released by Netty's finalizers, which run only
after a collection has found the pools unreachable, and the job's class loader goes only after
that.
In the measured BigQuery jobs, a finished job's class loader went after two or three full
collections run one after another with no other collection between them, where a job without the
connector needed one.

A TaskManager with no work ran no collection at all for three minutes when measured, so these class
loaders can stay for minutes.
G1's concurrent cycles did not unload them either on JDK 17: with `-XX:G1PeriodicGCInterval`
forcing one every 20 seconds on an idle TaskManager, none of them unloaded a class, for a job with
the connector or one without it.
Only full collections did.
On Flink 2.2.1, three finished BigQuery reading jobs raised the TaskManager's direct memory by about
16 MB, which was back at its earlier level when read after the third full collection, and
metaspace fell by about 50 MB as the loaders went ([#2244]({{< param BookRepo >}}/issues/2244)).
When a direct-memory reservation cannot be met even after pending references have been processed,
the JVM requests a collection and retries briefly before it gives up, and pools that need further
collections are not necessarily released in that time.
A session cluster that starts many short jobs can therefore fail a job with
`OutOfMemoryError: Cannot reserve ... bytes of direct buffer memory` while that memory is
waiting to be released.
At ordinary job lifetimes the retention does no harm.

A session cluster that runs many short jobs can avoid it in either of two ways:

- **Unpooled gRPC buffers**: set the gRPC transport's allocator to unpooled on the TaskManagers.

  ```yaml
  env.java.opts.taskmanager: -Dio.grpc.netty.shaded.io.netty.allocator.type=unpooled
  ```

  The property name carries the prefix that grpc-netty-shaded relocates Netty under, so it applies
  to gRPC alone and leaves Flink's own Netty alone.
  A `flink-sql-connector-gcp-*` jar relocates gRPC a second time, under its own prefix, so for a
  SQL jar added with `ADD JAR` the property is
  `io.github.flink.gcp.connector.<product>.shaded.io.grpc.netty.shaded.io.netty.allocator.type`,
  with `<product>` being `bigquery`, `pubsub` and so on, one per jar.
  gRPC then allocates and frees each buffer instead of pooling it.
  In the measurement, direct memory after three jobs stayed within 2 MB of its value before them,
  and the class loaders went within two full collections.
  A session cluster's JobManager also loads the connector for a source's enumerator, and
  `env.java.opts.jobmanager` sets the same property there, although only TaskManagers were
  measured.
  Its effect on throughput has not been measured, and the connector's performance measurements use
  the pooled default.
- **The connector in `lib/`**: with the connector and its dependencies in `lib/` and no longer in
  the job jars, every job shares one copy of the gRPC transport and its pools.
  A job jar that still bundles the connector, or grpc-netty-shaded for another client, loads its
  own copy first, because Flink resolves classes child-first by default.
  On the TaskManager measured, with the same reading job run 30 times from a jar holding only the
  job's class, direct memory grew by about 20 MB over the first 15 jobs and not at all over the
  next 15.
  This deployment needs the same connector release wherever the job graph is built, as
  [Upgrading the connector](#upgrading-the-connector) describes.

A Spanner job also had a second, permanent retention, which the connector now removes.
The Spanner client library keeps two thread pools in static fields, one in grpc-gcp for its
channel pools and one that maintains multiplexed sessions, and their threads did not exit once a
job's clients had closed.
Each of those threads keeps the job's class loader reachable, so a finished Spanner job's class
loader stayed for the life of the TaskManager or JobManager, whatever the collections.
On Flink 2.2.1, with the SQL jar shipped with each job, every change-stream job left about 25 MB of
metaspace in both processes and about 20 MB of direct memory on the TaskManager.
The TaskManager's tasks failed with direct-memory errors from the sixth job on, and the JobManager
stopped with `OutOfMemoryError: Metaspace` at the ninth
([#2296]({{< param BookRepo >}}/issues/2296)).
Before 1.5.0, a `lib/` deployment avoids the growth, because the pools then belong to a class loader
that never closes, although each of their few threads can still keep the class loader of the job
that started it; that follows from the heap path and was not measured.
From 1.5.0 the connector lets those threads exit once they have had nothing to run for a minute,
without shutting the pools down.
In the same measurement, 75 seconds after the last of four jobs no thread of either pool was left,
and the JobManager's four class loaders went at the second full collection, after change-stream
jobs and after bounded scans; so did the TaskManager's after bounded scans.
After change-stream jobs the TaskManager still kept its four class loaders through six full
collections, held in a way the heap dump does not show, so consecutive change-stream jobs on one
TaskManager may still run out of direct memory there; that retention is
[#2317]({{< param BookRepo >}}/issues/2317), and whether either way out above avoids it has not
been measured.
That measurement ran against the emulator, where the client starts no Cloud Monitoring exporter.
Against the service with the built-in export on, which is the default, the exporter's JVM
shutdown hook keeps the class loader of the client library that started it until the process
exits ([#2311]({{< param BookRepo >}}/issues/2311)), unless the export is kept from starting as
[gRPC threads and job shutdown](#grpc-threads-and-job-shutdown) describes.

The reasoning, and the fixes that were weighed and declined, are in
[ADR-0210]({{< param BookRepo >}}/blob/main/docs/adr/0210-a-session-cluster-can-opt-out-of-grpc-buffer-pools-and-spanner-static-executors-let-idle-threads-exit.md).
