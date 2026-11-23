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

# ADR-0210: A finished job's gRPC buffer pools are released by Netty's finalizers, and a session cluster can opt out of them

- Status: Accepted
- Date: 2026-11-23
- Issues: [#2244](https://github.com/flink-gcp/flink-connector-gcp/issues/2244)
- Modules: bigquery, pubsub, bigtable, spanner, firestore, cloudtasks
- Current behavior: `docs/content/docs/connectors/_index.md` § Memory a finished job leaves on a
  session cluster

## Context

While reproducing [#2237](https://github.com/flink-gcp/flink-connector-gcp/issues/2237) on a
session cluster, the user-code class loaders of finished jobs stayed reachable for minutes, each
with its own copy of grpc-netty-shaded and its own pooled Netty allocator.
One job start failed with a direct-memory `OutOfMemoryError` on a `grpc-default-executor` thread.
The question was what held the class loaders, and whether that was this connector's code.

gRPC's Netty transport gives every channel the allocator that `Utils.getByteBufAllocator` returns
(grpc-netty 1.83.0).
That is a `PooledByteBufAllocator` the transport builds once per class loader, the first time a
channel needs it, unless the relocated `io.netty.allocator.type` property is `unpooled`, in which
case it is Netty's `UnpooledByteBufAllocator`.
A channel option set through `ChannelOption.ALLOCATOR`, which no connector here sets, overrides it.
A job that opens no gRPC channel builds no pool at all.
In the bundled Netty, `PoolThreadCache$FreeOnFinalize` and `PoolArena` both override `finalize()`,
and the heap path below runs through the second.

## Decision

**The connector leaves gRPC's allocator as gRPC chooses it, and the documentation gives a session
cluster that runs many short jobs two measured ways out of the retention.**

- A class loader is not held by anything the connector keeps.
  In the dump below it was held only by objects waiting for finalization, and the class loader
  went after two or three full collections.
  Those counts are observations of when the loaders disappeared, not a sequence the JVM
  guarantees.
- On the TaskManagers, `-Dio.grpc.netty.shaded.io.netty.allocator.type=unpooled` removes the pools
  for every gRPC connector in the job's class loader, without touching Flink's own Netty.
  A `flink-sql-connector-gcp-*` jar relocates gRPC again under
  `io.github.flink.gcp.connector.<product>.shaded`, so its property carries that prefix too
  (checked in the constant pool of the released `flink-sql-connector-gcp-bigquery` 1.4.0).
- Placing the connector in `lib/`, and removing it from the job jars, leaves one copy of the
  transport and its pools for every job, under the same-release requirement the upgrade guidance
  already states.
  A job jar that still bundles the connector, or grpc-netty-shaded for another client, loads its
  own copy first, because Flink resolves classes child-first by default.

## Evidence

Measured 2026-11-22 and 2026-11-23 on a Docker session cluster: Flink 2.2.1 on JDK 17.0.21 with
G1, one JobManager and one TaskManager with two slots and the default memory settings (512 MB of
heap, a 256 MB direct-memory limit, 256 MB of metaspace), under
`cluster.uncaught-exception-handling: LOG`.
The job jar shaded `flink-connector-gcp-bigquery` with grpc-netty-shaded 1.83.0 and read
`bigquery-public-data.samples.shakespeare` through `BigQuerySource` to completion.
Unless a line says otherwise, it was built from the released 1.4.0; the lines marked *main* used
the connector built from `main` at `3dc260082`, which runs on the event-loop group of
[ADR-0208](0208-grpc-channels-run-on-a-connector-owned-event-loop-group.md).
A full collection here is `jcmd GC.class_histogram`, and class unloading was read with
`jstat -class`.
Direct memory was read from Flink's REST metrics, which are refreshed asynchronously, at most once
per `metrics.fetcher.update-interval` (10 s by default); it was read twice, 15 s apart, keeping the
second value, which can still lag a collection, so the direct-memory figures are approximate.
These are single runs of one reading workload, observations rather than bounds.

- **What holds the loader**: one job, then one `GC.class_histogram` and a heap dump with
  `jcmd GC.heap_dump`, whose own full collection makes two.
  A breadth-first search from the dump's GC roots reached the job's `ChildFirstClassLoader`
  neither through strong references nor when soft and weak referents were also followed.
  It reached it only through `Finalizer` referents: a `PoolArena$DirectArena` of the shaded
  allocator, its class, and that class's loader.
- **Collections needed**, pooled: after one job the loader survived the first and second full
  collections and was gone after the third.
  After three jobs, two loaders went at the second and one at the third, both on 1.4.0 and on
  *main* (one run each); in the 1.4.0 run, `jstat -gc` showed no young or concurrent collection
  between the full ones.
  A job jar without the connector, three jobs: all three loaders went at the first full
  collection.
- **Collections needed**, unpooled: after three jobs, the loaders survived the first full
  collection and were gone after the second, on 1.4.0 and on *main* (one run each), and no
  shaded `PoolArena` existed.
  The remaining step was the JDK's HTTPS keep-alive cache, which held an `SSLSocketImpl` whose
  `AccessControlContext` carried the job's protection domain (one job, one `GC.class_histogram`,
  then `GC.heap_dump -all`, which runs no collection).
  The JDK evicts that entry only some time after the connection goes idle, so two full collections
  before then could both find the loader held.
  The three-job unpooled runs were not dumped, so what held their loaders through the first
  collection was not identified.
- **Idle TaskManager**: after three jobs, no collection of any kind ran in the next three minutes,
  and all three loaders stayed.
- **G1 periodic collections**: with `-XX:G1PeriodicGCInterval=20000`, 13 concurrent cycles over
  four and a half minutes after three connector jobs unloaded no class, and 5 cycles after three
  jobs without the connector unloaded none either.
  In an earlier run with the same setting, about 11 cycles after three connector jobs, a heap dump
  taken with `GC.heap_dump -all` reached none of the three loaders from a GC root, `Finalizer`
  referents included, and a single full collection afterwards removed all three: the cycles had
  advanced the finalizer chain but unloaded nothing.
  Why G1's concurrent cycles left the loaders was not determined.
- **Memory held**, pooled, three jobs: the TaskManager's direct memory rose from 136 MB to 152 MB
  on *main* and 153 MB on 1.4.0, and on 1.4.0 it was back to 136 MB after the third full
  collection; metaspace fell from about 100 MB to 49 MB when the loaders went.
  Unpooled, three jobs: direct memory stayed between 136 MB and 138 MB.
  The [#2244](https://github.com/flink-gcp/flink-connector-gcp/issues/2244) report (2026-11-21,
  six writing jobs each on 1.4.0 and on the #2237 prototype) measured about 14 MB of direct memory
  per finished writing job.
- **`lib/`**: the connector and its dependencies in `lib/`, and a job jar holding only the job's
  class, 30 jobs in rounds of three with two full collections after each round.
  The shaded transport's allocators kept 20 direct arenas between them throughout, each job's loader
  held its two classes and went within two full collections, metaspace stayed between 67 MB and 69
  MB, and direct memory rose from 136 MB to 156 MB over the first 15 jobs and stayed at 156 MB over
  the next 15.
- **The out-of-memory path**: `java.nio.Bits.reserveMemory` (JDK 17) first retries while pending
  reference processing runs, then calls `System.gc()` once if the reservation still fails, retries
  with back-off sleeps that total about half a second, and then throws.
  Under G1's default settings that call is one full collection; with
  `-XX:+ExplicitGCInvokesConcurrent` it starts a concurrent cycle, and with
  `-XX:+DisableExplicitGC` it does nothing.
  Without periodic cycles, the measured loaders outlived the first full collection, and the heap
  dump above still found an arena awaiting finalization after two, so one collection and the retries after it need not be
  enough to release a finished job's pools.

## Alternatives declined

- **The unpooled allocator by default**, set through `ChannelOption.ALLOCATOR` in the event-loop
  group's channel configurator.
  It would change every deployment's buffer allocation, application clusters and `lib/`
  deployments included, where the pools never outlive their use, to fix a cost that only a session
  cluster running many short jobs from job jars pays.
  Its throughput cost was not measured, and the operator who pays the retention can already choose
  it with one JVM property.
- **Freeing the pools when a job's class loader is released.**
  Netty's `PooledByteBufAllocator` has no method that frees its arenas, and gRPC keeps the allocator
  in an internal static holder, so a Flink release hook would have nothing to call.
- **Recommending `-XX:G1PeriodicGCInterval`.**
  The periodic cycles it starts unloaded no class in the measurement above.

## Consequences

- With the default allocator, a finished job's class loader, its metaspace and its pooled direct
  memory stayed for up to three full collections in the measured jobs, which on an idle
  TaskManager can be minutes or longer.
- A session cluster that needs the memory back sooner chooses between the unpooled property and a
  `lib/` deployment; the documentation states what each costs.
- Only BigQuery jobs were measured, but the allocator belongs to the transport, so the same chain
  forms for every connector that ships grpc-netty-shaded in the job's class loader and opens a gRPC
  channel.
