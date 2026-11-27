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

# ADR-0210: A session cluster can opt out of gRPC's buffer pools, and Spanner's static executors let their idle threads exit

- Status: Accepted
- Date: 2026-11-23; revised by [#2296](https://github.com/flink-gcp/flink-connector-gcp/issues/2296)
  (2026-11-23)
- Issues: [#2244](https://github.com/flink-gcp/flink-connector-gcp/issues/2244),
  [#2296](https://github.com/flink-gcp/flink-connector-gcp/issues/2296),
  [#2317](https://github.com/flink-gcp/flink-connector-gcp/issues/2317)
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

The Spanner trials of [#2265](https://github.com/flink-gcp/flink-connector-gcp/issues/2265) then
found a retention that collections did not end
([#2296](https://github.com/flink-gcp/flink-connector-gcp/issues/2296)).
Each Spanner job on a session cluster left its class loader on the TaskManager and on the
JobManager until the process failed: the TaskManager's tasks with direct-memory errors from the
sixth job on, and the JobManager with `OutOfMemoryError: Metaspace` at the ninth.
Two executors that the Spanner client library keeps in static fields hold those loaders.
grpc-gcp's `GcpManagedChannel.SHARED_BACKGROUND_SERVICE` (two to four core threads named
`gcp-mc-bg-N`) and the client's `MultiplexedSessionDatabaseClient.MAINTAINER_SERVICE` (one core
thread named `multiplexed-session-maintainer-N`) are created once per class loader, and their core
threads never time out (grpc-gcp 1.20.0, google-cloud-spanner 6.127.0, and the same on upstream
`main`).
grpc-gcp's executor runs every channel pool's periodic tasks, and the maintainer's runs every
database client's session maintenance.
Once a client has used them, their threads outlive every client, so each job's copy of the client
library keeps its own threads alive.
A live thread is a GC root, and each of these reaches its class loader in two ways: the executor
on its stack holds a thread factory whose class that loader defined, and the thread recorded the
protection domains of the code that started it (`Thread.inheritedAccessControlContext`, JDK 17).

## Decision

**The connector leaves gRPC's allocator as gRPC chooses it, and the documentation gives a session
cluster that runs many short jobs two measured ways out of the retention.**

- A BigQuery job's class loader is not held by anything the connector keeps.
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

**The Spanner connector lets the idle threads of the client library's two static executors exit,
so that they do not hold a finished job's class loader
([#2296](https://github.com/flink-gcp/flink-connector-gcp/issues/2296)).**

- `SpannerClients.open`, which every Spanner client goes through, calls
  `SpannerStaticExecutors.allowIdleThreadsToTimeOut()` before it opens the client.
  That sets, on both executors, `allowCoreThreadTimeOut(true)` with a keep-alive of 60 seconds, and
  `setRemoveOnCancelPolicy(true)`, and then purges the queue.
  The maintainer's executor comes from `Executors.newScheduledThreadPool`, whose policy keeps a
  cancelled task in the queue, so without the second setting a closed client's maintenance task,
  scheduled every ten minutes by default, would keep a thread until its next run.
  The policy covers only later cancellations, so the purge removes a task cancelled before it, as
  a client closed in the same class loader before the connector's first one leaves.
  grpc-gcp already removes cancelled tasks.
- Neither executor is shut down, so a client opened later in the same class loader, after a
  failover for example, still schedules on it.
  JDK 17's `ThreadPoolExecutor.getTask` lets the last thread exit only when the queue is empty,
  so an open client's periodic task keeps one thread, and `ScheduledThreadPoolExecutor` starts a
  thread again when new work is scheduled.
- The fields are private and are read by reflection.
  The maintainer's class is package-private, so its name is built from the package of the public
  `SpannerOptions`, which also follows the relocation a `flink-sql-connector-gcp-spanner` jar
  applies.
  A field a client release renames or retypes is logged once as a warning and the client still
  opens, because the cost of missing it is retained memory, not a wrong result.
  `SpannerStaticExecutorsTest` fails instead, on the `libraries-bom` bump that brings such a
  release.
- It applies wherever the connector opens a client: on TaskManagers, in the JobManager's
  enumerators, and where a Spanner catalog runs, in every deployment.
  In a `lib/` deployment the executors belong to a class loader that never closes, but each of
  their threads can still keep the class loader of the job whose thread started it, through the
  context class loader and the protection domains it inherited.
  The setting lets such a thread go once it is idle; while another job's open client keeps a
  task queued, the executor's last thread stays, and with it that job's class loader, which was
  not measured.
- Upstream reports: [googleapis/google-cloud-java#15147](https://github.com/googleapis/google-cloud-java/issues/15147)
  (grpc-gcp) and [#15148](https://github.com/googleapis/google-cloud-java/issues/15148) (the
  maintainer), tracked here by [#2314](https://github.com/flink-gcp/flink-connector-gcp/issues/2314)
  and [#2315](https://github.com/flink-gcp/flink-connector-gcp/issues/2315).

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

The Spanner measurements
([#2296](https://github.com/flink-gcp/flink-connector-gcp/issues/2296), 2026-11-23) used the same
cluster shape, with the Spanner emulator and the `flink-sql-connector-gcp-spanner` jar shipped
with each job through `sql-client.sh -j`.
A change-stream job read a table into a blackhole sink and was cancelled about 8 s after it started
running; a bounded scan of the same table was left to finish.
*main* here is `74aaeb9a`, which already runs on the event-loop group of ADR-0208; *fixed* is the
same tree with this revision.
Each dump was taken with `jcmd GC.heap_dump` from a JDK 17.0.21 mounted into the image, which ships
only a JRE.

- **What holds the loader**, *main*, four change-stream jobs, then one `GC.class_histogram` and a
  heap dump: both processes still held all four job class loaders, and each job had added four
  `gcp-mc-bg` threads and one `multiplexed-session-maintainer` thread.
  A breadth-first search over strong references from the dump's GC roots found every path it could
  follow running through one of those threads: from its stack to the executor, the executor's
  thread factory, that factory's class and its loader, and from its
  `inheritedAccessControlContext` to a protection domain of the loader.
  Removing one pool's threads from the roots left all four loaders reachable in both processes;
  removing both pools' threads left none, and so did removing only one job's own five threads for
  that job's loader.
  In the JobManager, three loaders were also reachable through `Reference.discovered` links
  between weak references, which this search does not count as holding; they are most likely the
  pending list of the dump's own collection, which the JVM keeps outside the heap and the dump does
  not mark.
- **Standalone**, outside Flink, against google-cloud-spanner 6.128.0 and grpc-gcp 1.21.0: three
  child class loaders that each opened and closed one grpc-gcp channel, or one Spanner client
  against the emulator with grpc-gcp off, were all still reachable after five `System.gc()` calls;
  with the executors' core threads allowed to time out, none were (the reproducers filed upstream).
- **Fixed**, four jobs, then 75 s idle and repeated `GC.class_histogram`: no thread of either pool
  was left in either process.
  The JobManager held its four loaders through the first collection and none from the second,
  after change-stream jobs and after bounded scans; the TaskManager did the same after the scans.
  *main* under the same procedure kept every pool thread and all four loaders in both processes
  through three collections.
- **Fixed, the TaskManager after change-stream jobs**: all four loaders stayed through six
  collections 10 s apart, and two after two jobs.
  The dump showed no strong path to them; the only path ran through `Finalizer.unfinalized` to the
  relocated Netty `AdaptivePoolingAllocator`s.
  Those `Finalizer`s' `discovered` fields were null, and the `Finalizer` thread was idle with an
  empty queue, so the collector most likely treated the allocators as reachable through a root
  the dump does not record.
  That root is Flink's, as the next paragraph shows
  ([#2317](https://github.com/flink-gcp/flink-connector-gcp/issues/2317)).

The #2317 measurements (2026-11-25) used the same cluster and jar, built from `main` at
`bd4e3241`, which includes the revision above, with change-stream jobs started about 35 seconds
apart.
The tools were JVMTI agents loaded with `jcmd JVMTI.agent_load`, and HotSpot's
`-Xlog:class+loader+data=trace`.

- **What holds the loader**: flink-table-runtime's `CompileUtils.COMPILED_CLASS_CACHE`, a static
  Guava cache with soft values and a five-minute `expireAfterAccess`, holds the class generated
  for the job's `ChangelogNormalize` (`DeduplicateRowEqualiser$0`).
  flink-table-runtime 1.20.5, 2.3.0 and a 2.5 snapshot build the same cache (read in their
  bytecode, not run).
  Its Janino class loader has the job's `SafetyNetWrapperClassLoader` as its parent.
  When a task resolves a job-jar class through that wrapper, HotSpot records a dependency from
  the wrapper's class-loader data to the `ChildFirstClassLoader`; the log shows it once per job as
  "adding dependency from loader data ... `SafetyNetWrapperClassLoader` ... to loader data ...
  `ChildFirstClassLoader`".
  The wrapper's `close()` clears its own field but not that dependency, which is a handle outside
  the heap, so neither the HPROF dump, walked with soft references followed, nor JVMTI's
  reference walk reports the edge.
- **Clearing the soft references**: an agent that cleared only the cache's two
  `LocalCache$SoftValueReference`s let both job loaders go; an earlier run that first cleared about
  1,900 others (the JDK's reflection-data and serialization caches) had not.
  With `-XX:SoftRefLRUPolicyMSPerMB=0` the loaders went at the third full collection, and with
  the default policy they stayed through four.
- **When it happens**: in `upsert` mode, whose plan here had a `ChangelogNormalize`, and in `full`
  mode followed by `GROUP BY`; not in `full` mode writing straight to the sink, chained or not, nor
  for `datagen` + `GROUP BY` or a `filesystem` Debezium source + `ChangelogNormalize`, whose classes
  come from Flink's `lib/`.
- **Consecutive jobs**, ten of them: tasks failed with `OutOfMemoryError: Direct buffer memory`
  from the sixth job on, at about 20 MB of direct memory and 25 MB of metaspace per job; the
  collection `Bits.reserveMemory` requests applies the soft-reference policy.
  The sixth job's tasks restarted for about four minutes before it was cancelled; the others
  started 35 to 45 seconds apart.
  Metaspace levelled off near 200 MB of its 256 MB as older loaders began to unload from the
  seventh job, about seven minutes after the first, when the first jobs' entries were past the
  cache's five-minute expiry and near the soft-reference age, so which released them was not
  determined; HotSpot's last collection before a metaspace `OutOfMemoryError`, which clears
  every soft reference to a softly reachable object, was not observed separately.
- **What the settings changed**, in runs too short to reach the failure: with the connector in
  `lib/`, three jobs left no loader at the first full collection and metaspace and direct memory
  stayed flat; with the unpooled property, direct memory stayed flat over two jobs and metaspace
  still grew by about 25 MB a job.

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
- **Shutting the Spanner client library's executors down from the release hook that
  [#2268](https://github.com/flink-gcp/flink-connector-gcp/issues/2268) adds on TaskManagers.**
  The JobManager has no such hook: Flink 2.2.1's `SplitEnumeratorContext` exposes neither the
  job's class loader nor a way to register one, and the JobManager is the process the retention
  stopped.
  It would also have made this fix wait for
  [#2268](https://github.com/flink-gcp/flink-connector-gcp/issues/2268).
- **Turning off the grpc-gcp extension** (`SpannerOptions.Builder.disableGrpcGcpExtension()`).
  It removes only the `gcp-mc-bg` executor, and the heap dumps show that the maintainer's thread
  alone keeps every loader; it would also change how the client pools its channels, which no
  measurement here covers.
- **Shutting both executors down when the class loader's last Spanner client closes.**
  The fields are final and a shut-down executor rejects new work, so a client opened afterwards in
  the same class loader, as a failover does, would fail.
- **Only documenting a `lib/` deployment.**
  It avoids the retention, but the connector shipped with the job, which the upgrade guidance
  offers as the way to keep one connector release, and the `ADD JAR` the Spanner SQL page shows,
  would keep stopping the JobManager.

## Consequences

- With the default allocator, a finished job's class loader, its metaspace and its pooled direct
  memory stayed for up to three full collections in the measured BigQuery jobs, which on an idle
  TaskManager can be minutes or longer.
- A session cluster that needs the memory back sooner chooses between the unpooled property and a
  `lib/` deployment; the documentation states what each costs.
- Only BigQuery jobs were measured for this chain, but the allocator belongs to the transport, so
  the same chain forms for every connector that ships grpc-netty-shaded in the job's class loader
  and opens a gRPC channel.
- A finished Spanner job's class loader is no longer held by the client library's executors once
  their threads have been idle for a minute.
  In the measurement it then went at the second full collection on the JobManager, and on the
  TaskManager after bounded scans; on the TaskManager after change-stream jobs it stayed, held by
  Flink's cache of generated classes until the last of the job's entries is dropped by the cache
  or cleared by a collection, which an ordinary HotSpot collection may do once it has gone unread
  for about eight minutes on the measured TaskManager, by a clock that moves only at collections
  ([#2317](https://github.com/flink-gcp/flink-connector-gcp/issues/2317)).
  Consecutive jobs within that time can still run out of direct memory there; the documentation
  states what `lib/`, the unpooled property and a lower `SoftRefLRUPolicyMSPerMB` changed in runs
  too short to reach the failure, and reporting the cache to Flink is tracked by
  [#2355](https://github.com/flink-gcp/flink-connector-gcp/issues/2355).
  These measurements ran against the emulator, where the client starts no Cloud Monitoring
  exporter; against the service with the built-in export on, the default, the exporter's JVM
  shutdown hook keeps the class loader of the client library that started it whatever this
  change does ([#2311](https://github.com/flink-gcp/flink-connector-gcp/issues/2311)).
- While a Spanner client is open, grpc-gcp's executor starts and retires threads instead of keeping
  its two to four core threads.
  A thread exits after a minute without work, and each periodic task's rescheduling starts one
  again whenever fewer than the core size are running (`ScheduledThreadPoolExecutor.reExecutePeriodic`,
  JDK 17), so how often depends on the channel pool's periodic tasks.
  Under the client's defaults grpc-gcp reports to OpenTelemetry and schedules no per-minute
  metrics logging (google-cloud-spanner 6.127.0, `GapicSpannerRpc`).
  Between those starts a task that comes due while every thread is busy waits for one.
  Those tasks are channel-pool maintenance (affinity cleanup, scale-down checks, metrics logging,
  draining); neither the churn nor the delay was measured.
- A periodic task that is running at the moment its client closes can be queued once more after
  the cancellation, because JDK 17's `reExecutePeriodic` re-queues it without checking; the last
  thread then stays until that run comes due (ten minutes for the maintainer by default) and exits
  a keep-alive later.
  The connector cannot close that window from outside the client library, so the thread can stay
  for up to the task's period plus the keep-alive after the close, nearly eleven minutes for the
  maintainer by default.
- The adjustment rests on two private fields, and can be removed once the upstream issues are
  resolved.
- A job that uses the Spanner client library from its own class loader without the connector
  still keeps the executors' threads, and with them its class loader.
