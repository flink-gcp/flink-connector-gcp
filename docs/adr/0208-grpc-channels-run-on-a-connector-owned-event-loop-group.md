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

# ADR-0208: gRPC channels run on a connector-owned event-loop group that stops before the job's class loader closes

- Status: Accepted
- Date: 2026-11-21
- Issues: [#2237](https://github.com/flink-gcp/flink-connector-gcp/issues/2237),
  [#2243](https://github.com/flink-gcp/flink-connector-gcp/issues/2243),
  [#2262](https://github.com/flink-gcp/flink-connector-gcp/issues/2262),
  [#2263](https://github.com/flink-gcp/flink-connector-gcp/issues/2263),
  [#2264](https://github.com/flink-gcp/flink-connector-gcp/issues/2264),
  [#2265](https://github.com/flink-gcp/flink-connector-gcp/issues/2265),
  [#2266](https://github.com/flink-gcp/flink-connector-gcp/issues/2266),
  [#2267](https://github.com/flink-gcp/flink-connector-gcp/issues/2267),
  [#2268](https://github.com/flink-gcp/flink-connector-gcp/issues/2268),
  [#2269](https://github.com/flink-gcp/flink-connector-gcp/issues/2269),
  [#2282](https://github.com/flink-gcp/flink-connector-gcp/issues/2282),
  [#2305](https://github.com/flink-gcp/flink-connector-gcp/issues/2305),
  [#2312](https://github.com/flink-gcp/flink-connector-gcp/issues/2312),
  [#2322](https://github.com/flink-gcp/flink-connector-gcp/issues/2322),
  [#2329](https://github.com/flink-gcp/flink-connector-gcp/issues/2329),
  [#2330](https://github.com/flink-gcp/flink-connector-gcp/issues/2330)
- Modules: base, bigquery, bigtable, cloudtasks, firestore, pubsub, spanner
- Refines: [ADR-0081](0081-emulator-transport-is-shared-by-channel-ownership-not-by-settings-type.md)
- Current behavior: `docs/content/docs/connectors/_index.md` § gRPC threads and job shutdown

## Context

gRPC's Netty transport runs every channel on one shared worker event-loop group (threads named
`grpc-default-worker-ELG-*`).
`SharedResourceHolder` creates the group for the first channel and, one second after the last
channel releases it, shuts it down from its destroyer thread; the group's own threads then run
their teardown.

When the connector ships in the job jar, grpc-netty-shaded is loaded by the job's user-code class
loader, and Flink closes that loader as soon as the job leaves the TaskManager.
A destroy that runs after that point needs classes the loader can no longer load.
In Tier-3 run `bq2150-seq-a1` this surfaced only as log4j failing to log from a
`grpc-default-worker-ELG` thread ([findings](evidence/0204-bigquery-fault-sequence-findings-2150.md)),
because log4j's extended throwable converter loads classes through the same closed loader.

Under the default `cluster.uncaught-exception-handling: LOG` the failure is noise.
Under `FAIL`, Flink's `FatalExitExceptionHandler` exits the process: a TaskManager, or a session
cluster's JobManager when the client was a source's enumerator.

## Decision

**A connector's gRPC channels run on an event-loop group the connector owns, and the thread that
releases the group's last lease shuts it down and waits for its threads.**

- `base.rpc.GrpcEventLoops` holds one group per class loader, the way gRPC does.
  `acquire()` opens a lease and creates the group if it is not running; closing the last lease
  shuts the group down (no quiet period, a 10 s bound) and waits for termination.
  A later lease creates a new group.
- **A group can instead be pinned to the job's class loader** (refined 2026-11-25, #2268).
  `GrpcEventLoops.pinTo(UserCodeClassLoader)` registers a release hook on the job's user-code class
  loader, through `registerReleaseHookIfAbsent`, which a writer's or reader's init context offers
  on a TaskManager (`WriterInitContext` and `SourceReaderContext`, Flink 1.20.5 and 2.2.1).
  While the hook is registered, the release of the last lease leaves the group running, and the
  hook shuts it down and waits for its threads, ignoring an interrupt, before Flink closes the
  loader (`BlobLibraryCacheManager` runs the hooks and then closes the loader).
  The hook then returns the group to the lease rule.
  Flink keeps only the first hook registered under a name, so the hook is registered under
  `GrpcEventLoops`' own class name, which a shaded copy carries relocated: if two copies in one
  job loader each pinned (the connector's and a SQL connector JAR's), neither hook would displace
  the other.
  Only one copy pins today, since the BigQuery sink resolves to one class in a job loader.
  Flink keeps its hooks in an unsynchronized `HashMap` (`ResolvedClassLoader`, 1.20.5 and 2.2.1),
  so a registration racing another registration on a different thread can be lost, and the
  group of a pin whose hook was lost survives the loader; this is Flink's limit, not one the
  connector can see.
  A copy that no pinning writer reaches keeps the lease rule.
  The pin keeps the whole group of that copy running, so in a job that ships several of these
  connectors unshaded, their clients run on the pinned group too, and their own last close no
  longer stops it; their channels still close with their clients.
  Only the BigQuery default-stream writer pins (see the BigQuery section).
- **A lease can still be open when the hook runs** (#2268).
  Flink releases the loader without waiting for the job's tasks when the TaskManager shuts down or
  drops the job (`TaskExecutor.closeJob`, Flink 1.20.5 and 2.2.1), and a client nothing closed
  holds its lease for good.
  The group stops under such a lease anyway, with a warning, since its threads must not outlive
  the loader.
  The hook drops such leases from the count, so a task still opening clients after the hook gets a
  new group that stops with its own last lease.
  The copy does not pin to that loader again, since Flink would never run the new hook; it
  compares the class loader itself, because Flink hands each `getUserCodeClassLoader()` call a new
  wrapper.
  A pin can still be registered after Flink has run the loader's hooks (a TaskManager shutting down
  or dropping the job while a task is still creating its writer): Flink keeps no record that it
  has run them, so that pin is never released and the group's threads stay until the process
  exits.
  Once the loader has closed, Flink's leak-check wrapper throws on the lookup the ownership check
  makes, which is then read as "not the job's" and refuses the pin; whether the JVM answers that
  lookup from its own record without asking the wrapper was not measured.
- **The pin applies only when the job's own loader defined `GrpcEventLoops`** (#2268): Flink's
  loader resolves the class to this copy, that loader's parent does not, and the class's loader is
  neither the system class loader nor one of its ancestors.
  The system-loader clause is for Flink's JobManager, which hands a job with no JARs or class
  paths of its own the system class loader itself, whose parent does not resolve the class.
  The check looks at the parent rather than at the class's own loader because Flink's leak-check
  wrapper defines nothing itself but shares the parent of the loader that does.
  A parent that resolves the class shares it with other jobs: the connector in Flink's `lib/`
  directory or on the application class path (a MiniCluster's or a test's), a copy the job's
  loader resolves parent-first although the job also ships one, and a parent that delegates to a
  loader outside its own chain.
  The class is `GrpcEventLoops` in the base module, not the writer: a job that ships the BigQuery
  sink but takes the base module from `lib/` is not pinned.
  What decides is which loader defined the class, not how the job was submitted: a connector a job
  supplies through `pipeline.classpaths`, or a connector JAR submitted to a MiniCluster, is the
  job's.
  In each excluded case the group is shared with other jobs in the process, so a job's hook must
  not stop it, and the lease rule stays.
  A runtime exception or a linkage error while reading the loaders or looking the class up, other
  than not finding it (a closed leak-check wrapper, a parent copy that cannot load, a security
  manager refusing `getClassLoader`), leaves the answer open, and the writer goes on without
  adding a pin, which is optional: the group then stops with its last lease unless another
  writer's pin holds it.
- That wait, up to 11 s in all, ignores an interrupt, except on a thread inside
  `GrpcEventLoops.interruptibleReleases()` (refined 2026-11-23, #2269).
  There it ignores one for its first second only; after that, an interrupt ends the wait, the
  releasing thread restores it, and the group finishes on its own threads.
  Flink's full lookup cache stops a load by interrupting it and fails its close if the load has
  not ended 10 s later (`CacheLoader.close()`, Flink 1.20.5, 2.2.1 and 2.3.0), so a wait of up to
  11 s on the load's thread could outlast that budget; the BigQuery and Spanner loads open, fetch
  and close inside the scope, and the Bigtable load opens and closes inside it (#2264), which is
  where its client, the only lease it holds, is created and closed.
  The first second keeps the guarantee for a shutdown that no busy event loop holds up, which
  took milliseconds when measured (see Evidence).
- A lease applies the group to a client's settings (`applyTo(ClientSettings.Builder)`) or to a
  transport provider builder, composing a channel configurator after the one the provider
  carries (such as `EmulatorChannels`' plaintext one).
  It refuses settings whose provider does not build its own channels, such as a fixed provider
  around a caller-built channel, because the group cannot reach that channel and leaving it on
  gRPC's group silently is the failure this decision removes.
  A caller that owns its channel opens it with `EmulatorChannels.openLeasedPlaintextChannel`
  instead (refined 2026-11-22, #2243), which builds the channel on the group under a lease of its
  own; the caller closes the clients using it first and the channel's holder last.
  Since 2026-11-23 (#2263) it takes the API's provider builder and copies the settings gax would
  put on a channel built from that provider: the maximum inbound message and metadata sizes and the
  keep-alive settings, without which an emulator channel runs at gRPC's 4 MiB inbound default.
  `EmulatorChannels.openLeasedClient` pairs that channel with a gax client created on it: its
  `LeasedClient` closes the client within the bounds below, then the channel, and a failure to
  create the client closes the channel.
  The lease-less `openPlaintextChannel` and `fixedProvider(ManagedChannel)`, and the overload with
  no provider builder, were removed with their last callers.
  The configurator reads the current group when gax builds a channel and leaves the builder alone
  when the group is not running (a pinned group runs with no lease open), so a configurator that
  outlives its lease never hands a channel a terminated group.
- `open(factory)` opens a lease, creates the client under it, and releases the lease if the
  creation fails.
  The `LeasedClient` it returns shuts the client down on `close()`, waits 5 s for its channels,
  cancels them and waits 5 s more if needed, and releases the lease whatever happened.
  The group therefore stops under a channel that is still closing only when its client did not
  terminate within both bounds, its shutdown threw, or the closing thread was interrupted, and it
  never outlives its leases because a client would not stop.
  A bare lease from `acquire()` is for channels that something else closes and awaits, such as
  the Storage Write API's connection pool.
- The group uses epoll where the shaded native transport loads and NIO otherwise, with Netty's
  default thread count, as gRPC's does.
  Its threads are daemons named `flink-gcp-grpc-elg-*`.

**In the BigQuery connector, every client that owns gRPC channels holds a lease for its lifetime.**

- The default-stream appender on the emulator path, the buffered-stream service, the source's
  session creator (on the JobManager, and on a TaskManager for a full lookup cache load, ADR-0209)
  and its row-stream opener each open their client as a `LeasedClient`.
- **On an emulator, those clients run on a caller-owned channel** (#2267, 2026-11-23), opened
  with `EmulatorChannels.openLeasedClient` from the API's transport defaults, which keep the
  maximum inbound message size at `Integer.MAX_VALUE`.
  They used `EmulatorChannels.plaintextProvider`, and so carried the two failures recorded for
  Cloud Tasks below; measured 2026-11-23, a `dns:///` endpoint failed client creation with
  `IllegalArgumentException: Invalid host or port`.
  The SDK's `StreamWriter` builds its connection's client from the given client's settings, so an
  emulator appender now runs on its client's channel, where gax built one per connection; every
  appender closes before the holder that closes that channel.
  A default-stream appender opens a client, and so a channel, of its own; the appenders one
  buffered-stream service opens all run on that service's channel.
  Their `AppendRows` streams share its one HTTP/2 connection without queueing behind each other:
  goccy/bigquery-emulator 0.8.1's gRPC server advertises no
  `SETTINGS_MAX_CONCURRENT_STREAMS` (measured 2026-11-23; its `SETTINGS` frame carries only
  `MAX_FRAME_SIZE`), which leaves the number of concurrent streams unlimited.
- **The session creator closes its client as soon as each `CreateReadSession` call returns**,
  instead of holding it until the enumerator closes, and its `close()` cancels a call still in
  flight, scheduled retries included, before it closes that call's client.
  At the end of a job, Flink 2.2.1 closes an operator coordinator through `closeAsync` on a thread
  of its own and discards the future, so nothing orders the enumerator's close before the release
  of the job's class loader; with the client held until then, the JobManager exited in 15 of the 16 trials
  below.
  A session is created whenever the enumerator plans, which a restore skips once a checkpoint
  records an initialized session, so a client per call costs one channel setup per plan.
- The cancellation goes through the call's `ApiFuture`, and the creating thread waits on that
  future interruptibly.
  gax schedules a retry on the client's executor, and a `shutdownNow()` of that executor drops it
  without completing the call's future, while the SDK's synchronous call waits on the future
  uninterruptibly (`ApiExceptions.callAndTranslateApiException`, gax 2.90.0); waiting that way
  could strand the coordinator's worker thread.
  A cancellation that lands while gax is starting a retry can miss it, and then the wait ends
  when Flink interrupts the worker as it closes the source.
- `LeasedClient.close()` waits for a close already under way, so the enumerator's close does not
  return while the creating thread is still closing the client, unless Flink interrupts it while
  it waits.
  The pooled default-stream appender holds a bare lease and releases it after
  `StreamWriter.close()`.
- The pooled default-stream path sets a transport provider on `StreamWriter.Builder`.
  Setting one replaces the provider the SDK builds, so `productionChannelProvider` restates the
  SDK's settings (one-minute keepalives that run without calls and two channels per CPU,
  `StreamWriter.getBigQueryWriteSettings` in google-cloud-bigquerystorage 3.38.0).
- **The default-stream writer pins the group to the job's class loader** when it is created
  (#2268), so that in a job whose loader defined the connector, connections the pool built from
  the connector's settings never outlive the group while the loader is open.
- The SDK's `ConnectionWorkerPool` builds its connections from the first writer's settings for the
  life of the class loader.
  A connection it opens after that writer's appender closed still lands on the group, because it
  is opened for a writer whose own appender holds a lease, and the pool closes and awaits a
  connection when its last writer closes, before that writer's appender releases its lease.

**In the Cloud Tasks connector (#2243, 2026-11-22), every client holds a lease the same way.**

- The task creator, which the eager writer and the exactly-once committer each open on a
  TaskManager, opens its production client with `open`.
- **The emulator arm keeps ADR-0081's caller-owned channel**, now opened on the group with its
  client by `EmulatorChannels.openLeasedClient`, which builds the channel as before from the
  endpoint's gRPC target (`EmulatorEndpoint.getTarget()`, resolver scheme intact) under a lease of
  its own, carrying `CloudTasksSettings`' transport defaults since #2263, and closes it after the
  client, on the creation failure path too.
  The channel now closes gracefully within the bounds below, where the creator used to call
  `shutdownNow()` on it after waiting up to 30 s for the client.
  Moving the arm to `EmulatorChannels.plaintextProvider` instead, which a lease reaches like any
  other provider, was tried and declined.
  gax-grpc 2.90.0 (`InstantiatingGrpcChannelProvider.createChannelBuilder`) builds that channel
  with `forAddress` from the endpoint split at its last colon, so a scheme-prefixed target such as
  `dns:///localhost:8123`, which `EmulatorEndpoint` accepts, stops working: measured 2026-11-22,
  creating the client fails with `IllegalArgumentException: Invalid host or port`.
  With `GOOGLE_API_USE_CLIENT_CERTIFICATE=true` and a client certificate available, the same
  method creates the builder with TLS channel credentials, which the plaintext configurator
  cannot override; that one is read from the source, not measured.
- The client's close is `LeasedClient`'s, 5 s and then 5 s more after cancelling its calls, in
  place of the creator's earlier 30 s wait that only logged a warning when it ran out.
- The committer's v2beta3 queue-retention readback opens a client the same way and closes it when
  the call returns.

**In the Pub/Sub connector (#2263, 2026-11-23), every client holds a lease the same way.**

- `Publisher` and `Subscriber` are not gax `BackgroundResource`s, so their hold cannot travel
  with them as a `LeasedClient`.
  `PubSubTransports` opens a `Transport` instead: a channel provider and its hold on the group,
  which the client's owner closes once the client has shut down or been given up on.
  In production the hold is a bare lease applied to a provider that restates the one the SDK's
  builder uses by default, because that default sits in a private field of `Publisher.Builder` and
  `Subscriber.Builder` with no getter, and a provider has to be set whole to carry the lease.
  The subscriber still sizes the pool from its parallel pull count, since the lease's copy of the
  provider accepts a pool size like the SDK's.
- The sink's publisher releases its transport through `BoundedShutdown`'s release slot, so it
  is released on the give-up path too; the dead-letter queue releases it as the entry after its
  publisher's teardown in its `close()` list; the source's subscriber releases it after the
  bounded wait for termination, and after a failed start, where no caller is left to.
- **The emulator arms open a caller-owned channel**, with the API's channel settings: the admins
  through `openLeasedClient`, the publisher and the subscriber through
  `openLeasedPlaintextChannel` inside their `Transport`.
  For the topic admin, the subscription admin and the subscriber, which used
  `EmulatorChannels.plaintextProvider`, that also removes the two failures recorded for Cloud
  Tasks below: a `dns:///` endpoint and a configured client certificate.
  A publisher's or subscriber's `Transport` cancels what is in flight with `shutdownNow()` before
  its holder waits for the channel, as the sink's release always did; the dead-letter queue's used
  to shut its channel down gracefully without waiting. An admin's channel closes after its client
  within `LeasedClient`'s bounds.
  A subscriber on an emulator now multiplexes its parallel pull streams over that one channel,
  where gax built a pool of that size.
- The topic admin, used on a TaskManager when a sink creates a missing topic, still opens a client for each creation call, now as a `LeasedClient`.
- **The subscription admin, on the JobManager, closes its client when each call returns, and its
  `close()` cancels a call still in flight** and closes that call's client, the session creator's
  treatment above: the calls go through `ApiFuture`s that the calling thread awaits
  interruptibly, and a call's client stays findable until its close has finished, so a racing
  `close()` waits for that close rather than returning first.
  Measured 2026-11-23 against an endpoint that accepts the connection and never answers: the
  enumerator's close ended a waiting `GetSubscription` within 4 s, against the 5 s it took when
  only the client's close ended it.

**In the Bigtable connector (#2264, 2026-11-23), every client holds a lease the same way.**

- None of the three Bigtable clients is a gax `BackgroundResource`: `BigtableDataClient`,
  `BigtableTableAdminClient` and `BigtableInstanceAdminClient` are plain `AutoCloseable`s
  (google-cloud-bigtable 2.88.0).
  The admin clients have been gax clients since #2322 (below); the data client still is not.
  The data client's `close()` drains its session pool for up to six minutes, then shuts its classic
  channels down and waits up to six minutes for them to terminate without cancelling the classic
  calls still in flight (gax 2.90.0 `GrpcTransportChannel.close()`); on an interrupted thread, or
  one interrupted during that wait, it gives the wait up at once, consuming the interrupt, and skips
  the rest of its cleanup.
  The admin clients' `close()` only shuts their channels down (`BackgroundResourceAggregation`).
  `LeasedBigtableClient` (removed in #2312, below: base's `LeasedClient` holds the client and
  `BigtableClients` applies the lease) pairs one with its lease: it applies the lease to the
  instantiating
  provider of the client's stub settings, since the Bigtable settings wrap stub settings rather
  than extend gax's `ClientSettings`, and its close closes the client and then releases the lease,
  whose last release waits for the group and so closes any channel still open on it.
- The lease reaches every channel the data client builds from that provider: its channel pool
  (`BigtableClientContext` composes its own configurators after the provider's), its session
  channels (`GaxBasicChannelProvider` and `GaxDirectAccessChannelProvider` apply the provider's
  configurator), and a DirectPath data channel, which gax builds through
  `Grpc.newChannelBuilder` and the shaded Netty transport.
- **The emulator arms keep the SDK's own `newBuilderForEmulator(host, port)`**, which sets an
  instantiating provider with a plaintext configurator of its own; the lease is composed after
  it, so no caller-owned channel is needed.
  Since #2322 only the data client's arm does: the admin clients' settings have no such builder.
- On a TaskManager, the sinks' per-instance clients release their leases as they close on the
  client reaper's threads, which the writer's close waits for; the readers' clients through
  `LazyBigtableDataClient`, the lookup and the full lookup cache hold theirs from open to close,
  the full lookup cache's load inside `interruptibleReleases()` as BigQuery's is, and a load
  stopped part-way cancelling its read stream before it closes the client, since the data client's
  close would otherwise wait for that stream (measured on the emulator with 8 MiB left unread: past
  180 s without the cancellation, at once with it);
  the table admin and the staged sink's metadata check open a client for each call, as before.
- **On the JobManager, the scan enumerator's row-key sampler and the Change Streams coordinator
  open a client for each call and close it when the call returns** (`PerCallClientRunner`, in base
  since #2312): the
  sampler's one `SampleRowKeys`, and the coordinator's profile check, retention read and partition
  listing, including the reconciliation scan every 10 s.
  The calls go through `ApiFuture`s awaited interruptibly, and `close()` cancels the call in
  flight before closing its client, the session creator's treatment above.
  Unlike that creator, the runner builds the client outside its monitor, because building a
  production data client makes calls of its own (each pooled channel's priming and the DirectPath
  check, each bounded at a minute, and the session configuration, 5 s for each of its attempts),
  and the reconciliation
  scan would otherwise hold a teardown up every 10 s; a client that finishes building after
  `close()` is closed by the calling thread unused.
  Every client it closes, from `call()` or `close()`, a late-built one included, is closed with
  the thread's interrupt held
  back and restored afterwards, because the data client's close gives up its wait at once on an
  interrupted thread and then skips the rest of its cleanup; Flink's `shutdownNow()` of the
  coordinator's executor interrupts a worker still building a client after the owner closed.
  An interrupt arriving during that close still cuts it short.
  The catalog holds one leased admin client from its first request until it closes, as the
  Spanner catalog does below (decided by the owner on 2026-11-23, to keep one rule for connector
  catalogs: a per-request client was the first form of this change).
- `LeasedBigtableClient` (base's `LeasedClient` since #2312) restores an interrupt the client's
  close consumed, whether set before the
  close or delivered during it, before it releases the lease, so that a release inside
  `interruptibleReleases()` still sees it: an interrupted full lookup cache load closes its data
  client first.
- **The JobManager-side data clients never export to Cloud Monitoring**, whatever the source's
  `exportClientMetricsToCloudMonitoring` says; the choice governs the readers' clients only.
  Decided by the owner on 2026-11-23.
  The built-in exporter builds its own `MetricServiceClient` from an endpoint and credentials
  alone (`BigtableCloudMonitoringExporter.create`), so its channel cannot carry a lease and runs
  on gRPC's group, which on the JobManager would bring back the failure this group exists to
  prevent.
  A client per call would also report under a fresh `client_uid` each time (a random UUID per
  client, `EnvInfo.computeUid`), so the reconciliation scan alone would start a new series every
  10 s, each close would flush an export, and a principal without permission to write to Cloud
  Monitoring would log the exporter's warning once per client, every 10 s, instead of once.
  Off Google Cloud, each close would also wait for the exporter's platform detection to time out
  connecting to the metadata server's address, which measured 75 s on #1815 and #1902.

**In the Spanner connector (#2265, 2026-11-23), every client holds a lease the same way.**

- `Spanner` is not a gax `BackgroundResource`, and `SpannerOptions` takes a channel configurator
  rather than a transport provider.
  `GrpcEventLoops.channelConfigurator(existing)` returns the composed configurator without a
  lease, because the settings are built before the client is opened; it reaches the group only
  while a lease is open.
  `SpannerClients.open` opens the lease before the client builds its channels and returns a
  `LeasedSpanner` (base's `LeasedClient` since #2312, below), whose `close()` closes the client and
  then releases the lease, and which a second caller waits on, since the client refuses a second
  close.
  The client waits up to 10 s for each of its four stubs and its watchdog in turn as it closes
  (google-cloud-spanner 6.127.0), so it takes that bound rather than `LeasedClient`'s.
- **The settings come from a `SpannerOptions.Builder` subclass that composes the group into every
  configurator set on it.**
  `build()` replaces the configurator with a bare plaintext one whenever an emulator host is set,
  the `SPANNER_EMULATOR_HOST` environment variable's included, and a `toBuilder()` copy keeps the
  emulator host, so a configurator set once is lost on every emulator build.
  `build()` sets its replacement through the overridable setter, so the subclass composes into
  it and the client stays in its emulator mode, which keeps recording no client metrics.
  `SpannerClientsTest` holds that against the client version in use.
  A component that sets more on the settings takes the builder from `SpannerClients`; `open`
  refuses settings whose configurator is not the composed one, which is what a rebuilt
  `toBuilder()` copy carries, and settings with a channel provider, which the client uses in
  place of the configurator.
  With direct access enabled (`GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS`), the DirectPath
  control-plane channels that gRPC's xDS and ALTS support build themselves are not reached by the
  configurator, and the ALTS handshaker runs on an event-loop group of its own; read from the
  libraries, not measured.
  Nor is the built-in Cloud Monitoring export, on by default with real credentials: its exporter
  builds a `MetricServiceClient` from settings of its own (`SpannerCloudMonitoringExporter`,
  6.127.0), held by a provider that is process-wide per class loader and shut down only by a JVM
  shutdown hook, so its channels run on gRPC's group for as long as the process does.
  The first client in the client library's class loader that keeps the export on starts it (with
  a `lib/` deployment that loader is shared by every job), and
  `client-metrics.export-to-cloud-monitoring = false` (or the builders' counterpart) keeps only
  that client out of it; the exporter is absent only if every Spanner client in that class loader
  turns the export off; a catalog can only stay out of it through `SPANNER_DISABLE_BUILTIN_METRICS`,
  which sets the client library's default and does not stop a client that turns the export on.
  A standalone reproducer (a client in a child class loader that is then closed, all endpoints on
  loopback) measured the consequence: with built-in metrics on, the exporter's JVM shutdown hook
  failed with `NoClassDefFoundError` in the closed loader in 7 of 7 runs, and in none of 7 with
  them off; reported upstream as
  [googleapis/google-cloud-java#15145](https://github.com/googleapis/google-cloud-java/issues/15145).
  Leaving the emulator mode to set the configurator after `build()`, and keeping the emulator arm
  on gRPC's group, were the alternatives; the first turns the client's metrics on against an
  emulator and drops the client's check that one is listening, the second leaves the arm every
  test reaches untested.
- **The batch planner and the change-stream coordinator, on the JobManager, close their client
  when their call returns.**
  The planner opens a client for each `plan` and closes it, with the batch transaction's handle,
  before `plan` returns; the readers rejoin the snapshot by its id on clients of their own.
  It no longer calls `cleanup()`, which can only run on an open client and would end every
  reader's read if it released the session.
  In 6.127.0 the batch client serves every batch transaction from a multiplexed session, which
  the client never deletes and which expires on its own, so the call cost nothing;
  were a later client to stop multiplexing, a session left behind would idle out on the service.
  The coordinator's enumerator closes its client once initialization, the start position
  included, has been resolved (at once if it failed), and its own `close()` waits for that
  release unless the closing thread is interrupted: a `latest` start resolved after the close
  would skip what was committed meanwhile.
  A `close()` that lands while the client is still being created waits for the creation, and
  the client created then is closed before that `close()` returns, outside the enumerator's
  monitor, so a further `close()` waits for it interruptibly too (refined 2026-11-25, #2305).
  The creation is not cancelled: against the service it waits for no Spanner call (it starts
  the database client's session creation in the background), and against an emulator the one
  call it waits for, the client library's check that an emulator is listening, has a 10 s
  timeout of its own, while a check that fails, as a cancelled one would, leaves the stubs the
  library built before it open (#2306).
  That cheap creation is why the coordinator and the planner wait where `PerCallClientRunner`,
  above, does not: building a Bigtable data client waits for calls bounded at a minute each
  ([#2347](https://github.com/flink-gcp/flink-connector-gcp/issues/2347) revisits that choice).
  A second refinement the same day (#2305) covered three more paths.
  A `close()` that takes the installed client over records its own close of it, so a further
  `close()` waits for that close, interruptibly, rather than returning while it runs.
  A client close that fails after the enumerator closed, whether of a client created late or of
  one initialization was closing, is logged, since the closed enumerator drops initialization's
  result; while the enumerator is open, such a failure still fails initialization.
  An `Error`, such as the `NoClassDefFoundError` a released class loader causes, is logged too,
  and the planner logs a failure to close a client built after its `close()` the same way, since
  the closed batch enumerator drops the plan's failure too.
  The planner builds its client in the root context rather than its call's cancellable one, so
  its `close()` no longer cancels the emulator check, which would leave the stubs open; it waits
  for the build, as it already waited for the call, and the client built then is closed before
  that `close()` returns unless the closing thread is interrupted.
  The end of a bounded read is never published while the client is open: initialization closes
  the client before it returns, and if the enumerator's `close()` took the client over first, the
  closed enumerator publishes nothing.
  The close's duration, normally milliseconds, then comes out of an `earliest` start's one-minute
  retention margin, like the readers' own start-up time: a first read that falls outside
  retention fails, and a region failover replays the same position, as for any expired position.
  It also lengthens the window, already open during the configuration reads, in which a
  checkpoint finds the enumerator not yet initialized and fails.
  Closing it on the worker after initialization returns instead was tried and declined: a bounded
  restore with nothing left to read can then signal the end before the close, letting the job
  finish, and Flink release its class loader, while the client is still closing, and a failed
  close would only be logged.
- **A JobManager-side `close()` cancels a call in flight through the gRPC `Context` it runs in.**
  Spanner's synchronous calls expose no `ApiFuture`; gRPC binds each call to the context it is
  started in and cancels it when that context is cancelled, and the client also checks the
  context between a streaming read's retry attempts (`ResumableStreamIterator`).
  The planner and the coordinator's client run their calls in a cancellable context (the planner
  builds its client outside it, #2305), and `close()` cancels it before it closes the client,
  since the client's close waits for its channels; the planner's `close()` then waits for the
  call to end, with no bound of its own beyond the client's shutdown and an interrupt, so its
  lease is released when it returns whichever thread closed the client.
  One wait is out of the context's reach: a read through the database client waits for the
  multiplexed session that client creates in the background when it is built.
  The coordinator's first read is one, and so is the planner's resolution of a deferred read operation (a Table API scan or FULL cache load that reads through `scan.index`, or whose primary-key filter depends on the key's sort order); the planner
  builds no database client for any other operation, which includes every DataStream API read.
  A close while that creation is in flight waits for the client's bounded shutdown; measured
  against an endpoint that never answers, the read failed about 20 s after the close, and its
  thread was not stranded.
- **The catalog keeps one client for as long as it is open**, as `AbstractReadOnlyCatalog` opens
  it, rather than one per metadata call.
  The SQL Gateway, and the SQL Client through it, close a session's catalogs synchronously before
  its class loader: `SessionContext.close()` closes each catalog and only then the session's
  class loader (Flink 2.2.1 through the catalog manager, 1.20.5 catalog by catalog; read from the
  source), so the catalog's lease is released before that loader closes; whether the group then
  stops depends on any other lease open in the same class loader, as for every release.
  In a program's own `TableEnvironment`, the client and lease stay until the catalog instance is
  closed, whoever closes it: the program itself, or Flink when it drops the catalog
  (`CatalogManager.unregisterCatalog`) or, during an `ALTER CATALOG`, before it opens the
  replacement (`alterCatalog`; both versions); a catalog left open keeps them for as long as the
  process runs, as its client did before #2265.
  A client per call would set up channels and a session for every catalog request.
- The readers, the change-stream query clients, the sink's writer and the lookup function hold
  their client's lease from open to close on the TaskManager, as before #2265 they held the client.

**The holder and the per-call runner moved to base (#2312, 2026-11-23).**

- **`GrpcEventLoops.LeasedClient` holds a client that is not a gax `BackgroundResource` too.**
  `open` still bounds a gax client's close as above; `openAutoCloseable` calls the client's own
  `close()` once, restores an interrupt that close consumed, and then releases the lease.
  Bigtable's `LeasedBigtableClient` and Spanner's `LeasedSpanner` were that second form in each
  module and are removed; Bigtable opens its clients through `BigtableClients`, and Spanner through
  `SpannerClients.open` as before.
  A holder that also owns an emulator channel now closes the channel before it releases the
  client's lease, rather than after; both leases are released whatever the closes did.
- **`Lease.applyTo(TransportChannelProvider)` is public**, for settings that take a transport
  provider without extending gax's `ClientSettings`, such as Bigtable's stub settings, which
  carried a copy of it.
- **`PerCallClientRunner` is in `base.rpc`**, and BigQuery's session creator and Pub/Sub's
  subscription admin use it alongside Bigtable's sampler and coordinator.
  For the two that move, two things change.
  The client is built outside the owner's monitor, so `close()` no longer waits for a client
  being built, and one built after it is closed unused by the calling thread.
  A call that `close()` cancels fails with an `IOException` that says so; BigQuery's creator threw
  the future's raw `CancellationException`, which its method does not declare (#2282).
  A client is published to the runner before its call starts, so an owner close that arrives while
  the calling thread closes a client whose call could not start waits for that close.
- **The runner holds a thread's interrupt back only while a client opened with
  `openAutoCloseable` closes**, and restores it before the lease is released: such a client's own
  `close()` can stop at its first interrupted wait and skip the rest of its cleanup, as a Bigtable
  data client's does.
  A gax client's bounded shutdown answers an interrupt with `shutdownNow()` and skips nothing,
  because its `shutdown()` reaches every resource it aggregates before anything waits
  (`BackgroundResourceAggregation`, gax 2.90.0 and 2.91.0); that corrects #2282's account of the
  BigQuery path, where restoring the interrupt before the close only shortened the termination
  wait.
  So the interrupt that ends a call's wait is set again before the client closes: BigQuery's and
  Pub/Sub's clients answer it at once, as they did, and a Bigtable client has it held back.
  Either way it is back before the lease is released, which a full lookup cache load relies on:
  Flink gives the load 10 s once it interrupts it, and a release inside `interruptibleReleases()`
  stops waiting for a busy group after its first second only on an interrupted thread.
  Bigtable's runner had restored it only after the lease was released.
- **Only the thread that closes a client holds its interrupt back.**
  A thread that finds another closing the same client waits for that close, and an interrupt ends
  the wait, whether it was set before the wait began or arrives during it (decided by the owner on
  2026-11-23).
  Bigtable's runner had held back an interrupt set before the wait as well, because it cleared the
  interrupt around the whole holder close.
  The two differ only on an owner's close that arrives interrupted, and Flink 2.2.1 and 1.20.5
  close a source's enumerator before they shut the coordinator's executor down
  (`SourceCoordinator.close()`), so the coordinator's worker is interrupted only after the owner's
  close has returned; the closing thread arrives interrupted only after `closeAsyncWithTimeout`
  aborted it at its 60 s timeout, when Flink has stopped waiting for it.
- **A client that fails to close with an exception after its call succeeded is logged, not
  reported as the call's failure**, the subscription admin's rule since #2263: the call's result
  does not depend on the close, and the lease is released either way.
  An `Error` from that close is thrown, as it is everywhere else.
  A failed call carries the close failure as a suppressed exception, as Bigtable's runner and
  BigQuery's creator did; they had thrown it in place of a result.
  The subscription admin had logged every close failure, and now one travels unlogged on a
  failure it answers instead of throwing (`NotFoundException` from `describe`, `AlreadyExists`
  from a creation, which reads the subscription back on a second client); a close failure in its
  own `close()` is still logged.
- **Spanner's batch planner stays off the runner.**
  Its calls are synchronous and expose no `ApiFuture`; it cancels them through the gRPC `Context`
  they run in, and a deferred operation's multiplexed session is created outside that context.

**The Bigtable admin clients are opened as gax clients (#2322, 2026-11-25).**

- **The admin clients are `BigtableTableAdminClientV2` and `BigtableInstanceAdminClientV2`**,
  opened from `BaseBigtableTableAdminSettings` and `BaseBigtableInstanceAdminSettings`, because
  google-cloud-bigtable 2.89.0 marks the wrapper clients, their factories and their settings
  `@ObsoleteApi` (ADR-0041).
  The V2 clients extend the generated `BaseBigtable*AdminClient`s, which are gax
  `BackgroundResource`s, so `BigtableClients` opens them with `open` rather than
  `openAutoCloseable`.
  Their close is therefore the bounded one above: the holder shuts the client down, waits for it
  to terminate, cancels its calls if it has not, and then releases the lease.
  The wrappers' close only shut their channels down and returned.
  The table admin client also owns an executor for its consistency and restore waits, which the
  module does not call; its shutdown and termination are part of the same close.
  Opening the generated `BaseBigtableTableAdminClient` instead would avoid that executor, and was
  declined: its factories and constructors are protected, so the connector would need a subclass
  of its own, the obsolete wrappers name the V2 clients as their replacement, and the executor
  starts no thread until something is scheduled on it.
- **Their emulator arms open a caller-owned channel with `EmulatorChannels.openLeasedClient`**,
  from the API's own transport defaults, as the Pub/Sub, BigQuery and Cloud Tasks clients do.
  The base settings have no `newBuilderForEmulator`, and an instantiating provider would carry the
  two failures recorded for Cloud Tasks above.
  The defaults keep the maximum inbound message size at `Integer.MAX_VALUE`, where the wrappers'
  emulator builder used gRPC's 4 MiB.
  The data client keeps the SDK's emulator builder.
- **A process with `BIGTABLE_EMULATOR_HOST` set to a non-empty value and no emulator endpoint
  configured opens no Bigtable client.**
  The data client's settings read the variable and switch to the emulator, with no credentials;
  the base admin settings do not read it.
  Before this change, a value the library could parse made every component fail while loading
  credentials, with a message about the client's credential scopes, because the loader read the
  scopes from a settings builder that the variable had switched to no credentials; a value it
  could not parse, such as `localhost`, failed in the library's own host and port parser.
  The loader now reads them from settings that never read the variable, and the data and admin
  settings refuse it by name before the library reads it, so the two families cannot point at
  different hosts.
  The refusal comes when a component opens a client.
  A configured emulator endpoint takes precedence and the variable is not consulted then.
  The Pub/Sub source ignores `PUBSUB_EMULATOR_HOST` for the same reason, that a stray value would
  silently redirect a production job; the Bigtable connector cannot ignore its variable, because
  the data client's settings read it.

**In the Firestore connector's Datastore mode (#2329, 2026-11-25), every client holds a lease the
same way.**

- **Every Datastore client is the generated `v1.DatastoreClient`**, a gax `BackgroundResource`
  that `DatastoreClients.open` opens with `open`, or on an emulator with `openLeasedClient` from
  `DatastoreSettings`' transport defaults, which keep the maximum inbound message size at
  `Integer.MAX_VALUE`: the sink's database access, the source's page reader and planner, and the
  table lookup, which already used it.
  The library's `Datastore`, which the others used, cannot carry a lease on an emulator:
  `GrpcDatastoreRpc` (google-cloud-datastore 3.11.0) builds a plaintext channel of its own through
  `ManagedChannelBuilder.forTarget`, and ignores the transport provider it is given, whenever
  `DatastoreUtils.isEmulator` holds, which a loopback host or `NoCredentials` makes true, and the
  connector's emulator arm set `NoCredentials`.
  Its host check also takes `dns` for the host of a `dns:///` target and fails to resolve it.
  Its close shuts its channel down and waits up to six minutes for it to terminate
  (`GrpcTransportChannel.close()`, gax 2.91.0) without cancelling the calls it carries, and it has
  no `shutdownNow()`.
  The lookup's emulator arm used `EmulatorChannels.plaintextProvider`, and so carried the two
  failures recorded for Cloud Tasks above; it was that method's last caller, and the method was
  removed with it.
  Moving to the library's `Datastore` with a channel provider of the connector's own was declined:
  on the emulator it would have needed credentials other than `NoCredentials` and a host that is
  not a loopback address, so that `isEmulator`, an `@InternalApi` check, would let the provider
  through, and an SDK change to that check would put the channel back on gRPC's group without a
  failure.
  Injecting a `DatastoreRpc` of the connector's own through `setServiceRpcFactory` was declined
  too: the interface is `@InternalExtensionOnly`, and the connector used `Datastore` itself only
  for `allocateId`.
- What moved with the client:
  - The sink's single attempt per call, which `GrpcDatastoreRpc` applied to every unary method,
    is now set on each unary method of the generated settings, and the source keeps the retry
    settings `GrpcDatastoreRpc` applied by default (`ServiceOptions.getDefaultRetrySettings()`,
    which `DatastoreClientsTest` compares with the library's); the lookup keeps the generated
    client's own.
  - The production channel pool is gax's default single channel, where `DatastoreOptions` started
    five and resized between one and 200 with the calls in flight.
    The sink sends one commit at a time per subtask, a reader one page and the planner one query,
    so a channel per client carries what each one sends.
  - An id allocation fails with the gax `ApiException` rather than the library's
    `DatastoreException` around it; the sink's classifier reads the status of either.
    The allocated keys are decoded through `Entity.fromPb`, since `Key.fromPb` is package-private.
  - A configured service-account key still signs its own scoped JWT, as `Datastore` sent it
    (`ServiceOptions.getScopedCredentials()` with its default `useJwtAccessWithScope`,
    google-cloud-core 2.81.0), rather than exchanging an assertion at the OAuth token endpoint; the
    lookup, which took the key as loaded, now does the same.
    The planner's HTTP splitter still takes the key as loaded and, as before, exchanges an
    assertion it signs locally for an access token at the token endpoint.
    These statements describe a key in the default universe domain; a key for another universe
    domain was not examined.
  - An emulator endpoint written with `http://` or `https://`, which `Datastore` removed before it
    dialled (`DatastoreUtils.removeScheme`), is still accepted: the scheme is removed before the
    caller-owned channel is built, since gRPC would read it as a name-resolver scheme it does not
    have. A `dns:///` target is kept for gRPC to resolve.
    This covers the gRPC clients only: the planner's HTTP splitter refuses such an endpoint, as it
    did before (`DatastoreOptions.Builder.localHost`, datastore-v1-proto-client 3.11.0; read, not
    measured), so a plan that cuts key ranges fails with it (tracked in
    [#2338](https://github.com/flink-gcp/flink-connector-gcp/issues/2338)).
  - The `google-cloud-resource-prefix` header and the client-library token that `GrpcDatastoreRpc`
    added are no longer sent; the generated stub routes each request by its `project_id` and
    `database_id`, which every request the connector builds sets.
- **The planner, on the JobManager, opens a client for each plan and closes it before the plan
  returns**, through `PerCallClientRunner`'s session form (below): the GQL parse, the read-time
  probe and the statistics queries are the session's calls, and `close()` cancels the one in
  flight through its future.
  It held its client from its first plan until the enumerator closed it.
  The splitter's calls run after the session, over the HTTP client ADR-0177 describes, which opens
  no gRPC channel and which the planner's `close()` still closes.
  That client takes an emulator's endpoint as a host, so a plan that cuts key ranges still fails
  against an emulator given as `dns:///host:port` (measured 2026-11-25; tracked in
  [#2338](https://github.com/flink-gcp/flink-connector-gcp/issues/2338)); a plan of one range, or
  of a query that cannot be split, never calls it.
- The page reader holds its client from its first page until the reader closes, on the
  TaskManager, as before, and **its close ends every page still in flight before it closes the
  client**.
  A fetcher can outlive the reader's close timeout, and the client's bounded close ends with
  `shutdownNow()`, which drops a retry gax has scheduled on the client's executor without
  completing the call's future; a fetcher waiting on that future, between attempts of a page that
  met `UNAVAILABLE`, would then wait for good.
  The library's `Datastore` never called `shutdownNow()`: it waited for its channel and then shut
  its executor down, which still runs a retry already scheduled, so the retry failed on the closed
  channel and released the fetcher (read from gax 2.91.0, not measured).
  Flink's split fetcher manager does not interrupt a fetcher it closes (`SplitFetcherManager.close`,
  Flink 1.20.5 and 2.2.1, shuts its executor down and waits), so only this cancellation ends such a
  wait; a page also waits interruptibly, where the library's blocking call ignored an interrupt.
  The page waits on a future derived from the call's (`ApiFutures.transform`), which the close
  cancels, completing it at once and passing the cancellation on to the call, because gax can miss
  a cancellation that lands between two of a call's attempts and then schedule the next one
  (`CallbackChainRetryingFuture.cancel` cancels only the current attempt, gax 2.91.0), which the
  client's `shutdownNow()` then drops.
  A full lookup cache load, of either mode, plans, opens, fetches and closes inside
  `interruptibleReleases()` (`FullCacheInputFormat`, which both modes extend).

**`PerCallClientRunner` runs a session of several calls on one client (#2329).**

- `session(prepare, opener, body)` opens one client and closes it once the body returns or
  throws; the body starts each call through `ClientSession.call(start)`, which starts it under the
  runner's monitor and waits for it, so the call in flight is always the one `close()` cancels.
  A failure the body catches, such as Datastore's refusal of a GQL query with `LIMIT 0` appended,
  leaves the session open for its next call.
  A call that a session starts after `close()` is refused without being started, and one started
  while another of the session's calls is in flight is refused with an `IllegalStateException`.
  `call(prepare, opener, start)` is a session of one call.
  `ClientSession.client()` hands the body the client for work that makes no call, such as
  building a Firestore query, which is bound to the client it is built from (#2330); every call
  still goes through `call`, the only one `close()` cancels.
- A client per call was the alternative: it would set up a channel for each of a plan's three to
  five calls, and a Firestore query is bound to the client it was built from (#2330).
  Composing the plan into one future was the other: it loses each step's failure message, and
  Datastore's fallback on `INVALID_ARGUMENT` would become a chain of futures.

**In the Firestore connector's Native mode (#2330, 2026-11-25), every client holds a lease the
same way.**

- **`FirestoreClients.open` builds the options twice.**
  `FirestoreOptions.Builder.setChannelProvider` accepts only an `InstantiatingGrpcChannelProvider`
  and `FirestoreOptions` is `final` (google-cloud-firestore 3.53.0), so a caller-owned channel
  can reach the client only through a hand-written `FirestoreRpc` (`setServiceRpcFactory`), which
  [#2343](https://github.com/flink-gcp/flink-connector-gcp/issues/2343) weighs.
  The first build sets up the provider the library would use, with its tracing configurator; the
  second takes that provider with the lease applied after its configurator.
  An emulator's arm sets a plaintext provider from `FirestoreSettings`' transport defaults, which
  keep the maximum inbound message size at `Integer.MAX_VALUE`, with the emulator's fixed token,
  in place of `setEmulatorHost`, whose provider the library builds itself at gRPC's 4 MiB and off
  the group.
  `GrpcFirestoreRpc` takes the provider for any client whose service host is not a `localhost`
  name and whose credentials are not `NoCredentials`; the connector sets neither.
- **A client closes with `shutdownNow()` before `close()`** (`FirestoreClients.close`, through
  `GrpcEventLoops.openAutoCloseable(factory, closer)`).
  Cancelling the future of `Query.get()` or of a read-only transaction ends the caller's wait but
  not the `RunQuery` stream, since the library discards the stream's controller, and
  `Firestore.close()` alone then waits for the stream until its deadline: a planner closed while a
  plan waited on a server that never answered took 300 s to close; with `shutdownNow()` first it
  took 25 ms, and the group stopped (measured 2026-11-25, `ClientQueryPlannerCloseITCase`).
  `close()` then waits up to a second each for the channel pool, the executor and the stream
  watchdog (`GrpcFirestoreRpc.close()` over the client context's background resources).
  A component closes its client once it has stopped using it or stopped waiting for it, so a call
  this cancels fails rather than holding the close: a sink's `BulkWriter` has flushed or been
  abandoned, and a lookup's caller has gone.
  `shutdownNow()` also drops a retry gax has scheduled on the client's executor without completing
  the call's future, as in Datastore mode above, so **the page reader's close ends every page
  still in flight before it closes the client**, by cancelling the future the page waits on,
  derived from the read's; the library's own `close()` shut its executor down gracefully, which
  still ran such a retry, and the fetcher was released by its failure (read from gax 2.91.0, not
  measured).
  The table lookup's close ends its reads in flight the same way, so an asynchronous lookup's
  future completes rather than waiting for good.
- **The planner, on the JobManager, opens a client for each plan and closes it before the plan
  returns**, through `PerCallClientRunner`'s session form: the snapshot-time probe, the probe at
  the read time, the offset resolution and `PartitionQuery` are the session's calls, and the
  user's query factory and the collection group are built from `ClientSession.client()`.
  It held its client from its first plan until the enumerator closed it.
- The sink's database access, the page reader and the table lookup hold their clients until they
  close, as before; a full lookup cache load plans, opens, fetches and closes inside
  `interruptibleReleases()`, through `FullCacheInputFormat`.
- **Residuals** ([#2343](https://github.com/flink-gcp/flink-connector-gcp/issues/2343)):
  - An emulator endpoint written as `dns:///host:port` or `http://host:port` fails at client
    creation, since gax parses the provider's endpoint as `host:port`
    (`URISyntaxException: Expected closing bracket for IPv6 address`, measured 2026-11-25 through
    the planner, with the same result before this change).
  - `GOOGLE_API_USE_CLIENT_CERTIFICATE` makes an instantiating provider build TLS credentials,
    which the plaintext configurator cannot override, as recorded for Cloud Tasks above (read, not
    measured, for Firestore).
  - `FIRESTORE_EMULATOR_HOST` takes a client off the group.
    `FirestoreOptions.Builder.build()` reads it whenever no emulator host was set and replaces the
    channel provider with a bare plaintext one.
    With an emulator endpoint configured, the connector reads the variable before the library
    does and builds the client on that endpoint through `setEmulatorHost`, so the configured
    endpoint still wins and the library never parses the variable's value, which it refuses when
    empty or malformed; a warning says the client runs on gRPC's shared group.
    Without an endpoint, the warning ADR-0171 kept says so too.
    Failing the job on the variable stays declined (ADR-0171).
  - A connector-owned `FirestoreRpc` through `setServiceRpcFactory` would remove the first two
    and could keep the third's channel on the group, though `build()` would still read the
    variable and replace the provider and credentials it was given.
    `FirestoreRpc` is public and not `@InternalApi`, but not documented as an extension point; it
    has twelve RPC callables besides its executor and shutdown methods, and its implementation,
    `GrpcFirestoreRpc`, wires a `ClientContext` and per-method retry settings the connector would
    have to copy; #2343 weighs that route.
- **A later `PartitionQuery` page can stay blocked**
  ([#2346](https://github.com/flink-gcp/flink-connector-gcp/issues/2346)).
  A `PartitionQuery` answered over several pages fetches the later pages inside the library's
  transform of the first, synchronously and without heeding an interrupt (`getUnchecked`), on the
  thread that completed the first page's call.
  If the planner closes while a later page is in flight, waiting on a retry gax has scheduled or
  on a call `shutdownNow()` fails with `UNAVAILABLE`, which `PartitionQuery` retries, and that
  retry is scheduled before the executor stops, the plan ends, since its future is cancelled, but
  `shutdownNow()` drops the retry, and that thread stays blocked for good, keeping the job's class
  loader reachable; it runs no further code, so it cannot fail after the loader closes (read from
  google-cloud-firestore 3.53.0 and gax 2.91.0, not measured).
  The library's graceful close had let the retry run and fail; the connector has no handle on the
  library's page fetch to cancel it, and
  [#2346](https://github.com/flink-gcp/flink-connector-gcp/issues/2346) weighs paging the call in
  the planner.
  If the first page answers before the library registers its transform, the plan's own thread
  runs the transform while it starts the call, under `PerCallClientRunner`'s monitor, so a close
  meanwhile waits for the later pages, up to their calls' deadlines, as the library's own close
  waited for a call in flight before this change (read, not measured).

## Evidence

Measured 2026-11-21 on a Docker session cluster (Flink 2.2.1, one JobManager and one TaskManager,
`cluster.uncaught-exception-handling: FAIL`), with a job jar shading `flink-connector-gcp-bigquery`
and grpc-netty-shaded 1.83.0, writing `STORAGE_API_AT_LEAST_ONCE` default streams to ten real
BigQuery tables.
The log pattern ended with `%throwable`, which formats without loading classes.

- **The failure**: `NoClassDefFoundError: io/grpc/netty/shaded/io/netty/util/collection/IntObjectHashMap$2`
  from `IntObjectHashMap.values` in `EpollIoHandler.prepareToDestroy`, on
  `grpc-default-worker-ELG-1-N`, about one second after the TaskManager logged
  `Close JobManager connection for job`.
  `FatalExitExceptionHandler` then exited the process with code 239.
- **Rate with 1.4.0**: the TaskManager exited in 5 of 24 cancellations and 2 of 4 JobManager stops,
  and in none of 6 cancellations after an in-job failover, since a failover keeps the class loader
  open.
- **Why intermittent**: the five crashes whose thread count was recorded each had eight shared
  worker threads during the run, and all 23 runs with four survived.
  In the runs whose class loading was logged (`-Xlog:class+load`, eight trials), the survivors had
  loaded `IntObjectHashMap$2` about two seconds into the job, when the short-lived location-lookup
  client closed and the group was destroyed and recreated while the loader was still open; the
  crashed ones had never loaded it.
- **With the group**: no TaskManager exit in 20 cancellations, 4 JobManager stops and 3 failovers
  then cancellations on the prototype, and in 14 cancellations and 3 JobManager stops on the code
  this records, with the TaskManager recreated every six jobs to keep
  [#2244](https://github.com/flink-gcp/flink-connector-gcp/issues/2244) out of the count.
  No `grpc-default-worker-ELG` thread existed during any of those runs, and the group's threads
  were gone by the time the writers had closed.
- **Reading jobs**, measured the same day on the same cluster with a job that reads one table
  through `BigQuerySource`:
  - cancelled once its read session existed, the JobManager exited with code 239 in 8 of 8 trials
    on 1.4.0, and in 7 of 8 with the group but the session client still held until the
    enumerator closed; the `NoClassDefFoundError` came from the group's teardown on the
    JobManager.
    With the client closed after each call, it exited in none of 8, both on the code this
    records and on an earlier form that cancelled through the client's `shutdownNow()`.
  - left to finish, the TaskManager exited in 3 of 3 trials on 1.4.0, from the reader's client
    on gRPC's group, and the JobManager in none; with this change, neither exited in 3 of 3 on
    either form.
  - On the code this records, a writing job was also cancelled 5 times and had its JobManager
    stopped once, with no exit.

- **Pub/Sub** (#2263), measured 2026-11-23 on the same kind of cluster with the Pub/Sub emulator:
  a SQL job reading one subscription into one topic, the SQL connector JAR shipped with it
  (`sql-client.sh -j`), cancelled 8 s after it started running, 12 times per arm.
  On 1.4.0 the TaskManager exited with code 239 in 12 of 12, after `NoClassDefFoundError:
  .../IntObjectHashMap$2` from a `grpc-default-worker-ELG` thread; with the group, neither process
  exited in 12 of 12 on the first form of this change and in 12 of 12 on the code this records,
  and no `grpc-default-worker-ELG` thread appeared.
  The JobManager did not exit on either arm; these trials cancel after the enumerator's startup
  calls have returned, so they do not exercise a cancellation while one is in flight.

- **Bigtable** (#2264), measured 2026-11-23 on the same kind of cluster with the Bigtable
  emulator, the SQL connector JAR shipped with each job (`sql-client.sh -j`).
  A first job on each fresh cluster wrote eight rows through the sink, creating the table, and
  finished.
  Then a bounded job scanned that table into a second table, through the scan enumerator's
  row-key sampler on the JobManager and the reader and the sink on the TaskManager, and ran to
  the end, 8 times per arm.
  A streaming job writing generated rows through the sink was also cancelled 8 s after it started
  running, 8 times per arm.
  On 1.4.0 the first job exited the TaskManager with code 239 on each of the 3 clusters, every one
  of the 8 scanning jobs exited both the JobManager and the TaskManager with code 239, and every
  one of the 8 cancellations exited the TaskManager with code 239, each after
  `NoClassDefFoundError: .../IntObjectHashMap$2` from a `grpc-default-worker-ELG` thread.
  With this change, neither process exited after any of these jobs, and no
  `grpc-default-worker-ELG` thread appeared.
  The emulator has no change streams, so the Change Streams coordinator was not exercised.

- **Releasing after an interrupt** (#2269), measured 2026-11-23 on a workstation with a unit-level
  probe that closed the last leased client 20 times per case, interrupted and not.
  The cases were a call still in flight on a local Netty gRPC server, a fake client whose
  shutdown did nothing and whose termination wait returned `false` at once while its channel
  stayed open on the group, and a connection still being opened to an unroutable address.
  Every close took 9 to 25 ms on the code before this change and 10 to 26 ms on a first form of
  it that cut every interrupted release short the way the scope now does (declined below), except
  an uninterrupted close with a call in flight, which spent the client's 5 s waiting for the call
  before it cancelled it.
  For the fake client, that time is the group's shutdown closing the channel the client left
  open, since its own waits returned at once.
  None of these cases held the group's shutdown up; a task still running on a group thread
  does, which a latch-blocked task in `GrpcEventLoopsTest` reproduces.
- **Spanner** (#2265), measured 2026-11-23 against a plaintext endpoint that answers the client's
  emulator check and leaves every data call open, a planner closed while its `CreateSession` was
  in flight, and the coordinator's client closed while its first query was in flight on a session
  the endpoint had created, each ended the call at once (each test class, server start included,
  took under 2 s).
  With the context's cancellation removed, both calls were still open 20 s after the close.
  On the same kind of Docker session cluster as Pub/Sub's, with the Spanner emulator and the SQL
  connector JAR shipped with the job, a SQL job reading a change stream into a blackhole sink was
  cancelled about 8 s after it started running, and a bounded scan of the same table was left to
  finish.
  On 1.4.0, the TaskManager exited with code 239 in 10 of 10 cancellations and the JobManager in 8
  of them, and the TaskManager in 5 of 5 finished scans, each after `NoClassDefFoundError:
  .../IntObjectHashMap$2` from a `grpc-default-worker-ELG` thread.
  With this change, neither process exited with code 239 in 26 cancelled jobs (28 attempted, on
  fresh clusters of up to nine jobs) or in 10 finished scans: 5 of each on a form that differs
  from the final code only in its close paths' ordering, waits and error handling, and the rest on
  an earlier form whose planner kept its call's context and built a database client for every
  plan; no `grpc-default-worker-ELG` thread appeared, and the
  JobManager held no gRPC event-loop thread while the job ran.
  Those clusters failed in another way from the sixth job on: per-job growth of about 27 MB of
  metaspace and 21 MB of direct memory, the same on 1.4.0 under `LOG`, ended in direct-memory
  failures in the TaskManager's tasks and a `Metaspace` `OutOfMemoryError` that stopped the
  JobManager at the ninth job.
  The threads that grew with them belong to executors the client libraries keep in static fields
  and never shut down (grpc-gcp 1.20.0's `gcp-mc-bg` pool, Spanner's `multiplexed-session-maintainer`).
  Heap dumps later showed every strong path to each finished job's class loader running through
  these threads in both processes; the connector now lets them exit
  ([ADR-0210](0210-a-session-cluster-can-opt-out-of-grpc-buffer-pools-and-spanner-static-executors-let-idle-threads-exit.md),
  [#2296](https://github.com/flink-gcp/flink-connector-gcp/issues/2296)).

- **The pin** (#2268), measured 2026-11-25 on the same kind of cluster (Flink 2.2.1) with the
  BigQuery emulator: a streaming SQL job wrote ten rows through a default-stream table sink whose
  destination idle timeout was 3 s, kept running with no further rows, and was cancelled 17 s after
  it started running, one trial per arm.
  With the SQL connector JAR shipped with the job, the group's 8 threads were still running after
  the destination's eviction, the TaskManager ran the hook once when the job left it (logged under
  the SQL JAR's relocated class name, `io.github.flink.gcp.connector.bigquery.shaded.io.github.flink.gcp.connector.base.rpc.GrpcEventLoops`),
  and no group thread remained 8 s after the cancellation.
  Both arms of this change were run on its final code and on its first form, which registered the
  hook under a fixed name, with the same results.
  On the code before this change, and with this change's JAR in `lib/`, the threads were gone after
  the eviction and no hook ran.
  No process exited and no `NoClassDefFoundError` was logged in any arm.
  The emulator's default stream does not go through the pool, so these trials show when the group
  stops, not a job's own writer on pooled connections.
- **Firestore's Datastore mode** (#2329), measured 2026-11-25 on the same kind of cluster with the
  Firestore emulator in Datastore mode (`google-cloud-cli:591.0.0-emulators`) and the SQL connector
  JAR shipped with each job (`sql-client.sh -j`).
  Each fresh cluster first ran a batch job that wrote eight entities through the sink and
  finished; when that took a process down, the cluster was recreated before the trial.
  A trial was then either a bounded scan of that kind into a blackhole sink, through the scan
  enumerator's planner on the JobManager and the reader on the TaskManager, left to finish, or a
  streaming job writing generated rows through the sink, cancelled 8 s after it started running.
  On 1.4.0 the writing batch job exited the TaskManager with code 239 on 12 of 12 clusters, and
  every one of 8 cancellations exited the TaskManager the same way.
  Of three finished scans, all three exited the TaskManager and two the JobManager; the second,
  which the JobManager survived, still showed four `grpc-default-worker-ELG` threads there after
  the job had finished, and the harness stalled after the third, so the other five were not run.
  Each exit followed `NoClassDefFoundError: .../IntObjectHashMap$2` from a
  `grpc-default-worker-ELG` thread.
  With this change, neither process exited after either of 2 writing batch jobs, 8 finished scans
  or 8 cancellations, no `grpc-default-worker-ELG` thread appeared, the TaskManager ran four or
  five `flink-gcp-grpc-elg` threads while a cancelled job wrote, and neither process held a gRPC
  event-loop thread once a scan had finished.
  The emulator tests that check the group's threads and dial a `dns:///` target were also run
  against the code before the change: every Datastore client failed the thread check, the lookup
  could not dial the target (`IllegalArgumentException: Invalid host or port`), and the other
  clients could not be created (`UnknownHostException: dns`).
- **Firestore's Native mode** (#2330), measured 2026-11-25 the same way with the emulator in
  Native mode (the same image), a collection seeded by the writing batch job and a bounded scan
  of that collection.
  On 1.4.0 the writing batch job exited the TaskManager with code 239 on 18 of 18 clusters, every
  one of 8 cancellations exited the TaskManager the same way, and every one of 8 finished scans
  exited the TaskManager, four of them the JobManager too.
  The four scans the JobManager survived each left five `grpc-default-worker-ELG` threads there
  after the job had finished.
  Each exit followed `NoClassDefFoundError: .../IntObjectHashMap$2` from a
  `grpc-default-worker-ELG` thread.
  With this change, neither process exited after either of 2 writing batch jobs, 8 finished scans
  or 8 cancellations, no `grpc-default-worker-ELG` thread was in any thread sample or log line,
  the TaskManager ran seven
  `flink-gcp-grpc-elg` threads while a cancelled job wrote, and neither process held a gRPC
  event-loop thread once a scan had finished or 6 s after a cancellation.

One prototype trial exited the TaskManager with a direct-memory `OutOfMemoryError` at job start.
The same A/B run on 1.4.0 and on the prototype left six user-code class loaders, each with its own
shaded Netty allocator, reachable after six jobs, so that is unrelated to this change.
A later investigation traced this kind of retention, on reading jobs, to Netty's finalizers
([ADR-0210](0210-a-session-cluster-can-opt-out-of-grpc-buffer-pools-and-spanner-static-executors-let-idle-threads-exit.md)); the
writing jobs here were not traced.

## Alternatives declined

- **Awaiting the client's termination in `close()`**, which #2237 first proposed.
  The destroy is scheduled inside gRPC one second after the last channel, whatever the client
  awaited, and on the pooled path the client belongs to the SDK's JVM-static pool, not to the
  appender.
- **A Flink release hook** (`UserCodeClassLoader.registerReleaseHookIfAbsent`) that waits for gRPC's
  group to go.
  The group is reachable only through gRPC's internal `SharedResourceHolder`, and the hook would
  block the TaskManager on gRPC's one-second timer for every job.
  It would also not cover the JobManager: a sink writer's and a source reader's init contexts
  expose `getUserCodeClassLoader()`, but `SplitEnumeratorContext` does not (Flink 2.2.1), so the
  enumerator's clients would have nothing to register on.
- **Loading the destroy path's classes while the loader is open**, which is what the surviving runs
  did by accident.
  It covers only the classes this Netty version's epoll destroy path was seen to need.
- **Documenting a `lib/` deployment for FAIL mode.**
  The parent class loader never closes, so it avoids the failure.
  But the upgrade guidance recommends shipping the connector with the job, which leaves one copy of
  it, and that is the deployment that exits.
- **A sibling of `LeasedClient` for clients that are not `BackgroundResource`s** (#2312).
  `PerCallClientRunner` holds clients of both kinds, so two holder types would need a third,
  shared one for the runner to take.
- **Holding the interrupt back for a gax client too** (#2312's first form).
  It protects no cleanup there, and a resource that would not terminate could then hold a full
  lookup cache load's close for `LeasedClient`'s bounds.
  The close waits once before cancelling and once after, and each wait passes the whole 5 s to
  each resource the client aggregates in turn until one times out (gax 2.91.0), so such a
  resource costs at least 10 s, Flink's entire budget.
- **Holding back a waiting thread's interrupt** (#2312), as Bigtable's runner did for one set
  before the wait.
  The thread closing the client holds its own interrupt back so that the client's cleanup runs to
  the end; a waiting thread does none of that cleanup, and the close finishes whether or not it
  stays.
  Holding its interrupt back would only keep a thread that is being stopped waiting for another
  thread's close, up to that client's whole close (twelve minutes for a Bigtable data client).
- **Giving the connector a connection pool of its own** (#2268), by passing credentials whose
  `hashCode` differs from the job's.
  It means wrapping `Credentials`, and gax and gRPC branch on the concrete credentials type
  (self-signed JWTs for `ServiceAccountCredentials`, the quota-project header, DirectPath's
  `ComputeEngineCredentials` check).
- **Pinning from every TaskManager-side writer and reader** (#2268).
  Only the Storage Write API's pool keeps channels the connector's settings built after the
  connector's last lease is released; elsewhere the lease rule already stops the group when its
  last channel closes.
  A pin moves the group's shutdown from the task's close to the release of the loader, which Flink
  runs on the TaskManager's main thread while it holds its library cache's lock, so it is taken
  only where it fixes something.
- **A group per client.**
  It would multiply event-loop threads by the number of clients, where one shared group matches
  what gRPC itself does.
- **Letting an interrupt end every release's wait after its first second** (#2269's first form).
  A task thread can release the last lease outside its close, while Flink may still interrupt it
  to cancel the task: a default-stream writer's checkpoint flush closes the appender of a
  destination idle past its timeout.
  Flink stops interrupting the task thread before it closes operators (`StreamTask`, Flink
  2.2.1), but not before a flush, so a cutoff there could let a busy group's threads outlive the
  class loader, the failure this decision removes.
  The scope confines the cutoff to the thread that needs it.

## Consequences

- Closing a reader or writer now waits for its client's channels to terminate, cancelling their
  calls after 5 s, and the last close also waits for the group, up to 11 s; in a full lookup
  cache load, once the first second has passed, an interrupt ends that wait.
  Closing the enumerator requests the cancellation of a session creation still in flight; one
  that misses a retry gax is starting waits like any other client close.
  gax passes each wait to every resource a client aggregates, so a client that will not stop can
  hold a close for a small multiple of that.
  An appender's close is the exception, because `StreamWriter.close()` is the SDK's: its
  `ConnectionWorker.close()` joins the append thread without a bound, calls
  `awaitTermination(150, SECONDS)` on the connection's client, which gax passes to each resource
  the client aggregates in turn until one times out, and waits up to 3 min for its callback
  executor (google-cloud-bigquerystorage 3.38.0).
  An unpooled writer, which the buffered-stream path and the emulator's default stream use, runs
  it at every close; the pooled default-stream writer runs it when it closes a connection's last
  writer.
  The appender releases what it holds in a `finally` around `StreamWriter.close()`, so only once
  that close has returned or thrown: the pooled appender its bare lease, and the emulator's
  default-stream appender its client's holder, which closes the client and then the emulator
  channel and its lease.
  A buffered-stream appender holds neither; its service keeps the client, and the writer closes
  the service after every appender.
- The Storage Write API's connection pool is JVM-static per class loader and keyed by location and
  credentials, and it builds every connection from the first writer's settings.
  If a job's own code also writes through that pool in the connector's class loader, its
  connections share the connector's settings and land on the group while a connector lease is
  open; created first, the job's settings leave the connector's connections on gRPC's group
  instead.
  Since #2268 a job whose loader defined the connector keeps that group until its class loader is
  released, so the job's writers keep their connections.
  With the connector in `lib/` there is no pin, and the connector stops the group when its last
  lease is released even if the job's writers still use those connections.
- In a job whose loader defined the connector, once a default-stream writer has been created,
  the group runs from its first client until Flink releases the job's class loader on that
  TaskManager, idle or not, and it serves every other connector in the same copy of
  `GrpcEventLoops` until then: a group the earlier consequences say stops with its last lease
  stops at the release of the loader instead.
  The hook's wait for the threads runs on the TaskManager's main thread, which releases the
  loader, while Flink holds its library cache's lock, so it holds up that TaskManager's RPCs and
  any task obtaining its class loader from the cache for as long as it takes: normally
  milliseconds, up to 11 s if a group thread is busy, after which the hook gives up and a thread
  still busy can outlive the loader.
  A failover inside the job keeps the loader while the TaskManager keeps a slot for the job or
  still tracks a result partition of it, so the group survives it; a TaskManager left with
  neither during a restart releases the loader, and the restarted tasks get a new loader with a
  new copy of the class.
- Otherwise the group stops whenever its last lease closes, for example when the last destination
  of a default-stream writer is evicted as idle in a `lib/` deployment, and the next client starts
  a new one.
- A channel a client builds without a lease still uses gRPC's group.
  A Firestore Native client built while `FIRESTORE_EMULATOR_HOST` is set is one
  ([#2343](https://github.com/flink-gcp/flink-connector-gcp/issues/2343)).
- **A Bigtable data client's built-in Cloud Monitoring export still uses gRPC's group.**
  Its `MetricServiceClient` takes no channel settings from the data client, so while the export
  is on, which is the default for the readers', the sinks' and the lookups' clients, each such
  client on a TaskManager keeps a channel on gRPC's group, and the delayed teardown of that group
  can still run after the job's class loader has closed.
  Reproduced outside Flink on 2026-11-23
  ([#2297](https://github.com/flink-gcp/flink-connector-gcp/issues/2297)): with the data channels
  on an event-loop group of the reproducer's own, closing the client and then its class loader
  logged `NoClassDefFoundError` from the teardown of gRPC's group in 5 of 5 runs with the export on,
  and in none of 5 with it off.
  Reported upstream as
  [googleapis/google-cloud-java#15144](https://github.com/googleapis/google-cloud-java/issues/15144).
  Turning the export off (`exportClientMetricsToCloudMonitoring(false)` or the Table option)
  removes it.
- Under DirectPath, gRPC's xDS client and ALTS handshaker open control channels of their own,
  which no channel configurator reaches; they stay on gRPC's own groups.
  This applies only on Google Cloud with DirectPath in use and has not been measured.
- The Change Streams coordinator now builds a data client for each reconciliation scan, so each
  scan pays a client's setup, channel priming included, every 10 s.
- `productionChannelProvider` restates the SDK's pooled-writer transport settings, so a change to
  them in a later google-cloud-bigquerystorage is not picked up by itself.
  `StreamWriterRowAppenderFactoryTest` compares the restated provider with the one the SDK builds,
  so such a change fails the build instead.
- `PubSubTransports` restates the default providers of the SDK's `Publisher` and `Subscriber`
  builders in the same way; `PubSubTransportsTest` compares them with the builders' own fields.
- A Pub/Sub publisher or subscriber whose shutdown overran its budget releases its hold anyway.
  If that was the group's last lease, the group's shutdown closes the channels the abandoned
  client still holds.
- On the JobManager a client is safe only if it is closed before the job's class loader is
  released: a release while a read session is being created, when a table read starts or once a
  query read's query has finished, can still see the group torn down after the loader closed,
  because that close runs on Flink's threads in no order with the release.
  The job ending in any way, a stop with a savepoint included, or the JobManager shutting down
  releases the loader.
  Every other connector's enumerator-side client needs the same treatment, which #2243 covers.
  A catalog does not: it keeps its client until it is closed, and the SQL Gateway closes it
  before the session's class loader (see the Spanner and Bigtable sections).
  A Spanner Change Streams coordinator, or the planner of a batch read whose operation is
  deferred, closed while its database client creates its first session waits for the client's shutdown (about 20 s measured
  for the coordinator; the planner's is the same wait, not measured), a longer window for that
  race than the other connectors' cancelled calls leave.
- If the SDK's location lookup fails, the short-lived client it opened for it is never shut down
  (google-cloud-bigquerystorage 3.38.0 closes it only on success), so its channels stay on the
  group until the last lease is released and the group closes them.
