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
  [#2243](https://github.com/flink-gcp/flink-connector-gcp/issues/2243)
- Modules: base, bigquery, cloudtasks
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
  `acquire()` opens a lease and creates the group if none is open; closing the last lease shuts the
  group down (no quiet period, a 10 s bound) and waits for termination.
  A later lease creates a new group.
- A lease applies the group to a client's settings (`applyTo(ClientSettings.Builder)`) or to a
  transport provider builder, composing a channel configurator after the one the provider
  carries (such as `EmulatorChannels`' plaintext one).
  It refuses settings whose provider does not build its own channels, such as a fixed provider
  around a caller-built channel, because the group cannot reach that channel and leaving it on
  gRPC's group silently is the failure this decision removes.
  A caller that owns its channel opens it with `EmulatorChannels.openLeasedPlaintextChannel`
  instead (refined 2026-11-22, #2243), which builds the channel on the group under a lease of its
  own; the caller closes the clients using it first and the channel's holder last.
  The configurator reads the current group when gax builds a channel and leaves the builder alone
  when no lease is open, so a configurator that outlives its lease never hands a channel a
  terminated group.
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
- The SDK's `ConnectionWorkerPool` builds its connections from the first writer's settings for the
  life of the class loader.
  A connection it opens after that writer's appender closed still lands on the group, because it
  is opened for a writer whose own appender holds a lease, and the pool closes and awaits a
  connection when its last writer closes, before that writer's appender releases its lease.

**In the Cloud Tasks connector (#2243, 2026-11-22), every client holds a lease the same way.**

- The task creator, which the eager writer and the exactly-once committer each open on a
  TaskManager, opens its production client with `open`.
- **The emulator arm keeps ADR-0081's caller-owned channel**, now opened on the group by
  `EmulatorChannels.openLeasedPlaintextChannel`, which builds it as before from the endpoint's
  gRPC target (`EmulatorEndpoint.getTarget()`, resolver scheme intact) and returns it under a lease of its own (a `LeasedClient`
  around gax's `GrpcTransportChannel`).
  `fixedProvider` hands it to the client, whose own `open` lease covers no channel and only gives
  it the same bounded close; the creator closes the client first and the channel last, and its
  creation failure path, `Error`s included, closes whichever of the two it had opened through
  `Closers.closeAllSuppressing`, whose contract decides which failure is reported.
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

The Pub/Sub, Bigtable, Spanner and Firestore connectors move onto the group in #2243.

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

One prototype trial exited the TaskManager with a direct-memory `OutOfMemoryError` at job start.
The same A/B run on 1.4.0 and on the prototype left six user-code class loaders, each with its own
shaded Netty allocator, reachable after six jobs, so that is unrelated to this change.
A later investigation traced this kind of retention, on reading jobs, to Netty's finalizers
([ADR-0210](0210-a-finished-jobs-grpc-buffer-pools-are-released-by-netty-finalizers.md)); the
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
- **A group per client.**
  It would multiply event-loop threads by the number of clients, where one shared group matches
  what gRPC itself does.

## Consequences

- Closing a reader or writer now waits for its client's channels to terminate, cancelling their
  calls after 5 s, and the last close also waits for the group, up to about 10 s.
  Closing the enumerator requests the cancellation of a session creation still in flight; one
  that misses a retry gax is starting waits like any other client close.
  gax passes each wait to every resource a client aggregates, so a client that will not stop can
  hold a close for a small multiple of that.
  The pooled default-stream writer is the exception: `StreamWriter.close()` is the SDK's, and when
  it closes a connection's last writer it joins the append thread without a bound, waits up to
  150 s for the client and up to 3 min for its callback executor (google-cloud-bigquerystorage
  3.38.0); the lease is released only after it returns.
- The Storage Write API's connection pool is JVM-static per class loader and keyed by location and
  credentials, and it builds every connection from the first writer's settings.
  If a job's own code also writes through that pool in the connector's class loader, its
  connections share the connector's settings: they land on the group while a connector lease is
  open, and the connector stops the group when its last lease is released (its last BigQuery
  client in that class loader closes) even if the job's writers still use those connections;
  created first, the job's settings leave the connector's connections on gRPC's group instead.
  The connector cannot give its pool a key of its own, so this is recorded rather than prevented.
- The group stops whenever its last lease closes, for example when the last destination of a
  default-stream writer is evicted as idle, and the next client starts a new one.
- A channel a client builds without a lease still uses gRPC's group.
  Until #2243 lands, that is every channel of the Pub/Sub, Bigtable, Spanner and Firestore
  connectors.
- `productionChannelProvider` restates the SDK's pooled-writer transport settings, so a change to
  them in a later google-cloud-bigquerystorage is not picked up by itself.
  `StreamWriterRowAppenderFactoryTest` compares the restated provider with the one the SDK builds,
  so such a change fails the build instead.
- On the JobManager a client is safe only if it is closed before the job's class loader is
  released: a release while a read session is being created, when a table read starts or once a
  query read's query has finished, can still see the group torn down after the loader closed,
  because that close runs on Flink's threads in no order with the release.
  The job ending in any way, a stop with a savepoint included, or the JobManager shutting down
  releases the loader.
  Every other connector's JobManager-side client (enumerators, catalogs) needs the same treatment,
  which #2243 covers.
- If the SDK's location lookup fails, the short-lived client it opened for it is never shut down
  (google-cloud-bigquerystorage 3.38.0 closes it only on success), so its channels stay on the
  group until the last lease is released and the group closes them.
