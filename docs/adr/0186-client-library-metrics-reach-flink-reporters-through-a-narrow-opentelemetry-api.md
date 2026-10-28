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

# ADR-0186: Client-library metrics reach Flink reporters through a narrow OpenTelemetry API

- Status: Accepted
- Date: 2026-10-25
- Updated: 2026-10-27 (Cloud Tasks adopts it through gax's tracer factory, refined by [#715])
- Updated: 2026-10-27 (every component that owns a Bigtable data client takes both options, refined by [#1903])
- Issues: [#713], [#1902], [#1903], [#1908], [#712], [#715], [#714], [#1815]
- Modules: base, bigtable, cloudtasks, `scripts/`, root POM
- Current behavior: `docs/content/docs/connectors/datastream/bigtable.md` § Client-library
  metrics; `docs/content/docs/reference/bigtable.md` § `BigtableSink.builder()`;
  `docs/content/docs/connectors/datastream/cloudtasks.md` § Client RPC metrics

## Decision

A Google Cloud client library's own metrics reach Flink's metric reporters through
`base.metrics.ClientMetrics`, which implements the part of the OpenTelemetry Metrics API
the clients record through, and a connector adopts it with an adapter of its own. The design of
2026-08-15 on [#712] is kept in its shape (an opt-in, `rpcMethod.METHOD.leaf` series, histogram
count and millisecond sum, no quantiles) and replaced in its mechanism, as the evidence below
records. What `ClientMetrics` and its first adopter are:

- **`ClientMetrics` implements the API, not the SDK.** The connector hands its
  `OpenTelemetry` to the client's metrics-provider hook. Its meter serves long counters and double
  and long histograms (through `MeterBuilder` too, with the `Context` overloads), and each `add` or
  `record` adds straight into a `LongAdder` or `DoubleAdder` behind a read-only Flink counter, on
  the client's thread. There is no collection step, no buffer and no thread; a reporter reads the
  current total. Nothing touches `GlobalOpenTelemetry`, so a relocated copy is self-contained.
- **One attribute, the RPC method, picks the series.** Every other attribute is ignored, so a
  component reports instruments times methods series. Its key is the adopter's: `method` for
  Bigtable and Spanner, `rpc.method` for gax's public factory (Cloud Tasks).
- **Counters only grow.** A negative or non-finite value is dropped, as the SDK drops it, because
  reporters' backends compute rates. A histogram keeps a count and, for a duration in `ms` or `s`,
  a millisecond sum; under another unit only the count. Percentiles, buckets and extremes are not
  kept, a choice that keeps every series a counter. A count and its sum are separate adders, so a
  report taken mid-recording can see one value more in one of them; a rate window absorbs it.
- **What is not mapped is a no-op with one `WARN`**: up-down counters, gauges, double counters
  and observable instruments (no adopter in the issue map records through one), a recording for an
  unregistered method, and an instrument nothing is registered for. An instrument the adapter
  declares with `ignore(...)`, because the client records it wrongly, is dropped without the
  warning. `close()` stops recording and keeps every value.
- **Every series is registered when the component opens.** A connector names the RPC methods its
  component's client calls, and the adapter registers each one's leaves as `rpcMethod.METHOD.leaf`
  on the component's group. The issue's `sdkSeries` bootstrap gauge is gone: nothing waits for a
  first collect.
- **The adapter lives in the connector and is a `[[subgroups]]` source.** It declares the
  instruments, the leaf constants and the registration, in the same one-`addGroup` shape as
  `ErrorClassCounters`. `check-metric-docs.py` reads its group and leaves and demands a templated
  row for each; the claim in `metric-docs.toml` keeps its registrations out of the module's
  `*MetricNames` inventory rule, and in exchange every registration in it must name a leaf
  constant.
- **Two independent options, named for a cross-connector spelling.** `exportClientMetricsToFlink`
  (`client-metrics.export-to-flink`, off by default) creates the `ClientMetrics` and installs its
  `OpenTelemetry` through the client's metrics-provider hook.
  `exportClientMetricsToCloudMonitoring` (`client-metrics.export-to-cloud-monitoring`, on by
  default) keeps or removes the client's built-in Cloud Monitoring exporter. They are
  connection-level, so the Table keys carry no direction prefix, as `service-account-key-file`
  does not. Spanner ([#712]) and Cloud Tasks ([#715]) reuse both names, Cloud Tasks only the first.
- **Bigtable maps ten leaves per method** from the public block `google-cloud-bigtable` 2.85.0
  records into an injected `OpenTelemetry`: five of its six double histograms in `ms` (operation,
  attempt, server, first-response and application latencies) and both long counters (retries,
  connectivity errors). Two histograms keep only their sum, because the client records them at most
  once per operation: application latency for every operation, so its count would repeat
  `operations`, and first-response latency only for a streamed `ReadRows` and for operations served
  on the session path (`BuiltinMetricsTracer`, `VRpcTracerImpl`; corrected in [#1903]). `throttling_latencies` is not mapped:
  `BuiltinMetricsTracer` adds a batch's flow-control wait to its total in milliseconds, records the
  total as nanoseconds, and never resets it between attempts, so the value is wrong in both unit
  and scope until a client release fixes it. The
  at-least-once writer was the first component: its batch factory owns the `ClientMetrics` and closes it
  after its clients. Every other component that owns a data client followed in [#1903]; see the
  refinement below.
- **Dependencies: `opentelemetry-api` only.** Base declares it optional, because base is bundled
  into every SQL jar, and the adopter declares it itself. Its version comes from an
  `opentelemetry-bom` import in the root POM, pinned to what `libraries-bom` resolves through the
  clients, because `libraries-bom` manages no OpenTelemetry. An enforcer `requireUpperBoundDeps`
  execution on `opentelemetry-api` and `opentelemetry-sdk-metrics` fails a `libraries-bom` bump
  whose clients ask for a newer version.

- **Names say what a thing holds or where it sends.** The base class is `ClientMetrics`, beside
  `DestinationMetrics` and `ErrorClassCounters`, and the adapter `BigtableClientMetrics`. Its
  readers are `counterSum`, `histogramCount` and `histogramSumMillis`: an instrument kind and its
  aggregate, in OpenTelemetry's words. The group key is `rpcMethod`, named for its value as
  `errorClass` and `destination` are, and matching OpenTelemetry's `rpc.method`. A counter leaf is
  a noun phrase ([ADR-0038]), so the server-timed subset of attempts is `attemptsWithServerTiming`.
  The two options name their destinations, Flink and Cloud Monitoring, as peers.

[ADR-0010]'s finding that the Pub/Sub client has no metrics to pass through is Pub/Sub's and still
holds at the pinned version; Bigtable and Spanner do ship meters, which is what this per-connector
divergence rests on. Pub/Sub has no adapter and no issue.

## Evidence

- **What the clients record through is small.** Bigtable 2.85.0's user recorder
  (`MetricRegistry.RecorderRegistry`) creates six `histogramBuilder` and two `counterBuilder`
  instruments and sends the rest to a no-op meter. gax 2.87.0 has two paths. Its public
  `OpenTelemetryMetricsFactory`, which Cloud Tasks adopts ([#715]), records one double histogram in
  `s` keyed by `rpc.method`. Its internal `MetricsTracerFactory` recorder, which Spanner 6.124.0
  builds on, records two double histograms in `ms` and two long counters keyed by `method`, and
  Spanner adds two histograms and two counters of the same kinds. None casts the instance to an SDK
  type for these; Spanner's gRPC-layer metrics require an
  `OpenTelemetrySdk` and are out of reach for any implementation that is not one.
- **The SDK cost a reader and its machinery.** The first implementation of this change used
  `opentelemetry-sdk-metrics` with views and a pull reader: reading a counter collected at most once
  a second into a snapshot, close collected once more and froze it. Each piece existed only to read
  back what the SDK aggregated, and the three review rounds' findings concentrated there. It also
  added about 680 KB to a SQL jar whose client has no OpenTelemetry (Cloud Tasks), against about
  230 KB for the API alone, and kept min and max per histogram that nothing reported.
- **Registration stays at open.** A registration from a client thread runs while a reporter may be
  iterating its metric map: `Slf4jReporter` iterates without a lock and swallows the
  `ConcurrentModificationException`, losing that report. A registration inside a reporter's own read
  would be worse: Flink 2.2.1's `OpenTelemetryMetricReporter` iterates a `HashMap` under
  `synchronized (this)` and a same-thread registration re-enters it (read from source).
- **The Flink export runs beside the built-in export, not instead of it.** In `google-cloud-bigtable`
  2.85.0, `BigtableClientContext` creates the built-in Cloud Monitoring exporter whenever
  `areInternalMetricsEnabled()`, independent of the metrics provider; a
  `CustomOpenTelemetryMetricsProvider` adds a user recorder beside it, and `NoopMetricsProvider`
  removes neither. Only `disableInternalMetrics()` removes the exporter, which is why the second
  option exists. The two are independent on the classic stub path that `MutateRows` takes. The
  session (vRPC) path that serves `ReadRow` and `MutateRow` is not: `MetricsImpl.newTableTracer`
  returns a no-op tracer when internal metrics are off, so the user recorder sees none of those
  RPCs then; the refinement below records how far that reaches. [#1815]'s first note named `NoopMetricsProvider` as the
  switch; the issue carries the correction.
- **Turning the export off removes the close stall; the Flink export alone does not.** On 2026-10-25, on a
  host where `metadata.google.internal` resolves to an address nothing answers, a client built
  with production settings and pointed at the emulator over plaintext closed in 0 ms with
  `disableInternalMetrics()`, and in 75,225 ms with only the `ClientMetrics` provider set (built-in
  export on). `EnvInfo::detect`, the platform detection, has one caller in 2.85.0, the built-in
  exporter. A second built-in client closed right after the stalled one did not stall, so the
  docs say a close *can* wait rather than that every close does.
- **The real client records against the emulator.** The emulator builder sets
  `NoopMetricsProvider`, and a provider set after it replaces it. `BigtableClientMetricsITCase`
  writes forty records through the public builder and a scheduled test reporter reads
  `rpcMethod.MutateRows.*` while the job runs; no unregistered method was reported. The emulator
  sends no Google front-end headers, so every attempt counts as a connectivity error and no server
  time is recorded, which the IT pins and the docs state. Spanner differs: its custom provider
  records nothing against the emulator, so [#712] needs another way to observe it.
- **The checker needed one change.** Its inventory rule demands a `*MetricNames` constant for every
  `.counter(`/`.gauge(` in a mapped module, so an adapter inside a connector module failed it; the
  2026-08-15 design's "zero checker changes" held only for a registrar in base. Claimed
  `[[subgroups]]` sources are now skipped by that rule, and in exchange every registration in one
  must name a leaf constant the template reader can see, or the check fails; a literal name there
  would otherwise be an undocumented metric nothing reports. Synthetic tests cover the claimed,
  unclaimed and literal-name cases.
- **The enforcer rule fires.** With `-Dopentelemetry-bom.version=1.57.0`, the Bigtable module fails
  naming `opentelemetry-api` and `opentelemetry-sdk-metrics` and the 1.62.0 the clients asked for;
  the pinned version passes the whole reactor. The plugin's `includes` matched neither a bare group
  nor `group:*` (measured), so it names the two artifacts.

## Refinement (2026-10-27): Cloud Tasks adopts it through gax's tracer factory

[#715] adopted `ClientMetrics` in the Cloud Tasks sink. Its client has no metrics of its own, so
`DefaultTaskCreatorFactory` sets gax's public `OpenTelemetryMetricsFactory` as the client's tracer
factory, over the `ClientMetrics` instance, on the production and emulator arms alike. gax records
one histogram per call, which `CloudTasksClientMetrics` maps to `operations` and
`operationTimeMillis` under `rpcMethod.CreateTask`; the leaves take Bigtable's words for the same
quantity rather than the issue's `requests` (owner, 2026-10-27). Every component that owns a Cloud
Tasks client registers them when it opens: the at-least-once writer, and the committer under
`EXACTLY_ONCE`, which owns the client in that mode. The task creator owns the `ClientMetrics` and
closes them after its client. The v2beta3 queue-retention readback, one `GetQueue` per committer
start, is not instrumented. There is no Cloud Monitoring exporter to switch, so only the first
option exists.

- **What gax records, and when.** In gax 2.87.0, `OpenTelemetryMetricsFactory` creates
  `gcp.client.request.duration` (a double histogram in `s`) only when the client's stub settings
  declare library metadata with an artifact name; without it the factory falls back to a no-op
  tracer and records nothing, silently. `google-cloud-tasks` 2.99.0 declares
  `com.google.cloud:google-cloud-tasks`. The tracer records once per call, on success, failure and
  cancellation, with `rpc.method` set to the gRPC full method name
  (`google.cloud.tasks.v2.CloudTasks/CreateTask`) and the response status beside it. The client's
  `CreateTask` retry codes are empty, so a call is one attempt. `setTracerFactory` is `@BetaApi`.
- **The emulator test is the tripwire.** The library-metadata condition and the annotation are
  re-read on a `libraries-bom` bump that moves gax or `google-cloud-tasks`, and
  `CloudTasksClientMetricsITCase` fails if the series stop moving: with the factory's installation
  removed from the client settings, both of its export cases failed (2026-10-27).
- **The SQL jar gains the API alone.** gax declares `opentelemetry-api` optional, so nothing
  brought it before; the connector now declares it, and the jar bundles `opentelemetry-api`,
  `-context` and `-common` 1.62.0 (about 230 KB as jars), relocated under its shaded prefix.

## Refinement (2026-10-27): every component that owns a data client takes both options

[#1903] applied the two options to every Bigtable component that owns a data client, and removed
part 1's refusals: the `EXACTLY_ONCE` committer, the single-row sinks and async helpers, the SQL
async functions (as `client-metrics.*` keys in their settings), the scan and Change Streams readers
and enumerators, and the lookups. Each registers the one method its client calls, on its own
metric group. Four findings shaped it.

- **The client metrics are closed only where the component's group outlives a client.** Closing
  freezes the counters, which matters where a reporter can still read them after the component
  ends. A writer's, reader's, committer's or function's group ends with the component, so those
  components leave their client metrics open, and a client still closing records into them without
  harm. The batch factory of part 1 still closes its own. The single-row client factory cannot: the
  staged committer mints one factory per application profile and gives them one shared set, because
  a set per factory would register the same names twice on the committer's group.
- **An enumerator's group outlives the enumerator, so its set belongs to the source.** In Flink
  2.2.1, `SourceCoordinatorContext.metricGroup()` returns a proxy over the holder's one
  `enumerator` subgroup, which a coordinator reset after a global failover reuses, and
  `AbstractMetricGroup.addMetric` drops a second registration of a name while the first stays
  reported (read from source). A set per enumerator would therefore report the first enumerator's
  totals after a reset, with nothing recording into them. `EnumeratorClientMetrics` registers once
  per source object, which the default scheduler keeps across resets, and hands every later
  enumerator the same set. The adaptive scheduler does not: it builds a new execution graph at each
  restart or rescale, and `OperatorCoordinatorHolder.create` deserializes a new source object, while
  `JobManagerJobMetricGroup.getOrAddOperator` returns the same operator group; there the series
  stop after the first graph, which [#1908] carries. Measured on 2026-10-27 in the same MiniCluster
  scan job with `jobmanager.scheduler: adaptive`: one task failure logged a `Name collision` for
  each of the ten `rpcMethod.SampleRowKeys` leaves, beside the connector's enumerator counters and
  Flink's own `unassignedSplits`; under the default scheduler the same failure reset no coordinator
  and logged none. The connector's own enumerator counters (`splitsAssigned` and the rest) register per
  enumerator and freeze the same way, which predates this change and is [#1908]: measured on
  2026-10-27 through a global failover in a MiniCluster job, `rowKeySamplesTaken` reported 1 while
  the shared `rpcMethod.SampleRowKeys.operations` reported both enumerators' 2.
- **The `FULL` lookup cache has no metric group to register on.** Flink 2.2.1's
  `InputFormatCacheLoader` clones the input format per reload and calls only `configure(...)` and
  `open(split)` on it, with no runtime context. The format applies the Cloud Monitoring option and
  exports nothing to Flink. The owner chose to accept and document the option there rather than
  refuse it, because one `CREATE TABLE` serves the scan and the lookup alike.
- **The session path is the client's to fix, not the connector's.** Single-row reads can take the
  session path that ignores the user recorder when the built-in exporter is off: the lookups'
  `ReadRow`, and a scan whose range is one row key, because `readRowsCallable` hands a single-point
  query to `createPointReadCallable`, which the session shim decorates and records as `ReadRow`.
  The scan reader registers only `ReadRows`, so with the exporter on such reads are dropped with
  one `WARN`; registering ten series per reader for a path that is off by default was not worth
  it. Nothing calls `MutateRow`. The client takes that path only when the
  service's client configuration sets a non-zero `session_load`, and the configuration the client
  ships sets 0. Forcing the classic path would take the `@InternalApi` `setSessionsEnabled(false)`;
  the owner declined a workaround on 2026-10-27 because the defect is the client's, and the docs
  state the effect instead. It was not measured against the service.

The registered names are held against the real client: emulator tests run the scan source, the
conditional sink, the read-modify-write helper and a point lookup with the export on, and assert
that `ReadRows`, `SampleRowKeys`, `CheckAndMutateRow`, `ReadModifyWriteRow` and `ReadRow` move and
that no recording is dropped as an unregistered method. Renaming the lookup's method made its test
fail. The emulator implements neither Change Streams RPC, so `ReadChangeStream` and
`GenerateInitialChangeStreamPartitions` rest on the client's span names in 2.85.0
(`EnhancedBigtableStub.getSpanName`).

The Table layer keeps no copy of either default ([ADR-0139]): the dynamic sink and sources hold
the two options as `@Nullable Boolean` and call a builder's setter only for an option the DDL set,
so a builder's default reaches SQL unchanged. The lookups and the SQL async functions have no
builder; they resolve an unset option to `BigtableDataClients`'s two default constants, which every
builder also starts from. The sink side carried the copied defaults since [#1902] and is corrected
here too.

The seams take the client metrics through a defaulted `useClientMetrics`, unlike the abstract
`useCredentials`: an implementation that skipped credentials would reach Bigtable as the process's
default identity, while one that skips client metrics only leaves its series at zero.

## Alternatives declined

- **The OpenTelemetry SDK with a pull reader**, this change's first implementation. It gives
  aggregation, attribute views, validation and a coherent count-and-sum snapshot for free, and
  Bigtable and Spanner already carry the SDK. It was declined by the owner on 2026-10-25, after two
  independent consultations split between the options, because everything it adds is machinery to
  read back what it aggregated, and the clients' recording surface is small enough to implement
  directly. The cost accepted is owning the API interfaces `ClientMetrics` implements: OpenTelemetry adds
  methods to them as defaults, and a client upgrade that records through a new instrument kind is
  dropped with a `WARN` until `ClientMetrics` maps it.
- **Register on first sight.** It reports only methods that occur and needs no method list, and it
  races a reporter's iteration as measured above.
- **One option that replaces the built-in export with the Flink export.** The client offers no such
  switch; the two are independent there, and a user may want either, both or neither.
- **Percentiles, buckets or extremes.** A bucket per series multiplies every series, and a
  percentile or extreme from cumulative state is a gauge that cannot be rated; count and sum keep
  every series a counter.

- **`Sdk` names, and an `enabled` pair of options.** The first names (`SdkMetricsBridge`,
  `sdkMethod`) used this repository's word for the client library, which reads as the OpenTelemetry
  SDK beside an OpenTelemetry implementation that is not one. `client-metrics.enabled` with
  `client-metrics.built-in.enabled` was proposed in review and declined: with the first off, the
  client's metrics are still enabled, to Cloud Monitoring, and the pair reads as a master switch
  over a sub-switch, which they are not. The vendor word "built-in" stays in the description of the
  Cloud Monitoring option (owner's decision, 2026-10-26, after two independent consultations).

[#712]: https://github.com/flink-gcp/flink-connector-gcp/issues/712
[#713]: https://github.com/flink-gcp/flink-connector-gcp/issues/713
[#714]: https://github.com/flink-gcp/flink-connector-gcp/issues/714
[#715]: https://github.com/flink-gcp/flink-connector-gcp/issues/715
[#1815]: https://github.com/flink-gcp/flink-connector-gcp/issues/1815
[#1902]: https://github.com/flink-gcp/flink-connector-gcp/issues/1902
[#1903]: https://github.com/flink-gcp/flink-connector-gcp/issues/1903
[#1908]: https://github.com/flink-gcp/flink-connector-gcp/issues/1908
[ADR-0010]: 0010-pubsub-sink-metric-placements.md
[ADR-0038]: 0038-metric-names-are-per-connector-inventories-in-one-shape-without-flinks-num-prefix.md
[ADR-0139]: 0139-an-option-description-never-restates-a-default.md
