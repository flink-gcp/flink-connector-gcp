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

# ADR-0186: Client-library metrics reach Flink reporters through a pull bridge

- Status: Accepted
- Date: 2026-10-25
- Issues: [#713], [#712], [#715], [#714], [#1815]
- Modules: base, bigtable, `scripts/`, root POM
- Current behavior: `docs/content/docs/connectors/datastream/bigtable.md` § Client-library
  metrics; `docs/content/docs/reference/bigtable.md` § `BigtableSink.builder()`

## Decision

A Google Cloud client library's own metrics reach Flink's metric reporters through
`base.metrics.sdk.SdkMetricsBridge`, and a connector adopts it with an adapter of its own. The
design of 2026-08-15 on [#712] stands, with the two changes the evidence below forced. What the
bridge and its first adopter are:

- **The bridge owns a private meter provider and collects only when read.** The connector hands
  the bridge's `OpenTelemetry` to the client's metrics-provider hook; nothing touches
  `GlobalOpenTelemetry`, so a relocated copy in a SQL jar is self-contained. Reading any bridged
  counter collects at most once a second and swaps in an immutable snapshot, so the bridge owns no
  thread and costs nothing when no reporter reads it. A failing collect logs once and keeps the
  previous snapshot. `close()` collects once more, shuts the provider down and freezes every value.
- **Views keep one attribute, the RPC method.** Every other attribute is aggregated away before
  data exists, so a component reports instruments times methods series. Histograms keep their
  count and sum only (no bucket boundaries), and a duration sum is converted to milliseconds from
  `ms` or `s`; under another unit it is not reported. A cumulative monotonic sum is a counter.
  Gauges and non-monotonic sums are not mapped: no adopter in the issue map has one.
- **Every series is registered when the component opens, never during a read.** A connector
  names the RPC methods its component's client calls, and the adapter registers each one's leaves
  as `sdkMethod.METHOD.leaf` on the component's group. A method the client reports without a
  registration is logged once at `WARN` and dropped. This replaces the design's
  register-on-first-sight, and the `sdkSeries` bootstrap gauge goes with it: with every series
  registered up front, nothing waits for a first collect.
- **The adapter lives in the connector and is a `[[subgroups]]` source.** It declares the
  instruments, the leaf constants and the registration, in the same one-`addGroup` shape as
  `ErrorClassCounters`. `check-metric-docs.py` reads its group and leaves and demands a templated
  row for each; the claim in `metric-docs.toml` also keeps its registrations out of the module's
  `*MetricNames` inventory rule.
- **Two independent options, named for a cross-connector spelling.** `exportClientMetricsToFlink`
  (`client-metrics.export-to-flink`, off by default) installs the bridge.
  `exportClientMetricsToCloudMonitoring` (`client-metrics.export-to-cloud-monitoring`, on by
  default) keeps or removes the client's built-in Cloud Monitoring exporter. They are
  connection-level, so the Table keys carry no direction prefix, as `service-account-key-file`
  does not. Spanner ([#712]) and Cloud Tasks ([#715]) reuse both names, Cloud Tasks only the first.
- **Bigtable maps eleven leaves per method** from the public instrument block the client records
  into an injected `OpenTelemetry` (operation, attempt, server, first-response, application and
  throttling latencies, retry count, connectivity error count). Three histograms keep only their
  sum, because the client records them once per operation or once per attempt and their counts
  would repeat `operations` or `attempts`. The at-least-once writer is the first component: its
  batch factory owns the bridge and closes it after its clients. `EXACTLY_ONCE` refuses the
  options, and the single-row writers, sources and lookups take them in #713's second pull
  request.
- **OpenTelemetry's version comes from an `opentelemetry-bom` import** in the root POM, pinned to
  what `libraries-bom` resolves through the clients, because `libraries-bom` manages none of it.
  An enforcer `requireUpperBoundDeps` execution on `opentelemetry-api` and
  `opentelemetry-sdk-metrics` fails a `libraries-bom` bump whose clients ask for a newer version.

[ADR-0010]'s finding that the Pub/Sub client has no metrics to pass through is Pub/Sub's and still
holds at the pinned version; Bigtable and Spanner do ship meters, which is what this per-connector
divergence rests on. Pub/Sub has no adapter and no issue.

## Evidence

- **A registration inside a read breaks a reporter.** Flink 2.2.1's `OpenTelemetryMetricReporter`
  reads its counters by iterating a `HashMap` under `synchronized (this)`, calling `getCount()` on
  each. A bridged counter that registered a new series from inside that call would re-enter the
  reporter's `notifyOfAddedMetric` on the same thread (the monitor is reentrant) and modify the map
  being iterated, so the report fails with `ConcurrentModificationException`. `Slf4jReporter`
  iterates without the lock and swallows that exception, losing the report. `DatadogHttpReporter`
  uses concurrent maps and would survive. Read from the reporter sources on 2026-10-25; OTLP is one
  of the backends this exists for, so registration moved to open time.
- **The bridge runs beside the built-in export, not instead of it.** In `google-cloud-bigtable`
  2.85.0, `BigtableClientContext` creates the built-in Cloud Monitoring exporter whenever
  `areInternalMetricsEnabled()`, independent of the metrics provider; a
  `CustomOpenTelemetryMetricsProvider` adds a user recorder beside it, and `NoopMetricsProvider`
  removes neither. Only `disableInternalMetrics()` removes the exporter, which is why the second
  option exists and why turning it off also removes the off-GCP close stall [#1815] measured.
  [#1815]'s first note named `NoopMetricsProvider` as the switch; the issue carries the correction.
- **The real client records against the emulator.** The emulator builder sets
  `NoopMetricsProvider`, and a provider set after it replaces it. `BigtableClientMetricsITCase`
  writes forty records through the public builder and a scheduled test reporter reads
  `sdkMethod.MutateRows.*` while the job runs; no unregistered method was reported. The emulator
  sends no Google front-end headers, so every attempt counts as a connectivity error and no server
  time is recorded, which the IT pins and the docs state.
- **The checker needed one change.** Its inventory rule demands a `*MetricNames` constant for every
  `.counter(`/`.gauge(` in a mapped module, so an adapter inside a connector module failed it; the
  2026-08-15 design's "zero checker changes" held only for a registrar in base. Claimed
  `[[subgroups]]` sources are now skipped by that rule, with synthetic tests for both directions.
- **The enforcer rule fires.** With `-Dopentelemetry-bom.version=1.57.0`, the Bigtable module fails
  naming `opentelemetry-api` and `opentelemetry-sdk-metrics` and the 1.62.0 the clients asked for;
  the pinned version passes the whole reactor. The plugin's `includes` matched neither a bare group
  nor `group:*` (measured), so it names the two artifacts.

## Alternatives declined

- **Register on first sight, during a collect.** It reports only methods that occur and needs no
  method list, and it breaks the OTLP reporter as measured above.
- **Register on first sight, but on another thread.** Handing the registration to an executor
  avoids the same-thread re-entry, but it gives the bridge the thread it was designed without, and
  `Slf4jReporter`, which iterates without a lock, would still lose a report to it.
- **A `PeriodicMetricReader`.** A thread per component, on an interval that cannot know the
  reporter's, collecting whether or not anyone reads (the 2026-08-15 design's reason, unchanged).
- **One option that replaces the built-in export with the bridge.** The client offers no such
  switch; the two are independent there, and a user may want either, both or neither.
- **Percentiles from the cumulative histograms.** A cumulative histogram has no per-request
  samples, so a percentile would be fabricated; buckets would multiply every series by the bucket
  count.

[#712]: https://github.com/flink-gcp/flink-connector-gcp/issues/712
[#713]: https://github.com/flink-gcp/flink-connector-gcp/issues/713
[#714]: https://github.com/flink-gcp/flink-connector-gcp/issues/714
[#715]: https://github.com/flink-gcp/flink-connector-gcp/issues/715
[#1815]: https://github.com/flink-gcp/flink-connector-gcp/issues/1815
[ADR-0010]: 0010-pubsub-sink-metric-placements.md
