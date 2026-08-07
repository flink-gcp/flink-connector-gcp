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

# ADR-0029: The BigQuery builder takes two emulator endpoints, one per transport

- Status: Accepted
- Date: 2026-08-06 (groundwork for [#287], under [#57])
- Issues: [#57], [#287]
- Modules: bigquery
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md` § Testing

## Decision

`emulatorEndpoint` (gRPC, the Storage Write API) and `emulatorRestEndpoint` (REST,
`BigQueryTableAdmin`). **Two, not one**, and that is the deviation from every sibling connector:
BigQuery serves its transports on separate ports (9050/9060 on the goccy emulator), so a single
value could only point half the sink at the emulator — silently, since a job whose tables all
exist never touches the REST client at all.

- This **reverses the [#15]/[#54] call** that the `@VisibleForTesting
  createWriter(appenderFactory, tableAdmin, metricGroup)` seam was sufficient: the SQL planner
  builds the sink through the production factory and cannot reach a seam — a new trigger rather
  than a re-argued one.
- `FILE_LOADS` **rejects both** in `build()` — it stages to GCS, which no emulator here stands
  in for, so an endpoint would be honored by the metadata half and silently ignored by the half
  that moves the rows.
- The emulator branch lives in `StreamWriterRowAppenderFactory` and carries the three goccy
  deviations the test-only `EmulatorAppenderFactory` used to (the `.../streams/_default` name
  form plus a `GetWriteStream` priming call, `UNKNOWN` instead of `NOT_FOUND`, and no connection
  pool); that class was **deleted** rather than left beside the production branch, so the
  emulator ITs now measure production code and exactly one copy of the workaround exists. It is
  still a workaround, not a fact about BigQuery:
  [goccy/bigquery-emulator#342](https://github.com/goccy/bigquery-emulator/issues/342) is fixed
  upstream but unreleased (v0.8.1 shipped 2026-06-13, the issue closed the day after), so the
  branch goes when a release carries the fix.
- The production, no-endpoint path is untouched — it opens no client at all, drawing connections
  from the SDK's JVM-static pool, which is exactly why an endpoint cannot be applied to it and
  the emulator branch has to build its own client per destination.
- `BigQueryEmulatorEndpointITCase` is the one test that goes through the production
  `createWriter(WriterInitContext)`: every other emulator test injects through the seam, so all
  of them would pass with the endpoints reaching no client at all. `gax-grpc` moved from test to
  compile scope with this.

[#15]: https://github.com/laughingman7743/flink-connector-gcp/issues/15
[#54]: https://github.com/laughingman7743/flink-connector-gcp/issues/54
[#57]: https://github.com/laughingman7743/flink-connector-gcp/issues/57
[#287]: https://github.com/laughingman7743/flink-connector-gcp/issues/287
