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
  still a workaround, not a fact about BigQuery, and the upstream status is **per deviation**
  (refined 2026-08-09, closing [#326] into upstream reports): the name form is
  [goccy/bigquery-emulator#342](https://github.com/goccy/bigquery-emulator/issues/342) — fixed
  upstream by [goccy/bigquery-emulator#491](https://github.com/goccy/bigquery-emulator/pull/491)
  (merged 2026-06-14) but **unreleased**: v0.8.1, shipped 2026-06-13, is still the latest as of
  2026-08-09 — but that
  fix does **not** cover the status code, measured 2026-08-09 against goccy main; the `UNKNOWN`
  is [goccy/bigquery-emulator#504](https://github.com/goccy/bigquery-emulator/issues/504), fix
  proposed as [goccy/bigquery-emulator#506](https://github.com/goccy/bigquery-emulator/pull/506);
  and the dropped appends need no report of their own — the mechanism is located (2026-08-09, in
  v0.8.1's `appendRows`): a follow-up request carries no stream name, and 0.8.1 resolved the
  empty name to an *arbitrary* entry of the global stream map, right only while that map held a
  single stream, so the ACK went to a stream the rows did not; the same unreleased #342 fix binds
  follow-ups to the connection's first-named stream. So the branch retires piecewise: the
  `UNKNOWN` rewrite goes when a release carries the
  [goccy/bigquery-emulator#504](https://github.com/goccy/bigquery-emulator/issues/504) fix —
  `BigQueryEmulatorMissingTableDeviationITCase` pins that deviation, so the image bump delivering
  it fails the build instead of leaving the rewrite to rot silently — while the priming and the
  no-pool guard go with a released
  [goccy/bigquery-emulator#342](https://github.com/goccy/bigquery-emulator/issues/342) fix,
  re-verifying pooled multi-append behaviour on that bump before the pool is enabled against an
  emulator. A buffered-path emulator round trip additionally needs
  [goccy/bigquery-emulator#505](https://github.com/goccy/bigquery-emulator/issues/505) (request
  offsets ignored, no flush cursor). **A new goccy release — or any of those upstream issues
  closing — is the event that moves this**: bump the pinned image
  (`BigQueryEmulatorContainers`), let the canary and the emulator suite report which deviations
  remain, and update this paragraph's upstream status in the same change — the bump work is
  tracked by [#419].
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
[#326]: https://github.com/laughingman7743/flink-connector-gcp/issues/326
[#419]: https://github.com/laughingman7743/flink-connector-gcp/issues/419
