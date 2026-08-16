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

# ADR-0016: The BigQuery sink is a `BigQueryIO`-style facade with per-write-method options

- Status: Accepted
- Date: 2026-07-19 ([#13], [#14]); option scoping settled in [#14] (deferred on PR
  [#46](https://github.com/laughingman7743/flink-connector-gcp/pull/46))
- Issues: [#13], [#14], [#10]
- Modules: bigquery
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md`

## Decision

- One builder, per-write-method SinkV2 implementations. Storage Write API connection
  multiplexing is delegated to the client SDK connection pool (`setEnableConnectionPool`); no
  self-built keyed writer pool.
- The serializer SPI is an abstract class (`BigQueryProtoSerializer`) with
  `getDescriptor(TableDestination)` + `ByteString` rows — not a functional interface
  (descriptors are not Java-serializable). `serialize` returning null skips the record in all
  three writers (ADR-0001) — which does **not** loosen the eager-derivation rule: a schema
  problem must still not surface from `serialize()`, and returning null instead of throwing
  would hide it *better* than the trap that rule exists for, since a skip is not routed anywhere
  and is invisible outside `recordsSkipped`.
- **Per-write-method option scoping**: write-method-only options live in a nested immutable
  options object set on the builder (`FileLoadsOptions` via `fileLoadsOptions(...)`,
  `BufferedStreamOptions` via `bufferedStreamOptions(...)`); `build()` requires it for its write
  method and rejects it for others. (The default-stream object is the one deliberate deviation —
  ADR-0028.)
- Deferred: `location()` granularity — recorded on PR
  [#46](https://github.com/laughingman7743/flink-connector-gcp/pull/46), decided in [#10]. The
  load-job half is answered by ADR-0018's [#491] revision: per destination, derived from each
  job's destination dataset when `location()` is unset.

[#10]: https://github.com/laughingman7743/flink-connector-gcp/issues/10
[#13]: https://github.com/laughingman7743/flink-connector-gcp/issues/13
[#14]: https://github.com/laughingman7743/flink-connector-gcp/issues/14
[#491]: https://github.com/laughingman7743/flink-connector-gcp/issues/491
