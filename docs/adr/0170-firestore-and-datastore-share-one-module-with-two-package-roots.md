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

# ADR-0170: Firestore and Datastore share one module with two package roots

- Status: Accepted
- Date: 2026-09-26 (design settled on [#355]); recorded with the first implementation, [#1540]
- Issues: [#355], [#1540]
- Modules: firestore
- Current behavior: `docs/content/docs/connectors/datastream/firestore.md` § Scope and provenance

## Context

[#355] brought both Firestore API surfaces into scope.
A database is created either in Native mode, reached through the `firestore.v1` API (in Java, `google-cloud-firestore`), or in Datastore mode, reached through the `datastore.v1` API (in Java, `google-cloud-datastore`), which refuses Firestore API requests.
Legacy Cloud Datastore is not a third target, because Google upgraded every legacy database to Firestore in Datastore mode and the Datastore API is how those databases are reached.
The question the design had to answer first was how the repository holds the two surfaces, because every later sub-issue depends on it.

## Decision

**One Maven module, `flink-connector-gcp-firestore`, with two package roots**, and later one shaded `flink-sql-connector-gcp-firestore`:

- `io.github.flink.gcp.connector.firestore` for Native mode;
- `io.github.flink.gcp.connector.datastore` for Datastore mode.

Each root follows the ADR-0055 layout on its own (`sink`, `source`, `table`, a root database value and a `*MetricNames` inventory), and the two share nothing beyond `flink-connector-gcp-base`.
A helper moves into a shared place only when its second consumer exists.
Each root carries its own `CrossVersionSink` under the per-major source roots.
Docs pages are per module, with the Datastore-mode material joining the Firestore pages when it lands.

The public types of both roots enter at `@PublicEvolving` under ADR-0141's youth clause: the surfaces have not survived a release.

Two further first-cut scope decisions were settled on [#355] and are recorded there with their reopen conditions: no change-stream source (neither API has a change-stream read; Firestore's change streams are documented only for Enterprise edition, through the MongoDB-compatible API, in Preview, as checked on 2026-09-26), and no exactly-once sink mode (ADR-0104's gates; the eligible transaction-and-ledger primitive is described on [#355]).

## Consequences

- One uber jar serves both surfaces, so the module adds one shaded artifact per version line to each release; a second module would add two, and the uber jars are the bulk of what a release publishes.
- A Datastore-only job carries the Firestore client library on its classpath and the reverse. Both libraries share their heavy transitive set (gax, gRPC, protobuf), so the cost is the two thin client layers.
- The module's emulator harness serves both roots: one `gcloud emulators firestore` binary runs in Native mode by default and in Datastore mode under `--database-mode=datastore-mode`.

## Alternatives declined

- **A sibling `flink-connector-gcp-datastore` module.** The only thing it buys is keeping one client library off the other surface's classpath, and it costs a second uber jar per version line, a second set of docs pages and checker entries, and a second CI lane, for two surfaces that are one product with one IAM permission family (`datastore.*`) and one emulator.
- **One package root with a mode switch.** The two APIs have different value models (`Map` values and document paths against `Entity` and `Key`), different write RPCs (`BatchWrite` with per-write status against a non-transactional `Commit` without one) and different split mechanisms, so a shared surface would be the union of two designs rather than a simplification of either.

[#355]: https://github.com/flink-gcp/flink-connector-gcp/issues/355
[#1540]: https://github.com/flink-gcp/flink-connector-gcp/issues/1540
