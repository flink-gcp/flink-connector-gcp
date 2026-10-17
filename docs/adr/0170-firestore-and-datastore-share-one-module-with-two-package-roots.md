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
- Date: 2026-09-26 (design settled on [#355]); recorded with the first implementation, [#1540];
  change-stream reopen conditions revised by [#1832] (2026-10-18)
- Issues: [#355], [#1540], [#1832]
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

The first cut has no exactly-once sink mode: ADR-0104's gates apply, and [#355] describes the eligible transaction-and-ledger primitive and the condition that reopens it.

### No change-stream source

The first cut also has no change-stream source, because neither API the module calls has a change-stream read.
`google.firestore.v1` has no such RPC, and its `Listen` call reports the state of a target rather than its history: one change it sends may be the result of several writes, deletes included, and a listen resumed after a disconnection may answer with a reset of the target or with a digest saying only that documents may have been removed, which leaves the client to work out which ones from what it holds, or by re-reading the target.
`google.datastore.v1` has no listener at all.
Firestore's Change Streams exist only through the MongoDB-compatible API of an Enterprise-edition database, in Preview since their release on 2026-04-20.
A stream is created ahead of time, in the console or with `gcloud alpha firestore change-streams create` for a database or a collection group, keeps one to seven days of changes, fixed at creation, and opens as a MongoDB `$changeStream` cursor that `firestoreWorkerConfig` splits across parallel workers.
Reading one takes a third client family, the MongoDB Java driver (`mongodb-driver-sync`, Apache-2.0), beside the two client libraries the module calls.
Apache Beam offers no reader to follow: `FirestoreIO` reads only through bounded RPCs, and `MongoDbIO` has no change-stream read ([apache/beam#23551] is open).
Until the question reopens, changes reach Flink through Pub/Sub, as the DataStream page describes: an Eventarc trigger invokes a Cloud Run function that publishes each event to a topic, which this repository's Pub/Sub source reads.

Either of two conditions reopens it:

1. A change-stream read appears on `firestore.v1` or in java-firestore.
2. Change Streams reach general availability, in any edition.
   The condition settled on [#355] also required the Enterprise-only restriction to be lifted.
   The owner dropped that clause on 2026-10-18 ([#1832]), because the connector already supports behaviour that only an Enterprise-edition database has: an array directly inside an array ([#1819]) and the BSON value types ([#1589]).

Reopening under the second condition starts a design of its own, from the questions the Preview documentation leaves open: whether a resumed parallel cursor can change its worker count, which matters to a rescaled job, and whether a document's changes stay with one worker, and in order, when it does (the documentation orders a stream by modification time and gives each worker a distinct set of documents, which settles the order only while the assignment is fixed); whether a start position exists that hands a bounded snapshot over to the stream; and whether the emulator serves change streams.
The `mongodb-to-mongodb` template in Google's [DataflowTemplates] reads MongoDB change streams with a resume token per partition, `startAtOperationTime` and hash-partitioned cursors; it is a design reference, untested against Firestore.

Status last checked on 2026-10-18: Change Streams are in Preview, and `google/firestore/v1/firestore.proto` has no change-stream RPC.

## Consequences

- One uber jar serves both surfaces, so the module adds one shaded artifact per version line to each release; a second module would add two, and the uber jars are the bulk of what a release publishes.
- A Datastore-only job carries the Firestore client library on its classpath and the reverse. Both libraries share their heavy transitive set (gax, gRPC, protobuf), so the cost is the two thin client layers.
- The module's emulator harness serves both roots: one `gcloud emulators firestore` binary runs in Native mode by default and in Datastore mode under `--database-mode=datastore-mode`.

## Alternatives declined

- **A sibling `flink-connector-gcp-datastore` module.** The only thing it buys is keeping one client library off the other surface's classpath, and it costs a second uber jar per version line, a second set of docs pages and checker entries, and a second CI lane, for two surfaces that are one product with one IAM permission family (`datastore.*`) and one emulator.
- **An unbounded source over `Listen`.** It would report state, not change data capture: a change may stand for several writes, and after a restart a delete made while the job was down may arrive only as a digest from which the job has to work out which documents are gone.
- **One package root with a mode switch.** The two APIs have different value models (`Map` values and document paths against `Entity` and `Key`), different write RPCs (`BatchWrite` with per-write status against a non-transactional `Commit` without one) and different split mechanisms, so a shared surface would be the union of two designs rather than a simplification of either.

[#355]: https://github.com/flink-gcp/flink-connector-gcp/issues/355
[#1540]: https://github.com/flink-gcp/flink-connector-gcp/issues/1540
[#1589]: https://github.com/flink-gcp/flink-connector-gcp/issues/1589
[#1819]: https://github.com/flink-gcp/flink-connector-gcp/issues/1819
[#1832]: https://github.com/flink-gcp/flink-connector-gcp/issues/1832
[apache/beam#23551]: https://github.com/apache/beam/issues/23551
[DataflowTemplates]: https://github.com/GoogleCloudPlatform/DataflowTemplates/tree/main/v2/mongodb-to-mongodb
