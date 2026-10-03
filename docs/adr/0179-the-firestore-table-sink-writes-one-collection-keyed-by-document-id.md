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

# ADR-0179: The Firestore table sink writes one collection keyed by document id

- Status: Accepted
- Date: 2026-10-04 (client library facts read in google-cloud-firestore 3.49.0 through
  libraries-bom 26.90.0; emulator behavior measured 2026-10-04 against the pinned
  `google-cloud-cli` emulators image, one run)
- Issues: [#1607], [#1544], [#355], [#1556]
- Modules: firestore (`io.github.flink.gcp.connector.firestore.table`, `table.sink`); base (`lineage.internal`); flink-sql-connector-gcp-firestore
- Current behavior: `docs/content/docs/connectors/table/firestore.md`

## Context

[#355]'s design comment settled the outline of the Firestore Table API: one factory, identifier `firestore`, mapped onto the DataStream builders with the option-to-setter parity test (ADR-0133) and no restated defaults (ADR-0139); the PRIMARY KEY as the document id within `collection`; no PRIMARY KEY as append-only creates; a type mapping with `GeoPoint` and references named by schema-side markers; `sink.write-mode` = `set` | `merge` | `update`; and the shaded `flink-sql-connector-gcp-firestore` that the Datastore-mode factory ([#1545]) later joins.
[#1544] was split into [#1606]–[#1609] on 2026-10-03 at the owner's request. [#1606] added `FirestoreDocumentReference` to the sink's value vocabulary so a reference column can be written; this record covers the factory, the sink direction and the uber jar ([#1607]). The scan and lookup directions are [#1608] and [#1609].

## Evidence

- Spanner's table connector names its both-direction type markers `schema.json-field-paths` and `schema.uuid-field-paths`; BigQuery's sink-only JSON marker is `sink.json-field-paths`, whose path grammar names a ROW field by `.name` and a map value by `value`.
- Flink's `CatalogPropertiesUtil.deserializeOptions`, which rebuilds a table's options from a catalog that persists them as properties, drops every option whose key starts with `schema` (flink-table-common 2.2.1: `!key.startsWith(SCHEMA)`) or with `schema.` (1.20.4), because those properties carry the serialized schema. A `schema.`-prefixed connector option therefore disappears from a table stored in such a catalog, without an error.
- The DataStream sink's `FirestoreWriterOptions` checks two pairs when built (rates without throttling, an initial rate above the maximum), and the sink's builder checks the `retry*` pairs; all three messages name builder setters. `FactoryUtil` wraps a factory's exception in a generic message, so the setter names would be all a SQL user saw of the cause.
- The client library's `CollectionReference.add` mints a document id of 20 characters from letters and digits; a write needs the full document path, so a table without a key mints its own ids.
- A `SET` and a `DELETE` of one document, sent by one sink subtask in that order through `FirestoreTableSinkITCase`'s first draft, left the document in place against the emulator: the `BulkWriter` reorders writes to one document, as ADR-0171 records.

## Decision

- **A table is one collection, and the PRIMARY KEY is the document id.** `collection` is a collection path with an odd number of segments, each a path component (ADR-0127); a path naming a document is refused under the option key. The key is exactly one `STRING` column and is never stored as a field. A key that is empty or holds `/` fails the record, because composing it would name another document, or a document in another collection, without an error.
- **No PRIMARY KEY means inserts only, each a `SET` under an id the serializer mints**, 20 characters from letters and digits drawn with `SecureRandom`, the library's own shape. #355 had said `CREATE`; round one of the review found that the client library retries a `BatchWrite` refused with `UNAVAILABLE` (`FirestoreStubSettings`, 3.49.0), so a create whose answer was lost would come back `ALREADY_EXISTS`, which the sink routes and the table's handler turns into a job failure. A `SET` under a fresh id makes the same new document and applies again. A replay after a restart writes another document; the docs say so.
- **With a PRIMARY KEY the sink takes an upsert changelog.** `sink.write-mode` chooses what an insert or update-after is: `SET`, `SET_MERGE` or `UPDATE` with every non-key column; a delete is a `DELETE` of the key's document. On Flink 2.x the declared changelog is `upsert(true)`, so a delete may carry the key alone. `sink.write-mode` on a table without a key is refused, and `update` on a table with no column besides the key is refused. **An `update` table declares inserts and updates-after but no deletes**, so the planner refuses an input that carries deletes: the sink keeps neither one document's write order nor an earlier application across a replay, so an update could meet the document a delete removed, and `NOT_FOUND` on an update fails the job on every restart. The independent review found, and a probe measured on the emulator, that the planner still sends a delete when it materializes an upsert whose key differs from the table's (`upsertMaterialize=[true]`; an aggregation keyed by its count deleted both pre-existing documents). Such a delete therefore fails the record, and so the job, without deleting anything, with a message naming the two remedies. The option is table-owned and carries its `set` default (the recorded ADR-0139 exception).
- **The type mapping is closed and checked when the statement is planned** (`FirestoreTableSchema`): `STRING`/`VARCHAR`/`CHAR`, `BIGINT`, `DOUBLE`, `BOOLEAN`, `BYTES`/`VARBINARY`/`BINARY` as a `Blob`, `TIMESTAMP_LTZ` as a `Timestamp`, `ARRAY` (not directly inside another), `ROW` and `MAP<STRING, …>` as maps. Every other type is refused naming the field. A SQL `NULL` is written as a Firestore null. Column and ROW field names are literal field names, as `FirestoreWrite` makes them; a name starting and ending with `__`, which Firestore reserves, is refused at planning, because the service would refuse every document (ADR-0127's per-record class). A MAP's keys are data and stay the service's.
- **Two markers name the values that share a Flink type with an ordinary one**: `geo-point-field-paths` for a `ROW<latitude DOUBLE, longitude DOUBLE>` written as a `GeoPoint`, and `reference-field-paths` for a `STRING` written as a `FirestoreDocumentReference`. The keys carry no prefix because they describe the schema in both directions once the scan reads them ([#1608]); Spanner's `schema.` prefix was declined for the reason in the Evidence, which would lose a marker from a persisting catalog and, Firestore being schemaless, write a point as a plain map and a reference as a plain string without an error. `FirestoreConnectorOptionsTest` holds that no key starts with `schema`. The path grammar is BigQuery's, with an `ARRAY` passed through so a path names its elements. A marker on an unknown path, on a field of the wrong type, on the key, on a path two fields share (a literal `a.b` column beside a ROW `a` with a field `b`), or both markers on one path, is refused.
- **Every writer knob maps through `OptionSetters`, and the cross-checks are restated in option keys**: the two throttling checks in `WriterOptionsMapper`, and the retry pair check through a `retrySettings` overload that takes the names to report, as the Pub/Sub table sink calls its pair check. A `WITH` clause cannot name a serializable failure handler, so the sink keeps the DataStream default of failing the job, and `maxConsecutiveRejections`, which only bounds a dropping handler, has no option. No table write carries a precondition.
- **Same-document order is not kept**, as in the DataStream sink and the Spanner and Bigtable table sinks; the docs say when it matters (an input that changes a key more than once). Keeping it is the opt-in mode of [#1556], left in v1.3.0 (owner's decision, 2026-10-04).
- **The sink reports a `firestore-collection` lineage resource**, named by the collection path relative to the database, through `Lineage.tableSink` under the table's catalog identifier (ADR-0160).
- **`flink-sql-connector-gcp-firestore` bundles the whole module, both package roots unrelocated** (ADR-0170, ADR-0015). Beyond the Cloud Tasks module's relocations it relocates `io.opentelemetry` and `com.fasterxml.jackson`, and it excludes `slf4j-api` (ADR-0035) and `javax.annotation-api` (ADR-0015); `org.checkerframework` stays unrelocated as annotation-only, as in the Bigtable jar.

## Consequences

- An aggregation or any input that updates one key repeatedly can leave an older value in the document until [#1556] lands; the table docs carry the warning beside the changelog section.
- A refused write, including an `update` of a missing document, fails the job: the table layer has no dead-letter path.
- A table without a key can leave duplicate documents after a restart.
- Reading a `firestore` table is refused at planning until [#1608]; the factory implements only the sink factory interface.
- The release publishes six SQL uber jars rather than five; `collect-release-jars.sh` and the lineage class-loading measurement count six.

## Alternatives declined

- **Detecting a geographical point from the ROW's shape alone**, without a marker. A map with `latitude` and `longitude` keys is an ordinary Firestore value too, and a write could not tell the two apart.
- **Accepting `INT`, `FLOAT` and the other narrower types by widening them.** A read could not narrow them back without a range check per value, and one mapping in both directions is what the markers already assume; `BIGINT` and `DOUBLE` are what the service stores.
- **Mapping `DECIMAL`.** Firestore has no decimal type ([#355]).
- **Refusing edge whitespace in a document id.** Unlike a configured name, the key is data, and Firestore accepts such ids; only the two values that would silently address another document are refused.
- **Accepting deletes under `update`**, documenting the restart loop. A job that fails again after every restart until an operator recreates a document is worse than a statement refused at planning, and `set` or `merge` take deletes.
- **Pulling the per-document order of [#1556] into this sink** (owner's decision, 2026-10-04: document the limit, keep [#1556] in v1.3.0).

[#355]: https://github.com/flink-gcp/flink-connector-gcp/issues/355
[#1544]: https://github.com/flink-gcp/flink-connector-gcp/issues/1544
[#1545]: https://github.com/flink-gcp/flink-connector-gcp/issues/1545
[#1556]: https://github.com/flink-gcp/flink-connector-gcp/issues/1556
[#1606]: https://github.com/flink-gcp/flink-connector-gcp/issues/1606
[#1607]: https://github.com/flink-gcp/flink-connector-gcp/issues/1607
[#1608]: https://github.com/flink-gcp/flink-connector-gcp/issues/1608
[#1609]: https://github.com/flink-gcp/flink-connector-gcp/issues/1609
