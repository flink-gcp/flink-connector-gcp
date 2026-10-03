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

# ADR-0175: The Datastore sink commits batches and confirms a refusal one write at a time

- Status: Accepted
- Date: 2026-10-03 (client library facts read in google-cloud-datastore 3.4.0 through
  libraries-bom 26.87.0 and found unchanged in 3.7.0 through 26.90.0; emulator behavior measured
  2026-10-03 against
  `google-cloud-cli:587.0.0-emulators` in Datastore mode, one run)
- Issues: [#1542], [#355], [#1546]
- Modules: firestore (`io.github.flink.gcp.connector.datastore`: `sink`, `sink.writer`)
- Current behavior: `docs/content/docs/connectors/datastream/firestore.md` § Datastore mode

## Context

[#355]'s design comment settled the shape of the Datastore-mode sink: a serializer returning an upsert, insert or update over a client-library entity with a complete key, or a delete over a key; non-transactional `Commit` batches bounded by count and bytes; a retry loop the sink owns; ramp-up throttling on by default; and solo confirmation of a row-level status, because a `Commit` reports no outcome per mutation.
It left the confirmation that the client library's types serialize, the routed statuses' exact conditions, and the write path's client surface to this implementation.
The Native-mode sink (ADR-0171) shares the module but nothing else (ADR-0170).

## Evidence

Read from google-cloud-datastore 3.4.0, and unchanged in 3.7.0 (the files below compared identical):

- `Datastore.newBatch()` sends every write it holds in one `CommitRequest` in `NON_TRANSACTIONAL` mode, mixing inserts, upserts, updates and deletes. A second write to a key it already holds is merged into the first or refused on the client (`BaseDatastoreBatchWriter`), never sent.
- The per-operation methods (`put`, `add`, `update`, `delete`) each send a commit of one operation; `put`, `update` and `delete` deduplicate a repeated key silently, and `add` refuses one.
- `DatastoreImpl` runs every call through `RetryHelper` with the client's `RetrySettings`, retrying `ABORTED`, `DEADLINE_EXCEEDED` and `UNAVAILABLE`, and `GrpcDatastoreRpc` applies the same settings to every generated call setting. One single-attempt setting therefore turns off both layers.
- The client takes its endpoint from `DATASTORE_EMULATOR_HOST` only when built without a host (its telemetry reads the variable too, to turn client-side metrics off, which does not move the endpoint), and switches to plaintext with no credentials when its credentials are `NoCredentials`.
- The `datastore.v1` proto requires that "no two mutations may affect a single entity" in a `NON_TRANSACTIONAL` commit.
- `Datastore.get` re-sends a `Lookup` for as long as the service answers with the key deferred, with no bound (`ResultsIterator.loadResults`), so neither a call timeout nor a retry budget bounds it. The client's RPC object (`ServiceOptions.getRpc()`, a `DatastoreRpc`) sends one `Lookup` under the client's own call settings.
- `FullEntity`, `Key` and every `Value` are `Serializable`. Their protobuf conversions (`toPb`) are package-private; `Entity.calculateSerializedSize` is public.
- The `datastore.v1` proto documents a non-transactional commit as one whose "mutations may not apply as all or none", and its `database_id` fields refuse `(default)` in favor of an empty id.

Measured against the emulator, each shape sent alone and, where the table says so, inside a commit with valid upserts:

| Write | Status | Rest of the commit |
|---|---|---|
| `insert` of an existing key | `ALREADY_EXISTS` | not applied |
| `update` of a missing key | `NOT_FOUND` | not applied |
| an indexed string over 1,500 bytes, a reserved kind or property name | `INVALID_ARGUMENT` | not applied |
| an entity over 1 MiB | `INVALID_ARGUMENT` | **the write before it applied**, the write after it not (two runs) |
| a key of another project or database | `INVALID_ARGUMENT` ("mismatched databases within request") | not applied |
| `delete` of a missing key | applied | |
| a commit of about 10.5 MiB | applied | the emulator does not enforce the 10 MiB request limit |
| a lookup of a missing key | answered | |
| an update and a lookup against a database id that was never created | `NOT_FOUND`, answered | the emulator serves any database id |

The oversized-entity row is the proto's "may not apply as all or none" observed rather than read: the emulator applied the request's mutations in order and stopped at the refused one, so the write ahead of it was in the database when the refusal arrived. The request's order is not the writer's: the client's `Batch` groups mutations by operation (inserts, updates, upserts, deletes; `BaseDatastoreBatchWriter.toMutationPbList`), so which writes of a refused commit were applied depends on their operations as well as their positions. That is what the confirmation pass is built for: every write of a refused commit is re-sent alone, and a write the commit had already applied is applied again, which is safe for `upsert`, `update` and `delete` and surfaces as a routed `ALREADY_EXISTS` for an `insert`.

## Decision

### The write path

- **The SPI carries the client library's own types**: `DatastoreMutation` holds a `FullEntity<Key>` or a `Key`, and its factories take `FullEntity<Key>` so an incomplete key is a compile error, with a runtime check for an erased or raw caller. That they serialize was confirmed by a round trip of every value type; `FailedMutation`'s payload is the Java-serialized write, the Native-mode sink's choice for the same reason (no public protobuf conversion).
- **The writer commits through `Batch`**, one request at a time on the task thread, with no mailbox: the Spanner sink's synchronous shape (ADR-0075). It commits before a write to a key the buffer already holds, so `Batch` never merges or refuses one, and a commit never names a key twice. A consequence the docs state: one subtask's writes to one key are applied in submission order, retries and solo re-sends included, with one exception the writer cannot see: an attempt that timed out on the client may still be applied by the service after a later commit.
- **Batches are bounded by `maxBatchMutations` (500) and `maxBatchBytes` (9,000,000, at most 10 MiB)**: 500 is Apache Beam's ceiling on its adaptive batch size (Beam starts at 50) and 9,000,000 bytes Beam's byte threshold; Datastore documents no count for a non-transactional commit, so the count is the connector's choice. A mutation's size is its protobuf size: the client library's entity size plus the framing of the mutation and request fields, computed by `MutationSizeEstimator`; the request's own project, database and mode fields count against the cap too, so the 10 MiB ceiling holds for the whole request.
- **A key must address the sink's project and database**, checked before the write joins a batch and routed when it does not, because the service refuses the whole commit for it. A key whose database id is null (the client library does not refuse one) is routed the same way.
- **The writer owns the retry loop** (ADR-0075 shape): the client makes one attempt per call, bounded by `requestTimeout` (60 s, the generated commit setting's own timeout), and a commit failing with `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `ABORTED` or `RESOURCE_EXHAUSTED` is re-sent whole on the `recovery*` schedule. `INTERNAL` is not retried, as on the Native-mode sink.
- **The writer re-checks a deserialized options object** through the builder's own setters, the batch bounds, the request timeout and the rejection bound alike (ADR-0068 leaves the re-check per connector).
- **A confirmation pass stops at an interrupt** before its next solo commit, because the client's blocking call does not observe one.
- **Ramp-up throttling is on by default**, re-implemented on Beam's `RampupThrottlingFn` shape: 500 operations per second shared by `throttlingParallelism` subtasks (the sink's parallelism unless set), growing by half for every five minutes past the first five since a subtask's first write (Beam measures from one job-wide instant and hints 500 workers by default), one permit per record in `write()`, on a monotonic clock; retries and solo re-sends are not throttled.

### Routing

- **A commit refused with a status one of its writes could have earned is confirmed one write at a time** (ADR-0045 shape): `INVALID_ARGUMENT` for any commit, `ALREADY_EXISTS` only beside an insert, `NOT_FOUND` only beside an update. Each write is re-sent as a commit of its own, in order, with its own retry budget. A write applied alone was collateral; a write refused alone is routed if the status is one its operation can earn. A commit of one write is its own confirmation. Every other refusal of a commit fails the job without routing.
- **`NOT_FOUND` is routed for an update only after a lookup of the same key is answered**, sent as one `Lookup` through the client's RPC object rather than `Datastore.get`, whose deferral loop is unbounded; any answer, a deferral included, shows the database is there. The status does not say whether the entity or the database is missing; a missing entity is an ordinary lookup answer, while a missing database is expected to refuse the lookup too, which then fails the job. The owner chose this discriminator over routing on the status alone and over never routing (the Native-mode sink's choice, ADR-0171) on 2026-10-03. The expectation is not measured: the emulator serves any database id, and [#1546] owes the measurement.
- **`maxConsecutiveRejections` (100) counts confirmed `INVALID_ARGUMENT` refusals and keys addressing another database**, the latter being what the service would refuse with `INVALID_ARGUMENT` had the writer sent it (round two found that leaving it uncounted let a misconfigured sink shed its whole stream under a dropping handler). `ALREADY_EXISTS` for an insert and `NOT_FOUND` for an update are what a restart's replay answers (a replayed insert; a replayed update of an entity the stream deleted afterwards), the ADR-0171 reasoning for leaving replay answers out.
- Classification reads the cause chain both ways (ADR-0042): a transient status anywhere wins; otherwise the outermost status decides.

### Names

The owner settled three names on 2026-10-03 under ADR-0137, departing from the issue's working names:

- **`DatabaseDestination`**, in the Datastore root, rather than `DatastoreDatabase`: ADR-0137 renamed Spanner's `SpannerDatabase` for naming nothing a `*Destination` does not. It shares a simple name with the Native-mode type in the same module; the two differ in the default database's spelling (empty here, `(default)` there), which is why they are two types.
- **`DatastoreMutation`**, `DatastoreMutationSerializationSchema` and `FailedMutation#getMutation()`, rather than a `DatastoreWrite` beside a `FailedMutation`: one noun per write path, and at an SDK seam the vendor's word, which here is the Datastore API's `Mutation` (the Spanner sink's vocabulary, so `failedMutationHandler`, `maxBatchMutations`, `bufferedMutations` and `mutationsRetried` carry over unchanged; `mutationsConfirmedAlone` is new here).
- **`requestTimeout`** rather than `commitTimeout`: the bound covers every call the writer makes, the lookup included, the Bigtable and Cloud Tasks spelling for such a bound.

The public types enter at `@PublicEvolving` (ADR-0170).

## Consequences

- A refused commit costs one solo commit per write, sequentially on the task thread, plus one lookup per update refused with `NOT_FOUND`; `mutationsConfirmedAlone` counts each mutation re-sent alone, once; retries of those solo commits count in `batchesSent`.
- With the defaults, one commit can take about a minute of backoff against fast transient refusals, and about eleven minutes when every attempt times out, before the job fails, and each solo re-send has a budget of its own, so a confirmation pass can exceed Flink's default checkpoint timeout under a sustained outage; the docs say to size them together.
- The lookup needs `datastore.entities.get`, which `roles/datastore.user` carries.
- `errorClass` counts the transient failures the writer recovered from, as on the Spanner sink (ADR-0076), because the writer sees its own retries.
- The request-size limit, the lookup discriminator and the statuses above are emulator evidence only; [#1546] measures them against the service.

## Alternatives declined

- **The proto `Mutation` on the SPI.** It covers all four operations in one type, but leaves users building protobuf `Entity` and `Value` messages by hand, and the high-level types serialize.
- **The per-operation client methods.** Each sends a commit of one operation type, so a mixed batch would cost one request per operation, and they merge a repeated key silently (`put`, `update`, `delete`) or refuse it (`add`).
- **The generated `DatastoreClient` with hand-built protobufs.** The entity-to-protobuf conversion is package-private, so the writer would carry its own converter.
- **The client library's retries.** They would re-send a commit on their own schedule with no hook, and the writer must see each refusal to confirm it.
- **Bisecting a refused commit** instead of re-sending each write. It costs fewer requests for one bad write among many, but confirms nothing about a commit with several, and the sibling sinks' solo shape is already documented and measured.
- **Beam's adaptive batch sizing.** Not needed for correctness; reopen if a measurement shows the fixed bounds limiting throughput.
- **`base_version` conflict detection and an exactly-once mode** stay deferred with their reopen conditions on [#355].

[#355]: https://github.com/flink-gcp/flink-connector-gcp/issues/355
[#1542]: https://github.com/flink-gcp/flink-connector-gcp/issues/1542
[#1546]: https://github.com/flink-gcp/flink-connector-gcp/issues/1546
