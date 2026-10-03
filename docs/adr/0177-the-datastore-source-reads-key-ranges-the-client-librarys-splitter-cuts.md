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

# ADR-0177: The Datastore source reads key ranges the client library's splitter cuts

- Status: Accepted
- Date: 2026-10-03 (client library facts read in google-cloud-datastore and datastore-v1-proto-client 3.7.0
  and google-cloud-core 2.77.0 through libraries-bom 26.90.0; emulator behavior measured 2026-10-03
  against `google-cloud-cli:587.0.0-emulators` in Datastore mode, one run)
- Issues: [#1543], [#355], [#1546]
- Modules: firestore (`io.github.flink.gcp.connector.datastore`: `source`, `source.batch`); base (`lineage.internal`)
- Current behavior: `docs/content/docs/connectors/datastream/firestore.md` § Datastore mode

## Context

[#355]'s design comment settled the shape of the Datastore-mode bounded source: a FLIP-27 source on `PullAssignmentSplitEnumerator` (ADR-0083) whose enumerator cuts the query into key ranges with `datastore-v1-proto-client`'s `QuerySplitter`, a split count estimated Beam's way from `__Stat_Kind__` statistics, a query the splitter cannot split read as one split, each split one key-range query plus the job's read time, readers that page and resume by cursor, a namespace option and a `Collector`-based deserializer over `Entity` (ADR-0108).
The Native-mode source (ADR-0173) had settled the read-time and planning protocol this one mirrors.
It left open what a user hands the source, how the splitter is reached, which queries count as splittable, and how a page is read.
Reading the client libraries and measuring the emulator decided each.

## Evidence

Read from google-cloud-datastore and datastore-v1-proto-client 3.7.0:

- `QuerySplitterImpl` sizes a list from the requested count before any check, and its only count check refuses a value below 1. It samples `(numSplits - 1) * 32` keys and holds them all; that `int` overflows above 67,108,864.
- `QuerySplitter.getSplits(Query, PartitionId, int, Datastore)` takes the proto-client `com.google.datastore.v1.client.Datastore`, an HTTP client of its own, whose constructor is package-private. Its implementation refuses a query that does not name exactly one kind, has an ordering, or has a `<`, `<=`, `>` or `>=` filter; it accepts a `limit`, an `offset`, cursors and the `!=`, `NOT_IN`, `IN` and `OR` filters. It also refuses a filter of no type. It samples 32 `__scatter__` keys per range boundary, sorts them with `DatastoreHelper`'s key comparator, which compares kind names and key names as Java strings (by UTF-16 code unit), and ANDs `__key__ >= start` and `__key__ < end` onto the query for each range. Its read-time overload is `@BetaApi`.
- The client library's own HTTP transport (`HttpDatastoreRpc`) builds that client with `DatastoreFactory.get().create(...)`: the credentials adapted to HTTP requests, or a plaintext `localHost` for an emulator on a loopback address. The client makes one attempt per call, apart from the credentials adapter re-sending a request once after refreshing an expired token.
- The high-level `StructuredQuery`'s conversion to the protobuf `Query` is package-private, reachable otherwise only through `@InternalApi` members (`RecordQuery.populatePb`, `StructuredQueryProtoPreparer`). The reverse, `Entity.fromPb`, is public.
- `DatastoreRpc.runQuery` is a synchronous unary call. With the client's default settings it retries `UNAVAILABLE` and `DEADLINE_EXCEEDED`, up to six attempts within 50 seconds (`ServiceOptions`' default retry settings, which the client applies to every generated call). The blocking call does not answer a thread interrupt. Flink's split fetcher manager waits for a closing reader's fetchers only up to `source.reader.close.timeout`, 30 seconds by default.
- Every `EntityResult` of a query batch carries the cursor after its entity; the batch carries its end cursor, how many results an `offset` skipped, and whether more results remain.

Measured against the emulator:

- The proto-client HTTP client reaches the emulator through `localHost` and is answered.
- The splitter's `__scatter__` sampling query returns no entity, so the splitter answers every request with the whole query.
- `__Stat_Total__` and `__Stat_Kind__` hold nothing, and neither do a namespace's `__Stat_Ns_Total__` and `__Stat_Ns_Kind__`.
- A read at an older read time returns the entities as they stood; a read time in the future is refused with `INVALID_ARGUMENT`; one two hours old is answered.
- A GQL query sent with `LIMIT 0` appended comes back parsed, with a zero limit, and reads nothing.
- A batch with an offset and a limit of zero carries neither a skipped cursor nor an end cursor; a batch that returned entities carries its end cursor and one cursor per entity.
- Keys sort by their names' UTF-8 bytes: `a！` (U+FF01) comes before `a😀` (U+1F600), which Java's string order puts first.
- A `__key__` range ANDed onto a kind query returns exactly the entities of the range.
- A projection returns a timestamp as an integer of microseconds and a blob as a string, each with meaning 18 (an index value); converted with `Entity.fromPb`, `getTimestamp` and `getBlob` throw `ClassCastException` on them. The client library reads them back only in its `ProjectionEntity`, whose `fromPb` and builder constructor are package-private.

## Decision

- **The user hands the source a kind, a protobuf `Query` or a GQL string** (owner's decision, 2026-10-03). The protobuf `Query` is what the service and the splitter take, so nothing converts it, and a kind is the `Query` that names it. The high-level `EntityQuery` was declined because its only conversion to the wire form is package-private or `@InternalApi`, which production code here does not depend on (ADR-0173). A GQL query is parsed by the service at planning, with `LIMIT 0` appended so that nothing is read; a query that already ends in a `LIMIT` or `OFFSET` clause refuses the appended clause with `INVALID_ARGUMENT` and is then parsed by running it as written, which reads and bills its first batch. That is Beam's procedure.
- **The splitter is called over its own HTTP client** (owner's decision, 2026-10-03), built through the factory `HttpDatastoreRpc` uses, on the JobManager only while a read is planned; closing the planner refuses later calls. An emulator endpoint is reached in plaintext whatever its address, as everywhere in this connector. Everything else, the readers included, goes through a gRPC client built by `DatastoreClients` as the sink's is, but keeping the client library's retry settings where the sink's makes one attempt per call.
- **A query is cut into key ranges only when it names one kind and filters only with `EQUAL` and `HAS_ANCESTOR` under `AND`.** The source refuses more than the splitter does: a `limit`, an `offset` and cursors name positions in the whole result, not in a key range; `!=` and `NOT_IN` are inequalities that the range's `__key__` inequality would meet; `IN` and `OR` are disjunctions the range would have to distribute over; `DISTINCT ON` has no meaning per range; and a filter of no type is one the splitter refuses. Any other query is one split, and the plan logs why. A `splitCount` on a query that cannot be split is refused when the source is built, or for a GQL query when it is planned.
- **The split count, when not configured, is Beam's estimate with the parallelism as a further floor**: the kind's `entity_bytes` in the latest statistics, one split per 64 MiB, at least 12 and at least the source's parallelism, at most 50,000. The floor is this source's addition: statistics are absent for a new kind, and Beam's floor of 12 alone would leave every subtask past the twelfth of a wide job without a range. The latest statistics time is read from `__Stat_Total__` first and the kind's entry matched on it, because ordering `__Stat_Kind__` by time for one kind needs a composite index. Outside the default namespace the namespace's own statistics kinds are read. A failure reading the statistics fails planning with a message naming `splitCount` as the way around it, rather than falling back to the floor with a warning, as Beam does: the same identity has just read the query, so a failure here is not the missing-statistics case.
- **The option is named `splitCount`** (owner's decision, 2026-10-03), after the splitter's query splits, as the Native-mode `partitionCount` is named after `PartitionQuery`'s partitions. A shared name was declined because Datastore has no partition in that sense: its `PartitionId` names a namespace.
- **The splitter samples at the current time**, not at the read time: the read-time overload is a beta API, and nothing is lost without it, because the ranges tile the key space and an entity of the snapshot falls in exactly one of them. This is ADR-0173's argument for `PartitionQuery`.
- **A split is a `RunQueryRequest` plus the job's read time**: the request names the project, the database and the namespace, and carries the range's query without read options. The serializer writes the request as its protobuf encoding, length-prefixed, for ADR-0173's reasons.
- **The read time is the service's**, taken from a one-entity probe of the query, or the configured one, probed at; ADR-0173's reasoning applies unchanged, including the decision not to round it.
- **A page is one synchronous `RunQuery` call through the client's RPC object**, at the split's read time, limited to `pageSize` or what is left of the query's `limit`, and carrying what is left of its `offset`. The service may answer with fewer entities and say more remain; the next page starts at the batch's end cursor. The client library's retries are kept, because a retried call is the same request at the same read time, and it is unary, so the Native-mode source's mid-stream retry hazard does not arise. The offset therefore stays on the query rather than being resolved at planning: the service applies it, page by page, reporting what it skipped. Entities are converted with the public `Entity.fromPb`. A batch that claims more results without moving, or with no cursor to continue from, or in an unknown more-results state, fails the read rather than loop or stop early.
- **A nearest-neighbour search is refused**, at build time or, for a GQL query, at planning. The service applies a query's cursor and limit before `find_nearest`, so the page limit and the resume cursor every read sets would change which entities it finds, and it reports tied distances in no stable order across requests.
- **The splitter's ranges are laid out again in the service's key order** (owner's decision, 2026-10-03). Its key comparator orders names by UTF-16 code unit and the service by UTF-8 byte, so two boundaries can arrive in the wrong order, leaving a range the service reads as empty beside two that overlap. The planner takes every `__key__` bound from the splitter's ranges, sorts the distinct ones in the service's order and rebuilds the ranges in the splitter's shape (`KeyRanges`). The service order is measured on the emulator only; the gated suite confirms it ([#1546]).
- **A projected timestamp or blob is read back before the deserializer sees it** (owner's decision, 2026-10-03), by `ProjectionEntity`'s two rules: an integer with meaning 18 becomes the timestamp of that many microseconds, a string with meaning 18 the blob of its UTF-8 bytes. It applies only to a query that projects, as the client library's `ProjectionEntity` does, and replaces the value rather than reading it back on request, so `getTimestamp` and `getBlob` answer on a projection as on a whole entity while `getLong` and `getString` no longer do. The deserializer contract stays `Entity`. Every other value is passed as the service returned it.
- **A split count is at most 50,000**, configured or estimated (`DatastoreSourceBuilder.MAX_SPLIT_COUNT`, Beam's ceiling). The splitter checks no upper bound: a huge count would exhaust the JobManager's memory sizing its list, or overflow its scatter limit. At the bound it samples and holds about 1.6 million keys on the JobManager, which `splitCount` lowers.
- **Resume is by the service's cursor.** A checkpoint rewrites a split's query to start at the cursor after the last entity successfully deserialized, with its `limit` reduced by the entities passed and its `offset` dropped; the service returns an entity only after the whole offset, so the offset is spent by then. The cursor is the service's, so continuing needs no knowledge of the query, and the Native-mode probe for a continuable projection has no counterpart here; the emulator tests restore a kind scan and continue ordered, filtered, offset, limited and end-cursor queries across pages.
- **The planner's split call is a protected seam**, as `ClientQueryPlanner.partitions` is for the Native-mode source: the emulator samples no keys, so the emulator tests replace it with ranges they choose and read them for real. The statistics estimate, the GQL fallback and the read-time probe are package-private functions of the RPC object, which unit tests drive through a hand-written fake.
- **A kind is reported as a `datastore-kind` lineage resource** when the job's configuration names it: a `kind(...)`, or a `query(...)` naming one kind. The namespace is `datastore://{project}/{database}` followed by `/{namespace}` outside the default namespace, and the name is the kind; the namespace sits in the canonical namespace because a kind may contain `/` and a namespace may not. A GQL query reports none, because the service parses it only at planning. ADR-0160 records the kind.

## Consequences

- A planning call reaches the service over HTTP as well as gRPC, so a network policy that admits only one of them to the JobManager breaks a split read. The readers are gRPC only.
- A page call runs for up to the 50-second retry budget and does not answer an interrupt; a reader that closes meanwhile waits for its fetcher up to `source.reader.close.timeout`, after which the call fails against the closed client.
- A projection whose key ranges need an index the whole query does not is unmeasured ([#1546]); `splitCount(1)` reads it as one split.
- A query the source does not split is read by one subtask, whatever the parallelism; the plan's log line names the reason.
- Planning costs a probe read, two statistics reads when the split count is estimated, the splitter's sampling, and for a GQL query a parse that reads its first batch when it ends in its own `LIMIT` or `OFFSET`.
- The emulator exercises neither the sampling nor the statistics; split counts against the service, the statistics estimate and the read-time window belong to the gated suite ([#1546]).
- A restart before the first completed checkpoint reads at a new read time unless `readTime` is configured, as for the Native-mode source.
- A query that orders by, or filters with an inequality on, a list-valued property can return an entity more than once across pages and restores, each repeat counting toward its `limit`: the service removes the duplicates such a property produces only within one request ([limitations of cursors](https://cloud.google.com/datastore/docs/concepts/queries#limitations_of_cursors)). The source passes repeats on and the docs say so (owner's decision, 2026-10-03); removing them would mean checkpointing every key a split has emitted.
- A projection result without a key, which the API allows and the emulator has not returned, fails the read with a message naming it, because the deserializer contract is an `Entity`, which always has a key (owner's decision, 2026-10-03). Widening the contract to a keyless type was declined for a case not observed; the gated suite ([#1546]) measures it.

## Alternatives declined

- **Sampling `__scatter__` keys and cutting the ranges in this module, over the gRPC client** (owner's decision, 2026-10-03). It would keep one transport, but #355 settled on calling the library rather than reproducing its sampling; only the final ordering of the boundaries is this module's.
- **A gRPC bridge passed to the splitter** as a subclass of the proto-client `Datastore`. Its constructor is package-private, and a class in the vendor's package is reserved for tests (ADR-0067).
- **The high-level `EntityQuery` as the input**, converted through `RecordQuery.populatePb`. See the first decision.
- **The splitter's validation as the splittability rule.** It splits a query with a `limit` into ranges that each apply the whole limit, and ANDs a key inequality onto `!=` and `NOT_IN`.
- **Beam's floor of 12 alone.** See the split-count decision.
- **Passing projected values as the service returns them, documented.** A deserializer calling `getTimestamp` on a projected property would fail with `ClassCastException`, and the reading rule it would need is the library's, not the user's.
- **Resolving an `offset` into a cursor at planning**, as the Native-mode source does. The reason there was the library's mid-stream retry, which a unary call does not have, and the emulator answers an offset-only batch without the cursor the resolution would need.
- **The GAPIC `DatastoreClient` for the readers, for an interruptible future call.** It would mean building and configuring a second kind of client, endpoint, credentials and emulator channel included, beside the one `DatastoreClients` builds for both directions, for a call bounded at 50 seconds.
- **Keeping the splitter's range order**, as Beam does. It reads the entities between two misordered boundaries twice.

[#355]: https://github.com/flink-gcp/flink-connector-gcp/issues/355
[#1543]: https://github.com/flink-gcp/flink-connector-gcp/issues/1543
[#1546]: https://github.com/flink-gcp/flink-connector-gcp/issues/1546
