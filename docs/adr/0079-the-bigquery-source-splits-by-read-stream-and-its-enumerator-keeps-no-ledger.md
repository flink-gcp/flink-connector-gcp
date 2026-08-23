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

# ADR-0079: The BigQuery source splits by read stream, and its enumerator keeps no ledger

- Status: Accepted
- Date: 2026-08-09 (BigQuery and emulator behaviour measured the same day), revised by [#392]
  (2026-08-10), [#393] (2026-08-11), and [#587] (2026-08-13)
- Issues: [#390], [#64], [#392], [#393], [#542], [#587]
- Modules: bigquery (`source`, `source.enumerator`, `source.reader`, `source.serializer`,
  `source.split`)
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md` § Source

## Context / Evidence

[#64] settled the shape — a FLIP-27 bounded source over the Storage Read API, split = one
`ReadStream` plus the consumed-input-row offset, one read session created once, pull assignment, Avro only
— from three public references (the Dataproc `flink-bigquery-connector`, Beam's `DIRECT_READ` paths
and the Spark connector). What it deferred to implementation is what this record holds, and most of
it turned on measurement rather than on the references.

Measured against BigQuery itself on 2026-08-09 (project `flink-gcp`, `google-cloud-bigquerystorage`
3.30.0), because the proto's own wording left the first two open:

- `ReadRowsRequest.offset` documents that "the offset requested must be less than the last row read
  from Read. **Requesting a larger offset is undefined.**" A checkpoint can be taken between the
  last row of a stream being emitted and the reader recording the stream as finished, which leaves a
  restored split at exactly the row count. **BigQuery answers that read with an empty stream and no
  error.** An offset past the rows allocated to the stream answers `FAILED_PRECONDITION: offset N
  has not been allocated yet`.
- **A stream's rows arrive in storage order, not the table's.** A read at offset 7 of a 20-row table
  returned 13 rows whose ids were neither sorted nor the ids 7..19 — it returned exactly the rows
  the full read had returned from index 7 on.
- **`maxStreamCount` is a cap that is honoured downwards and never a floor.** Requesting 3 gave 3 on
  a 910 GB table (936 by default) and on a 23 GB table (138 by default); a 6 MB table answered with
  one stream at `maxStreamCount(8)` *and* `preferredMinStreamCount(4)`. `preferredMinStreamCount`
  raised the 23 GB table from 138 to 552 when 5000 was asked for — best effort, as documented.
  `preferredMinStreamCount` above `maxStreamCount` is `INVALID_ARGUMENT`.
- A session expires exactly six hours after creation.

Measured against goccy/bigquery-emulator 0.8.1 — the pinned image — the same day:

- `CreateReadSession` rejects `maxStreamCount` above 1; `0` yields one stream.
- **`ReadRows` ignores the request's offset entirely** and answers every call from row zero,
  including offsets past the table's row count.
- A whole table arrives in one `ReadRowsResponse`.
- The Avro schema's namespace is `<project>.<dataset>` where BigQuery sends `__root__` with no
  namespace — and a hyphen, legal in a project id, is not legal in an Avro namespace, so a read
  against a hyphenated project fails in Avro's schema parser before a row is decoded.
- `selectedFields` and `rowRestriction` do work, and a missing table answers `UNKNOWN` rather than
  `NOT_FOUND` (the read-path twin of ADR-0029's deviation).

One more measurement decided the deserializer's shape: `TypeInformation.of(GenericRecord.class)` is
a generic type backed by Kryo, and Kryo **cannot serialize a `GenericData.Record` at all** — it
throws `UnsupportedOperationException` on the record's own schema (Flink 2.2, Avro 1.12.1).

## Decision

**A split is one `ReadStream` plus the number of input rows already consumed, and the offset advances
once after each successful deserializer call and all its synchronous downstream emissions.**
Zero, one, or many outputs advance the offset by one because it counts input rows rather than output
records.
A deserialization or downstream collection failure does not advance it, so recovery retries the
input row whose processing did not complete.
Split and split state are **two types**, because the split reader reads one on the fetcher thread
while the emitter mutates the other on the task thread.

**The read session is created exactly once**, guarded by a checkpointed `initialized` flag; a
restore adopts the session. A second session would pin a second snapshot of the table and a
failed-over job would read it as of two different instants. `readSessionsCreated` reports the same
fact at runtime: 1 for a job that started, 0 for one that restored.

**The enumerator keeps no ledger.** No map of subtask to splits — one queue of unassigned splits and
the `initialized` flag. (Since [#452] that protocol is
`base.source.PullAssignmentSplitEnumerator`, shared with the Bigtable scan source and recorded in
ADR-0083; what stays here is the read session, and the paragraphs below describe both.) The Dataproc connector's own change log records a "critical data loss bug in
reader split handling", fixed "by signaling no-more-splits per reader and removing completed readers
from queue" (read 2026-08-09; an earlier draft of this record glossed that as the bug living *in*
that bookkeeping, which is the opposite of what the entry says). What it establishes is that the
assignment-and-completion protocol is where a hand-written enumerator gets it wrong silently — and
the per-reader half of that protocol is something Flink's coordinator already does, which is why
this one can answer each request from a single queue and keep nothing else.

Flink keeps one thing this enumerator does not, and the distinction is worth stating because an
earlier draft of this record got it wrong: `SourceCoordinator.handleRequestSplitEvent` suppresses a
split request from a subtask already told there are no more splits, and
`SourceCoordinatorContext.subtaskReset` is the only thing that clears the flag (verified against
flink-runtime 2.2.1). Since `addSplitsBack` is reached only through that same reset, a returned
split is always reachable by the subtask coming back for it — but a *different*, already-finished
subtask will not pick it up, because its request never reaches the enumerator. The design is safe;
it is safe for that reason rather than for "nothing remembers". The metrics follow the same rule — `splitsAssigned` and
`splitsReturned` are monotone counters, and the unassigned side is Flink's own gauge reading the
queue, so nothing needs reconciling and nothing can be reconciled wrongly.

**The terminal-offset case is handled by the measurement, not by a lookahead.** A split restored at
the row count is opened like any other and read as an empty stream, which is what BigQuery answers.
A `finished` flag on the split was implemented first and then removed: nothing could set it, because
`SourceReaderBase` removes a split's state before calling `onSplitFinished`, so it was a field, a
serializer slot and a branch for a mechanism that was never wired up. The alternative — an envelope per record carrying
`lastInStream`, so the emitter could mark the state finished in the same `pollNext` that emits the
last row — was designed and declined once the measurement landed: it costs an object per row on the
hot path to close a window the service already closes.

**Both stream-count knobs are exposed, and both default to `0`.** `0` is how the API spells "the
server decides", it is also the only value the emulator accepts, and the documentation says what the
measurement shows: `maxStreamCount` is a cap and never a floor. The builder rejects
`preferredMinStreamCount > maxStreamCount` with the service's own rule, so a job fails where it is
written rather than at session creation.

**The deserializer takes a `GenericRecord` and emits zero or more non-null records through a
synchronous collector.**
Emitting nothing skips the input row and increments `recordsSkipped` once; retaining the collector
or using it after the call returns is invalid.
All source emitters enforce that boundary through the shared synchronous invocation collector in
ADR-0108.
It may declare a **reader schema**, and rows are then resolved into it by Avro's resolution rules,
so a hand-written schema naming a subset of the columns works and the records carry the schema the
produced `TypeInformation` was derived from.
The shipped `genericRecord(...)` implementation answers with **`GenericRecordAvroTypeInfo`**, which
is why `flink-avro` is a new `provided` dependency: the Kryo fallback does not work at all.
**This SPI is the only one**, and Avro the source's only wire format: [#393] measured a batch-aware
Arrow variant and declined to build it, the gain being reachable only by a consumer that never asks
for a row (ADR-0090).

**The source takes one emulator endpoint where the sink takes two** (ADR-0029). It makes no REST
call: the read session carries the schema. The moment the source grows a metadata call, this is the
decision to revisit — **and [#392] revisited it**: a source reading a `query(...)` submits a query
job, which is a REST call, so it takes `emulatorRestEndpoint(...)` as well. What is stated here
holds unchanged for a source reading a table, and adding a metadata call to *that* path was declined
along with the automatic view materialization that would have needed one (ADR-0087).

**Explicit credentials remain a key-file path until runtime.**
When `serviceAccountKeyFile(...)` is set, the JobManager loads the same service-account JSON for
the client that creates the read session and for the REST client that runs a query or materializes
a view; TaskManagers load it for their stream-reading clients.
Absent uses ADC.
Either emulator endpoint is mutually exclusive with the key because both emulator transports are
credential-free.
The path rather than a parsed credential travels in the job graph, so the same key file must be
mounted at that path on both process types after failover or rescaling too ([#542]).

**No recovery test may be written against the emulator.** Resume is covered by a unit test over a
fake that honours offsets, a MiniCluster job that fails once and is asserted to have resumed rather
than restarted, and a gated real-GCP case that measures the service. The emulator's deviations are
pinned by `BigQueryEmulatorReadDeviationITCase`, each beside a `@Disabled` test carrying the
behaviour to enable when it retires; the emulator harness uses a project id without a hyphen, for
the Avro-namespace reason above.

## Consequences

- A restored job resumes after every successfully processed input row. A row whose deserializer or
  downstream collection failed is replayed; when an earlier output from that same row had already
  succeeded, that output can therefore be emitted again. The three-way test split verifies this
  input-progress boundary — no single tier could.
- The enumerator's simplicity is load-bearing: an assigned-splits gauge cannot be added without
  reintroducing the ledger this record exists to refuse. The unassigned-splits gauge Flink registers
  reads the queue from the reporter thread, so its value is best-effort — as it is in Flink's own
  enumerators.
- `flink-avro` joins the tier audit's artifact list, with `GenericRecordAvroTypeInfo` as an
  unannotated entry. A job using the shipped `GenericRecord` deserializer needs `flink-avro` on its
  classpath — which it needs anyway to move a `GenericRecord` between operators.
- The SQL uber-jar relocates `org.apache.avro`, so the shaded artifact cannot serve the
  `GenericRecord` deserializer; that is the trade ADR-0035 already made for `AvroRecordSerializationSchema`,
  and a DataStream job takes the plain connector jar.
- Multiple streams are not covered by a real-GCP test: BigQuery decides the count from the table's
  size, and a table large enough to be split costs more than the assignment logic is worth there.
  The enumerator's unit tests carry it instead.
- Error-handling depth, session-expiry reporting and wider real-GCP restore coverage are [#391];
  query input is [#392], landed and recorded in ADR-0087; Arrow was [#393], measured and declined in
  ADR-0090, so Avro is the source's only wire format.

[#64]: https://github.com/laughingman7743/flink-connector-gcp/issues/64
[#390]: https://github.com/laughingman7743/flink-connector-gcp/issues/390
[#391]: https://github.com/laughingman7743/flink-connector-gcp/issues/391
[#392]: https://github.com/laughingman7743/flink-connector-gcp/issues/392
[#393]: https://github.com/laughingman7743/flink-connector-gcp/issues/393
[#452]: https://github.com/laughingman7743/flink-connector-gcp/issues/452
[#542]: https://github.com/laughingman7743/flink-connector-gcp/issues/542
[#587]: https://github.com/laughingman7743/flink-connector-gcp/issues/587
