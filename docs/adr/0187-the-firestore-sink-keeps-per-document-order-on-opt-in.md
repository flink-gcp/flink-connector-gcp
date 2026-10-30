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

# ADR-0187: The Firestore sink keeps per-document order on opt-in

- Status: Accepted
- Date: 2026-10-27 (emulator behavior measured 2026-10-27 against the pinned `google-cloud-cli`
  emulators image, three runs; service behavior measured 2026-10-27 in an ephemeral
  Standard-edition database in us-central1, google-cloud-firestore 3.49.0)
- Issues: [#1556], [#1618], [#1619], [#1620]
- Modules: firestore (`sink`, `sink.writer`, `table`, `table.sink`)
- Current behavior: `docs/content/docs/connectors/datastream/firestore.md` § Delivery guarantee;
  `docs/content/docs/connectors/table/firestore.md` § Changelog and ordering;
  `docs/content/docs/reference/firestore.md` § Order per document

## Context

[ADR-0171] left the Firestore sink without same-document order: `BulkWriter` sends several
`BatchWrite` requests at once, `BatchWrite` applies the writes of one request in no guaranteed
order, and a failed write is retried in a later request. [ADR-0179] carried the limit into the
Table sink, where it matters most, because an upsert changelog that changes a key more than once
can leave an older value in the document. [#1556] is the opt-in mode both records point at, and
[#1618] makes it the first of three sinks (Firestore, Spanner [#1619], Bigtable [#1620]) that take
the same option spelling.

## Decision

- **`OrderingMode.PER_KEY` holds a write while an earlier write to the same document is
  unanswered.** The writer keeps a `PerKeyWriteSequencer` keyed by document path on the task thread: the
  first write of a document is admitted and handed to the library, and a later one is buffered in
  the writer. A write keeps its document's turn until the library answers it — applied, routed to
  the failure handler, or failing the job — and the answer readies the document's next buffered
  write. Writes to other documents are not held back. The default, `NONE`, leaves the writer as
  [ADR-0171] describes it.
- **Waiting for the answer, not for the send.** The library retries a failed write in a later
  request, and one request applies its writes in no guaranteed order, so a write sent after an
  earlier one can still be applied before it; only the earlier write's answer orders them.
- **A write parked for a solo re-send keeps its turn.** An `INVALID_ARGUMENT` may answer the whole
  request rather than the write ([ADR-0171]), so the parked write may still be applied by its solo
  re-send. Releasing its turn at the park would let the next write to the document overtake it.
- **The completion mail only readies the next write; the writer submits it from its own code**:
  inside and at the end of each admission wait, and in each pass of `flush`.
  A submission inside a mail could run while an isolation pass waits for a solo re-send, and the
  readied write would then share the re-send's request and break the isolation the pass exists
  for.
- **Buffered writes count against `maxInFlightWrites` and `maxInFlightBytes`**, which is what
  bounds the buffer. When nothing is in flight and only buffered writes hold the bounds, their
  turns sit with parked writes that only an isolation pass answers, so the admission wait runs
  the pass itself rather than wait for a mail that cannot arrive. A wait that finds buffered
  writes with no in-flight, ready or parked write to end a turn raises an `IllegalStateException`
  instead of spinning, as the isolation pass does for its own broken invariant.
- **A drawn-id create is never sequenced.** Its id names a document no other write addresses, and its
  redraw under a new id would otherwise release a turn the sequencer never gave it.
- **The gauge is `bufferedWrites`**: [ADR-0137]'s state ladder calls a write held before hand-off
  *buffered*.
- **The option is `orderingMode(OrderingMode)` on `FirestoreWriterOptions` and
  `sink.ordering-mode` = `none` | `per-key` in a `WITH` clause, and each connector declares its own
  `OrderingMode` in its `sink` package** (owner's decision, 2026-10-27). The spelling extends the
  Pub/Sub source's `scan.ordering-mode` (`none` | `per-key`), the existing name of the same
  connector-owned concept, as [ADR-0137] asks of a new name. Spanner and Bigtable adopt it in
  [#1619] and [#1620].
- **`PerKeyWriteSequencer` carries no Firestore type** and stays in this module until a second sink uses
  it, when it moves to `flink-connector-gcp-base`. (Revised 2026-10-29: it moved to `base.sink` when
  the Bigtable sinks adopted it, [ADR-0190].)

## Evidence

- `FirestoreWriterITCase.perKeyOrderAppliesEachDocumentsWritesInTheOrderReceived` writes ten
  rounds of sets and deletes, interleaved across twenty documents, and checks that each document
  ends at its last round. With the test switched to `OrderingMode.NONE` it failed in each of three
  runs against the emulator (document `d0` held round 1 instead of round 9), so the emulator
  reproduces the reordering and the test can fail without the sequencer.
- `FirestoreWriterTest` pins the sequencer against the scripted `BulkWriter`: a buffered write is sent
  after its document's earlier write is answered, a parked write keeps its turn through the
  isolation pass, a routed write ends its turn, buffered writes count against both in-flight
  bounds, and a drawn-id create and its redraw pass through. Each of those tests fails when its
  rule is removed from the writer (checked by mutation, 2026-10-27).

Measured against the service on 2026-10-27, from one writer on a workstation outside Google Cloud,
with the library's throttling off. There were two runs in fresh ephemeral databases: the first ran
three repetitions of each workload in each mode, alternating which mode went first; the second was
cut short by the test timeout after its first repetition.

| Workload | `NONE` | `PER_KEY` |
|---|---|---|
| 2,000 sets, each to its own document | 5.2–6.2 s (325–382 writes/s), 4 runs | 4.0–6.3 s (318–501 writes/s), 4 runs |
| 100 sets to one document | 2.4–3.6 s; the document ended at an earlier write in 4 of 4 runs | 43.2–45.5 s, about 0.44 s per write; the last write stood in 4 of 4 runs |
| 200 documents changed five times each, interleaved, a third of the changes deletes | 3.7–4.2 s, every document at its last change in 4 of 4 runs | 5.2–5.4 s, every document at its last change in 3 of 3 runs |

- **The sequencer's own bookkeeping costs nothing measurable**: the distinct-document workload falls in
  the same range in both modes.
- **The service does reorder one document's writes.** Without the mode, the hot document ended at
  an earlier write every time, so the reordering is a property of the service and the library, not
  of the emulator alone.
- **The cost is the sequential round trip**, about 0.44 s per buffered write to one document from
  this client. A changelog that changes each key a few times, spread over many keys, took about
  1.3 times the unordered duration on average (1.2 to 1.5 between runs); that shape did not show a reordering without the mode in
  these runs, which says only that the reordering needs writes to one document close enough
  together to share requests.

## Consequences

- A document written n times in a row takes n round trips under `PER_KEY`. A document written
  faster than that fills the in-flight bounds, and the admission wait then holds back every
  document of the subtask, not only the hot one; `bufferedWrites` shows it.
- The order kept is the order one subtask received. The DataStream user must key the stream by
  document path before the sink. A Table changelog gets it when Flink shuffles the input by the
  primary key, which an insert-only input at the sink's own parallelism is not unless
  `table.exec.sink.keyed-shuffle` is `FORCE` (the rule the Datastore table page already states).
- A buffered write readied while no record arrives waits for the next record or the next
  checkpoint, as a write the library holds unsent already does ([ADR-0171]).

## Alternatives declined

- **A boolean option (`sink.ordering.enabled`, `orderingEnabled(boolean)`).** Four of the 22
  distinct boolean `WITH` keys end in `.enabled` (counted 2026-10-27), so neither spelling was forced by the fleet; the enum
  matches the sibling `scan.ordering-mode` and leaves room for an ordering by a version carried in
  the data, which [#1556] leaves out of scope (owner's decision, 2026-10-27).
- **One `OrderingMode` in `flink-connector-gcp-base`** for every connector, the shape of
  `base.source.StartPosition`. It adds a public type to `base` for three two-constant enums; the
  per-connector shape is the one `sink.delivery-guarantee` already takes (owner's decision,
  2026-10-27).
- **A bound of its own for buffered writes.** A third knob beside the two in-flight bounds would
  bound the same memory twice; counting buffered writes against them keeps one budget.
- **Submitting the readied write from the completion mail.** See the Decision: it breaks the solo
  isolation of [ADR-0171].
- **Ordering by a version carried in the data.** Firestore has no conditional write on a field
  value, only the server-assigned `updateTime`, so it needs a read-compare-write transaction per
  document, a different cost profile; [#1556] scoped it out.

[#1556]: https://github.com/flink-gcp/flink-connector-gcp/issues/1556
[#1618]: https://github.com/flink-gcp/flink-connector-gcp/issues/1618
[#1619]: https://github.com/flink-gcp/flink-connector-gcp/issues/1619
[#1620]: https://github.com/flink-gcp/flink-connector-gcp/issues/1620
[ADR-0137]: 0137-a-cross-connector-name-diverges-only-to-name-a-real-difference.md
[ADR-0171]: 0171-the-firestore-sink-writes-through-bulkwriter-and-confirms-invalid-writes-alone.md
[ADR-0179]: 0179-the-firestore-table-sink-writes-one-collection-keyed-by-document-id.md
[ADR-0190]: 0190-the-bigtable-sinks-keep-per-row-order-on-opt-in.md
