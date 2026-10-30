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

# ADR-0190: The Bigtable sinks keep per-row order on opt-in

- Status: Accepted
- Date: 2026-10-29 (emulator behavior measured 2026-10-29 against the pinned
  `google-cloud-cli:588.0.0-emulators` image; service behavior measured 2026-10-29 on ephemeral
  one-node SSD instances in us-central1-b, google-cloud-bigtable 2.85.0)
- Issues: [#1620], [#1618], [#471]
- Modules: base (`sink`), bigtable (`sink`, `sink.mutaterows.writer`, `sink.singlerow`,
  `sink.singlerow.writer`, `sink.conditional`, `sink.readmodifywrite`, `table`, `table.sink`)
- Current behavior: `docs/content/docs/connectors/datastream/bigtable.md` § What this connector is
  for and § Shared request runtime; `docs/content/docs/connectors/table/bigtable.md` § Two rows for
  one key in one batch have no defined winner; `docs/content/docs/reference/bigtable.md`

## Context

[ADR-0093] kept the Bigtable sink's bulk path without per-row order. `MutateRows` may apply the
entries of one request in any order, even two for the same row, and the client keeps several
requests in flight, but a 2026-08-11 campaign saw no reversal inside one request, so the cost of
ordering every job was not justified. The single-row sinks (`insert-if-absent`, `conditional`,
`append`, `increment`) have the same gap for a different reason: the writer keeps up to
`maxInFlightRequests` requests outstanding, and nothing orders two of them for one row.

[#1618] asks each keyed sink for an opt-in order, and [ADR-0187] gave the Firestore sink the shape
this record reuses: `sink.ordering-mode` = `none` | `per-key`, a connector-owned `OrderingMode`, and
a sequencer that admits one write per key at a time. The owner reopened ADR-0093's question for
Bigtable as an opt-in on 2026-10-04 and, on 2026-10-28, included the single-row sinks in its scope
and asked the `aggregate` mode to refuse the option.

## Decision

- **`OrderingMode.PER_KEY` holds a write while an earlier write to the same row is unanswered, in
  both write families.** A row is a `TableRowKey`, a row key in one table. `BigtableWriter` takes the
  row key from the `toProto()` it already builds for the entry's size, so the order costs no second
  proto. `SingleRowRequestWriter` takes it from the request. Under the default, `NONE`, both
  writers behave as they did, allocate nothing more per record, and differ only in resolving a
  destination after the admission wait rather than before it, which no `NONE` wait can observe.
- **An entry parked for its solo verdict or for the auto-creation repair keeps its row's turn.** A
  batched `INVALID_ARGUMENT` may answer the request rather than the entry ([ADR-0045]), and a
  `NOT_FOUND` under `CREATE_IF_NEEDED` is re-applied after the repair ([ADR-0073]); either way the
  parked entry may still be applied, so releasing the turn at the park would let the row's next
  entry overtake it. The turn ends on success, on a routed solo verdict and on a fatal answer.
- **The completion mail only readies the next write; the writer submits it from its own waits**,
  as [ADR-0187] decided for Firestore and for the same reason: a submission inside a mail could
  share the isolation pass's solo request.
- **Held writes count against the in-flight bounds** (`maxInFlightEntries` and `maxInFlightBytes`,
  or `maxInFlightRequests`), which is what bounds the buffer. When only held entries fill the
  bounds and nothing is in flight, the admission wait runs the repair or the isolation pass they
  wait behind itself, and it may send the batchers again after it submits held entries, since an
  entry it submitted would otherwise wait for the client's one-second timer.
- **`aggregate` refuses the option**, because `AddToCell` contributions give the same result in any
  order (owner's decision, 2026-10-28). The staged `EXACTLY_ONCE` sink refuses it too: its
  committer keeps no per-row order, and accepting `PER_KEY` would promise one. The two async
  helpers refuse `PER_KEY` in `requestOptions(...)`, because the async operator starts each
  request as its input arrives and has no place to hold one back.
- **`OrderingMode` is an `@Experimental` closure stop**, because the `@Public`
  `BigtableWriterOptions` names it ([ADR-0124]'s revision, [ADR-0141]); Firestore's namesake can stay
  `@PublicEvolving` because its options type is `@PublicEvolving` too.
- **One option feeds both runtime option types.** `sink.ordering-mode` maps to
  `BigtableWriterOptions.orderingMode` or `BigtableRequestOptions.orderingMode`, whichever the write
  mode selects, as `sink.destination-idle-timeout` already does.
- **`PerKeyWriteSequencer` moves to `flink-connector-gcp-base`** (`base.sink`), as [ADR-0187]
  planned once a second sink used it. Its invariant failures render a key, so Bigtable's
  `TableRowKey` renders the table and the key's length, not the key.
- **The gauges are `bufferedEntries` and `bufferedRequests`**, in each runtime's own counting unit
  ([ADR-0082]). The async function registers no `bufferedRequests`.

## Evidence

- `BigtableWriteITCase.perKeyOrderAppliesEachRowsWritesInTheOrderReceived` writes ten rounds over
  twenty rows, interleaved, every cell at one timestamp and every third round a row delete, with
  two entries per request. With the test switched to `OrderingMode.NONE` it failed in three of four
  runs against the emulator (12 or 18 of the 20 rows left, a delete landing after the last set).
  `SingleRowRequestWriterEmulatorITCase.perKeyOrderAppliesEachRowsRequestsInTheOrderReceived`
  appends ten rounds to each of twenty rows; under `NONE` it failed in each of four runs (a row
  read `1;0;2;3;5;4;7;6;8;9;`). Both pass under `PER_KEY`, so the emulator reproduces the
  reordering and the tests can fail without the sequencer.
- `BigtableWriterRowOrderTest` and `SingleRowRequestWriterRowOrderTest` pin each rule against the
  fakes: a parked entry keeps its turn through the isolation pass and through the repair, a routed
  write and a write the client refuses before sending end their turn, held writes count against
  every in-flight bound, and the admission wait runs a pass itself when only held entries fill the
  bounds. Each of those tests fails when its rule is removed from the writer (checked by mutation,
  2026-10-29/30: thirteen mutants of `BigtableWriter` and six of `SingleRowRequestWriter` failed
  a test; one more of each, which stops the flush from submitting held writes, hung the test run
  instead, as a loop the test's timeout cannot interrupt).

Measured against the service on 2026-10-29, from one writer on a workstation outside Google Cloud,
three repetitions of each workload in each mode, alternating which mode went first. Each writer
had sent and flushed one record before the clock started, so the times exclude client creation.
A row's cells all carry one timestamp and every third write of a row is a row delete, so a row
ends at its last write only if its writes were applied in order.

| Workload | `NONE` | `PER_KEY` |
|---|---|---|
| Batch path, default thresholds: 2,000 writes, each to its own row | 0.40–0.42 s | 0.38–0.41 s |
| Batch path, default thresholds: 100 writes to one row | 0.20–0.21 s; the row ended at its last write in 3 of 3 runs | 16.8–17.2 s, about 0.17 s per write; 3 of 3 correct |
| Batch path, default thresholds: 200 rows changed five times each, interleaved | 0.20–0.21 s; 100, 100 and 200 of the 200 rows ended at an earlier write | 0.99–1.00 s; 0 rows wrong in 3 runs |
| Batch path, two entries per request: 2,000 writes, each to its own row | 0.72–1.16 s | 0.78–0.80 s |
| Batch path, two entries per request: 100 writes to one row | 0.22–0.33 s; the row ended at an earlier write, or was deleted, in 3 of 3 runs | 16.8–17.3 s; 3 of 3 correct |
| Batch path, two entries per request: 200 rows changed five times each | 0.65–0.70 s; 54, 60 and 60 rows wrong | 1.31–1.42 s; 0 rows wrong |
| `ReadModifyWriteRow` append, 1,000 requests, each to its own row | 1.81–1.97 s | 1.80–1.94 s |
| `ReadModifyWriteRow` append, 100 requests to one row | 0.27–0.29 s; the appends out of order in 3 of 3 runs (43–49 descents) | 16.7–17.2 s; 3 of 3 in order |

- **The service does reorder one row's writes once they travel in different requests.** At the
  default thresholds the wrong rows came in blocks of 100, the batcher's default request size: in
  one run rows 0 to 99 held the delete of their third write, in another rows 100 to 199 did. That
  fits a whole request applied after a later one, though the measurement did not observe the
  requests themselves. [ADR-0093]'s campaign measured pairs inside one request, where it saw no
  reversal, and the hot-row workload at the default thresholds, which also fits in one request,
  ended correctly in every run here.
- **The order costs nothing measurable when rows do not repeat**, on either path.
- **The cost is the sequential round trip**, about 0.17 s per held write to one row from this
  client, on both paths. A row changed n times takes n round trips, so the interleaved workload
  took about five times as long at the default thresholds and twice as long at two entries per
  request.
- An earlier run on 2026-10-29, which timed client creation and stopped at the harness's
  ten-minute test timeout, agrees: the interleaved workload under `NONE` left 0, 100 and 100 rows
  wrong, and under `PER_KEY` none.

## Consequences

- A row written n times in a row takes n round trips under `PER_KEY`. A row written faster than
  that fills the in-flight bounds, and the admission wait then holds back every row of the subtask,
  not only the hot one; the buffered gauges show it.
- The order kept is the order one subtask received, as for Firestore: a DataStream job keys its
  stream by row key before the sink, and a Table changelog gets the order when Flink shuffles the
  input by its primary key. A table without a primary key, which every `conditional` table is,
  gets no keyed shuffle, so its rows of one key are ordered only if the input already sends them
  to one subtask.
- The order is of application, not of visibility: a read returns a cell's highest-timestamp
  version, so a later write that keeps earlier versions (an ordinary `setCell`) and carries an
  older explicit cell timestamp stays hidden behind an earlier one in either mode. A write that
  deletes what it replaces first, as `keep-latest` and a `MAP` family's `replace` do ([ADR-0153]),
  shows whatever its timestamp. Writes at one timestamp, and deletes, take effect in the order
  applied, which is what the measurements above use.
- A turn ends when the client answers, not when the service has stopped acting on the write. A
  `MutateRows` attempt the client abandoned at its own per-attempt timeout and then retried, or a
  single-row request cancelled by a restart, may in principle still be applied after the row's next
  write; the order is as strong as client-side sequencing can make it.
- A held write readied while no record arrives waits for the next record or the next checkpoint,
  as a parked entry already does; unlike an entry the batcher holds, it has no timer. With sparse
  input a row's held writes advance one per record, a checkpoint's flush spends one round trip per
  write still held for a row, and with checkpointing off a paused input leaves them waiting.
- `bufferedEntries` and `bufferedRequests` count the held writes; the bytes of held entries count
  against `maxInFlightBytes` without a gauge of their own, as Firestore's do. No log line reports
  a hot row: the stall warning keeps quiet while answers arrive every round trip.
- [ADR-0093] still describes the default, and its caveat stays in the documentation for it.

## Alternatives declined

- **Ordering only the batch path.** The single-row sinks were the other half of [#1620]'s
  description, and their writer has no isolation pass, so the same sequencer fits it with less
  bookkeeping (owner's decision, 2026-10-28).
- **Accepting `PER_KEY` and ignoring it where it cannot apply** (aggregate, staged, async). A
  silently ignored order is the failure a job cannot see; a refusal at build or plan time names
  the option.
- **Moving the writers' ready queue and buffered count into `base.sink` beside the sequencer.**
  The three writers that use it (Firestore and Bigtable's two) share that bookkeeping, but what
  can hold a turn differs between them (two parks here, one park in Firestore, whose redraw queue
  holds only drawn-id creates the sequencer never sees, nothing in the single-row writer), as does what the buffer is counted in, so the shared part is
  the few lines around `release`. Moving it would also rewrite Firestore's merged writer for no
  change in behavior.
- **Putting the single-row setter on the two sink builders rather than on
  `BigtableRequestOptions`**, which would spare the async helpers and the staged sink their
  refusals. The spelling settled for [#1618] puts `orderingMode` on each sink's runtime options
  type, and the refusal names the option where a user sets it.
- **Setting the batch element count to one.** It produces concurrent one-entry requests, not
  ordered ones ([ADR-0093]).

[#471]: https://github.com/flink-gcp/flink-connector-gcp/issues/471
[#1618]: https://github.com/flink-gcp/flink-connector-gcp/issues/1618
[#1620]: https://github.com/flink-gcp/flink-connector-gcp/issues/1620
[ADR-0045]: 0045-a-row-level-verdict-is-confirmed-solo-before-it-is-routed.md
[ADR-0073]: 0073-bigtable-auto-creation-parks-not-found-and-repairs-through-an-ensure.md
[ADR-0082]: 0082-the-bigtable-batch-knobs-are-bounded-flush-triggers-and-the-client-counts-mutations.md
[ADR-0093]: 0093-the-bigtable-sink-does-not-serialize-same-row-batch-entries.md
[ADR-0124]: 0124-the-stability-boundary-at-1-0-0-is-a-promoted-public-entry-surface-checked-by-japicmp.md
[ADR-0141]: 0141-a-surfaces-stability-tier-is-set-by-what-can-reshape-its-inputs-and-outputs.md
[ADR-0153]: 0153-the-table-keep-latest-mode-replaces-only-written-cells.md
[ADR-0187]: 0187-the-firestore-sink-keeps-per-document-order-on-opt-in.md
