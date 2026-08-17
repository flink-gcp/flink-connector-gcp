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

# ADR-0080: The Bigtable scan source splits by sampled row-key range, and a checkpoint truncates the range

- Status: Accepted
- Date: 2026-08-09, revised by [#587] (2026-08-13) and [#947] (2026-08-17)
- Issues: [#216], [#34], [#248], [#481], [#587], [#910], [#947]
- Modules: bigtable (`source`, `source.readrows`)
- Current behavior: `docs/content/docs/connectors/datastream/bigtable.md` § Source

## Context / Evidence

The connector could write and not read. [#34] settled the shape on 2026-08-02 and split it into
[#216] (this DataStream scan source) and #217 (Table API); the design references were Apache Beam's
`BigtableIO.read()` for split planning and flink-connector-hbase for the API surface, with no code
copied from either.

Checked against the pinned versions before the design was committed (`google-cloud-bigtable`
2.80.0, gax 2.82.0, protobuf-java 4.33.6), all on 2026-08-09:

- **`ByteStringRange` is mutable.** `startClosed`/`endOpen` and their siblings assign to the
  receiver and return it, despite javadoc reading "Creates a new Range" — verified by identity. Its
  `clone()` is `protected` on a package-private superclass, so a copy has to be rebuilt from the
  four accessors.
- **The SDK normalises an empty key on any bound to `UNBOUNDED`**, on both sides. So the connector
  never has to special-case the empty key on input — and *does* have to on output, where widening a
  truncated range to the whole table would replay a split forever.
- **`Query.shard(List<KeyOffset>)` is the client's own implementation of this design**: it drops
  empty sample keys, sorts and dedupes them, and cuts with the left piece ending exclusively at the
  boundary and the right piece starting inclusively at it.
- **`Query.shard` refuses a request carrying a row limit** (`IllegalStateException: Can't shard
  query with row limits`) — independent confirmation of [#216]'s deferral of `Query.limit()`.
- **`ByteStringRange.prefix` handles an all-`0xFF` prefix** by returning a range with an unbounded
  end, which a hand-rolled prefix conversion gets wrong.
- **A cancelled `ServerStream` is indistinguishable from an ended one at the iterator.**
  `cancel()` sets a flag after which `getNext()` always answers with the EOF marker, so `hasNext()`
  returns false exactly as at a clean end; and if the consumer was already blocked on the buffer,
  the cancellation instead arrives as a buffered error and `hasNext()` throws. Both paths, read out
  of gax's `QueuingResponseObserver` and `ServerStreamIterator`.
- **`ReadRowsResumptionStrategy` resumes a broken stream transparently** (`canResume()` is true, and
  it tracks the last key), so "retries stay in the client" — ADR-0041's rule for the write path —
  holds on the read path too, and the connector owns no retry loop.
- **A `Query` is serializable and round-trips with its trailing data intact**, in four shapes
  including a field followed by a later-sorting field: `ObjectInputStream`'s block-data framing ends
  the read its `mergeFrom` would otherwise run past. The trap the design anticipated does not exist.
- **The emulator this project pins answers `SampleRowKeys` for an empty table with no samples at
  all**, and for a populated one with the table's final key plus others at roughly one-in-a-hundred
  probability. (The first differs from the emulator's own current upstream source, which describes a
  single end-of-table marker — the pinned image is older.)
- **`roles/bigtable.admin`, which the E2E service account already holds, covers the source's whole
  RPC set** (`bigtable.tables.readRows`, `bigtable.tables.sampleRowKeys`) and the app-profile
  administration its gated test needs. No infrastructure change.
- **Real Bigtable refuses the two reads the gated suite first assumed it would answer empty**
  (measured 2026-08-10, on the suite's first contact with the service — [#481]): a `ReadRows` range
  whose start is exclusive at its own end key is `INVALID_ARGUMENT` ("start_key must be less than
  end_key" — while the closed-closed single-key range `[K, K]` is answered with that row alone, so
  the refusal is about a range empty by construction, not about `start == end`), and a read whose
  filter names a column family the table does not
  have is `NOT_FOUND` ("Requested column family not found"). The emulator answers both empty with
  no error (measured the same day against the pinned image), so neither refusal could have been
  found anywhere but the gated suite.

## Decision

**A split is one row-key range, and the range is the remaining work.** The enumerator samples the
table once, cuts every configured range at each sampled boundary strictly inside it, and hands the
pieces out one per request. A reader checkpoints its split by truncating that range to start
*exclusively* at the last row it successfully deserialized, including a row that produced no
output, so a restore resumes rather than replays. `ReadRows` has no row offset to resume at — only a
range to ask for — which is what makes the range, and not a count, the unit of progress.

**The cut convention is the client library's**, so a connector split boundary and an SDK one agree:
`endOpen(k)` on the left, `startClosed(k)` on the right, and a boundary equal to a range's start is
not a cut in either bound type while one equal to an inclusive end is. `Query.shard` is used as a
test oracle rather than as the implementation, because a sharded segment may hold several disjoint
ranges when two configured ranges fall in one section, whereas a split holds exactly one — cutting
per range gives a strictly finer plan that never spans a gap the user excluded.

**Overlapping configured ranges are merged, not rejected.** Nested prefixes are easy to write by
accident, and left alone the shared rows land in two splits, which two subtasks read, so a single
*successful* run emits them twice. An **empty** configured range is rejected instead: a range that
reads nothing under a green job is indistinguishable from a job with nothing to read.

**A restore never samples again.** Tablets split and merge while a job runs, so a second sampling
would produce different boundaries under the same ordinal split ids the readers already hold, and
`addSplitsBack` and the restored readers would disagree about which range each id names. The
checkpointed `planned` flag is what prevents it, and it is deliberately not the same statement as
"the pending queue is non-empty": a plan fully handed out must not be recomputed either. (Since
[#452] the assignment protocol around it — the queue, the parking, the closed-guard on the sampling
call — is `base.source.PullAssignmentSplitEnumerator`, shared with the BigQuery source and recorded
in ADR-0083. What stays here is the sampling and the plan.)

**The split reader keeps its own delivered key, beside the split state's emitted key.** They are
two clocks on two threads, and the rows between them are in the element queue: a reopen after a
wake-up must use the reader's, or every in-flight row is handed over twice inside one successful
run; a checkpoint must use the state's, or those rows are dropped on restore.

**The cancelled flag decides whether a stream ended, never the stream's behaviour** — because,
measured above, a cancelled stream both *can* report a clean end and *can* throw. A fetch that ends
either way while cancelled hands over what it read and reopens next time; the same shapes without
the flag are a finished split and a real failure respectively.

**A truncated range may be empty, and the reader finishes such a split without opening a stream.**
That is the normal state of a split whose inclusively-ended range had its last row successfully
deserialized before the checkpoint, and it is also what keeps an inverted range from ever reaching
the service. That
short-circuit is **load-bearing, not tidy**: the gated suite measured what Bigtable answers such a
range with ([#481]), and the answer is `INVALID_ARGUMENT` — a job that let one through would fail,
not read empty — so the unit test pinning the short-circuit asserts zero stream opens rather than
merely an empty fetch. Note the deliberate asymmetry with the builder, which *rejects* an empty
range: user error and normal end-of-split are different things.

**The deserialization SPI is collector-shaped**, as are the other row-oriented source SPIs.
A successfully deserialized row producing zero, one, or many outputs advances the source's input
progress by exactly one row: this source resumes at a row key, while BigQuery records a
consumed-input-row count and Spanner replays an interrupted partition from its start.
The shared synchronous invocation collector in ADR-0108 enforces that boundary before progress can
advance.
On deserialization or downstream failure, this source leaves the row-key progress unchanged; a
partial output can therefore be emitted again when the row is retried.
A Bigtable row is a whole row — many families, many qualifiers, many cell versions — so fanning one
out per qualifier or per cell is a mapping wide-table jobs want. `recordsSkipped` counts rows that
emitted nothing; ADR-0001 is unaffected, its scope being sink serialization SPIs.

**No `Query` in the configuration or the split, and no options object.** The `Query` rule survives
its own evidence collapsing: a checkpointed split must own a byte format this connector controls
rather than one a client upgrade can move; a `Query` cannot be read back (internal target-id
accessor, no row-set accessor, a lossy bound); and it is mutable with a transient builder. A
reflective test asserts no such field exists, because a serialization round-trip would now pass. An
empty-but-reserved `BigtableReadOptions` was declined for a mechanical reason on top of the usual
one: `check-option-docs` globs `*Options.java` and fails outright on a matched class declaring no
setter.

**The client settings both directions build are one helper at the module root.** The
emulator-versus-credentials branch is exactly the code that reads green on a developer machine and
red in CI — the failure ADR-0064 exists to describe — so the sink's batcher factory was refactored
onto it rather than left as a second copy.

### Refinement, [#947] (2026-08-17): a rendered range escapes every structural byte

`RowRanges.format` renders a range for a log line or a message, and `escape` shows printable ASCII
as itself so a text key stays recognisable. Three printable bytes carry structure in that output —
`\` introduces an escape, `*` is the sentinel for an absent bound, and `,` separates the two bounds
— and only the first was escaped. A key holding one of the other two made two different ranges
render as one string: `[a, b, c)` is both "from `a, b` to `c`" and "from `a` to `b, c`", and a
partition ending at the row key `*` rendered exactly like one running to the end of the table.

**This reverses a position the repository had recorded and tested.**
`PendingMergeAccumulatorTest.partitionKeysDoNotAliasLogRenderings` asserted the comma collision
*deliberately*, as the evidence for its real claim — that a partition key is a range and never a
rendering — and [#910] declined making the renderer injective for the same reason: correctness
should not depend on a display form. That reasoning still stands and is unchanged. What it did not
weigh is that these strings are also **what an operator reads to tell two ranges apart**, in the
`StartPositionResolver` retention-expiry warning and in four other messages, at exactly the moment
the distinction matters. Documenting the ambiguity was the alternative, and it costs about the same
work while delivering a caveat instead of a fix.

Escaping the comma is what leaves `", "` occurring exactly once, so the two halves can always be
told apart; with all three escaped the rendering is injective, asserted as a property over an
alphabet containing each of them rather than over bound shapes alone. Only keys *containing* one of
the three render differently.

The property is a readability guarantee and **not a licence to compare renderings**: nothing decides
identity from one, and [#910] is why.

## Alternatives declined

- **An offset-based split.** `ReadRows` has no row offset; there is nothing to count from.
- **A size-weighted planner**, cutting sections into equal byte budgets. The offsets are
  approximate and describe sections rather than rows, and a split finer than a tablet buys nothing
  the service will honour. The estimates are logged once and dropped instead, which is what surfaces
  a skewed table before the job runs.
- **A per-record last-in-split envelope**, so a reader could know a range had ended without a
  further call. It would put a field in every record to save one empty read per split.
- **A single-split fallback when sampling fails.** The client has already retried the transient
  codes under a total timeout of its own, so a failure reaching the enumerator is a permissions,
  quota or configuration problem — and a fallback would turn it into a job that reads the whole
  table on one subtask for reasons nothing reports.
- **Verifying Data Boost.** It needs an Enterprise-edition instance and SPU billing this project's
  account cannot practically provide; [#248] holds the deferral. What ships is the `appProfileId`
  pass-through, and the gated suite asserts a configured profile reaches the wire — which is the
  only claim this project can make. The three facts the documentation states come from Google's own
  pages rather than from the issue that cited them, checked 2026-08-09: the eligible methods are
  `ReadRows`, `SampleRowKeys` and `PingAndWarm` (named by the `data_boost/ineligible_reasons`
  metric's documentation); there is **no guarantee** for data written less than 35 minutes before
  the read, which is stronger than staleness; and traffic above 1,000 read requests per second per
  cluster becomes *ineligible* rather than failing, which a parallel source can reach without any
  error to show for it.

## Consequences

- Parallelism is the service's decision, not the job's: a table stored in few tablets is read by
  few subtasks whatever the parallelism, and the enumerator warns when it plans fewer splits than
  there are subtasks.
- Split planning is exercised by a unit test and by the gated real-GCP suite over a pre-split table,
  and by nothing in between — the emulator models no tablets, so its plans are always one split.
  The failover coverage is scripted for the same reason: one split cannot show a reassignment.
- The per-fetch row cap is a correctness floor rather than a knob, and is reachable only through a
  `@VisibleForTesting` setter. Promoting it to a builder option needs a measurement.

[#34]: https://github.com/laughingman7743/flink-connector-gcp/issues/34
[#216]: https://github.com/laughingman7743/flink-connector-gcp/issues/216
[#248]: https://github.com/laughingman7743/flink-connector-gcp/issues/248
[#452]: https://github.com/laughingman7743/flink-connector-gcp/issues/452
[#481]: https://github.com/laughingman7743/flink-connector-gcp/issues/481
[#587]: https://github.com/laughingman7743/flink-connector-gcp/issues/587
[#910]: https://github.com/laughingman7743/flink-connector-gcp/issues/910
[#947]: https://github.com/laughingman7743/flink-connector-gcp/issues/947
