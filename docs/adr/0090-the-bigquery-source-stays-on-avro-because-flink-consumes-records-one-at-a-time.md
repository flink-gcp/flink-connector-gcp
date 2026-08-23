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

# ADR-0090: The BigQuery source stays on Avro, because Flink consumes records one at a time

- Status: Accepted
- Date: 2026-08-11 (measured 2026-08-10)
- Issues: [#393], [#64]
- Modules: bigquery (`source`)
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md` § Source

## Context

The Storage Read API serves rows in either Avro or Arrow, and [#64] split the second as [#393]. That
issue was opened already sceptical and said so: the Spark connector's CHANGES.md claims Arrow
"provides read performance faster by 40% then Avro", and it earns that by handing Arrow batches to
Spark as `ColumnarBatch`es with no per-row conversion — while Beam supports Arrow and then converts
each row into a `GenericRecord`, which spends the gain. So [#393] required a measurement against the
shipped Avro path before anything was built, and named a null result as a legitimate outcome.

ADR-0079 shipped that Avro path and left a door open for this one: as it read before this record, a
batch-aware Arrow variant was to arrive as a sibling abstract class rather than as a widening of
`BigQueryRowDeserializer` (now `BigQueryRowDeserializationSchema`), one call per row being what the
resume offset rests on. This record closes that door and says on what evidence.

## Decision

**The source reads Avro, and Arrow is not offered — not as a default, and not as an option.**

The decision rests on Flink's execution model rather than on the measurement, which came out
*favourably* for Arrow on the half a decoder controls. Flink processes one record at a time, and
every part of Arrow's advantage lives in not doing that.

**Do not re-propose an Arrow deserializer without engaging the evidence here.** "Arrow is faster" is
not the claim this record refutes — it is the claim this record *confirms*, and then shows to be
unreachable. A re-proposal therefore has to exhibit a consumer that takes a batch without
materialising a row from it, which today means a Flink API that accepts one; a faster decode number
is not that, because this record already has one.

## Evidence

Measured 2026-08-10 against BigQuery, `bigquery-public-data.usa_names.usa_1910_current`, every
column, one stream, 2,000,000 rows per pass, two warm-up rounds discarded and five measured with the
arm order rotated each round. macOS aarch64, JDK 17.0.20, 2 GiB maximum heap; arrow 17.0.0, avro
1.12.1, google-cloud-bigquerystorage 3.30.0. Each format was read once and its bytes replayed
through the decoders, because at this fixture's size an end-to-end timing measures the network
rather than either decoder.

**The decision rule was registered on [#393] before the run**, so its thresholds were not fitted to
the numbers. It had three arms and a veto: close if the batch reader came within ±15% of the Avro
baseline on decode and improved allocation by less than 2x; build the sibling SPI if it beat the
baseline by ≥30% or ≥2x *and* the per-row Arrow arm did not; re-cut the scope if that per-row arm
also won. The veto: **any build outcome additionally required Arrow's uncompressed wire bytes per
row to be no worse than Avro's +20%.** The run cleared the build bar on decode and **failed the
veto**, at +84%.

Full per-arm and per-round numbers, the arm definitions and the metric caveats are on [#393]; that
issue is the specification, and this section carries what the decision turns on.

**Arrow decodes far better, and only when nothing per-row is built.**

| Arm | ns/row | Heap bytes/row | Against the Avro baseline |
|---|---:|---:|---|
| Avro, the shipped `AvroRowCursor` | 140.0 | 260.0 | — |
| Arrow, fields read off the loaded vectors | 75.4 | 3.5 | **46% faster, 74x less garbage** |
| Arrow, materialised into a `GenericRecord` per row | 187.1 | 261.5 | **34% slower, and the garbage is back** |
| Arrow with LZ4 buffer compression, off the vectors | 189.6 | 369.0 | 35% slower, 42% more garbage |

The third row is the finding, not the second. Materialising one object per row — which is what a
record-at-a-time runtime requires — is slower than decoding Avro straight into that object, and
costs the same garbage, so **both** of row two's advantages belong to a consumer that never asks for
a row. That is Beam's trap reproduced.

Two guards make those numbers admissible rather than merely plausible. Every arm folded each column
of each row into a `long` checksum and all five checksums matched, which is what says no arm's
decode was optimised away and all of them read the same rows from the same snapshot. And a
deliberately degraded Avro arm was carried as a firing control: it allocated 561,124,512 bytes
against the baseline's 520,020,616, a gap wider than the 40,313,663 bytes its per-block copy alone
accounts for — without which "no difference" between any two arms would have been inadmissible.

**Arrow costs more on the wire, and not because of the fixture.** Bytes per row, over the same table
and the same day, for every column and for a projection of each column kind (500,000 rows).
Percentages are the harness's, computed on unrounded byte counts, so they will not reproduce exactly
from the rounded figures beside them:

| Columns | Avro | Arrow | Arrow+LZ4 | Arrow vs Avro | Arrow+LZ4 vs Avro |
|---|---:|---:|---:|---:|---:|
| every column | 20.2 | 37.1 | 17.1 | +84% | −15% |
| the three string columns | 15.0 | 21.1 | 16.3 | +40% | +8% |
| the two integer columns | 5.1 | 16.0 | 0.9 | +212% | −83% |

The integers are Arrow's worst case — a fixed eight bytes against Avro's varint — but the strings
alone are +40%, so the penalty is not an artefact of this table. LZ4 undoes it on every column and
on the integers, and **does not** on the strings, where it is still 8% above Avro.

**The bandwidth at which each format wins.** Per stream, per two million rows, and it depends on
whether the reader overlaps transfer with decode. Serialised, the crossover is Δbytes over Δdecode;
pipelined, the reader is bound by `max(transfer, decode)` and the crossover moves:

| | Serialised | Pipelined |
|---|---:|---:|
| Arrow (off the vectors) beats Avro above | 263 MB/s | 265 MB/s |
| Arrow+LZ4 beats Avro below | 63 MB/s | 106 MB/s |

**Those crossovers belong to the arm that cannot be shipped.** The arm a record-at-a-time connector
could actually ship is the third one, and it is 34% slower to decode *and* 84% larger on the wire —
worse on both axes, so it loses to Avro at every bandwidth including an unbounded one. There is no
crossover for the shippable configuration, which is why an in-region end-to-end measurement could
not change the outcome. This run sat at 17.9 MB/s, from a laptop outside GCP; that number is
reported as a limitation of the end-to-end samples, not as the answer.

Two objections the crossover model would otherwise leave standing, both raised on [#393] and both
answered by the table above. The allocation win *is* bandwidth-independent and would survive in
every regime — but only in the second arm: materialising a row costs 261.5 heap bytes against Avro's
260.0, so it evaporates exactly where the decode win does. And decode does not have a CPU to itself
on a saturated TaskManager running many readers — which counts against Arrow here, since the
shippable arm spends more CPU per row than Avro, not less.

**Flink's own relationship with Arrow**, checked 2026-08-11. The ecosystem survey on [#393] recorded
its leads as unverified; this is what checking them established:

- `flink-table-common` 2.2.1 carries the columnar data structures — `org.apache.flink.table.data.columnar`
  with `ColumnarRowData`, the `ColumnVector` family and `VectorizedColumnBatch` — and every one of
  them is `@Internal`.
- **Neither `flink-table-common` 2.2.1 nor `flink-table-runtime` 2.2.1 contains a single Arrow
  class.** Flink's Arrow↔`RowData` bridge — `ArrowUtils`, `ArrowWriter` and `ArrowReader` in
  `org.apache.flink.table.runtime.arrow` — lives in `flink-python`, serving the PyFlink UDF
  boundary, where Arrow is a *serialization format* crossing JVM↔Python rather than something the
  runtime computes over. It is not on the table-API classpath, so a connector could not reuse it
  even for the Table API case.
- The dev@ thread [*\[DISCUSS\] Add support for Apache Arrow format*][discuss] (March 2023) proposed a
  `flink-arrow` **format** module converting `VectorSchemaRoot` ↔ `RowData`. Its proposer said it
  "solely aims to introduce flink-arrow as a new format" and "will not impact the internal data
  structure representation in Flink"; the thread went on to question whether the demand for it
  existed, and no FLIP resulted.
- Columnar execution for Flink is pursued out of tree. [Iron Vector] targets Flink SQL and keeps
  record-at-a-time semantics over a columnar layout; [Auron] describes itself as an accelerator for
  big data engines including Flink, but its working integration and its documentation are Spark's,
  with the Flink extension still in progress.

None of that is a runtime that consumes columnar records, and the last two are evidence about how
hard that boundary is rather than about it moving.

**Incidental facts the run established**, none of which had been asked before:

- BigQuery honours `ArrowSerializationOptions` LZ4_FRAME, and leaves
  `ReadRowsResponse.uncompressed_byte_size` unset even when it does — so a compression ratio has to
  be computed against a separate uncompressed read rather than read off the response.
- Allocating any Arrow buffer needs `--add-opens=java.base/java.nio=ALL-UNNAMED`; without it
  `MemoryUtil` fails in a static initialiser (arrow-memory 17.0.0). Any Arrow path therefore imposes
  a JVM option on every TaskManager, failing far from where the option was chosen.
- **Avro's stream decoder allocates a fresh 8 KiB buffer on every `configure`**, so the shipped
  `AvroRowCursor` pays one per response block. This broke the first version of the firing control,
  on a 20,000-row offline fixture: a degraded Avro reader built on the array-backed decoder *saves*
  that buffer per block while spending its block copy, leaving 107 KB of separation where the copy
  alone was 207 KB. Both arms were rebuilt to read through a stream, which is the form the numbers
  above were taken with.
- Surefire's `argLine` is set by `flink-connector-parent`, so `-DargLine` on the command line is
  silently ignored. `JDK_JAVA_OPTIONS` is what reaches the forked JVM.

## Alternatives declined

- **Ship the batch-aware sibling SPI anyway, with Avro as the default.** Weighed seriously, since
  the decode result clears the pre-registered build bar and offering a choice with the numbers
  attached has real value. Declined on what the option would cost to keep against what it would
  deliver: a second deserializer SPI, the split's schema representation and a third serializer
  version with restore compatibility, a second cursor, a `RecordsWithSplitIds` owning off-heap
  buffers with a release hook, a second record emitter, a branch in `createReader`, format-aware
  metrics and builder validation — for an option whose honest documentation would have to say that
  the only shape a Flink job can consume is slower than what it replaces. It would also introduce a
  failure mode this connector cannot currently have: a batch handed to user code is a view over
  allocator-owned memory, so a deserializer retaining it past the call reads freed memory — a crash
  or silent corruption rather than an exception, on a path few users take and therefore few
  exercise.
- **Ship LZ4 only, for the payload.** It is the fastest configuration end to end on a slow link and
  the only one that beats Avro on every-column bytes, so this is the closest call here. Declined on
  deployment shape rather than on the data: the regime where it wins is a client reading from
  outside GCP, a TaskManager reading in-region is not in it, and the 35% decode and 42% allocation
  regressions are paid by *every* job that selects it while the byte saving reaches only the
  out-of-region minority. A knob whose sign flips with link speed is a support burden rather than a
  choice, and it needs `arrow-compression` as a runtime dependency with the licence and uber-jar
  work that implies.
- **Measure end to end from inside GCP before deciding.** The one number that could overturn a
  crossover argument is per-stream throughput to an in-region TaskManager. Declined because the
  shippable arm has no crossover to overturn, per the Evidence above.
- **Wait for a BigQuery Table API source and decide then.** The `ColumnarRowData` case is genuinely
  stronger there — that path pays decode *and* a conversion, so a columnar reader could avoid both.
  Declined as a *reason to hold this issue open*: there is no such source yet, the adapters would be
  ours to write regardless (Flink's live in `flink-python`), and an SPI committed now against the
  weaker half would constrain that design rather than serve it. If that source is built, this
  decision is re-examined there.

## Consequences

- ADR-0079's forward reference is resolved: the sibling abstract class it anticipated is not being
  written, and its paragraph now points here.
- The connector's Arrow dependencies remain what ADR-0035 already accepted: arrow, netty and
  flatbuffers bundled in the SQL uber-jar for a code path never run. The DataStream half of [#64]
  turns out not to need them back; the Table API case above is undecided.
- No `--add-opens` is imposed on any deployment.
- The measurement harness was not kept. It was built on a throwaway branch; what decides the answer
  is recorded above and on [#393] — the capture-once-replay-many design, the arms, the two guards,
  the fixture and the pinned versions — and its numbers would need re-taking rather than re-running
  if this is re-examined, so a paid gated benchmark for a feature that does not exist would be
  maintenance for nothing.

[#64]: https://github.com/flink-gcp/flink-connector-gcp/issues/64
[#393]: https://github.com/flink-gcp/flink-connector-gcp/issues/393
[discuss]: https://www.mail-archive.com/dev@flink.apache.org/msg65824.html
[Auron]: https://github.com/apache/auron
[Iron Vector]: https://irontools.dev/blog/introducing-iron-vector/
