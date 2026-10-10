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

# ADR-0072: Parquet staging is opt-in, batch-shaped, and brings its own dependencies

- Status: Accepted
- Date: 2026-08-08; revised by [#1704] (2026-10-10)
- Issues: [#284] (measurements on [#281] and [#285]), [#1313] (in-region re-measurement), [#1704]
  (one row group per Parquet file)
- Modules: bigquery (`sink.fileloads`)
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md` § File loads
- Evidence: [in-region tuning findings](evidence/0146-fileloads-in-region-tuning-1313.md)

## Context

[#284] opened by deciding to **stage Parquet by default**, falling back to Avro only for `JSON`
columns, on measurements showing Parquet loading 2.4-2.5x faster and staging 0.71x the bytes. Two
of that decision's four premises did not survive re-measurement.

## Decision

**Parquet is an opt-in `FileLoadsOptions.stagingFormat`, defaulting to Avro, with its dependencies
`provided` rather than shipped.** `parquetCompression` selects `ZSTD` (default) or `NONE`, and is
rejected under Avro rather than ignored.

Four things drove it, all measured 2026-08-08 against real BigQuery unless stated:

- **A 256 MiB step.** A Parquet load of less than 256 MiB of *total input* takes 3-5x longer than
  one just above it — ~150 MiB in 13.4-16.7 s against Avro's 6.0 s, ~250 MiB in 17.1-23.4 s against
  6.7 s, then 4.7 s at 262 MiB. Verified independent of file count (7-38) and of bytes per file
  (8/16/32 MiB); Avro is flat across the same range. Streaming FILE_LOADS commits one load per
  checkpoint, so most streaming jobs would sit permanently below it — the case [#284]'s own
  rationale called decisive is where Parquet loses by the largest margin. The size of the step
  depends on the rows: three in-region runs on 2026-10-10 ([#1313], [#1704]), across one 1 KiB
  `BYTES` column and 32 or 64 string columns per row with two to five loads per cell, put Parquet's
  median load at 1.3-2.8x Avro's below 256 MiB. Above it, measured only for the rows with one
  `BYTES` column, the first run found 1.2-1.3x and the second 0.8-0.95x.
- **The rule that follows cannot be automatic.** The quantity that decides the format is the load
  job's total input, known at *commit* time; the format is fixed at *write* time, and one load job
  cannot mix formats. A per-subtask estimate would let two subtasks disagree for the same
  destination and checkpoint. So the choice moves to the user, who knows the deployment's volume.
- **"No Hadoop" was false.** `parquet-avro` declares its `hadoop-*` dependencies `provided`, so a
  dependency tree shows none — but compressed Parquet cannot be written without Hadoop classes at
  runtime: `CodecFactory.getCodec` is Hadoop's `CompressionCodec` SPI, and it fails for gzip and
  snappy exactly as for zstd. Only `UNCOMPRESSED` escapes, and it stages **1.21x** the bytes of
  Avro/zstd (local measurement), so the Hadoop-free path costs more than the format it replaces.
  Hadoop's `Configuration` is also genuinely instantiated and parses `core-site.xml` off the
  classpath, which is a coupling no user should get without asking.
- **What survives.** 0.785x staged bytes, flat across a 64x range of file sizes — the 20% inflation
  at small files reported earlier did not reproduce and is a property of a row shape dominated by
  dictionary-compressible columns. The ratio is a property of the row shape too: on 2026-10-10 one
  1 KiB `BYTES` column staged the same bytes in both formats, 32 random-text columns 1.08x, and 64
  dictionary-friendly columns 0.52x ([#1313], [#1704]). And the `JSON`
  constraint, which is not a preference: a `PARQUET` load is refused at job-configuration level
  whenever the provided schema names one.

What Parquet gains therefore depends on rows the connector cannot inspect, which is one more reason
the choice stays with the user, who should measure it on the deployment's own rows.

## Consequences

**The `JSON` fallback stays automatic** and is decided where the destination's schema is first
resolved — with a per-record destination resolver the full set of schemas is not known at graph
construction. Logged once per destination, because a user who asked for Parquet and silently got
Avro has no other way to find out.

**The dependencies are `provided` and probed on the client.** `FileLoadsOptions.build()` resolves
`AvroParquetWriter` — and `org.apache.hadoop.conf.Configuration` unless the codec is `NONE` — so a
missing artifact fails at graph construction naming what to add, rather than as a
`NoClassDefFoundError` on a TaskManager when the first staging file is opened. A client whose
classpath differs from the cluster's defeats that, which is why the docs name the artifacts too.
`ParquetStagedFileWriter` is referenced only from the `PARQUET` branch, so a deployment that never
selects it never resolves the class.

**Parquet's row-group size comes from `maxStagingFileBytes`**, and that is correctness rather than
tuning: Parquet buffers a whole row group before anything reaches the stream, so at its own 128 MiB
default no row group would flush before close, and a 16 MiB threshold, which the writer applies at
row-group boundaries, would never fire. Affordable because row-group count was measured not to affect load
duration (1/3/5/11 groups per 32 MiB file: 7.5-8.0 s, ADR-0070's run).

**A Parquet file rolls at the row-group boundary it expects to be nearest the threshold**
([#1704]): once the bytes written are within half the last row group of `maxStagingFileBytes`. The
expectation is that the next group will be the size of the last. The byte count alone does not
work, because Parquet sizes a row group from an estimate that counts each column's open page
uncompressed, preferring to land under its target. Measured on 2026-10-10 by writing rows through
the production writer at 16 MiB, a row group was 15.9 MiB for one incompressible 1 KiB column,
13.6-14.6 MiB for 8 string columns, 8.1-9.6 MiB for 32, and 0.69 MiB for 64 dictionary-friendly
columns. Rolling on the byte count made every file of the first two shapes take a second group, at
1.7-2.0x the threshold, which is what [#1313] saw in-region at about 30 MiB; rolling after one
group would have left the last shape at 0.7 MiB files. The rule closed every shape's files at
13.6-19.3 MiB, and in-region, across four shapes including the 32- and 64-column ones, the average
full Parquet file stayed between 14.3 and 16.9 MiB. Rows whose compressibility shifts within a file
can grow the next group and carry a file further past the threshold: on 2026-10-10, eight files
each of one `BYTES` column alternating between zero and random payloads closed at up to 1.06x the
threshold with 2,000-row blocks and 1.20x with 20,000-row blocks, measured locally.

**The converters are shared, so the constraints are too.** Both formats are written from the same
Avro schema, so `TableSchemaToAvroConverter`'s rejections — `INTERVAL`, `RANGE`, and BigQuery
flexible column names — apply identically. [#281] asked whether Parquet lifts the flexible-name
restriction; under converter reuse it does not, and lifting it would mean a direct
`TableSchema` → `MessageType` converter, which is abandoning the reuse rather than a detail of it.

The load-job side of this — the format travelling in the committable and load jobs grouping on it —
is ADR-0018, refined there rather than repeated here.

[#281]: https://github.com/flink-gcp/flink-connector-gcp/issues/281
[#284]: https://github.com/flink-gcp/flink-connector-gcp/issues/284
[#285]: https://github.com/flink-gcp/flink-connector-gcp/issues/285
[#1313]: https://github.com/flink-gcp/flink-connector-gcp/issues/1313
[#1704]: https://github.com/flink-gcp/flink-connector-gcp/issues/1704
