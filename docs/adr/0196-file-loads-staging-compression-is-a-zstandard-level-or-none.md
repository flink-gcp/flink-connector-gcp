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

# ADR-0196: FILE_LOADS staging compression is a zstandard level or none

- Status: Accepted
- Date: 2026-11-05
- Issues: [#2022], [#283] (the zstandard default), [#2019] (the conversion passes whose removal made
  compression a larger share of a record)
- Modules: bigquery (`sink.fileloads`, `table.sink`)
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md` § File loads, Compression

## Context

[#283] chose zstandard at level 3 for Avro staging files on CPU grounds, and the Avro codec was not configurable.
Parquet staging offered `ZSTD` and `NONE` through `parquetCompression`, with no level.

The right setting depends on the job.
Staging objects are written once, read once by a load job and deleted, so the codec trades the writer's task-thread CPU against the bytes uploaded to Cloud Storage and, when the staging bucket and the dataset are in different regions, against transfer volume.
A job short of CPU and one short of upload bandwidth want opposite settings.
After [#2019] removed two protobuf passes from each record, compression is a larger share of what remains.

BigQuery load jobs read zstandard and uncompressed Avro, which the gated load test in this change measures at the offered settings, and deflate Avro, which this writer staged before [#283]; [#2022] lists snappy as accepted too, and it was not measured here.
Only zstandard is on this connector's classpath as a native codec; deflate is in the JDK, and snappy would need `snappy-java` and its native libraries in the shaded distribution.

## Decision

**Avro staging offers `avroCompression(ZSTD | NONE)`, and one `zstdLevel` applies to every staging file written with zstandard.**
`ZSTD` at level 3 remains the default for both formats, so existing jobs and their serialized options behave as before; an options instance serialized before these fields existed restores them as the defaults.
The level accepts -131,072 (zstandard's fastest negative level) to 19, except 0, which zstandard reads as its default level and which 3 states plainly.
19 is the highest level zstandard offers without its "ultra" opt-in, and the cap is about memory rather than CPU.
Avro's `ZstandardCodec` builds a new compression stream for every block, and parquet-hadoop's compressor one for every page, so each block or page allocates a fresh zstandard context.
`ZSTD_estimateCStreamSize` (libzstd 1.5.5, whose level table matches the bundled zstd-jni 1.5.7) puts that context at 3.5 MiB at level 3, 89.5 MiB at 19, 193.5 MiB at 20 and 777.6 MiB at 22, all native memory outside Flink's budget.
The Table API exposes `sink.file-loads.avro-compression` and `sink.file-loads.zstd-level`.

**Every combination that would be ignored is rejected at build time.**
`avroCompression` is rejected under Parquet, as `parquetCompression` already is under Avro, and `zstdLevel` is rejected when the selected codec is `NONE`.
A Parquet sink still stages Avro for a destination with a `JSON` column; that file is zstandard at the shared level, because `avroCompression` cannot be set under Parquet.

The level reaches Parquet through `parquet.compression.codec.zstd.level`, which parquet-hadoop's `ZstandardCodec` reads from the Hadoop configuration its `CodecFactory` derives from the writer's `PlainParquetConfiguration`.
A writer test holds that the key arrives: the same rows stage smaller at level 19 than at -5.

## Evidence

A probe of the writer's per-record cost (`FileLoadsWriterRecordCostProbe`), with the codec swapped in a scratch build, measured a nested row of about 9.5 KB with 120 repeated sub-messages and three additional fields.
Each codec ran in its own JVM, twice, in forward and then reverse order; the table gives both medians.

| Avro codec | µs per record | Staged bytes per record |
|---|---:|---:|
| none | 111-115 | 8,745 |
| zstandard -5 | 120-128 | 6,240 |
| zstandard -3 | 122-124 | 6,021 |
| snappy | 123-131 | 6,301 |
| zstandard 1 | 135-136 | 4,796 |
| zstandard 3 | 135-136 | 4,778 |
| zstandard 6 | 176-181 | 4,752 |
| deflate 1 | 185-186 | 5,177 |
| deflate 6 | 333-367 | 4,843 |

On this row, compression at the default level costs about a sixth of the record, and levels above it buy almost no size.
The negative levels and no codec at all are the points worth choosing for a CPU-bound job.

## Alternatives declined

- **Offer snappy.**
  zstandard at -3 matched its CPU with 4% fewer bytes on the measured row, and on [#283]'s flatter mix snappy staged 1.21 times the bytes of zstandard at level 3 for 0.81 times its write time; the negative levels were not measured on that mix.
  It would add `snappy-java` with native libraries, shading and licence curation to the SQL distribution for no setting zstandard lacks.
- **Offer deflate.**
  It was slower and larger than zstandard at every level measured here and on [#283]; it adds no dependency, but also no useful point on the trade-off.
- **One codec option shared by both formats.**
  `ParquetCompression` is `@Public` and frozen, and the two formats' codec sets differ: Parquet's `NONE` is about the Hadoop runtime, not CPU.
- **A level per format.**
  Both formats use the same zstandard library and the same level scale; two knobs would let one sink mean two things by "level".

[#283]: https://github.com/flink-gcp/flink-connector-gcp/issues/283
[#2019]: https://github.com/flink-gcp/flink-connector-gcp/issues/2019
[#2022]: https://github.com/flink-gcp/flink-connector-gcp/issues/2022
