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

# Bigtable staged-state sizing on 2026-09-07

This local experiment informs the Flink-state choice in [ADR-0163](../0163-bigtable-checkpointed-writes-stage-immutable-mutations-and-retain-row-markers.md).
It uses the test-only staged sink and actual Flink writer/committer operators, without a Bigtable client or credentials.
It is not a production-runtime, service-throughput or process peak-memory measurement.

## Setup and reproduction

Each cell ran in a fresh JDK 17 JVM on an arm64 workstation with `-Xmx768m -XX:+UseSerialGC`, Flink 2.2.1 and the repository's `libraries-bom` 26.87.0.
The wire model is Bigtable SDK/protos 2.82.0.
The standalone entry point is `BigtableStagedStateSizeProbe` under the Bigtable module's test sources.
After compiling the module's tests, launch it with the Surefire report's `surefire.test.class.path` as its classpath and arguments `1024 100`, `1024 1000`, `65536 100`, and `65536 1000`, in separate JVMs.
Its required JVM opens are `--add-opens=java.base/java.lang=ALL-UNNAMED` and `--add-opens=java.base/java.util=ALL-UNNAMED`.
The probe prints one `SIZING` CSV line and cleans its temporary checkpoint directory.

Each interval has N distinct requests, each with a newly allocated payload, an eleven-character row key, a fixed destination/profile and one marker.
Two intervals are staged without notifying completion.
Snapshot 1 therefore holds N requests and snapshot 2 holds 2N, including the first interval's pending work.
Both snapshot handles and all requests remain reachable during sampling.
After taking the snapshots, the probe serializes and deserializes each of the 2N requests into another retained list, modeling overlapping restored payload objects without running a committer or a service fake.
That last phase measures serializer-copy overlap; it is not a complete Flink restore-time heap trace.

Checkpoint storage is `FileSystemCheckpointStorage` in a fresh local temporary directory.
The initial run used the harness's default memory-backed storage and failed at its 5 MiB state limit for 64 KiB × 100 entries (`Size=6576019, maxSize=5242880`).
The completed matrix uses filesystem storage for every cell, including the smaller inputs; it does not combine measurements from different storage configurations.

## Observations

All numbers below are bytes.
Wire bytes cover both intervals together; snapshot sizes are Flink's `OperatorSubtaskState.getStateSize()`.
Heap samples come from `MemoryMXBean` after `System.gc()` with the specified Serial GC; collection and heap layout remain JVM-dependent, and these four samples establish no universal memory bound.

| Payload | Entries per interval | Total request wire bytes | Snapshot 1 | Snapshot 2 | Baseline heap | Heap with pending requests and snapshots | Heap with additional deserialized copies |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 KiB | 100 | 244,400 | 124,519 | 248,767 | 19,862,968 | 20,774,312 | 21,213,336 |
| 1 KiB | 1,000 | 2,444,000 | 1,242,319 | 2,484,367 | 21,295,272 | 26,606,816 | 30,987,256 |
| 64 KiB | 100 | 13,147,400 | 6,576,019 | 13,151,767 | 19,865,464 | 60,037,088 | 73,378,512 |
| 64 KiB | 1,000 | 131,474,000 | 65,757,319 | 131,514,367 | 19,864,320 | 286,098,208 | 419,582,928 |

The largest cell retains 125 MiB of input payload across its two intervals.
Its heap above baseline is about 254 MiB with pending requests/snapshots and about 381 MiB after keeping deserialized copies too.
This shows why a serialized-byte admission charge cannot be called a heap cap.
The probe retained protobuf request objects; replacing that representation with byte arrays would need a new measurement, not an inferred lower multiplier.

For this fixed resource/key shape, a request's wire size is 1,222 bytes for a 1 KiB payload and 65,737 bytes for 64 KiB.
The proposed 256-byte accounting allowance makes the admission charges 1,478 and 65,993 bytes respectively.
A 64 MiB per-interval budget therefore admits at most 45,405 or 1,016 of these requests, before the independent 100,000-entry cap.
Different resource/key/mutation shapes change these counts.
At an assumed 1,000 inputs/s per writer, 64 KiB inputs over a 60-second interval would require about 3.96 GB of admission budget per interval, before accounting for older pending intervals and snapshot/restore heap.
That arithmetic is an extrapolation, not a measured supported workload.

Every applied envelope also leaves a marker in Bigtable.
The qualifier and value alone contain 33 logical bytes, excluding family/row keys, timestamps, storage/index metadata, replication and compression.
At an assumed 10,000 inputs/s continuously, that is 864 million marker cells and 28.512 GB of qualifier/value bytes per day across the target, before those other costs.
These are lower-level logical counts, not physical or billable storage estimates.
Stage 2 must measure actual storage and hot-row growth; the local probe cannot price them.

## Decision supported by this measurement

Flink state can carry the tested request shape and pending intervals on the selected filesystem storage, including the largest 2,000-request case.
Adopt it for the initial mode with configurable entry/byte admission limits, observable collector backlog and an explicit heap-sizing requirement.
This does not establish acceptable cost at every input rate or checkpoint interval.
The full Stage 2 matrix, checkpoint storage performance, production factory recovery and supported workload limits remain unmeasured.
