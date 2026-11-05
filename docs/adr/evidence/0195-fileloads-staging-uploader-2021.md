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

# BigQuery FILE_LOADS background staging uploader: measurements

This records the in-region measurements behind [ADR-0195](../0195-file-loads-offers-an-opt-in-background-staging-uploader.md) for [#2021](https://github.com/flink-gcp/flink-connector-gcp/issues/2021), run on 2026-11-04.
They ask how much of the FILE_LOADS writer's thread the staging upload takes, and how much of it the background staging uploader, the upload chunk size and checkpoint finalization concurrency each recover.
Times are UTC.

## Summary

- **With one or two destinations, the uploader at two operations made the writer 14–35% faster.**
  The share of writer-thread samples inside the Cloud Storage client fell from 29–35% to 8–12%.
- **Two operations are enough.**
  Four was within 0.04 of two in eight of nine cells and 0.08 faster in the ninth; one left the writer thread waiting for the permit in 9–14% of samples with one or two destinations, and was slower than two in every such cell.
- **The chunk stays at 4 MiB.**
  With the uploader, 4 MiB was the fastest or within about 1% of it at two operations for every destination count (two destinations: 24.24 MiB/s at 4 MiB against 24.48 at 8 MiB).
  Without it, 16 MiB was 7–10% faster than 4 MiB for every destination count, at four times the per-file buffer.
- **With 16 destinations, the uploader alone did not help, and with concurrent checkpoint finalization it did.**
  Alone it measured 0.94–1.07 of the off cell, with about 43% of samples still in Cloud Storage, because each file held about one chunk per checkpoint and was uploaded by the checkpoint's own finalization.
  `maxConcurrentCheckpointFinalizations` 8 alone gave 1.19, and both together 1.34; with two destinations both together gave 1.38.

## Method

The probe is `FileLoadsWriterStagingUploadProbe`, a hand-run main in the BigQuery module's test sources.
It builds the production `FileLoadsWriter` through the public sink builder and drives it from one thread, which stands in for the task thread, against the production `GcsStagingStorage` and a bucket in the same region.
No BigQuery load job runs: the probe measures staging only, and deletes every object it staged after each run.

- **Rows**: [#2019](https://github.com/flink-gcp/flink-connector-gcp/issues/2019)'s nested event, 9,482 serialized bytes with 120 repeated sub-messages, plus three additional fields, cycling through 256 distinct events, each routed to the destination its id hashes to, so destinations received similar but not equal volumes.
  Avro with Zstandard staged them at about half their serialized size.
- **Volume**: 512 MiB of serialized rows per run, with a `prepareCommit` every 128 MiB and one at the end; the default 16 MiB `maxStagingFileBytes`.
- **Measures**: the writer thread's wall and CPU time from the first write to the last `prepareCommit`; input MiB per second; and stack samples of the writer thread every 5 ms through `Thread.getStackTrace()`, classed as waiting for an uploader permit, inside the Cloud Storage client or its sockets, otherwise runnable, or otherwise waiting.
  The samples share a thread dump's safepoint bias, and the peak heap is the largest used heap the sampler saw, garbage included.
- **Order**: two discarded warm-up runs, then every cell once per round in a fresh random order, five rounds.
- **Environment**: one `e2-standard-8` VM in `us-central1-a`, the bucket's region, Debian 12, OpenJDK 17, `-Xmx8g`, G1.
  The connector was this change's head `d981d3845635b8ed9b625959b644d49726571e4a` with the probe added, before the option was split into `backgroundStagingUploadEnabled` and `maxInFlightUploadOperations`; the probe expressed "off" as an operation count of 0, which that split does not change at run time.

## Run 1: operations, chunk size and destinations

16:26:32 to 17:44:32, 36 cells, 180 runs, `maxConcurrentCheckpointFinalizations` 1.

| Destinations | Chunk (MiB) | Operations | Input MiB/s, median (range) | vs. off | Writer CPU share | Cloud Storage samples | Permit-wait samples | Peak heap (MiB) |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 4 | off | 19.31 (18.3–19.7) | 1.00 | 0.65 | 0.35 | 0.00 | 251 |
| 1 | 4 | 1 | 24.96 (22.4–25.8) | 1.29 | 0.81 | 0.08 | 0.09 | 315 |
| 1 | 4 | 2 | 26.06 (23.8–26.7) | 1.35 | 0.86 | 0.08 | 0.00 | 287 |
| 1 | 4 | 4 | 25.71 (25.1–27.0) | 1.33 | 0.87 | 0.09 | 0.00 | 247 |
| 1 | 8 | off | 20.41 (18.1–21.8) | 1.00 | 0.69 | 0.31 | 0.00 | 255 |
| 1 | 8 | 1 | 24.12 (21.5–24.8) | 1.18 | 0.80 | 0.07 | 0.10 | 377 |
| 1 | 8 | 2 | 25.73 (25.1–26.3) | 1.26 | 0.85 | 0.08 | 0.01 | 303 |
| 1 | 8 | 4 | 25.53 (24.0–26.9) | 1.25 | 0.86 | 0.09 | 0.00 | 323 |
| 1 | 16 | off | 21.19 (20.5–21.7) | 1.00 | 0.71 | 0.29 | 0.00 | 267 |
| 1 | 16 | 1 | 22.57 (20.8–23.4) | 1.07 | 0.76 | 0.07 | 0.14 | 363 |
| 1 | 16 | 2 | 24.62 (22.3–25.8) | 1.16 | 0.84 | 0.08 | 0.00 | 383 |
| 1 | 16 | 4 | 25.38 (21.7–26.1) | 1.20 | 0.86 | 0.07 | 0.00 | 375 |
| 2 | 4 | off | 19.02 (17.3–19.6) | 1.00 | 0.65 | 0.34 | 0.00 | 287 |
| 2 | 4 | 1 | 22.27 (21.4–23.9) | 1.17 | 0.76 | 0.10 | 0.12 | 289 |
| 2 | 4 | 2 | 24.24 (21.2–25.1) | 1.27 | 0.83 | 0.10 | 0.03 | 397 |
| 2 | 4 | 4 | 25.76 (24.2–26.4) | 1.35 | 0.85 | 0.11 | 0.00 | 381 |
| 2 | 8 | off | 20.33 (18.2–20.8) | 1.00 | 0.68 | 0.32 | 0.00 | 287 |
| 2 | 8 | 1 | 23.29 (23.0–23.9) | 1.15 | 0.78 | 0.10 | 0.09 | 383 |
| 2 | 8 | 2 | 24.48 (23.2–24.7) | 1.20 | 0.82 | 0.11 | 0.03 | 453 |
| 2 | 8 | 4 | 24.38 (23.6–25.3) | 1.20 | 0.84 | 0.13 | 0.00 | 355 |
| 2 | 16 | off | 20.89 (20.2–21.6) | 1.00 | 0.71 | 0.29 | 0.00 | 343 |
| 2 | 16 | 1 | 22.57 (21.7–23.0) | 1.08 | 0.75 | 0.11 | 0.11 | 419 |
| 2 | 16 | 2 | 23.75 (22.9–24.6) | 1.14 | 0.80 | 0.12 | 0.04 | 443 |
| 2 | 16 | 4 | 23.99 (23.8–24.8) | 1.15 | 0.82 | 0.12 | 0.00 | 469 |
| 16 | 4 | off | 15.41 (14.0–15.9) | 1.00 | 0.53 | 0.46 | 0.00 | 530 |
| 16 | 4 | 1 | 16.30 (16.1–16.4) | 1.06 | 0.56 | 0.38 | 0.04 | 692 |
| 16 | 4 | 2 | 16.47 (15.9–16.6) | 1.07 | 0.57 | 0.38 | 0.01 | 688 |
| 16 | 4 | 4 | 16.43 (16.1–17.2) | 1.07 | 0.57 | 0.38 | 0.00 | 682 |
| 16 | 8 | off | 16.26 (15.9–16.9) | 1.00 | 0.57 | 0.43 | 0.00 | 607 |
| 16 | 8 | 1 | 16.17 (15.2–17.0) | 0.99 | 0.56 | 0.44 | 0.00 | 783 |
| 16 | 8 | 2 | 16.21 (14.8–16.8) | 1.00 | 0.56 | 0.44 | 0.00 | 727 |
| 16 | 8 | 4 | 16.12 (15.5–17.0) | 0.99 | 0.57 | 0.43 | 0.00 | 828 |
| 16 | 16 | off | 16.43 (16.2–16.7) | 1.00 | 0.58 | 0.42 | 0.00 | 913 |
| 16 | 16 | 1 | 15.67 (15.1–16.2) | 0.95 | 0.55 | 0.45 | 0.00 | 1163 |
| 16 | 16 | 2 | 15.44 (15.2–16.2) | 0.94 | 0.56 | 0.44 | 0.00 | 1190 |
| 16 | 16 | 4 | 15.89 (13.7–16.4) | 0.97 | 0.56 | 0.44 | 0.00 | 1534 |

## Run 2: checkpoint finalization concurrency

23:09:14 to 23:27:45, 8 cells, 40 runs, 4 MiB chunk.

| Destinations | `maxConcurrentCheckpointFinalizations` | Operations | Input MiB/s, median (range) | vs. 1 and off | Writer CPU share | Cloud Storage samples | Permit-wait samples | Peak heap (MiB) |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2 | 1 | off | 18.37 (17.5–19.2) | 1.00 | 0.68 | 0.31 | 0.00 | 263 |
| 2 | 1 | 2 | 24.50 (23.1–25.8) | 1.33 | 0.86 | 0.10 | 0.00 | 306 |
| 2 | 8 | off | 19.04 (14.9–19.4) | 1.04 | 0.66 | 0.32 | 0.00 | 267 |
| 2 | 8 | 2 | 25.31 (20.0–25.7) | 1.38 | 0.87 | 0.05 | 0.01 | 331 |
| 16 | 1 | off | 15.95 (12.9–16.4) | 1.00 | 0.58 | 0.42 | 0.00 | 475 |
| 16 | 1 | 2 | 17.04 (15.9–17.3) | 1.07 | 0.61 | 0.36 | 0.01 | 759 |
| 16 | 8 | off | 18.92 (17.9–20.2) | 1.19 | 0.71 | 0.23 | 0.00 | 536 |
| 16 | 8 | 2 | 21.44 (18.2–21.5) | 1.34 | 0.75 | 0.13 | 0.01 | 926 |

## Limits

- The probe measures one writer thread, not a Flink job: no network shuffle, no checkpoint barrier alignment, no load jobs, and no other task competing for the CPU.
- The VM's CPU encoded about 20 MiB/s of these rows per thread, so without the uploader a third of the writer thread waited on Cloud Storage. The production job in #2021 spent about 14% of its task-thread samples there, so it would gain less.
- Destinations were routed by a hash of the event id over 256 distinct events, so their volumes were similar but not equal; a strongly skewed mix sits between the one- and many-destination cells.
- Only Avro staging was measured.

## Leftovers and cleanup

In run 1's first warm-up run, one of 64 objects in a batched delete was not deleted, and the probe logged the connector's existing warning for it.
The object was deleted by hand after the run.
Both VMs were created with `--max-run-duration` and a `DELETE` termination action, but a VM that powers itself off is stopped rather than deleted, so both were deleted by hand; no VM, object or prefix remained.
