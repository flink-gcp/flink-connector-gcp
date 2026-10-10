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

# BigQuery FILE_LOADS in-region tuning: findings

This records the tuning half of [#1313](https://github.com/flink-gcp/flink-connector-gcp/issues/1313), run on 2026-10-10 under [#1554](https://github.com/flink-gcp/flink-connector-gcp/issues/1554).
It asks whether the FILE_LOADS tuning guidance holds when the production sink runs through real Flink checkpoints in the dataset's region.
The guidance came from probes that ran outside the region and outside Flink: [ADR-0146](../0146-file-loads-bounds-writer-checkpoint-finalization-concurrency.md) for finalization concurrency and [ADR-0072](../0072-parquet-staging-is-opt-in-batch-shaped-and-brings-its-own-dependencies.md) for Parquet.
The correctness half, a deployed recovery trial, is recorded in [the FILE_LOADS trial findings](0165-bigquery-fileloads-findings-1313.md).
Times are UTC.

## Summary

- **Finalization concurrency holds in-region.**
  Raising `maxConcurrentCheckpointFinalizations` from 1 to 8 cut the writer's checkpoint finalization by 70–76% at 10 destinations and 83–87% at 50, for both 64 KiB and 5 MiB files.
  The absolute cost is lower than ADR-0146 measured from outside the region: one close took 114–136 ms in-region, against 0.4–1.3 s per close through the production finalizer there.
- **Sink parallelism matched it.**
  Eight writer subtasks at concurrency 1 finalized 50 destinations in the same time as one subtask at concurrency 8.
- **Neither knob changes the commit.**
  The median load job ran in 1.1–2.0 s whatever the writer shape.
  The slowest commits came from the service: a single load job running about 35 s, or jobs held pending for up to 34 s.
- **Parquet was slower than Avro below 256 MiB, with no clear gain above it.**
  Across three runs and four row shapes, Parquet's median load below 256 MiB was 1.28–2.76 times Avro's.
  Above 256 MiB, measured only for the shapes with one `BYTES` payload column, the first run found it 1.21–1.33 times Avro's and the second 0.82–0.95 times.
  ADR-0072 measured Parquet at 2.2–3.5 times Avro's below 256 MiB, and at three to five times its own load just above it; here the penalty against Avro was 1.3–2.8.
- **What Parquet stages depends on the rows.**
  For a 1 KiB `BYTES` payload it staged the same bytes as Avro, and for 64 dictionary-friendly string columns about half; ADR-0072's 0.785 lies between.
- **Parquet files reached about twice `maxStagingFileBytes` until [#1704](https://github.com/flink-gcp/flink-connector-gcp/issues/1704).**
  At the default 16 MiB they averaged about 30 MiB in the first run; with the final rule the third run's loads put the average full Parquet file between 14.3 and 16.9 MiB for every row shape, like Avro's.

Every cell loaded exactly the rows it emitted, no load job failed, and no staging object or table was left behind.

## Method

`FileLoadsTuningProbe` in the BigQuery module's test tree runs one streaming job per cell on a local MiniCluster with the production `BigQuerySink` in FILE_LOADS mode.
No surefire execution selects it; it was run by hand.

- **Volume per checkpoint.**
  A data-generator source gated per checkpoint emits a fixed quota of 1 KiB rows and then waits for a checkpoint to complete.
  The gate reopens on completion rather than at the barrier, so a checkpoint that triggers before the quota is out carries only part of it, and the next carries the rest together with the following quota.
- **Routing.**
  Rows are partitioned by destination, so a writer subtask holds the destinations congruent to its index.
  Sink parallelism therefore spreads destinations instead of giving every subtask a file for every destination.
- **Options.**
  Connector defaults except the staging format, the concurrency under test, `minCheckpointInterval` set to the cell's interval, and `maxOpenDestinations` 64.
  The default 16 would close and reopen a file on almost every row once one writer holds 50 round-robin destinations.
- **Writer finalization.**
  Flink runs the writer's finalization in its pre-barrier step, which neither the synchronous nor the asynchronous duration includes.
  The probe reads each writer subtask's checkpoint statistics from the MiniCluster's REST API and takes the end-to-end duration less the start delay, alignment and both snapshot parts.
  The remainder also holds the committables' emission and the acknowledgement, a few milliseconds.
  A checkpoint's value is its slowest subtask, because the checkpoint waits for it.
- **Commit and load jobs.**
  The committer runs after the checkpoint completes, so commit time is outside the checkpoint duration.
  Load jobs were listed afterwards by a principal holding `bigquery.jobs.listAll`, because the run's service account may only create jobs.
  The `-c<N>-` segment of a job ID names its checkpoint; a commit's span runs from its first job's creation to its last job's end.
- **Oracle and cleanup.**
  After each job the probe compares the destination tables' row counts with the rows emitted and lists the staging prefix, then deletes the tables and the prefix.
  The job listing confirms the count from the other side: every checkpoint had one load job per destination, and their output rows sum to the rows emitted in every cell.

Each cell reports medians over its steady checkpoints, for finalization, checkpoint duration, load jobs and commits alike.
A checkpoint is steady when it is not the first, a warm-up, and its load jobs carried exactly one quota of rows.
In 7 cells, 13 checkpoints carried more or less than one quota and are excluded, the end-of-input checkpoints that loaded a split quota's tail among them: the four 50 × 5 MiB cells at concurrency 8 or sink parallelism 8, the first pass of 50 × 5 MiB at concurrency 1, and the two Parquet cells above 256 MiB.

The run used one `e2-standard-8` VM in `us-central1-a`, the dataset's and bucket's region, with OpenJDK 17.0.20.1, Flink 2.2.1 and the connector at `90c236421`.
The VM ran the 28 cells serially from 05:59 to 07:37 and was deleted afterwards.
A second run repeated the eight staging-format cells with an early form of [#1704](https://github.com/flink-gcp/flink-connector-gcp/issues/1704)'s roll fix on `79515f793` and the probe as committed, on the same VM shape from 12:32 to 13:32, with six checkpoints per cell and a 90-second interval for the cells above 256 MiB.
A third run, from 15:51 to 16:45 on the same VM shape, used [#1704](https://github.com/flink-gcp/flink-connector-gcp/issues/1704)'s final rule on `ba37b8adc` and two wide row shapes the probe gained with it, in eight cells below 256 MiB: five checkpoints at a 60-second interval, and four at 180 seconds for the 64-column cells.
No cell's log records a job restart or failure.
The committed probe differs from the one the first run used in four ways that leave a completed cell's numbers unchanged: it counts rows with a `COUNT(*)` query rather than table metadata, it disables restarts so a replayed batch cannot hide in the timings, it deletes the tables and the staging prefix when a cell fails, and it names its per-checkpoint output `checkpointStats` instead of overwriting the requested `checkpoints` count.

## Finalization concurrency and sink parallelism

Q1 cells used Avro and random payloads with a 30-second interval and six checkpoints.
Every cell ran twice, the second pass in the opposite order within each pair.

| Destinations | Bytes per destination | Writer shape | Finalization, pass 1 (s) | Finalization, pass 2 (s) | Checkpoint, pass 1 / 2 (s) | Commit, pass 1 / 2 (s) | Steady checkpoints |
|---:|---:|---|---:|---:|---:|---:|---:|
| 10 | 64 KiB | concurrency 1 | 1.21 | 1.14 | 1.22 / 1.14 | 2.57 / 2.24 | 5 / 5 |
| 10 | 64 KiB | concurrency 8 | 0.29 | 0.34 | 0.30 / 0.34 | 2.68 / 3.44 | 5 / 5 |
| 10 | 5 MiB | concurrency 1 | 1.28 | 1.30 | 1.29 / 1.30 | 3.97 / 4.30 | 5 / 5 |
| 10 | 5 MiB | concurrency 8 | 0.36 | 0.31 | 0.37 / 0.32 | 4.49 / 5.00 | 5 / 5 |
| 50 | 64 KiB | concurrency 1 | 6.19 | 6.67 | 6.20 / 16.61 | 3.92 / 31.27 | 5 / 5 |
| 50 | 64 KiB | concurrency 8 | 0.90 | 0.85 | 0.91 / 0.86 | 2.92 / 2.90 | 5 / 5 |
| 50 | 64 KiB | sink parallelism 8, concurrency 1 | 0.89 | 0.98 | 0.90 / 0.99 | 5.20 / 2.69 | 5 / 5 |
| 50 | 5 MiB | concurrency 1 | 6.79 | 6.06 | 7.09 / 6.07 | 7.09 / 5.14 | 4 / 5 |
| 50 | 5 MiB | concurrency 8 | 1.15 | 0.99 | 1.16 / 1.00 | 5.46 / 6.27 | 4 / 4 |
| 50 | 5 MiB | sink parallelism 8, concurrency 1 | 0.94 | 1.01 | 0.95 / 1.01 | 6.74 / 6.35 | 4 / 4 |

Concurrency 8 reduced finalization by 70–76% at 10 destinations and 83–87% at 50, and the two passes agree within 0.8 s at concurrency 1 and 0.2 s at concurrency 8.
Serial finalization grew with the number of files rather than their size: 64 KiB files took 114–133 ms per close and 5 MiB files 121–136 ms.
A 5 MiB file uploads its first 4 MiB chunk while rows are appended, so the close carries only the remainder.

Sink parallelism 8 finalized 50 destinations in 0.89–1.01 s against 0.85–1.15 s for concurrency 8.
Each of its subtasks held six or seven destinations and finalized them serially, so the two shapes reached a similar per-checkpoint wait by different routes.
This run does not separate them, and ADR-0146's order of levers stands for its other reasons: sink parallelism also spreads appends and serialization, while writer-local concurrency is the lever when slots are fixed.

The checkpoint duration follows the writer except where a commit was slow.
The committer commits on its task thread when the previous checkpoint completes, so a slow commit holds back the next barrier at the committer.
The commits that ran longest had two service-side causes, neither of them the writer shape:

- **Queued jobs.**
  In the second pass of 50 × 64 KiB at concurrency 1, the commits of checkpoints 2, 5 and 6 spanned 31–37 s, and checkpoints 3 and 6 took 16.6 s and 20.2 s while their writer finalization was 6.8 s and 6.7 s.
  The second pass of 50 × 64 KiB at concurrency 8 had one such commit, 29.6 s.
  The delayed jobs in those commits waited 17–34 s between creation and start and then ran in a few seconds.
- **One slow job.**
  In the first passes of 50 × 5 MiB at concurrency 1 and at sink parallelism 8, one load job of a commit ran 34.5 s and 35.2 s, while the others' medians were 1.5 s and 1.7 s and their slowest 4.4 s; the first of these held the next checkpoint to 45.0 s.

The same 50 × 64 KiB second pass also holds the one writer-side outlier of the run: checkpoint 4's finalization took 17.2 s against 5.9–6.8 s for the others, a tail in the staging closes that the medians absorb.
This is why the record reads the writer separately from the checkpoint.

The commit itself did not depend on the writer shape: the median load job ran in 1.1–1.5 s for 64 KiB files and 1.6–2.0 s for 5 MiB files in every shape, and the median commit span was 2.2–7.1 s outside the queued cell.

## Staging format

Q2 cells used one destination and one writer at concurrency 1; the first run used a 60-second interval and four checkpoints.
`random` payloads are incompressible; `hex` payloads draw from sixteen digits, so Zstandard roughly halves them.
In the first two runs the raw volumes were chosen so that each shape's loads land on both sides of 256 MiB; the third run stayed below it.

### First run

| Format | Rows | Raw per checkpoint | Staged per load (MiB) | Files per load | Load duration (s) | Steady checkpoints | Writer finalization (s) |
|---|---|---:|---:|---:|---:|---:|---:|
| Avro | random | 128 MiB | 129 | 9 | 6.63 | 3 | 0.17 |
| Parquet | random | 128 MiB | 129 | 5 | 8.92 | 3 | 0.14 |
| Avro | random | 512 MiB | 515 | 33 | 10.32 | 3 | 0.23 |
| Parquet | random | 512 MiB | 515 | 17 | 13.69 | 2 | 0.24 |
| Avro | hex | 256 MiB | 134 | 9 | 12.46 | 3 | 0.13 |
| Parquet | hex | 256 MiB | 135 | 5 | 19.93 | 3 | 0.34 |
| Avro | hex | 1 GiB | 537 | 34 | 10.55 | 3 | 0.14 |
| Parquet | hex | 1 GiB | 540 | 18 | 12.74 | 2 | 0.33 |

Parquet's median load was 1.35 and 1.60 times Avro's below 256 MiB and 1.33 and 1.21 times above it.
The individual loads overlapped in every pair, though:

| Rows | Raw per checkpoint | Avro loads (s) | Parquet loads (s) |
|---|---:|---|---|
| random | 128 MiB | 6.4–9.6 | 7.7–9.4 |
| random | 512 MiB | 10.2–12.4 | 10.5–16.9 |
| hex | 256 MiB | 10.8–19.8 | 19.0–26.1 |
| hex | 1 GiB | 9.5–14.5 | 9.8–15.6 |

With two or three loads per cell, this run found Parquet not faster on either side rather than a measured slowdown factor.

Parquet files averaged about 30 MiB, against 15–16 MiB for Avro: 515–540 MiB arrived in 17–18 Parquet files and 33–34 Avro files.
Parquet sets its row-group size from `maxStagingFileBytes` and closes a row group just under that size, preferring under to over, while the writer then rolled a file only once its written bytes reached the threshold.
A file holding one row group never did, so every Parquet file took a second row group.
A local run of the production writer reproduced it at 8, 16 and 32 MiB, with files at 1.89–2.00 times the threshold.

### Second run, after the roll fix

[#1704](https://github.com/flink-gcp/flink-connector-gcp/issues/1704) rolls a Parquet file at the row-group boundary it expects to be nearest the threshold.
The second run used an early form of the fix that rolled after the first row group; for these rows with one `BYTES` payload column, whose groups land just under the threshold, the final rule rolls at the same boundary, as a local run of the writer confirmed.
It repeated the eight cells with more checkpoints.

| Format | Rows | Raw per checkpoint | Staged per load (MiB) | Files per load | Load duration (s) | Steady checkpoints | Writer finalization (s) |
|---|---|---:|---:|---:|---:|---:|---:|
| Avro | random | 128 MiB | 129 | 9 | 7.06 | 5 | 0.11 |
| Parquet | random | 128 MiB | 129 | 9 | 13.33 | 5 | 0.13 |
| Avro | random | 512 MiB | 515 | 33 | 12.29 | 5 | 0.00 |
| Parquet | random | 512 MiB | 515 | 33 | 10.11 | 4 | 0.00 |
| Avro | hex | 256 MiB | 134 | 9 | 12.62 | 5 | 0.13 |
| Parquet | hex | 256 MiB | 135 | 9 | 16.13 | 4 | 0.48 |
| Avro | hex | 1 GiB | 537 | 34 | 10.11 | 5 | 0.12 |
| Parquet | hex | 1 GiB | 540 | 35 | 9.57 | 5 | 0.27 |

Parquet now staged about as many files per load as Avro, about 15 MiB each.
Its median load was 1.89 and 1.28 times Avro's below 256 MiB and 0.82 and 0.95 times above it:

| Rows | Raw per checkpoint | Avro loads (s) | Parquet loads (s) |
|---|---:|---|---|
| random | 128 MiB | 5.9–8.9 | 12.1–17.1 |
| random | 512 MiB | 11.4–15.9 | 7.8–12.1 |
| hex | 256 MiB | 7.9–14.7 | 14.7–25.6 |
| hex | 1 GiB | 6.3–11.4 | 5.8–11.1 |

Below 256 MiB the ranges did not overlap, though for hex rows they came within 26 ms, so Parquet was slower there.
Above 256 MiB Parquet's median was lower, but its range overlapped Avro's for both shapes.

### Third run, with the final rule

The third run checked the final rule end to end, including the wide rows the early form would have rolled too soon: `wide32`, the probe's `WIDE_HEX`: 32 string columns of 32 random hexadecimal digits; and `wide64`, its `WIDE_DICT`: 64 string columns each holding one of 100 values.
Their quotas are counted in rows of a nominal 1 KiB.
A load's files are all full but the last, so the average full file lies between the load's bytes divided by its file count N and by N − 1.

| Format | Rows | Raw per checkpoint | Staged per load (MiB) | Files per load | Average full file (MiB) | Load duration (s) | Steady checkpoints | Writer finalization (s) |
|---|---|---:|---:|---:|---|---:|---:|---:|
| Avro | random | 128 MiB | 129 | 9 | 14.3–16.1 | 7.75 | 4 | 0.12 |
| Parquet | random | 128 MiB | 129 | 9 | 14.3–16.1 | 17.79 | 3 | 0.12 |
| Avro | hex | 256 MiB | 134 | 9 | 14.9–16.8 | 15.15 | 4 | 0.22 |
| Parquet | hex | 256 MiB | 135 | 9 | 15.0–16.9 | 22.30 | 4 | 0.31 |
| Avro | wide32 | 256 MiB | 135 | 9 | 15.0–16.9 | 17.76 | 4 | 0.13 |
| Parquet | wide32 | 256 MiB | 146 | 10 | 14.6–16.2 | 25.41 | 3 | 0.11 |
| Avro | wide64 | 2 GiB | 226 | 15 | 15.0–16.1 | 13.35 | 2 | 0.00 |
| Parquet | wide64 | 2 GiB | 117 | 8 | 14.6–16.7 | 36.79 | 3 | 0.00 |

The average full Parquet file stayed at about the threshold for every shape, including the 64-column rows whose row groups are 0.69 MiB each, so the final rule held end to end.
Parquet staged 1.08 times Avro's bytes for `wide32` and 0.52 times for `wide64`.
Its median load was 2.30, 1.47, 1.43 and 2.76 times Avro's, and the ranges separated for all but `wide32`:

| Rows | Avro loads (s) | Parquet loads (s) |
|---|---|---|
| random | 7.5–9.7 | 12.7–18.2 |
| hex | 13.7–17.7 | 18.8–25.2 |
| wide32 | 14.1–21.8 | 19.1–28.4 |
| wide64 | 12.5–14.2 | 29.9–48.8 |

For `wide64` Parquet loaded half the bytes in more than twice the time.

### Reading the runs together

All three runs put Parquet's median load above Avro's below 256 MiB, 1.28–2.76 times across four row shapes, and the second and third separated the ranges for every shape but `wide32`.
Above 256 MiB the first run's medians found Parquet no faster and the second's slightly faster, with overlapping ranges in both.
The first two runs differ in two things at once, the file layout before and after the fix and the hour at which BigQuery ran the loads, and these data do not separate them.
What the runs support is narrower than ADR-0072's step: Parquet's median penalty against Avro below 256 MiB was 1.3–2.8 times, against ADR-0072's 2.2–3.5, and it gained at most a little above it.

ADR-0072 also measured Parquet staging 0.785 times Avro's bytes.
Here Parquet staged the same bytes for one opaque `BYTES` column, which gives its columnar encoding nothing to exploit, 1.08 times for 32 random-text columns, and 0.52 times for 64 dictionary-friendly columns.
The ratio is a property of the rows.

Median writer finalization stayed under half a second in every staging-format cell.
A single destination's file rolls at the size threshold during appends, so a checkpoint finalizes only the last partial file.
In the second run's 512 MiB cells and the third run's `wide64` cells it finalized none, and every steady checkpoint found no open destination; that is consistent with the writer finishing its quota more than a minute before the checkpoint and the one-minute `destinationIdleTimeout` closing the last file in between, which a 90- or 180-second interval allows but does not guarantee: the second run's 1 GiB cells, at 90 seconds, still had a file open.

## Scope

`JSON` destinations were not measured.
They stage Avro whatever `stagingFormat` says, a correctness fallback covered by unit tests, so no Parquet cell could include one.

The cells covered rows of about 1 KiB on one VM, with wide rows only below 256 MiB.
They do not cover wide rows above 256 MiB, skewed destinations, or TaskManager heap at Parquet close.
