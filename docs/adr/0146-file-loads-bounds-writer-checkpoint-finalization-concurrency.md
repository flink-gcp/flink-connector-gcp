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

# ADR-0146: FILE_LOADS bounds writer checkpoint finalization concurrency

- Status: Accepted
- Date: 2026-08-30
- Issues: [#1164] (measurement), [#38] (GKE Autopilot follow-up)
- Modules: bigquery (`sink.fileloads`)
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md` § File loads

## Context

Each `FileLoadsWriter` appends to its destination files on the Flink task thread.
At a checkpoint or end of input, the same thread closes every remaining file before it can emit
the committables.
The close finalizes each Cloud Storage resumable upload, so one writer with many active
destinations accumulates their finalization latency serially.

Three forms of concurrency could remove that serialization.
Increasing sink parallelism gives each writer subtask its own serial upload lane and Cloud Storage
client.
A writer-local finalization pool could keep appends serial and close independent files concurrently.
A writer-local upload pool could own each file from creation through close, but it would also need
bounded queued bytes, per-file ordering, checkpoint drains, failure precedence, and
channel-confined shutdown.

The choice cannot come from local delayed fakes alone.
Cloud Storage performs chunk uploads during `write` once a file crosses the connector's 4 MiB
upload chunk, while smaller files defer most service work to `close`.
The comparison therefore needs both sides of that boundary and enough destinations to expose
serialization.

## Decision

**FILE_LOADS keeps append and non-checkpoint close paths serial, but permits bounded writer-local
checkpoint finalization.**
Sink parallelism remains the first staging-upload concurrency lever when slots are available and
destinations distribute across subtasks.
When TaskManager or slot count must remain fixed, `maxConcurrentCheckpointFinalizations` lets each
writer finalize independent open files concurrently at a checkpoint or end of input.
The option defaults to 1, accepts 1 through 8, and has no effect when a writer holds only one file.
Size-based rolls, capacity evictions, and idle closes remain on the task thread.
No full-upload pool is added.

A bounded real-GCS probe on 2026-08-30 compared four shapes from a local client to the existing
integration-test bucket.
Each sample used 1, 10, or 50 independent objects carrying either 32 KiB or 5 MiB, repeated three
times, with every sample prefix deleted and checked empty before the next sample.
The 5 MiB arm crossed the production client's 4 MiB upload chunk.
The probe isolated staging upload and did not include serialization, compression, Flink checkpoint
coordination, or BigQuery load jobs.
Its finalization-only arm was a purpose-built implementation of the same serial-write and
bounded-close shape, not an invocation of the production `StagedFileFinalizer` through Flink.
It measured the network and service effect available to close concurrency, but not the production
executor, result ordering, failure handling, or lifecycle overhead.

- **Current** opened and wrote every object serially, then closed them serially.
- **Finalization only** kept writes serial and closed objects through a bounded pool.
- **Sink parallelism** distributed objects evenly across eight independent serial writers and
  clients.
- **Full upload lanes** ran create, write, and close through eight bounded workers sharing one
  client.

The table gives median wall time in seconds.

| Destinations | Bytes per object | Current | Finalization only (8) | Sink parallelism (8) | Full upload lanes (8) |
|---:|---:|---:|---:|---:|---:|
| 1 | 32 KiB | 1.261 | 1.248 | 1.252 | 1.050 |
| 1 | 5 MiB | 3.872 | 3.882 | 3.779 | 3.780 |
| 10 | 32 KiB | 10.054 | 5.965 | 2.258 | 2.268 |
| 10 | 5 MiB | 37.450 | 27.100 | 8.271 | 7.628 |
| 50 | 32 KiB | 42.092 | 27.090 | 7.243 | 7.076 |
| 50 | 5 MiB | 178.489 | 126.370 | 26.381 | 26.021 |

Single-object arms had no concurrency to exploit and showed no repeatable regression.
At 10 and 50 destinations, sink parallelism reduced the current medians by 77.5–85.2%.
Full upload lanes differed from sink parallelism by at most 7.8%, below the preselected 25%
threshold for accepting their additional lifecycle and buffering surface.

Finalization-only candidates 1, 2, 4, 8, and 16 were also compared.
Eight was the smallest candidate to improve both 10- and 50-destination 5 MiB medians by at least
25%, and 16 added at most 5% on the completed multi-destination arms.
The first 16-way 50-by-5-MiB arm did not return after the other candidates had completed; 48
finalized objects were identified, deleted, and followed by an empty-prefix check.
That tail sets 8 as the public option's evidence-backed upper bound but is not treated as a service
limit.

After implementing the decision, a second local real-GCS probe loaded the production
`StagedFileFinalizer` and `GcsStagingStorage` classes directly from the built connector.
A temporary raw `StagedFileWriter` adapter kept object sizes exact so the probe could isolate the
production executor, ordered result collection, and finalizer-call overhead from the surrounding
format writer.
It repeated each concurrency 1, 2, 4, and 8 sample three times, rotating their order, and deleted
each prefix and checked it empty before continuing.
The table separates median close time from total serial-write-plus-close time, in seconds.

| Files | Bytes per object | Close (1) | Close (2) | Close (4) | Close (8) | Total (1) | Total (8) |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 10 | 32 KiB | 5.046 | 3.030 | 1.354 | 1.071 | 10.040 | 6.038 |
| 10 | 5 MiB | 12.987 | 7.267 | 4.224 | 2.757 | 37.034 | 27.331 |
| 50 | 32 KiB | 19.885 | 10.092 | 5.500 | 3.345 | 43.161 | 23.886 |
| 50 | 5 MiB | 58.058 | 31.539 | 16.508 | 9.138 | 179.741 | 131.200 |

Concurrency 8 reduced close medians by 78.8–84.3% and total medians by 26.2–44.7%.
The total improvement has the same direction and a similar magnitude to the prototype's
27.6–40.7%, while the separate close medians show the production finalizer's scaling without
serial-write variation.

A further twelve-sample confirmation replaced the raw adapter with the production
`AvroStagedFileWriter`, retaining three repetitions at concurrency 1 and 8 over ten objects.
The small target produced 65,705-byte median objects because the writer's encoded-byte count trails
an Avro block; the large target produced 5,245,814-byte median objects.
Concurrency 8 reduced their close medians by 79.5% and 79.6%, respectively, and their total medians
by 43.7% and 27.2%.
This confirms the trend through Avro compression flush, object finalization, committable creation,
the production executor, and ordered result reporting.
It still does not include Flink checkpoint coordination, metrics, destination routing, or BigQuery
load jobs.

The sink-parallelism arm was deliberately balanced.
A real Flink topology can route several hot destinations to one subtask, and a single hot
destination cannot be split between upload lanes without changing record partitioning.
The local network path can also magnify fixed Cloud Storage round trips.
Issue [#38] is the place to repeat the matrix on GKE Autopilot with actual Flink routing, subtask
metrics, the production `FileLoadsWriter` and `StagedFileFinalizer`, and a region-local network
path.
That follow-up can revise the default or reopen full-upload lanes if balanced sink parallelism
fails to reproduce or writer-local finalization is insufficient under common skew.

## Consequences

The writer keeps one-thread ownership during append, size rolls, capacity evictions, and idle
closes.
At a checkpoint, it creates no executor for the default 1 or a singleton file; otherwise it creates
a pool bounded by the smaller of the option and the number of open files.
Workers close independent staging channels, while the task thread records successful committables
and metrics in destination input order.
Every successfully submitted close drains before the method returns or throws on success, ordinary
failure, or interruption.
Ordinary file failures retain input order as primary and suppressed failures; the task thread polls
for a JVM-fatal failure, cancels its peers when one arrives, and keeps it primary so it cannot stay
hidden behind an earlier stalled close and Flink can act on it.
An interrupt received while the pool drains is remembered until the drain completes, restored on
the task thread, and reported after file failures.
A flag that was already set before the drain is preserved without changing the serial path's
checkpoint outcome.
The pool is not retained beyond that finalization call.

The option adds a serialized configuration field.
Jobs restored from a previous serialized form see the Java default zero and map it to the
compatible value 1.
The pool queues only references to files whose upload buffers already exist, so it does not keep a
second queue of row bytes.
Each active close still adds one worker thread and one concurrent Cloud Storage finalization
request.
For Parquet, several workers may also encode and compress their already-buffered row groups at the
same time, multiplying close-time working memory and CPU even though the persistent row-group bound
does not change.
Existing checkpoint-duration, file-count, and destination metrics are the operator-facing signals;
this change adds no metric.

Increasing sink parallelism is not free.
Each subtask can hold up to `maxOpenDestinations` files and `maxPendingFiles` committables, and
records for one destination can create one file in every subtask they reach.
Operators therefore measure TaskManager heap, destination distribution, file counts, checkpoint
duration, and Cloud Storage request behavior together rather than treating parallelism as an
isolated upload knob.

Changing `maxStagingFileBytes` is a separate load-job trade-off, not a substitute for upload
concurrency.
The threshold counts compressed bytes written to Cloud Storage, and ADR-0070 records why 16 MiB
sits in the measured load-duration basin while preserving twice the exact-URI headroom of 8 MiB.
Smaller files can increase source URIs, partition load jobs, temporary tables, and quota use;
larger files can leave the measured basin.

Staging format remains separate too.
ADR-0072 records that Parquet/Zstandard staged 0.785 times the Avro/Zstandard bytes for the measured
row shape, but BigQuery loaded Parquet several times slower below 256 MiB of total job input and
rejects a provided schema containing `JSON`.
Upload density alone therefore does not reopen the Avro default or the automatic JSON fallback.

[#38]: https://github.com/flink-gcp/flink-connector-gcp/issues/38
[#1164]: https://github.com/flink-gcp/flink-connector-gcp/issues/1164
