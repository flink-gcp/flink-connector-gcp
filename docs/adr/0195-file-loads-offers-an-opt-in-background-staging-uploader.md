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

# ADR-0195: FILE_LOADS runs a background staging uploader by default

- Status: Accepted
- Date: 2026-11-04; revised by [#2034] (2026-11-05: the uploader is on by default)
- Issues: [#2021], [#1164], [#1127], [#2019], [#2034]
- Modules: bigquery (`sink.fileloads`, `table`)
- Refines: [ADR-0146] (size rolls, capacity evictions and idle closes may now leave the task
  thread)
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md` § File loads
- Evidence: [in-region measurements](evidence/0195-fileloads-staging-uploader-2021.md),
  [deployed recovery trials](evidence/0195-fileloads-uploader-recovery-2034.md)

## Context

[ADR-0146] kept appends, size rolls, capacity evictions and idle closes on the task thread and
added concurrency only to checkpoint finalization.
That leaves a writer with one or two busy destinations uncovered.
Each open file sends a resumable-upload request whenever its upload chunk fills, and a destination
that reaches `maxStagingFileBytes` finalizes its file at the roll, not at the checkpoint, so
`maxConcurrentCheckpointFinalizations` never reaches it.

[#2021] measured that case on a production-shaped job: connector 1.3.0 on Flink 2.2, Avro/Zstandard
staging, two destinations with about 90% of the bytes in one, 2-minute checkpoints, and four slots
on a TaskManager with a 2 vCPU request and a 4 vCPU limit.
Each subtask staged about 1.2 MB/s and rolled a file about every 13 seconds, so at most 4 of the 36
files a checkpoint loaded closed at the checkpoint.
About 14% of 40 thread samples of the task threads sat in Cloud Storage upload writes, blocked in
the socket write of a chunk request.
In the 2-slot run the two task threads used about 1.2 CPU together while Flink reported both fully
busy, with no CFS throttling, and raising slots from 2 to 4 on the same CPU limit raised throughput
from 1,484 to 2,339 records/s at about 1,000 records per consumed vCPU.
The waits therefore cost slots, memory and source connections rather than CPU.

Two shapes could reduce the wait.
A larger chunk sends a file in fewer requests, but the largest sampled frame was the byte transfer
itself, which a larger chunk moves to the close rather than removes, and every open destination
holds one chunk, so the 4 MiB chosen for [#1127] would multiply across `maxOpenDestinations`.
A background uploader moves the transfer and the roll's finalization off the task thread, at the cost
of a lifecycle: ordering, a memory bound, drain, failure reporting and abort.
The client's own parallel composite upload also uploads in the background, but it creates part
objects, so an abandoned file would no longer create no object, which [ADR-0146]'s abort relies on.

## Decision

**FILE_LOADS exposes both shapes as options and turns the uploader on by default; the chunk stays
4 MiB.**
`stagingUploadChunkBytes` (`sink.file-loads.staging-upload-chunk-bytes`) sets the chunk, 4 MiB by
default.
`backgroundStagingUploadEnabled` (`sink.file-loads.background-staging-upload.enabled`) defaults to
true, which starts one upload thread per writer; false keeps every upload on the task thread, as
the writer did before the uploader existed.
The default was false until [#2034], as Consequences records.
`maxInFlightUploadOperations` (`sink.file-loads.in-flight.max-upload-operations`) sizes that
uploader, 2 by default, and is rejected when set with the uploader off, as `parquetCompression` is
under Avro and the Firestore sink's rates are with throttling off, because an ignored value is worse
than a refused one.
The switch is separate from the size because a single count whose 0 meant "no uploader" read as a
tuning value rather than an on/off choice; the switches here that other options depend on are
spelled `.enabled` (`sink.cdc.enabled`, `sink.throttling.enabled`, `sink.message-ordering.enabled`).
The Table API exposes both, unlike the checkpoint-concurrency options, because one busy destination
is the case the uploader serves.

The uploader follows [#1164]'s constraints:

- **Order**: one thread runs upload operations in submission order, so a file's chunks reach the service in
  order and its close follows its last chunk.
  Committables move to the commit in close order, and only after the uploader has finalized the file.
- **Memory**: each upload operation, a full chunk or a file's close with its last chunk, if any,
  takes one of `maxInFlightUploadOperations` permits until it completes; the
  task thread blocks while none is free.
  The uploader adds at most that many chunks per writer whatever the number of open destinations, and a
  full chunk reaches the client without a copy, so an open file still holds one chunk.
  Closing a file briefly holds one more, because the client copies a partial chunk into its own
  buffer; the uploader closes one file at a time.
  Finalizations still on the uploader count towards `maxPendingFiles`.
- **Checkpoint and end of input**: the writer waits for the uploader to run every upload operation submitted so far,
  then detaches each remaining open file so it is finalized on the task thread or by
  `maxConcurrentCheckpointFinalizations` exactly as without the uploader.
  A detached file keeps its partial chunk until its own close, so the extra copy is held only by the
  files closing at that moment.
- **Failure**: the first uploader failure is kept, every later uploader task skips its work, and the task
  thread reports it on its next submission or checkpoint, a JVM-fatal error as itself.
- **Abort**: closing the writer stops the uploader without waiting.
  Queued files are never finalized and create no object; a close already running may finalize an
  object that no committable references, and the staging bucket's lifecycle rule removes it, as it
  does for a failed attempt's other leftovers.
- **Recovery**: object names are still assigned on the task thread from the writer-global sequence,
  so the uploader changes no object identity.

The uploader's option is named after [ADR-0137]'s connector-owned ledger, `inFlight` under a
`sink.*.in-flight.max-*` key, as Pub/Sub's `maxInFlightMessages` and Cloud Tasks'
`maxInFlightTasks` are; BigQuery's own `maxInflightRequests` carries the Storage Write client's
spelling and is not this case.
It counts operations rather than chunks because a close may carry no remaining chunk and still takes
a permit.
Its default is 2 and 1 is still accepted: in the measurements two was within 0.04 of four in eight
of nine cells, while one left the writer waiting for the permit in 9–14% of samples and was slower
than two wherever the uploader helped.
It has no upper bound.
Every ceiling in this module rests on a measurement or on a thread or client it contains, such as
`maxConcurrentCheckpointFinalizations`'s 8 from [ADR-0146] or `maxConcurrentDestinations`'s 64; the
uploader adds one thread whatever the value, so the value costs only payload heap, which the
operator sizes with `maxOpenDestinations` and `stagingUploadChunkBytes`, themselves unbounded or
bounded only by the client.
A ceiling copied from a different mechanism would bound nothing in bytes, since the chunk itself is
configurable; a measured knee in throughput would justify one in a revision of this record.

Because every limit is valid on its own, the writer also logs one warning at start when its own
`maxOpenDestinations` plus, with the uploader on, `maxInFlightUploadOperations` chunks exceed a quarter of the JVM's
maximum heap.
The quarter is a round heuristic, not a measurement, chosen to leave room for format buffers,
clients and the rest of the task; it is per writer, because a writer cannot see how many other
FILE_LOADS writers share its TaskManager, so the docs tell the operator to multiply by them.
It warns rather than fails: only the TaskManager knows its heap, and how much of it the rest of the
task needs is the operator's call. The Bigtable sink likewise warns rather than fails above a
fixed 100 MiB `maxInFlightBytes`; this threshold is a share of the heap instead, because the
writer's buffers scale with three options.

The chunk must be a multiple of 256 KiB, because the client rounds any other value up and the
writer's buffers would then not match the client's.
The upper bound is the largest such multiple an `int` chunk size can express.

## Measurements

An in-region probe on 2026-11-04 drove one writer thread against real Cloud Storage with #2019's
nested rows; the [evidence](evidence/0195-fileloads-staging-uploader-2021.md) carries its method,
every cell and its limits.

- With one or two destinations the uploader at two operations made the writer 14–35% faster, and
  writer-thread samples inside the Cloud Storage client fell from 29–35% to 8–12%.
- With 16 destinations it measured 0.94–1.07 of the uploader-off cell, because each file held about
  one chunk per checkpoint and was uploaded by the checkpoint's own finalization, which the
  uploader cannot take: a checkpoint waits for every file to be finalized.
  `maxConcurrentCheckpointFinalizations` 8 measured 1.19 there, and 8 together with the uploader
  1.34; with two destinations the pair measured 1.38.
- With the uploader at two operations, the 4 MiB chunk was the fastest or within about 1% of it for every
  destination count, so
  the chunk default stays.

The documented recommendation is therefore the uploader for a writer with few busy destinations, and
the uploader together with concurrent checkpoint finalization for a writer with many.

## Consequences

With the uploader off the writer wraps the client channel in the same stream as before and starts no
thread.
The one change on that path is the start-up warning, which counts the open files' chunks with or
without the uploader, so 16 files of 4 MiB warn on a maximum heap under 256 MiB with the uploader
off, and with the default two operations on, under 288 MiB.
The storage SPI now returns the channel and takes the chunk size, which is internal.

The measurements show the gain and no material regression with many destinations, but they ran
the writer alone, not a Flink job.
The switch therefore stayed off until the uploader had also run through real Flink checkpoints,
loads and recovery: the real-GCP streaming test that finalizes every file on the uploader, which
passed in [#2021], and the deployed recovery scenario with the uploader on.

That scenario ran under [#2034] on 2026-11-05: `fl-10` with the uploader at two operations, its
savepoint upgrade and JobManager replacement, then the same with a TaskManager replacement, a
rescale out and a rescale in in place of one recovery.
All four were `usable`, and each loaded its 1,843,200 records into 10 tables exactly once, with no
duplicate, missing or invalid row.
In a sample of the first trial's load jobs its staged files were about 5.8 MiB after compression,
so each sent a full 4 MiB chunk through the uploader every checkpoint; the uploader itself is not metered, so that is shown by size rather
than observed.
The [findings](evidence/0195-fileloads-uploader-recovery-2034.md) carry every attempt, including
one that ran out of recovery time while Autopilot provisioned a node, which raised the scenario's
deadline rather than implicating the uploader.
The uploader is therefore on by default.
A writer that is CPU-bound, or that must not start a thread, sets it to false; its cost is one
thread and up to two chunks per writer.
An options instance serialized before the option existed reads the default, as every other
FILE_LOADS option added after its first release does.
The uploader moves only the upload wait; encoding and compression stay on the task thread, which
[#2019] addresses separately.

[ADR-0146]: 0146-file-loads-bounds-writer-checkpoint-finalization-concurrency.md
[#1127]: https://github.com/flink-gcp/flink-connector-gcp/issues/1127
[#1164]: https://github.com/flink-gcp/flink-connector-gcp/issues/1164
[#2019]: https://github.com/flink-gcp/flink-connector-gcp/issues/2019
[#2021]: https://github.com/flink-gcp/flink-connector-gcp/issues/2021
[#2034]: https://github.com/flink-gcp/flink-connector-gcp/issues/2034
[ADR-0137]: 0137-a-cross-connector-name-diverges-only-to-name-a-real-difference.md
