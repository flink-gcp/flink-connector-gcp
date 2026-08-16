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

# ADR-0070: The staging roll threshold is a measured throughput band, not limit arithmetic

- Status: Accepted
- Date: 2026-08-08
- Issues: [#285] (measurements there; [#281] produced the first, confounded pass)
- Modules: bigquery (`sink.fileloads`)
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md` § File loads,
  `docs/content/docs/reference/bigquery.md` § `FileLoadsOptions`

## Context

`FileLoadsWriter.DEFAULT_MAX_FILE_BYTES` was 1.5 GiB, and its comment said exactly where the value
came from: *"1.5 GiB keeps 10,000 files (the per-load-job URI limit) at ~15 TB"*. The axis being
optimised was the per-job URI cap — how much data can go through one load job before the
temporary-table plus copy path is needed. Nothing about how long a load takes went into it, and
nothing ever measured it.

It was not derived here either. The Dataproc connector this module's FILE_LOADS design references
carries the same value with the same reasoning — `SizeBasedCheckpointRollingPolicy.DEFAULT_MAX_FILE_SIZE`,
above a comment ending *"that's 10,000 files, which matches the number of source URIs allowed per
load job"* — so the number was adopted along with the argument for it. (Theirs is 1.5 **GB**,
`1500L * 1000 * 1000`; the value here had been 1.5 GiB, and this module's README described the
reference implementation with the wrong one of the two until this change.)

Streaming FILE_LOADS commits synchronously (ADR-0018): the committer waits for the load job, so
load duration is checkpoint duration is backpressure. Batch pays it on committer subtask 0, where
nothing scales with job parallelism. Load duration is therefore the quantity the value most affects,
and it was the one nobody had looked at.

## Decision

**The default is 16 MiB, chosen from a measured load-throughput band, and the URI cap becomes the
constraint the choice trades against rather than the thing it is derived from.** The value moves to
`FileLoadsOptions.maxStagingFileBytes`, so a deployment whose shape the default cannot anticipate
can set it.

Measured against real BigQuery on 2026-08-08 — 769 MiB staged as Avro/zstd, seven load jobs per
point, configurations interleaved so drift in the shared slot pool could not map onto the axis:

| bytes per file | 2 MiB | 4 MiB | 8 MiB | 16 MiB | 32 MiB | 128 MiB |
|---|---:|---:|---:|---:|---:|---:|
| median load | 15.0 s | 9.7 s | **8.3 s** | 9.3 s | 11.1 s | 16.9 s |

Three things follow, and only the first is obvious:

- **Smaller is not monotonically better.** The curve is a basin, so any change to this value needs a
  floor as well as a ceiling. An earlier file-count sweep stopped before the turn and reported
  "still improving", which is how the opposite conclusion nearly got recorded.
- **16 MiB rather than the 8 MiB optimum**, because the URI cap is a file *count*: at 16 MiB one
  load job covers ~156 GiB of a destination, at 8 MiB ~78 GiB. Twice the headroom costs 12% of load
  time. The 1.5 GiB it replaces bought ~14.6 TiB of headroom at roughly twice the load time.
- **The threshold is inert at high parallelism.** A checkpoint's data divided by the subtask count
  is already inside the band, so the roll never fires. This value is a lever for low parallelism,
  few destinations, or very large per-subtask volumes — which is why it is worth saying in the docs
  rather than leaving a reader to expect a speedup where the threshold was never reached.

The option exists for the reason the #54 workload-versus-service test asks for: the useful value
depends on the deployment's volume and parallelism, not on the service. It is also what gives back
the single-load ceiling this change costs a very large batch destination.

## Consequences

The comment on the constant now states both halves — the band and the ceiling — because stating
only one is how the previous value came to look justified. The measurement is quoted with its date
and sample size on the assumption that a service is free to change; a later measurement edits those
numbers in place rather than appending beside them.

`FileLoadsWriter` no longer carries a duplicate of the value or a test-only constructor that took
it: the writer reads the option, so there is one source of truth and the tests configure it the way
a user would.

Two findings from the same runs are recorded on their own issues rather than here, because neither
changes this decision: BigQuery's Parquet loader has a step at 256 MiB of total input ([#284]), and
the earlier probe wrote Avro blocks and Parquet row groups of sizes the connector's own writers do
not produce — the confound that made the first pass unusable ([#285]).

[#281]: https://github.com/laughingman7743/flink-connector-gcp/issues/281
[#284]: https://github.com/laughingman7743/flink-connector-gcp/issues/284
[#285]: https://github.com/laughingman7743/flink-connector-gcp/issues/285
