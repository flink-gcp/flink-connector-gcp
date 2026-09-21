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

# Bigtable Stage 2 service assessment of checkpoint-owned writes

This record holds the service measurements that [#1327](https://github.com/flink-gcp/flink-connector-gcp/issues/1327) asked for and the supported-workload decision that follows from them.
The [formal protocol](0163-bigtable-staged-performance-protocol.md) defines the matrix, the periods, the thresholds and the variability rule; the [production preparation record](0163-bigtable-production-stage2-preparation.md) defines the instrument and the retained-trial lifecycle.
A **cell** is one point of the 108-cell matrix, measured in two **arms**, the staged checkpoint-owned mode under test and the existing at-least-once bulk sink, with three **runs** of each arm in fresh JVMs.
One campaign completed the matrix, on 2026-09-20 and 2026-09-21, and the assessment declines the mode at the ADR-0104 gate.

## Where and how the campaign ran

The campaign ran from compute co-located with the instance, which is the condition [ADR-0166](../0166-bigtable-implementation-precedes-final-stage2-acceptance.md) set when it deferred this work.
The 2026-09-07 lease had measured a staged visibility p95 of 23.3 seconds from an execution host in Japan, and no record then attributed that to network distance rather than to the mode.

| Input | Value |
| --- | --- |
| Bigtable instance | `stage2-1327-trial`, free trial, one SSD node, Enterprise edition, `us-central1-b`, created 2026-09-19T08:57:32Z through the Console |
| Observation host | `stage2-1327-host`, `e2-standard-4`, `us-central1-b`, 50 GiB pd-balanced, created 2026-09-19T07:39:01Z with a 120-hour automatic deletion |
| Source | `aad72cd8e52cec7bdb25c958bf61dc820a549054` |
| Runtime | classpath built from that source, manifest SHA-256 `c178daf6173f5c51bc62f56ac6b1822e1a0b5f01b17806dde147a10d27a76d0a`, plus the untracked controller jar the manifest does not cover |
| JVM | OpenJDK 17.0.20 from the host's Ubuntu 24.04 packages, `-Xmx10g --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED` |
| Per-run capacities | `inventoryEntries=8000000`, `stagedEntries=4000000`, `stagedBytes=201326592`, `workBytes=8589934592`, `checkpointTimeoutMillis=60000`, `drainMillis=120000` |
| Reservation | `runOverheadSeconds=535`, `leaseOverheadSeconds=600`, `campaignOverheadSeconds=16000`, `hostBoundSeconds=496600`, USD 19.99 against the separate USD 20 ceiling |

The reservation is a worst case that the host's own deadline undercuts: `hostBoundSeconds` is 138 hours, and the host deletes itself 120 hours after creation, so a campaign that approached its reserved budget would have lost the host first.
The campaign finished in 25 hours and neither bound was approached, but the reservation should be planned against the shorter of the two rather than stated beside it.

The controller that sequenced the campaign is temporary automation kept outside tracked source, as ADR-0166's 2026-09-19 refinement records.
It calls only the merged runtime: the campaign journal, the trial adapter, the service probe for each worker, and a Monitoring read for physical storage after each cell.

Three earlier attempts produced no matrix.
The first stopped in the protocol's readback, which then read the inventory one slot at a time and took minutes at eight million slots; that defect is fixed in the merged source this campaign used.
The second stopped at its 427th run, when the readback of a 64 KiB cell needed about 310 seconds against a 300-second per-run reservation, which is why this campaign reserved 535.
The third stopped at its 27th cell, when one admin call exceeded its 15-second deadline with no retry of its own.
Each is retained as evidence, and none produced a service measurement that this record uses.

## Calibration on the observation host

Calibration ran the protocol's minimum on the observation host before the trial existed: one no-service observation per arm against a matched 100 ms slower control, and one observation of the largest cells to fix the capacity inputs.

The bulk control behaved as the protocol requires.
The fast run admitted 3,354 inputs per second at a visibility p95 of 0.37 ms; the 100 ms control admitted 36 inputs per second at a p95 of 200 ms, so the deliberately slower configuration registered as slower in both reported metrics.
The staged fast run admitted 3,017 inputs per second, but its 100 ms control produced no measured acknowledgement at all.
The staged path acknowledges nothing until the commit stage of a completed checkpoint has finished, and at 100 ms per conditional write that stage does not finish inside the 30-second measured window, so the control ends with nothing to report rather than with a slower figure.
The staged arm therefore entered the campaign without a satisfied control.
Its control failed in the way the mode predicts, and the bulk control passed, but the protocol's check asks for a slower reading and did not get one; the verdict below inherits that weakness.

The largest cells set the capacity inputs.
A first attempt at 64 KiB with 4 GiB of staged bytes per subtask exhausted the heap, so `stagedBytes` was fixed at 192 MiB per subtask, the largest value the parallelism-16 cells admit inside a 10 GiB heap, and `stagedEntries` at four million.
Both values are the same in every cell, so the arms and the cells stay comparable.
At those limits the 1 KiB largest cell completed at 1,913 inputs per second with a peak heap of 6.97 GB, and the 64 KiB largest cell terminated as a censored run with its staging capacity exhausted.
That second outcome anticipated what the service campaign then found across the 64 KiB half of the matrix.

## What ran

The campaign started at 2026-09-20T10:11:08Z and reached its terminal state at 2026-09-21T11:12:10Z, 25.0 hours later.
It dispatched and recorded all 648 preregistered runs and retained evidence for all 108 cells, then ran both auxiliary observations.

| Outcome | Runs |
| --- | ---: |
| Bulk observed | 324 of 324 |
| Staged observed | 80 of 324 |
| Staged failed or censored | 244 of 324 |
| Cells with all six runs valid | 15 of 108 |
| Cells with no valid comparison | 93 of 108 |

The journal records each failure as `FAILED` and admits the next preregistered run, which is the rule ADR-0166 adopted on 2026-09-19 after the earlier stop-on-first-failure rule would have ended a campaign at its second cell, where the first staged run failed.

One admin call needed its transport retry.
Creating the tables of cell 98 exceeded the 15-second HTTP deadline, and the retried create found the tables already present, which the controller accepts as a call that had in fact succeeded.
The same failure without a retry had ended the previous attempt at cell 27, so this is a recurring condition of the admin API at this call rate rather than a single transient event, and it is recorded as one retry rather than as a measurement.

## The fifteen cells measured in both arms

Fifteen cells produced three valid repetitions in both arms, which is what the protocol's comparison requires.
A cell id gives the payload bytes, the key distribution, the parallelism, the per-subtask in-flight entries and the checkpoint interval in seconds.
The ratio columns are computed from the unrounded measurements, so dividing the two rounded p95 columns printed here reproduces them only to within a few per cent.

| Cell | Bulk inputs/s | Staged inputs/s | Throughput ratio | Bulk p95 | Staged p95 | p95 ratio |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `b1024-even-p1-i1-c1` | 171 | 170 | 99% | 0.013 s | 8.875 s | 668x |
| `b1024-even-p1-i4-c1` | 635 | 798 | 126% | 0.013 s | 9.853 s | 787x |
| `b1024-even-p1-i16-c1` | 2,084 | 1,463 | 70% | 0.013 s | 5.723 s | 446x |
| `b1024-even-p4-i16-c1` | 4,698 | 1,984 | 42% | 0.027 s | 35.668 s | 1,329x |
| `b1024-even-p16-i1-c10` | 1,517 | 489 | 32% | 0.030 s | 45.685 s | 1,546x |
| `b1024-even-p16-i4-c1` | 3,590 | 1,827 | 51% | 0.034 s | 34.527 s | 1,025x |
| `b1024-even-p16-i16-c1` | 6,336 | 1,720 | 27% | 0.056 s | 15.895 s | 282x |
| `b1024-even-p16-i16-c10` | 6,021 | 130 | 2% | 0.059 s | 45.088 s | 760x |
| `b1024-hot-p1-i1-c1` | 170 | 243 | 143% | 0.013 s | 7.530 s | 569x |
| `b1024-hot-p1-i4-c1` | 625 | 868 | 139% | 0.013 s | 10.082 s | 791x |
| `b1024-hot-p1-i16-c1` | 2,061 | 1,162 | 56% | 0.013 s | 10.394 s | 813x |
| `b65536-even-p16-i1-c10` | 1,079 | 311 | 29% | 0.043 s | 48.913 s | 1,147x |
| `b65536-even-p16-i4-c1` | 1,752 | 534 | 31% | 0.067 s | 19.615 s | 292x |
| `b65536-even-p16-i16-c1` | 2,172 | 558 | 26% | 0.164 s | 21.176 s | 129x |
| `b65536-even-p16-i16-c10` | 2,242 | 349 | 16% | 0.165 s | 39.907 s | 241x |

[ADR-0104](../0104-exactly-once-modes-use-service-native-replay-protection-and-pass-a-performance-gate.md) grants general support at a throughput ratio of at least 70% with a visibility p95 ratio of at most 2x, and constrained opt-in at 25% with at most 4x.
Throughput alone would clear the constrained bound in thirteen of the fifteen cells and the general bound in five.
The visibility criterion fails in every cell, by between 129 and 1,546 times the bulk figure, against a limit of 4.

Twelve of the fifteen cells are inconclusive under the protocol's variability rule, because at least one arm's three-repetition throughput range exceeds 10% of its mean.
That rule governs the throughput comparison, and the decline does not rest on it.
It rests on visibility, where the closest cell is `b65536-even-p16-i16-c1` at 129 times bulk: its staged p95 would have to fall by 97% to reach the constrained bound.
No spread within a cell moves a cell across a bar two orders of magnitude away, which is why the twelve inconclusive cells do not leave the verdict open.

The protocol anticipated the mechanism: checkpoint waiting alone can fail the latency criterion, and it may not be subtracted after the result is seen.
The measured staged p95 values run from 5.7 to 48.9 seconds at checkpoint intervals of 1 and 10 seconds, so the delay is not the interval itself but the interval plus the queue of conditional commits that each completion releases.
The bulk arm shares the host, the zone, the client configuration and the workload, and its p95 across these fifteen cells runs from 13 to 165 ms, so the staged seconds are not the network path that ADR-0166 set out to rule out.

## Why most cells have no comparison

Of the 324 staged runs attempted, 244 produced no valid observation.
The bulk arm ran the same source, host, controller and workload and produced a valid observation in all 324 of its runs.

| Cause | Runs | 1 KiB | 64 KiB |
| --- | ---: | ---: | ---: |
| Staging capacity exhausted (censored) | 156 | 27 | 129 |
| Checkpoint expiry | 78 | 74 | 4 |
| No measured acknowledgement in the window | 8 | 8 | 0 |
| Drain deadline after admission closed | 2 | 2 | 0 |

The first cause belongs to the mode.
A staged writer holds every envelope admitted since the last checkpoint, and `BigtableStagedWriter.write` fails the run when the retained work reaches its configured capacity, because the writer applies no backpressure and the harness source has no rate limiter.
Retained work is the payload size times the admission rate times the checkpoint interval, so at the same rate and interval the 64 KiB half of the matrix exhausts the same capacity sixty-four times sooner than the 1 KiB half.
The counts follow that: 129 of the 162 staged runs attempted at 64 KiB were censored this way, against 27 of 162 at 1 KiB.

The second cause is checkpoint expiry, concentrated in the 1 KiB half where capacity lasts long enough for a checkpoint to be attempted over a large retained set.
A checkpoint of the staged mode must snapshot every envelope the writers have retained since the last one, so the same accumulation that exhausts capacity also lengthens the snapshot, and these runs exceeded the configured 60-second timeout until Flink's failure threshold stopped the job.
That timeout was fixed before the campaign and not changed during it, as the protocol requires.
A longer one would have converted some of these runs into observations, but into observations whose visibility latency was already past the criterion by two orders of magnitude.
The [2026-09-07 lease](0163-bigtable-stage2-experiment-harness.md) saw this symptom once and could not repeat it; 78 occurrences here settle that it reproduces.

Censoring and failure are distinct and this record keeps them apart.
A censored run is one the protocol's own per-run ceiling stopped, and it is not by itself evidence that the mode cannot run that workload; what it does show is that the staged arm reaches those ceilings where the bulk arm, under the same ceilings, never does.

## The sustained hot-row observation

The campaign's sustained auxiliary phase ran for its full 1,800 seconds on 2026-09-21 and its observation succeeded, but the campaign recorded the phase as failed and its marker timecourse as unmeasured.
The cause is a defect in the sampler rather than in the mode.
The sampler reads one hot row on a timer through a second data client, and that client cleared the retryable codes of the streaming read path alone; the client requires the single-row, streaming and bulk read paths to agree and rejects the mismatch when the settings are built, before any endpoint is contacted.
The sampler therefore died at construction, and its failure surfaced only when the observation beside it joined thirty minutes later.
The repair clears all three paths together and is held by a unit test that builds the settings, so a future caller that clears one path alone fails in the unit suite rather than inside a paid observation.

The observation was then repeated on 2026-09-21 between 11:58:10Z and 12:28:57Z with the repaired sampler, on a separately created instance, `stage2-1327-remeasure`, one SSD node, Enterprise edition, `us-central1-b`.
The campaign journal is a one-shot state machine and cannot replay an auxiliary phase, so the repeat ran through a standalone runner that supplies its own lease and drives the same production entry point with the same frozen parameters: 1 KiB payloads, parallelism 1, four in-flight entries per subtask, a one-second checkpoint interval, a ten-second warmup and an 1,800-second measured window.
The runner creates and deletes no service resource.

The observation completed, admitting 1,611,366 measured inputs at 895.1 inputs per second.

| Observation | Value |
| --- | --- |
| Marker samples | 30, one per minute across the window |
| Marker cells on the hot row | 0 at the first sample, 1,445,269 at the last |
| Growth per sample | 34,187 minimum, 53,256 maximum, 49,837 mean |
| Marker logical bytes on the hot row | 47.7 MB at the last sample |
| `table/bytes_used` | 0 until 12:16Z, then 125.9 MB, flat to the end |
| `cluster/cpu_load` | 0.02 rising to 0.25, flat from 12:02Z |

Same-row marker growth is linear across the window and shows no reclamation within a run: one row accumulated 1.4 million marker cells in thirty minutes, one per committed envelope, and nothing removed them while the job ran.
That is the growth #1327 asked to observe, and it is a property of the marker design rather than of this workload's size.

The storage timecourse is coarser than the marker one and this record does not read more into it than it carries.
`table/bytes_used` is published infrequently and reported a single step rather than a curve, so the within-run storage trajectory is not observable from it; the marker series above is the instrument that resolves the growth.
The flat `cluster/cpu_load` at about a quarter of one node says the observation was client-bound, so these figures bound the marker growth rather than the service's capacity.

## What is unmeasured

Every cell of the matrix produced retained evidence and both auxiliary observations ran, so nothing of the protocol's coverage is outstanding.

The serialized control ran inside the campaign and is recorded there.
The sustained phase's marker timecourse and storage series come from the repeat described above, on a different instance of the same shape and edition, and not from the campaign's own sustained phase; the record treats them as a separate observation rather than as part of the campaign's sequence.

Physical Bigtable storage was captured per cell for all 108 cells, from the original Monitoring timestamps, and the metric's publication delay means a sample at or after the end of a cell's observations is not available for every cell.
Checkpoint storage, serialized checkpoint size, heap after collection, peak heap, GC time and serialization allocation are recorded per run for every valid observation.

## Verdict

The assessment declines the checkpoint-owned mode at the ADR-0104 gate.
No cell of the fifteen measured in both arms meets the visibility criterion, and the closest is thirty-two times the constrained bound.
No supported workload follows from these measurements.

The 93 cells without a valid comparison are not a gap in coverage but a result: the staged arm could not produce three valid repetitions there under ceilings the bulk arm never reached, and 129 of the 162 staged runs at 64 KiB ended at the staging capacity.
A supported workload is therefore unlikely to be found among the heavier cells, and this record does not claim to have ruled one out by measurement.

One campaign is one sample.
The three repetitions bound the variation within a cell and nothing here bounds the variation between campaigns.
The margin against the latency criterion is two orders of magnitude, which is why the verdict does not wait for a second campaign.

The mode's implementation is unaffected by this verdict and its correctness acceptance stands, recorded under [#1319](https://github.com/flink-gcp/flink-connector-gcp/issues/1319) on 2026-09-14.
What the verdict withholds is release and support: user-facing documentation continues to describe `EXACTLY_ONCE` on this connector as experimental and not supported, now with a measured reason rather than a pending assessment.
Whether the mode stays in the tree as experimental or is withdrawn is a separate decision and needs its own record.

## Cost and resources

| Resource | Period | Rate | Cost |
| --- | --- | --- | ---: |
| Observation host `stage2-1327-host`, `e2-standard-4` | 2026-09-19T07:39Z to 2026-09-21T12:50Z, about 53 hours | USD 0.134 per hour | about USD 7.10 |
| 50 GiB pd-balanced on that host | the same 53 hours | USD 0.000136986 per GiB-hour | about USD 0.36 |
| Bigtable instance `stage2-1327-trial`, one SSD node | 2026-09-19T08:57Z to 2026-09-21T11:12Z | free trial | none |
| Bigtable instance `stage2-1327-remeasure`, one SSD node, Enterprise edition | 2026-09-21T11:45Z to 12:50Z, about one hour | USD 0.65 per node-hour published for `us-central1` | about USD 1.30 |

The matrix itself cost nothing in Bigtable charges: a free trial gives one SSD node for ten days, which is the whole instrument this protocol needs, and the campaign neither upgraded it nor exceeded it.
The repeat of the sustained observation ran on a paid instance because the campaign's trial no longer existed by then and a free trial is available once per project.
Bigtable bills the maximum node count in each clock hour with a one-hour minimum, so an instance that lives for forty minutes across two clock hours is charged for two node-hours; the figure above uses that rule rather than the elapsed time.
The published `us-central1` node rate was not separately confirmed for the Enterprise edition this instance used, so its cost may be somewhat above the figure shown.
Storage held 125.9 MB for under an hour and is negligible against the node charge.

Total spend is therefore of the order of USD 9 against the separate USD 20 authorization for #1327, and the campaign consumed no part of the earlier #1319 allowance.

All owned resources were deleted after their evidence was collected on 2026-09-21, and their absence was verified: the project lists no Bigtable instance, no Compute Engine instance and no disk from this campaign.
