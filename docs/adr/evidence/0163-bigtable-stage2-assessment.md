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
The verdict above does not subtract it.
Reading the result is another matter, because the baseline is an eager sink and a staged write cannot be visible before its checkpoint completes, so the ratio charges that wait to the mode by construction.
The measured staged p95 values run from 5.7 to 48.9 seconds at checkpoint intervals of 1 and 10 seconds, exceeding the interval by 4.7 to 38.9 seconds.
Waiting for the owning checkpoint accounts for at most one interval, since an input can arrive just before its checkpoint or just after the previous one, so the excess is checkpoint completion and the commit drain together.
Percentiles do not subtract, so the excess bounds those two rather than measuring the drain; the commit records below measure the drain directly.
[What limits the drain](#what-limits-the-drain) below shows what set its length.
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

The first cause is the staging capacity, and the capacity is a value this campaign chose.
A staged writer holds every envelope admitted since the last checkpoint, and `BigtableStagedWriter.write` fails the run when the retained work reaches its configured capacity, because the writer applies no backpressure and the harness source has no rate limiter.
The behaviour belongs to the mode; the threshold does not.
`stagedBytes` was 192 MiB per subtask, three times the production default of 64 MiB, and it was the largest value the parallelism-16 cells admitted inside a 10 GiB heap, as the calibration above records, so the binding constraint behind the number is heap.
Retained work is the payload size times the admission rate times the checkpoint interval, so at the same rate and interval the 64 KiB half of the matrix exhausts the same capacity sixty-four times sooner than the 1 KiB half: with the 256 accounting bytes each entry is charged, 192 MiB holds at most about 157,000 envelopes of 1 KiB but about 3,060 of 64 KiB, and fewer once row keys and markers are counted.
The counts follow that: 129 of the 162 staged runs attempted at 64 KiB were censored this way, against 27 of 162 at 1 KiB.

The second cause is checkpoint expiry, concentrated in the 1 KiB half where capacity lasts long enough for a checkpoint to be attempted over a large retained set.
A checkpoint of the staged mode must snapshot every envelope the writers have retained since the last one, so the same accumulation that exhausts capacity also lengthens the snapshot, and these runs exceeded the configured 60-second timeout until Flink's failure threshold stopped the job.
That timeout was fixed before the campaign and not changed during it, as the protocol requires.
A longer one would have converted some of these runs into observations, but into observations whose visibility latency was already past the criterion by two orders of magnitude.
The [2026-09-07 lease](0163-bigtable-stage2-experiment-harness.md) saw this symptom once and could not repeat it; 78 occurrences here settle that it reproduces.

Censoring and failure are distinct and this record keeps them apart.
A censored run is one the protocol's own per-run ceiling stopped, and it is not by itself evidence that the mode cannot run that workload; what it does show is that the staged arm reaches those ceilings where the bulk arm, under the same ceilings, never does.

## What limits the drain

The matrix's in-flight dimension is the committer's concurrency.
For a staged run the harness builds the production sink with `BigtableRequestOptions.maxInFlightRequests` set to the cell's in-flight value, so every staged cell committed with 1, 4 or 16 conditional writes in flight.
The production default is 100, and no cell measured it.

The campaign recorded each committer's drain.
Dividing a run's largest commit batch by its longest commit batch time bounds that committer's drain rate from above, and dividing the committer's concurrency by that rate gives the time each conditional write held its slot:

| Keys | Commit concurrency | Runs | Median drain per committer | Time per write |
| --- | ---: | ---: | ---: | ---: |
| Distinct rows | 1 | 16 | 144/s | 6.9 ms |
| Distinct rows | 4 | 16 | 397/s | 10.1 ms |
| Distinct rows | 16 | 22 | 596/s | 26.9 ms |
| Hot | 1 | 6 | 90/s | 11.1 ms |
| Hot | 4 | 7 | 229/s | 17.4 ms |
| Hot | 16 | 13 | 605/s | 26.5 ms |

The last column assumes that every slot was occupied for the whole batch; the concurrency-16 runs of the invalid pass described below kept only 6 to 11 of their 16 slots on the wire on average, which suggests it was not.
At a concurrency of one the committer issued one conditional write every 6.9 ms, against an in-zone round trip of about 4.4 ms measured later, so work around each request already took a third of the time.
A hot cell sends nine inputs in ten to one row, which is why its quotient starts higher.

A probe on 2026-09-23 measured what the service accepts at the committer's request shape.
Each request was a `CheckAndMutateRow` whose predicate looks for the envelope's marker qualifier in the marker family and, when it is absent, writes a 1 KiB cell and the marker.
It ran against a separately created one-node Enterprise instance in `us-central1-b`, from a workstation about 150 ms away, with concurrency bounded by a semaphore:

| Keys | Concurrency | Rate | p50 | p95 |
| --- | ---: | ---: | ---: | ---: |
| Distinct rows | 1 | 4/s | 152 ms | 735 ms |
| Distinct rows | 100 | 650/s | 150 ms | 159 ms |
| Distinct rows | 400 | 2,506/s | 150 ms | 167 ms |
| Distinct rows | 1,000 | 5,553/s | 154 ms | 258 ms |
| One row | 100 | 652/s | 150 ms | 158 ms |
| One row | 400 | 2,199/s | 175 ms | 208 ms |
| One row | 1,000 | 2,073/s | 458 ms | 507 ms |

On distinct rows the rate grew with concurrency while the median latency stayed at the round trip, so one node was not saturated at 5,553 conditional writes per second.
On a single row the rate stopped growing between concurrencies of 400 and 1,000, near 2,100 per second, while the median latency tripled.
From that distance a concurrency of 100 was held down by the round trip, so the probe could not show where an in-zone committer reaches either limit; the measurement below does.

## Visibility at the default commit concurrency

[#1464](https://github.com/flink-gcp/flink-connector-gcp/issues/1464) measured the staged mode at its default commit concurrency of 100 before release, with the at-least-once sink at its own default of 1,000 in-flight entries as the reference.
The instrument gained four things for it:

- the bound on its in-flight value was raised to admit both defaults
- each commit invocation is recorded with its entries, duration and the committing thread's CPU time
- the time each completion spent in the instrument's bookkeeping is recorded apart from the request's round trip
- the peak of outstanding writes is reported whether a run succeeds or fails, a report added after the staged matrix ran

A test in `Stage2JobITCase` drives a staged run at 100 and fails unless more than 16 requests are outstanding at once.

The first staged pass is not used as a measurement of the default.
Its runtime was built from compiled test classes that still held a mutation from checking that test, which capped the sink's `maxInFlightRequests` at 16.
Every staged run that reported a peak had 16 requests outstanding per committer, 16 at parallelism 1 and 64 at parallelism 4, which is how the defect was found.
The staged half was then repeated with a runtime built from a clean compile and checked for the mutation in its bytecode.
The bulk arm does not pass through the mutated class and is taken from the first pass.

| Input | Value |
| --- | --- |
| Bigtable instances | `stage2-1464` for the bulk arm and the short probes, `stage2-1464b` for the repeated staged arm, `stage2-1464c` for the host comparison; each one SSD node, Enterprise edition, `us-central1-b`, app profile `single-cluster` with transactional writes |
| Observation hosts | `e2-standard-4` in `us-central1-b`, the campaign's shape (`stage2-1464-host`, `stage2-1464-host2`, `stage2-1464-host4c`); `e2-standard-16` in the same zone for the host comparison (`stage2-1464-host16`, `stage2-1464-host16b`) |
| Source | the staged matrix ran `cc3c4f102a95e7b699702cb43da55774e32b0905` (runtime SHA-256 `bf0ff9a3b84e96284b7f8537b745b62a1ee53dcfb0a829f1294746f8f301a30e`); the host comparison ran `6c416e760244162b23c5c0ef330af2fe4923143e` (runtime `c6b43be0db1728e4ca485f00515c02510be10e5df1da9cb7909a70b8b74d368f`), which adds the thread CPU reading |
| JVM and Flink | OpenJDK 17.0.20.1 from Ubuntu 24.04 with the campaign's flags; Flink 2.2.1, the build's default, so the figures were not measured on Flink 1.20 |
| Per-run capacities | the campaign's, listed above |
| Matrix | 1 KiB payloads; distinct rows and hot key; parallelism 1 and 4; checkpoint intervals of 1 and 10 seconds; staged at 100 and bulk at 1,000; three runs of each arm in fresh JVMs, in an order shuffled with seed 1464 |
| Windows | the campaign's: warmup of the larger of 10 seconds and the interval, measurement of the larger of 30 seconds and three intervals |

Every figure below is a range across a cell's runs, each run's value computed from its own records.

| Keys | Parallelism | Interval | Arm | Observed | Inputs/s | Visibility p50 | Visibility p95 |
| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: |
| Distinct rows | 1 | 1 s | bulk | 3 of 3 | 9,691–10,174 | 0.014–0.015 s | 0.023–0.024 s |
| Distinct rows | 1 | 1 s | staged | 3 of 3 | 3,243–3,474 | 1.7–2.5 s | 3.5–5.1 s |
| Distinct rows | 1 | 10 s | bulk | 3 of 3 | 9,573–9,823 | 0.014–0.015 s | 0.023–0.024 s |
| Distinct rows | 1 | 10 s | staged | 3 of 3 | 781–1,723 | 4.7–36.5 s | 7.6–37.7 s |
| Distinct rows | 4 | 1 s | bulk | 3 of 3 | 9,576–9,994 | 0.031–0.032 s | 0.059–0.061 s |
| Distinct rows | 4 | 1 s | staged | 3 of 3 | 3,068–3,241 | 4.8–5.8 s | 7.2–7.6 s |
| Distinct rows | 4 | 10 s | bulk | 3 of 3 | 9,726–10,073 | 0.031–0.033 s | 0.058–0.062 s |
| Distinct rows | 4 | 10 s | staged | 3 of 3 | 337–2,475 | 13.4–29.0 s | 17.0–30.1 s |
| Hot key | 1 | 1 s | bulk | 3 of 3 | 10,310–10,427 | 0.012 s | 0.020–0.021 s |
| Hot key | 1 | 1 s | staged | 3 of 3 | 1,563–1,893 | 2.4–3.8 s | 6.0–8.1 s |
| Hot key | 1 | 10 s | bulk | 3 of 3 | 10,008–10,584 | 0.012–0.013 s | 0.020–0.023 s |
| Hot key | 1 | 10 s | staged | 3 of 3 | 387–1,135 | 21.9–44.0 s | 24.6–52.3 s |
| Hot key | 4 | 1 s | bulk | 3 of 3 | 9,886–10,393 | 0.029–0.030 s | 0.056–0.059 s |
| Hot key | 4 | 1 s | staged | 3 of 3 | 2,187–2,266 | 23.2–30.2 s | 43.6–67.4 s |
| Hot key | 4 | 10 s | bulk | 3 of 3 | 9,196–10,290 | 0.029–0.033 s | 0.057–0.064 s |
| Hot key | 4 | 10 s | staged | 1 of 3 | 219 | 50.3 s | 52.0 s |

All 24 bulk runs and 22 of the 24 staged runs were observed; the two others, both on hot keys at parallelism 4 and ten seconds, ended in checkpoint expiry.
Most staged runs at a ten-second interval, and the hot-key runs at parallelism 4, show the backlog the campaign saw: the harness source has no rate limiter, a commit could take longer than the interval, and visibility grew through the window.
One distinct-row run at parallelism 1 and ten seconds cleared its backlog after a long first commit and kept its p95 at 7.6 seconds.
The two failed runs predate the instrument's failure-path report and have no recorded peak.

At the default the committer's bound was reached.
Every observed staged run at parallelism 1 peaked at 93 to 100 outstanding requests, and runs at parallelism 4 at 207 to 375 across the job.
On distinct rows at parallelism 1 the job committed 3,784 to 4,785 conditional writes per second of commit time with a mean round trip of 3.7 to 5.9 ms, so by Little's law about 15 to 22 writes were on the wire on average.
That count is not the committer's own queue: the bound applies to requests issued and not yet collected, and the committer collects its oldest request first.
The invalid pass ran a genuine concurrency of 16 on a host of the same shape and drained 1,760 to 2,774 per second there, so the default roughly doubled a single committer's drain.

The limit was then looked for by running the same staged cell, distinct rows at a one-second interval and parallelism 1 or 4, on both host shapes with the committing thread's CPU time and the host's load recorded.
Beside it a bare client issued the committer's request shape from one JVM for 200,000 requests, so that its rate is measured after the JIT has warmed:

| Host | What issued | Drain | Host busy (mean) | Committer thread share | Mean outstanding | Visibility p95 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `e2-standard-4` | staged, parallelism 1 | 3,026–3,982/s | 68–77% | 0.56–0.58 | 21–44 | 4.5–6.6 s |
| `e2-standard-4` | bare client, concurrency 100 | 8,156–9,697/s | 81–84% | — | — | — |
| `e2-standard-16` | staged, parallelism 1 | 7,041–7,799/s | 20–21% | 0.61–0.69 | 41–46 | 2.6–3.5 s |
| `e2-standard-16` | staged, parallelism 4 | 8,681–10,212/s | 27–30% | 0.32–0.35 | 112–134 | 1.7–2.1 s |
| `e2-standard-16` | bare client, concurrency 100 | 12,644–13,654/s | 27–31% | — | — | — |

The thread share is the committing thread's CPU time over its commit time.
The host's load is averaged over the whole run, including the source, the writer and the warmup, while the drain covers commit time only and the bare-client rate covers the second half of each run, so the comparisons below are approximate.

The staged path writes each mutation with its own `CheckAndMutateRow`, because the check for the envelope's marker and the write must be one atomic operation and the Bigtable data API has no multi-row conditional write, while the at-least-once sink sends `MutateRows` batches that carry many rows per call.
Each staged write therefore pays a round trip and the client's per-call processor cost that a batch amortizes, and a gap to the at-least-once sink's roughly 10,000 inputs per second in the same matrix is expected from the design.
On the four-processor host the staged committer drained 37 to 42% of a bare client's rate, paired run by run, and busy share times processors over rate gives about 0.70 to 1.02 ms of host processor time per staged write against 0.33 to 0.40 ms per client write, about two to two and a half times as much.
That ratio includes the instrument's own per-write work, which the [next section](#where-a-single-committers-time-goes) measures at about a third to two-fifths of the staged path's processor time per write.

The service was not the limit on distinct rows.
One node accepted at least about 12,600 of these writes per second from one client, and its `cluster/cpu_load` stayed at or below 0.48 while the staged runs committed.

These measurements did not identify what stops a single committer short of the service's rate; the [next section](#where-a-single-committers-time-goes) does.
The instrument recorded the time the committing thread spent waiting for its oldest request, either at the bound or at the end of a batch, without telling the two apart.
On the sixteen-processor host at parallelism 1 the thread was on a processor for 61 to 69% of its commit time and waiting on a request for 20 to 29%, while the host stayed about 80% idle; at parallelism 4 it waited for 46 to 51%.
On the four-processor host the thread was on a processor for 56 to 58% and waiting for 6 to 26%, and the host ran at 90% or more for stretches of 6 to 8 seconds in every run, so contention for processors there is not excluded.
More subtasks raised the drain on the sixteen-processor host, from 7,041 to 7,799 per second with one to 8,681 to 10,212 with four, but on the four-processor host four subtasks drained 2,833 to 3,252 per second against 3,784 to 4,785 with one, and visibility was longer.
Part of the per-write cost is the instrument's own: it additionally holds each completion on the transport thread for its bookkeeping, about 2.4 ms at parallelism 4 and a one-second interval and 0.06 to 0.5 ms elsewhere, and it converts each request a second time to observe it.

The short probes run earlier on the four-processor host understate the client.
From a fresh JVM, over 20,000 to 40,000 requests, they stopped near 2,500 to 3,000 per second at every concurrency from 16 to 1,000, and running two or four of them at once did not raise the total; the warmed runs above show that this was a limit of short runs from a fresh JVM, not the host's ceiling.
They remain useful for the single-row plateau and for comparing settings under the same conditions:

| Keys | Concurrency | Slot reuse | Rate | p50 | p95 |
| --- | ---: | --- | ---: | ---: | ---: |
| Distinct rows | 1 | any | 220–229/s | 4.1–4.3 ms | 4.9–5.2 ms |
| Distinct rows | 16 | any | 2,441–2,554/s | 3.9–4.2 ms | 6.5–7.4 ms |
| Distinct rows | 16 | oldest first | 2,159–2,459/s | 3.5–4.1 ms | 5.6–6.6 ms |
| Distinct rows | 100 | any | 2,380–3,000/s | 3.8–7.7 ms | 6.8–25.2 ms |
| Distinct rows | 100 | oldest first | 2,401–2,962/s | 3.9–7.9 ms | 6.9–23.2 ms |
| Distinct rows | 1,000 | any | 2,730–3,139/s | 6.6–8.7 ms | 20.0–27.9 ms |
| One row | 16 | any | 1,657–2,052/s | 7.1–9.3 ms | 9.6–12.8 ms |
| One row | 100 | any | 1,828–2,069/s | 45.8–54.1 ms | 58.4–62.4 ms |
| One row | 100 | oldest first | 1,703–2,217/s | 32.4–57.5 ms | 48.7–68.6 ms |
| One row | 400 | any | 1,712–2,124/s | 182–231 ms | 209–263 ms |
| One row | 1,000 | any | 1,632–2,173/s | 360–616 ms | 453–677 ms |

Under those conditions a channel pool fixed at a single channel ran about 20 to 30% faster than the default; a pool of 40 channels, sessions turned off, and the client's built-in metrics or DirectPath turned off did not change the rate beyond run-to-run spread, and awaiting the oldest request first measured the same as reusing any free slot.

On a hot key the single row is the limit and the service imposes it.
The row accepted about 1,600 to 2,200 conditional writes per second at every concurrency from 16 to 1,000, and its latency grew in proportion to the concurrency, as it must when requests queue for one row.
The staged hot-key runs at parallelism 1 committed 1,417 to 2,085 per second with a mean round trip of 30 to 49 ms, at or just below that plateau; the harness sends nine inputs in ten to the hot row, so the writes to that row cannot go faster whatever the concurrency, and at parallelism 4 the job's 2,233 to 2,360 per second across all rows is consistent with that rate plus the tenth that goes elsewhere.
That limit belongs to the key distribution rather than to the connector, and it holds for any writer that uses conditional writes on one row.

These measurements are consistent with the campaign's quotient growing from 7 to 27 ms, though the campaign recorded no processor data to confirm the mechanism.
The quotient divides the configured concurrency by the drain, and when the drain stops short of the bound, a larger bound adds idle slots to the numerator; at parallelism 16 the job's sixteen committers also shared the four-processor host.
At the default a write's mean round trip on distinct rows was 3.7 to 5.9 ms at parallelism 1 and 8.9 to 15.5 ms at parallelism 4 on the four-processor host, with the instrument's hold of about 2.4 ms at parallelism 4 and a one-second interval coming on top, and it rose further on a hot key because requests queued for the row.

## Where a single committer's time goes

[#1476](https://github.com/flink-gcp/flink-connector-gcp/issues/1476) took the question the previous section left open and measured it on 2026-09-23 and 2026-09-24, without changing the connector.
It extended the Stage 2 instrument in three ways.
Each commit invocation now records its send phase (from its start to its last send) and its drain phase (from the last send to its end), the committing thread's time in a waiting state, and, for each wait on the oldest request, whether a send followed it (a wait at the bound) or not (the end-of-batch drain).
A lightweight mode (`-Dstage2.instrument=light`) skips the per-write bookkeeping that proves delivery: the inventory ledger's writes and reads, the marker records, the completion histogram and the instrument's own copy of each request.
It keeps the batch records, every budget and target guard, and the processor readings, and it still wraps each completion to record the committer's waits and the completion hold.
It therefore measures the connector's drain with far less of the instrument's cost added, though not with none, and it cannot verify readback or visibility, so it runs paired with the full mode rather than alone.
The run also records the process's processor time and, for each group of threads by name, their processor time and allocated bytes, and a source admission limit (`-Dstage2.admitPerSecond`) runs different arms at the same offered load.

Three environments answer different parts of the question.
The Bigtable emulator (`google-cloud-cli:583.0.0-emulators`) on a ten-core Apple M1 Pro workstation costs nothing and gives short round trips, where processor cost dominates.
The same workstation against a one-node Enterprise instance in `us-central1-b`, where the full instrument measured a mean round trip of 171 to 174 ms, makes the round trip dominate.
An `e2-standard-16` host in the instance's zone repeats the #1464 conditions.
All runs used parallelism 1, 1 KiB rows on distinct keys and a one-second checkpoint interval, and every figure below is a range across runs, each computed from that run's own records.

### Where the committer waits

The committer's waiting is almost all at its in-flight bound.
On the emulator at 10,000 inputs per second, a wait that a send followed took 75 to 76% of commit time under the full instrument and 59 to 63% under the lightweight one, and the end-of-batch drain took 0.5 to 0.6%; the drain figures here are the waits after each invocation's last send.
From the workstation to the instance the drain took 3 to 4% under the lightweight instrument and 12 to 21% under the full one, whose ledger slows the source and so leaves batches of a few hundred writes, each ending with about one round trip in which fewer requests are outstanding.

When the committer blocks on its oldest request, some requests behind it are usually complete already.
On the emulator at 10,000 inputs per second that count averaged 11.2 to 11.4 under the lightweight instrument and 26.0 to 26.7 under the full one; from the workstation it was 3 to 5 of 100, and in the zone 12 to 45.
A committer that waited for any completion rather than the oldest could reuse those slots sooner, so the reaping order was changed and measured.
The committer offered each request to a completion queue as it finished and took the first one from it, under the same bound, and a unit test confirmed that a completed request behind a pending one freed its slot.
On the emulator at 25,000 inputs per second under the full instrument, the drain was 14,216 to 15,013 per second against 14,275 to 15,267 with the oldest first, and the committing thread's processor time per write rose from 14.7 to 15.4 µs to 27.0 to 29.2 µs.
The reason is how often the thread wakes: with the oldest first, one wake-up collects the head and the requests already complete behind it, while a completion queue wakes the thread for every completion.
The change was reverted, and a wait for any completion is not adopted.
The bare-client probes in the previous section, which measured the two orders the same at a concurrency of 100 on the service, agree.

### What the instrument, the connector and the client each cost

The emulator separates the three at the same offered load of 10,000 inputs per second, with a bare client issuing the committer's request shape from one JVM at the same rate:

| Arm | Runs | Process CPU per write | CPU no thread accounts for | Task-thread allocation per write |
| --- | ---: | ---: | ---: | ---: |
| Bare client | 3 | 101–103 µs | 1.8–2.7 µs | 18.6 KiB on the issuing thread |
| Staged, lightweight instrument | 3 | 126–130 µs | 30–33 µs | 105–107 KiB |
| Staged, full instrument | 3 | 206–214 µs | 35–40 µs | 105–107 KiB |

The means of the three arms are 102, 129 and 209 µs, so the full instrument added about 80 µs per write and the staged path over the bare client about 27 µs.
The full instrument's share is therefore about 38% of its per-write processor time here, and about three-quarters of what the staged path cost beyond the bare client; on the zone's host below, the same comparison of run means gives about a third.
Processor time that no Java thread accounts for is most likely the collector's and the compiler's: it is 1.8 to 2.7 µs per write for the bare client and 30 to 40 µs on the staged path, which allocates five to six times as much per write, though removing most of that allocation below lowered it only to 26 to 28 µs.

A flight recording of one lightweight run at 12,000 inputs per second shows where the task thread's time goes.
Of the samples on the thread that runs the source, the writer and the committer, the instrument's input generation took 37%, the Bigtable client's gax and gRPC code about 20%, protobuf about 3%, staging the envelope about 13%, and the committer's own code, including converting the stored request back into a `ConditionalRowMutation`, about 1%.
Sending the stored request without that conversion, or resolving the destination once per batch, could therefore save at most that 1%, and neither is pursued.

Most of the staged path's allocation is Flink's rather than the connector's.
Flink's `SimpleVersionedSerialization.writeVersionAndSerializeList` calls the element serializer twice for every element, once for the length it writes and once for the bytes, and the committer operator's state nests that list three levels deep, so every pending committable is serialized eight times per checkpoint.
The instrument counted exactly eight serializations per committed write in every commit interval and no deserialization.
With a copy of that method that writes the first result, placed ahead of `flink-core` on the test classpath, the count fell to one and the lightweight arm allocated 43 to 44 KiB per write instead of 105 to 107, used 111 to 114 µs of processor time per write instead of 126 to 130, and drained at the same rate.
Nothing in the connector can remove the repetition, and caching the serialized committable would not help because the encoding took about 0.1% of the samples; [#1486](https://github.com/flink-gcp/flink-connector-gcp/issues/1486) tracks reporting it upstream.

### What limits one committer

From the workstation to the instance, the committer is bound by the round trip:

| Arm | Bound | Runs | Drain | Committing thread on a processor | Waiting |
| --- | ---: | ---: | ---: | ---: | ---: |
| Full instrument | 100 | 2 | 272–293/s | 3% | 95–96% |
| Lightweight instrument | 100 | 3 | 372–399/s | 3–4% | 95–96% |
| Lightweight instrument | 400 | 2 | 1,469–1,538/s | 5–6% | 93–94% |

Four times the bound gave 3.7 to 4.1 times the drain.
These runs admitted at most 1,000 inputs per second, or 3,000 at the larger bound, because an unlimited source over this round trip let the first batch grow until its commit outlasted the checkpoint timeout or left the measurement window without an acknowledgement.
A third full-instrument run reused a table that a failed run had written to, measured a drain of 403 per second and then failed its readback on the earlier run's rows, so it is excluded.

In the instance's zone, as in #1464, the committer is bound by the committing thread's own processor time:

| Arm | Bound | Runs | Drain | Committing thread on a processor | Waiting | Host busy (mean) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Full instrument | 100 | 3 | 8,439–8,760/s | 76–79% | 20–23% | 21–25% |
| Lightweight instrument | 100 | 3 | 9,917–10,062/s | 74–80% | 19–26% | 24–27% |
| Lightweight instrument | 400 | 2 | 11,865–12,245/s | 84–85% | 14–15% | 25–26% |
| Bare client, second half of 200,000 requests | 100 | 2 | 12,328–13,062/s | — | — | 33% |

Four times the bound raised the drain by about a fifth, and the host stayed three-quarters idle.
The committer sends every request from the one committing thread, and in the workstation's flight recording most of that thread's commit time was the client's own send path rather than the connector's code, which suggests that a single committer's rate is what one thread can push through the client; the zone's runs recorded no profile to confirm it.
The gap #1464 measured, 7,041 to 7,799 per second staged against 12,644 to 13,654 for a bare client on the same host shape, is thus mostly the instrument and the bound: the lightweight instrument at a bound of 400 drained 11,865 to 12,245 per second, close to the bare client's 12,328 to 13,062 at 100.
More committers raise the job's rate where processors are free, as #1464's four subtasks on the sixteen-processor host did.

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
What the verdict withholds is a supported-workload claim.
[ADR-0166](../0166-bigtable-implementation-precedes-final-stage2-acceptance.md) retains the mode as experimental.
Most of the visibility cost the verdict rests on is the commit drain rather than the checkpoint wait.
The default-concurrency measurement lowers that cost but does not change the verdict: at the default the staged p95 on distinct rows was 3.5 to 7.6 seconds at a one-second interval on the campaign's host shape and 1.7 to 3.5 seconds on a sixteen-processor host, and grew through most runs at ten seconds, against 20 to 64 milliseconds for the at-least-once sink in every cell.

## Cost and resources

| Resource | Period | Rate | Cost |
| --- | --- | --- | ---: |
| Observation host `stage2-1327-host`, `e2-standard-4` | 2026-09-19T07:39Z to 2026-09-21T12:50Z, about 53 hours | USD 0.134 per hour | about USD 7.10 |
| 50 GiB pd-balanced on that host | the same 53 hours | USD 0.000136986 per GiB-hour | about USD 0.36 |
| Bigtable instance `stage2-1327-trial`, one SSD node | 2026-09-19T08:57Z to 2026-09-21T11:12Z | free trial | none |
| Bigtable instance `stage2-1327-remeasure`, one SSD node, Enterprise edition | 2026-09-21T11:45Z to 12:50Z, about one hour | USD 0.65 per node-hour published for `us-central1` | about USD 1.30 |
| Bigtable instance `stage2-1464`, one SSD node, Enterprise edition | 2026-09-23T02:59Z to 09:44Z, eight clock hours | USD 0.65 per node-hour | about USD 5.20 |
| Observation host `stage2-1464-host`, `e2-standard-4` | 2026-09-23T02:56Z to 09:30Z | USD 0.134 per hour | about USD 0.88 |
| Comparison host `stage2-1464-host16`, `e2-standard-16` | 2026-09-23T04:16Z to 09:43Z | USD 0.536 per hour | about USD 2.92 |
| Bigtable instance `stage2-1464b`, one SSD node, Enterprise edition | 2026-09-23T10:08Z to 11:13Z, two clock hours | USD 0.65 per node-hour | about USD 1.30 |
| Observation host `stage2-1464-host2`, `e2-standard-4` | 2026-09-23T10:32Z to 11:12Z | USD 0.134 per hour | about USD 0.09 |
| Bigtable instance `stage2-1464c`, one SSD node, Enterprise edition | 2026-09-23T13:23Z to 13:55Z, one clock hour | USD 0.65 per node-hour | about USD 0.65 |
| Comparison hosts `stage2-1464-host16b`, `e2-standard-16`, and `stage2-1464-host4c`, `e2-standard-4` | 2026-09-23T13:25Z to 13:54Z | USD 0.536 and 0.134 per hour | about USD 0.32 |
| Bigtable instance `stage2-1476`, one SSD node, Enterprise edition | 2026-09-23T20:45Z to 21:03Z, one clock hour | USD 0.65 per node-hour | about USD 0.65 |
| Bigtable instance `stage2-1476g`, one SSD node, Enterprise edition | 2026-09-24T05:42Z to 05:57Z, one clock hour | USD 0.65 per node-hour | about USD 0.65 |
| Observation host `stage2-1476-e16`, `e2-standard-16`, 50 GiB pd-balanced | 2026-09-24T05:43Z to 05:57Z | USD 0.536 per hour | about USD 0.13 |

The matrix itself cost nothing in Bigtable charges: a free trial gives one SSD node for ten days, which is the whole instrument this protocol needs, and the campaign neither upgraded it nor exceeded it.
The repeat of the sustained observation ran on a paid instance because the campaign's trial no longer existed by then and a free trial is available once per project.
Bigtable bills the maximum node count in each clock hour with a one-hour minimum, so an instance that lives for forty minutes across two clock hours is charged for two node-hours; the figure above uses that rule rather than the elapsed time.
The published `us-central1` node rate was not separately confirmed for the Enterprise edition this instance used, so its cost may be somewhat above the figure shown.
Storage held 125.9 MB for under an hour and is negligible against the node charge.

Total spend is therefore of the order of USD 9 against the separate USD 20 authorization for #1327, and the campaign consumed no part of the earlier #1319 allowance.

All owned resources were deleted after their evidence was collected on 2026-09-21, and their absence was verified: the project lists no Bigtable instance, no Compute Engine instance and no disk from this campaign.

The #1464 measurement cost about USD 11.5, including under USD 0.1 of disk, against a USD 5 authorization.
Three things account for the overrun: after the first matrix finished at 04:45Z the resources stayed up for about four and a half hours before the remaining probes ran, the staged arm had to be measured again once its instrument was found to be mutated, and the host comparison was added at the owner's request to identify what limits the drain.
Its resources were deleted on 2026-09-23 after the evidence was collected, and their absence was verified in the same way.

The #1476 measurement cost about USD 1.45 against a USD 5 authorization, because the emulator runs cost nothing and each paid resource lived for under twenty minutes.
Each resource was deleted once its evidence was on the workstation, on 2026-09-23 and 2026-09-24, and the project then listed no Bigtable instance, no Compute Engine instance and no disk.
The runtime that ran on the `e2-standard-16` host was built from a clean compile of `e27279766104c98f434e63a94ba183bd2ebdcfb3`, a working commit of the #1476 branch, and its archive's SHA-256 was `75793273fe67f0ad41f42f68bb17f87f13dc47682a24aad11838f9da0fa0155c`; its bytecode was checked for the reverted completion queue and for any cap on the requested concurrency before it was copied.
Every run in this section used that commit's instrument, and the review of the pull request then changed it in five ways that bear on the figures.
A request now reports itself complete to its invocation only after publishing its result, where the measured instrument reported it just before, so a count of requests complete behind the head may include the head itself; each blocked wait could be overstated by at most one.
The waiting-time accounting is now switched on before the run rather than at its first commit.
An invocation that sends nothing now reports no drain phase, and none of the 1,267 recorded invocations sent nothing.
A paced reader now counts an input only when one is emitted, so the measured runs admitted slightly less than their limit, which is what they show: 9,876 to 9,939 inputs per second against 10,000 on the emulator.
A lightweight run now fails when any write found its marker already present; every recorded lightweight run reported none.
