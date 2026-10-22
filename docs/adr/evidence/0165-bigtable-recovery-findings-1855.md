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

# Bigtable recovery trials: findings

These are the findings of the deployed Bigtable trials of [#1855](https://github.com/flink-gcp/flink-connector-gcp/issues/1855), run under their [preregistration](0165-bigtable-recovery-preregistration-1855.md) between 2026-10-19 and 2026-10-22 for [#1316](https://github.com/flink-gcp/flink-connector-gcp/issues/1316) and [#1317](https://github.com/flink-gcp/flink-connector-gcp/issues/1317).
One FlinkDeployment carried the checkpoint-owned `EXACTLY_ONCE` sink into `eo`, the `AT_LEAST_ONCE` sink into `alo` and the Change Streams relay from `eo` into `relay`.
Each of the four trial kinds reached a `usable` verdict, so all three components took a savepoint upgrade, a JobManager replacement, a TaskManager replacement, a rescale out and a rescale in on the deployed rig without losing a record.
Reaching them took eleven attempts; the [list of attempts](#every-admitted-attempt) gives each one, and none showed a connector defect.

## Result

| Trial | Run ID | Verdict | `eo` rows matched, missing | `alo` rows matched, missing, extra contributions | `relay` rows matched, missing, replayed rows |
| --- | --- | --- | --- | --- | --- |
| `jm-replacement` (pilot) | `bt1855-jm-a1` | `usable` | 180,000, 0 | 180,000, 0, 2,273 | 180,000, 0, 2,997 |
| `tm-replacement` | `bt1855-tm-a6` | `usable` | 180,000, 0 | 180,000, 0, 2,575 | 180,000, 0, 1,499 |
| `rescale-out` | `bt1855-ro-a1` | `usable` | 180,000, 0 | 180,000, 0, 2,253 | 180,000, 0, 3,000 |
| `rescale-in` | `bt1855-ri-a3` | `usable` | 180,000, 0 | 180,000, 0, 2,741 | 180,000, 0, 3,006 |

Every trial ran the `bigtable-recovery` image `sha256:f041f5486adbbe505d98ed112a7bf270f506863c5ad1cafefbcfbfac6cadf2bf` from the [Bigtable recovery publication](../../../kubernetes/images/README.md#bigtable-recovery-publication) of main `ed422edd4`.
No oracle found a foreign row or family, an `eo` row whose SUM differed from its contributions, a second marker, or a relayed change `eo` had not applied.
Each relay was judged: the input finished 1,250 to 1,485 seconds before the change stream's end, more than the 120 seconds the relay needs.

For each trial the final receipt and `just tier3-analyze` agree on `usable`, and the fault reads under the preregistration's acceptance criteria pass:

- `bt1855-jm-a1`: the receipt's kind is `jm-replacement`, the timing table names `savepoint-upgrade` and `jm-replacement`, and the failover proof records a new JobManager Pod UID (`5c766381…`, after `2cbe4b4d…`).
- `bt1855-tm-a6`: the kind is `tm-replacement`, and the timing table names `savepoint-upgrade` and `tm-replacement`. The supervisor deleted `taskmanager-1-2` (`c1a6110e…`), the TaskManager its rule named, and the `inventory` events show `taskmanager-1-1` (`fa0c7eb0…`) and the JobManager (`34117607…`) with their UIDs before and after the failover, with `taskmanager-1-5` in the deleted Pod's place.
- `bt1855-ro-a1`: the kind is `rescale-out`, the timing table names `rescale-out` and `jm-replacement`, and the upgrade proof records the checkpointed subtasks moving from 4 to 7, the three component vertices going from one subtask to two while the input stays at one.
- `bt1855-ri-a3`: the kind is `rescale-in`, the timing table names `rescale-in` and `jm-replacement`, and the upgrade proof records the checkpointed subtasks moving from 7 to 4, the three component vertices going from two subtasks to one.

Each read every family the runbook names in its `baseline`, `upgraded` and `finishing` windows, three to five readings in each window; the supervisor also measures outside the windows, during startup, the recoveries and the rest of the input, so each run holds 42 to 45 readings in all.

The pilot measured two premises the [Bigtable recovery foundation](../0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md#bigtable-recovery-foundation) left open, one of them only in part: the unconditional creator role was enough for the runner to create `bt1316-bt1855-jm-a1`, which does not test whether IAM checks a create against the project, the reason the role carries no condition; and the instance-name conditions admitted the workload's writes and change-stream reads and the supervisor's oracle reads, which no other Bigtable grant of those identities could have admitted. No grant finding stopped any attempt.

### Recovery

Each recovery is timed from the supervisor's record of its stage, as [ADR-0165](../0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md#recovery-timing-from-flinks-timestamps) defines it.
`Restored after` is Flink's restore of the state, which precedes the tasks' deployment, so it is a lower bound; `Acknowledged after` is the last acknowledgement of the checkpoint that proved the restored job, which follows its first input past the fault, so it is an upper bound.

| Run | Recovery | Fault | Restored from | Restored after | Acknowledged after |
| --- | --- | --- | --- | ---: | ---: |
| `bt1855-jm-a1` | upgrade | `savepoint-upgrade` | savepoint 15 | 32.9 s | 74.6 s |
| `bt1855-jm-a1` | failover | `jm-replacement` | checkpoint 26 | 35.3 s | 53.5 s |
| `bt1855-tm-a6` | upgrade | `savepoint-upgrade` | savepoint 15 | 49.0 s | 88.0 s |
| `bt1855-tm-a6` | failover | `tm-replacement` | checkpoint 26 | 10.8 s | 84.8 s |
| `bt1855-ro-a1` | upgrade | `rescale-out` | savepoint 15 | 53.5 s | 118.9 s |
| `bt1855-ro-a1` | failover | `jm-replacement` | checkpoint 26 | 50.4 s | 101.0 s |
| `bt1855-ri-a3` | upgrade | `rescale-in` | savepoint 15 | 49.0 s | 119.5 s |
| `bt1855-ri-a3` | failover | `jm-replacement` | checkpoint 26 | 66.7 s | 128.8 s |

Every recovery proved within 130 seconds, under both the 300-second budget the first attempts ran with and the 600-second one [#1882](https://github.com/flink-gcp/flink-connector-gcp/issues/1882) set before `bt1855-ri-a2`.

#### TaskManager replacement

The supervisor deleted `taskmanager-1-2` at 00:32:26Z on 2026-10-20, and `taskmanager-1-5` was in the next poll's inventory 18 seconds later.
The job had three pipelined regions, as its JobManager logged: the input reaches both sinks through an all-to-all partitioner, so they form one region, and the relay's forward-connected chain forms one region per subtask.
Each TaskManager has one slot, so the lost one held the sinks' region and one of the two relay regions, and the JobManager logged a five-task restart for the first and a one-task restart for relay subtask 1 of 2, each restoring checkpoint 26, which [#1871](https://github.com/flink-gcp/flink-connector-gcp/issues/1871) counts as one recovery.
The relay replayed 1,499 rows, about half the replays of the JobManager failovers, which restart both relay regions.

#### Committer deduplication and relay replays

The staged committer counts a committable it finds already applied by its marker as `requestsDeduplicated`, published here as a finding, not a threshold.

- After each upgrade the counter read 1,755, 1,541, 1,879 and 1,301 (`jm`, `tm`, `ro`, `ri`).
  A savepoint upgrade is a stop-with-savepoint, after which the committer has committed every envelope, so these are the savepoint's restored committables found applied, consistent with the drain the preregistration describes; the staged commit runs without client retries and no request failed, so nothing else adds to the count.
- After each failover it read 3,000, 2,997, 3,000 and 3,006, about one 30-second checkpoint interval at 100 records per second.
  A count of already-applied committables is consistent with checkpoint 26's commit having finished before the fault and does not prove it; whether any failover restored a commit still in flight stays open.

`alo`'s extra contributions, 2,253 to 2,741, count what the recoveries replayed and the client retried, without telling the two apart.
After each JobManager failover the relay's replayed rows equal the deduplication count within three rows, 2,997 against 3,000, 3,000 against 3,000 and 3,006 against 3,006.
That fits how the relay reads: `eo` makes a checkpoint's contributions visible only when that checkpoint's commit runs, after the relay source has taken its own snapshot for the same checkpoint, so restoring checkpoint 26 replays the batch `eo` committed for it, about one interval wherever in the next interval the fault struck.
The replayed rows count only replays whose first delivery had already reached `relay`, so they are a lower bound on what the source emitted again; the source contract delivers replayed changes again, and no exactly-once delivery is claimed for it.

#### Change Streams partitions

`eo`'s initial splits gave the source four partitions, and no reader received a partition change: `changeStreamCloseStreamsRead` stayed at zero in every reading of all four trials, so no service split or merge was observed, and none was forced.
At two subtasks each subtask held two partitions, both open.
At one subtask from a fresh start, in `bt1855-ro-a1`'s `baseline`, the coordinator handed out two partitions and held the other two unassigned, as the documentation describes once the subtask's two reads are open: they were unread for the ten minutes until the rescale out assigned them, and the relay oracle still passed because the rescaled readers opened them from their checkpointed start. The exercise reads the reader metrics, not the coordinator's unassigned-partition lag, so that wait shows only in the JobManager log and in the reader metrics' two reads and zero queued.
After `bt1855-ri-a3`'s rescale in, the one subtask held all four with two open, its bound, and two queued, and rotated them as the [source documentation](../../content/docs/connectors/datastream/bigtable.md) describes: `changeStreamReadsStarted` rose by about 30 every 75 seconds, each of the two open reads rotating at the five-second service heartbeat, and read 956 at the end, counting the reads since the JobManager failover reset it.
The queued partitions' lag read up to 28.3 seconds in the `upgraded` and 17.9 seconds in the `finishing` window, and up to 55.5 seconds in readings outside the windows, 50.5 seconds in the first reading after the rescale and peaks drifting up from about 30 to 55 seconds over the rest of the source's run, which went on reading for 23 minutes after the input finished at 00:45:15Z, until the change stream's end.
Readings 75 seconds apart cannot resolve single rotations, and whether the lag stays bounded over a longer input is not shown.

### Measurement families

The largest value any reading in the window returned, so readings outside the windows are not in it; they describe these runs at their fixed rate, and none is a threshold.
The components share TaskManagers, so the heap column describes the job.

| Run | Window | `eo` records in/s (sum) | `eo` busy ms/s | Back-pressured ms/s | `eo` staged entries (sum) | `eo` checkpoint end-to-end ms | `alo` busy ms/s | `relay` busy ms/s | Queued partition lag ms | TaskManager heap MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `bt1855-jm-a1` | `baseline` | 100.0 | 53 | 0 | 2,302 | 712 | 5 | 21 | 0 | 239 |
| `bt1855-jm-a1` | `upgraded` | 100.0 | 66 | 0 | 2,102 | 675 | 5 | 26 | 0 | 244 |
| `bt1855-jm-a1` | `finishing` | 100.0 | 57 | 0 | 2,563 | 676 | 4 | 20 | 0 | 180 |
| `bt1855-tm-a6` | `baseline` | 100.0 | 54 | 0 | 2,087 | 763 | 4 | 19 | 0 | 248 |
| `bt1855-tm-a6` | `upgraded` | 100.0 | 75 | 0 | 1,999 | 718 | 4 | 24 | 0 | 254 |
| `bt1855-tm-a6` | `finishing` | 100.1 | 60 | 0 | 2,101 | 757 | 5 | 25 | 0 | 253 |
| `bt1855-ro-a1` | `baseline` | 100.1 | 120 | 0 | 2,828 | 1,240 | 4 | 22 | 0 | 223 |
| `bt1855-ro-a1` | `upgraded` | 104.8 | 71 | 0 | 2,690 | 838 | 6 | 25 | 0 | 230 |
| `bt1855-ro-a1` | `finishing` | 100.1 | 53 | 0 | 1,739 | 815 | 5 | 20 | 0 | 284 |
| `bt1855-ri-a3` | `baseline` | 100.8 | 71 | 0 | 2,340 | 883 | 4 | 25 | 0 | 237 |
| `bt1855-ri-a3` | `upgraded` | 100.0 | 207 | 0 | 1,808 | 1,206 | 8 | 48 | 28,324 | 263 |
| `bt1855-ri-a3` | `finishing` | 100.1 | 67 | 0 | 2,128 | 903 | 3 | 16 | 17,896 | 298 |

A phase at one subtask carries the whole input on one `eo` subtask: the rescale out's `baseline` and the rescale in's `upgraded` window show the highest busy time and checkpoint duration, with no back-pressure.
The rescale in's queued-partition lag comes from the rotation above.
The staged committer reported no failed request in any window.

## Observations

- **The platform stopped or interrupted three of the seven attempts that did not reach `usable`.**
  A `kube-system` `kube-dns` Pod of system priority preempted the supervisor twice before admission (`bt1855-tm-a1` and `bt1855-tm-a2`), and Compute Engine reclaimed the Spot node of `bt1855-ri-a2`'s TaskManager 27 seconds after its change stream's end.
  The reclamation is read from Compute Engine's `compute.instances.preempted` audit entry and the kubelet's graceful-shutdown log, and the preemptor of the supervisor from the cluster's events read at the time, since the exported evidence carries none of them: it records only that the supervisor Job had no Pod.
- **Calls that got no answer stopped two attempts, and the rig now resolves both.**
  A table create at admission (`bt1855-tm-a3`) and an evidence write (`bt1855-tm-a4`) did not answer in time. [#1867](https://github.com/flink-gcp/flink-connector-gcp/issues/1867) reads an unanswered create back, and [#1869](https://github.com/flink-gcp/flink-connector-gcp/issues/1869) reads an unanswered control or evidence write back.
- **A TaskManager replacement restores once per failover region.**
  `bt1855-tm-a5` stopped as an unplanned job restart because the second region's restore of the same checkpoint was read as one, and #1871 corrected the count.
- **A replacement TaskManager can wait for a node.**
  `bt1855-ri-a1`'s new TaskManager was refused at 02:55:58Z on 2026-10-20 with two nodes unschedulable, which by the events are the two its predecessors had vacated, was scheduled after an Autopilot scale-up at 02:56:58Z, and had its tasks running 20.5 seconds before the 300-second budget ran out.
  Of the 135 seconds from its scheduling to running tasks, the image pull onto the fresh node took 19 and the TaskManager's launcher announced its start 61 seconds after the container started, its first Java log line three seconds later; the evidence does not show why the start took that long, and #1882 widened the budget to 600 seconds rather than removing the wait.
- **A Spot reclamation can be invisible to the exercise.**
  In `bt1855-ri-a2` Flink's resource manager deleted the reclaimed TaskManager's Pod and requested another within a second, before the supervisor's next poll could read the Pod's status, and the node's `Preempted` and `NodeNotReady` events are outside the namespace the exercise reads.
  The exercise therefore read "Unplanned workload replacement", which its campaign does not repeat, and the attribution to the platform was read from the audit logs by hand.
  This gap stays open.

## Cleanup

After every attempt, and before the next was dispatched, these were read: `bt1316-<run-id>` absent from the instance listing, nothing under `runs/<run-id>/` or `.inprogress/` in `gs://flink-gcp-tier3-bigtable`, `tier3-bigtable` holding no Pod or FlinkDeployment with its `tier3-idle` quota at zero Pods and claims, and the run record and the environment lock gone.
The supervisor's delete disables `eo`'s change stream before deleting the instance, which the service requires, as the [Bigtable recovery trial](../0165-opentofu-owns-kubernetes-foundation-and-cue-owns-applications.md#bigtable-recovery-trial) section records, and every receipt that created an instance records it `cleaned`.
For `bt1855-ro-a1` and `bt1855-ri-a1`, which a campaign dispatched as soon as the attempt before had released the lock, these reads were made after the campaign ended, from the receipts and directly, since no read could come between the attempts.
The `flink-gcp`, `tier3-bootstrap` and `tier3-operator` plans are the ones each receipt refreshed after its attempt, all empty.

## Every admitted attempt

Before each dispatch by hand the reads the preregistration lists passed, with the deadline its revisions give: the environment lock and run records absent, every earlier approval with its result, no other `tier3-run` workflow run queued or in progress, the run's evidence prefix and instance name free, the application digest present, and the window ending before the image deadline in force. For an attempt a campaign dispatched itself, its admission's own reads of the lock and the run record stood in for them.

| Run ID | Workflow run | Dispatched (UTC) | Rig | Outcome | Stop condition |
| --- | --- | --- | --- | --- | --- |
| `bt1855-jm-a1` | [39204161720](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/39204161720) | 2026-10-19T05:21Z | `ed422edd4` | `usable` | none |
| `bt1855-tm-a1` | [39210973798](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/39210973798) | 2026-10-19T06:37Z | `ed422edd4` | `kube-dns` preempted the supervisor before admission | Supervisor preempted before the instance |
| `bt1855-tm-a2` | [39212246176](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/39212246176) | 2026-10-19T06:50Z | `ed422edd4` | `kube-dns` preempted the supervisor before admission | Supervisor preempted before the instance |
| `bt1855-tm-a3` | [39213621606](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/39213621606) | 2026-10-19T07:04Z | `ed422edd4` | The `eo` table create did not answer within the 20-second transport timeout, read from the runner's log at the time; the run was stopped at admission and its instance deleted | A rig finding: repaired in #1867 |
| `bt1855-tm-a4` | [39255134858](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/39255134858) | 2026-10-19T12:50Z | `07a72c909` | An evidence write did not answer in the `baseline` stage | A rig finding: repaired in #1869 |
| `bt1855-tm-a5` | [39273269832](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/39273269832) | 2026-10-19T14:55Z | `a8ed85488` | Upgrade proved; "Unplanned job restart" on the second region's restore during the failover | A rig finding: repaired in #1871 |
| `bt1855-tm-a6` | [39345574896](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/39345574896) | 2026-10-20T00:05Z | `dd6d546dd` | `usable` | none |
| `bt1855-ro-a1` | [39352590897](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/39352590897) | 2026-10-20T01:17Z | `dd6d546dd` | `usable` | none |
| `bt1855-ri-a1` | [39359177398](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/39359177398) | 2026-10-20T02:29Z | `dd6d546dd` | "Recovery exercise deadline expired: upgrade"; the TaskManager waited for a node | Any other inconclusive: stop, repair (#1882) and approve again |
| `bt1855-ri-a2` | [39516936445](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/39516936445) | 2026-10-20T23:44Z | `5b428e379` | Both recoveries proved; a Spot reclamation of the TaskManager's node after the change stream's end; "Unplanned workload replacement" | Platform, read by hand: repeat once |
| `bt1855-ri-a3` | [39697226055](https://github.com/flink-gcp/flink-connector-gcp/actions/runs/39697226055) | 2026-10-22T00:02Z | `5b428e379` | `usable` | none |

From `bt1855-tm-a4` on, each dispatch by hand was a campaign of #1867, each approved by the owner: `bt1855-tm-a4`, `bt1855-tm-a5` and `bt1855-tm-a6` each began one of the remaining three kinds with two repeats, up to five attempts, and the first two ended at their own first attempt. The third ran `bt1855-tm-a6`, `bt1855-ro-a1` and `bt1855-ri-a1`, and `bt1855-ri-a2`, a one-kind campaign with one repeat, and `bt1855-ri-a3` were approved within its five attempts.
Every attempt dispatched the rig at the `main` commit current at its dispatch, and every attempt ran the one application image above.
Eight attempts ran the application, `bt1855-tm-a3` created an instance and deleted it at admission, and two stopped before admission.
The estimate charges USD 3.36 per attempt, so the eight come to at most USD 26.88 on its basis, plus `bt1855-tm-a3`'s instance-hour; the owner approved each attempt before its dispatch, and the actual charges were not read from billing.

## Deviations from the preregistration

The preregistration's [revisions](0165-bigtable-recovery-preregistration-1855.md#revisions-after-the-pilots-dispatch) record each one with the owner's decision:

- `tm-replacement` took six attempts and `rescale-in` three, beyond interpretation 1's one attempt per kind; each repeat was approved.
- The rig changed between attempts: the campaign dispatch and create read-back (#1867), unanswered writes (#1869), per-region restores (#1871), the recovery budget (#1882) and the renewed images ([#1884](https://github.com/flink-gcp/flink-connector-gcp/issues/1884)). The application image did not change.
- Within a campaign the next attempt was dispatched by the workflow itself, so the before-dispatch and cleanup reads the record places between attempts were replaced by the admission's own reads and read afterwards, as above, and the record's read that no other run is queued cannot hold for an attempt a campaign queues.
- `bt1855-ri-a2`'s platform stop was attributed by hand, since the exercise could not see it, and its repeat was dispatched by hand.
- The campaign took from the pilot's dispatch at 2026-10-19T05:21Z to the last refreshed plans at 2026-10-22T01:17Z rather than the eight hours the record expected, and the image deadline passed on the way; the owner approved deleting the `operator` and `flink` mirror versions so a new publication recreated them, and every window ended before the deadline in force.

`bt1855-ri-a1` is not a deviation: every Pod was Running at its deadline and no platform cause fell within the stage, so the Cloud Tasks deadline condition this record adopts does not apply, and the attempt falls to "any other inconclusive attempt", whose repair was #1882 and whose rerun the owner approved.
No result triggered interpretation 3: no `eo` contribution was missing or doubled, and no relay delivery was missing.

## Coverage of #1316 and #1317

The scope both issues share ([#1316](https://github.com/flink-gcp/flink-connector-gcp/issues/1316#issuecomment-6247061185)) is done when four deployed trials reach `usable`, one per kind, each with its savepoint upgrade, and cleanup verifies the instance absent, the namespace idle and the plans empty; the result above meets it.
Against the issues' acceptance lists:

| Acceptance item | Where it stands |
| --- | --- |
| A digest-pinned image, CUE application, and workload identities separate from provisioning and cleanup authority | [#1852](https://github.com/flink-gcp/flink-connector-gcp/issues/1852), [#1853](https://github.com/flink-gcp/flink-connector-gcp/issues/1853), [#1860](https://github.com/flink-gcp/flink-connector-gcp/issues/1860) and [#1864](https://github.com/flink-gcp/flink-connector-gcp/issues/1864); every trial created, wrote, read and deleted its instance under those grants |
| Production entry points through native remote clients with workload ADC, on a pinned runtime | DataStream sink and source, with no emulator endpoint or forwarding client, on Flink 2.2.1 and Java 17 only. The Table API entry points were set aside by the owner's scope decision |
| An at-least-once baseline and a checkpoint-owned case with immutable markers, a raw no-GC marker family and transactional single-cluster routing | `alo` and `eo` in every trial, through the `recovery` app profile |
| Pre-notification visibility, a stop-savepoint that drains pending work, completed-checkpoint recovery after a JobManager or TaskManager replacement during active writes, and rescale in and out with stable UIDs | Pre-notification visibility was not checked. Each trial's savepoint upgrade; `bt1855-jm-a1`, `bt1855-ro-a1` and `bt1855-ri-a3` for the JobManager and `bt1855-tm-a6` for the TaskManager; `bt1855-ro-a1` and `bt1855-ri-a3` for the rescales. Whether a failover restored a commit still in flight stays open, as the deduplication counter shows only already-applied committables |
| Generated identities read back to detect missing effects, extra SUM contributions and changed replay identities | The three oracles over all 180,000 identities in each trial. No response-loss claim is made: a Pod deletion is not a lost RPC response |
| Source events compared against input identities, with replays reported separately and no exactly-once delivery claimed | The relay oracle and its replayed-row counts above |
| Continued progress, bounded termination and active-read limits after recovery | Every job finished at its change stream's end; after the rescale in one subtask rotated four partitions through two reads, its queued lag under a minute but its peaks drifting up |
| Exported identities, checkpoint and savepoint IDs, event evidence, cancellation results and source metrics, failing on missing evidence, stalled progress or exhausted limits | Partly: Pod, job, checkpoint and savepoint identities, events and source metrics are exported, but the oracles export counts and the first keys of any deviation rather than the row identities, which the instance's deletion removes; the verdict withholds `usable` from missing coverage or an unproved recovery; cancellation results are the cleanup receipts, and the per-scenario limits were not carried over by the scope |
| Restored positions and partition coverage, distinguishing task rescaling from service topology change | The relay oracle's completeness implies every partition was relayed in each trial, but restored positions were not exported or observed directly; no service split or merge was observed, so that case is uncovered |
| Restart history, checkpoint statistics, memory, GC, back-pressure and connector metrics through external observation | The recovery table, the families above, and the exported Pod logs and events |
| Preregistered ceilings and stop conditions, shared admission and independent cleanup, and verified absence and idle state | A preregistered estimate, window and stop conditions, the per-scenario caps not carried over by the scope; the attempts' approvals; and [Cleanup](#cleanup) |

## What this does not establish

Each kind has one usable attempt at one input rate on Flink 2.2.1, so variance across repetitions and Flink 1.20 are not claimed, and the Table API entry points were not run.
The deduplication counter shows each failover's restored committables already applied, which leaves open whether any failover restored a commit that had not finished.
No service partition split or merge was observed, so the source's handling of one under recovery is not shown here, and the queued-partition lag after the rescale in drifted up, so its bound over a longer input is not shown either.
Every TaskManager and JobManager these trials lost shut down on a SIGTERM, whether the supervisor deleted it or a node was reclaimed; a process lost without one, detected only by a heartbeat timeout, did not occur.
These trials are not the sink's acceptance under [#1319](https://github.com/flink-gcp/flink-connector-gcp/issues/1319), which may reuse this evidence with its limits.
