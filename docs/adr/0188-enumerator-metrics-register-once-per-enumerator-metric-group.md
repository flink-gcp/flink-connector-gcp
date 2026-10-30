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

# ADR-0188: Enumerator metrics register once per enumerator metric group

- Status: Accepted
- Date: 2026-10-28
- Issues: [#1908], [#1914]
- Modules: base, bigquery, bigtable, firestore, pubsub, spanner
- Current behavior: an enumerator rebuilt by a failover or a restart keeps reporting through the
  series the first enumerator registered

## Context

Every source in this repository registers its enumerator's counters and gauges on
`SplitEnumeratorContext.metricGroup()`. That group outlives the enumerator.
`SourceCoordinatorContext.metricGroup()` answers `new InternalSplitEnumeratorMetricGroup(parent)`,
whose constructor calls `parent.addGroup("enumerator")`, and `AbstractMetricGroup.addGroup` answers
the existing subgroup of a name while that subgroup is open. The parent is the operator
coordinator's group, which survives a coordinator reset under both schedulers: the default
scheduler's `RecreateOnResetOperatorCoordinator` keeps the holder's context, and the adaptive
scheduler, which deserializes a new coordinator provider and source object at every restart or
rescale, reaches the same operator group through `JobManagerJobMetricGroup.getOrAddOperator`. Each
source operator has its own holder and group, keyed by operator id and name. The group is closed
only with the job's metric group, which `JobMaster.stopScheduling` closes when the JobMaster stops:
at the job's end, or when it loses leadership. Nothing closes it on a reset, a restart or a rescale.
A JobMaster that regains leadership in the same process registers a new job group. This was read
from `flink-runtime` 1.20.4, 2.2.1 and 2.3.0, which agree, and measured on 1.20.4 and 2.2.1 by the
end-to-end tests below.

`AbstractMetricGroup.addMetric` refuses a second registration of a name with a `Name collision`
warning and keeps the first, while `MetricGroup.counter(name, counter)` still answers the caller's
counter. So a later enumerator counted into counters no reporter saw, and its gauges were never
read; the reporter kept the first enumerator's frozen values. Measured on 2026-10-27 in MiniCluster
Bigtable scan jobs against the emulator:

- Default scheduler, one global failover forced by an expired checkpoint before any completed:
  `rowKeySamplesTaken` reported 1 while the client's `rpcMethod.SampleRowKeys.operations`, which
  [ADR-0186] then shared through the source object, reported both enumerators' 2.
- Adaptive scheduler, one task failure: a `Name collision` for each of the ten
  `rpcMethod.SampleRowKeys` leaves, for `splitsAssigned`, `splitsReturned`, `rowKeySamplesTaken`,
  and for Flink's standard `unassignedSplits`. The default scheduler answers the same task failure without
  a coordinator reset.

Every enumerator was affected: the five `PullAssignmentSplitEnumerator` subclasses (Bigtable scan,
Spanner batch, BigQuery read, Firestore and Datastore batch), the Bigtable and Spanner Change Streams
enumerators, the Pub/Sub enumerator's gauges, and Flink's standard `unassignedSplits` gauge, which
each of them sets. No Flink issue reports the collision; a search of the FLINK project for the
warning text and for `unassignedSplits` found none.

## Decision

**An enumerator takes its metrics from `base.source.EnumeratorMetrics`, which registers each name
once per enumerator metric group and hands every later enumerator on that group the same
registration.** A view is opened per enumerator with `EnumeratorMetrics.of(context.metricGroup())`
and closed with the enumerator.

- **Counters continue across resets.** `counter(name)` answers the same `ThreadSafeSimpleCounter`
  to every enumerator on the group, so a counter totals the job for as long as one JobMaster leads
  it. The owner
  chose this over restarting from zero (2026-10-28), for the reason the Bigtable client metrics
  already gave in [ADR-0186]: a counter that keeps counting is what a rate over it expects, and it
  matches the JobManager's job-lifetime metrics. A JobManager failover or a loss of leadership stops
  the JobMaster and closes the job's metric groups; the next JobMaster, in this process or another,
  registers new groups and new counters.
- **Gauges follow the enumerator now running.** `gauge(name, gauge)` and `unassignedSplitsGauge`
  register a gauge that reads whichever enumerator bound it last. Closing a view freezes the gauges
  it still feeds at their last value, so the registration holds no closed enumerator; a gauge a
  later enumerator has bound already is left alone, since the old enumerator may close after the new
  one starts. A gauge that throws on that last read, as one iterating coordinator state mid-change
  can, freezes at `null`, so closing never fails over a metric. An enumerator closed before its
  queued `start()` ran (Flink starts it on the coordinator thread and closes it on another) releases
  what `start()` then binds, because `close()` sets the enumerator's closed flag first.
- **A shared value is registered once.** `EnumeratorMetrics.shared(group, key, registration)` runs
  the registration the first time and answers its result after that. The Bigtable and Spanner
  sources take their enumerators' client metrics this way, through their adapters'
  `registerForEnumerators`, which replaces the source-object holders `EnumeratorClientMetrics` and
  `SpannerEnumeratorClientMetrics`. The client metrics are one shared value and each RPC method's
  series another, because a key names what is registered: the sub-sources of Flink's `HybridSource`
  run their enumerators on the hybrid's one group (`HybridSourceSplitEnumerator`'s context proxy
  answers the real context's `metricGroup()`), so a Bigtable scan followed by a change stream shares
  the client metrics and still registers both sources' methods. Keyed by the first caller alone,
  the second source's methods would have no series and its recordings would be dropped.
- **The group's identity is found through a subgroup.** Neither the context nor the source object
  carries the group across a reset, and the context answers a new wrapper each call. But
  `addGroup("sharedRegistrations")` answers the same open subgroup for as long as the enumerator
  group is open, and a new one once Flink has closed it. The registrations are kept in a
  process-wide `WeakHashMap` keyed by that subgroup, so an entry lasts as long as its job's metric
  groups. The subgroup never holds a metric, and a reporter is offered metrics, not groups, so it
  shows up nowhere.

A context that offers no group gets `EnumeratorMetrics.unregistered()`: counters nothing reports,
no gauges and no shared values, as the enumerators' own null checks did before.

## Alternatives declined

- **One registration per source object** ([ADR-0186]'s `EnumeratorClientMetrics` and
  `SpannerEnumeratorClientMetrics`). It holds under
  the default scheduler only. The adaptive scheduler builds a new source object at each restart, so
  the series froze after the first restart there.
- **A string key from the group's scope and variables** (`<job_id>`, `<operator_id>`). It needs no
  extra subgroup, but it outlives the group: a job resubmitted under the same id into a JobManager
  whose connector jar sits in `lib/`, as the SQL connectors are deployed, would find the closed
  group's registrations and register nothing on the new group. So would a JobMaster that loses and
  regains leadership in one process even for a job jar, since the job's classloader is kept. The
  identity of an open subgroup ends when Flink closes it.
- **Reading the wrapped group out of `ProxyMetricGroup`.** Its `parentMetricGroup` field is a
  `flink-runtime` implementation detail reached by reflection. `addGroup` is public API and leans on
  the same rule of `AbstractMetricGroup` that causes the collision in the first place.
- **A subgroup per enumerator** (an attempt number). Every restart would add a series and none would
  ever go, since Flink cannot unregister a metric, and a dashboard would have to know which attempt
  is current.
- **Counters that start again from zero at each reset.** Reachable with the same registry by
  re-pointing a delegating counter, but the owner chose continuation. Its one advantage, that the
  reader's counters also start again on a task restart, does not carry over: the reader's group is
  new per task attempt, while the enumerator's is not.
- **Waiting for Flink.** Closing the enumerator group on a coordinator reset would fix every source,
  the standard `unassignedSplits` included, and this registry stays correct if Flink does so: a
  closed group yields a new identity subgroup and a fresh registration. But no report exists and the
  supported range runs from 1.20 to 2.3 unchanged, so the connectors cannot wait for it.

## Consequences

- Planning counters change meaning. `rowKeySamplesTaken`, `readsPlanned`, `readSessionsCreated`
  and `queryJobsSubmitted` used to read their first enumerator's value forever. The first three now
  rise each time an enumerator's planning call completes, after the plan is installed, so a restart
  before the plan is checkpointed raises them legitimately. A checkpoint records only the current
  enumerator's state and planning is asynchronous, so neither the first completed checkpoint nor
  one started after a rise need hold the plan: a restart can bring a new enumerator that has not
  planned yet. One started after a rise, with no restart before it completes, does. The connector
  pages therefore tell an operator to alert on a rise after such a checkpoint, and on any rise in a
  run started from a savepoint or checkpoint that recorded the plan. `queryJobsSubmitted` rises
  inside the planning call, when the query's result returns and before the read session exists, so
  the rule does not hold for it alone; the BigQuery page reads it only beside a `readSessionsCreated`
  rise. Inside a `HybridSource` each sub-source that plans adds its own rise.
- The registry is process-wide state in `base`, scoped by the classloader that loads it: per job for
  a job jar, per JobManager for a jar in `lib/`. Each SQL connector jar relocates `base`, so each
  carries its own map. Two sources meet in one entry only inside a `HybridSource`, where they share
  counters and gauges by name as consecutive enumerators do, provided both load the same copy of
  `base`. Sub-sources from two relocated SQL jars run two registries on one group and collide as
  they did before; `HybridSource` is a DataStream API, which the SQL jars do not target.
- A finished job's entry leaves the `WeakHashMap` once its metric groups are collected and the map
  is next used, so the last finished job's counters can stay reachable until another enumerator
  registers.
- [ADR-0186]'s rule that an enumerator's client metrics belong to the source object is replaced by
  this decision, recorded in that ADR's 2026-10-28 refinement.
- A component that registers on the enumerator group without `EnumeratorMetrics` freezes again
  after the first reset. Two end-to-end tests hold the rule, one per scheduler, and both fail with
  the registry's sharing disabled (2026-10-28): `BigtableEnumeratorMetricsRestartITCase` restarts a
  scan job under the adaptive scheduler, and `SpannerSourceGlobalFailoverITCase` drives a global
  restore under the default scheduler and finds no `Name collision` on the enumerator group.
  `EnumeratorMetricsTest` and `PullAssignmentSplitEnumeratorTest` hold the shared group directly.

[#1908]: https://github.com/flink-gcp/flink-connector-gcp/issues/1908
[#1914]: https://github.com/flink-gcp/flink-connector-gcp/issues/1914
[ADR-0186]: 0186-client-library-metrics-reach-flink-reporters-through-a-narrow-opentelemetry-api.md
