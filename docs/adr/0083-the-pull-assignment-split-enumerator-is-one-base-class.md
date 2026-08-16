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

# ADR-0083: The pull-assignment split enumerator is one base class, parameterised by the planning step

- Status: Accepted
- Date: 2026-08-10 ([#452])
- Issues: [#452], [#390], [#216]
- Modules: base, bigquery, bigtable
- Current behavior: unchanged — no user-visible behaviour, option or metric name moves with this

## Context

The BigQuery source ([#390]) and the Bigtable scan source ([#216]) each wrote the same bounded
pull-assignment enumerator: a queue of unassigned splits, a set of subtasks parked before planning
finished, a `volatile closed` flag, no per-subtask ledger, three counters plus Flink's
`setUnassignedSplitsGauge` behind the same defensive null check, and one asynchronous one-shot
planning step through `context.callAsync` whose completion handler returns quietly when closed.

**What the two files shared was the protocol, not the text**, and the distinction is what shaped
this decision. Measured on them before the change: only `addReader` matched verbatim modulo the
split type. `serve`, `addSplitsBack` and `close` all differed — by log wording, by the counter that
counts the planning call (`readSessionsCreated` against `rowKeySamplesTaken`), and by the field the
parked subtasks sat in. So a textual merge was never available; what could be shared was the shape.

That shape is where a hand-written enumerator loses a split without anything reporting it, which is
why `BigQueryReadSplitEnumerator`'s javadoc cites the reference implementation's recorded "critical
data loss bug in reader split handling". Writing it a third time was the cost worth avoiding.

## Decision

- **`base.source.PullAssignmentSplitEnumerator<SplitT, StateT, PlanT>` is an abstract class, and
  each connector's enumerator stays its own named class extending it.** The base owns the queue,
  the parking, `addReader`/`handleSplitRequest`/`addSplitsBack`, the `serve` that skips an
  unregistered subtask and signals no-more-splits on an empty queue, the `callAsync` planning step
  with its closed-guard, and the close of the one seam the enumerator owns — all `final`. A
  connector supplies five hooks: `restore()`, `onPlanningStarted()`, `plan()`, `onPlanned(...)`,
  `registerCounters(...)`, plus its own `snapshotState`.
- **The checkpointed state type stays per connector**, which is why `StateT` is a type parameter
  and `snapshotState` is not implemented here: BigQuery's carries the session name and expiry,
  Bigtable's carries the planned flag alone, and both are serialized by their own
  `SimpleVersionedSerializer`.
- **Counter registration stays in the connector**, behind `registerCounters`, and this is the one
  place the shape was chosen against the obvious one. Passing three metric *names* into the base
  would read better at the call site, but `scripts/check-metric-docs.py` decides that a documented
  metric is registered by matching a literal `.counter(<Product>MetricNames.X)` in that module's
  sources; moving the call would have made every one of the six enumerator metrics report as
  undocumented-and-unregistered, to be repaired by teaching the checker a "shared registrar"
  mechanism and its synthetic tests. **Measured rather than reasoned** (2026-08-10): replacing the
  BigQuery hook's body with `EnumeratorCounters.unregistered()` — the exact shape a
  names-in-the-base version would leave behind — makes `just check-metric-docs` report six problems
  for that connector alone, three saying the inventory constant is registered nowhere and three
  saying the docs table names a metric the module does not register. The checker is not wrong about
  this — a name belongs to the connector that owns it — so the code moved to fit it.
  `EnumeratorCounters` is the value the hook answers with, and `EnumeratorCounters.unregistered()`
  is what the base holds when the context offers no metric group.
- **The base logs through `LoggerFactory.getLogger(getClass())`**, so every protocol line keeps
  being emitted under the concrete connector's class name and a log configuration scoped to one
  connector keeps matching. This is deliberately the opposite of `BoundedShutdown`, which logs
  under its own name so that one client's operator is not sent after another client's defect
  (ADR-0007): there, the shared class is the subject of the message; here, the connector is.
- **The protocol's log lines took generic wording** — one `splitNoun` per connector ("read stream
  split", "scan split"), and no product word, since the logger name carries it. The messages that
  name a product still do: the planning-start line, the plan report, the restore line and the
  failure messages are all the connector's own text, passed in or emitted from a hook.
- **The test suite split follows the code split.** `PullAssignmentSplitEnumeratorTest` in the base
  module owns the protocol invariants — parking order, an unregistered requester, no-more-splits on
  an empty queue, a returned split reassigned exactly once, a reader served after being told there
  are none left, both arms of the closed-guard, the `IOException` a refusing seam becomes, the
  counters and the gauge, and a checkpoint that is a copy. Each connector's suite keeps what would
  pass in the base module and still be wrong in the connector: a second session or a second
  sampling, a seam left open, a failure message that does not name the table, a metric under the
  wrong name. The base suite needs no third copy of the fake context because
  `FakeSplitEnumeratorContext<SplitT>` had moved to test-utils five hours earlier the same day
  ([#437], ADR-0050) — the timing was luck, the dependency is not: writing this suite against
  per-module fakes would have meant a third one, which is the shape ADR-0050 exists to refuse.

## Consequences

- The next pull-assigned bounded source implements a planning step and its state, not a protocol.
  **The honest next consumer is [#221]** — the Spanner batch source, whose scope already reads as
  this shape: "enumerator opens `BatchClient.batchReadOnlyTransaction(TimestampBound)` and calls
  `partitionQuery`", with "readers checkpoint remaining splits (Bigtable [#216] shape)". [#452]
  named the Bigtable change streams source ([#35]) instead, and that is the one place its reasoning
  does not hold: [#35]'s own scope is `generateInitialChangeStreamPartitions` plus Beam's
  "split/merge continuation tokens, partition reconciliation" held in enumerator state — continuous
  partition discovery, not a plan taken once. Nothing here forbids a continuous variant later; it
  would be a second base class or a widening argued on its own evidence.
- **A connector whose teardown is more than one seam composes it.** `close()` releases exactly one
  `AutoCloseable`; [#221]'s enumerator also "owns `cleanup()`", which reaches this class as one
  closeable wrapping both rather than as a second constructor parameter — the shape stays
  one-seam until a second consumer argues otherwise.
- The base module gains a `base.source` package and no dependency. The types come from what the
  module already declares: `flink-core` owns `SplitEnumerator` and `SplitsAssignment`, and
  `flink-metrics-core` — which `flink-core` brings — owns `SplitEnumeratorMetricGroup` and
  `ThreadSafeSimpleCounter` (verified against the resolved 2.2.1 jars). `ThreadSafeSimpleCounter`
  is the only unstable tier among them and its `scripts/config/flink-api-tiers.toml` entry already
  existed, so the main code needed no new one.
- A connector can no longer quietly opt out of the protocol: `start()`, `close()` and the three
  assignment methods are `final`, so the closed-guard and the no-ledger assignment cannot be
  overridden away in one module and left correct in the other.

## Evidence

- **The coordinator claim this class's javadoc rests on holds on both supported Flink majors.**
  ADR-0079 verified against flink-runtime 2.2.1 that `SourceCoordinator` suppresses a split request
  from a subtask already told there are no more splits and clears that only on `subtaskReset`. The
  text now lives in shared code that also compiles against the 1.x LTS, so it was checked there too
  (flink-runtime **1.20.3** sources, read 2026-08-10): `handleRequestSplitEvent` calls the
  enumerator only `if (!context.hasNoMoreSplits(subtask))`, `signalNoMoreSplits` sets
  `subtaskHasNoMoreSplits[subtask] = true`, and `subtaskReset` sets it back to `false`. Identical
  on both, so no compat seam is owed.
- **The check-metric-docs measurement** behind the registration decision is in the Decision section
  above, with its date and what was changed to produce it.

## Alternatives declined

- **Composition — a `final` generic enumerator plus a planner SPI and a state factory** (the shape
  [#452] sketched). It moves each connector's enumerator out of existence: the design-record javadoc,
  the state assembly and the connector-specific logging would have become a factory method plus
  closures over mutable fields (BigQuery's session name and expiry are read at `snapshotState` time
  and written at planning time, so they must live somewhere the state factory can see). Declined
  with the user: the class identity, its javadoc and its state type are what a later reader looks
  for, and the template-method form keeps all three where they are.
- **Passing the three metric names to the base.** Declined for the checker reason above — the cost
  is a mechanism in `scripts/check-metric-docs.py` and its tests, bought for a slightly shorter
  subclass.
- **Sharing only a queue helper object**, leaving each connector its own `start`/`close`/`callAsync`.
  Declined because the asynchronous planning step and its closed-guard are precisely the part that
  fails quietly — a teardown turning a cancellation into a job failure is invisible until it
  happens in production — so a split that shared the queue and duplicated the guard would have
  shared the easy half.

[#35]: https://github.com/laughingman7743/flink-connector-gcp/issues/35
[#216]: https://github.com/laughingman7743/flink-connector-gcp/issues/216
[#221]: https://github.com/laughingman7743/flink-connector-gcp/issues/221
[#390]: https://github.com/laughingman7743/flink-connector-gcp/issues/390
[#437]: https://github.com/laughingman7743/flink-connector-gcp/issues/437
[#452]: https://github.com/laughingman7743/flink-connector-gcp/issues/452
