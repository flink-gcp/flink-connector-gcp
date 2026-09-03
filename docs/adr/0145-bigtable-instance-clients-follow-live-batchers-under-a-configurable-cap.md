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

# ADR-0145: Bigtable instance clients follow live batchers under a configurable cap

- Status: Accepted
- Date: 2026-08-29
- Issues: [#1133]
- Modules: bigtable (`sink`, `sink.writer`, `table.sink`)
- Current behavior: `docs/content/docs/connectors/datastream/bigtable.md` § Per-record destinations

## Context

ADR-0074 made the writer own one batcher per table over one shared client per `(project, instance)`.
It retained every instance client until writer close and explicitly declined reference-counted
release because the active instance set was assumed to be one in ordinary use and no measured gain
justified another lifecycle path.

Issue #1133 measured the opposite workload with `google-cloud-bigtable` 2.81.0.
An isolated child JVM retained about 5.0 MiB and 201 live threads after creating 100 historical
instance clients, while 1,000 table batchers sharing one client plateaued at 11 threads and retained
about 4.7 MiB.
Table eviction therefore bounded the batcher set but not the heavier client, channel-pool, and
executor set when a resolver rotated through instances.

This ADR supersedes [ADR-0074] because it reverses that record's client-lifetime decision and adopts
the reference-counted release [ADR-0074] declined.
Every other [ADR-0074] decision remains accepted and is incorporated here by reference, including the
per-record resolver, client sharing, writer-global in-flight budgets, two-phase batcher teardown,
repair and isolation ordering, and per-destination metric policy.

## Decision

A shared instance client's lifetime follows its live table batchers.
The production factory reference-counts successful batcher creations per `(project, instance)`.
After the writer safely closes an evicted table's batcher, it releases that ownership; the last
release removes and closes the client, while any sibling table keeps it alive.
A later write to the instance creates a fresh client and batcher lazily.

Logical removal precedes physical close.
The writer attempts every affected batcher shutdown, batcher close, and factory release, and logs a
hygiene close failure rather than failing an otherwise successful checkpoint.
The factory normally runs client close on bounded daemon reapers because the production SDK's
default built-in OpenTelemetry exporter can wait up to ten seconds during close.
The successful-checkpoint idle sweep therefore normally starts physical client close without
putting that wait on the task thread.
If the runtime refuses to schedule a reaper task, the factory closes that client synchronously
before reporting the scheduling failure, preferring a bounded exceptional wait to a resource leak.
An actual thread interruption still propagates as cancellation instead of being swallowed as
hygiene.

One writer subtask holds at most `maxActiveInstances` physical client slots, 16 by default.
An open client keeps its permit while it is closing, so a burst cannot move the leak into an
unbounded reaper queue.
The positive DataStream builder knob maps to the `sink.max-active-instances` Table API option.
That option is inert because one DDL sink names one instance, but it preserves the table layer's
one-key-per-writer-knob invariant instead of creating an exemption that can silently go stale.
When a new instance would exceed the cap, the writer sends and drains every outstanding mutation,
removes the least recently used instance and all its table batchers, and then releases them.
If every permit is still open or closing, creation waits interruptibly for a physical close to
finish; that is limit-pressure backpressure, outside a checkpoint's idle sweep.
Writing any table in an existing instance refreshes the shared instance's recency and consumes no
new slot.
A workload rotating through more instances than its configured cap deliberately pays a global drain
and client recreation at limit pressure; the user can raise the cap when that throughput cost is
preferable to the default resource bound.

The existing `destinationIdleTimeout` remains table-scoped.
Its successful-checkpoint sweep closes a client only when it removes the instance's last live table,
so there is no separate client timeout.
`activeClients` reports logically active instance slots and therefore drops before asynchronous
physical close finishes, while `capacityEvictions` and `idleEvictions` count the two logical
instance-removal causes.

## Evidence

The production-path child-JVM regression constructs and closes actual SDK clients against a
no-listener emulator endpoint without issuing RPCs.
It warms the SDK, exercises 512 historical instances, waits up to a bounded deadline for asynchronous
client shutdown to converge, and requires the factory to retain zero clients with bounded thread and
heap growth.

Writer tests hold the lifecycle ordering and state transitions with fakes: sibling preservation,
last-table release, recreation, instance-scoped LRU refresh, global drain before capacity eviction,
idle eviction only after a successful checkpoint flush, no idle sweep at end of input, direct and
suppressed interruption handling, exhaustive non-interruption close-failure handling, and the
metrics.
Factory tests drive real reference counts, verify that closing one table cannot close the client
still serving its sibling, and pin that a reaper returns immediately while its permit remains held
until physical close finishes.

## Alternatives declined

**Retain clients until writer close.**
The measured historical-instance cost is unbounded and is the resource leak this decision removes.

**Let each table batcher close its client.**
The client is shared, exposes no closed state, and would make the first evicted table invisibly break
all sibling batchers.

**Add a separate instance idle timeout.**
The table timeout already says when an instance has no live batchers.
A second clock would let the two lifecycle policies disagree without expressing another measured
workload.

**Fail when capacity is reached.**
A rotating destination set is expected to revisit instances.
Safe drain and LRU eviction preserve delivery and bound resources without converting that workload
into a configuration failure.

**Leave the cap unbounded or infer it from the in-flight limits.**
In-flight entries and bytes bound mutation data, not SDK channel pools and executors, and therefore
cannot bound the resource measured by #1133.

## Refinement (2026-09-03): the reaper is shared by two client families

ADR-0148 ([#1178]) moved the bounded close pool from the batcher factory to the module root as
`BigtableClientReaper`, with its contract unchanged: a permit per open client, held while closing,
daemon reapers bounded by `maxActiveInstances`, synchronous close when scheduling is refused. The
single-row request family's client factory takes its permits from the same type, so the cap this
record describes bounds that family's clients in the same way.

## Consequences

Ordinary fixed-instance and many-table workloads retain their shared client without capacity churn.
Dynamic-instance workloads have a finite per-subtask resource envelope and can trade memory and
threads for less churn by increasing one explicit knob.
Capacity eviction is a delivery barrier because every live batcher must be drained before a client
can be closed safely, and client creation can then wait for the physical-close permit when the
configured cap is fully occupied.
Close failures can make physical SDK shutdown lag the logical gauge, so the metrics deliberately
describe tracked slots and removal pressure rather than operating-system resources.

[#1133]: https://github.com/flink-gcp/flink-connector-gcp/issues/1133
[ADR-0074]: 0074-the-bigtable-writer-pools-a-batcher-per-table-over-a-client-per-instance.md
[#1178]: https://github.com/flink-gcp/flink-connector-gcp/issues/1178
