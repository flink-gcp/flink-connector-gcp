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

# ADR-0038: Metric names are per-connector inventories, in one shape, without Flink's `num` prefix

- Status: Accepted
- Date: 2026-08-05
- Issues: [#280] (the mechanical half is `just check-metric-docs`, [#296])
- Modules: base rule; every connector's `<Product>MetricNames`
- Current behavior: the datastream pages' metrics tables

## Decision

- **Every connector declares its metric names in one `<Product>MetricNames` class at its module
  root**, and that file is the connector's inventory. A connector's metric names stay **inside
  that connector** — a shared holder for names several connectors happen to share was built
  first and **withdrawn**, because it split each connector's inventory across two modules to
  close one narrow drift. Cross-connector consistency is checked by **diffing those files**,
  which is the whole mechanism. The registering classes take every name from it — a `*Metrics`
  class declaring its own constant puts the inventory back in two places. What the class does
  not hold: Flink's standard names (metric-group accessors), and the subgroup leaves
  `base.metrics` registers on a connector's behalf.
- **A metric this repository registers itself is a lowerCamelCase noun phrase, and its shape
  says which kind it is**: a **counter** names the *event* it counts
  (`<plural noun><past participle>` — `tablesCreated`, `messagesAcked`, `recordsSkipped`; a
  count with no actor to name is a plain noun phrase — `appendRetries`, `errors`); a **gauge**
  names the *state* it reports (`<adjective or participle><plural noun>` — `openDestinations`,
  `inFlightEntries`, `pendingAcks`). Read it as a test, not decoration: a name in the wrong
  shape reports the wrong kind of quantity to whoever reads the dashboard. [#280] found exactly
  two and renamed both (`stagedFiles`, `checkpointsPendingAck`), so a later addition that does
  not fit is a review finding rather than a precedent. **Flink prescribes nothing here**
  (FLIP-33 standardizes a list of names and leaves a connector's own names alone), but Flink's
  own connectors have the same shape — checked against Kafka, Kinesis and HBase before this was
  written. The one deliberate departure is `DestinationMetrics`' subgroup leaves
  (`recordsSend`/`sendErrors`): each is Flink's standard name with the `num` prefix dropped, so
  `destination.X.recordsSend` reads against `numRecordsSend`, which is the quantity it
  partitions.
- **A metric this repository registers itself never takes Flink's `num` prefix.** `num…` is
  Flink's own vocabulary — `MetricNames` spells 22 such names, and `SinkWriterMetricGroup`
  exposes four, of which every writer here takes three — so a custom counter inside it costs two
  things: a reader cannot tell it from a Flink-defined one, and it can be silently dropped —
  `AbstractMetricGroup.addMetric` resolves a name collision by keeping the metric **already**
  registered (logging only), and `InternalSinkWriterMetricGroup` registers all four in its
  **constructor**, so ours is always the later one and so the one dropped. That second cost aims
  the rule at the sink writer group in particular; `pendingAcks`/`pendingCheckpoints` are **not**
  counter-examples — those Flink names are registered only via
  `setPendingRecordsGauge`/`setPendingBytesGauge`, which this source never calls.
- The one counter that had taken the prefix was `recordsSkipped` (`numRecordsSkipped` until
  [#280]), and the argument offered for keeping it is recorded as **measured false** so it is
  not re-argued: `numRecordsSend`, `numRecordsSendErrors` and the skip counter do *not*
  partition every record the writer is handed, in five of the six writers — `numRecordsSend`
  counts records handed to the client, so a record the service then rejects is counted by it and
  by `numRecordsSendErrors` both. `FileLoadsWriter` alone partitions, and only because it makes
  no per-record request.

[#280]: https://github.com/laughingman7743/flink-connector-gcp/issues/280
[#296]: https://github.com/laughingman7743/flink-connector-gcp/issues/296
