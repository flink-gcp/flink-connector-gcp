<!--
Copyright 2026 laughingman7743

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

# ADR-0043: Bigtable metrics are the failure-series standard, reached late

- Status: Accepted
- Date: 2026-08-03 ([#237], absorbing [#234])
- Issues: [#237], [#234], [#239]
- Modules: bigtable
- Current behavior: `docs/content/docs/connectors/datastream/bigtable.md` § Metrics

## Decision

[#33] shipped `numRecordsSend`/`numBytesSend` ahead of the [#37] series; the rest arrived with
[#237], which also absorbed [#234] — its "counter for *dropped* elements" is
`numRecordsSendErrors`, the series having settled on one counter for everything routed rather
than a second one for what a handler then discarded. Bytes come from
`entry.toProto().getSerializedSize()`, counted at admission (the Kafka connector's placement).

Five decisions:

- **No per-destination counters, and so no `perDestinationMetrics` option.** One fixed table per
  sink means `destination.TABLE.*` restates the writer's totals — the same reason [#210] leaves
  them off the buffered-stream path. A batcher pool (ADR-0041's one-table decision) is what
  would change this.
- **`errorClass` counts RPC failures only, never the serializer's.** A serialization failure
  carries no status, so counting it would put every one under `UNCLASSIFIED` beside the RPC
  failures that genuinely carry none. Both sibling sinks draw the line in the same place.
- **`BigtableErrorClassifier.statusCode` reports the chain's outermost classifiable status, not
  the code `classify` acted on**, mirroring `PubSubErrorClassifier.statusCode`. The two diverge
  exactly when a transient status is buried under a data-shaped one: routing scans the whole
  chain and calls it fatal, while the counter names what the failure was reported with. A real
  divergence, pinned by
  `namesTheOutermostStatusOfAChainItTreatsAsFatalForABuriedTransientOne` — added because the
  mutant reporting the routing decision instead **survived** the first test set.
- **`close()` zeroes the two gauge-backing counters, and does it *before* `Closers.closeAll`.**
  A reporter can sample between `close()` and the metric group's own close, and nothing
  decrements them afterwards (completion mails no longer run), so a writer torn down mid-flight
  would keep reporting mutations it will never wait for. The *placement* is this module's own:
  either close below can still throw, and a mid-flight teardown is when both this clear and
  those failures happen, so a clear after `Closers.closeAll` would be skipped in exactly the
  case it exists for. Found in review round 2, having been missed by a round 1 that looked only
  at increment sites — **when a series brings the same shape to another connector, diff the
  `close()` paths too.**
- **Every failure reaching the writer is counted, fatal ones included and fatal ones after the
  first.** Only the first becomes `asyncError`, but each is a mutation the client gave up on.
  The consequence, stated on the docs page: since the retries are the client's, the sum over the
  transient codes is *not* this connector's retry volume — the client's own attempts are
  invisible here. **The one exclusion is a batched row-level rejection** ([#239]): the client
  reports one request-level status against every co-batched entry, so counting them all would
  multiply a single incident by the batch size. The isolation pass counts what it confirms,
  which makes both counters report rejected *records* — the Pub/Sub cascade-exclusion argument
  arriving by the same route — and `parkedMutations` is the gauge that took the excluded
  reports' place: a parked mutation has left the in-flight counters and has not reached the
  handler, so between the two nothing else reports it at all.

[#33]: https://github.com/laughingman7743/flink-connector-gcp/issues/33
[#37]: https://github.com/laughingman7743/flink-connector-gcp/issues/37
[#210]: https://github.com/laughingman7743/flink-connector-gcp/issues/210
[#234]: https://github.com/laughingman7743/flink-connector-gcp/issues/234
[#237]: https://github.com/laughingman7743/flink-connector-gcp/issues/237
[#239]: https://github.com/laughingman7743/flink-connector-gcp/issues/239
