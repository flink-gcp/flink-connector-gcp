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

# ADR-0010: Pub/Sub sink metric placements

- Status: Accepted
- Date: 2026-08-03
- Issues: [#208] (the [#37] series; the repo-wide counting and naming rules are the base
  module's record)
- Modules: pubsub
- Current behavior: `docs/content/docs/connectors/datastream/pubsub.md` § Sink metrics

## Decision

`PubSubWriterMetrics` (`sink.writer`) on the `PubSubSourceReaderMetrics` model, but with
**plain counters, not `ThreadSafeSimpleCounter`** — every increment happens on the task thread
here, since completions arrive as mailbox mails, which is exactly what the source cannot say.

Four decisions worth keeping:

- **`numRecordsSend` is counted inside `publishTo`, guarded by its `firstAttempt` parameter** —
  not at the `write()` call site, and not unguarded. Two properties have to hold at once, and
  only that placement gives both: a repair re-enters `publishTo` for every parked message, so an
  unguarded increment would count publish *attempts* rather than records; and counting at the
  call site would count a record whose `publisher.publish(...)` threw synchronously, which
  registers no callback and reached the client not at all. So the counter sits beside
  `inFlightMessages++`, after the publish was accepted, under the flag.
- **`parkedMessages` is a plain `int` field** maintained by the sole `park(...)` helper rather
  than a sum over `states`, since the gauge is read from the reporter thread and walking those
  maps would race the task thread; `close()` zeroes it, because parked messages are dropped with
  the writer.
- **Error-class counters skip cascade cancellations**: under [#78] a cancellation always trails
  a root failure that is itself counted, and it carries no status, so counting it would both
  multiply one incident by the key's queue length and bury real unclassifiable failures under
  `UNCLASSIFIED`. The traversal that finds the code is `PubSubErrorClassifier.statusCode` —
  beside `classify`, since that class owns the connector's cause-chain policy.
- **Per-destination counters are resolved once per `DestinationState`**, not per record, so the
  topic's resource name (the same `toTopicPath()` `describeDestination()` uses) is composed
  once.

## Evidence

Measured, so it is not re-investigated: `google-cloud-pubsub` 1.152.0 exposes **no**
programmatic metric accessor on `Publisher` — only
`setEnableOpenTelemetryTracing`/`setOpenTelemetry`, and `OpenTelemetryPubsubTracer` emits spans,
not meters — so the flink-connector-kafka-style passthrough of client-native metrics has no
source to read here.

[#37]: https://github.com/flink-gcp/flink-connector-gcp/issues/37
[#78]: https://github.com/flink-gcp/flink-connector-gcp/issues/78
[#208]: https://github.com/flink-gcp/flink-connector-gcp/issues/208
