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

# ADR-0005: Exactly two Pub/Sub sink failures are routed to the failure handler

- Status: Accepted
- Date: 2026-08-02
- Issues: [#206] (the [#37] series)
- Modules: pubsub
- Current behavior: `docs/content/docs/connectors/datastream/pubsub.md` § Failed-message policy

## Context

[#37]'s shared `FailureHandler<FailedMessage>` SPI reached the Pub/Sub sink via [#206]:
`failedMessageHandler(...)` defaults to `failJob()` — behaviourally the pre-existing
capture-and-rethrow, which is why `PubSubWriterTest` and `PubSubWriterAutoCreationTest` were left
untouched and are the regression guard.

## Decision

**Exactly two failures are routed, and the boundary is the decision**: a record the serializer
rejects, and a publish rejected `INVALID_ARGUMENT`. `PubSubErrorClassifier` (`sink.writer`)
absorbs the writer's `isNotFound`/`isCancellation` and fixes the precedence
`TOPIC_NOT_FOUND` → `CANCELLATION` → `MESSAGE_LEVEL` → `FATAL`, each walking the cause chain;
the order is pinned by test, because a chain can carry both a cancellation and a status.

Not routed, in two directions and for two different reasons:

- **Outage-shaped failures** (an unavailable service, an exhausted SDK retry budget) must never
  reach a dropping handler, or an incident bleeds the stream one message at a time rather than
  backpressuring.
- **Configuration-shaped failures** (a `DestinationResolver` returning null, an ordering key
  without `enableMessageOrdering`) fail *every* record alike, so dropping them would leave an
  empty topic under a green job — the same trap eager schema derivation closes on the BigQuery
  side.

A record the serializer *skips* by returning null is in neither class (ADR-0001): it is not a
failure, no publisher is even opened for it — the check sits ahead of `stateFor(...)` — and
`recordsSkipped` is the only thing that reports it.

Type decisions: `FailedMessage` sits at the `sink` root (a one-class `sink.failure` fails the
[#119] layer test) and carries the **whole serialized `PubsubMessage`** as `getPayloadBytes()`,
not just its data, so a dead-letter consumer recovers attributes and ordering key with
`parseFrom`; `describeDestination()` is the `projects/p/topics/t` resource name the
`FailedElement` javadoc prescribes, not `TopicDestination.toString()`'s `project/topic`. A
`MESSAGE_LEVEL` handler failure is captured into `asyncError` rather than thrown, because it
happens inside a mailbox mail; an unchecked one is wrapped naming the topic.

## Consequences

The configuration-shaped argument does not reach as far as it sounds, and the docs say so: a
serializer producing an *invalid message* for every record is rejected per message, so it is
routed and droppable exactly like a genuine one-off — the classification reads a response status
code and cannot tell a systematic rejection from a per-message one. The answer is the [#208]
error metrics, and BigQuery carries the identical exposure.

Coverage is unit tests only: the emulator validates nothing here, and what real Pub/Sub answers
`INVALID_ARGUMENT` to had to be measured before a gated IT could assert it — which [#303]
(ADR-0008) later did.

[#37]: https://github.com/flink-gcp/flink-connector-gcp/issues/37
[#119]: https://github.com/flink-gcp/flink-connector-gcp/issues/119
[#206]: https://github.com/flink-gcp/flink-connector-gcp/issues/206
[#208]: https://github.com/flink-gcp/flink-connector-gcp/issues/208
[#303]: https://github.com/flink-gcp/flink-connector-gcp/issues/303
