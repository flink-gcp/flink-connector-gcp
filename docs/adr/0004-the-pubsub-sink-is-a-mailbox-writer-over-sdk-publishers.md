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

# ADR-0004: The Pub/Sub sink is a mailbox writer over SDK publishers, with writer-owned caps

- Status: Accepted
- Date: 2026-07-20 (sink [#18]); revised by [#78] (ordering×repair, 2026-07-25) and [#85]
  (in-flight bounds, 2026-07-26)
- Issues: [#18], [#19], [#20], [#85], [#78], [#21]
- Modules: pubsub
- Current behavior: `docs/content/docs/connectors/datastream/pubsub.md`

## Context

The sink needed a delivery model over the `google-cloud-pubsub` `Publisher`, a tuning surface,
memory bounds, and a recovery story for missing topics and paused ordering keys.

## Decision

- **Publisher-based flush-on-checkpoint stateless writer** ([#18]): FLIP-171 `AsyncSinkBase` was
  evaluated and rejected — the SDK `Publisher` already batches, and AsyncSink persists buffers
  into writer state. Mailbox-based backpressure with in-flight caps; writer-owned per-topic
  publishers (no JVM-wide cache); publish failures are capture-and-rethrow (the Apache
  connector's infinite republish is deliberately not adopted).
- **Topic auto-creation is reactive** ([#19]): `NOT_FOUND` publishes are parked and republished
  after creating the topic via the `TopicAdmin` SPI (`ALREADY_EXISTS` = success), gated by
  `CreateDisposition`.
- **Tuning lives in one `PubSubPublisherOptions` object** ([#20]): nested-options pattern, plain
  serializable values, no gax types on the public API, unset = SDK/sink default — batching,
  publish retries, `enableMessageOrdering`, the in-flight caps and the recovery backoff.
- **The writer owns both in-flight caps** ([#85], revising [#20]): `maxInFlightMessages` (1000)
  and `maxInFlightBytes` (64 MiB per subtask), and the two SDK `flowControl*` knobs [#20] exposed
  are **removed**, not deprecated. gax flow control could never be the byte bound an ordered sink
  needs: SDK 1.152.0 leaks a permit per publish cancelled on a paused key (so the builder
  rejected combining it with ordering — exactly where cascades pile up), and it blocks the task
  thread instead of yielding to the mailbox. Message count alone bounds no memory, since Pub/Sub
  allows 10 MiB per message.
- **Per-key order is restored by sorting the parked batch on a publish sequence, never by
  observation order** ([#78], revising the first design): the "mailbox FIFO preserves per-key
  order" premise is false, because the SDK cancels an ordering key's queued publishes from its
  own thread, so a cascade can be observed before the failure that caused it. Anything derived
  from observation order is a race — including deciding whether to park a cascade by whether
  something is parked already, which was the [#78] flake and was *also* the only thing hiding a
  silent ordering violation, since the parked list was appended in observation order too.
- **Emulator support is a builder option** ([#21]): `emulatorEndpoint(host:port)` — plaintext +
  no credentials for publishers (each owning its channel) and the auto-creation admin, mirroring
  the Apache connector's `withHostAndPortForEmulator`; the emulator ITs (including a MiniCluster
  streaming test through the public builder) reuse the production factory/admin, no test-only
  factory.

## Consequences — constraints not to re-litigate

- The three drains (`InFlightTracker.drainToEmpty()` since [#755], named apart from
  `awaitCapacity()` for exactly this
  reason) must keep meaning "empty, and `checkAsyncError`" — [#78]/[#110] made that
  load-bearing.
- Admission is "below the cap", never "does this message fit": `yield()` blocks until a mail
  arrives and no mail can arrive at zero in flight, so a fits-predicate would hang the task on an
  oversized message instead of backpressuring it.
- A repair republishes its parked batch **exempt from both caps**, because yielding between a
  key's republishes reorders it. Parked messages are counted by neither cap (their failure mail
  released them).
- A cancellation is never a root cause, so one is parked unconditionally and a fatal root is
  caught by the pre-repair drain (the drain → the writer's `checkAsyncError`) rather than by
  classifying the cascade. The disposition no longer gates parking — [#215] (ADR-0006) moved
  that guarantee onto `topicMissing`.
- The two writer test classes carry `@Timeout(30)`: the fake mailbox blocks like the real one,
  so a broken predicate hangs rather than fails.

[#18]: https://github.com/laughingman7743/flink-connector-gcp/issues/18
[#19]: https://github.com/laughingman7743/flink-connector-gcp/issues/19
[#20]: https://github.com/laughingman7743/flink-connector-gcp/issues/20
[#21]: https://github.com/laughingman7743/flink-connector-gcp/issues/21
[#78]: https://github.com/laughingman7743/flink-connector-gcp/issues/78
[#85]: https://github.com/laughingman7743/flink-connector-gcp/issues/85
[#110]: https://github.com/laughingman7743/flink-connector-gcp/issues/110
[#215]: https://github.com/laughingman7743/flink-connector-gcp/issues/215
[#755]: https://github.com/flink-gcp/flink-connector-gcp/issues/755
