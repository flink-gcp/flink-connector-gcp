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

# ADR-0036: `base.failure` is the shared, user-implemented failure SPI — the module's one public package

- Status: Accepted
- Date: 2026-08-01 ([#205], first consumer BigQuery; the [#37] design)
- Issues: [#37], [#205], [#61]
- Modules: base (consumed by every connector)
- Current behavior: each connector's docs page § Error handling / Dead-lettering

## Decision

The base module is **main-code shared infrastructure only** (test-support code stays in
test-utils, whose record states the mirror-image rule), everything `@Internal` **except
`base.failure`**. The exception is structural, not preferential: `base.failure` is a
user-*implemented* SPI, an interface users implement cannot be internal, and keeping it
per-connector would mean three copies and no cross-connector `DeadLetterQueue` — the point of
[#37]. A second public package needs the same kind of argument. A type only moves into base once
it has multiple consumers (the same bar test-utils applies).

The SPI: `FailedElement` (read-only contract — connector id, destination string, nullable
payload bytes, message, cause), `FailureHandler<F extends FailedElement>` (`handle` =
drop-or-throw; built-ins `failJob()` — the default everywhere — `logAndDrop()`,
`sendToDeadLetterQueue`), `DeadLetterQueue` (`@Experimental`), and the `@Internal`
`DefaultFailureHandlerContext` the sinks build from their `WriterInitContext`.

Decisions not to re-litigate:

- **`flush()` runs from each writer's `flush(boolean)` after the write-path drain** — failures
  are discovered by the drain, so flushing first would checkpoint past unflushed dead letters.
- The guarantee is **at-least-once for failures that recur on replay**; exactly-once is
  deliberately not offered (it would require the dead-letter write to join the sink's own commit
  protocol).
- `open()` carries subtask index and metric group and nothing more — grow it only when a real
  consumer demands it.
- The generic parameter keeps `failedRowHandler(...)`-style setters typed per connector while
  `FailedElement` lets one `DeadLetterQueue` implementation serve every connector; the
  built-ins' unchecked casts are safe because handlers only consume elements, and the connector
  builders' setters take `FailureHandler<? super X>` so a cross-connector
  `FailureHandler<FailedElement>` is accepted without a cast.
- `getConnector()` values are lower-case module words (`bigquery`, `pubsub`, `cloudtasks`) and
  are API — dead-letter consumers key on them. `describeDestination()` is not `getDestination()`
  because a connector's concrete type keeps a typed `getDestination()`, and a same-signature
  `String` override would be an irreconcilable clash.
- **Which failures are row-level stays per-connector** (only data-shaped failures reach a
  handler), exactly as retryability classification stays per-connector under [#61]; the Pub/Sub
  adoption is [#206] (ADR-0005), Cloud Tasks' [#207].
- `protobuf-java` (BOM-managed) is here for `ByteString` on `FailedElement`.

[#37]: https://github.com/laughingman7743/flink-connector-gcp/issues/37
[#61]: https://github.com/laughingman7743/flink-connector-gcp/issues/61
[#205]: https://github.com/laughingman7743/flink-connector-gcp/issues/205
[#206]: https://github.com/laughingman7743/flink-connector-gcp/issues/206
[#207]: https://github.com/laughingman7743/flink-connector-gcp/issues/207
