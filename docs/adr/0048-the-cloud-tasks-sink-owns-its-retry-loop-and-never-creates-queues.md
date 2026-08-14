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

# ADR-0048: The Cloud Tasks sink owns its retry loop and never creates queues

- Status: Accepted
- Date: 2026-07-25 ([#23] design, [#24] implementation via PR
  [#107](https://github.com/laughingman7743/flink-connector-gcp/pull/107)); revised 2026-08-12
  ([#545]), 2026-08-13 ([#608], [#628]) and 2026-08-14 ([#632])
- Issues: [#23], [#24], [#25], [#545], [#608], [#628], [#632]
- Modules: cloudtasks
- Current behavior: `docs/content/docs/connectors/datastream/cloudtasks.md`

## Decision

- Cloud Tasks is an HTTP dispatch queue whose **pacing lives on the queue**
  (`maxDispatchesPerSecond`, `maxConcurrentDispatches`, retry config), so the sink has no rate
  knobs and there is **no queue auto-creation** — an auto-created queue would carry default
  limits, discarding the throttling that is the reason to use the service, and a deleted queue
  name cannot be reused for 3 days.
- The serializer supports both request-target arms of the task `oneof`: external HTTP and App
  Engine. HTTP targets need a publicly routable endpoint and choose OIDC or OAuth from what they
  call, so the builder rejects setting both tokens. App Engine targets use a relative URI and
  optional service/version/instance routing within the queue's project; the queue location must
  correspond to the App Engine application's permanent region. Fixed **and** per-record queue
  destinations remain available — unlike Pub/Sub topics and BigQuery tables this costs nothing,
  since one `CloudTasksClient` serves every queue with no per-destination stream.
- **Unnamed tasks by default**; `taskIdExtractor(...)` **on the sink builder, not the
  serializer** (a `Task` has no id field — only the full `name` path, which needs the resolved
  queue) opts into deduplication (`ALREADY_EXISTS` = success), and the sink **hashes the
  extracted key with SHA-256**, because Google documents that sequential ids raise latency *and*
  error rates. The serializer never sets a name, so there is no second path around the hashing.
  The dedup window is **contradicted in Google's own sources — REST says up to 24 h, the v2
  proto says ~1 h — so design against 1 h**.
- **Retries are the sink's responsibility**: the generated client gives `CreateTask` an empty
  retryable-code set and a 20 s timeout (verified in `CloudTasksStubSettings` 2.94.0), as it
  does every mutating method. Retrying is **one sink-owned loop in the writer**, not gax
  `createTaskSettings` retry — gax has a single retryable-code set and schedule per method,
  which cannot express the separate short `NOT_FOUND` budget (a 30-day-idle queue re-activates
  slowly, but a mistyped queue must not burn the full budget per record), and a sink-owned loop
  is testable against a fake client. A failed create is **parked with a due time** and
  re-dispatched unchanged from the next `write()`/`flush()`; parked creates count against
  `maxInFlightTasks` (records the service has not accepted) and are dropped on close (not
  covered by a checkpoint, so the restart replays them).
- `BatchCreateTasks` and `BufferTask` are **both REST-only and absent from the Java client**,
  and no method is configured with batching, so one RPC per record with a mailbox-based
  in-flight cap. Queue-level `httpTarget.uriOverride` can silently override per-task URLs and
  **cannot be detected through the v2 client at all** (the field does not exist in the v2
  proto).
- The external HTTP serializer uses a two-stage immutable API: `httpTarget(url)` returns a
  non-generic body-binding stage, and `withBody(SerializationSchema<T>)` returns the configured
  schema.
  App Engine uses a conventional mutable `AppEngineTargetBuilder`: `withBody(...)` binds `T`,
  optional `with*` methods accumulate settings on the builder, and `build()` snapshots them into
  an immutable `AppEngineTargetSerializationSchema` with no configuration methods.
  HTTP URLs and App Engine relative URIs/routing can be resolved per record.
  HTTP bodies
  are sent only under POST/PUT/PATCH; App Engine bodies only under POST/PUT, matching the distinct
  proto contracts.
- Queue-level `appEngineRoutingOverride` always replaces task-level App Engine routing. Unlike the
  REST-only HTTP target override, this field is visible in the v2 `Queue` proto, but the sink does
  not fetch queue configuration before each write. The documented queue configuration therefore
  remains authoritative.
- App Engine dispatch and queue-level routing have a gated real-service acceptance suite because
  the emulator implements neither behavior.
  The gated suite creates one isolated queue per case and inspects requests while queues are
  paused before selectively resuming them for dispatch.
  Its manually scaled App Engine fixture is started only around that suite and is stopped and
  verified at zero instances on success, failure or handled interruption.
  The daily sweep restores that state after a hard cancellation that cannot run shell cleanup.
- Production uses application-default credentials unless `serviceAccountKeyFile(path)` selects a
  service-account JSON key. Only the path enters the job graph; each writer loads and scopes the
  key when it starts, so every eligible TaskManager must see the same path. Missing, malformed and
  non-service-account credentials fail with one sanitized message that carries no path, key
  material or parser cause. Emulator mode stays plaintext and credential-free, and the builder
  rejects configuring both modes. The loader stays internal to the Cloud Tasks module: its scope
  and failure identify this client family, while a shared public provider surface would admit
  credential forms outside the connector contract.
- At-least-once, stateless writer, flush on checkpoint.

[#23]: https://github.com/laughingman7743/flink-connector-gcp/issues/23
[#24]: https://github.com/laughingman7743/flink-connector-gcp/issues/24
[#25]: https://github.com/laughingman7743/flink-connector-gcp/issues/25
[#545]: https://github.com/laughingman7743/flink-connector-gcp/issues/545
[#608]: https://github.com/laughingman7743/flink-connector-gcp/issues/608
[#628]: https://github.com/laughingman7743/flink-connector-gcp/issues/628
[#632]: https://github.com/laughingman7743/flink-connector-gcp/issues/632
