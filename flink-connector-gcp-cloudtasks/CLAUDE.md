# CLAUDE.md — flink-connector-gcp-cloudtasks

Module-scoped guidance, loaded when Claude works in this module. Repository-wide rules
(build, workflow, version policy, licensing, package layout) stay in the root `CLAUDE.md`.

## Design decisions (do not silently revisit)

- **Cloud Tasks sink** (#23, design settled; implemented in #24): Cloud Tasks is an HTTP dispatch
  queue whose **pacing lives on the queue** (`maxDispatchesPerSecond`, `maxConcurrentDispatches`,
  retry config), so the sink has no rate knobs and there is **no queue auto-creation** — an
  auto-created queue would carry default limits, discarding the throttling that is the reason to
  use the service, and a deleted queue name cannot be reused for 3 days. HTTP targets only (App
  Engine targets are region-locked, invert 429/503 overload behaviour and are "less common");
  targets need an external IP; OIDC vs OAuth is chosen by the target, not by preference, so the
  builder rejects setting both. Fixed **and** per-record queue destinations from v1 — unlike
  Pub/Sub topics and BigQuery tables this costs nothing, since one `CloudTasksClient` serves every
  queue with no per-destination stream. **Unnamed tasks by default**; `taskIdExtractor(...)` **on
  the sink builder, not the serializer** (a `Task` has no id field — only the full `name` path,
  which needs the resolved queue) opts into deduplication (`ALREADY_EXISTS` = success), and the
  sink **hashes the extracted key with SHA-256**, because Google documents that sequential ids
  raise latency *and* error rates. The serializer never sets a name, so there is no second path
  around the hashing. The dedup window is **contradicted in Google's own sources — REST says up to
  24 h, the v2 proto says ~1 h — so design against 1 h**. **Retries are the sink's
  responsibility**: the generated client gives `CreateTask` an empty retryable-code set and a 20 s
  timeout (verified in `CloudTasksStubSettings` 2.94.0), as it does for every mutating method; the
  sink retries `UNAVAILABLE`/`DEADLINE_EXCEEDED`/`RESOURCE_EXHAUSTED` and gives `NOT_FOUND` a
  separate short budget (a 30-day-idle queue re-activates slowly, but a mistyped queue must not
  burn the full budget per record). `BatchCreateTasks` and `BufferTask` are **both REST-only and
  absent from the Java client**, and no method is configured with batching, so one RPC per record
  with a mailbox-based in-flight cap. Queue-level `httpTarget.uriOverride` can silently override
  per-task URLs and **cannot be detected through the v2 client at all** (the field does not exist
  in the v2 proto).
  At-least-once, stateless writer, flush on checkpoint. Decision record in the connector
  documentation page
- **Cloud Tasks implementation** (#24, PR #107): retrying is **one sink-owned loop in the writer**,
  not gax `createTaskSettings` retry — gax has a single retryable-code set and schedule per method,
  which cannot express the separate short `NOT_FOUND` budget, and a sink-owned loop is testable
  against a fake client. A failed create is **parked with a due time** and re-dispatched unchanged
  from the next `write()`/`flush()`; parked creates count against `maxInFlightTasks` (they are
  records the service has not accepted) and are dropped on close (not covered by a checkpoint, so
  the restart replays them). The serialization schema is a **two-stage builder**:
  `httpTarget(url)` returns a non-generic stage whose `withBody(SerializationSchema<T>)` binds the
  record type, so the chain infers `T` without a witness; `withUrl(...)` gives per-record URLs. The
  body is sent **only under POST/PUT/PATCH** — Cloud Tasks errors on a body under any other method,
  and since `withBody` binds `T` it cannot be omitted
