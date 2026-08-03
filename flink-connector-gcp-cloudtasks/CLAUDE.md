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
- **Per-task failure policy** (#207, the #37 series): `failedTaskHandler(...)` takes the shared
  `FailureHandler<FailedTask>` from `base.failure`, defaulting to `failJob()` — behaviourally
  today's capture-and-rethrow, which is why `CloudTasksWriterTest` was left in place as the
  regression guard (its one edit is a message assertion: a serialization failure now carries the
  handler's wording). `FailedTask` sits at the `sink` root (a one-class `sink.failure` fails the
  #119 layer test) and carries the **whole serialized `Task`** as `getPayloadBytes()`, not just the
  HTTP body, so a dead-letter consumer recovers the target, method, headers and authorization with
  `parseFrom`; `describeDestination()` is the queue resource path the `FailedElement` javadoc
  prescribes, not `QueueDestination.toString()`. **Exactly three failures are routed, and the
  boundary is the decision**: a record the serializer rejects, a task id extractor that *throws*,
  and a creation rejected `INVALID_ARGUMENT`. Not routed, in two directions: outage-shaped failures
  (an exhausted transient or `NOT_FOUND` budget, `PERMISSION_DENIED`) must never reach a dropping
  handler, or an incident bleeds the stream one record at a time instead of backpressuring; and
  configuration-shaped ones (a resolver returning null, a serializer returning an already-named
  task, an extractor returning null or an empty key) fail *every* record alike, so dropping them
  would leave an empty queue under a green job. The extractor is the pair worth not re-litigating —
  a throw is per-record and routed, a missing key is per-stream and fatal. `ALREADY_EXISTS` on a
  named task stays success and never reaches the handler. **Classification is a precedence over the
  whole cause chain, not a first-match** — but **only the transient half**: routing takes
  `firstMatching(throwable, TRANSIENT_CODES) == null && code == INVALID_ARGUMENT`, where `code` is
  the chain's first classifiable status. The transient lookup scans the whole chain, so "an unstable
  service can never produce a dead letter" is a property of the code rather than of gax producing
  one status per failure — which it does today, making the forms equivalent *now*. The
  `INVALID_ARGUMENT` half deliberately does **not** scan: searching for it anywhere would drop a
  task whose outermost status is `INTERNAL` or `UNKNOWN`, which is the mirror image of the mistake
  the transient scan prevents. That asymmetry was found by mutation testing — the scanning variant's
  mutant survived, and inspecting why showed the mutant was the better code. Same shape as `PubSubErrorClassifier`'s precedence, adopted
  there because Pub/Sub chains really can carry two. `INVALID_ARGUMENT` is the one routed status
  because gRPC defines it as arguments "problematic regardless of the state of the system" and
  AIP-194 puts it in must-not-retry ("retrying … will never succeed"), unlike `FAILED_PRECONDITION`
  and `OUT_OF_RANGE`, which are state-dependent and stay job-level. The `INVALID_ARGUMENT` branch sits
  **before** the `asyncError` early-return, deliberately: the job is failing either way, but a
  dead-letter destination missing the task is worse than one holding a duplicate the replay will
  produce again. A handler failure inside `onCreateFailed` is captured into `asyncError` rather than
  thrown (a mailbox mail cannot throw at its caller); an unchecked one is wrapped naming the queue.
  `flush()` runs after the drain loop, which exits only with nothing in flight *and* nothing parked,
  so a re-dispatched retry cannot land after the handler flushed. Coverage is unit tests only: the
  emulator's `INVALID_ARGUMENT` surface is not evidence about the service's (see the emulator rule)
- **Sink metrics** (#209, the #37 series): `CloudTasksWriterMetrics` (`sink.writer`) over the shared
  `base.metrics` helpers, with plain counters — completions arrive as mailbox mails, so every
  increment is on the task thread. Four things not to re-litigate. **`numRecordsSend` is counted inside
  `dispatch(...)`, guarded by `pending == null`** — that argument already means "first attempt", so
  the retry-safety property is carried by the method's own signature rather than by a call-site
  convention. `dispatchDueRetries` re-enters `dispatch` for every parked creation, and this sink's
  retries are its own (#24), so an unguarded increment would report a job working through an outage
  as a busier one. Counting in `write()` instead — where it sat until the second review round of
  PR #242 — was wrong for the mirror reason: a `TaskCreator` throwing synchronously registers no
  callback, so that record reached Cloud Tasks not at all and must not count as sent. The Pub/Sub
  sink resolves the same pair with an explicit `firstAttempt` flag on `publishTo`. The repo-wide
  decision is on #208 and in the base module's CLAUDE.md. **Error classes count every attempt, retryable ones
  included**, which is the deliberate asymmetry with `numRecordsSend`: the sum over the transient
  codes *is* the retry volume, and that is why the separate retries counter the issue considered was
  declined. **`ALREADY_EXISTS` on a named task is `tasksDeduplicated`, not an error** — it is the
  success naming exists to produce, so it appears in neither `numRecordsSendErrors` nor
  `errorClass`, and the `metrics.createFailure(code)` call sits *after* that branch's early return
  for exactly that reason. **The per-queue counters are looked up per record**, unlike the Pub/Sub
  sink's cached-per-`DestinationState` handle: this writer keeps no per-destination state at all
  (one client serves every queue, #23), so there is nowhere to cache one; the lookup is a map read
  on the queue path the request already carries, and it is a no-op object when the option is off.
  Assertions ride `CloudTasksWriterTest`'s fakes in a `CloudTasksWriterMetricsTest` beside it, all
  by registered name
