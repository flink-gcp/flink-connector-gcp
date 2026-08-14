# Detailed guidance — flink-connector-gcp-cloudtasks

Module-scoped guidance, read when working in this module. Repository-wide rules
(build, workflow, version policy, licensing, package layout) stay in the root `AGENTS.md`.
This file holds the rules a session must follow; each decision's record — context, evidence,
declined alternatives — is the named ADR under `docs/adr/` or the docs page.

## Sink (`docs/adr/0048`)

- No rate knobs and **no queue auto-creation** — pacing lives on the queue, and an auto-created
  queue would discard the throttling that is the reason to use the service. External HTTP and
  App Engine targets are separate serializer arms. OIDC vs OAuth is the external HTTP target's
  choice, so that builder rejects setting both; App Engine uses the task `oneof`'s internal
  request arm and optional same-project service/version/instance routing.
- **Retries are the sink's one owned loop in the writer**, never gax `createTaskSettings`;
  `NOT_FOUND` keeps its separate short budget. A failed create parks with a due time; parked
  creates count against `maxInFlightTasks` and drop on close.
- Task naming: unnamed by default; `taskIdExtractor(...)` on the **sink builder**, key hashed
  SHA-256, `ALREADY_EXISTS` = success; design against the 1 h dedup window (Google's own
  sources contradict each other). `httpTarget(url)` uses the existing two-stage immutable schema
  API. `appEngineTarget(relativeUri)` returns a mutable builder: `withBody(...)` binds the body
  type, optional settings stay on that builder, and `build()` produces the immutable serializer.
  External HTTP bodies go
  out only under POST/PUT/PATCH; App Engine bodies only under POST/PUT, matching their distinct
  proto contracts. Queue-level `appEngineRoutingOverride` remains authoritative over task-level
  routing.

## Failure policy and metrics (`docs/adr/0049`)

- **Exactly three failures are routed**: serializer rejection, extractor throw, creation
  rejected `INVALID_ARGUMENT`. An extractor's missing key is per-stream and fatal; a serializer
  `null` is a skip (`docs/adr/0001`).
- The transient half of classification scans the whole chain; the `INVALID_ARGUMENT` half
  deliberately does **not** — the asymmetry was found by mutation testing, and the scanning
  variant is the worse code. The routing branch sits before the `asyncError` early-return.
- `numRecordsSend` counts inside `dispatch(...)` under `pending == null`; error classes count
  every attempt (the sum over transient codes *is* the retry volume — no separate retries
  counter); `ALREADY_EXISTS` is `tasksDeduplicated`, never an error; per-queue counters are
  looked up per record (no per-destination state exists to cache a handle on).

## Table sink (`docs/adr/0107`)

- `cloud-tasks` is insert-only and maps one table to one fixed queue. `target.type` defaults to
  external HTTP and can select App Engine without translating between their protobuf request arms.
  A generic Flink format sees physical columns only; writable target metadata configures the task
  around the body.
- HTTP POST/PUT/PATCH and App Engine POST/PUT invoke the body format. Fixed request options are
  defaults and non-null row metadata overrides them; header names compare case-insensitively. With
  no fixed address, the selected target's `url` or `relative-uri` metadata must be declared `STRING
  NOT NULL` in the catalog schema. Wrong-family options and metadata are rejected.
- Selecting `task-id` installs the existing sink-builder extractor — never name a task in the table
  serializer. API credentials, OIDC/OAuth dispatch tokens and network reachability remain three
  separate concerns. App Engine rejects dispatch-token options and exposes separate service,
  version and instance metadata; queue-level routing override remains authoritative.
- The built-in `form-urlencoded` format accepts only `STRING` and `ARRAY<STRING>`, preserves
  physical schema and array order, and owns `application/x-www-form-urlencoded` for body-carrying
  methods. Fixed and metadata headers may repeat that value but may not conflict with it.
