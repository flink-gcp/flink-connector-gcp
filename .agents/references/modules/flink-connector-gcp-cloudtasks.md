# Detailed guidance — flink-connector-gcp-cloudtasks

Module-scoped guidance, read when working in this module. Repository-wide rules
(build, workflow, version policy, licensing, package layout) stay in the root `AGENTS.md`.
This file holds the rules a session must follow; each decision's record — context, evidence,
declined alternatives — is the named ADR under `docs/adr/` or the docs page.

## Sink (`docs/adr/0048`)

- No rate knobs and **no queue auto-creation** — pacing lives on the queue, and an auto-created
  queue would discard the throttling that is the reason to use the service. HTTP targets only;
  OIDC vs OAuth is the target's choice, so the builder rejects setting both.
- **Retries are the sink's one owned loop in the writer**, never gax `createTaskSettings`;
  `NOT_FOUND` keeps its separate short budget. A failed create parks with a due time; parked
  creates count against `maxInFlightTasks` and drop on close.
- Task naming: unnamed by default; `taskIdExtractor(...)` on the **sink builder**, key hashed
  SHA-256, `ALREADY_EXISTS` = success; design against the 1 h dedup window (Google's own
  sources contradict each other). The serialization schema is the two-stage builder
  (`httpTarget(url)` → `withBody(...)`); a body goes out only under POST/PUT/PATCH.

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
