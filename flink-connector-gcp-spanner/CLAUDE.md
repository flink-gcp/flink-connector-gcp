# CLAUDE.md — flink-connector-gcp-spanner

Module-scoped guidance, loaded when Claude works in this module. Repository-wide rules
(build, workflow, version policy, licensing, package layout) stay in the root `CLAUDE.md`.
This file holds the rules a session must follow; each decision's record — context, evidence,
declined alternatives — is the named ADR under `docs/adr/` or the docs page.

## Sink design (`docs/adr/0075`)

- **The destination is a database, not a table.** The serializer's `Mutation` names its own table,
  so one sink writes to as many tables as it produces. That is why `SpannerDatabase` is at the
  module root, why the cell weights cover the whole database, and why there are no
  per-destination metrics — the cardinality would be the serializer's to choose.
- **One mutation per `MutationGroup`.** The group is the unit `BatchWriteResponse` reports a
  status for; anything else gives up per-record failure routing, which is the whole reason this
  sink uses `batchWriteAtLeastOnce` rather than a commit. Measured, not assumed: every rejection
  shape `SpannerRejectionITCase` provokes comes back per group.
- **The writer is synchronous — no mailbox, no in-flight bookkeeping.** This RPC has no
  asynchronous or self-batching form, so there is nothing to wrap. Do not port the Bigtable or
  Pub/Sub writer's machinery here on the assumption that it was forgotten.
- **The client library retries this RPC not at all** (`SpannerStubSettings.batchWriteSettings` is
  `no_retry_0_codes`; `runWithSessionRetry` recovers only a lost session). The retry loop and its
  knobs are the connector's, the Cloud Tasks shape rather than the Bigtable one. Recheck on a
  client upgrade.
- **Batch write has no replay protection**, so idempotence is the serializer's to supply; it is
  documented on the SPI and on the sink, never enforced.
- `SpannerDatabaseAccess` holds its three operations as functional values over the `Spanner`
  service handle, for the reason `docs/adr/0047` gives for the Bigtable batcher adapter. A test
  faking `DatabaseClient` itself is the thing this exists to avoid.
- `null` from the serializer is skip, and the check sits immediately after the serializer's
  `catch`, ahead of the buffer, the weights and every metric that counts a send (`docs/adr/0001`).

## Error handling (`docs/adr/0076`)

- **Routed: `INVALID_ARGUMENT` and `ALREADY_EXISTS`, per group only.** Retried: `ABORTED`,
  `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`. Everything else fails the job.
- **A constraint violation is `constraintViolationPolicy`'s decision, defaulting to `FAIL_JOB`.**
  It covers **both** `FAILED_PRECONDITION` (NOT NULL, over-long value, foreign key) and
  `OUT_OF_RANGE` (`CHECK`) — measured; a policy covering one of them is a bug, not a subset.
  The default is failing for two reasons, and the first is the load-bearing one: a constraint
  violation usually means the record-to-column mapping is wrong rather than that one record is
  anomalous, so failing is right *even under a dropping handler*; and `FAILED_PRECONDITION` is
  also what every write gets while a CMEK key is unreachable, which self-heals.
- **Never split a status by its message text** to separate those cases. Spanner's own error-codes
  page says the message may change at any time.
- **Do not add a three-valued fail/DLQ/drop option.** `failedMutationHandler` already is that
  choice; two policies over one decision can contradict each other (ADR-0076).
- **A failure of the request is never routed**, whatever its status: it names no mutation, and the
  mutations it carried have no reported outcome.
- Both code mappings (`ErrorCode` → gax, canonical number → gax) are written-out switches, and
  `SpannerErrorClassifierTest` iterates every value of both. A code added to the client library
  must fail a test rather than silently become unclassifiable.

## Batching and cell weights (`docs/adr/0077`)

- Spanner's 80,000-mutation and 100 MiB limits are **per request**, not per group. The three batch
  limits are correctness, not tuning.
- `maxBatchCells` counts index entries, read once from `INFORMATION_SCHEMA` when the writer opens
  — dialect-branched, primary-key index excluded by name, names folded to lower case. Reading it
  at creation is what makes an unreadable schema a job that never starts.
- **A table the weights do not know is counted without index entries, never rejected.** The
  default's 16-fold headroom under 80,000 is what absorbs that, so raising `maxBatchCells` toward
  the limit is a real trade and the docs say so.
- **The client library exposes no public route from a `Mutation` to its wire form**, and
  `Mutation.toString()` truncates strings at 36 characters. Hence: the byte limit is an estimate,
  and the dead-letter payload is the Java-serialized `Mutation`. Recheck both on a client upgrade
  — a public conversion appearing upstream would reopen the payload encoding.

## Testing

- Emulator ITs pin `gcr.io/cloud-spanner-emulator/emulator`, **not** the `google-cloud-cli` bundle
  the other connectors use: its Spanner emulator predates `BatchWrite` (added in v1.5.31), so the
  whole write path would answer `UNIMPLEMENTED`.
- Identifiers in the ITs are lower case and unquoted, which is what lets one set of mutations and
  one query serve both dialects.
- The production `createWriter(WriterInitContext)` is covered by the emulator ITs rather than by a
  closed-port unit test: this sink reads the schema while creating the writer, so a closed port
  costs the client's whole retry budget (27 s, measured 2026-08-09) to prove less.
- An emulator is never the authority. Real-GCP confirmation of the rejection table is `#224`.
