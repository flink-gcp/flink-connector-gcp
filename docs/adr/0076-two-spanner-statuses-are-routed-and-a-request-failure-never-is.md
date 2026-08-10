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

# ADR-0076: Two statuses are always routed, constraint violations are the job's choice

- Status: Accepted
- Date: 2026-08-09 (rejection statuses measured 2026-08-09 against
  `gcr.io/cloud-spanner-emulator/emulator:1.5.56`, google-cloud-spanner 6.119.0)
- Issues: [#220]
- Modules: spanner (`sink.writer`)
- Current behavior: `docs/content/docs/connectors/datastream/spanner.md` § Error handling

## Context / Evidence

The sink wires the shared `base.failure` SPI from day one, so it has to decide which failures are
a *record's* and may be handed to a handler that might drop them, and which are the job's. The
governing rule is the one ADR-0042 states for Bigtable: **only a status that is unrecoverable by
definition may be routed**, because a dropping policy must never turn an unstable service into
silent data loss.

`SpannerRejectionITCase` provokes each shape of bad mutation and records what comes back. Measured
2026-08-09, one run, GoogleSQL dialect, emulator v1.5.56:

| Rejection | Status | Reported |
|---|---|---|
| `insert` of a row whose primary key is already there | `ALREADY_EXISTS` | per group |
| `insert` colliding on a `UNIQUE` index | `ALREADY_EXISTS` | per group |
| `NULL` written into a `NOT NULL` column | `FAILED_PRECONDITION` | per group |
| a value longer than the column's declared maximum | `FAILED_PRECONDITION` | per group |
| a foreign-key violation | `FAILED_PRECONDITION` | per group |
| a `CHECK` constraint violation | `OUT_OF_RANGE` | per group |
| a column the table does not have | `NOT_FOUND` | per group |
| a table the database does not have | `NOT_FOUND` | per group |
| an `update` whose row is not there | `NOT_FOUND` | per group |
| a `delete` whose row is not there | *applied* | per group |

Two things in that table are load-bearing. Every rejection is reported **per group** rather than
as a request failure, which is what ADR-0075's one-mutation-per-group shape is built on. And the
two most ordinary data errors a user will hit — a null in a `NOT NULL` column, a value over its
column's length — arrive under `FAILED_PRECONDITION`.

The client library's own generated retry policy is the other piece of evidence:
`SpannerStubSettings` gives `Commit` and `ExecuteSql` the retryable set
`{UNAVAILABLE, RESOURCE_EXHAUSTED}` (`retry_policy_3_codes`), and gives `INTERNAL` to nothing.

That table was emulator evidence when it was written — a starting point rather than an authority.
**Every row of it is now confirmed against the service** (2026-08-10, `SpannerRejectionRealGcpITCase`
under [#224], `google-cloud-spanner` 6.119.0, a 100-processing-unit regional instance): same status
for every shape, and every one reported per group, including the `delete` of a missing row that is
simply applied. The emulator was right, which is worth recording precisely because it was not
something the emulator could establish. The gated class now asserts each row, so a change on either
side has to be declared.

One shape the two do *not* agree on turned up while porting the class, and it is a harness
deviation rather than a rejection: real Spanner refuses a `CreateDatabase` request that carries
extra DDL for a **PostgreSQL-dialect** database ("DDL statements other than &lt;CREATE DATABASE&gt;
are not allowed in database creation request for PostgreSQL-enabled databases"), where the emulator
applies them. The gated harness therefore issues the DDL as a separate `updateDatabaseDdl` for that
dialect.

## Decision

**Routed to the failure handler: `INVALID_ARGUMENT` and `ALREADY_EXISTS`, and only when reported
for one mutation group.**

- `INVALID_ARGUMENT` — gRPC defines it as "problematic regardless of the state of the system", and
  AIP-194 lists it as must-not-retry. Same status, same reasoning as ADR-0042.
- `ALREADY_EXISTS` — a replayed `insert`. It *is* state-dependent, and that is why routing it is
  right rather than despite it: the state it depends on is this mutation's own earlier
  application, so the row the record describes is in the database either way. Under an
  at-least-once sink with no replay protection (ADR-0075) this is a normal event, not an anomaly.

**Retried: `ABORTED`, `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`.** The first is
Spanner's own contention signal — the emulator's "only one transaction at a time" refusal arrives
under it too. `RESOURCE_EXHAUSTED` is the one [#220] asked to be decided here, and it is decided
by the client library's own classification of it as retryable for `Commit`: it names an instance at
capacity, which passes the transient bar.

**Everything else fails the job**, including a status carrying no classifiable code at all.

**A constraint violation is the job's decision, and it defaults to failing.**
`constraintViolationPolicy` takes `FAIL_JOB` (default) or `ROUTE_TO_FAILURE_HANDLER`; the latter
moves **both** `FAILED_PRECONDITION` and `OUT_OF_RANGE` into `ROW_LEVEL`, after which the
already-configured `failedMutationHandler` decides between failing, dropping and dead-lettering.
Covering both statuses is not optional: the measurement above splits constraint violations across
them, so a policy that moved only one would leave a job that opted in still dying on its first
`CHECK` violation.

The option exists because the two readings of such a refusal are both defensible and only the
pipeline's owner knows which applies. It defaults to failing for two reasons, one of them measured
by someone else:

- A constraint violation usually says the *mapping* from records to columns is wrong rather than
  that one record is anomalous. Every record of that shape will be refused, so shedding them one at
  a time hides a systematic problem behind a green job. (This is the argument the repository owner
  made, and it is the better one — it explains why failing is right even under a dropping handler,
  which the ambiguity argument below does not.)
- `FAILED_PRECONDITION` has one documented cause that is neither data nor permanent: while a
  database's CMEK key is disabled, destroyed or unreachable, *every* write is refused with it, and
  Cloud KMS restores access automatically when the key returns. A job routing it into a dropping
  handler would shed its whole stream through a key incident.

Failing loses nothing — the records are replayed from the source. It costs progress, which is the
trade the opt-in lets a job make differently.

**`NOT_FOUND` is not routed either**: a missing table or column fails every record alike, so it is
a configuration failure rather than a bad row, and this sink creates nothing.

That status is the one place the rule costs something real, and it was measured rather than
assumed: an `update` whose row has since been deleted also answers `NOT_FOUND`, and that *is* a
per-record condition. It is still not routed, because the status does not distinguish it from a
wrong table name — routing it would drop every record of a misconfigured job. The docs page says so
beside the table and names `insertOrUpdate` as the operation for a stream that can legitimately
update a row that may be gone. **Reopen** if Spanner ever distinguishes the two, or alongside the
opt-in option the `FAILED_PRECONDITION` decision already names.

**A failure of the *request* is never routed, whatever its status.** It says nothing about which
mutation is at fault, and the mutations it carried have no reported outcome — dropping all of them
over one status would discard records the service may not have looked at yet. So the request-level
classification answers only "retry" or "fail the job", and `INVALID_ARGUMENT` on a whole request
(too many mutations, too large) fails it.

Transient wins over anything in front of it in the cause chain, so an unstable service can never
present as terminal because a wrapper exception sat on top of it — the precedence ADR-0042 sets.

## Consequences

- **A Spanner dead-letter queue is narrower than the sibling connectors' out of the box.** By
  default it catches replayed inserts and malformed arguments, and not the schema violations a user
  might expect; `constraintViolationPolicy` is how a job widens it. The docs page says so beside the
  table rather than leaving it to be discovered.
- **This diverges from ADR-0042 deliberately, and the discriminator is real.** Bigtable declines
  `FAILED_PRECONDITION` outright, and the reason it can is that its statuses arrive fanned
  request-level over every entry, so one there may not be about the row at all. Spanner reports per
  mutation group — measured, every shape above — so the status *is* about that mutation, and the
  remaining doubt is the CMEK case rather than the reporting channel. That is why an opt-in is
  defensible here and would not be there.
- Both `ErrorCode` → gax and canonical-number → gax mappings are written out as switches rather
  than matched by enum name, and `SpannerErrorClassifierTest` iterates every `ErrorCode` and every
  canonical number: a code added to the client library fails a test instead of silently becoming
  unclassifiable, which would read as fatal and could hide a retryable status.
- Because the writer sees its own retries (ADR-0075), a transient status it recovered from is
  still counted in `errorClass.*.errors`. That makes the breakdown a retry-cause view here, unlike
  on the sinks whose SDK absorbs the same failures — stated on the metric rather than left to a
  dashboard to generalise.

## Alternatives declined

- **Routing constraint violations by default.** It would make the dead-letter queue far more useful
  — those are the refusals a user actually meets — and everything *documented* about
  `FAILED_PRECONDITION` on a write is data-shaped, with every retry configuration, AIP-194 and
  Beam treating it as permanent. Declined for the CMEK case above, which is documented, transient
  and affects every record equally. The opt-in is the resolution rather than a compromise: a job
  that knows its stream carries occasional unacceptable records can say so, and one that does not
  is not silently exposed to the incident.
- **A three-valued option (fail / dead-letter / drop).** Rejected as a duplicate of
  `failedMutationHandler`, which already offers exactly those three for every other routed failure.
  Two policies over the same decision would let a job say "constraint violations to the DLQ" while
  its handler says "drop", and would need a second dead-letter configuration to honour it.
- **Splitting `FAILED_PRECONDITION` by message text** to separate a constraint violation from the
  CMEK case. The status is the contract; Spanner's own error-codes page warns that "the text
  provided in the message might change at any time so applications shouldn't depend on the actual
  text".
- **Matching the failure message to tell a data-shaped `FAILED_PRECONDITION` from a transient
  one.** Message text is not contract, and this repository has already paid for message-coupled
  logic elsewhere; a service reword would silently turn job failures into dropped records.

[#220]: https://github.com/laughingman7743/flink-connector-gcp/issues/220
[#224]: https://github.com/laughingman7743/flink-connector-gcp/issues/224
