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

# ADR-0073: Bigtable auto-creation parks `NOT_FOUND` and repairs through an ensure

- Status: Accepted
- Date: 2026-08-09 (emulator behaviour measured 2026-08-08; reconciliation bound refined by [#414])
- Issues: [#233], [#414]
- Modules: bigtable (`sink`, `sink.tables`, `sink.writer`)
- Current behavior: `docs/content/docs/connectors/datastream/bigtable.md` § Table auto-creation

## Context / Evidence

The sink created neither its table nor the table's column families; `NOT_FOUND` was classified
fatal (ADR-0042's table), and [#233] recorded why a Pub/Sub-style disposition alone would be the
wrong feature: a Bigtable table's schema *is* its column families and their garbage-collection
policies, so a table created bare rejects every mutation, and the GC policy is exactly what
decides whether an at-least-once sink's replayed duplicates accumulate forever.

Measured before the design was committed (2026-08-08, `google-cloud-cli:441.0.0-emulators`,
`google-cloud-bigtable` 2.80.0):

- **A missing table answers `NOT_FOUND`** ("table … not found"), fanned request-level over every
  entry — on the emulator, which is what lets the emulator suite drive the repair end to end,
  unlike the missing-*family* case the emulator answers `INTERNAL` (ADR-0044's deviation table).
  The service answers the same `NOT_FOUND` with different wording — "No tables found for
  instance …", measured 2026-08-09 against an instance holding no tables, pinned by
  `BigtableAutoCreationRealGcpITCase`, whose run also exercised the repair against real metadata
  propagation and the add-only reconcile; the sink classifies by status alone, so the wording
  difference costs nothing. The missing-family `NOT_FOUND` was already pinned against the
  service ("Requested column family not found").
- **The emulator's admin surface behaves like the service's for what the repair needs**: a
  repeated `CreateTable` answers `ALREADY_EXISTS`; `ModifyColumnFamilies` with a `create` mod for
  an existing family answers `ALREADY_EXISTS` for the whole atomic request; created GC rules read
  back intact (`VersionRule`, `DurationRule`, nested `UnionRule`, and no-rule as `DefaultRule`).
- **The SDK retries neither `CreateTable` nor `ModifyColumnFamilies`** — both ship an empty
  retryable-code set (`BigtableTableAdminStubSettings`, read in 2.80.0) — the same no-layer-behind
  situation ADR-0071 found in BigQuery, minus the measured rate-limit shape, which for Bigtable is
  unmeasured.

## Decision

**The repair is the Pub/Sub sink's reactive shape, single-destination** (multi-destination since
ADR-0074, [#232]: `tableMissing` became a set, one repair ensures every table parked at the time,
and its budget is shared across them)**.** A mutation failing
`NOT_FOUND` under `CREATE_IF_NEEDED` is parked into a second queue (`pendingRepair`), and
`runRepair()` — from the next `write()` and inside `flush()` — drains the writer, ensures the
table and its declared families exist, re-applies the parked batch exempt from the capacity check
(a park cannot exceed `maxInFlightMutations`), and retries on the jittered recovery schedule until
the batch lands or the budget is spent. No `repairNeeded` flag: with no ordering keys, nothing can
owe a repair with an empty queue, so the queue's non-emptiness is the trigger. (That conclusion
rested on two premises when it was written; ADR-0074 removed the fixed table, and the *no ordering
keys* half carries it alone.)

**Creation needs a schema object, not a flag.** `createDisposition(CREATE_IF_NEEDED)` requires
`tableCreateOptions(...)` naming at least one column family, and the reverse combination —
options beside `CREATE_NEVER` — is rejected too, both at `build()`. The default stays
`CREATE_NEVER`: a table nobody declared is the liability [#233] names. The GC rules ride in the
sink's own `Serializable` model (`GcRule`: `maxVersions`, `maxAge`, `union`, `intersection`),
because the config ships in the job graph and the client's `GCRules` wrappers do not serialize.
The `maxAge` conversion is seconds-and-nanos to seconds-and-nanos (threeten), never `toNanos()`,
so ADR-0068's setter ceiling is deliberately not applied.

**The ensure is idempotent and add-only.** `TableAdmin.ensureTable` (SPI in `sink.tables`, the
BigQuery precedent; implementation short-lived-client-per-call, the `PubSubTopicAdmin` precedent)
tries `CreateTable` first — one RPC on the common path — and on `ALREADY_EXISTS` reconciles:
reads the live families, adds only the declared absentees in **one atomic**
`ModifyColumnFamiliesRequest` (a blind add of one existing family would fail the genuinely missing
ones with it), and on a lost family race re-reads and retries the remainder. An
existing family's rule is never compared or updated —
creation-only, per family, the `TopicCreateOptions` semantics carried down one level.

**The reconciliation is bounded by what its own termination argument asserts** ([#414], refining
this ADR's original "unbounded under perpetual external churn, accepted rather than capped"). A
losing round means at least one family that round read as missing is now present, so the missing
set shrinks strictly and is a subset of the declared families: at most that many rounds can lose,
and one more either adds the remainder or finds nothing to add. `declared + 1` is therefore the
exact bound rather than a chosen cap, and spending it is not a slow ensure but a contradiction —
a declared family being deleted between the read and the modify, or a read not seeing what the
modify reports. Three reasons the argument moved from a comment to a `for` budget with a tripwire,
the shape ADR-0045's isolation pass already uses: the loop was the one pass in this design that
was not self-bounding, which is what the `flush()` paragraph below assumes of every pass; a loop
with no end holds the task thread and would be reported only as checkpoints that stop completing
and a task that will not cancel, never as the reconciliation behind them, whereas the tripwire's
`IllegalStateException` becomes the `IOException` the recovery schedule already spends an attempt
on, backoff and all — where the loop itself re-issued admin RPCs with no backoff between rounds;
and the bound is what made the round behaviour testable at all — the seam below has to stop
somewhere for a scripted contradiction to be an assertion rather than a hang.

**The ensure's three admin operations are taken as functional values** — `BigtableTableAdmin`
tries them through a `@VisibleForTesting ensureWith(...)`, and only `ensureTable` binds them to a
real client — because that is the only seam a test can drive ([#414]): the client is final, this
repository uses no mocking framework, and it is built inside the call and closed with it, so it
cannot be injected; and nothing short of interposing on the RPC stream times a *concurrent* family
addition to land between one call's read and its modify — a live emulator alone cannot, whatever
it is asked to do. ADR-0047's shape, one level down from an adapter object
because the client here is per-call. The read seam yields the live family ids rather than the
client's `Table`, whose only construction is an `@InternalApi fromProto` a test would have to
reach for; what that leaves outside the seam is one projection the emulator ITCase pins in both
directions, and the same ITCase covers the method references — this is not [#321]'s untested
wiring, since it drives `ensureTable` itself down the creation, addition and no-op paths.

**`NOT_FOUND` outranks everything in the classifier**, including the transient-anywhere check that
ADR-0042 puts first for routing: `PubSubErrorClassifier`'s precedence, adopted because acting on
`NOT_FOUND` is safe where a drop is not — the repair re-applies and never discards, and under
`CREATE_NEVER` the outcome is a job failure with the disposition named either way. Unlike Pub/Sub
(ADR-0006), **the disposition gates the parking itself**: there is no cascade or ordering-key
reason to park under `CREATE_NEVER` here. `tableMissing` still carries the repair's reason,
consumed per attempt (per table since ADR-0074) — ADR-0006(b)'s shape — so a later incident cannot inherit an earlier
repair's answer; once the ensure has succeeded it is not repeated within the repair, and an ensure
that *fails* spends an attempt from the recovery schedule rather than the job, because the admin
client retries neither of its RPCs and this loop is the only retry layer a transient admin
failure has.

**The two queues never drain each other, and repair runs before isolation.** A solo re-submission
of the isolation pass (ADR-0045) that meets `NOT_FOUND` migrates to the repair queue — never back
to `pendingIsolation`, so the pass's tripwire invariant survives verbatim — and a repair's
re-application that draws a request-level `INVALID_ARGUMENT` parks for isolation, which the pass
right after it confirms. `flush()` became a loop over both passes; it terminates because each is
self-bounding (recovery schedule; park size with every entry applied, routed or migrated), so
non-termination requires an unbounded stream of external incidents — the property
`PubSubWriter.flush()` already has. The declined alternative — an isolation pass that early-exits
wholesale on the first `NOT_FOUND` — was rejected for muddying the tripwire's invariant to save
at most park-size solo RPCs in the rare table-vanished-mid-pass case.

**The budget is the three Pub/Sub-named `recovery*` knobs on `BigtableWriterOptions`** (500 ms →
10 s, 10 attempts, `RetrySchedule.DEFAULT_JITTER_RATIO`), settled with the user over fixed
constants. This *refines* ADR-0041's "no retry knobs" rather than reversing it: that rule is about
RPC retries the client owns, and these knobs budget a sink-owned repair — the same
`recovery*`-versus-`retry*` naming line the BigQuery options draw (ADR-0028).

**Metrics**: `tablesCreated` counts created tables (their declared families ride along);
`columnFamiliesAdded` counts only families added to a pre-existing table — first contact and
schema drift are different signals to an operator. Both register whatever the disposition.
`parkedMutations` sums both queues (its meaning — held by the writer, in neither the in-flight
counters nor the handler — is true of both). A parked `NOT_FOUND` **is** counted under
`errorClass.NOT_FOUND.errors` per entry, unlike a parked row-level report (ADR-0043's exclusion):
a missing table leaves no identity to confirm, and Pub/Sub counts its parked `NOT_FOUND`s the
same way.

## Consequences

- ADR-0041's "no auto-creation" bullet and ADR-0042's `NOT_FOUND`-is-fatal example are refined by
  this ADR; both carry a pointer rather than being rewritten.
- A failing ensure is retried on the recovery schedule, not per RPC: the budget covers a
  transient admin failure (the SDK gives these RPCs no retry layer of their own), and with the
  budget spent the ensure's own failure surfaces. A per-RPC `RetryingTableAdmin` tier — which
  would need its retriability verdict measured, the way ADR-0071's was for BigQuery's admin-plane
  rate limit — is deliberately deferred until such a shape is observed
  (widen-only-what-was-observed, ADR-0030's rule).
- A mutation naming a family `tableCreateOptions` does not declare is unrepairable by
  construction; the budget-exhaustion message names that case, since it is the one a user can fix
  by editing the options.
- The repair extends checkpoint duration by up to the recovery budget (~1 minute at defaults),
  stated on the documentation page beside Pub/Sub's identical caveat.
- `FakeMutationBatcher` gained `tableMissing` (request-level `NOT_FOUND` fan-out, the measured
  shape) and a send-count trigger for the mid-flush table-vanished case; `FakeTableAdmin`'s
  `onEnsure` hook clears the fake's missing state so repair convergence emerges from the ensure
  rather than being scripted turn by turn.

[#232]: https://github.com/laughingman7743/flink-connector-gcp/issues/232
[#233]: https://github.com/laughingman7743/flink-connector-gcp/issues/233
[#321]: https://github.com/laughingman7743/flink-connector-gcp/issues/321
[#414]: https://github.com/laughingman7743/flink-connector-gcp/issues/414
