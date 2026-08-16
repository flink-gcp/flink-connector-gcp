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

# ADR-0030: A missing BigQuery table does not answer `NOT_FOUND`

- Status: Accepted
- Date: 2026-08-06 (measured on [#289], which grew a writer change because of it); buffered half
  measured 2026-08-08 on [#318], which grew a committer change the same way; the buffered
  **append** side measured 2026-08-08 on [#382], which grew no code change because of it
- Issues: [#289], [#318], [#382]
- Modules: bigquery (`sink.storage`)
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md` § A missing table does
  not say NOT_FOUND

## Context / Evidence

Opening a Storage Write API stream against a table that is not there answers
`PERMISSION_DENIED: Permission 'TABLES_GET' denied on resource '<table>' (or it may not exist)`.
The service masks existence, as an API that must not let an unauthorised caller probe for table
names has to. The goccy emulator answers `NOT_FOUND` (and `UNKNOWN` on both the default stream and
`CreateWriteStream`), and `AppendErrorClassifier` recovered on `NOT_FOUND` alone — so
**`CREATE_IF_NEEDED` had never once created a table against the real service**, while every
emulator test said it did. Nothing caught it because the gated storage-path suites create their
tables up front.

The masked message means the text alone cannot tell a permission failure from a missing table.
The decisive read-only probe is **`bq show <table>` under the *same* ADC**: it answers
`Not found`, which proves the credentials hold `bigquery.tables.get` and therefore that only
the table was missing — run it before blaming credentials.

The buffered path was left inferred, and measuring it (2026-08-08, `CreateWriteStream` driven
directly at a table that does not exist) produced two facts the inference did not carry:

- **The code matches; the permission name is noise.** `CreateWriteStream` answers
  `PERMISSION_DENIED: Permission 'TABLES_UPDATE_DATA' denied on resource '<table>' (or it may not
  exist)`. That is now the third observation, and no rule connects them: an absent table said
  `TABLES_GET` to a default-stream append and `TABLES_UPDATE_DATA` to `CreateWriteStream`, while
  the propagation window said `TABLES_UPDATE_DATA` to that same append. The permission tracks
  neither the RPC nor whether the table is absent — one more reason the rule matches codes, since
  any text match would have been written against whichever of the three was seen first.
- **The propagation window reaches the committer.** A `FlushRows` on a stream whose table the
  writer has just created can answer the same masked code, and `BufferedStreamCommitter` retried
  on `isTransient` alone — so the first commit after auto-creation failed the checkpoint. Observed
  once in five end-to-end runs; the other four never saw the window, on `CreateWriteStream` either.
  The datum, since a masked code is exactly what this ADR says not to reason about without one:
  `PERMISSION_DENIED: Permission 'TABLES_UPDATE_DATA' denied on resource
  'projects/<p>/datasets/<d>/tables/<t>' (or it may not exist)` — **naming the table, not the
  stream**, from `BufferedStreamCommitter.flush` on the first commit of a two-subtask job.

The second is a defect of the same shape as the one this ADR's first half fixed, found the same
way: the half nobody had driven at a missing table was the half that was wrong.

The buffered writer's **append** side was inferred in its turn, and measuring it settled it the
other way round: the window does not reach an append, so its four recovery decisions need no
missing-table verdict. (A table *dropped* mid-run would of course answer the masked code to an
append; that is terminal by design here, and no allowance would repair it.) 140 trials, in seven
runs of twenty, a table of its own each, driving the writer's own sequence and pausing before no
RPC that had not already failed — `CreateWriteStream` at the absent table, a creation through
`BigQueryTableAdmin`, `CreateWriteStream` again, the first `AppendRows` on the stream that opens,
then the `FlushRows` that would commit it:

| the RPC, in the order a trial drives it | denials | trials affected |
|---|---|---|
| `CreateWriteStream` at the absent table | 140 | 140, by construction |
| `CreateWriteStream` after the creation | 45 | 32 (23%), never more than two in a row |
| the first `AppendRows` on the stream | **0** | **0** |
| the first `FlushRows` on that stream | 11 | 11 (8%), every one cleared by the next attempt |

The zero is not a weak observation, and keeping the flush in the same trial is what makes it strong:
the appends had *the same 140 opportunities* the flush was denied at eleven times, on the same
table, immediately before it. So they were not sheltered by a window that had already closed — the
window was demonstrably still open for `FlushRows` — and had they shared the flush's rate, zero of
140 would have arrived with probability 1 × 10⁻⁵. Zero of 140 puts the 95% upper bound on the
per-trial rate at about 2%.

Five end-to-end runs of the auto-creating job in the same session, two subtasks racing to create one
table and ten creation attempts across five tables, add nothing at either place: no committer
retry, no failed append, no restart. That is the shape the single flush observation above came from
and it did not recur in five, so read that one-in-five as a small sample rather than as a rate.
A reading that fits both write paths, offered as a reading and not as a mechanism: an append is
denied when it is the writer's *first* contact with the table, and not otherwise. On the default
stream the first RPC **is** an append — where the `TABLES_GET` denial and the post-creation window
were both seen — while on the buffered path a stream has already been opened on the table before any
append is sent. What it does not explain is `FlushRows`, which names the table too and is denied
*after* an append to the same table has succeeded; so whatever propagates here is not one monotonic
per-table fact, and the mechanism stays unpinned exactly as the paragraph below says.

**What one observation does not settle** — the fix is right under every reading, but the mechanism
is not pinned, and the masked code is precisely what cannot distinguish them. Table-metadata
propagation is the reading recorded above; ACL propagation on a freshly created table would look
identical, and is not obviously weaker given that the writer's own `CreateWriteStream` had already
succeeded against that table moments earlier. Waiting is the right response either way, which is
why this was not held for a discriminating measurement — but do not cite the mechanism as measured.

## Decision

The verdict is `AppendErrorClassifier.isMissingTable`, taking both codes, consumed by
`BigQueryDefaultStreamWriter` (three sites) and `BigQueryBufferedStreamWriter.createStream`.
`BufferedStreamCommitter.flush` takes `isExistenceMasked`, the masked half alone — see the
committer paragraph below for why the two cannot be the same predicate.

**The buffered writer's append side takes no verdict at all, and that is a measured decision rather
than an omission.** Its four recovery decisions — `recover`, `resendAtSameOffset`, `replayBatches`
and `probeRestoredStream` — stay transient-only. Widening them would have been this ADR's own
mistake a third time, on zero observations where the committer's allowance has one, and it is not
free either: under `CREATE_IF_NEEDED` each of the four would then spend the recovery budget on a
genuine denial that fails at once today. `openAppender` is not a fifth candidate: read against
bigquerystorage 3.30.0, `StreamWriter.build()` sends no `AppendRows` — its `ConnectionWorker`
creates a client of its own and starts its append thread, and the bidi stream opens from that
thread on the first append — so no server verdict can arrive there, only a local `INVALID_ARGUMENT`
for a null writer schema.

Three things not to re-derive:

- **`isRetriable`'s post-creation clause had to widen too**, and that is measured, not symmetry:
  the propagation window right after this writer creates the table masks the same way, naming
  `TABLES_UPDATE_DATA` — a run that fixed only the first site created the table and then failed
  on the very next append.
- **Status codes, never the message text**: the "(or it may not exist)" wording is the service's
  prose and nothing pins it, whereas a code cannot quietly stop matching — which is exactly how
  this defect survived.
- **A failure naming rows is excluded**, since the SDK copies the response's code onto a
  row-detailed exception, so rows plus a code is a verdict about the data; that guard does real
  work for `PERMISSION_DENIED` and none for `NOT_FOUND`, and is written about the shape so the
  two cannot drift.

**The widening needs `scheduleFor`, and that is not tidiness**: `createTableIfMissing` is
reached from *schema* repairs too, which run on the fifteen-minute `schemaWaitSchedule`. An
existing table the credentials cannot write to answers the masked code, the creation attempt
then returns HTTP 409 and is swallowed as success, and `isRetriable`'s post-creation clause is
true from then on — so without the bound a failure that used to be immediate and well named
becomes a checkpoint timeout with no cause attached. The bound caps a missing-table verdict at
the recovery schedule wherever the repair happens to be, at **both** `retryBatches` call sites —
the `rebuildState` catch and the append loop; fixing only the first leaves the defect reachable,
which is how it was found. It **also restores** the schema budget for a later mismatch: the
escalation fires only on the reconciliation, which runs once per repair, so a mismatch arriving
*after* a missing-table verdict would otherwise wait out schema propagation on the one-minute
budget and fail a repair that was progressing. Deliberately those two failures only — a
transient or stale-writer failure during a schema repair keeps the long budget.

**The committer's allowance is narrower than the writers' in two ways, and both are load-bearing.**

It is **gated on the disposition**, because the committer has no `tableCreated` flag to key on — the
writer creates tables, the committer only flushes — so the question it can answer is not "did we
just create this table" but "does anything in this job create tables", which is exactly
`CreateDisposition`. Widening unconditionally was declined for costing a `CREATE_NEVER` job the
budget for nothing.

It takes **`isExistenceMasked` and not `isMissingTable`**, i.e. the masked `PERMISSION_DENIED`
without the `NOT_FOUND`, and the rule behind that is **widen only what was observed** — the same
discipline whose absence this ADR exists to record. Every observation of a table the real service
will not confirm is the masked code; `NOT_FOUND` is in the wide verdict for the emulator, which
answers it, and for the writers, which recovered on it before any of this was measured. Adding a
commit-time allowance for a code the service has never been seen to answer for this condition would
be a fresh unbacked inference, in the direction that costs rather than saves: about 55 s per
committable with the default `recovery*` values, serially, across every committable a restore
replays.

The direction being declined is worth naming as a risk and not as a fact: `FlushRows` targets a
write *stream*, streams age out on a seven-day TTL, and a missing stream is terminal mid-run — but
what expiry answers is undocumented and unmeasured (the DataStream page's "Stream lifetime"
paragraph says so), and a gone stream is recognised elsewhere in the classifier by a `StorageError`
rather than by a status code. That is an argument for not guessing, not a claim about `NOT_FOUND`.
The narrowing costs nothing measured: the emulator, whose signal *is* `NOT_FOUND`, cannot reach
auto-creation on this write path at all ([#326] — closed as an upstream report,
[goccy/bigquery-emulator#504](https://github.com/goccy/bigquery-emulator/issues/504), rather than
a second workaround).

Leaving the committer alone was declined too, and not because a restart would not recover it —
`FlushRows` is idempotent and the restored commit succeeds — but because a job that sets
`restart-strategy: none`, as a batch-shaped job reasonably does, then fails outright on a race it
cannot influence.

## Consequences

- The cost is stated rather than hidden: a job whose credentials genuinely lack the permission
  now attempts one creation before failing — naming `bigquery.tables.create`, which tells a
  reader more than the masked permission did — and if it holds `tables.create` but not the
  data-write permission it leaves behind the empty table it was authorised to create.
- Tests that used `PERMISSION_DENIED` as their unambiguous *terminal* example use
  `INVALID_ARGUMENT` instead — two on the first pass, and on the second `BufferedStreamCommitterTest`
  plus `BigQueryBufferedStreamWriterRestoreTest`, which the first pass missed because its writer
  path never consulted the verdict. No test in this module uses it that way now. Two *javadoc*
  sentences did, and were the last places a user could read the old rule — `BigQuerySinkBuilder`'s
  failure-handler doc and the `FailureHandler` SPI's in the base module, both of which now say
  `INVALID_ARGUMENT`; the base one keeps a line about why, since which codes are terminal is a
  per-connector fact rather than a general one.
- Two messages stopped asserting what the masked code cannot establish: the four "does not
  exist, creating it" logs became "may not exist", and `retryFailureMessage`'s "after creating
  the table" became "after a table-creation attempt" — a 409 means the table was already there.
  `reconcileSchema`'s own "does not exist" log is **not** in that set: it is driven by a REST
  `getSchema` returning null, which does establish nonexistence.
- Under `CREATE_IF_NEEDED` a genuine permission denial on `FlushRows` now surfaces after the
  recovery budget rather than at once, and the flush's failure message says how many attempts it
  spent rather than only that a budget ran out — the word "transient" would be wrong for the
  denial. The trade is the one the writer already makes, and it is bounded: the committer has one
  schedule, so the `scheduleFor` hazard above cannot arise here.
- `AppendErrorClassifier.isExistenceMasked` is `public` because the committer lives in a sibling
  package; `isMissingTable` stays package-private, since every caller of it is in the writer
  package and the committer only names it in prose. Both are `@Internal` either way.
- `BufferedStreamCommitter.getCreateDisposition` exists **only** so the sink's wiring of it can be
  asserted. Without it that seam is untestable short of a live client, and a `createCommitter` that
  hardcoded a disposition would ship green — the failure would appear as a real-GCP job losing a
  race it usually wins.
- The gated case that made the measurement (`BigQueryBufferedStreamMissingTableITCase`) drives
  `CreateWriteStream` **directly** for the response and through a job for the auto-creation, which
  is not decoration: a job's own recovery swallows exactly the response being measured, so a job
  alone can only tell you that auto-creation worked, never what the service said. Its end-to-end
  case is deliberately not the regression test for the committer — the window appeared in one run
  of five, so a job that fails only when the race is lost would be flaky rather than
  discriminating. `BufferedStreamCommitterTest` carries that.
- `createStream`'s allowance is **exercised**, not defensive: 45 denials across 32 of the 140 trials
  needed it, and nothing had measured that before. It does not contradict the four job runs above
  that saw no `CreateWriteStream` denial — those are eight opportunities, and at this rate eight
  clean ones arrive about one time in eight.
- The append case stays in that class, and it **asserts its append count is zero** rather than only
  printing it: the four sites are transient-only *because* of that zero, so the run that finds
  otherwise has to stop. Logging it alone would leave the observation that overturns this decision
  in a weekly job's output and nowhere else, which is the same as never having measured it. A denial
  at the stream or the flush is expected and only recorded, and the tally is logged *before* the
  assertion so a failing run still carries all three numbers. What keeps a **quiet** run from
  reading as an answer is the flush in the same trial: no append denial beside no flush denial says
  only that nothing propagated slowly that day, and at the 8% rate above one run of twenty lands
  there about 20% of the time — as one of the seven did. Every failure it meets is also held to a
  missing-table or transient verdict, so a change in what the service answers fails it too.
- What would reopen this: **one** observed masked denial from an append on either storage path earns
  a new issue naming this ADR, not a reopening of [#382] — the [#174] protocol. The four sites are
  named above so that issue can start from the list rather than rediscover it.

[#174]: https://github.com/laughingman7743/flink-connector-gcp/issues/174
[#289]: https://github.com/laughingman7743/flink-connector-gcp/issues/289
[#326]: https://github.com/laughingman7743/flink-connector-gcp/issues/326
[#318]: https://github.com/laughingman7743/flink-connector-gcp/issues/318
[#382]: https://github.com/laughingman7743/flink-connector-gcp/issues/382
