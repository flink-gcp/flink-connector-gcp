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

# ADR-0077: Batch limits are counted in index-aware cells, read once when the writer opens

- Status: Accepted
- Date: 2026-08-09 (client library facts read in google-cloud-spanner 6.119.0; schema read verified
  2026-08-09 against `gcr.io/cloud-spanner-emulator/emulator:1.5.56`, both dialects), revised by
  [#435] (2026-08-09)
- Issues: [#220], [#435]
- Modules: spanner (`sink`, `sink.writer`)
- Current behavior: `docs/content/docs/connectors/datastream/spanner.md` § Batching, and
  `docs/content/docs/reference/spanner.md`

## Context / Evidence

A request Spanner refuses is refused as a whole, taking every mutation in it with it. So a sink that
accumulates mutations has to know how much of the budget it has spent before it sends — and has to
know which budget.

**What the documentation actually says.** The quotas page carries four rows that could bear on a
batch write request:

| Row | Value |
|---|---|
| Mutations per commit (including indexes) | 80,000 |
| Commit size (including indexes and change streams) | 100 MiB |
| Mutations per **mutation group** in a batch write request | 80,000 |
| Request size other than for commits | 10 MiB |

and the batch write page adds exactly one sentence about limits — *"The maximum size for a batch
write request is the same as the limit for a commit request"* — which is about **size**, and states
no figure of its own.

Three things follow, and they are narrower than the batch-write page alone suggests:

- **No per-request mutation count is documented for batch write.** The 80,000-per-commit row governs
  `Commit`, which this sink does not use. The only mutation figure naming batch write bounds one
  *mutation group*.
- **A one-mutation-per-group sink reaches that per-group figure only through a single mutation.**
  A range delete over a table carrying secondary indexes costs one mutation for the table plus one
  per index *per row it matches* — see the delete discussion below — so it is reachable, but never
  by accumulating, and not at all on a table with no secondary index.
- **The request size cap is 100 MiB — measured, and the documentation could be read either way.**
  The batch write page's sentence, read as a carve-out, puts batch write on the 100 MiB commit row;
  the "request size other than for commits" row, read literally, puts it on 10 MiB, and the quotas
  page has no batch-write size row to break the tie. [#441] measured it against the service
  (2026-08-10, `SpannerRejectionRealGcpITCase`): a request of roughly 12 MiB is accepted, and one of
  roughly 110 MiB is refused with `RESOURCE_EXHAUSTED: SERVER: Received message larger than max
  (115350024 vs. 104857600)`. 104,857,600 is 100 MiB exactly, so the carve-out reading holds and
  `MAX_BATCH_BYTES_LIMIT` needed no change. Two things the refusal shows beyond the number: it is a
  **transport-level** refusal rather than a quota `INVALID_ARGUMENT`, and it arrives under
  `RESOURCE_EXHAUSTED`, which this connector classifies as transient — so a request that breaches
  the ceiling is retried rather than failed. Nothing reachable through the public builder can
  produce one, since `maxBatchBytes` is bounded at the same 100 MiB and the estimate would have to
  undercount by the whole margin between the configured value and the ceiling; it is recorded here
  because the margin narrows as `maxBatchBytes` is raised toward its own bound.

The defaults below are Apache Beam's, and **Beam batches for `Commit` rather than for batch write**,
which is where the commit-shaped figures entered this connector. They sit far under every reading of
every row all the same.

How Spanner counts is not how a naive reader would: "insert and update operations count with the
multiplicity of the number of columns they affect, and primary key columns are always affected";
a delete counts as one "regardless of the number of columns affected"; and in both cases every
secondary-index entry the write changes is counted individually. The index part is not derivable
from a `Mutation` — it is a property of the schema.

Two client-library facts constrain what can be measured at all (checked in 6.119.0):

- **There is no public route from a `Mutation` to its wire form.**
  `Mutation.toProtoAndReturnRandomMutation`, `Value.toProto()`, `Key.toProto()` and
  `KeySet.appendToProto` are all package-private. So the byte size of a mutation cannot be
  measured, only estimated — which is what Apache Beam's `MutationSizeEstimator` has done for
  years, and why this module has its own.
- **`Mutation.toString()` truncates every string value at 36 characters**
  (`Value.MAX_DEBUG_STRING_LENGTH`), so the debug rendering cannot stand in for the wire form
  anywhere — not for sizing, and not for the dead-letter payload.

## Decision

**Three limits, counted three ways, checked before a mutation joins the batch.**

- `maxBatchCells` (default 5,000) is counted the way Spanner counts: a written column costs one
  cell for the table plus one for each secondary index that contains it — as a key column or as a
  `STORING` one, since both rewrite an index entry — and a delete costs one plus the table's index
  entries.
- `maxBatchMutations` (default 500) and `maxBatchBytes` (default 1 MiB) are the other two. The byte
  figure is an estimate over the public accessors, documented as such on the setter; a type the
  client library adds later is counted at a fixed fallback rather than rejected, because a new
  Spanner type must not stop a running job.
- All three are Beam's long-proven values, read from its code rather than its prose: `SpannerIO`'s
  `DEFAULT_BATCH_SIZE_BYTES` (1 MiB), `DEFAULT_MAX_NUM_MUTATIONS` (5,000) and `DEFAULT_MAX_NUM_ROWS`
  (500), verified 2026-08-09 on Beam master and on the v2.68.0 tag. Beam's own javadoc for
  `withMaxNumRows` says 1,000, contradicting its own constant — so a reviewer checking Beam's
  documentation rather than its source will read this as wrong. The cell default sits **16 times**
  under the 80,000 ceiling, and that headroom is load-bearing rather than decorative — it is what
  absorbs a table whose indexes the writer could not read, and what would keep an undercounted batch
  under a per-request mutation count if Spanner enforces one it does not document.
- **Deletes are counted differently from Beam, deliberately.** Beam charges a point delete the sum
  of *every* column's weight and a range delete zero (`MutationCellCounter`: "There is no clear way
  to estimate range deletes, so they are ignored"). This sink charges both one plus the table's
  index entries, which is what Spanner's own documentation describes — "delete and delete range
  operations count as one mutation regardless of the number of columns affected", plus the index
  entries individually. Beam's point-delete figure over-counts and its range-delete figure
  under-counts without bound; one plus the indexes is the single-row truth, and it is the *exact*
  truth for a range delete on a table with no secondary index, since the quotas page counts the
  table part once however many rows a range matches. Where there are indexes it undercounts: the
  page's own worked example is "1 mutation for the table, plus 2 mutations for each row" on a table
  with two indexes, and how many rows a range matches is not something client-side can know.
- The check runs **before** a mutation is added, so a batch only ever exceeds a limit when a single
  mutation does so on its own. Refusing that one here would be this connector inventing a limit;
  Spanner's own refusal names the real one better. A range delete is the one way this sink can
  reach the per-mutation-group 80,000, and it is exactly such a single mutation.

**All three are bounded at the setter** ([#435]), as `maxCommitDelay` already was — a `*_LIMIT`
constant, a message naming what the figure is, and a reject/accept test pair. `maxBatchBytes(512L * 1024 * 1024)` built fine before them, and a job so
configured then dies on a task manager, one request-level refusal per batch — which
`SpannerErrorClassifier` calls `FATAL`, since a failure of the request names no mutation — for a
mistake that was visible at submission.

Two qualifications the guard's own framing has to carry, or it claims more than it delivers:

- **The three limits are ANDed**, and a batch flushes on whichever binds first. Raising one alone
  usually changes nothing — `maxBatchCells(500_000)` against the default 1 MiB and 500 mutations
  produces exactly the batches the defaults did. The failure needs the knob that binds to be the
  one raised.
- **Only the byte ceiling defends a refusal Spanner documents.** Whether the service refuses a
  request over 80,000 mutations is not documented either way, so the cell ceiling is precautionary:
  it holds a batch to the only mutation figure Spanner publishes for this RPC. It is worth having
  for that reason and for the headroom argument above, not because a refusal at 80,001 has been
  seen.

- `MAX_BATCH_CELLS_LIMIT` is **80,000**: the per-mutation-group row, taken as the request-level
  ceiling because no request-level count is documented at all, so a batch under it is under every
  row that could apply.
- `MAX_BATCH_BYTES_LIMIT` is **100 MiB**, chosen as the looser of the two readings before the
  measurement and **confirmed by it** ([#441], 2026-08-10): the service refuses at exactly
  104,857,600 bytes and accepts well above 10 MiB, so the looser reading was the right one and the
  constant did not move. Choosing it that way was still the right call at the time — a bound at the
  looser reading rejects only what is illegal under both, so it could not refuse a legal
  configuration whichever way the measurement came out, where a bound at 10 MiB would have been a
  decision taken without it. The ceiling remains
  a guard against a misconfiguration rather than a value to set — the estimate still reads low by
  the framing it ignores (about sixty bytes a mutation, measured), so a request built at exactly the
  ceiling is over it on the wire.
- **A `BYTES` value is counted at its base64 length, and that is the one place the estimate does not
  count a value as itself.** Measured 2026-08-10: 83,886,080 raw bytes were refused at 111,852,884
  received — four thirds, because a Spanner value travels inside a `google.protobuf.Value`, which
  has no bytes kind. Counting the raw length made the estimate read a **quarter** low for a
  `BYTES`-heavy batch, far more than the framing gap absorbs, and the failure it enabled is not a
  clean one: an over-sized request is refused under `RESOURCE_EXHAUSTED`, which this connector
  classifies as transient, so it is retried until the budget runs out rather than failed with a
  reason. Splitting `RESOURCE_EXHAUSTED` by its message text to fix that end instead is ruled out by
  ADR-0076's rule, which is why the fix is at the estimator.
- `MAX_BATCH_MUTATIONS_LIMIT` is **derived — `= MAX_BATCH_CELLS_LIMIT`, not a second literal**.
  Every mutation costs at least one cell (`CellWeights.weigh` returns at least one for every
  operation), so a batch never holds more mutations than cells, and a value above the cell ceiling
  names a batch that cannot exist. The derivation is the point: writing 80,000 twice would leave
  this ceiling cutting below what a batch may legally hold the moment the cell ceiling moved —
  rejecting not a meaningless value but a correct one, with nobody re-checking. The test pins the
  equality rather than the figure.
- **A value legal at the ceiling but above the *configured* `maxBatchCells` is warned about, not
  rejected.** It cannot take effect either — the cell cap is reached first however cheap the
  mutations are — but the configuration works, so refusing it would reject something harmless while
  saying nothing would leave a user believing they had capped a batch by count. `build()` logs it,
  naming both values. That is the only log statement in an options class in this repository, and
  the argument for it is that nothing else could carry the information: no exception is due, and
  the value the user typed is kept unchanged.
- **The warning suggests no remedy**, which is deliberate. "Lower it below `maxBatchCells`" would be
  false — whether the count cap binds depends on what each mutation costs in cells, not on the two
  knobs' order, so a user who lowers it by one silences the warning and has still not capped by
  count. "Raise `maxBatchCells`" spends the headroom that absorbs a schema the writer could not
  read. A log line that names the situation is worth more than one that prescribes a fix that is
  wrong half the time.
- **It is emitted from `build()`, not from the writer**, which would repeat it once per subtask.
  That is normally the job's main method — **but not only**: initializing this class runs
  `DEFAULTS = builder().build()`, and a task manager holding a deserialized instance has
  initialized the class. What keeps the line off a task manager is therefore not the mechanism but
  the defaults, which do not satisfy the condition; `SpannerWriterOptionsTest` pins that rather than
  leaving it to chance. Where it lands is the client log under `flink run`, the JobManager log in
  application mode, and the console in an IDE — the docs say so rather than promising the user will
  see it.
- Both constants are **package-private**, with the figure named in the setter's `@param` rather than
  linked as a symbol — `OptionChecks`' rule ("widen it when something asks, not before"). The
  argument that a public compile-time constant is inlined into every caller was pointed at the byte
  one in particular while [#441] might have lowered it; the measurement left it where it was, and
  the rule stands on its own.
- These are guardrails against a misconfiguration, not the thing that keeps a default-configured
  request legal — the defaults sit two to three orders of magnitude below both.

**The index coverage is read once, when the writer opens**, with a dialect-branched query over
`INFORMATION_SCHEMA.INDEX_COLUMNS` — GoogleSQL scopes the default schema at the empty catalog and
empty schema, PostgreSQL at schema `public`. The primary-key index is excluded by name
(`PRIMARY_KEY` in both dialects, the filter Beam has shipped against both the service and the
emulator), since its cells are already counted by the columns themselves.

- Reading at writer creation rather than lazily makes a database whose schema the sink cannot read
  a job that never starts, instead of one that dies at its first record.
- Names are matched case-insensitively. Spanner will not let two tables, or two columns of one
  table, differ only in case, so folding costs nothing — and it stops a serializer that spells a
  table `orders` where the schema says `Orders` from silently losing its index weights.
- **A table the weights do not know is counted without its index entries**: one created after the
  writer opened, or living in a named schema rather than the default one. That undercounts, and
  the headroom above is the answer. It is not an error, because the alternative — failing a job
  over a table that exists and works — is worse.
- A dialect the client library adds later throws rather than reading nothing, since reading
  nothing would silently undercount every mutation of every table.

## Consequences

- The sink needs read access to the database's `INFORMATION_SCHEMA` as well as write access —
  `roles/spanner.databaseUser` covers both. The quickstart's permission table says so.
- Raising `maxBatchCells` toward its 80,000 ceiling removes the headroom that makes an unknown table
  safe. The setter's javadoc and the reference page both say what the number counts.
- **How large a batch write request may be is 100 MiB**, measured by [#441] once the gated
  real-GCP suite ([#224]) existed to measure it in. The byte ceiling therefore did not move, and the
  ambiguity is gone from this ADR, the setter's javadoc and both docs pages. What the measurement
  added rather than removed: the refusal is a transport one under `RESOURCE_EXHAUSTED`, a status
  this connector retries, so the ceiling is not a fail-fast boundary.
- The dead-letter payload cannot be a protobuf either, for the same missing-conversion reason. It
  is the Java-serialized `Mutation` — `Mutation`, `Value`, `Key` and `KeySet` each declare a
  `serialVersionUID`, so it is an affordance the library maintains — and `FailedMutation` exposes
  `getMutation()` for a handler that wants the object. `FailedMutationTest` round-trips a
  500-character value through the payload, and pins the truncation that rules out the alternative.

## Alternatives declined

- **Counting columns only, without index entries.** Cheaper by the whole schema read, and with a
  5,000 default it takes roughly fifteen covering indexes per written column to breach 80,000 — so
  it would rarely bite. Declined because it breaks exactly where a user raises the limit, which is
  the moment they most need the count to be true, and because it would make `maxBatchCells` mean
  something different from what Spanner's documentation means by the same word.
- **Building the wire proto ourselves to size mutations exactly.** It needs a `Value` → protobuf
  conversion for every Spanner type, arrays and structs included — some 250 lines duplicating SDK
  internals, silently wrong the first time Spanner adds a type. The estimate plus a 10- to 100-fold
  gap to the real limit is the better trade; Beam reached the same one.
- **Waiting for [#441] before bounding `maxBatchBytes` at all**, which is the order [#435] proposed.
  Declined once the readings were laid out: the looser bound is correct under both, so waiting buys
  no accuracy and leaves the knob unbounded until a gated suite that does not exist yet does.
- **Dropping the cell ceiling too** once it was clear that 80,000 is not a request-level figure.
  Declined: it is the only mutation figure Spanner publishes for this RPC, a batch under it is under
  every row that could apply, and the alternative is no guard at all on a knob that can drive a
  request past its size limit.
- **Leaving `maxBatchMutations` unbounded**, on the ground that a ceiling there cannot bind — which
  is true, and was the shape this change carried for a round. Declined for two reasons that only
  become visible once written down: a wrong value is worth refusing whether or not it is *harmful*,
  since it always means a misunderstanding; and the argument for leaving it out (that the ceiling
  would go stale if the cell one moved) is an argument for **deriving** the constant, not for
  omitting it. Deriving it keeps both properties.
- **Rejecting `maxBatchMutations` above the configured `maxBatchCells`** instead of warning.
  Declined: unlike a value above the ceiling, that one describes a batch that *can* exist and a job
  that works — the cell cap simply decides every flush. Refusing it would reject a harmless
  configuration, and clamping it would silently change a value the user typed.
- **Refreshing the weights periodically.** A schema change during a job is real, but the failure it
  causes is an undercount inside a 16-fold headroom, not a wrong answer. Reopen if a measurement
  shows the undercount reaching the limit.

[#220]: https://github.com/laughingman7743/flink-connector-gcp/issues/220
[#224]: https://github.com/laughingman7743/flink-connector-gcp/issues/224
[#435]: https://github.com/laughingman7743/flink-connector-gcp/issues/435
[#441]: https://github.com/laughingman7743/flink-connector-gcp/issues/441
