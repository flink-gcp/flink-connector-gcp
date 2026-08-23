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

# ADR-0087: A query source runs one query job, and the caller chooses where its result lands

- Status: Accepted
- Date: 2026-08-10 (BigQuery measured the same day)
- Issues: [#392], [#64]
- Modules: bigquery (`source`, `source.query`, `source.enumerator`)
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md` § Source

## Context / Evidence

[#390] built the source over the Storage Read API, and that API reads storage. Google's own
documentation states the consequence: "Because the Storage Read API operates on storage, you cannot
use the Storage Read API to directly read from logical or materialized views." So the connector
could not read a view at all, and in BigQuery deployments a view is the ordinary governed access
layer. Google's documented workaround is this issue — run the query, read its result.

[#392] carried a recorded caution against building it: the Dataproc `flink-bigquery-connector`
*removed* its bounded query source in 1.0.0, so the value/maintenance trade-off was to be weighed
again first. Weighed 2026-08-10, that premise turned out to be incomplete rather than wrong:

- PR #212 (2025-02) removed the unbounded source **and** the bounded query source in one change,
  with no reason stated beyond "Remove unbounded and query source abilities."
- PR #307 (2026-05) merged `isView` and `materializeView` — the query-job-into-a-table machinery,
  back, narrowed to views, hooked into `SplitDiscoverer` at the point this connector calls `plan()`.
- PRs #331 and #333 are open, extending that to their Table API.

The general form was removed and the machinery returned within fifteen months for the case with no
alternative. `CREATE TABLE ... CLONE` was considered as a cheaper substitute and does not apply:
"You can't create a clone of a view or a materialized view", and a clone's source must be a standard
table, which this source already reads directly. Table snapshots hit the same wall.

Measured against BigQuery on 2026-08-10 (project `flink-gcp`, a 20-row table), because the choice of
where a result lands rested on two documented cautions rather than on evidence:

- **A query submitted with no destination table lands in an anonymous table the Storage Read API
  reads.** Five distinct queries, each a genuine cache miss and so each its own anonymous table,
  every one answering `streams=1` with the expected row counts. **No zero-stream session was seen**,
  which is what Google's own Storage Read API sample warns can happen.
- **An identical re-run is a cache hit onto the same table**: `cacheHit=true`,
  `totalBytesBilled=0`, the same `anon…` id.
- The control arm — the same query with an explicit destination — also answered `streams=1`.
- **A logical view and a materialized view fail identically**: `INVALID_ARGUMENT: request failed:
  non-table entities cannot be read with the storage API`, the same code *and* the same words, so
  one mapping covers both.

What that run does **not** establish, and the record says so rather than implying otherwise: six
trials on one small table in one project on one day do not refute the zero-stream warning; the
anonymous tables reported `estimatedTotalBytesScanned` of 0 against 250 for the named one, which is
unexplained and therefore supports no billing claim; multi-stream splitting of an anonymous table is
unmeasured; and "a result above the maximum response size is not kept as a cached result" is taken
from Google's documentation, not measured here.

## Decision

**A query source is `query(...)`, and nothing infers one.** `table(...)` and `query(...)` are
alternatives at `build()`. A `table(...)` naming a view fails, and the failure gains a sentence
naming `query(...)`.

**View materialization ships as `materializeViews()`, opt-in and off by default** — the shape the
reference converged on (`viewsEnabled` in Spark and in the Dataproc connector), gated the same way.
With it, the source asks BigQuery once what the configured name is and materializes a view through
the same query path; an ordinary table is read directly. The two costs are exactly why it is a knob
and not a default: it spends a metadata call, and ADR-0079 named "the source makes no REST call" as
a property to revisit deliberately rather than to erode — a source pointed at a table must not pay a
round trip to be told it is a table — and it bills a query nobody wrote, which is a thing to ask for
rather than to inherit. Making it *unconditional* was declined for the first of those; both survive
as the argument for the default rather than against the feature.

**Detecting a view reactively — catching the `INVALID_ARGUMENT` and materializing then — was
declined outright**, and this one is a rule rather than a trade. It would cost no metadata call at
all, which makes it look strictly better; what it actually does is turn the message-text match below
into a *routing* decision, which is precisely what this module's match-status-codes rule
(ADR-0030) exists to prevent. A rewording by BigQuery would silently stop materializing views
instead of silently dropping a hint.

**The generated `SELECT` folds `selectedFields` and not `rowRestriction`.** A view's `SELECT *`
scans every column and the query is billed for the scan, so leaving the projection to the read
session would prune the transfer after paying for it. The restriction is BigQuery's own syntax and
not a SQL `WHERE`; folding it would give one knob two meanings depending on what the source was
pointed at. The rule that decides both: **the connector folds into SQL it wrote, never into SQL the
user wrote** — which is also why nothing is folded into `query(...)`. `snapshotTime` is rejected
beside `materializeViews()`, because a view's result table is created by that job and has no earlier
version, and the failure would otherwise land at session creation rather than where the value was
typed.

**Both landing places ship, and the knob is what selects them.** Unset, the query is submitted with
no destination table and BigQuery writes the result into its anonymous dataset, expires it and
charges no storage — nothing created, nothing to clean up, and a re-plan is a free cache hit onto
the same table. That is the default because it needs no configuration, not because it is the safer
of the two; its constraints are real and documented on the connector page, and they are Google's
(owner-only access, the advice against depending on a cached results table, the response-size
limit). `queryResultDataset(...)` opts into a table the connector creates, with a one-day
expiration. Deciding this by measurement alone and shipping one path was the earlier plan; shipping
both is the user's call, taken 2026-08-10.

**Nothing deletes a result table at teardown.** A source's teardown also runs on a JobManager
failover, where the restored job goes on reading the read session that table backs — so a delete
there would break the recovery it appears to tidy after. The expiration is the cleanup, and it
cannot cut a read short: a session lasts six hours and a bounded read must finish inside that.

**The query runs from `plan()`, guarded by the same checkpointed flag as the read session.** A
restore adopts the session the first plan created and runs no query. Nothing after planning can
tell the two kinds of source apart — a split names a stream, and `ReadClientRowStreamOpener` opens
by stream name — so **neither the enumerator state nor its serializer changed**, and neither did the
split's. `ReadSessionRequests.of` takes the resolved table as a parameter, which is the one point
where the two converge.

**The job id is random by default and no previous attempt is re-attached to then**, the opposite
of `BigQueryLoadJobRunner`'s choice. A deterministic id derived from the query would re-attach the
second run of the same pipeline to the first run's completed job — BigQuery keeps a job's metadata,
and so its id, for six months — and read a stale result. What buys only the window between
submitting the query and the first checkpoint is deduplicating a *failover*: there a re-plan is a
cache hit on the anonymous path and a second expiring table on the named one, unless the first
query is still running, where nothing mitigates the doubled scan. `queryJobsSubmitted` reports it.
**The opt-in that closes that window is ADR-0089** ([#477]): an id keyed on the Flink job name and
a digest of the query configuration, valid inside a configurable window that rides in the id — the
shape that keeps the six-month retention from ever answering `ALREADY_EXISTS` for a job too old to
attach to, with the load runner's probing kept for failed ids only.

**The polling has no attempt bound**, matching the load runner, and here the service is what makes
it terminate: BigQuery ends a query job at its own execution limit, so the job reaches `DONE` either
way. A bound short enough to be useful would abandon a legitimately long query — the opposite of the
problem ADR-0084 solved for `ReadRows`, where nothing on the service side ever ended anything.

**The view hint matches message text, and that is argued rather than overlooked.** This module's
rule is to match status codes and never message text (ADR-0030), and `INVALID_ARGUMENT` alone
identifies nothing here — a bad projection, an unparsable row restriction and an out-of-window
snapshot all answer with it. What makes the text acceptable is that the result is only a sentence
added to a failure being thrown either way, never a decision about retrying, dropping or routing,
which is what that rule protects. A rewording costs the hint and nothing else.

**`QueryRunner` is not `AutoCloseable`.** `com.google.cloud.bigquery.BigQuery` extends
`com.google.cloud.Service` and nothing else — there is no `close` (verified against
google-cloud-bigquery 2.68.0, what `libraries-bom` resolves), which is also why `TableAdmin` on the
sink side is not closeable. A closeable seam here would have to be composed into the enumerator's
single planner for no released resource.

**The load runner's job machinery is not hoisted.** `BigQueryQueryRunner` follows its shape —
submit, poll through `BigQuery#getJob` and never `Job#reload()` (ADR-0018), an `IOException`
contract — but the load runner's `-rN` probing, its `activeJobs` map and its copy jobs exist for the
sink's exactly-once commit. Sharing would mean refactoring that path for a caller that needs none of
it. The one thing that *is* shared is `BigQueryTableAdmin.emulatorOptions`, widened to public: its
argument for requiring a project id is one paragraph that would otherwise be restated.

## Consequences

- A view is readable, which it was not. That is the change users will name.
- **The source now makes a REST call, on the query path only**, so it takes a second emulator
  endpoint — `emulatorRestEndpoint(...)`, the sink's spelling. This is the revisit ADR-0079 asked
  for, and the answer is that the property holds for a `table(...)` source and is given up only
  where a query job makes it impossible to keep.
- A query source is billed twice, and the connector page says so beside the existing read-cost note.
  `selectedFields` and `rowRestriction` apply to the *result*, so they cannot make the query
  cheaper; `snapshotTime` is rejected beside a query rather than ignored, since the result table is
  created by the query and has no earlier version.
- The query path is covered against BigQuery and nowhere else: the emulator's answer to a
  destination-less query is not the service's, and where the result lands is BigQuery's mechanism
  rather than an API this connector drives. The planning behaviour around it is unit-tested against
  a scripted runner and needs no service.
- A named-dataset run leaves a table behind until its expiration. That is a cost the anonymous path
  does not have, and it is the reason the anonymous path is the default.
- **No dry run**, which [#392]'s scope named as part of Beam's shape. Beam dry-runs for three
  things this source does not need: the schema (the read session carries it — ADR-0079), validation
  (the query job itself rejects an invalid query, and its message is what the failure carries), and
  a size estimate (nothing here decides anything from one). A dry run is a second job per read for
  no answer this connector consumes.
- **No dedicated temp dataset**, the other half of that shape: the two landing places above replace
  it, and neither creates a dataset. Beam's eager `PassThroughThenCleanup` has no counterpart
  either — Flink gives a source no post-read hook, and the teardown that does exist runs on a
  failover too.
- `materializeViews()` makes the source's one metadata call, so its emulator coverage needs the
  REST endpoint too, and `isView`'s round trip is covered by the gated case rather than by a unit
  test: `Table` has no constructor reachable outside the vendor's package, and minting one would
  need a second helper there (ADR-0067). What *is* unit-tested is the decision — which table types
  count as a view — split out for exactly that reason.
- Not done, and deliberately: a `queryResultProject`, so the result dataset must live in
  `parentProject`; and any Table API surface, which follows the [#57] pattern when it comes.

[#57]: https://github.com/flink-gcp/flink-connector-gcp/issues/57
[#64]: https://github.com/flink-gcp/flink-connector-gcp/issues/64
[#390]: https://github.com/flink-gcp/flink-connector-gcp/issues/390
[#392]: https://github.com/flink-gcp/flink-connector-gcp/issues/392
[#477]: https://github.com/flink-gcp/flink-connector-gcp/issues/477
