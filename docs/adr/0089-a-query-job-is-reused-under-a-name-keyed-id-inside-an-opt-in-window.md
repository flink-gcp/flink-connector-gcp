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

# ADR-0089: A query job is reused under a name-keyed id, inside an opt-in window

- Status: Accepted
- Date: 2026-08-10 (the Flink measurements ran the same day)
- Issues: [#477], [#392]
- Modules: bigquery (`source`, `source.query`), test-utils
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md` § Source

## Context / Evidence

ADR-0087 shipped the query source with a random job id and recorded why a deterministic one was
declined: derived from the query, it would re-attach the *next run of the same pipeline* to a
six-month-old completed job and read a stale result. [#477] reopened that with two ideas from
review — derive the id from the job's *context* rather than the query, and bound how far back a
re-attach may look — and closed itself "not doing it now" pending measurements. This ADR records
the design that resulted, and the measurements, three of which moved the design off the issue's
own sketch.

**The Flink JobID is not legible to users, and the issue's premise about it is half wrong.**
[#477] proposed `<job_id>` + `<operator_id>` on the premise "different submissions → different
`<job_id>`". Read in flink-clients 2.2.1 and confirmed on 1.20.4 and 2.3.0: that holds for a
session-cluster submission (`StreamGraph` constructs a random `JobID`), but in a high-availability
*application* deployment — the ordinary production shape on Kubernetes —
`ApplicationDispatcherBootstrap` derives the JobID from the HA cluster id
(`new JobID(clusterId.hashCode(), 0)`) precisely so it survives failover, and it therefore *recurs
across redeploys*. 2.3.0 moves the derivation to `ApplicationJobUtils.maybeFixIds` and makes it
unconditional in HA mode (application-id-derived, with the cluster-id hash kept for backward
compatibility), so on the newest supported minor the JobID is *never* run-separating there. Whether the JobID separates runs depends on deployment
mode, and nothing on the coordinator lets a user see which they have. An id the user cannot reason
about cannot carry the contract "this deduplicates only your failover". Keying on the **job name**
was the user's call, taken 2026-08-10, for the property the JobID lacks: the user sets it, so
"rename the job to force a fresh result" is a contract a user can act on.

**A task failure never re-runs the query at all** (measured 2026-08-10, Flink 2.2.1/2.3.0/1.20.4,
`BigQueryQueryJobIdentityITCase`): under the default `region` failover strategy *and* under
`full`, a task failure restarts tasks around a surviving operator coordinator — the enumerator,
and with it the planned query, live on. Only the global-restore path
(`OperatorCoordinatorRestoreBehavior.RESTORE_OR_RESET`) rebuilds the enumerator: a JobManager
failover, or a coordinator-reported failure. The exposure [#477] describes is therefore exactly a
JobManager failover before the first checkpoint, not "any restart", and the same ITCase pins the
other half the design rests on: `SplitEnumeratorContext.metricGroup().getAllVariables()` carries
`<job_name>` — the only route to it, since the context has no first-class job identity — with the
name the user passed to `execute(...)`, stable across the failover, on all three supported Flink
versions. Reading it is a use of the metrics system as a data channel; this paragraph is the
recorded decision the issue asked for, and it costs no API-tier entry because `getAllVariables()`
is a plain `MetricGroup` method and the `"<job_name>"` key is inlined rather than imported from
the `@Internal` `ScopeFormat`.

**Both landing places expire at about a day**: the anonymous dataset by BigQuery's own policy, the
named path by the 24-hour expiration `BigQueryQueryRunner` sets (ADR-0087). Past a day there is
nothing left to reuse — the adoption's existence check ([#485]) refuses a job whose table is gone
— so a longer window could only ever run the query again while appearing to deduplicate it.

## Decision

**Reuse is opt-in, one knob: `reuseQueryResultWithin(Duration)`.** Unset — the default — the id
stays random and every plan runs the query, which is always correct and merely sometimes paid
twice. The knob is bounded at the setter (ADR-0068's rule applied) to positive and **at most 24
hours**, for the expiration fact above — and it **requires `queryLocation(...)`**, measured rather
than reasoned: BigQuery scopes a job to (project, location, id), a `jobs.get` naming no location
sees only the US multi-region, and against the gated suite's us-central1 dataset the first version
of this path found nothing to reuse and then hit the SDK's own conflict absorber NPE-ing on its
location-less re-fetch (google-cloud-bigquery 2.68.0, `BigQueryImpl.create` line 611; measured
2026-08-10, first gated run of the reuse case). Requiring the location where the knob is typed is
what keeps both failures unreachable.

**The id is `flink_bigquery_source_<name>_<digest>_<bucket>`.** The sanitised job name is the
readable, user-controlled part; sanitisation is lossy on purpose (`my job` and `my-job` collapse)
because distinctness is the digest's to keep. The digest — sixteen hex characters of SHA-256, the
load runner's choice — covers the *original* name and **everything the runner reads**: the SQL,
the project, the location, the result dataset, and the window setting itself. Equal ids therefore mean equal jobs, up to the sixteen-hex digest — the
load runner's own precedent for deterministic ids, and a 64-bit accident two colliding jobs would
additionally need to share a sanitised name and a bucket — which is what answers [#477]'s
objection to name keying (two unrelated pipelines sharing Flink's default name still differ in
their queries) and why
`<operator_id>` stays out (two identically-configured sources in one job *should* share the one
query job; the user's call, same date). A missing `<job_name>` — a fake context, or a runtime
change — logs a warning naming the knob and falls back to the random id: the knob is a cost
optimisation, and it must not hold correctness hostage.

**The window rides in the id as a bucket, and attaching — not submitting — checks age.** The
bucket is `now / window`, so an id from an older window is never *submitted* again; that is what
makes the dead zone [#477] tabled — BigQuery holds a finished job's id for six months, far longer
than any usable window, so a runtime-checked design meets ids it can neither attach to nor reuse —
structurally unreachable. A re-plan that straddles a bucket rollover would find nothing under the
current id, so the previous bucket's id is *looked up* (never submitted) and its job reused only
if its reported creation time is inside the window — which also makes the documented window exact
rather than "up to twice N", and a job reporting no creation time is not reused. The rollover
therefore costs nothing: the straddling failover attaches across the boundary.

**Found jobs are judged as the load runner judges them, without hoisting it** (ADR-0087's
no-hoist decision stands; the shape is shared, the code is not). `RUNNING` is attached to and
polled — the concurrent-scan case, the one the anonymous dataset's cache cannot help, since a
cache serves only completed results. `DONE` without error spends one `getTable` on the result
table its metadata names — the metadata answers whether or not the table still exists, so a
vanished one is probed past exactly like a failed job and the query runs fresh ([#485]) — then
adopts the table, with the expiration backstop re-applied. `DONE` with error is probed past to
`_rN` — underscore, not the load runner's hyphen, because the id doubles as the result table's
name and a hyphen is illegal there. A create that loses a race answers HTTP 409, and the
winner's job is adopted as a reuse.
`queryJobsReattached` counts every reuse, separately from `queryJobsSubmitted`, so the latter
keeps meaning "the query was billed".

## Consequences

- A JobManager failover before the first checkpoint no longer bills the query twice on an opted-in
  source — including the worst case, two concurrent full scans of a still-running query, which no
  cache mitigates.
- **The knob's honest reading is "attempts inside a window share a result", not "failover
  deduplication".** In deployments where the pipeline redeploys under the same job name inside one
  window, the new run adopts the old run's result even though the source data may have moved; that
  is the freshness-for-cost trade the user opts into, the documentation says so, and renaming the
  job is the escape. The connector cannot distinguish a failover from a redeploy — nothing on the
  coordinator can — so the window is the only bound.
- The result table's name changes on the named-dataset path: deterministic (the job id) instead of
  random, still one-per-window rather than accumulating per attempt, and `WRITE_TRUNCATE`
  unchanged.
- `FakeSplitEnumeratorContext` gained variable injection (its `getAllVariables()` was an empty
  map, which is now also the tested fallback path), and `TestJobs` mints a creation time — two
  more package-private reaches, `Job.Builder#setStatistics` and
  `JobStatistics.CopyStatistics#fromPb`, recorded in its javadoc per ADR-0067.
- `BigQueryQueryJobIdentityITCase` stays as a permanent test, so the weekly Flink matrix
  re-verifies the `<job_name>` channel and the coordinator-recreation behaviour every supported
  version relies on.
- A result table vanished *early* — deleted by hand, or an anonymous cache table dropped inside
  its nominal day — no longer crash-loops at session creation ([#485]): the adoption's `getTable`
  probes past the job like a failed link, the query is submitted fresh under the next retry id,
  and `queryJobsSubmitted` reports the re-bill. The check reaches every `DONE` adoption — the
  current window's id, the previous window's chain, and a conflict's winner — while `RUNNING`
  jobs are exempt, their table being created only at completion. Its tests needed a
  vendor-package `Table` mint: `TestJobs` reaches the package-private `Table.Builder(BigQuery,
  TableId, TableDefinition)` constructor, recorded in its javadoc per ADR-0067 (decided with the
  user, 2026-08-10). The gated suite additionally pins the service half: `jobs.get` still
  reports the deleted table's job, metadata intact.
- Not done, deliberately: attaching across *window settings* (the setting is in the digest, so
  changing it starts fresh — simpler than migrating attachments, and it happens once per
  reconfiguration); any attempt to detect redeploys; and a `<job_id>`-keyed mode, which the
  legibility finding above argues against resurrecting without new evidence.

[#392]: https://github.com/flink-gcp/flink-connector-gcp/issues/392
[#477]: https://github.com/flink-gcp/flink-connector-gcp/issues/477
[#485]: https://github.com/flink-gcp/flink-connector-gcp/issues/485
