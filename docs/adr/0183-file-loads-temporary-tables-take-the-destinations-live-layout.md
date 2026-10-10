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

# ADR-0183: FILE_LOADS temporary tables take the destination's live layout

- Status: Accepted
- Date: 2026-10-10
- Issues: [#1671](https://github.com/flink-gcp/flink-connector-gcp/issues/1671)
- Modules: bigquery
- Current behavior: [File loads](../content/docs/connectors/datastream/bigquery.md#file-loads)

## Context

A `FILE_LOADS` commit that one load job cannot hold (more than 10,000 staged files or 11 TiB for a
destination), or a replacing commit that mixes staging formats, loads its files into temporary
tables and finishes with copy jobs ([ADR-0018](0018-file-loads-commits-deterministic-load-jobs-in-the-committer.md)).
The loads that create those tables carried no partitioning and no clustering.
[ADR-0021](0021-every-file-loads-load-reconciles-against-the-live-table-first.md) had retired
`LoadJobSpec`'s partitioning and clustering fields when table creation moved to the table admin,
because they had only ever described the destination the load created.

BigQuery refuses to copy such tables into most partitioned or clustered destinations, so the copy
failed with a terminal `invalid` error on every attempt once a commit grew past one load job.
Those destinations are the usual target of the large backfills that reach this path.

## Decision

Reconciliation already reads every destination table, and the same read now yields the table's
**layout**: its time or integer-range partitioning and its clustering.
When a copy job writes the destination (`WRITE_APPEND`, `WRITE_TRUNCATE`, `WRITE_EMPTY`) and the
destination is partitioned by a column or an integer range, or clustered, every leaf load gives
its temporary table that layout, and every intermediate table is created with it too.
The layout comes from the live table, not from `TableCreateOptions`, because a destination the sink
did not create has a layout the options do not describe, and the options cannot express range
partitioning at all ([ADR-0182](0182-bigquery-table-description-and-labels-are-creation-options.md)).

The layout leaves out the destination's partition expiration and partition filter requirement.
A copy accepts sources without them and the destination keeps its own, while a partition filter
requirement on a temporary table would refuse a `SELECT *` that read it.
A time-partitioned leaf instead gets an explicit partition expiration of 10,000 years; BigQuery
stores no timestamp before year 1, so no partition of a running commit is that old.
Without an expiration of its own, a table inherits its dataset's default partition expiration, so
a leaf in a dataset with one would drop the rows of older partitions that the destination, created
without it or with a longer one, keeps (measured).

Two cases keep unpartitioned, unclustered temporary tables, and their ids and names.
One is a destination partitioned by ingestion time alone, whose copy accepts such sources, so its
names and ids stay as an earlier version made them.
The other is `WRITE_TRUNCATE_DATA`, whose last step is a terminal query rather than a copy into the
destination, and that query writes a partitioned, clustered destination from an unpartitioned
aggregate.

A laid-out commit runs on its own temporary tables and ids.
Every temporary table's name and every job that fills one (leaf load, intermediate copy) take a
hash of the layout, and the final copy into the destination takes a fixed marker.
The reasons are three retries a deterministic plan otherwise gets wrong:

- **A retry after an upgrade.** An earlier version filled the same temporary tables without the
  layout, under the same ids, and was refused the final copy. Under the earlier ids the retry
  would re-attach to those completed jobs and be refused again. A load also cannot give an
  existing table a layout, even when it truncates the table, so new ids alone would fail on the
  tables those jobs left.
- **A job that restarted in a loop before the upgrade.** Each restart was refused the final copy
  under a fresh retry id, until none of the runner's six was left. The marker gives the final copy
  ids the earlier version never used.
- **A change of the destination's clustering between attempts.** Clustering, unlike partitioning,
  can change on an existing table. The new layout's hash gives the retry new tables and jobs, while
  the final copy keeps its id, because the marker does not depend on the layout. A final copy that
  already succeeded is then re-attached to rather than repeated.

A change that adds the destination's first clustering, or removes its last, switches a commit
between a laid-out plan and an unchanged one, whose final copies have different ids.
Each final copy therefore names the other's id as **equivalent**: before it submits, the runner
looks for a job under that id or its retry ids that has not failed, and re-attaches to it instead
of copying the rows again.
A failed one is passed over, so the earlier version's refused final copies do not stand for the
laid-out one.

A laid-out temporary table is **created ahead of the job that fills it**, with that partitioning
and clustering and a table expiration of `tempTableExpiration` from now (1 day by default, at least
1 hour), and every later attempt of the commit extends that expiration.
Two measured facts force the creation:
a load job cannot set a table expiration, and a partitioned table does not take its dataset's
default table expiration when the dataset also sets a default partition expiration.
Left to the load, an orphaned laid-out table in such a dataset would never expire, where an
unpartitioned one always had.
The owner chose an option with a one-day default over a fixed value.
Its floor of one hour is BigQuery's own floor for a dataset's default table expiration; a commit's
jobs routinely run for minutes, and a shorter expiration would only add reloads.
The jobs that write a laid-out table then never create one (`CREATE_NEVER`): a table that vanished
after its preparation fails the attempt, and the next attempt's preparation creates it with its
expiration, where a creating load would have left it without one.

An expiration means a table can vanish between attempts, while the job that filled it still
reports success.
A retry that re-attached to that job would copy the empty table its preparation recreated, and
drop that job's rows without an error.
So every job that fills a laid-out table also names the **incarnation** of that table, its
creation time: within one incarnation every retry derives the same ids and re-attaches, and a
table created anew is filled again from the staged files.
Before it prepares anything, the commit asks whether the destination's final copy already
succeeded, under its id or its equivalent one; if it did, nothing is prepared, loaded or copied into
a temporary table, and the final copy re-attaches to that job.
It asks for every destination on the temporary-table path but under `WRITE_TRUNCATE_DATA`,
laid out or not: one that lost its only clustering since an attempt whose laid-out final copy
succeeded would otherwise load its files again into its unchanged plan's tables before that copy is
re-attached to.
A copy still running is not a success: the commit then prepares, re-attaches to the loads of the
same incarnation, and attaches to the running copy, so a copy that later fails does not leave the
attempt reading tables it neither prepared nor extended.
Without that question, a destination whose final copy succeeded in an attempt that failed
elsewhere would have its expired tables recreated and reloaded on the retry, which needs staged
files a lifecycle rule may have removed, for rows already in the destination.

The tables an earlier attempt left under other names are not deleted by the commit; a laid-out one
expires on its own.
A destination whose copy needs no layout keeps its ids and names, and its temporary tables are
still created by the jobs that fill them; except under `WRITE_TRUNCATE_DATA`, it now also asks
whether its final copy succeeded under either id before its loads, up to twelve job reads, and its
final copy looks for the laid-out id before it runs, one job read, up to six past failed copies.

## Evidence

Measured on 2026-10-09 against real BigQuery, with throwaway tables in the gated dataset:

| Copy into the destination | Result |
|---|---|
| Non-partitioned source into a column-partitioned destination | Refused: "Failed to copy Non partitioned table to Column partitioned table: not supported" |
| Non-partitioned source into a range-partitioned destination | Refused with the same message |
| Non-clustered source into a clustered destination, `WRITE_TRUNCATE` or `WRITE_APPEND` | Refused: "incompatible clustering fields" |
| Non-partitioned source into an ingestion-time-partitioned destination (`DAY`, `HOUR`, `MONTH`, `YEAR`), `WRITE_APPEND` or `WRITE_TRUNCATE` | Succeeds |
| Source with the destination's column, ingestion-time or range partitioning, clustering, or both, under `CREATE_NEVER` with `WRITE_APPEND`, `WRITE_TRUNCATE` and `WRITE_EMPTY` | Succeeds; the destination keeps its layout, description and labels |
| Column-partitioned source without expiration into a destination with partition expiration and a partition filter requirement | Succeeds; the destination keeps both |

| Other step | Result |
|---|---|
| `CREATE_IF_NEEDED`/`WRITE_TRUNCATE` copy of two partitioned, clustered sources into a new table | The new table inherits the partitioning and clustering |
| `WRITE_TRUNCATE` load carrying partitioning and clustering into an existing unpartitioned table | Refused: "Incompatible table partitioning specification. Expects partitioning specification none, but input partitioning specification is interval(type:day,field:ts) clustering(region)"; clustering alone is refused the same way |
| Load into a new column-partitioned table with an expiration of 10,000, 100,000 or about 292 million years, of rows dated 1900, 1971 and 2026 | Every row is kept |
| `CREATE_IF_NEEDED` copy of a leaf with a 10,000-year expiration into a new table, in a dataset without a default partition expiration | The new table has the same expiration |
| `WRITE_TRUNCATE` or `WRITE_APPEND` copy of that leaf into a destination without an expiration | The destination still has none |
| `WRITE_TRUNCATE_DATA` query from an unpartitioned or a partitioned aggregate into a partitioned, clustered destination with a partition filter requirement | Succeeds |
| `WRITE_TRUNCATE_DATA` query from an aggregate that carries a partition filter requirement | Refused: "Cannot query over table ... without a filter over column(s) 'ts' that can be used for partition elimination" |

Measured on 2026-10-10 in a throwaway dataset with a default table expiration of one day and a
default partition expiration of 30 days, deleted afterwards:

| Step | Result |
|---|---|
| Load into a new column-partitioned, clustered table, without an explicit partition expiration, of rows dated 1971 and 2026 | The table takes the 30-day default and the 1971 row is dropped: 3 of 4 rows |
| The same load with a 10,000-year partition expiration | All 4 rows are kept |
| Either partitioned table's table expiration | None; an unpartitioned table loaded alongside takes the one-day default |
| `CREATE_IF_NEEDED` copy of the 10,000-year table into a new table | The copy takes the source's 10,000-year partition expiration, not the dataset's, and no table expiration |
| Table created with a table expiration and the layout, then loaded with `WRITE_TRUNCATE`, or written by a `CREATE_IF_NEEDED`/`WRITE_TRUNCATE` copy | Keeps its table expiration |
| `tables.patch` of only `expirationTime` on a filled table | Applied; the table is otherwise unchanged |

`BigQueryFileLoadsTempTableLayoutRealGcpITCase` holds the decision against BigQuery.
It forces each write disposition onto the temporary-table path, through one intermediate copy
level, into a column-, ingestion-time- and range-partitioned destination, a clustered one, a
partitioned and clustered one, one with a partition expiration and a partition filter requirement,
and one the commit creates from `TableCreateOptions`.
Each keeps its layout, description and labels, and the expiring one its expiration.
Without the layout on the leaf loads, every disposition but `WRITE_TRUNCATE_DATA` fails with the
refusal above.
Four further cases replay a retry.
In one, an earlier version's leaf loads and intermediate copy run first under the planned ids
without the layout, and its final copy is refused until no retry id is left; the commit then
succeeds.
In the second, this version's own leaf loads run first, into the tables its preparation created
with their expiration, and the commit re-attaches to them with the staged files already deleted.
In the third, one of those tables is deleted, as its expiration would, and the commit fills it
again and loads every row; without the incarnation in the job ids the same commit succeeded with 2
of 3 rows, the deleted table's row silently missing.
In the fourth, a commit that succeeded runs again with its staged files deleted: it re-attaches to
its final copy alone, and the destination still holds each row once.

Measured on 2026-10-10 with a throwaway service account (impersonated, no key) holding BigQuery Job
User (`roles/bigquery.jobUser`, which carries `bigquery.jobs.create`) on the project and,
conditioned to a throwaway dataset alone, a custom role of `bigquery.tables.create`, `updateData`,
`get`, `getData` and `delete`; then the same role plus `bigquery.tables.update` as the control.
Afterwards, listing confirmed the account, the dataset and the bindings were gone and the custom
roles soft-deleted.

| Step into the throwaway dataset | Without `bigquery.tables.update` | With it |
|---|---|---|
| `tables.insert` of a table with an expiration | Succeeds | Succeeds |
| Copy into a new table, `CREATE_IF_NEEDED`/`WRITE_TRUNCATE` | Refused: "Permission bigquery.tables.update denied" | Succeeds |
| Copy into an existing table, `CREATE_NEVER`/`WRITE_TRUNCATE` | Refused the same way | Succeeds |
| Copy into an existing table, `CREATE_NEVER`/`WRITE_APPEND` | Succeeds | Succeeds |
| Load into a new table, `CREATE_IF_NEEDED`/`WRITE_TRUNCATE` | Refused the same way | Succeeds |
| Load into an existing table, `CREATE_NEVER`/`WRITE_TRUNCATE` | Refused the same way | Succeeds |
| `tables.patch` of only `expirationTime` | Refused the same way | Succeeds |

A second probe the same day, set up the same way, with the account re-created, new roles and a new
dataset, measured the destination-side jobs that the table above leaves out.
Each job ran against a table created beforehand, every refusal names `bigquery.tables.update`, and
listing confirmed the resources were removed afterwards in the same way.
With `bigquery.tables.update` every step succeeded, the column addition included.

| Step into an existing table | Without `bigquery.tables.update` |
|---|---|
| Load, `CREATE_IF_NEEDED`/`WRITE_APPEND` | Succeeds |
| Load, `WRITE_APPEND` with `ALLOW_FIELD_ADDITION` and `ALLOW_FIELD_RELAXATION`, rows of the table's schema | Refused |
| Load, `WRITE_APPEND` with `ALLOW_FIELD_ADDITION`, adding a column | Refused |
| Load, `CREATE_IF_NEEDED`/`WRITE_EMPTY`, empty table | Succeeds |
| Load, `CREATE_IF_NEEDED`/`WRITE_TRUNCATE_DATA` | Succeeds |
| Load, `WRITE_TRUNCATE_DATA` with both schema update options | Refused |
| Copy, `CREATE_NEVER`/`WRITE_EMPTY`, empty table | Succeeds |
| Copy, `CREATE_NEVER`/`WRITE_APPEND` | Succeeds |
| `SELECT *` query, `CREATE_NEVER`/`WRITE_TRUNCATE_DATA` | Succeeds |
| `SELECT *` query, `WRITE_TRUNCATE_DATA` with both schema update options | Refused |

A third probe the same day, set up the same way again, sent each option alone with rows of the
table's schema: a `WRITE_APPEND` load and a `WRITE_TRUNCATE_DATA` `SELECT *` query, each with only
`ALLOW_FIELD_ADDITION` and each with only `ALLOW_FIELD_RELAXATION`.
Without `bigquery.tables.update` all four were refused, naming that permission, and with it all four
succeeded.
Its cleanup listing found the dataset and the policy bindings gone and the roles soft-deleted, but
still listed the just-deleted account; a re-check minutes later no longer did.
A fourth probe, set up the same way, sent the remaining combination: a `WRITE_TRUNCATE_DATA` load
with rows of the table's schema, once with only `ALLOW_FIELD_ADDITION` and once with only
`ALLOW_FIELD_RELAXATION`.
Both were refused without `bigquery.tables.update`, naming it, and both succeeded with it; its
cleanup listing found the account, dataset and bindings gone and the roles soft-deleted.

The gated dataset sets a 24-hour default table expiration but no default partition expiration, so
the suite shows the explicit expirations are accepted and kept; the losses they prevent are
reproduced only by the throwaway dataset with default expirations above.

## Consequences

A commit into a destination on this path, laid out or not, first makes up to twelve `jobs.get` to
learn whether its final copy already succeeded, except under `WRITE_TRUNCATE_DATA`; if it did, only
the final copy's own lookup and re-attachment and the cleanup follow.
Otherwise a laid-out destination makes, one after another before any of its jobs, one
`tables.insert` per temporary table, followed by a `tables.patch` of that table on an attempt that
finds it already created; a destination that needs no layout prepares nothing, and its loads create
its temporary tables as before.
The patch needs `bigquery.tables.update` on the temporary dataset, which this path already
needed: BigQuery refuses every `WRITE_TRUNCATE` load or copy, and so every leaf load and every copy into
a temporary table, without it (measured above). BigQuery Data Editor, which the docs recommend,
includes it; the docs' permission list had left it out.

A job stopped for longer than `tempTableExpiration` while a commit is pending loses its laid-out
temporary tables, and the restored commit reloads them from the staged files, unless the final copy
already succeeded; it fails only if a staging bucket lifecycle removed those files first.
One case is left: if only some of a destination's temporary tables expired and the reconciled
schema changed between attempts, the reloaded tables take the new schema while the re-attached ones
keep the old, and the copy, which needs identical schemas, is refused on every attempt until the
remaining tables expire or are deleted. It needs both a schema change and a partial expiry within
one pending commit, and it fails loudly rather than losing rows.

## Alternatives declined

- **Reading the layout from `TableCreateOptions`.** It describes only tables the sink creates and
  has no range partitioning, so a pre-existing destination would still be refused.
- **Keeping temporary-table names and deleting a table found with another layout.** It was built
  and measured first. Whether to delete rests on comparing the layout BigQuery reports for a table
  with the destination's, and a table an attempt under the current layout filled, if it compared
  unequal for any reason, would be deleted while its completed load was re-attached to, so the copy
  would never find its source. It also costs a read of every planned temporary table and needs
  `bigquery.tables.delete`.
- **Keeping the final copy's id.** A job that restarted in a loop before the upgrade can have used
  every retry id of that copy, and the upgraded commit could then never submit it.
- **Leaving laid-out tables to the dataset's default table expiration.** In a dataset that also
  sets a default partition expiration they would never expire, where every temporary table used to.
- **Setting the expiration after the job, by `tables.patch`.** One call fewer than creating the
  table, but an attempt that stopped between the job and the patch would leave a table without
  one.
- **Job ids without the table's incarnation.** A table that expired before a retry would be
  recreated empty, and the retry would re-attach to the job that filled its predecessor and copy
  nothing from it.
- **A final copy without an equivalent id.** It was pushed first. A clustering added or removed
  between an attempt whose final copy succeeded and its retry would copy the rows again under
  `WRITE_APPEND`, and wedge `WRITE_EMPTY` on a table that is no longer empty.
- **Laying out an ingestion-time-only destination too.** Its copy needs no layout, so laying it
  out would only give it new names and ids.
- **Finishing every disposition with a query, as `WRITE_TRUNCATE_DATA` does.** A query accepts an
  unpartitioned source, but it scans every column of the aggregate and is billed for it, where a
  copy within one location is free.
- **Giving `WRITE_TRUNCATE_DATA`'s temporary tables the layout too, for one rule.** Its query works
  without it, so the layout would add new names and ids to a path that was not broken.
