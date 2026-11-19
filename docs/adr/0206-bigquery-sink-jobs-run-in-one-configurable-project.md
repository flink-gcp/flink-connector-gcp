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

# ADR-0206: BigQuery sink jobs run in one configurable project, and FILE_LOADS state records it

- Status: Accepted
- Date: 2026-11-18
- Issues: [#2178](https://github.com/flink-gcp/flink-connector-gcp/issues/2178)
- Modules: bigquery
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md` § File loads (Job
  project); `docs/content/docs/connectors/table/bigquery.md` § Sink options

## Context

FILE_LOADS ran its load, copy and terminal query jobs in the environment's default project.
Its committer built its client with `BigQueryOptions.getDefaultInstance()`, or with options
carrying a configured key file, and its job ids named no project, so the client library filled in
the project it resolved: the key file's own project, or else `GOOGLE_CLOUD_PROJECT`, then the
metadata server where one answers, then the application-default credentials file or the gcloud
configuration (google-cloud-core 2.80.0).
On GKE with Workload Identity the metadata server answers with the cluster's project.
A job writing tables in another project then looked up its previous attempts' jobs in the
cluster's project, where its identity often held no BigQuery permission; the issue reports the first
commit failing with a 403 and the job restarting in a loop.
Nothing in the sink's configuration pointed at the cause, and the only lever, `GOOGLE_CLOUD_PROJECT`,
is process-wide.

The other paths of the connector already named their project.
The CDC provisioning statements ran in the destination's project, and the source runs its read
sessions and query jobs in `parentProject(...)`, which defaults to the table's project.

## Decision

**The sink runs every BigQuery job it creates in one project, which `parentProject(...)` names.**

- `BigQuerySinkBuilder#parentProject(String)` and the SQL option `sink.parent-project` take the
  source's name for the same concept.
  The project covers the FILE_LOADS load, copy and terminal query jobs and the CDC statements
  that manage maximum staleness.
  Storage Write API writes create no jobs, so the option has no effect on them; it is accepted
  under every write method rather than rejected.
- The value is checked with `ResourceNames.checkComponent`: it becomes the project segment of every
  job id, the shape ADR-0127 gives `queryResultDataset` as a segment of a `TableId`.
- The default is unchanged.
  Without the option, FILE_LOADS runs its jobs in the project the client library resolves (the
  key file's own project, or else the environment's default) and CDC runs its statements in each
  destination's project.
- With the option, the FILE_LOADS client is built with that project, so the environment no longer
  has to name one.
  Without it, a client that cannot resolve a project fails with a message naming the option.
- Every FILE_LOADS job id, at creation and at every look-up of a previous attempt, names the job
  project when one is configured or recorded; otherwise it is left to the client's project, as
  before.

**A FILE_LOADS committable records the configured project of the run that staged it.**

- `FileLoadsCommittableSerializer` moves to version 4; versions 2 and 3 are migrated with no
  project.
- A commit's committables share one project, checked beside their checkpoint id and Flink job id.
- A commit runs in the recorded project when there is one, and otherwise where the committing job
  is configured: `parentProject(...)`, or without it the client's project.

## Evidence

- Flink's `CommitterOperator.snapshotState` stores the committables of the checkpoint being taken
  before they commit, and `initializeState` commits the restored committables again.
  Read from the bytecode of Flink 1.20.5 and 2.2.1 on 2026-11-17.
  Every restore, from a savepoint as from a checkpoint, therefore re-commits the last checkpoint's
  committables and relies on finding the previous attempt's jobs.
- `LoadJobOrchestrator` deletes the staged files once every destination of a commit has succeeded.
  A re-commit that cannot find the previous attempt's jobs resubmits loads of deleted files, which
  fail on every retry id: a load naming a missing Cloud Storage object is created and then fails
  with `Not found: URI` (measured 2026-11-18, one load job against the test dataset).
- `JobId.setProjectId` in google-cloud-bigquery 2.76.0 fills the client's project only into an id
  whose project is unset, so an id that names a project is created and looked up there.
- Apache Beam's `BigQueryIO` runs FILE_LOADS loads and copies in the destination's project unless
  `withLoadJobProjectId` names another (`WriteTables`, `WriteRename`); the option was added by
  [BEAM-3433](https://issues.apache.org/jira/browse/BEAM-3433) because write access to a dataset
  does not imply `bigquery.jobs.create` in its project.
  The Spark BigQuery connector builds its client with `parentProject`, which defaults to the
  environment's project, and creates every job, its indirect-write loads included, in it.

## Alternatives declined

- **Each destination's own project as the default**, as Beam does.
  Declined by the owner: with destinations across projects it requires `bigquery.jobs.create` in
  every destination project, a grant that grows with every destination, and it spreads the jobs
  across projects, so a reservation assigned to one project no longer serves them.
- **Reading the job project from configuration at commit time only.**
  A restored commit after a project change would look for its previous attempt's jobs where none
  were created, and its resubmitted loads of deleted files would fail on every retry id.
  Leaving that state means discarding the committer's state, which loses any commit not yet done.
  The record is what makes a later change of the project safe, for state written while the
  option was set.
  A committable written while the option was unset records none, like one from an earlier
  version, so the first setting of the option has to name the project the jobs ran in so far.
- **Resolving a committable without a record to the environment's default project.**
  The expected adoption sets `parentProject(...)` to the project `GOOGLE_CLOUD_PROJECT` named and
  then removes the variable, after which the environment's default is another project (the
  gcloud configuration's, or on GKE the cluster's), so the restored commit would look in the
  wrong one.
  The owner chose that such a commit follows the current configuration.
- **Treating a 403 on a look-up in the previous default project as "no job there".**
  This would have let a job stuck in the 403 loop keep its state across the upgrade while naming
  another project.
  Declined by the owner as logic that serves only this transition; under the decision above that
  job recovers by setting `parentProject(...)` anyway, and a rare duplicate is acceptable where no
  row is lost.
- **Rejecting `parentProject(...)` on a sink that creates no jobs.**
  Declined by the owner: the value is inert there rather than wrong, a CDC sink that does not
  manage maximum staleness creates no jobs either, and neither the source's
  `scan.parent-project` nor Spark's `parentProject` is rejected.

## Consequences

- A running job adopts the option by setting `parentProject(...)` to the project its jobs have run
  in so far: the key file's project when it sets `serviceAccountKeyFile(...)`, otherwise the one
  its environment resolved, usually through `GOOGLE_CLOUD_PROJECT`.
  It can then remove the variable, and change the project only when restoring from a checkpoint
  or savepoint taken after the option was set: older state records no project and would be looked
  up in the new one.
  Naming another project in the adopting restart repeats loads whose files remain and fails on
  loads whose files were already deleted.
- State written in version 4 cannot be read by a release that predates it.
- The gated FILE_LOADS tests leave the option unset and still need the environment to name a
  project.
- A `bigquery` catalog that sets `sink.parent-project` carries it to the tables it resolves
  (ADR-0169, [#2196](https://github.com/flink-gcp/flink-connector-gcp/issues/2196)), so a
  FILE_LOADS statement on a catalog table need not repeat it in a hint.
