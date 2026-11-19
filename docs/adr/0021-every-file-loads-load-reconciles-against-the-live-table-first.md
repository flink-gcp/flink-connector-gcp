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

# ADR-0021: Every FILE_LOADS load reconciles against the live table first

- Status: Accepted
- Date: 2026-07-27; overflow wording revised by [#72] (2026-08-13); destination-task ownership
  revised by [#1129] (2026-08-29); temporary-table layout revised by [#1671] (2026-10-09); a direct
  load never creates its destination, revised by [#2181] (2026-11-17)
- Issues: [#72], [#142], [#1129], [#1671], [#2181]
- Modules: bigquery (`sink.fileloads`)
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md` § Schema evolution

## Context / Evidence

The defect this fixes was measured on real BigQuery: a load job *adding* a `REQUIRED` column is
rejected at submission even under `ALLOW_FIELD_ADDITION`, so the old direct path — serializer
schema, unreconciled — failed the whole job under `allowNewFields()` exactly when the run fit
one partition, while the temp-table path demoted the addition to `NULLABLE` and succeeded.
Tightening an *existing* column's mode, the other measured row, is silently ignored and was
never a problem.

## Decision

`ensureFinalTable` is the shared entry point for **every** load — direct and temp-table alike —
through `FileLoadsSchemaReconciler.finalTableSchema`.
`LoadJobOrchestrator` builds one reconciler for each destination in the commit's bounded
reconciliation phase and stores the returned schema in that destination's commit plan.
The later commit-wide job phases retain each destination's load and copy-level order, so an
overflow's partition loads still reconcile once while distinct tables may reconcile concurrently.
That reconciliation phase and plan ownership are the guarantee: every commit builds a fresh plan,
and no later phase asks the reconciler to read the table again.
`FileLoadsCommitterTest.eachCommitReconcilesTheDestinationAgain` holds the lifetime boundary.

Consequences that are decisions, not accidents:

- Missing tables are created via `TableAdmin` (with `TableCreateOptions`) before the load,
  retiring the load-job-driven creation machinery (`mayCreate`/`missingTables`) and
  `LoadJobSpec`'s partitioning/clustering fields as a description of the destination. Where the
  copy into the destination needs it, a temporary-table load carries the destination's partitioning
  and clustering instead, read by the same reconciliation from the live table, so its table can be
  copied into the destination
  ([ADR-0183](0183-file-loads-temporary-tables-take-the-destinations-live-layout.md)). A
  pre-load creation means a failed load can leave an empty table or
  an applied schema union behind, as the temp path always could, columns being irreversible
  anyway. `CREATE_NEVER` + missing table is a client-side `IOException` before anything is
  submitted. `bigquery.tables.get` became an unconditional FILE_LOADS requirement (one read per
  destination per run).
- The native `ALLOW_FIELD_ADDITION`/`ALLOW_FIELD_RELAXATION` options are **kept** on
  `WRITE_APPEND` jobs — asked, and the user chose keeping them (2026-07-27) as belt-and-braces
  against external mid-run schema changes — even though a reconciled provided schema makes them
  no-ops otherwise; do not drop them as "dead" in a cleanup.
- With updates **disabled** the live schema wins outright, and — measured — BigQuery silently
  ignores a staged Avro field the provided schema lacks: the rows load and that column's data is
  dropped, where the unreconciled direct path had failed loudly at submission. The orchestrator
  warns once per destination by probing the union with the disabled options and catching
  `SchemaUnionException` — that warn is what remains of the old loudness, so it is load-bearing,
  not a simplification target.
- `WRITE_EMPTY` + updates enabled unions on the direct path too (batch-only; streaming rejects
  non-append).

Both measured rows are pinned against real BigQuery by
`BigQueryFileLoadsSchemaEvolutionITCase`.

## Revision: a direct load never creates its destination (2026-11-17)

The decision above moved creation to `TableAdmin`, but a direct load still carried the sink's create
disposition, so under `CREATE_IF_NEEDED` it could create the destination itself.
It did so only when the table was dropped after the reconciliation, and then from the load's schema
alone: without partitioning, clustering, partition expiration, partition filter requirement,
description, labels or, unless it truncated, the Cloud KMS key (ADR-0203).
The commit still succeeded, and later commits found the table present and left it that way
([#2181]).

Every direct load is now `CREATE_NEVER`, as the final copy into the destination already was.
A destination dropped mid-commit fails that commit's load when the load is submitted, rather than
re-attached to from an earlier attempt that already succeeded; the job restarts, and the retried
commit's reconciliation treats the table as any missing one: under `CREATE_IF_NEEDED` it creates it
with its `TableCreateOptions` before the load runs again under the next retry id, and under
`CREATE_NEVER` it fails the commit client-side.
That retry needed a runner change too: `jobs.insert` refuses such a load at once, and the runner
used to probe past its own refused job through every retry id within the attempt, leaving the retry
none (ADR-0018's 2026-11-17 revision).
Permissions are not expected to change: the reconciliation needs `bigquery.tables.create` under
`CREATE_IF_NEEDED` either way, and a `CREATE_NEVER` load asks for nothing a `CREATE_IF_NEEDED` load
into an existing table did not; that is reasoned from the dispositions, not measured with a
restricted account as ADR-0183's permission tables were.
`BigQueryFileLoadsTempTableLayoutRealGcpITCase` drops a destination just before its direct load,
and asserts that the commit fails and that the retried commit's table carries the creation options.

[#142]: https://github.com/flink-gcp/flink-connector-gcp/issues/142
[#72]: https://github.com/flink-gcp/flink-connector-gcp/issues/72
[#1129]: https://github.com/flink-gcp/flink-connector-gcp/issues/1129
[#1671]: https://github.com/flink-gcp/flink-connector-gcp/issues/1671
[#2181]: https://github.com/flink-gcp/flink-connector-gcp/issues/2181
