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
- Date: 2026-07-27; overflow wording revised by [#72] (2026-08-13)
- Issues: [#72], [#142]
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
memoized once per destination per commit (`finalTableSchema`). Both live on `FileLoadsSchemaReconciler`,
which owns the memo and nothing else; `LoadJobOrchestrator` builds one in its own constructor and
`FileLoadsCommitter` builds one orchestrator per commit, so the memo is per-commit by construction
and an overflow's partition loads reconcile once. That chain is the guarantee — the reconciler is
deliberately not a constructor parameter, because injecting it is what would let one memo outlive
its commit, and `FileLoadsCommitterTest.eachCommitReconcilesTheDestinationAgain` is the only test
that would notice.

Consequences that are decisions, not accidents:

- Missing tables are created via `TableAdmin` (with `TableCreateOptions`) before the load,
  retiring the load-job-driven creation machinery (`mayCreate`/`missingTables`) and
  `LoadJobSpec`'s partitioning/clustering fields — so a failed load can leave an empty table or
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

[#142]: https://github.com/flink-gcp/flink-connector-gcp/issues/142
[#72]: https://github.com/flink-gcp/flink-connector-gcp/issues/72
