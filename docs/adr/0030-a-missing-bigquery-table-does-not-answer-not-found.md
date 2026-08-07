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

# ADR-0030: A missing BigQuery table does not answer `NOT_FOUND`

- Status: Accepted
- Date: 2026-08-06 (measured on [#289], which grew a writer change because of it)
- Issues: [#289], [#318] (the unmeasured buffered half)
- Modules: bigquery (`sink.storage`)
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md` § A missing table does
  not say NOT_FOUND

## Context / Evidence

Opening a Storage Write API stream against a table that is not there answers
`PERMISSION_DENIED: Permission 'TABLES_GET' denied on resource '<table>' (or it may not exist)`.
The service masks existence, as an API that must not let an unauthorised caller probe for table
names has to. The goccy emulator answers `NOT_FOUND` (and `UNKNOWN` on the default stream), and
`AppendErrorClassifier` recovered on `NOT_FOUND` alone — so **`CREATE_IF_NEEDED` had never once
created a table against the real service**, while every emulator test said it did. Nothing
caught it because the gated storage-path suites create their tables up front.

## Decision

The verdict is `AppendErrorClassifier.isMissingTable`, taking both codes, consumed by
`BigQueryDefaultStreamWriter` (three sites) and `BigQueryBufferedStreamWriter.createStream`.

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

## Consequences

- The cost is stated rather than hidden: a job whose credentials genuinely lack the permission
  now attempts one creation before failing — naming `bigquery.tables.create`, which tells a
  reader more than the masked `TABLES_GET` did — and if it holds `tables.create` but not the
  data-write permission it leaves behind the empty table it was authorised to create.
- Two existing tests used `PERMISSION_DENIED` as their unambiguous *terminal* example and now
  use `INVALID_ARGUMENT`; that premise is gone, so do not restore it.
- Two messages stopped asserting what the masked code cannot establish: the four "does not
  exist, creating it" logs became "may not exist", and `retryFailureMessage`'s "after creating
  the table" became "after a table-creation attempt" — a 409 means the table was already there.
  `reconcileSchema`'s own "does not exist" log is **not** in that set: it is driven by a REST
  `getSchema` returning null, which does establish nonexistence.
- **`BigQueryBufferedStreamWriter`'s half is unmeasured**: the gated exactly-once suite
  pre-creates its tables, so whether `CreateWriteStream` masks the same way is inferred rather
  than observed — [#318].

[#289]: https://github.com/laughingman7743/flink-connector-gcp/issues/289
[#318]: https://github.com/laughingman7743/flink-connector-gcp/issues/318
