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

# ADR-0112: BigQuery CDC auto-creation combines the Tables API with verified DDL

- Status: Accepted
- Date: 2026-08-14
- Issues: [#65](https://github.com/flink-gcp/flink-connector-gcp/issues/65),
  [#627](https://github.com/flink-gcp/flink-connector-gcp/issues/627)
- Modules: bigquery
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md#change-data-capture`,
  `docs/content/docs/connectors/table/bigquery.md#change-data-capture`

## Context

BigQuery CDC requires a destination primary key.
Its optional `max_staleness` setting lets BigQuery apply pending mutations in the background instead of performing every merge while a user query waits.
The setting therefore changes recurring query latency and cost rather than merely decorating table metadata.

The BigQuery REST [`Table` resource][table-resource] exposes both `tableConstraints` and `maxStaleness`, and [`tables.insert`][tables-insert] accepts a `Table` as its request body.
That surface suggested that the connector could create the schema, primary key, and maximum staleness atomically without submitting a GoogleSQL job.
The [official CDC guide][cdc-guide], however, documents `CREATE TABLE` for setting `max_staleness` during creation and `ALTER TABLE` for changing it later.

Issue #627 originally selected external pre-creation after the first REST experiment disproved the atomic API path.
The requirement was then refined: the sink should still create each missing table through the Tables API, and it may use a narrowly scoped DDL job only for the property the API does not store.

## Service evidence

The REST paths were measured against BigQuery on 2026-08-14 with `google-cloud-bigquery` 2.68.0 and generated BigQuery REST model `v2-rev20260612-2.0.0`.

The `tables.insert` request contained the physical schema, `tableConstraints.primaryKey.columns = ["id"]`, and `maxStaleness = "0-0 0 0:0:0.001"`.
BigQuery returned success and created the table.
The stored primary key was present, but `INFORMATION_SCHEMA.TABLE_OPTIONS` contained no `max_staleness` row.

Separate `tables.patch` and full-resource `tables.update` experiments also returned success after receiving `maxStaleness`.
Neither stored a `max_staleness` row.
The full-resource update preserved the schema and primary key, so the negative result was not caused by an accidentally destructive replacement request.

A GoogleSQL control table created with `OPTIONS (max_staleness = ...)` did expose the requested value through `INFORMATION_SCHEMA.TABLE_OPTIONS`.
A direct `tables.get` still returned `maxStaleness = null` for that successful control.
The generated REST getter is therefore not a storage oracle for this property.

Every temporary experiment table was deleted and independently confirmed absent.
This evidence is deliberately dated because the service may implement the exposed field later.

## Decision

CDC supports both creation dispositions.
`CREATE_IF_NEEDED` authorizes `tables.insert` when a destination is missing, while `CREATE_NEVER` fails on a missing destination.
The disposition does not decide whether an existing table may be changed.

The Table API derives the BigQuery primary-key columns from the declared Flink primary key.
The DataStream API supplies the CDC table contract through `CdcTableOptions` or a per-destination `CdcTableOptionsProvider`.
`TableCreateOptions` remains limited to partitioning and clustering because those properties apply only at creation time.
The Table API keys are `sink.cdc.max-staleness`, `sink.cdc.clear-max-staleness`, and `sink.cdc.table-reconciliation`.

The sink separates creation permission from the **CDC table reconciliation policy**.
The policy is job-wide because a job must not change ownership behavior as it routes between destinations.
It has these outcomes:

| Create disposition | Reconciliation policy | Missing table | Existing table |
|---|---|---|---|
| `CREATE_IF_NEEDED` | `VERIFY_ONLY` (default) | Create, then complete the CDC contract | Verify without adoption or drift repair; resume a matching partial attempt |
| `CREATE_IF_NEEDED` | `RECONCILE` | Create, then complete the CDC contract | Converge mutable managed properties |
| `CREATE_NEVER` | `VERIFY_ONLY` | Fail | Verify without adoption or drift repair; resume a matching partial attempt |
| `CREATE_NEVER` | `RECONCILE` | Fail | Converge mutable managed properties |

`VERIFY_ONLY` is the conservative default.
It never adds or changes a provisioning label on an unlabeled existing table.
`RECONCILE` may adopt an unlabeled table, repair drift behind a matching label, or migrate a completed older specification.
It manages only `max_staleness` and the connector provisioning label.
It never changes the primary key, partitioning, clustering, or schema through this protocol.

The primary key is authoritative when the sink creates or reconciles a table.
The Table API always supplies it from the DDL, while a DataStream sink must configure it for a missing table and under `RECONCILE`.
For an existing table under `VERIFY_ONLY`, a DataStream sink may omit it and adopt the nonempty primary key returned by the Tables API for verification.
A configured primary-key mismatch always fails before DDL.

Maximum staleness has three desired states.
An absent value is unmanaged, a duration requests that value, and an explicit clear requests `ALTER TABLE ... SET OPTIONS (max_staleness = NULL)`.
The clear operation exists separately from absence so a migration can remove an earlier value without making every unconfigured job an owner of the property.

The sink follows this protocol for a missing CDC table:

1. It calls `tables.insert` with the physical schema, primary key, ordinary creation settings, and a connector provisioning label.
2. With maximum staleness unmanaged, the label is complete and no query job is submitted.
   Measured on 2026-08-14, a new table without the option had no `max_staleness` row, while
   `SET OPTIONS (max_staleness = NULL)` retained the row with
   `option_value = "0-0 0 0:0:0"`.
   Verification therefore accepts either absence or that zero interval.
3. With a managed maximum-staleness duration or explicit clear, the label is pending and contains a hash of the effective primary key and managed state.
4. It checks `INFORMATION_SCHEMA.TABLE_OPTIONS` and submits `ALTER TABLE ... SET OPTIONS (max_staleness = ...)` only when the managed state does not match.
   A new table normally already satisfies an explicit clear, so that path verifies the absent option and completes without DDL.
   After DDL, it reads a fresh pending-table snapshot.
5. It verifies the value through `INFORMATION_SCHEMA.TABLE_OPTIONS`, retaining the ETag from the
   table snapshot that immediately preceded that verification, then changes the matching pending
   label to complete with that ETag as an `If-Match` precondition.
   A metadata change between the snapshot, verification, and patch therefore loses the optimistic
   race instead of publishing a stale specification as complete.
6. It opens the Storage Write API appender only after the protocol completes.

The DDL interval is rendered from a validated positive, exact microsecond count rather than from user text.
The table name is backtick-quoted from the structured destination, and the `INFORMATION_SCHEMA` query binds the table name as a parameter.
The job uses the destination project and the configured sink location when present.

The provisioning label provides recovery and ownership boundaries.
A label value is `pending_` or `complete_` followed by the first 128 bits of a SHA-256 digest rendered as hexadecimal.
The digest keeps a potentially long composite primary key within BigQuery's [63-character label-value limit][labels] while making the desired state comparable without storing user configuration in labels.
A restarted writer can observe a matching pending table, skip an already successful DDL after verification, and finish the label transition.
Parallel subtasks may issue the same idempotent DDL, but conflicts, propagation delay, and the measured per-table metadata rate limit remain bounded by the existing table-recovery retry schedule.
The same schedule retries BigQuery's documented transient query-job reasons, including backend, internal, and job rate-limit failures.
It also retries an ambiguous transport failure or a table deleted during the conditional completion patch; a retry re-reads the durable state before deciding whether to resume or recreate it.
Dynamic destinations run the same protocol independently, and an evicted destination is verified again when it becomes active.

An unlabeled table under `VERIFY_ONLY` is checked without mutation.
Under `RECONCILE`, the sink first claims an unlabeled or completed table through an ETag-conditioned transition to the desired pending label, then applies and verifies DDL before publishing the complete label.
A matching pending label resumes under either policy because it records an already-started operation.
A different pending specification fails rather than being taken over.
A completed different specification fails under `VERIFY_ONLY` and migrates under `RECONCILE`.
A primary-key mismatch always fails before a CDC append.

`INFORMATION_SCHEMA.TABLE_OPTIONS`, not the REST getter or a successful DDL response, is the acceptance oracle for maximum staleness.

## Permissions and operational cost

CDC auto-creation without maximum staleness uses the same table create/get permissions as ordinary auto-creation and submits no query job.
Setting or explicitly clearing maximum staleness additionally requires permission to create BigQuery jobs and to update the destination table.
Reconciling an existing table also requires table-update permission for the connector provisioning label, even when maximum staleness is unmanaged.
That label-only path submits no query job.
Managing maximum staleness adds DDL and metadata-verification query latency on first use, restart recovery from pending state, and reactivation after local destination eviction.
The verification query scans metadata rather than user table data, but it is still a BigQuery query job subject to the project's job quotas and billing rules.

## Alternatives declined

- **Use `tables.insert`, `tables.patch`, or `tables.update` alone.**
  All three accepted the field but silently failed to store it in the measured service behavior.
- **Trust the REST `maxStaleness` getter after DDL.**
  The getter remained null for a table whose `INFORMATION_SCHEMA` row proved the setting existed.
- **Require every CDC table to be externally pre-created.**
  This avoids connector-owned jobs but pushes a dynamic destination's lifecycle outside the sink and was rejected as the default workflow.
- **Create the whole table with one GoogleSQL `CREATE TABLE`.**
  BigQuery documents this path, but it makes schema construction, table creation, and job recovery DDL concerns when the Tables API already handles schema and primary-key creation.
- **Always reconcile existing tables.**
  This would overwrite externally managed maximum staleness without an explicit ownership choice, so reconciliation is opt-in and verification is the default.

## Reconsideration trigger

Revisit the DDL half when BigQuery documents and implements a non-job Tables API request that stores `maxStaleness` for native CDC tables.
A gated acceptance test must prove the stored primary key, prove maximum staleness through `INFORMATION_SCHEMA.TABLE_OPTIONS`, prove CDC UPSERT and DELETE ordering, and prove cleanup before the query-job path is removed.
The generated REST model continuing to expose the field is not sufficient evidence.

## Consequences

The table itself is always created through the Tables API.
The only DDL is the `ALTER TABLE` required to set or explicitly clear maximum staleness.
The sink owns a small, recoverable provisioning state machine instead of requiring operators to pre-create every CDC destination.
Operators who do not want connector-owned DDL can leave maximum staleness unmanaged and retain the default `VERIFY_ONLY` policy.
`CREATE_NEVER` can still use `RECONCILE`; it denies creation but permits explicitly selected convergence of an existing table.
A job graph serialized before this decision retains that legacy pre-created-table behavior after an upgrade; a new deployment opts into connector-managed CDC creation.

[cdc-guide]: https://cloud.google.com/bigquery/docs/change-data-capture
[labels]: https://cloud.google.com/bigquery/docs/labels-intro
[table-resource]: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables
[tables-insert]: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/insert
