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

# ADR-0203: BigQuery jobs that replace a table state the destination's own key

- Status: Accepted
- Date: 2026-11-08
- Issues: [#1995](https://github.com/flink-gcp/flink-connector-gcp/issues/1995)
- Modules: bigquery
- Current behavior: `docs/content/docs/connectors/datastream/bigquery.md` § Customer-managed
  encryption keys

## Context

A deployment that must keep its data under a customer-managed Cloud KMS key needs every BigQuery
table and query result the connector creates to be encrypted with that key.
BigQuery already applies a dataset's default key, and a project's default key
(`region-<location>.default_kms_key_name`), to new tables and to query results in the anonymous
dataset, so the question was whether the connector takes a key at all.
It does: an explicit key is the only way to encrypt one destination differently from its dataset, and
the Pub/Sub sink already takes one for the topics it creates.

The sink's `FILE_LOADS` commit writes tables the user never names: temporary tables, created by
`tables.insert` or by a `CREATE_IF_NEEDED` load or copy, and the destination itself, written by
loads, copies and a `WRITE_TRUNCATE_DATA` query.
Measured against `us-central1` on 2026-11-08, with one key and tables with and without it:

| Job against an existing table | No key stated | Same key | Different key |
|---|---|---|---|
| Append (load or copy) | Keeps the table's encryption | Succeeds | Fails: "Existing table encryption settings do not match" |
| `WRITE_TRUNCATE` (load, copy or query) | Replaces it with the default | Succeeds | Re-keys the table |
| `WRITE_TRUNCATE_DATA` (query) | Keeps the table's encryption | Not measured | Fails |

A copy into a new table does not inherit its source's key.
A query whose result lands in the anonymous dataset is encrypted with the key it states, and
BigQuery keys its result cache by that key.

So a key stated only when a table is created is not enough.
The destination of a `WRITE_TRUNCATE` sink would lose a key the connector gave it at the next
commit, and a destination the user encrypted by hand would lose its key the same way; the second
happened before this change too.

## Decision

The connector does not change the encryption of a table that exists, apart from the two
`WRITE_TRUNCATE` cases under Consequences.

- `TableCreateOptions.kmsKeyName(...)` (`sink.table-create.kms-key-name`) applies when the sink
  creates a table, on every write method.
  An existing table's key wins over a configured one, without an error.
- `FILE_LOADS` reads the destination's actual key with its schema, during reconciliation, and
  states it on every `WRITE_TRUNCATE` job: the loads and copies that fill temporary tables, and the
  load or copy that replaces the destination under `WRITE_TRUNCATE`.
  Temporary tables created ahead of their jobs take the same key.
  Appends, `WRITE_EMPTY` jobs and the `WRITE_TRUNCATE_DATA` query state none.
- The query source takes `queryResultKmsKeyName(...)` (`scan.query-result-kms-key-name`) for its
  result, in the named or the anonymous dataset.
  The reusable query job's id digests the key only when one is set, so a job submitted by an
  earlier version keeps its id and is still reused.
- Objects `FILE_LOADS` stages in Cloud Storage take the bucket's default key; the connector has no
  option for them.

## Consequences

- A destination's temporary tables take the destination's key, and a `WRITE_TRUNCATE` sink keeps
  it.
  A retried commit re-attaches to the temporary-table loads an earlier attempt completed, so those
  tables keep the encryption they had then, such as none of their own for a commit in flight across
  the upgrade; making the key part of the job ids would re-run those loads and give up the
  re-attachment the commit protocol relies on (ADR-0018), for working tables the commit deletes.
  That deletion is best-effort: a failed delete is logged, and the table stays until it expires,
  which an unlaid temporary table does only under its dataset's default table expiration.
- The rule is enforced in one place, the commit's choice of the key a job states, rather than by a
  check that compares keys.
- A destination encrypted with a Google-owned key, under `WRITE_TRUNCATE`, still takes the default
  encryption, because a job cannot state "no customer-managed key"; with a dataset or project
  default key that default can differ from what the table had.
- The key is the one the destination had when the commit reconciled it, as its schema is.
  An operator who re-keys the destination during a `WRITE_TRUNCATE` commit has the change undone
  by that commit's replacing job, and the next commit reads the restored key, so the operator's
  key is lost until it is set again; a re-key between commits is kept.
- A direct load that may create its destination states no key unless it truncates.
  The commit has just created or read that table, so the load creates it only if the table was
  dropped in between, and then with the default encryption, as it would without its partitioning.
- A configured key that differs from an existing table's is silent.
  Reporting it would need a second comparison and a policy for the case, and the sink already treats
  every other creation option the same way.

## Alternatives considered

- **State the configured key on every job that may create a table.**
  Rejected: an append that states a key fails against an existing table with another one, and
  `WRITE_TRUNCATE` would re-key a table the user encrypted differently.
- **State the key only at creation.**
  Rejected: `WRITE_TRUNCATE` would remove it at the next commit, as measured above.
- **An option for the staging objects' key.**
  Declined by the owner: the objects are a commit's working files, deleted after it succeeds, and a
  bucket default key covers them without a connector setting.
