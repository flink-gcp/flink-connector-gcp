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

# ADR-0181: BigQuery schema updates patch only the schema under an If-Match precondition

- Status: Accepted
- Date: 2026-10-08
- Issues: [#1667](https://github.com/flink-gcp/flink-connector-gcp/issues/1667)
- Modules: bigquery (`sink.tables`)
- Current behavior: [Schema evolution](../content/docs/connectors/datastream/bigquery.md#schema-evolution)

## Context

Both schema reconcilers (the Storage Write API one and the FILE_LOADS one) read the live table, union its schema with the serializer's, and call `TableAdmin.updateSchema`.
A `false` return means the update lost a race, and the reconciler re-reads and retries; an `IOException` fails the job.
That loop is how parallel subtasks evolve one table without coordination.

`BigQueryTableAdmin.updateSchema` used to submit the whole `Table` resource it had read, with the merged schema swapped in, through `BigQuery.update`.
It assumed the `etag` in that resource made the write conditional.
It did not, for two reasons measured in #1667.
google-cloud-bigquery never sends an `If-Match` header, and BigQuery ignores an `etag` field in a `tables.patch` body, applying a stale one.
A request with a stale `If-Match` header instead answers `412 Precondition check failed`.

Two defects followed.
A second writer whose union was built on a stale read sent a schema lacking the column the first writer had added, and BigQuery answered `400 invalid` ("Provided Schema does not match Table ... Field a is missing in new schema").
`isLostRace` does not classify that as a race, so the job failed instead of re-reading.
And every attribute of the read (description, labels, expiration, partitioning, clustering) was written back, which could revert a change another party made after the read.

## Decision

`updateSchema` sends a `tables.patch` whose resource names only the table and the merged schema, conditioned on the snapshot's etag through an `If-Match` header.

The update is still issued through `BigQuery.update`, on a client derived for that update from the admin's client options with a header provider that adds `If-Match`.
The library's request initializer applies the header provider to every request, so the derived client sends a real precondition.
The derived client replaces the original's header provider rather than merging with it, because the merge (`ServiceOptions.getMergedHeaderProvider`) is `@InternalApi`, and `BigQueryOptions.toBuilder()` does not copy the BigQuery-specific options (location, OpenTelemetry tracing, the result retry algorithm); the clients the connector passes to it set none of them.
The library also keeps serializing the schema, which matters because `mergeSchema` preserves REST-only field attributes (policy tags, collation and others) of existing columns, and only the library's own conversion carries all of them.

The resource is `TableInfo.of(tableId, StandardTableDefinition.of(mergedSchema))`.
Its unset labels and resource tags are dropped from the JSON rather than sent as null, its other fields are null and omitted, and the RPC layer clears the output-only `type`.
`tables.patch` leaves every omitted attribute as it is.

`isLostRace` is unchanged.
A stale update now answers `412`, which it already classified as a lost race.
The `400 invalid` for a schema missing a live column stays a failure, on the widen-only-what-was-observed rule of [ADR-0030](0030-a-missing-bigquery-table-does-not-answer-not-found.md): the gated test measures that the precondition answers first.

A snapshot without an etag is updated without a precondition, and the update is still schema-only.
BigQuery returns an etag with every table read in the gated runs, but the goccy emulator the integration tests use returns none (measured on 2026-10-08 by logging the snapshot's etag during `BigQuerySchemaEvolutionITCase`), so the emulator only exercises this unconditional path.

A transient failure that the library retries after the service applied the patch now answers `412` on the retry.
The reconciler then re-reads, finds the union already applied, and stops.

## Evidence

The library behavior was read from the google-cloud-bigquery 2.73.0 and google-cloud-core-http 2.77.0 sources on 2026-10-08.
`Schema.toPb()` and `TableInfo.toPb()` are package-private, `HttpTransportOptions.getHttpRequestInitializer` sets the options' merged header-provider headers on each request, and `HttpBigQueryRpc` sets no `If-Match` of its own.

`BigQueryTableAdminTest` sends the update through a mock HTTP transport and asserts the `If-Match` header, a body with only `tableReference` and `schema` although the snapshot carries labels, partitioning and clustering, a preserved policy tag, `false` on `412`, and an `IOException` on `400`.
Restoring the previous whole-resource write-back fails the body assertion, which then lists `clustering`, `etag`, `labels` and `timePartitioning`, and removing the header fails the `If-Match` assertions.

`BigQueryTableAdminSchemaUpdateRealGcpITCase`, in the weekly gated suite, reproduces both races against the service.
Two writers read one table and add different columns, and the stale writer's update answers `412` and converges after a fresh read.
A writer whose read predates an `ALTER TABLE` of the description and labels adds its column and leaves both as the other party set them.
On 2026-10-08 both cases passed against the service, and the stale update answered `412 Precondition check failed` although its schema lacked the column the other writer had added.
The same run with the previous whole-resource write-back failed both cases: the stale union answered `400` ("Provided Schema does not match Table ... Field a is missing in new schema"), which `updateSchema` reports as an `IOException`, and the update reverted the concurrent description to the one it had read.

## Alternatives declined

- **A hand-built `PATCH`**, as the CDC provisioning label update does ([ADR-0112](0112-bigquery-cdc-auto-creation-combines-the-tables-api-with-verified-ddl.md)): the labels it sends are plain strings, but a schema body would need this project's own JSON form of `Field`, which would drop any REST attribute it does not know about, including ones a later library release adds.
- **Patching only the schema and treating the `400` as a lost race**: this classifies a lost race by an English error message shared with genuinely invalid updates, and it still lets a stale read overwrite the schema whenever the service accepts it, reverting for example a column description or policy tag changed after the read.
- **Reflective or same-package access to `toPb()`**: it depends on library internals that a dependency update can change without notice.
