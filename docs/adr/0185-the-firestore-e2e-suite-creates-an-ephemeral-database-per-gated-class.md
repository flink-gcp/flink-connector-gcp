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

# ADR-0185: The Firestore E2E suite creates an ephemeral database per gated class

- Status: Accepted
- Date: 2026-10-10
- Issues: [#1546], [#1705], [#1706]
- Modules: firestore (tests), `opentofu/`, `scripts/`
- Current behavior: `docs/content/docs/connectors/datastream/firestore.md` § Testing; the
  `just e2e` entry of `.agents/references/repository-guide.md` § Build

## Decision

The gated real-service suite of the Firestore module follows [ADR-0044] and [ADR-0088]: nothing
persistent is provisioned, and each gated class creates a named database, works in it and deletes
it. `FIRESTORE_IT_PROJECT` is the gate, with no companion variable, because there is no standing
resource to name. What is Firestore's own:

- **A database, not an instance, and both modes from one harness.** `EphemeralDatabases` creates
  a database of either mode through the Admin API in `us-central1`, with delete protection and
  point-in-time recovery off. It is public, and takes the mode as a parameter, so that the
  Datastore-mode harness of [#1707] can share its naming and its sweep from the other package root. A class that needs a second database, such as a Datastore-mode one to write
  to through the Firestore API, creates it through the same helper and deletes it with its own.
- **Only the acceptance of a delete is awaited, then the database's absence.** The Admin API's
  delete is a long-running operation, and the client library stops polling one after five minutes
  and cancels its future. The operation of a database that held about 11 MiB had not finished by
  then (2026-10-10), so the first teardown that waited for it failed and skipped the second
  database's delete. The database was no longer listed when that run ended, and in every later run
  a database was gone from `getDatabase` about 200 milliseconds after its delete was accepted. Teardown attempts every delete
  whatever the one before it did.
- **Leak control is the class-start sweep alone.** Ids are `flink-it-<epochSeconds>-<runId>-<n>`,
  and each class deletes this suite's databases older than two hours before creating its own, as
  [ADR-0044] does. [ADR-0119]'s scheduled `scripts/sweep-e2e.sh` does not sweep them. That script
  exists for fixtures billed while they stand; a database bills for what it stores and the
  operations run on it, so an abandoned one costs fractions of a cent until the next gated class
  removes it.
- **`roles/datastore.admin` on the E2E account** ([#1705]). Of the Datastore and Firestore roles,
  only admin and owner carry `datastore.databases.delete`; the clone and restore roles carry the
  create without it, and the Firebase admin roles that also carry it span every Firebase product.
  `opentofu/flink-gcp/e2e-sa.tf` records the comparison.

## Evidence

Measured on 2026-10-10 with `gcloud` and the Admin API, Standard edition, `us-central1`: a create
took 2.0 to 2.6 seconds in either mode, and a write four seconds after it was applied. A delete of a
database holding at most one document finished in 2.3 to 13.4 seconds through `gcloud`, and in the
suite a database was gone from `getDatabase` about 200 milliseconds after its delete was accepted. A deleted id cannot be reused for about five minutes: the service answered
`FAILED_PRECONDITION: Database ID '…' is not available in project 'flink-gcp'. Please retry in 298
seconds.` The timestamp and run id in every name avoid that.

**What the suite measured that the emulator could not.** The rejection statuses of the sink's
error-handling table are the service's as well as the emulator's, including `FAILED_PRECONDITION`
for a stale precondition on a missing document. Every `INVALID_ARGUMENT` shape answers every write
of its request, where the emulator was measured for one shape only. A well-formed database id that
names no database and a malformed one (`Bad_Id`) both answer `NOT_FOUND` for every write, and a
Datastore-mode database written through the Firestore API answers `FAILED_PRECONDITION` for every
write. A collection group of 3,000 documents came back in all 8 partitions asked for. A read time
finer than a microsecond is refused with `INVALID_ARGUMENT`, version times are whole microseconds,
and a timestamp value is floored to the microsecond.

**Where the service and the emulator disagree.** The service stored an array inside an array in a
Standard-edition database, which the emulator refuses with `INVALID_ARGUMENT` and Google's
documentation allows only in Enterprise edition; the `firestore` table maps `ARRAY<ARRAY<…>>` on
that measurement ([ADR-0179]'s revision), and a gated class writes and reads one through SQL. It applied a `BatchWrite` of 11,060,930 bytes (twelve documents of 900 KiB) in one request,
past the documented 10 MiB limit; fifteen such documents did not finish within the client's
60-second deadline from a residential uplink, so where the ceiling lies was not established. A
read time from before the database existed is refused with `INVALID_ARGUMENT: The requested
'read_time' cannot be before database creation time.`, which means a database created minutes
earlier cannot show the one-hour window of a database without point-in-time recovery. That window
was measured once, by hand, on four temporary databases more than an hour old, one per mode with
and without point-in-time recovery, all deleted afterwards. Without recovery, a read time older than
about an hour is refused with `FAILED_PRECONDITION` ("too old"); with it, one that old is answered
on a whole minute and refused otherwise with `FAILED_PRECONDITION` ("not a whole minute"). With
recovery the whole-minute rule began between 59 minutes 33 seconds and 59 minutes 51 seconds back;
without it, the refusal began between 59 minutes 43 seconds and 60 minutes 1 second back. Both
modes answered alike. The
documentation page records this rather than a test asserting it every week, because keeping a
database standing for it was declined below.

## Alternatives declined

- **A standing database, with or without point-in-time recovery, in `opentofu/flink-gcp`.** It is
  the only way to assert the one-hour and seven-day read-time windows every week, since a database
  younger than an hour refuses an older read time for predating it. The owner declined it: a
  database that stands costs money for a measurement that changes rarely, and a fixture that
  creates and deletes one within a run is the shape to reach for if the measurement must recur.
- **Joining `scripts/sweep-e2e.sh`.** It would add a third listing and delete shape to a script
  whose purpose is billed idle fixtures, for a resource that is not one.
- **A negative IAM probe.** The E2E account holds `roles/datastore.admin`, so nothing in the suite
  can be refused for permissions; measuring `PERMISSION_DENIED` would need a second identity, as
  for Spanner. Ramp-up throttling and `RESOURCE_EXHAUSTED` stay unmeasured too: the suite writes
  far below the 500 operations a second that the service's ramp-up guidance starts from.

[#1546]: https://github.com/flink-gcp/flink-connector-gcp/issues/1546
[#1705]: https://github.com/flink-gcp/flink-connector-gcp/issues/1705
[#1706]: https://github.com/flink-gcp/flink-connector-gcp/issues/1706
[#1707]: https://github.com/flink-gcp/flink-connector-gcp/issues/1707
[ADR-0044]: 0044-the-e2e-suite-creates-an-ephemeral-bigtable-instance-per-gated-class.md
[ADR-0088]: 0088-the-spanner-e2e-suite-creates-an-ephemeral-standard-edition-instance-per-gated-class.md
[ADR-0119]: 0119-a-scheduled-source-derived-sweep-returns-billed-e2e-fixtures-to-their-idle-state.md
[ADR-0179]: 0179-the-firestore-table-sink-writes-one-collection-keyed-by-document-id.md
