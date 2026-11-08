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
- Date: 2026-10-10; revised 2026-10-11 by [#1707] (the Datastore-mode harness), 2026-10-17 by
  [#1819] (an Enterprise-edition database for the array inside an array) and 2026-10-24 by [#1895]
  (either Standard-edition answer accepted), and 2026-11-08 by [#2079] (writes that arrange data
  retried while throttled)
- Issues: [#1546], [#1705], [#1706], [#1707], [#1819], [#1895], [#2079]
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
  point-in-time recovery off, in the edition its caller names (Standard, unless a class needs
  Enterprise; [#1819]). It is public, and takes the mode as a parameter, because the
  Datastore-mode harness (`AbstractDatastoreRealGcpITCase`, [#1707]) shares its naming and its sweep
  from the other package root. A class that needs a second database, such as one of the other mode
  to address through the wrong API, creates it through the same helper and deletes it with its own.
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
- **A write that arranges a test's data is sent again while the service throttles it** ([#2079]).
  `ThrottledWrites` retries a write answered `RESOURCE_EXHAUSTED` or `UNAVAILABLE`, with a backoff
  from one second doubling to sixteen, and starts no attempt after two minutes. Both answers ask
  the client to retry, and the connector's own writers do. Each harness's direct writes go through
  it, a write a test expects to be refused included, since no refusal status is one it retries.
  Every such write is an upsert, so one applied despite its answer is applied again harmlessly.
  The calls of the connector under test do not, and the two over-10-MiB probes keep their own
  loops, because
  they must tell the throttle from a size refusal: a write is retried only on `UNAVAILABLE` or on a
  `RESOURCE_EXHAUSTED` whose message names the write bandwidth.
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
Standard-edition database on 2026-10-10, which the emulator refuses with `INVALID_ARGUMENT` and
Google's documentation allows only in Enterprise edition. By 2026-10-17 a Standard-edition
database refused one too, in either mode, and an Enterprise-edition database stores one ([#1819]).
On 2026-10-24 a Standard-edition database stored one again, in either mode ([#1895]), so the
gated classes accept either Standard-edition answer, log which one they saw, and hold what the
connector relies on under each.
The `firestore` table keeps mapping `ARRAY<ARRAY<…>>` for that edition ([ADR-0179]'s revisions),
so `EphemeralDatabases` takes the edition as an argument and a gated class writes and reads one
through SQL in an Enterprise-edition database of its own. Only a Native-mode database can be
Enterprise edition, and the Admin API's `firestore_data_access_mode` defaults to disabled for
that edition (google-cloud-firestore-admin 3.49.0), so the harness enables it.

The service applied a `BatchWrite` of 11,060,930 bytes (twelve documents of 900 KiB) in one request,
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

**Throttling of a fresh database.** No gated run up to 2026-10-31 inclusive logged a throttled write. The
scheduled run of 2026-11-07 did, one second after the database was created:
`RESOURCE_EXHAUSTED` ("This database has exceeded their maximum bandwidth for writes, please retry
with exponential backoff") for five of the over-10-MiB probe's twelve writes. A manual run of
2026-11-08 got the same answer for a 3,000-document seed in 500-write batches 8 seconds after
creation and for every probe write 11 and 18 seconds after, then `UNAVAILABLE` ("The service is
temporarily unavailable. Please retry with exponential backoff.") for one write of the probe's
third attempt, 27 seconds after. Every run from 2026-10-11 on wrote from the same two forks to
fresh databases at once, so that layout alone does not explain the change. On 2026-11-08 a single
fresh database written from a workstation answered all 39 writes of its first three minutes, a
single write, 500 small writes and four of 500 KiB about every 14 seconds, so the throttle is not a
fixed warm-up after creation that a wait could cover. What triggers it was not established.

## Alternatives declined

- **Waiting after a database is created until it accepts writes.** The single-database measurement
  above found no warm-up to wait out, so no condition such a wait could test was known.

- **A standing database, with or without point-in-time recovery, in `opentofu/flink-gcp`.** It is
  the only way to assert the one-hour and seven-day read-time windows every week, since a database
  younger than an hour refuses an older read time for predating it. The owner declined it: a
  database that stands costs money for a measurement that changes rarely, and a fixture that
  creates and deletes one within a run is the shape to reach for if the measurement must recur.
- **Joining `scripts/sweep-e2e.sh`.** It would add a third listing and delete shape to a script
  whose purpose is billed idle fixtures, for a resource that is not one.
- **A negative IAM probe.** The E2E account holds `roles/datastore.admin`, so nothing in the suite
  can be refused for permissions; measuring `PERMISSION_DENIED` would need a second identity, as
  for Spanner. The service's ramp-up behaviour is not asserted either: the throttle the suite met is
  retried as described above, not measured.

[#1546]: https://github.com/flink-gcp/flink-connector-gcp/issues/1546
[#1705]: https://github.com/flink-gcp/flink-connector-gcp/issues/1705
[#1706]: https://github.com/flink-gcp/flink-connector-gcp/issues/1706
[#1707]: https://github.com/flink-gcp/flink-connector-gcp/issues/1707
[#1819]: https://github.com/flink-gcp/flink-connector-gcp/issues/1819
[#1895]: https://github.com/flink-gcp/flink-connector-gcp/issues/1895
[#2079]: https://github.com/flink-gcp/flink-connector-gcp/issues/2079
[ADR-0044]: 0044-the-e2e-suite-creates-an-ephemeral-bigtable-instance-per-gated-class.md
[ADR-0088]: 0088-the-spanner-e2e-suite-creates-an-ephemeral-standard-edition-instance-per-gated-class.md
[ADR-0119]: 0119-a-scheduled-source-derived-sweep-returns-billed-e2e-fixtures-to-their-idle-state.md
[ADR-0179]: 0179-the-firestore-table-sink-writes-one-collection-keyed-by-document-id.md
