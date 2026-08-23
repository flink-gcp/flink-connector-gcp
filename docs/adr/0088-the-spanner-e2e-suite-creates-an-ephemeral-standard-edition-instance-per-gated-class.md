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

# ADR-0088: The Spanner E2E suite creates an ephemeral Standard-edition instance per gated class

- Status: Accepted
- Date: 2026-08-10
- Issues: [#224], [#441], [#535]
- Modules: spanner (tests), `opentofu/`, `scripts/`
- Current behavior: `docs/content/docs/connectors/datastream/spanner.md` § Testing; the root
  CLAUDE.md `just e2e`/`sweep-e2e` entries

## Decision

The shape is [ADR-0044]'s, reached independently for the same reason: a Spanner instance bills for
as long as it exists, so nothing persistent is provisioned and each gated class creates an instance,
uses it and deletes it. What is Spanner's own:

- **100 processing units, `STANDARD` edition, `regional-us-central1`.** 100 PU is the floor for a
  regional configuration and orders of magnitude more than a few thousand rows need. The edition is
  **set rather than defaulted**, and the reason is Data Boost. Google's editions page lists Data
  Boost from Standard up, and rather than rest on that reading the suite demonstrates it: a boosted
  read on this very instance returns every row (below), so no upgrade is needed and the claim is
  measured rather than cited. The instance that would otherwise be cheapest — a free-trial instance
  — is excluded by the same feature, since Data Boost is one of the few things a trial instance's
  documentation explicitly lists as unsupported.
- **A class creates its databases once, in its own `@BeforeAll`, and shares them across its tests.**
  The emulator classes can afford a database per test method; against the service each costs
  seconds, and a test that only reads a schema does not need one of its own.
- **PostgreSQL DDL goes in a second call.** Real Spanner refuses a `CreateDatabase` request carrying
  extra DDL for a PostgreSQL-dialect database, where the emulator applies it, so the harness issues
  `updateDatabaseDdl` separately for that dialect and keeps the one-call form for GoogleSQL, which
  is the dialect the emulator tests share.
- **Leak control is [ADR-0044]'s, unchanged**: `flink-it-<epochSeconds>-<runId>`, a sweep of
  anything older than two hours at the start of each class, and a threshold far above the E2E
  workflow's ceiling so the sweep cannot reach a live run. That ceiling moved from 40 to 60 minutes
  when this suite joined and must stay under two hours.
- **The scheduled sweep became one script over both services.** `scripts/sweep-bigtable-e2e.sh` is now
  `scripts/sweep-e2e.sh`, sweeping Bigtable and Spanner independently and reporting the worst
  status. Not tidiness: `just` stops at its first failing line, so a recipe line per service would
  let one service's failed delete skip the other's sweep entirely — the guardrail failing quietly in
  the direction that costs money.
- **`roles/spanner.editor` on the E2E account — not admin, and this is where Spanner departs from
  [ADR-0044] rather than following it.** Editor carries every permission the suite uses, checked
  one by one against `gcloud iam roles describe`: `spanner.instances.create`/`delete`/`list`/`get`,
  database creation and DDL, read, write, the two transaction verbs, `partitionQuery`,
  `partitionRead`, session create/delete, and `spanner.databases.useDataBoost`. What admin adds is
  what a CI identity should not hold — `setIamPolicy` on instances, databases, backups and backup
  schedules, which is a privilege-escalation surface — plus CMEK key handles and tag bindings.
  The Bigtable grant beside it stays admin because there the claim holds: `roles/bigtable.user` is
  data access only and nothing between it and admin creates an instance. Spanner has a middle role
  Bigtable does not.

  **This was wrong when it first landed.** [#469] granted `roles/spanner.admin` on the stated
  ground that "no predefined role narrower than admin can create an instance", naming only
  `roles/spanner.databaseAdmin` and `roles/spanner.databaseUser` as the alternatives.
  `roles/spanner.editor` was never checked. Round two of the self-review asked what the role
  actually contains and found it; [#476] narrowed the grant. Recorded rather than quietly fixed,
  because the false claim is what a reader of that merged pull request will otherwise find.

## Evidence

**Every row of [ADR-0076]'s rejection table is confirmed against the service** (2026-08-10, one run,
GoogleSQL, `google-cloud-spanner` 6.119.0): same status for all ten shapes, all reported per group,
including the `delete` of a missing row that is simply applied. The emulator was right — which is
worth recording precisely because it is not something the emulator could establish. The gated class
asserts each row, so a divergence on either side has to be declared.

**The batch-write request ceiling is 100 MiB** ([#441]). A request of roughly 12 MiB is accepted and
one of roughly 110 MiB is refused with `RESOURCE_EXHAUSTED: SERVER: Received message larger than max
(115350024 vs. 104857600)`. 104,857,600 is 100 MiB exactly, so the carve-out reading of the quotas
page holds, the tighter 10 MiB reading does not, and `MAX_BATCH_BYTES_LIMIT` needed no change. Two
things beyond the number: the refusal is **transport-level**, not a quota `INVALID_ARGUMENT`, and it
arrives under a status this connector classifies as transient — so breaching the ceiling is retried
rather than failed. The permanent test keeps three points rather than a search: 8 MiB as the
control, 12 MiB as the answer to [#441], and 110 MiB asserting the figure.

**Partition planning, and what it cannot claim.** Over 5,000 rows the service planned **one**
partition, for `partitionQuery` with default hints, with `maxPartitions = 16`, with
`partitionSizeBytes = 1024`, and for `partitionRead`. A table that small is one split, so this is a
measurement of that scale and not evidence about a large one. Seeding gigabytes to make the number
larger was declined: it would cost real money every week to demonstrate something Google already
documents as a hint.

**Root-partitionability: the emulator's verdict was right and its message was not.** The service
refuses `SELECT COUNT(*)`, `ORDER BY` and `LIMIT` with `INVALID_ARGUMENT: Query is not root
partitionable since it does not have a DistributedUnion at the root`, plus a link to the documented
conditions; a plain scan and a `WHERE` predicate are planned. [ADR-0085] framed the emulator's
stricter check as something the service might disagree with, and on every shape tried it did not.
What differs is the message, and that is the argument for surfacing the refusal unwrapped.

**Data Boost serves a read on the cheapest instance the suite can create.** A boosted
`partitionQuery` returned all 5,000 rows, and a job built with `dataBoostEnabled(true)` read the
table end to end — the first evidence anywhere in this repository that the flag does more than reach
the partition call. Its concurrency quota and its billing stay unmeasured; a suite this size reaches
neither.

**Change Streams recovery is accepted against both service dialects** ([#535], 2026-08-13).
The same ephemeral instance hosts GoogleSQL and PostgreSQL Change Stream databases.
The suite verifies both service result shapes, stream metadata and retention, exclusion options, an explicit-column warning, commit timestamps, heartbeat watermarks, child partitions, an intentional checkpoint failure, savepoint restore, an expired restore failure, and opt-in whole-ledger fallback.
The GoogleSQL recovery run delivered all 5,000 unique mutation ids and repeated 500 records across its inclusive checkpoint boundary; that count records the run rather than defining the at-least-once contract.

**A regression the ported tests caught, worth recording because it is a shell trap rather than a
Spanner one.** Moving the per-service sweep into a function called under `|| outcome=$?` suppresses
`set -e` inside that function, so the assignment from `gcloud … list` stopped being fatal and an
unauthenticated sweep would have reported "0 stale instances swept" and exited 0 — the one outcome
the original script's comments say a guardrail must never fake. The listing failure is now checked
explicitly, and `test_a_listing_that_fails_is_not_an_empty_sweep` is what caught it.

## Alternatives declined

- **A persistent instance in `opentofu/flink-gcp`.** It would remove a minute of provisioning per
  class and cost continuously for something used once a week, where an ephemeral one costs cents per
  run and nothing between runs. It would also need a Spanner admin role on the *apply* account,
  which nothing needs today.
- **One instance per run rather than per class.** [ADR-0044]'s reasoning transfers without
  amendment: two surefire forks would race a shared holder, and a single class must stay runnable by
  hand.
- **A negative IAM probe for `spanner.databases.useDataBoost`**, in the `e2e-no-pubsub` shape. It
  needs a second identity and an impersonation grant, and the positive path already shows the
  permission is honoured. Recorded as not done rather than left implied.

[#224]: https://github.com/flink-gcp/flink-connector-gcp/issues/224
[#441]: https://github.com/flink-gcp/flink-connector-gcp/issues/441
[#535]: https://github.com/flink-gcp/flink-connector-gcp/issues/535
[#469]: https://github.com/flink-gcp/flink-connector-gcp/pull/469
[#476]: https://github.com/flink-gcp/flink-connector-gcp/pull/476
[ADR-0044]: 0044-the-e2e-suite-creates-an-ephemeral-bigtable-instance-per-gated-class.md
[ADR-0076]: 0076-two-spanner-statuses-are-routed-and-a-request-failure-never-is.md
[ADR-0085]: 0085-the-spanner-batch-source-splits-by-server-planned-partition.md
