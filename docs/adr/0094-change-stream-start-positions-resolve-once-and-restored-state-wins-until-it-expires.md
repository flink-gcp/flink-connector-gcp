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

# ADR-0094: Change-stream start positions resolve once, and restored state wins until it expires

- Status: Accepted
- Date: 2026-08-10
- Issues: [#492](https://github.com/laughingman7743/flink-connector-gcp/issues/492),
  [#35](https://github.com/laughingman7743/flink-connector-gcp/issues/35),
  [#222](https://github.com/laughingman7743/flink-connector-gcp/issues/222),
  [#535](https://github.com/laughingman7743/flink-connector-gcp/issues/535)
- Modules: base, bigtable, spanner
- Current behavior: generated API reference for `base.source.StartPosition`; the connector pages
  join when the change-stream sources land

## Context

The Bigtable and Spanner change-stream sources have different protocols and discover retention
through different service APIs, but expose the same four ways to choose an initial position and
face the same failure mode after an outage longer than retention. Leaving resolution to each
connector would duplicate the user-visible boundary behavior: what "now" means, whether a position
before retention is accepted, and whether an expired restore may silently skip records.

The shared type is implemented before its two consumers because their settled implementation order
is [#492](https://github.com/laughingman7743/flink-connector-gcp/issues/492), then
[#35](https://github.com/laughingman7743/flink-connector-gcp/issues/35), then
[#222](https://github.com/laughingman7743/flink-connector-gcp/issues/222). It is nevertheless a
two-consumer API rather than a speculative base utility: both connector designs name the type and
their builders' mapping onto it.

## Decision

**`base.source.StartPosition` is an immutable `@PublicEvolving` value object with four factories:**
`earliest()`, `latest()`, `at(Instant)` and `ago(Duration)`. It is public because users configure
both change-stream source builders with it, and `Serializable` because it crosses Flink's job
configuration boundary. `ago` requires a strictly positive duration; a zero-distance start is
spelled `latest()`.

**The internal resolver captures one startup instant and lazily discovers retention at most once.**
`latest()` resolves to that instant without a retention lookup. `earliest()` needs retention, as do
`at` and `ago` after future rejection because both must be checked against retained history. Every
restore also needs retention for expiry detection. This corrects the pre-implementation Bigtable
note that said the table-admin permission was needed only for `earliest()`: the clamp contract makes
the permission necessary for `at` and `ago`, and the restore contract makes it necessary on every
resume.

**The computed earliest position is `now - retention + 1 minute`.** Retention is a moving window;
the fixed minute keeps the request admitted after the time spent discovering partitions and opening
the first read. A requested absolute or relative position after `now` is rejected. One before the
computed earliest is clamped, with a warning naming the requested position, computed earliest and
unavailable range. The resolver logs under a connector-supplied class so connector-scoped logging
continues to see the warning.

**Configured start position applies only to a fresh start; restored split and enumerator state
wins.** A restored partition whose read position or low watermark predates the computed earliest is
expired. The default is to fail with recovery guidance: restart without state and choose a retained
`StartPosition`. An explicitly configured fallback resolves against the same startup instant and
retained window and warns with the lost range. Bigtable can restart each expired range separately.
Spanner instead discards the whole partition ledger and starts one null-token query at the fallback:
advancing an old token can skip its terminal child-partitions record and lose every descendant.
Data loss is therefore never an unreported consequence of restore.

Retention discovery and residual service-error translation stay in the connectors. Bigtable reads
the table's change-stream retention; Spanner reads its information-schema option and owns the
configurable default for an absent row. Neither connector may classify an out-of-window request by
matching undocumented service-error text.
Spanner initializes all stream metadata eagerly, including for a fresh `latest()` start, so it can validate the stream definition and hold restored reader splits until the expiry decision is complete.
This refines the shared resolver's lazy lookup rule without changing Bigtable's permission boundary.

## Evidence

The gated Spanner acceptance reads one-day explicit retention and the service's absent-row seven-day default through both GoogleSQL and PostgreSQL information schemas.
It creates reader-owned state two days in the past, restores it against the one-day stream, and verifies that the coordinator rejects it before a partition query reaches Spanner.
With `resumeFallback(StartPosition.latest())`, the same savepoint discards its stale reader splits, starts one new null-token query, and consumes a later mutation.
The separate pre-creation test asserts the connector's documented guidance while treating the service exception type, rather than undocumented message text, as the classification boundary.

## Consequences

- Bigtable needs its table-retention read permission for `earliest`, `at`, `ago` and restore, but
  not for a fresh `latest` start.
- Every partition in one restore is compared against the same clock instant and earliest boundary;
  a slow loop cannot make later partitions expire against a newer clock sample.
- The base resolver exposes expiry evidence separately from fallback resolution so a connector can
  make one recovery decision for a dependent group of partitions.
- The future connector builders expose `latest()` as their default and an opt-in
  `resumeFallback(StartPosition)`; those APIs land with the connector sources rather than this
  base-only change.

## Alternatives declined

- **Resolve separately in each connector.** Declined because the policy, not only the four-value
  carrier, is shared; two copies could diverge on clamping or restore data loss.
- **Discover retention eagerly for every fresh start.** Declined because `latest()` cannot clamp
  and cannot be expired, so the service call and Bigtable permission would buy no behavior.
- **Wait for the service to reject expired state.** Declined because neither service documents a
  distinct expiry error shape, while the checkpoint already carries the timestamp needed to make
  the decision explicitly.
