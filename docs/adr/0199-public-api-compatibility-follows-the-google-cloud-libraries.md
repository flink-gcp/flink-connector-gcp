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

# ADR-0199: Public API compatibility follows the Google Cloud libraries, and no japicmp gate checks it

- Status: Accepted
- Date: 2026-11-07
- Issues: [#2068](https://github.com/flink-gcp/flink-connector-gcp/issues/2068),
  [#2064](https://github.com/flink-gcp/flink-connector-gcp/issues/2064)
- Modules: all
- Current behavior: `docs/content/_index.md` § API reference

## Context

This record supersedes
[ADR-0124](0124-the-stability-boundary-at-1-0-0-is-a-promoted-public-entry-surface-checked-by-japicmp.md).
ADR-0124 promised that a `@Public` type breaks only in a major release, and wired japicmp into
every `verify` to compare each connector jar against the latest published release.

That promise cannot be stronger than the libraries the connectors are built on.
google-cloud-java follows semantic versioning per GA library only, and a surface it marks
`@BetaApi` may break in a minor or patch release.
`com.google.cloud:libraries-bom` promises nothing between its own versions: 26.50.0 moved
protobuf-java from 3.x to 4.28.3, and its release notes warn of binary-compatibility failures for
code that calls the removed runtime methods.
This project adopts each new libraries-bom on `main` as it ships
([ADR-0191](0191-libraries-bom-updates-are-proposed-weekly-by-renovate.md)), so every minor
release carries the BOMs published since the previous one.
Many `@Public` signatures name Google Cloud or protobuf types, so those BOM updates can change the
frozen API in a minor release.
japicmp does not see such a change, because it compares only this project's own classes.

The service or the SDK can also withdraw a capability.
In [#2064](https://github.com/flink-gcp/flink-connector-gcp/issues/2064), the Spanner sink's
`maxCommitDelay` never reached the service, because `BatchWriteRequest` has no such field.
Under ADR-0124 the only option within 1.x was a deprecated setter that does nothing.

## Decision

**A public type may change incompatibly at a minor release, and every such change gets a
release-notes entry.**
This is the compatibility model of the Google Cloud libraries underneath, and the project no
longer promises more than they do.

- `@Public` marks the surface the project intends to keep.
  It changes when a dependency or a service forces it, or when it no longer does what it says,
  as with [#2064](https://github.com/flink-gcp/flink-connector-gcp/issues/2064).
- `@PublicEvolving` marks supported API that is still expected to move.
- No public signature changes in a patch release.
  A patch release therefore takes no libraries-bom update that changes a public signature; such an
  update waits for the next minor release.
- `@Experimental` and `@Internal` may change in any release, as before.
- API that does nothing, or that the project can no longer support, is removed rather than kept
  behind `@Deprecated`.
  The release notes name the removal.

**No build-time gate checks this.**
The japicmp declaration, the `japicmp.referenceVersion` property, the `japicmp-patch` profile,
the per-module `japicmp.skip` properties and the release recipe's `-Djapicmp.skip=true` are
removed.
The release-notes entry is the artifact a break owes, and the release tracker's checklist holds
it.

**The tier annotations and their assignment rules stay.**
`just check-declared-api-tiers` still requires every top-level main-tree type to carry one.
The tier rule of
[ADR-0141](0141-a-surfaces-stability-tier-is-set-by-what-can-reshape-its-inputs-and-outputs.md)
still decides a new type's tier, and the signature closure ADR-0124 introduced still applies: a
`@Public` signature names only `@Public` types or one of the closure stops ADR-0124 and its
revisions record (the `@Experimental` types, and the `@Internal` split, enumerator-state and
`RetrySchedule` types), because a stable-labelled method that requires a less stable type promises
less than it reads.
ADR-0124's census, closure stops and revisions remain the record of those assignments.

## Evidence

Measured on 2026-11-07 at `7c11f367`, over the main trees:

- 95 files carry `@Public`; 31 of them import a `com.google.*` type, and 14 import
  `com.google.protobuf` (mostly `ByteString` in the failure types, plus `Descriptors` and
  `Message` in the BigQuery serializers).
  Public signatures that name a vendor type include Pub/Sub's `PubsubMessage`, Spanner's
  `Mutation`, Bigtable's `RowMutationEntry` and Cloud Tasks' `Task`.
- From v1.0.0 to v1.3.0 the root POM never carried a japicmp exclusion.
  The first one was added for [#2064](https://github.com/flink-gcp/flink-connector-gcp/issues/2064), to remove an option that never took effect.

## Alternatives declined

- **Keep the gate and break only at a major release**: every libraries-bom bump that reshapes a
  vendor type in a public signature would then need a major release.
  The gate would still not detect that break, so the promise would rest on a check that cannot
  see the most likely cause of a break.
- **Keep the gate as a report that never fails the build**: a report nobody is required to read
  keeps the release-procedure costs (the reference-version bump, the release-build skip that
  ADR-0124 found load-bearing) without holding anything.
- **Keep deprecated shims for withdrawn capabilities**: a setter that validates a value and then
  drops it tells the user the option works.
  Removing it fails the job at compile time or at planning, where the user can see it.

## Consequences

- `just verify` and every CI lane no longer resolve a published reference artifact, so a Central
  outage can no longer turn the check into a silent warning, and the release build needs no
  japicmp flag.
- The release tracker no longer bumps `japicmp.referenceVersion` or enumerates japicmp changes.
  It instead requires the release notes to name every incompatible public change.
- Users upgrading across a minor release read the release notes for breaking changes, as they
  already must for libraries-bom.
- The nullable older-serialized-form fields on options classes are Java-serialization
  compatibility, not japicmp compatibility, and this decision does not remove them.
  [#2071](https://github.com/flink-gcp/flink-connector-gcp/issues/2071) removes them where an
  options object travels only in the job graph.
