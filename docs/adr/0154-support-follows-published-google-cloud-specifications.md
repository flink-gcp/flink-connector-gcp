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

# ADR-0154: Support follows published Google Cloud specifications

- Status: Accepted
- Date: 2026-09-06
- Issues: [#1238](https://github.com/flink-gcp/flink-connector-gcp/issues/1238), [#1239](https://github.com/flink-gcp/flink-connector-gcp/issues/1239)
- Supersedes: only [ADR-0104](0104-exactly-once-modes-use-service-native-replay-protection-and-pass-a-performance-gate.md)'s blanket Cloud Tasks G0 stop and mandatory investigation prerequisites
- Modules: bigquery, pubsub, cloudtasks, bigtable, spanner
- Current behavior: `docs/content/docs/connectors/delivery-guarantees.md`

## Context

The Cloud Tasks G0 investigation in ADR-0104 treated unresolved questions about all proposed retention histories and late request effects as a stop for dependent work.
That mixed the limits of a stronger recovery claim with the project's responsibility to implement behavior within Google's published specification.
Google already documents task-name deduplication and configured tombstone retention.
The connector can rely on that specification without obtaining an individual vendor confirmation or offering stronger service guarantees.

This ADR supersedes only the blanket stop on dependent Cloud Tasks work and the mandatory investigation prerequisites in ADR-0104's G0 decision.
The original stronger recovery claim remains unproved; all other decisions, correctness requirements, measurements and performance gates in ADR-0104 remain accepted and are incorporated here by reference.
[ADR-0162](0162-cloud-tasks-implementation-precedes-final-performance-acceptance.md) subsequently supersedes the separate Cloud Tasks primitive-pass prerequisite, allowing implementation before final performance acceptance while retaining the published-service scope and release thresholds.

## Decision

The service assumptions for every connector come from Google's official documentation for the API version, resource type and configuration in use.
User documentation links to those official sources for service behavior, limits and guarantees.
The project supports its documented connector behavior within that scope and does not extend Google's service guarantees.
Individual vendor confirmation of published behavior is not a development or release prerequisite.

The implementation remains responsible for constructing the intended requests and correctly handling responses, retries, errors, checkpoint state and recovery under the published semantics.
A connector guarantee must state its prerequisites and exclusions, and the implementation must satisfy that guarantee for every case within the stated scope.
If an intended guarantee depends on undocumented service behavior, narrow or decline that guarantee; do not assume it or hold unrelated supported behavior for a private assurance.
An ordinary failure that the published API permits, such as an [ambiguous timeout outcome](https://docs.cloud.google.com/tasks/docs/reference/rpc/google.rpc#code), remains an implementation concern inside the supported scope.
Pointing users to Google documentation does not excuse a connector defect.

Tests verify the implementation against those semantics, including failure and recovery cases.
Real-service tests can verify integration and reveal discrepancies, but neither an emulator result nor successful real-service samples create a stronger service guarantee.
Use the published service contract as the premise rather than requiring tests to prove it for every possible service history.

## Cloud Tasks application

The [v2 task-creation reference](https://docs.cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues.tasks/create) defines same-name collisions and name reuse.
The [v2beta3 Queue reference](https://docs.cloud.google.com/tasks/docs/reference/rest/v2beta3/projects.locations.queues) defines configured `tombstoneTtl` retention after deletion or execution.
The sink creates tasks through v2; the TTL field belongs to v2beta3 queue administration and is absent from the v2 Queue resource.
Those are the service references for the applicable API and queue configuration.
Task creation deduplication does not suppress [duplicate handler execution](https://docs.cloud.google.com/tasks/docs/common-pitfalls#duplicate_execution).

Today's sink remains stateless, with optional stable task IDs and `ALREADY_EXISTS` treated as success while the service remembers the name.
It does not administer retention, enforce a bounded recovery deadline or implement checkpoint-coordinated creation.
The protocol work in [#1240](https://github.com/flink-gcp/flink-connector-gcp/issues/1240) may select a supported scope based on the published specification; the performance repeat in [#1241](https://github.com/flink-gcp/flink-connector-gcp/issues/1241) retains ADR-0104's gates.
Neither requires a private vendor assurance.
ADR-0104's stale-request counterexample still limits the original stronger proposal: a local timeout alone does not establish that a create cannot take effect.
This clarification supplies no numerical recovery bound and does not approve an exactly-once label for an unvalidated protocol.

## Consequences

ADR-0104 retains its service-native replay primitives, correctness requirements, measurements and performance thresholds.
Its Cloud Tasks evidence informs the next protocol's scope without making an exhaustive lifecycle investigation a prerequisite for documented behavior.
Service-dependent claims remain tied to explicit official references, and connector correctness remains a project responsibility.

## Alternatives declined

- Require individual vendor confirmation before using published behavior: this adds an assurance beyond the project's support boundary.
- Infer stronger guarantees from successful probes or client deadlines: observations and client controls cannot supply an undocumented service contract.
- Limit the project to documentation links without verifying its implementation: users still need correct requests, error handling and recovery within the documented scope.
