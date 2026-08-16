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

# ADR-0042: `INVALID_ARGUMENT` alone is routed, and both halves of the condition read the chain

- Status: Accepted
- Date: 2026-08-02 (settled on [#207] the same day, reversing [#33]'s design comment, which
  listed `FAILED_PRECONDITION` too)
- Issues: [#33], [#207]
- Modules: bigtable (the rule is repository-wide: only state-independent codes may be
  droppable)
- Current behavior: `docs/content/docs/connectors/datastream/bigtable.md` § Failed-mutation
  policy

## Decision

- **`INVALID_ARGUMENT` alone is routed, and `FAILED_PRECONDITION` deliberately is not.** The
  rule: only a status an authority defines as *state-independent* may reach a handler that may
  drop it. gRPC defines `INVALID_ARGUMENT` as "problematic regardless of the state of the
  system" and AIP-194 lists it must-not-retry, while `FAILED_PRECONDITION` and `OUT_OF_RANGE`
  are explicitly state-dependent — so a mutation rejected with one of those might be accepted
  later, and dropping it is data loss. **Cite the definition rather than the plausibility of the
  failures a code names.** Everything else — `PERMISSION_DENIED`, `UNAUTHENTICATED`, and
  anything the client's own retries gave up on — is fatal. `NOT_FOUND` (a missing table *or*
  column family) left this class in ADR-0073: it is checked ahead of everything, repaired under
  `CREATE_IF_NEEDED` and fatal under `CREATE_NEVER` — never routed, so this ADR's rule that only
  a state-independent status reaches a handler is unchanged.
- **Routing takes both halves of a condition, and they read the cause chain differently.** No
  transient status *anywhere* in the chain (so an unstable service cannot produce a dead letter
  even behind a data-shaped status — a property of this code, not of the client surfacing one
  status per failure), **and** the chain's *first* classifiable status is `INVALID_ARGUMENT` (so
  an `INVALID_ARGUMENT` buried under an `INTERNAL` describes the inner call and does not discard
  a record over a server-side failure). The two mistakes are mirror images; both are pinned by
  test. `BigtableErrorClassifier.firstMatching(throwable, codes)` is the shared primitive, the
  same shape `CloudTasksWriter` uses.

[#33]: https://github.com/laughingman7743/flink-connector-gcp/issues/33
[#207]: https://github.com/laughingman7743/flink-connector-gcp/issues/207
