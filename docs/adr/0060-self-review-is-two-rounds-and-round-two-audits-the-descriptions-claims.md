<!--
Copyright 2026 laughingman7743

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

# ADR-0060: Self-review is two rounds with different lenses, and round two audits the description's claims

- Status: Accepted
- Date: 2026-08-06 (measured across PRs
  [#314](https://github.com/laughingman7743/flink-connector-gcp/pull/314) and
  [#317](https://github.com/laughingman7743/flink-connector-gcp/pull/317))
- Issues: —
- Modules: all (workflow)
- Current behavior: root `CLAUDE.md` § Workflow rules (the imperative form, with the review
  tooling)

## Decision

Every draft PR is self-reviewed before review is asked for, in **two rounds that answer
different questions** — this is not "review twice":

- **Round one asks whether the code does what the description says**, taking the description
  as the specification. It applies simplification and efficiency findings, not only
  correctness ones.
- **Round two asks whether the description is true** — which is why its lenses point outward
  rather than at the diff: the user meeting the error, the operator reading the dashboard,
  the blast radius of a move, an adversary trying to defeat the invariant.

Three rules the rounds share:

- **Converging agents are one source** — two lenses reporting the same thing is one finding
  to verify, not two, and it is verified against the *pinned* dependency version rather than
  whichever one an agent happened to read.
- **A deferred measurement is a claim**: if a premise was flagged as reasoned-but-unmeasured,
  round two measures it, because the flag is not a substitute.
- **The mutation batch is re-run after acting on a review**, not only before — PR [#317](https://github.com/laughingman7743/flink-connector-gcp/pull/317)'s
  rework left alive a mutant that had been alive all along, because no test pinned that the
  call sites feed the counter the metric reports.

## Evidence

Measured on 2026-08-06 across PRs [#314](https://github.com/laughingman7743/flink-connector-gcp/pull/314) and [#317](https://github.com/laughingman7743/flink-connector-gcp/pull/317), both of which had passed round one and were
CI-green: round two found, in each, **claims written in the PR's own javadoc, docs or
description that were false** — "it reaches SQL unchanged as an `IllegalStateException`"
(`FactoryUtil` wraps everything a factory throws), "the only shape that works" (it forced the
storage, not the instrument), "the repository's first main-code static" (it is the second),
"scoped to the job's class loader" (true for a job jar, false for the `lib/` deployment the
docs recommend). None of those is reachable from a lens aimed at the diff.

## Consequences

- The cost is real — three agents plus verification, and on PR [#317](https://github.com/laughingman7743/flink-connector-gcp/pull/317) round two changed the
  design — so the full second round is for changes whose description makes claims about
  framework behaviour, deployment, or "this is the only way", not for a typo fix.
- Findings *and* deferrals, with their reasons, are recorded as a PR comment; recording is
  not routing, which ADR-0061 governs.
