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

# ADR-0065: Gated real-GCP suites are opt-in per command, and `just e2e` is the opt-in

- Status: Accepted
- Date: 2026-08-02 ([#245])
- Issues: [#245]
- Modules: all connectors (tests/CI)
- Current behavior: root `AGENTS.md` § Build (`just e2e`, `just check-gated-tags`)

## Context

The real-GCP ITCases were gated on environment variables alone
(`@EnabledIfEnvironmentVariable`), which made the choice per *shell*, all-or-nothing:
`just verify` runs the same `integration-tests` execution, so a shell holding
`BIGTABLE_IT_PROJECT` created two one-node Bigtable instances on every full build — billed
suites running as a side effect of an ordinary verify.

## Decision

Each gated ITCase carries `@Tag("gated")`, which the root pom's `test.excluded.groups`
excludes from every surefire execution; **`just e2e` is the only thing that clears it**, with
`-Dtest.excluded.groups=`. That makes the choice per command instead of per shell, for
`./mvnw verify` as much as for `just verify`. The environment annotation stays exactly where
it is (the E2E discovery greps it), so **the two markers must be kept together**, which
`just check-gated-tags` enforces in both directions — a gate with no tag runs a billed suite
during any `verify` in a holding shell, a tag with no gate runs nowhere at all, since
`just e2e` selects by the gate. The check is deliberately **gate-agnostic**, matching the
annotation rather than the three variables the E2E workflow sets, so
`BigQueryDefaultStreamSchemaEvolutionITCase` — outside that suite on purpose, ~2 h against
the real service — is covered too. Because the recipe is now the opt-in, its pre-flight makes
a missing variable an error, and a post-run assertion (`scripts/e2e-gated-its.sh`, which
derives the class list from the gating annotation) checks the gated classes actually
executed.

[#245]: https://github.com/laughingman7743/flink-connector-gcp/issues/245
