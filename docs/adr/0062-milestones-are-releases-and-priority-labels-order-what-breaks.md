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

# ADR-0062: Milestones are releases and priority labels order what breaks inside them

- Status: Accepted
- Date: 2026-08-07 (the `v0.3.0+` catch-all retired at 50 open issues)
- Issues: —
- Modules: all (tracker)

## Context

The `v0.3.0+` catch-all milestone had become the receptacle for every unfinished thing — 50
open issues — which expresses neither an order nor a release.

## Decision

**An issue carries three things: a milestone, a `priority:` label, and a `module:` or
`area:` label.**

- **One milestone per release through the public one**: `v0.3.0` correctness on shipped
  paths, `v0.4.0` completeness and performance of the shipped connectors, `v0.5.0` sources,
  `v0.6.0` Spanner, `v0.7.0` Table API and the deferred features, `v1.0.0` **going public** —
  publishing, the release checklist and everything that waits on a public repository. Each
  milestone's description carries its theme; read it rather than guessing from the number.
- **Priority labels are ordered by *what breaks*, not by urgency**: `priority:P0` is a
  shipped path where data breaks silently or the job stays green while broken, `P1` is
  correctness with a narrower blast radius or a blocker for going public, `P2` is a feature,
  performance or guardrail that breaks nothing by waiting, `P3` is a future feature or one
  blocked outside the repository.
- **Milestone and priority are orthogonal on purpose** — the milestone says which release,
  the label says the order inside it, which a milestone alone cannot express.
- **GitHub sub-issues are used where a parent genuinely decomposes** ([#36] → [#220]–[#225]);
  a *cluster* of issues sharing one root cause is not that, and is recorded as a comment on
  the one to work instead, so each keeps its own closure record ([#348], holding
  [#349]/[#350]/[#351]).

[#36]: https://github.com/laughingman7743/flink-connector-gcp/issues/36
[#220]: https://github.com/laughingman7743/flink-connector-gcp/issues/220
[#225]: https://github.com/laughingman7743/flink-connector-gcp/issues/225
[#348]: https://github.com/laughingman7743/flink-connector-gcp/issues/348
[#349]: https://github.com/laughingman7743/flink-connector-gcp/issues/349
[#350]: https://github.com/laughingman7743/flink-connector-gcp/issues/350
[#351]: https://github.com/laughingman7743/flink-connector-gcp/issues/351
