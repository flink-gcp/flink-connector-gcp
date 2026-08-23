---
title: Contributing
type: docs
weight: 20
---

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

# Contributing

Contributions are welcome, and they start with an issue rather than a pull request — the same
consensus-first shape as [Apache Flink's contribution
process](https://flink.apache.org/how-to-contribute/contribute-code/). Agreeing on the problem
and the approach before code exists is what keeps a pull request reviewable and keeps effort
from being spent on a change that would not be accepted.

## Before writing code

Open an issue describing the problem or the proposal, or comment on an existing one, and reach
agreement on the approach there. For a typo-level fix a pull request alone is fine; for
anything that changes behavior, a public API, or a settled design, the discussion comes first.

## Pull requests

- Open the pull request **as a draft**, with the repository's pull-request template filled in —
  the `WHAT` and `WHY` sections are what the review rounds below audit against.
- Carry a closing reference — `Closes #N` — for the issue the pull request resolves. An
  ordinary mention ("related to #N") leaves the issue open after the merge, silently. A trivial
  fix that needed no issue has nothing to close.
- Build and test locally as [Development]({{< relref "docs/development" >}}) and
  [Testing]({{< relref "docs/development/testing" >}}) describe, and run `just format` before
  committing.
- Write commit messages, pull-request text and code comments in English.
- A behavior or public-API change carries its documentation update in the same pull request.

## AI-assisted contributions

AI-assisted contributions are welcome — most of this repository is developed that way. What
does not shrink with the authorship is the review work. Before a pull request is called ready
it goes through two self-review rounds — the first asks whether the code does what the
description says, the second whether the description is true
([ADR-0060]({{< param BookRepo >}}/blob/main/docs/adr/0060-self-review-is-two-rounds-and-round-two-audits-the-descriptions-claims.md)) —
and then an independent review by a model that did not write the change
([ADR-0130]({{< param BookRepo >}}/blob/main/docs/adr/0130-an-independent-review-follows-the-two-self-review-rounds.md));
when no second model is available, that round is recorded as skipped with the reason rather
than omitted in silence.
Findings and the reasons for any deferral are recorded on the pull request, so the review
history is readable by the next contributor. The licensing rules below also bind AI-assisted
work with particular force: a model can reproduce or closely follow code it has seen, and the
disclosure obligation does not shrink because the borrowing was the model's.

The repository ships its agent tooling rather than assuming it: `AGENTS.md` holds the
development rules an agent loads, the skills under
[`.agents/skills/`]({{< param BookRepo >}}/tree/main/.agents/skills) encode the workflows
([Checks]({{< relref "docs/development/checks" >}}) describes them), and `.mcp.json` configures
two MCP servers — [Serena](https://github.com/oraios/serena) for Java symbol discovery,
reference graphs and symbol-aware refactoring (run in its no-memories mode, so the tracked
guidance, docs and ADRs remain the only source of truth), and
[Context7](https://github.com/upstash/context7) for current third-party library documentation,
asked version-specific questions resolved from this repository's POM and BOM.

## Licensing and attribution

Write code from scratch wherever possible. When a contribution adapts or closely follows code
from another project, that fact must be disclosed in the pull request — the source project, its
license, and what was taken — and recorded durably in both the module README's provenance section
and the repository-level `NOTICE` file. Adapted code keeps
or follows the original copyright notice; original contributions carry this repository's
`The flink-gcp authors` header. This is practiced, not theoretical — the Pub/Sub
module began as an adaptation of
[GoogleCloudPlatform/pubsub](https://github.com/GoogleCloudPlatform/pubsub) and nine of its
files still carry the upstream `Google LLC` header; the module README's provenance section and
[ADR-0123]({{< param BookRepo >}}/blob/main/docs/adr/0123-a-pubsub-file-keeps-its-upstream-notice-while-it-still-carries-upstream-expression.md)
record how such a determination is made. An undisclosed adaptation is a licensing defect the
reviewers cannot catch, which is why the disclosure is the contributor's obligation.

## Design decisions

Decisions with lasting consequences are recorded as architecture decision records under
[docs/adr]({{< param BookRepo >}}/blob/main/docs/adr/README.md), whose README describes when a
record is written and how. Before proposing a change to a settled design, read the record that
settled it: a refinement updates the existing record, and a reversal adds a superseding one. A
pull request that changes a recorded design without touching its record is incomplete.
