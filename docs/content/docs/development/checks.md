---
title: Checks
type: docs
weight: 15
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

# Checks

Beyond the Maven build, CI runs a suite of repository-specific checkers — scripts under
`scripts/` (mostly Python, some shell), most behind a `just check-*` recipe.
Several are deliberately two-way: they
hold the documentation to the source *and* the source to the documentation, so an option that
loses its table row fails the same check as a table row that loses its option. A contributor
meeting one for the first time should know that its failure message names the repair, and that
it is cheaper to run the relevant checker locally than to discover it on the pull request.

| Checker | What it holds |
|---|---|
| `just check-license-headers` | Every Java source carries a complete copyright-bearing or canonical ASF Apache-2.0 header (stricter than apache-rat, and the first step of `just verify` — the only checker inside the build) |
| `just check-option-docs` | Every connector option is documented, and every documented option is real |
| `just check-metric-docs` | Every connector metric is documented, and every documented metric is real |
| `just check-flink-api-tiers` | Main sources depend only on allowlisted Flink API stability tiers |
| `just check-javadoc-links` | Javadoc member references resolve to the members they name, every public or protected member of a tier-annotated type carries Javadoc, and a `ConfigOption` constant's Javadoc equals its runtime description (in-project targets; the checker states its own limits) |
| `just check-readme-examples` / `just check-doc-snippets` | Source-backed Java examples stay synchronized with their tagged source regions and those regions compile against the working tree — the first covers the module READMEs, the second runs it plus the docs-page and Javadoc halves and the compile; a deliberately abbreviated example carries its classification marker and says so in prose |
| `just check-notice <module>` / `just check-notice-sources` | A shaded jar's generated `NOTICE` matches what it bundles, and the pinned licence texts still match what is served |
| `just check-gated-tags` | Every credential-gated test carries both its environment gate and the tag that keeps it out of ordinary builds |
| `just lint` / `just test-scripts` | The scripts, workflows, rendered Markdown and OpenTofu formatting — and the checkers' own test suite |

`just --list` is the full index; the table stays at this altitude so it does not chase every
recipe change.

## When a check fails

Each checker with judgment calls in its repair pairs with a skill under
[`.agents/skills/`]({{< param BookRepo >}}/tree/main/.agents/skills) — a playbook that says what
a given failure message means, which responses are acceptable, and which decisions must go to a
maintainer. Coding agents load them by name; a human contributor can read them the same way.
The `curate-*` skills answer their checker's failures (`curate-option-docs`,
`curate-metric-docs`, `curate-flink-api-tier`, `curate-licence-source`), the `maintain-*`
skills own the compiled documentation examples, `add-a-connector-option` covers everything a
new option owes, and the workflow skills (`push-pr-branch`, `self-review`,
`self-review-round-two`, `independent-review`, `project-memory`) encode the pull-request
process that [Contributing]({{< relref "docs/development/contributing" >}}) describes.
`just check-skill-frontmatter` validates the skills themselves.
