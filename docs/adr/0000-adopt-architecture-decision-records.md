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

# ADR-0000: Decisions are recorded as ADRs in `docs/adr/`, unrendered

- Status: Accepted
- Date: 2026-08-07
- Issues: —
- Modules: all

## Context

By August 2026 the repository's ~150 settled decisions lived in seven `CLAUDE.md` files
(3,792 lines) as flat bullet lists, plus GitHub issues and the docs pages. Measured problems:
only ~10% of the root file was operational guidance; single bullets had grown past 200 lines
with sub-items lettered `a, a′, a″ … a⁶`; 171 distinct issue references — 84% of them closed —
were the only citation apparatus, so tracing a decision meant issue archaeology; corrections
accreted inline because a superseded decision had nowhere to go (one pre-#324 factual error was
copied into six places and outlived two correction sweeps); and decisions were recorded twice, in
a docs page for users and a `CLAUDE.md` for sessions, with nothing holding the copies together.

## Decision

Settled decisions are recorded one per file under `docs/adr/`, in the house shape the three
former root-`CLAUDE.md` decision sections already had (decision-as-title, context, dated
evidence, alternatives declined with reasons, consequences). The boundary between the three
documentation homes:

- `docs/content/` pages keep current behavior and the rationale a **user** needs — unchanged.
- `docs/adr/` keeps the decision **event**: evidence, declined alternatives, supersession.
- `AGENTS.md` files keep the imperative **rules** a session must follow, each a few lines ending
  in a pointer to the ADR or docs section that carries the record. Agent guidance auto-loads
  (root always, module files on touch) and ADRs never do, so `AGENTS.md` must stay sufficient
  for compliance — and only for compliance.

Where a docs page already carries a decision's full operative record, the `AGENTS.md` entry
becomes a pointer to that page and **no ADR is written** — an ADR exists for what a user page
must not carry (declined alternatives, incidents, internal/CI/test decisions), not as a third
copy.

The archive is maintained through the ordinary development flow, and most pull requests never
touch it: design discussion stays on the issue (the `Design (settled YYYY-MM-DD)` comment,
which may simply say "Settled — see ADR-NNNN" once the file exists); the PR that implements a
settled decision carries the ADR in its diff; and a later PR that extends the decision edits
the same ADR in place. **The trigger is the residue, not the pull request: an ADR is owed
exactly where, before this archive, a decision record would have been owed to agent guidance,
README or docs page** — something was weighed and declined, measured, or chosen in a way a
later reader must not re-argue without engaging the reasoning. Granularity is the decision
*cluster*, not the pull request — a design evolving across several issues stays one file.

**A decision is revisable by design.** "Do not silently revisit" binds a session to engage the
record — argue against its evidence — not the project to keep the decision: when discussion
concludes a better architecture exists, the implementing PR adds a new ADR whose Context states
what it replaces and why, and flips the old one's status to `Superseded by ADR-NNNN`. The old
file stays, so supersession replaces inline correction and the chain of reasoning survives in
both directions.

## Alternatives declined

- **Rendering ADRs on the Hugo site** (`docs/content/docs/decisions/`): the archive records
  process — withdrawn claims, incident postmortems — which is not user documentation, and
  publishing it creates pressure to sanitize history. It would also add front-matter/shortcode
  obligations and depart from the content tree's Flink-layout mirror (`docs/hugo.toml` records
  that portability aim).
- **A top-level `adr/` directory**: `scripts/ci-maven-args.py` classifies `docs/**` as root-only
  (a ~1-minute rat build), while an unknown top-level path triggers the full reactor — so the
  ADR directory lives under `docs/` and CI needed no change.
- **In-place restructuring of `CLAUDE.md` only** (headings, status lines, an issue-side `design`
  label instead of new files): cheaper, but it deepens the dependency on GitHub as the archive —
  the exact problem being solved — and does nothing for the root file's per-session token cost.
- **A `design` label on decision-bearing issues**: the index below carries issue links both ways;
  a label is a second index with no review and no diff.
- **An index checker script**: declined for now under the measure-before-building rule. The
  known trigger: if the index in `README.md` is found to have drifted from the files twice, a
  `scripts/check-adr-index.py` in the house checker style (a `just` recipe, synthetic pytest
  cases, and an allowlist only if one is needed) is the recorded fix. The two properties this
  once also listed have since acquired counter-examples and are not part of the style:
  `check-gated-tags` has no `curate-*` skill because it has no allowlist to exercise judgment
  over. Java-aware checkers share the root project's locked Tree-sitter runtime, while
  `check-skill-frontmatter` keeps its single-script YAML runtime in PEP 723 metadata (ADR-0069,
  ADR-0118).

## Consequences

- `docs/adr/*.md` carry the Apache-2.0 HTML-comment header (apache-rat scans them; only
  `README.md` is rat-excluded by pattern) and explicit issue links (GitHub renders these files).
- markdownlint covers `docs/adr/**/*.md` via `.markdownlint-cli2.jsonc`; `lint.yaml`'s
  push-trigger paths list the directory.
- The hand-maintained index table in `docs/adr/README.md` is the discovery surface; keeping it
  current is part of adding an ADR.
- The original detailed `CLAUDE.md` corpus is retained under `.agents/references/`; concise
  `AGENTS.md` files route sessions to those records without loading the entire corpus by default.
