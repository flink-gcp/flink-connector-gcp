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

# ADR-0126: Documentation link fragments are checked against the rendered site

- Status: Accepted
- Date: 2026-08-22 (measured 2026-08-22)
- Issues: [#867](https://github.com/flink-gcp/flink-connector-gcp/issues/867),
  [#777](https://github.com/flink-gcp/flink-connector-gcp/issues/777)
- Modules: all (docs)
- Current behavior:
  [`just check-doc-fragments`](../../justfile), run by the documentation workflow

## Context

Two tools each validated part of a documentation link and neither validated a *cross-page*
fragment.
markdownlint's MD051 judges in-page `[](#anchor)` fragments only, and judges them against the
Markdown.
Hugo's `relref` resolves and validates the *page* and never the fragment: measured, a
`{{< relref "page#not-a-heading" >}}` builds clean under `--panicOnWarning` and emits the fragment
verbatim, so the build reddens on a broken page reference and stays green on a broken anchor.

Renaming a heading therefore broke every inbound link to it silently.
PR [#836](https://github.com/flink-gcp/flink-connector-gcp/pull/836) renamed
`## No emulator path` on the BigQuery examples page while `docs/content/docs/examples/_index.md`
went on linking to `docs/examples/bigquery#no-emulator-path`.
The emitted HTML pointed at a fragment its target page no longer contained, and `just lint`,
`just docs` and CI all reported clean; a self-review sweep found it.
The three-arm control taken during that review is what establishes the gap rather than merely
suggesting it: the real bug produced zero markdownlint issues and a green Hugo build, while a
broken *page* reference did fail — which is what makes the gap easy to mistake for coverage.

The site's prose carries 172 cross-page and 172 same-page fragments today, all of them resolving,
so the check starts green.

## Decision

Every internal link fragment the documentation prose writes must reach an anchor the built site
emits, and `just check-doc-fragments` holds it there.

**The input is the rendered site, not the Markdown.**
Hugo's heading ids come from a *configured* algorithm — `markup.goldmark.parser.autoidtype`, which
resolves to `github` on this site — so a checker with those rules baked in would agree with the
render today and go silently wrong the day that setting moved.
Reading what was emitted cannot, and it covers an anchor a shortcode or raw HTML defines without
knowing anything about either.
The recipe depends on `docs` rather than judging whatever is in `docs/public`, because a stale build
gives a confident wrong verdict in both directions.
That dependency is not sufficient on its own, so `just docs` gained `--cleanDestinationDir`: Hugo
leaves orphans in `publishDir`, and `docs/public` is gitignored, so a page deleted or renamed
locally keeps a built copy across branch switches that a link would then resolve against.

**Ids are collected from the whole page; links only from the prose.**
Two kinds are the theme's rather than this repository's, and both are satisfied by construction.
Links outside `<article>` are the sidebar menu and both copies of the table of contents.
Links *inside* the element whose id they name are the self-anchors hugo-book renders beside every
heading, as `<h2 id=x>Title<a class=anchor href=#x>#</a></h2>`.
That second rule is written against the enclosing id rather than against the theme's class name,
so it holds whatever the theme calls it.
Skipping them is not only tidiness: measured on this site there are 430 of them against 172 prose
links, so left in they would hold the "no fragment links" floor up while every prose link had
stopped being seen.
With both rules applied the check counts 172 cross-page and 172 same-page links, which is exactly
what grepping the Markdown for the two link forms finds — the render and the source agree on what
the prose writes.

**The base path is read from `hugo.toml`, never hardcoded.**
A hardcoded prefix would classify every internal link as external the day `baseURL` moves, and a
check that judges nothing reports clean.
A link path is resolved against the page's own URL before it is measured against that prefix, so
`../target/#deep` — which a browser resolves inside the site — is not reported as leaving it.

**Three site-wide floors fail as infrastructure rather than passing empty**: no built pages, no page
carrying an `<article>`, and no internal fragment links at all.
This check's whole risk is finding less than it claims, which is indistinguishable from a clean run
unless the checker says so.

**No allowlist, and therefore no `curate-*` skill.**
ADR-0000 states the rule and records the exemption for `check-gated-tags`; `check-javadoc-links` and
`check-skill-frontmatter` take it on the same argument.
A fragment resolving to no anchor is a link to correct or a heading to restore, and the failure
carries that repair: the Markdown file, every line writing the fragment, and the nearest ids on the
target page.

## Consequences

- The documentation workflow's build step runs `just check-doc-fragments`, which builds the site
  once and then sweeps it, so the check costs no extra build and the artifact it uploads is the
  swept one.
- A heading rename now fails on the pull request that makes it, naming the pages that linked to it.
- Same-page fragments are judged twice, by MD051 against the Markdown and by this against the
  render. That is redundant on an ordinary heading and is not redundant on an anchor only the
  render has. Both must pass; where they would disagree, the render is the one readers get.
- `docs/public` is a build output, so this check cannot run inside `just lint`, which stays offline.
- Synthetic sites under `scripts/tests/` cover the parser and the floors, with one mutant per rule
  each killed by the case that pins it. The fixtures render a `<head>` and a heading self-anchor
  because both are where a plausible cleanup — dropping the `name` attribute's `<a>` guard, or the
  enclosing-id rule — otherwise leaves the suite green.
- Nothing here judges whether a fragment points at the *right* heading, only that its target
  exists. Review retains that.

## Alternatives declined

- **Parse the Markdown and re-implement Hugo's slugification.**
  It would need no site build and could run in `just lint`.
  What it would copy is a *setting* — `markup.goldmark.parser.autoidtype` — so the copy would agree
  with the render today and nothing would report the day it stopped.
  It would also see nothing of the anchors shortcodes and raw HTML emit, and nothing of what a
  theme upgrade does to either.
- **Check external link liveness too.**
  A different failure mode with a different cost, and it would make a per-pull-request check depend
  on the network.
- **An allowlist for links that are known to be broken.**
  There is nothing here to forgive: the repair is always mechanical, and an entry forgiving nothing
  is a claim nobody can check.
