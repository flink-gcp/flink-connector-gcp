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

# ADR-0159: Documentation keeps two released minors and Development

- Status: Accepted
- Date: 2026-09-06
- Issues: [#731](https://github.com/flink-gcp/flink-connector-gcp/issues/731)
- Modules: docs, CI
- Current behavior: [Documentation versions](../content/docs/versions.md)
- Supersedes: ADR-0056's single unversioned publication; its API filtering and validation decisions continue below

## Context

Issue #731 deferred versioning until a pinned release needed its own documentation or a user asked for it.
The user has requested it, and 1.0.0 is published while `main` contains changes for the next release.
A site that follows `main` can therefore describe behavior absent from a deployed connector.

## Decision

Publish the latest patch of the current and previous connector minor, plus Development from `main`.
The count is `retained_minor_lines` in `docs/versions.toml`, inclusive of the current minor.
Order versions numerically across major boundaries; a patch of an older minor does not become the current series.
Select only published, non-draft, non-prerelease GitHub Releases with a bare `vX.Y.Z` tag.
The `X.Y.Z-1.20` artifact is part of the same connector release and earns no separate documentation slot.
The number mirrors Flink's [current-and-previous support policy](https://flink.apache.org/downloads/#update-policy-for-old-releases); it does not couple documentation retention to the supported Flink range.

Each run resolves the selected tags and `main` to immutable commit SHAs.
Each line builds its own source, dependencies, snippets and aggregated Javadoc in a separate checkout and runner.
The controller adds common navigation only after every build and validation succeeds.
It checks each build's recorded version and SHA before assembly.
The output directory is fresh, so a retired series or removed page cannot survive a deployment.
The pipeline rebuilds retained sources rather than treating an expiring Actions artifact or cache as an archive.

The current release has a minor URL such as `1.0/`.
Unversioned HTML paths redirect to matching pages in the current series, preserving the query and fragment when JavaScript is enabled.
An old unversioned path without a current-release counterpart, including an API added only on main, reaches the root 404 page.
The shared selector preserves a page when the target contains it, otherwise links to its homepage.
The documentation menu ends with an API reference (Javadoc) link for that series, followed by a Version dropdown.
On narrow screens, these controls stay in the menu drawer.
Javadoc displays the selected version in its header and links back to that series' documentation; version switching belongs to the documentation menu.
Without JavaScript, ordinary version links remain available.
Development is labeled separately from released versions and excluded from search-engine indexing.
A root 404 page routes retired or missing URLs to the current documentation, Development and release archives.

The publication workflow is serialized from discovery through deployment and resolves the latest main after acquiring its concurrency slot.
Pull requests build and package the same retained lines without deploying.
Main pushes, manual dispatch against main and successful tag-triggered Release workflow completions publish.
Release dry runs and failures publish nothing.
`workflow_run` handles the release workflow's `GITHUB_TOKEN`-created Release, whose `release` event does not start another workflow.
GitHub retains at most one pending concurrency-group member; the newest pending publication represents all releases present when it starts.
A failed generation leaves the previously deployed site available and reports a failed Docs verdict.

Release tags contain a SNAPSHOT reactor version because the publishing workflow re-versions its disposable checkout.
The docs build makes the same metadata adjustment, including the opt-in docs-validation module.
For the existing 1.0.0 tag only, a checked bootstrap adapter replaces the old homepage's claim that its API reference tracks main.
Its behavior text and snippet sources remain those of the tag.
Released pages disable the main edit link and point source links to their exact commit.

ADR-0056's other decisions remain in force: aggregate Javadoc includes every API tier, doclint stays off with warnings fatal, and runnable examples are checked against exact compiled regions.
The API reference remains the default Flink 2.x build; source-level 1.20 compatibility does not add a second Javadoc build.

## Alternatives declined

- Keeping every patch as a separate site increases both the selector and the hosted output without serving the agreed minor-series policy.
- A mutable documentation branch per release creates another backport workflow. Tags provide the behavior snapshot; a patch release updates a retained series.
- Keeping generated HTML on a publishing branch or as long-lived Actions artifacts introduces persistent archive state. Rebuilding at most two released lines plus Development keeps the complete output reproducible from source.
