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

# ADR-0164: Workflow skills and documentation design come from dev-tools

- Status: Accepted
- Date: 2026-09-06; revised by [#1984](https://github.com/flink-gcp/flink-connector-gcp/issues/1984) (2026-11-01)
- Issues: [flink-gcp/flink-datastream-protobuf#24](https://github.com/flink-gcp/flink-datastream-protobuf/issues/24), [#1984](https://github.com/flink-gcp/flink-connector-gcp/issues/1984)
- Modules: all (workflow), docs
- Current behavior: [Contributing](../content/docs/development/contributing.md#shared-development-assets)

## Decision

Adopt the shared workflow skills, installation recipe and Hugo design module from
[flink-gcp-dev-tools](https://github.com/flink-gcp/flink-gcp-dev-tools).
The initial Hugo pin is its merged commit `f33d4549a3be1790ef26650ba9cc35a94a44b49f`, following the
[shared-source decision](https://github.com/flink-gcp/flink-gcp-dev-tools/blob/f33d4549a3be1790ef26650ba9cc35a94a44b49f/docs/adr/0001-share-development-assets.md).
The original shared assets are first-party flink-gcp sources under Apache-2.0, extracted from this repository and
the protobuf library; Hugo Book remains an external module dependency.
The shared search override adapts Hugo Book v0.14.0 and retains its upstream MIT notice.

Keep the four skill directories, including their Codex metadata, as tracked copies selected by
`dev_tools_revision` in the justfile.
`just skills-sync` installs that commit, and updates land as reviewed diffs with their pin.
Keep the copied `dev-tools.just` recipe aligned with that upstream revision.
Its shebang body is the explicit exception to the local one-command-per-recipe-line convention:
`just lint` extracts the body and checks it with ShellCheck, preserving the reason for that convention.
The small WHAT/WHY template remains an intentional copy and already matches the shared source.

Use the full shared procedures restored from this repository's commit
`02c4bd594d2b774cc24b0c0194c1834dd5032120`, retaining their decision conditions, exceptions, examples
and historical evidence within each SKILL.md.
The initial compact extraction omitted shared operational detail; a second locally maintained
procedure would leave other consumers without the original workflow.
The shared inventory records each retained source section and the consumer-binding substitutions.

Keep only repository-specific bindings, such as verification commands and compatibility versions,
in `AGENTS.md` and `.agents/references/repository-guide.md`.
The shared skills own issue routing, one-commit pushes, conflict-only proof, review-tool adapters
and ADR-0130's recorded unavailable-reviewer exception.
The prior incident evidence remains in ADR-0060, ADR-0069 and ADR-0130, linked from the shared
procedures at the frozen source revision.
Review records use inline comments with an empty review body and a comments array.
All other skills remain locally maintained.

Import the shared Hugo module through `docs/go.mod` and `docs/go.sum`, and avoid local copies of shared
theme files so that local precedence does not hide shared updates.
Keep class-based highlighting and the local assets mount in the site's own configuration.
The local mount preserves site-specific extensions beside the source-snippet sub-target mounts.
Palette generation, theme-control changes and search initialization belong in dev-tools; this repository owns content,
source-backed snippets, Javadoc, documentation version assembly and Pages deployment.

Publish only the theme files the site uses.
`docs/hugo.toml` ignores the design module's own imports, which today are only Hugo Book, and imports Hugo
Book after it, so the design module's layouts and assets still take precedence.
From Hugo Book's `static/` directory the site publishes the two favicons and the Fuse.js module, and it
excludes the KaTeX, Mermaid and asciinema-player shortcodes and code-block hooks, so a page that tries one
of those features fails the build or renders plain code rather than linking a file that is not published.
No page used those features, and their files carry no notice: KaTeX's fonts are under the SIL Open Font
License 1.1, and the Mermaid and asciinema-player bundles compile in third-party dependencies.
The hidden site licenses page carries the notices for what is published, and its `hugo-book-version`
shortcode fails the build when the resolved Hugo Book version differs from the documented one or when a
module replacement or vendored copy substitutes other files.
Release lines built from earlier tags keep their full theme `static/` directory until they leave the
retention window.

## Evidence

Measured on 2026-09-06, one before/after pair at connector base
`43d6980cdb25d641155b909a63dcfb15591eb14e` using Hugo Extended 0.164.0: changing only the module import/configuration and removing
the six local theme files produced the same 137 output files byte for byte under `just docs`.
This comparison preceded the development-guide edits in the adoption change.
It covers the ordinary site build, without generating the separate Javadoc or versioned-site artifacts.

Measured on 2026-11-01 for the reduced static mount, at connector base
`2b417720fbdbb7c3347ff5bd6253c216862d3273` with Hugo Extended 0.164.0: changing only the module
configuration removed 68 published files (KaTeX 63, of which 60 are fonts; asciinema-player 3;
`mermaid.min.js`; `favicon.svg`) and left every other output file byte-identical.
`favicon.svg` was restored afterwards because the web app manifest that Hugo Book renders names it.
The Mermaid 11.14.0 package lists 21 direct dependencies, and its bundle retains license comments for only
some of them; asciinema-player 3.15.1 embeds SolidJS; the `name` table of every KaTeX font declares OFL 1.1.
The shortcode guard was probed six ways: the pinned version passes, while a different documented version,
a `go.mod` replace, a `HUGO_MODULE_REPLACEMENTS` replacement, a Hugo workspace and `hugo mod vendor` each
fail the build.

## Alternatives declined

- **Publish the KaTeX, Mermaid and asciinema-player bundles with every notice they owe.** Declined: the
  versions of the dependencies compiled into each bundle are recoverable only from each project's lockfile
  at the bundled release, and that inventory would be re-derived on every theme update for features no
  page uses.
- **Reduce the mount inside the design module instead.** Not done here: it would change
  flink-datastream-protobuf's site in the same step, which tracks the same problem in
  [flink-gcp/flink-datastream-protobuf#190](https://github.com/flink-gcp/flink-datastream-protobuf/issues/190).
  The local configuration copies no theme file, so the rule above about local precedence still holds;
  the contributing guide asks a design module update to check that the module imports nothing but Hugo
  Book.
- **Remove the bundles from retained release lines when the site is assembled.** Declined by the
  maintainer on 2026-11-01: the 1.2 and 1.3 lines, built from tags that predate this decision, stay as
  built and leave the site as later minors are released.

## Consequences

Ordinary agent use reads committed skills without a network request or a separate checkout.
Hugo uses its normal module dependency resolution and cache.
Updates to either shared unit remain explicit and reviewable; the two pins can be updated separately.
The local recipe is visible and maintained through deliberate upstream copies.
No Git submodule, shared Java runtime dependency or new template-management tool is introduced.
