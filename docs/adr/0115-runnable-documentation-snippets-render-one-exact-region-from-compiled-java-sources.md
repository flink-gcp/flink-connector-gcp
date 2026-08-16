<!--
Copyright 2026 The flink-gcp authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# ADR-0115: Runnable documentation snippets render one exact region from compiled Java sources

- Status: Accepted
- Date: 2026-08-15
- Issues: [#658](https://github.com/laughingman7743/flink-connector-gcp/issues/658),
  [#665](https://github.com/laughingman7743/flink-connector-gcp/issues/665),
  [#704](https://github.com/laughingman7743/flink-connector-gcp/issues/704)
- Modules: all (documentation tooling)
- Current behavior: [Java snippet maintenance skill](../../.agents/skills/maintain-doc-java-snippets/SKILL.md),
  [`java-snippet` shortcode](../layouts/_shortcodes/java-snippet.html)

## Context

Hugo treats an ordinary fenced Java block as text, so a documentation build cannot detect when an
API change makes a runnable example stop compiling.
Compiling a second copy of that example would detect API drift but would allow the checked Java and
the rendered Markdown to diverge.

A compiled source contains imports, wrapper methods and support types that help the compiler but
would distract from the API guidance.
The renderer therefore needs an explicit region boundary, and that boundary remains trustworthy
only while its marker syntax and validation stay strict.
A marker line is a Java comment, so the compiler cannot validate text appended to it as executable
code.

The first source-backed examples and shortcode landed under [#658].
Their missing, duplicate and non-exact marker controls were measured while the change was under
review, but those one-time controls could not detect a later shortcode or Hugo regression.
Issue [#665] made the rendering and error contract persistent.

## Decision

New or materially changed runnable Java guidance uses one compilation unit under
`flink-connector-gcp-docs-validation/src/test/java`.
Hugo renders the exact region between one start marker and one end marker from that same source:

```java
// tag::example-name[]
// end::example-name[]
```

The `java-snippet` shortcode requires non-empty named `file` and `tag` arguments.
After trimming whitespace, it recognizes marker-only lines by exact equality, requires exactly one
start and one end marker in that order, rejects an empty region, and renders only the lines between
the markers.
Before highlighting, the shortcode removes the exact leading horizontal-whitespace prefix shared
by every non-blank region line.
It preserves relative indentation and all other whitespace in the displayed source.
After argument validation, its failures name the documentation position, source file and tag, and
marker-count failures also state the observed count.

Three checks own distinct parts of this contract:

- `just check-doc-snippets` compiles opted-in source-backed examples against the current reactor.
- `just test-java-snippet-shortcode` runs the repository shortcode through Hugo against synthetic
  pages and Java sources, checking the rendered boundary, indentation normalization and every
  validation branch.
- `just docs` builds the production documentation site and catches integration failures in the
  current content tree.

The synthetic site lives under `docs/tests/fixtures/java-snippet/`, outside Hugo's production
content and static roots.
It mounts the repository shortcode instead of copying the parser into a test helper.
Each failing branch has an independent page so the runner can invoke and assert that branch
separately even when Hugo aggregates multiple template errors.

Intentionally partial or pseudocode examples remain ordinary fenced blocks and state what they
omit.
Existing ordinary fences remain outside compilation until a change to their displayed Java or API
meaning requires migration.

## Evidence

On 2026-08-14, [PR #662](https://github.com/laughingman7743/flink-connector-gcp/pull/662)
showed that replacing a builder call with a nonexistent method failed compilation in the named
documentation source.
It also showed that missing, duplicate and non-exact markers failed the Hugo build with the page,
file, tag and observed count.

On 2026-08-14, [PR #697](https://github.com/laughingman7743/flink-connector-gcp/pull/697)
injected eleven defects across extraction, marker recognition, marker counts, ordering, empty
regions, diagnostics and support-type exclusion.
Every defect failed its intended synthetic fixture, and the unmodified shortcode passed the full
fixture suite.

## Alternatives declined

- **Compile every fenced Java block**: many existing blocks intentionally depend on surrounding
  setup or are pseudocode, so treating each fence as a compilation unit would reject valid
  documentation.
- **Maintain Java in both Markdown and a compiled source**: the two copies could drift while both
  the documentation build and Java compilation remained green.
- **Copy the shortcode parser into a test helper**: a template regression could leave the copied
  implementation and its tests green together.
- **Use live documentation as the failure fixtures**: production pages contain valid guidance and
  should not carry malformed markers merely to exercise error branches.
- **Recognize markers by prefix**: text after a marker is part of a Java comment and is not checked
  as executable code, so accepting it would weaken the boundary between marker metadata and the
  compiled region.

## Consequences

A change to runnable guidance updates its Java source and surrounding prose together.
A change to the shortcode preserves the marker and diagnostic contract, extends the synthetic
fixtures for each rendering or validation change, and proves the fixture with a targeted defect
injection.

The validation module stays behind the `docs-snippets` Maven profile and is never published.
The synthetic fixture tree is test input, not production documentation, and the production Hugo
configuration does not mount it into generated output.

[#658]: https://github.com/laughingman7743/flink-connector-gcp/issues/658
[#665]: https://github.com/laughingman7743/flink-connector-gcp/issues/665
