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

# ADR-0120: Module README Java examples remain Markdown checked against compiled regions

- Status: Accepted
- Date: 2026-08-15 ([#705])
- Issues: [#658] (source-backed site snippets), [#694] (compiled Javadoc examples), [#705]
- Modules: all (docs)

## Context

The documentation site renders tagged Java source through a Hugo shortcode and compiles that source
against the connector reactor.
Module READMEs need a different rendering path because GitHub displays their Markdown without Hugo
processing.
They therefore retained ordinary Java fences, including a Pub/Sub sink copy that combined
`Sink<MyEvent>` with a `String` serializer until [#701] corrected it.

The README copies need both GitHub rendering and compiler-backed drift detection.
Generating tracked READMEs would still require CI to reject stale generated output, while making a
small README edit depend on a rewrite command.

## Decision

Every Java fence in a module README is classified as runnable or intentionally partial.

A runnable fence remains ordinary Markdown and carries a hidden `readme-example` marker naming one
tagged Java region in the internal docs-validation module.
The README checker compares the displayed and tagged text exactly apart from container indentation,
then `just check-doc-snippets` compiles the backing source against the current reactor.
A backing region may also serve a Hugo page or Javadoc, which keeps the Pub/Sub README tied to
`PubSubConnectorOverview` rather than creating a third source.

An intentionally partial fence carries a hidden reason and visible prose beginning
`Abbreviated, not compiled:` that states what the fragment omits.
The visible statement prevents a GitHub reader from mistaking an unchecked fragment for compiled
guidance.

The repository validates Markdown instead of generating it.
This keeps README edits and reviews direct while CI provides the stale-output check that generation
would also require.

## Consequences

- `just check-readme-examples` names an unclassified fence, invalid mapping, or exact display diff.
- `just check-doc-snippets` runs the README and Javadoc source-copy checks before compiling the
  docs-validation module.
- The documentation workflow includes module READMEs and both checkers in its push-side inputs.
- Synthetic temporary trees test the CommonMark fence parser and mapping failures without coupling
  the checker tests to the live repository inventory.
- Compilation and exact synchronization do not prove that an example is useful or correct at
  runtime; review retains that responsibility.

[#658]: https://github.com/flink-gcp/flink-connector-gcp/issues/658
[#694]: https://github.com/flink-gcp/flink-connector-gcp/issues/694
[#701]: https://github.com/flink-gcp/flink-connector-gcp/pull/701
[#705]: https://github.com/flink-gcp/flink-connector-gcp/issues/705
