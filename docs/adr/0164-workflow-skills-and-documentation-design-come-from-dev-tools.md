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
- Date: 2026-09-06
- Issues: [flink-gcp/flink-datastream-protobuf#24](https://github.com/flink-gcp/flink-datastream-protobuf/issues/24)
- Modules: all (workflow), docs
- Current behavior: [Contributing](../content/docs/development/contributing.md#shared-development-assets)

## Decision

Adopt the shared workflow skills, installation recipe and Hugo design module from
[flink-gcp-dev-tools](https://github.com/flink-gcp/flink-gcp-dev-tools).
The initial Hugo pin is its merged commit `f33d4549a3be1790ef26650ba9cc35a94a44b49f`, following the
[shared-source decision](https://github.com/flink-gcp/flink-gcp-dev-tools/blob/f33d4549a3be1790ef26650ba9cc35a94a44b49f/docs/adr/0001-share-development-assets.md).
The assets are first-party flink-gcp sources under Apache-2.0, extracted from this repository and
the protobuf library; Hugo Book remains an external module dependency.

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

Import the shared Hugo module through `docs/go.mod` and `docs/go.sum`, and remove the six local
theme files so that local precedence does not hide shared updates.
Keep class-based highlighting and the local assets mount in the site's own configuration.
The local mount preserves site-specific extensions beside the source-snippet sub-target mounts.
Palette generation and theme-control changes belong in dev-tools; this repository owns content,
source-backed snippets, Javadoc, documentation version assembly and Pages deployment.

## Evidence

Measured on 2026-09-06, one before/after pair at connector base
`43d6980cdb25d641155b909a63dcfb15591eb14e` using Hugo Extended 0.164.0: changing only the module import/configuration and removing
the six local theme files produced the same 137 output files byte for byte under `just docs`.
This comparison preceded the development-guide edits in the adoption change.
It covers the ordinary site build, without generating the separate Javadoc or versioned-site artifacts.

## Consequences

Ordinary agent use reads committed skills without a network request or a separate checkout.
Hugo uses its normal module dependency resolution and cache.
Updates to either shared unit remain explicit and reviewable; the two pins can be updated separately.
The local recipe is visible and maintained through deliberate upstream copies.
No Git submodule, shared Java runtime dependency or new template-management tool is introduced.
