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

# ADR-0091: Codex and Claude Code share one agent-guidance surface

- Status: Accepted
- Date: 2026-08-11
- Issues: —
- Modules: all (workflow/tooling)

## Context

The repository was developed with Claude Code, so its instructions occupied root and module
`CLAUDE.md` files and its skills lived under `.claude/skills/`. The root instructions had grown
past 55 KiB. Codex discovers `AGENTS.md` and repository skills under `.agents/skills/`, and its
default combined project-instruction budget is 32 KiB. Keeping two independent copies would make
the rules drift, while loading the complete historical guide into every session would exceed that
budget and spend context on guidance unrelated to the current task.

Both agents also benefit from the same two complementary discovery tools: version-aware library
documentation from Context7 and symbol-aware Java navigation from Serena. Tool-generated memories
would create a third, agent-specific repository knowledge store alongside guidance and ADRs.

## Decision

- Concise root and module `AGENTS.md` files are the canonical, automatically loaded instructions.
  The former detailed `CLAUDE.md` content remains available on demand under `.agents/references/`.
- Each `CLAUDE.md` is a thin `@AGENTS.md` import, so Claude Code follows the same canonical rules.
- Repository skills live under `.agents/skills/`; `.claude/skills` is a relative symbolic link to
  that directory. Every skill also carries Codex UI metadata in `agents/openai.yaml`.
- The repository commits project-scoped MCP configuration for anonymous Context7 and Serena 1.7.0.
  Serena starts from the current worktree with the agent-specific context and `no-memories` mode.
- The skill checker validates both skill metadata and the shared layout, including the 32 KiB
  combined root-and-module guidance limit. Agent-only file changes remain documentation-only for
  Maven CI classification.

## Alternatives declined

- **Independent Codex and Claude copies**: familiar to each tool, but every policy change would
  need two reviewed edits and no automatic check could prove their meaning stayed equivalent.
- **Symlinking `CLAUDE.md` to `AGENTS.md`**: one physical file, but Claude Code's documented import
  syntax is explicit and works without relying on consumers preserving file symlinks.
- **Keeping `.claude/skills` canonical**: usable through additional Codex configuration, but it
  makes the compatibility path the source of truth and diverges from Codex's repository convention.
- **Serena memories**: potentially convenient across sessions, but they duplicate durable facts
  already reviewed in `AGENTS.md`, references, documentation, and ADRs.

## Consequences

- A rule or skill is edited once and is available to both agents. Compatibility depends on Git
  preserving the `.claude/skills` symlink; the checker rejects a copied directory.
- Fresh sessions receive a small operational index and load detailed guidance only when the task
  requires it. The checker prevents later growth beyond Codex's default instruction budget.
- Context7 can be used without committing a credential. Projects that need higher service limits
  may provide an API key locally without changing the tracked configuration.
- Serena startup may download the pinned package on a new machine, but all sessions use the same
  server version and keep generated cache and memory data out of version control.
