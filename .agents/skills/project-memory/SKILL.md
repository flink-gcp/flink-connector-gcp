---
name: project-memory
description: Consult the private local agent memories that belong to this repository — the Claude Code project memory and the Codex memory store — without copying them into source control. Use before planning non-trivial repository work, when prior decisions, in-progress investigations, or measurements may matter, when working from a git worktree whose Claude project key differs from the main checkout, or when the user asks to retain or recall local project context.
---

# Consult project memory

Claude Code and Codex each keep a private memory outside the repository. Claude Code keys its
project memory by checkout path, so a dedicated git worktree does not automatically see the memory
accumulated in the main checkout. Codex keeps a global store shared across repositories and tags
each task group with the working directory it was made from. This skill locates both stores from
any worktree, so either agent can consult the same local knowledge, including in-progress
context the other agent recorded but has not yet settled into tracked docs or ADRs.

## Locate the memories

Run this read-only discovery with bash from anywhere inside the repository:

```bash
repository_root="$(dirname "$(git rev-parse --path-format=absolute --git-common-dir)")"
project_key="$(printf '%s' "${repository_root}" | sed 's/[^[:alnum:]-]/-/g')"
claude_memory_dir="${CLAUDE_CONFIG_DIR:-${HOME}/.claude}/projects/${project_key}/memory"
codex_memory_dir="${CODEX_HOME:-${HOME}/.codex}/memories"
test -f "${claude_memory_dir}/MEMORY.md" \
  && printf 'claude: %s\n' "${claude_memory_dir}" || printf 'claude: none\n'
test -f "${codex_memory_dir}/MEMORY.md" \
  && printf 'codex: %s\n' "${codex_memory_dir}" || printf 'codex: none\n'
```

A `none` line is normal: report that the store does not exist and continue with the tracked
guidance and whichever store is present. No output at all, or a `git` error, means discovery
itself failed rather than that memory is absent. Do not broaden the search across other Claude
projects or other agent configuration directories: their contents are outside this repository's
scope.

## Read progressively

The Claude store is scoped by its directory: every file in it belongs to this repository. The
Codex store is global — its presence proves nothing about this repository — so scoping happens
while reading.

1. Claude store: read `MEMORY.md` first; it is the routing index, not evidence that every linked
   note is current. Then read only the entry files relevant to the task, completely.
2. Codex store: in `memory_summary.md` (when present), read the subsections of the "What's in
   Memory" index headed by this repository's root path — both the recent one and the one under
   "Older Memory Topics". Then read only the task groups of the Codex store's own `MEMORY.md`
   whose `applies_to` line names this repository's root, or one of its dedicated worktrees under
   `/tmp/worktrees/`, as its `cwd` (macOS may record a `/private` prefix), and follow their
   `rollout_summaries/` files only when the task group's record is not enough. Skip the user
   profile and every section that belongs to another repository; when nothing is headed by this
   repository, report that and continue.
3. Treat memory as a lead. Verify drift-prone claims against the current tree, GitHub state,
   dependency version, or live service before acting. A Codex task group often records an
   investigation whose fix was never selected, and its record states what was left unverified.
4. Prefer the tracked `AGENTS.md`, `.agents/references/`, docs, ADRs, and current code when they
   conflict with local memory.

## Keep the boundary private

- Never copy or symlink either memory store into the repository.
- Never commit memory text without reviewing it for credentials, personal information, private
  repository details, machine-specific paths, stale issue state, and superseded decisions.
- Do not quote private values in tool output, PRs, issues, or chat. Summarize only the repository
  fact needed for the task.
- Do not edit either memory store while using this skill, and never edit the other agent's store
  at all. Memory maintenance stays native to each agent; durable project facts belong in tracked
  docs, ADRs, guidance, or references through a separately reviewed change.
- Keep Serena in `no-memories` mode. Its memory store is not the bridge and must not become a
  second writable source of truth.

## Report usage

When memory materially changes the work, say which indexed topic informed the decision and what
current evidence verified it. When it is unavailable or irrelevant, continue without ceremony.
