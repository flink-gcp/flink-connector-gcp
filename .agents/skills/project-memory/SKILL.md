---
name: project-memory
description: Consult the private Claude Code project memory that belongs to this repository without copying it into source control. Use before planning non-trivial repository work, when prior decisions or measurements may matter, when working from a git worktree whose Claude project key differs from the main checkout, or when the user asks to retain or recall local project context.
---

# Consult project memory

Claude Code stores project memory outside the repository and keys it by checkout path. A dedicated
git worktree therefore does not automatically see the memory accumulated in the main checkout.
Use the main checkout derived from Git's common directory so Claude Code and Codex can consult the
same local knowledge from any worktree.

## Locate the memory

Run this read-only discovery from anywhere inside the repository:

```bash
repository_root="$(dirname "$(git rev-parse --path-format=absolute --git-common-dir)")"
project_key="$(printf '%s' "${repository_root}" | sed 's/[^[:alnum:]-]/-/g')"
claude_config_root="${CLAUDE_CONFIG_DIR:-${HOME}/.claude}"
memory_dir="${claude_config_root}/projects/${project_key}/memory"
test -f "${memory_dir}/MEMORY.md" && printf '%s\n' "${memory_dir}"
```

If it prints nothing, report that no memory exists for the canonical checkout and continue with
the tracked guidance. Do not broaden the search across other Claude projects: their contents are
outside this repository's scope.

## Read progressively

1. Read `MEMORY.md` first; it is the routing index, not evidence that every linked note is current.
2. Select only entries relevant to the task and read those files completely.
3. Treat memory as a lead. Verify drift-prone claims against the current tree, GitHub state,
   dependency version, or live service before acting.
4. Prefer the tracked `AGENTS.md`, `.agents/references/`, docs, ADRs, and current code when they
   conflict with local memory.

## Keep the boundary private

- Never copy or symlink the local memory directory into the repository.
- Never commit memory text without reviewing it for credentials, personal information, private
  repository details, machine-specific paths, stale issue state, and superseded decisions.
- Do not quote private values in tool output, PRs, issues, or chat. Summarize only the repository
  fact needed for the task.
- Do not edit Claude memory while using this skill. Memory maintenance remains local to Claude
  Code; durable project facts belong in tracked docs, ADRs, guidance, or references through a
  separately reviewed change.
- Keep Serena in `no-memories` mode. Its memory store is not the bridge and must not become a
  second writable source of truth.

## Report usage

When memory materially changes the work, say which indexed topic informed the decision and what
current evidence verified it. When it is unavailable or irrelevant, continue without ceremony.
