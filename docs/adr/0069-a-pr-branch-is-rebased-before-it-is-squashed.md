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

# ADR-0069: A PR branch is rebased before it is squashed, and the deletion list is read before it is pushed

- Status: Accepted
- Date: 2026-08-08 (measured on [#376])
- Issues: [#376] — a pull request rather than an issue, and the first in this column: the decision
  came out of that PR's own failure, and no issue was ever filed for it. `—` was the alternative
  and says less.
- Modules: all (workflow)
- Current behavior: `.agents/skills/push-pr-branch/`, root `AGENTS.md` § Workflow rules

## Context / Evidence

This repository squashes each pull request to one commit. Done as
`git reset --soft origin/main && git commit` on a branch that has **not** been rebased, that
produces a commit whose parent is main's tip and whose tree is the branch's older one — a
well-formed revert of everything main gained since the branch started, carrying the author's commit
message.

It happened on [#376] and was pushed. The commit deleted an ADR, two Pub/Sub source files, two test
files and two skills — seven files, from two merged pull requests. It was found days-worth of
context later, by a `git rebase` whose conflicts were all in files the branch never touched.

Worth being exact about, because it says where the danger is: the *same* squash form ran twice on
that branch and only the second was corrupt. The first ran while `origin/main` had not moved, and
deleted nothing. The command is not the hazard — the command **on a branch whose base has moved
underneath it** is, which is why the rule is about ordering rather than about avoiding
`reset --soft`.

Three things that did not catch it, each measured rather than assumed:

- **CI passed, 14/14.** Reverting a feature deletes its tests along with its code, so nothing failed
  to compile and no test was left referencing anything missing. `scripts/ci-maven-args.py` even
  selected the Pub/Sub module from the diff and built it — green.
- **"Require branches to be up to date" would not have fired.** It asserts *ancestry*: is main's tip
  an ancestor of the head? The corrupted commit's parent was `67285c3`, the merge commit of the very
  pull request it reverted. The setting was satisfied by the commit that undid the work. (It is
  `strict: false` on this repository today; turning it on has independent merits and no bearing
  here.)
- **GitHub's "Update branch" cements it.** Merging main back in was tried: exactly one conflict, in
  `docs/adr/README.md`, and the seven files stayed deleted — main has not touched them since the
  merge base, this side deleted them, and the deletion wins. The button a reader reaches for as the
  fix makes the revert permanent.

`git diff --stat` was read before each push — the habit an earlier incident had already installed —
and it did not help: the deletions hid inside a plausible-looking insertion count.

## Decision

**Rebase, then squash**, and gate every push on the deletion *list*:

```bash
git fetch origin && git rebase origin/main          # then reset --soft is a move to the same commit
git diff --diff-filter=D --name-only origin/main    # empty, or every path deliberate
```

The procedure, its recovery path and the reasoning live in `.agents/skills/push-pr-branch/`, which
the workflow rule names; `git reset --soft $(git merge-base HEAD origin/main)` is the equivalent
safe form when a rebase is genuinely unwanted.

**Deliberately not a CI check.** A content-only check cannot tell this apart from an intentional
deletion: the deleted files existed at the merge base, exactly as they would if their removal were
the point. The intent exists only in the author's head at squash time, which is where this
procedure runs — a CI check would have to demand that intent back as a declaration or an allowlist,
for a defect whose actual cure is not using `reset --soft` on a moving ref. Measured for scale
before deciding: **9 of the last 150 first-parent commits on `main` delete a file at all** (1 to 4
files each; measured 2026-08-08 — say which walk, since a non-first-parent one gives a different
number), so a
blocking "declare your deletions" check was affordable — it was declined on the argument above, not
on cost. A non-blocking report was declined because a guard nobody is required to read is not a
guard.

## Consequences

- The habit installed after the earlier incidents — read `git diff --stat` before pushing — is
  **demoted, not retired**: a file count is a number to interpret, and it demonstrably absorbed
  seven deletions, so the verdict is the `--diff-filter=D` list. The diffstat stays in the skill's
  gate as cheap corroboration, third of three, and must not be read as the check.
- A skill that does not load is the same class of silent guard, and `just check-skill-frontmatter`
  now refuses frontmatter that is not strict YAML. **What that is, and is not**: it is a house
  style, not Claude Code's requirement. The unquoted `description:` carrying a colon-space that started
  this — which PyYAML rejects outright — was in fact loaded by Claude Code, description intact,
  verbatim to its last word; measured by reading a live session's skill listing built from that
  file. So Claude Code's reader is the more tolerant of the two, this check cannot say what that
  reader would refuse, and the first thing it found was a false positive against that standard.
  What it does buy is a directory whose files every tool agrees about, in a place where
  disagreement is silent. It parses with **PyYAML** rather than an approximation — a second,
  diverging parser is the failure this repository has paid for elsewhere — declared in the
  script's own **PEP 723** header, so the root project does not take PyYAML as a runtime
  dependency. Two costs remain and are stated rather than hidden: that script alone runs
  as `uv run --no-project scripts/…`, and pyyaml joins the **dev** group regardless, because
  `just test-scripts` loads every script by file path — which a local `.venv` left over from an
  earlier lock hid until CI ran it clean. Because it downloads, it is a `verify.yaml` job rather
  than part of `just lint`: this repository already had that rule, written on
  `check-flink-api-tiers` and relied on by three other comments and by ADR-0058, and round two of
  this pull request's own review found the first draft breaking it.
- **Where that block *ends* is the loader's answer, not the script's** — refined on [#388], which
  had recorded the opposite as an unclosable gap. Measured against 2.1.223 on 2026-08-09, by
  loading deliberately malformed skills through `--plugin-dir` beside a well-formed control and
  reading back the descriptions a session was given: Claude Code ends the frontmatter at the
  **first `---` anywhere** after the opening line, not at the first `---` *line*. A `----` rule
  closed it, so did a line reading `--- not a delimiter`, so did a `---` mid-sentence inside a
  comment; and a `---` inside a `description:` value truncated that description at the dashes
  while the skill still loaded, advertising a sentence its author never wrote. The control loaded
  intact in the same run, which is what lets the rest of the column mean anything. So the check
  delimits the way that reader does, and asks one question of its own — is the `---` it stopped at
  alone on its line? — which is enough to fail three files that reported clean, with no allowlist
  and no line budget. That question is knowingly the one place the check is *stricter* than the
  loader: a `-----` typed into the closing line loads with its description intact, measured, and
  is reported anyway; a rule that fired only where the skill was already unloadable would have
  missed the truncated description, which is the case worth having. What survives is a body rule
  of exactly `---` standing in for a deleted closing delimiter: the loader stops there too, the
  skill keeps the name and description its author wrote and loses only the body above the rule —
  confirmed by invoking such a skill and reading back its content, not inferred — so nothing is
  misdescribed, and telling a horizontal rule from a delimiter needs judgment this check has none
  of. Worth noting which way both corrections went — the measurement above
  found the loader more tolerant than PyYAML, this one found it more tolerant than a line-anchored
  delimiter search, and a guess in either direction would have been wrong.
- **The gate is only meaningful after the rebase**, which the pull request adding it found by
  running it too early on its own branch: against a `main` that has moved, `git diff origin/main`
  reports upstream work the branch has not picked up yet as deletions, in exactly the shape the
  corruption has. Ordering the steps is therefore not stylistic — read before rebasing, the gate is
  a coin flip. `$(git merge-base HEAD origin/main)` is the comparison that answers the question
  regardless of ordering, and is the fallback when a rebase must wait.
- Recovery, when a branch is already contaminated, is to rebuild rather than to resolve:
  `git branch -f backup HEAD`, `git reset --hard origin/main`, `git checkout backup -- <own files>`.
  A rebase whose conflicts are all in untouched files is the signal to stop resolving. The result
  is auditable by construction, since the branch can only hold what was named.
- A contaminated branch that was already pushed owes a PR comment saying what was reverted and
  restored: a reviewer who read the earlier diff read a wrong one.

[#376]: https://github.com/flink-gcp/flink-connector-gcp/pull/376
[#388]: https://github.com/flink-gcp/flink-connector-gcp/issues/388
