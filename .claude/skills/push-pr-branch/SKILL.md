---
name: push-pr-branch
description: Squash a pull request's branch to one commit and push it without silently reverting work that landed on main meanwhile. Use before EVERY push of a PR branch — the first one, each review round's fix-ups, and the final push before calling the PR ready — and whenever about to run `git push`, `git push --force`, `git push --force-with-lease`, `git reset --soft`, or "squash the commits". Mandatory when `origin/main` may have moved since the branch was created, which on this repository it usually has.
---

# Push a PR branch

The operation this repository asks for on every PR — one commit, squashed, force-pushed — has a
failure mode that nothing downstream reports. This skill is the procedure that avoids it.

## What goes wrong

`git reset --soft origin/main` followed by `git commit` builds a commit whose **parent is main's
tip** but whose **tree is the branch's older one**. Everything main gained since the branch started
is then *deleted* by that commit. It is a well-formed, internally consistent revert of other
people's merged work, wearing your change's commit message.

Three properties make it invisible, all measured on PR #376 (2026-08-08; `docs/adr/0069`):

- **CI passes.** Reverting a feature deletes its tests along with its code, so nothing fails to
  compile and no test is left referencing anything missing. That PR was 14/14 green while deleting
  an ADR, two connector source files, two test files and two skills.
- **Branch protection cannot see it.** "Require branches to be up to date" asserts *ancestry* — is
  main's tip an ancestor of the head? — and the corrupted commit's parent **was** the merge commit
  of the PR it reverted. The setting was satisfied by the very commit that undid the work.
- **"Update branch" cements it.** Merging main back in leaves the deletions in place: main has not
  touched those files since the merge base, your side deleted them, and the deletion wins. The
  button a reader reaches for as the fix makes the revert permanent.

So the only place to catch it is here, before the push.

## The procedure

```bash
git fetch origin                                    # 1. always, first
git rebase origin/main                              # 2. bring the branch up to date, resolving
                                                    #    conflicts; NEVER skip to step 3 instead
git reset --soft origin/main                        # 3. safe only after 2 — see below
git commit -F <message-file>                        # 4. one commit, WHAT/WHY per the template
git push --force-with-lease                         # 5. after the gate, never before
```

Step 2 is the whole point, and the reason is precise rather than slogan-shaped. `reset --soft` sets
HEAD to the named commit while keeping the tree, so it is a squash either way — what changes is
*what the new commit's tree is measured against*. On a branch that has not been rebased, that
parent is a `origin/main` the tree has never seen, and everything main gained becomes a deletion.
After a rebase, `origin/main` and the branch's merge base are the same commit, so the squash
measures against what the branch actually started from. If a rebase is genuinely unwanted, the
equivalent safe form is `git reset --soft $(git merge-base HEAD origin/main)`, which is immune to
main moving — but prefer the rebase, because a stale branch has to be brought forward before merge
anyway.

Step 4 takes a message file because the WHAT/WHY belongs in the commit, not only in the PR body;
write it first. Step 5 needs `--force-with-lease` because steps 3-4 rewrite history the remote
already has — and note that step 1's `git fetch` updates the remote-tracking ref, which is what
`--force-with-lease` compares against, so on a branch someone else might also push to, run the
gate and the push together rather than fetching again in between.

## The gate, before every push

Three commands. All three, every time, including on a push you are sure about.

```bash
git log --oneline origin/main..HEAD                 # exactly one line
git diff --diff-filter=D --name-only origin/main    # empty, or every path deliberate
git diff --stat origin/main | tail -1               # file count matches what you touched
```

**Run them only after the rebase, never instead of it.** On a branch that is behind,
`git diff origin/main` reports upstream work the branch simply has not picked up yet as
*deletions* — indistinguishable, at a glance, from the corruption this skill exists to catch. That
is a false positive with the same shape as the true one, and reading it as either without rebasing
first is a coin flip. If a rebase is genuinely impossible right now, compare against
`$(git merge-base HEAD origin/main)` instead, which answers the question the gate is actually
asking: what does *this branch* change?

The second is the one that matters, and it is **a list to read rather than a number to interpret**.
On #376 the diffstat *was* read at each push and the deletions hid inside a plausible-looking
insertion count; only the explicit list makes them impossible to miss. If any path in it is one you
did not intend to remove, stop — do not push, do not "just re-run the squash".

A push may proceed when the deletion list is empty, or when every path in it is a file this change
genuinely removes and the commit message says so.

## Recovery, when the gate fails

Rebuilding beats untangling. A rebase whose conflicts are all in files you never touched is a
signal to start over, not to resolve.

```bash
BASE=$(git merge-base HEAD origin/main)             # what this branch actually started from

# Confirm none of your files also moved upstream, or the checkout below would
# revert *that*. Expect no output. (bash; under fish use `(… | psub)` for `<(…)`.)
comm -12 <(git diff --name-only "$BASE" origin/main | sort) \
         <(git diff --name-only "$BASE" HEAD | sort)

git branch -f "backup/$(git branch --show-current)" HEAD    # nothing is lost
git reset --hard origin/main
git checkout "backup/$(git branch --show-current)" -- <the files this change owns>
git commit -F <message-file>                        # the rebuild is not done until it is a commit
```

The result is auditable by construction: the branch can only contain what was named. The backup
branch is named after the one it saves, because this repository runs one worktree per PR and a
fixed `backup` cannot be forced while it is checked out in another.

If the corrupted commit was already pushed, this is still the fix — force-push the rebuilt branch
and say plainly in a PR comment what was reverted and restored, because a reviewer who read the
earlier diff read a wrong one.

## Why this is a skill and not a checker

A content-only check cannot tell this apart from an intentional deletion: on #376 the deleted files
existed at the merge base, exactly as they would if the author had meant to remove them. The intent
lives in the author's head at squash time, which is where this procedure runs. A CI check would
need that intent restated as an allowlist or a declaration, for a defect whose real cure is not
using `reset --soft` on a moving ref.

Related: root `CLAUDE.md` § Workflow rules, `docs/adr/0069`, and `.claude/skills/self-review/`,
whose fix-up commits are the most common reason a branch is squashed a second time.
