---
name: self-review
description: Run round one of this repository's mandatory two-round self-review of a draft pull request — does the code do what the description says? Use immediately after `gh pr create --draft` and use its bounded fix-up mode after a narrow repair. A conflict-only base refresh follows `push-pr-branch`. Covers the three distinct lenses, a frozen coverage inventory, verifying findings before acting, and recording findings and deferrals. Round two is a separate skill, `self-review-round-two`.
license: Apache-2.0
---

# Self-review, round one

This procedure is restored from [the connector skill at `02c4bd59`](https://github.com/flink-gcp/flink-connector-gcp/blob/02c4bd594d2b774cc24b0c0194c1834dd5032120/.agents/skills/self-review/SKILL.md).
The incidents and measurements below are historical connector evidence, not observations about the consuming project.
Before using the procedure, read the consuming repository's `AGENTS.md` and the development guidance it names for repository identity, verification commands, supported versions, and authorization boundaries.
Keep the procedure and its decision conditions shared; bind only those repository-specific inputs locally.
Current review records described below as PR comments use inline comments on reviewed diff lines, an empty review body, and the GitHub review API `comments` array.
Existing user authorization remains applicable; the workflow diagram does not request approval again for work already authorized.

Where this sits in the development flow:

```text
plan → approval → implement → draft PR → [self-review] → self-review-round-two →
  independent-review → ask for review
```

Reaching the draft PR is the trigger. Nothing else needs to happen first, and nothing downstream —
asking for review, calling the PR ready, marking it ready-for-review — may happen before all three
rounds have.

The first pass covers the full diff. After a narrow repair, compare the previous and current patch
series with `git range-diff <previous-base>..<previous-reviewed-SHA> <current-base>..HEAD`, then
review its changed hunks and affected inventory entries. Do not use `git diff` between the two head
SHAs: every fix-up is rebased and squashed, so that tree delta also contains intervening `main`
changes. Restart the full pass only when the repair expands scope or changes a contract beyond the
recorded finding. A base-only rebase proven unchanged by `push-pr-branch` keeps this round's result.

**Round one asks whether the code does what the description says**, taking the PR description as
the specification. It is mandatory on every draft PR (the consuming repository's `AGENTS.md`;
[connector ADR-0060](https://github.com/flink-gcp/flink-connector-gcp/blob/02c4bd594d2b774cc24b0c0194c1834dd5032120/docs/adr/0060-self-review-is-two-rounds-and-round-two-audits-the-descriptions-claims.md)), it applies simplification and efficiency findings rather than correctness ones
only, and it is followed by `self-review-round-two` before the PR is called ready.

In Claude Code, do not confuse this with the built-in `/code-review` (and its alias `/review`).
Only the user can start those commands. They can provide a second opinion when the user chooses
to run them, but they are not this round and this round does not wait for them. Codex has no
equivalent slash command; use the passes below directly.

## What to review

Resolve the pull request's actual base once before starting and copy its full SHA into every review
prompt, command, and record. Use `origin/main` for an ordinary pull request and the parent branch
for a stacked one; do not let a later fetch change which patch a reviewer reads:

```bash
gh pr view <n> --json baseRefName,body # actual base and specification
git merge-base HEAD origin/<baseRefName> # copy as literal <review-base>
git diff <review-base>..HEAD             # the pushed patch
```

Before the first pass, freeze a coverage inventory in the review record: every changed file or
published surface, and the behavior, test, public-contract, and factual-claim invariants it owes.
Each lens reports which inventory entries it checked. Record the reviewed base as well as HEAD, and
before bounded mode verify both previous objects. Run `git cat-file -e <previous-base>^{commit}` and
`git cat-file -e <previous-reviewed-SHA>^{commit}`; if either fails, bounded mode refuses and the
full pass runs. For a narrow fix-up, use the `range-diff` above and recheck only entries affected by
it.

## The three lenses, kept apart

One pass asked for "a review" returns much less than three passes asked for different things.
Take these one at a time over the full initial diff, or over the bounded repair delta, and do not
merge them into a single sweep:

| Lens | What it looks for |
|---|---|
| **Correctness and concurrency** | Wrong logic, unhandled failure paths, thread confinement claims that the code does not honour, ordering assumptions, what happens on restart, on cancellation, on an empty input |
| **Public API and simplification** | A type or method that need not be public, a knob whose name is wrong, duplication that a call to something existing removes, a shape that is more general than any caller needs, altitude (does this belong at this layer?) |
| **Test quality and flakiness** | A test that passes for the wrong reason, a sleep, an assertion that cannot fail, a fake modelling less — or more — than the vendor does, a test whose failure message would not locate the defect |

If the session supports subagents, run the three as separate subagents **in parallel**, each given
only its own lens and told to return findings as `file:line`, a one-sentence claim, and a concrete
failure scenario. If it does not, run the three passes yourself, sequentially, re-reading the diff
for each. Say which of the two you did when you record the round; a single combined pass is a
different, weaker thing and must not be reported as three lenses.

## Verify before acting

- **A finding is a hypothesis until it is checked against the code.** Read the code the finding
  names before changing anything. Agents report plausible-sounding defects that do not exist.
- **Converging lenses are one source, not two.** Two lenses reporting the same thing is one
  finding to verify once — the agreement is not evidence.
- **Verify against the pinned dependency version**, not whichever one a search result described:
  inspect the consuming repository's resolved dependency graph and the matching dependency sources.
  For a Maven project, `./mvnw dependency:tree` and the sources jar for the version the pom resolves are the corresponding tools.

## Act on it

Apply what survives verification — correctness *and* the simplification and efficiency findings,
which are the half most often skipped — then push.

The consuming repository's verification policy determines when a mutation batch is required.
The batch instructions here and in the completion checklist apply whenever a batch is performed.
Record the item as not applicable only when no batch is required or performed.

Two rules from the repository that bite here:

- **`git commit` is the first line of every mutation batch**, before the first mutant, no
  exceptions.
- **Re-run the mutation batch after acting on the review**, not only before. Rework has left
  alive a mutant that had been alive all along (PR [#317](https://github.com/flink-gcp/flink-connector-gcp/issues/317)).

Re-run whatever the change touches, at the scope the consuming repository's guidance sets: targeted
tests while iterating, the affected module's suite before the push, and the full verification when
that repository's build-input policy requires it. Run its applicable lint, configuration/metrics
reference, documentation and generated API-documentation checks as well.

In the original connector workflow, those bindings are the full `just verify` only for a shared
build input, with the per-connector CI lane carrying full verification for a single-module change;
`just lint`, `just check-option-docs`, `just check-metric-docs`, `just docs` when a page changes, and
`just docs-javadoc` when a `{@link}` or a signature moves. The last command is the separate build
that catches a broken Javadoc link. Other consumers provide their own commands and applicability;
the connector commands are an example, not requirements for a repository without those artifacts.

## Record it

Post one PR comment carrying the full reviewed HEAD and base SHAs, the coverage inventory entries
checked, **the findings and the deferrals, with the reason for each deferral**. For a fix-up, also
name the previous reviewed HEAD and base and why the `range-diff` did or did not expand inventory.

Recording is not routing. Routing is the user's ([connector ADR-0061](https://github.com/flink-gcp/flink-connector-gcp/blob/02c4bd594d2b774cc24b0c0194c1834dd5032120/docs/adr/0061-a-finding-outside-the-issue-is-routed-by-the-user-with-drop-as-a-real-outcome.md)): a finding outside the issue
being worked has three outcomes — folded into this change, filed as an issue, or dropped — and
**you never run `gh issue create` to route one**. File only what the user has already said is not
being folded in.

## Done when

- [ ] Coverage inventory frozen before the initial review
- [ ] Three lenses run separately over the full initial diff or bounded repair delta
- [ ] Every finding verified against the code before it was acted on
- [ ] Simplification and efficiency findings applied, not only correctness ones
- [ ] Mutation batch re-run after the fix-ups, opened by a commit
- [ ] Build and the checkers green again
- [ ] One PR comment recording findings **and** deferrals with reasons
- [ ] That PR comment names the full reviewed HEAD and base SHAs
- [ ] `self-review-round-two` run next, then `independent-review` — this round is not the whole
      obligation
