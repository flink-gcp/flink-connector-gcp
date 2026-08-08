---
name: self-review
description: Run round one of this repository's mandatory two-round self-review of a draft pull request — does the code do what the description says? Use immediately after `gh pr create --draft`, before asking anyone for review, and again after pushing fix-ups. Covers the three distinct lenses, verifying a finding before acting on it, re-running the mutation batch afterwards, and recording findings *and* deferrals as a PR comment. Round two is a separate skill, `self-review-round-two`, and this round is not finished until that one has run.
---

# Self-review, round one

Where this sits in the development flow:

```text
plan → approval → implement → draft PR → [self-review] → self-review-round-two → ask for review
```

Reaching the draft PR is the trigger. Nothing else needs to happen first, and nothing downstream —
asking for review, calling the PR ready, marking it ready-for-review — may happen before both
rounds have.

**Round one asks whether the code does what the description says**, taking the PR description as
the specification. It is mandatory on every draft PR (root `CLAUDE.md` § Workflow rules;
`docs/adr/0060`), it applies simplification and efficiency findings rather than correctness ones
only, and it is followed by `self-review-round-two` before the PR is called ready.

Do not confuse it with the built-in `/code-review` (and its alias `/review`). Those are marked
`disable-model-invocation` — **only the user can start them**, and they cannot be scheduled or
invoked on your behalf. They are a useful second opinion to *ask the user for*; they are not this
round, and this round does not wait for them.

## What to review

The pushed diff of the draft PR, against its merge base:

```bash
gh pr diff <n>                       # what the reviewer will see
git diff origin/main...HEAD          # the same thing locally
gh pr view <n> --json body           # the specification you are reviewing against
```

## The three lenses, kept apart

One pass asked for "a review" returns much less than three passes asked for different things.
Take these one at a time, each over the **whole** diff, and do not merge them into a single
sweep:

| Lens | What it looks for |
|---|---|
| **Correctness and concurrency** | Wrong logic, unhandled failure paths, thread confinement claims that the code does not honour, ordering assumptions, what happens on restart, on cancellation, on an empty input |
| **Public API and simplification** | A type or method that need not be public, a knob whose name is wrong, duplication that a call to something existing removes, a shape that is more general than any caller needs, altitude (does this belong at this layer?) |
| **Test quality and flakiness** | A test that passes for the wrong reason, a sleep, an assertion that cannot fail, a fake modelling less — or more — than the vendor does, a test whose failure message would not locate the defect |

If the session permits the Agent tool, run the three as separate subagents **in parallel**, each
given only its own lens and told to return findings as `file:line`, a one-sentence claim, and a
concrete failure scenario. If it does not — several sessions are configured that way — run the
three passes yourself, sequentially, re-reading the diff for each. Say which of the two you did
when you record the round; a single combined pass is a different, weaker thing and must not be
reported as three lenses.

## Verify before acting

- **A finding is a hypothesis until it is checked against the code.** Read the code the finding
  names before changing anything. Agents report plausible-sounding defects that do not exist.
- **Converging lenses are one source, not two.** Two lenses reporting the same thing is one
  finding to verify once — the agreement is not evidence.
- **Verify against the pinned dependency version**, not whichever one a search result described:
  `./mvnw dependency:tree`, or the sources jar for the version the pom resolves.

## Act on it

Apply what survives verification — correctness *and* the simplification and efficiency findings,
which are the half most often skipped — then push.

Two rules from the repository that bite here:

- **`git commit` is the first line of every mutation batch**, before the first mutant, no
  exceptions.
- **Re-run the mutation batch after acting on the review**, not only before. Rework has left
  alive a mutant that had been alive all along (PR #317).
- **A fix that changes a design leaves every statement of that design stale** — the javadoc, the
  ADR's Decision paragraph, the module `CLAUDE.md`, an option table's Default column. Neither the
  mutants nor the build can see it. `self-review-round-two` step 5 is the sweep.

Re-run whatever the change touches: `just verify-module <module>` while iterating, then
`just verify`, plus `just lint`, `just check-option-docs`, `just check-metric-docs`, `just docs`
when a page changed, and `just docs-javadoc` when a `{@link}` or a signature moved — that last one
is the only thing that catches a broken javadoc link.

## Record it

Post one PR comment carrying **the findings and the deferrals, with the reason for each
deferral**. A deferral with no stated reason is the silent deferral wearing a disguise.

Recording is not routing. Routing is the user's (`docs/adr/0061`): a finding outside the issue
being worked has three outcomes — folded into this change, filed as an issue, or dropped — and
**you never run `gh issue create` to route one**. File only what the user has already said is not
being folded in.

## Done when

- [ ] Three lenses run separately over the whole diff
- [ ] Every finding verified against the code before it was acted on
- [ ] Simplification and efficiency findings applied, not only correctness ones
- [ ] Mutation batch re-run after the fix-ups, opened by a commit
- [ ] Build and the checkers green again
- [ ] One PR comment recording findings **and** deferrals with reasons
- [ ] `self-review-round-two` run next — this round is not the whole obligation
