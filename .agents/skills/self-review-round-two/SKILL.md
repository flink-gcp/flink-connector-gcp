---
name: self-review-round-two
description: "Run round two of this repository's mandatory two-round self-review — is the pull request description *true*? Use after `self-review`, with bounded fix-up review after a narrow repair. A conflict-only base refresh follows `push-pr-branch`. Its lenses point outward: the user, operator, blast radius, adversary, and docs reader. Also pays measurements deferred by round one and records coverage."
---

# Self-review, round two

Where this sits in the development flow:

```text
plan → approval → implement → draft PR → self-review → [self-review-round-two] →
  independent-review → ask for review
```

The trigger is round one finishing, or any turn about to call the PR ready. It runs in the
session, on the draft PR, before the draft is offered to anyone — deliberately not in CI, where a
push-triggered review is noise on every commit and a ready-for-review trigger would force a
draft/undraft cycle through the same workflow.

A base-only rebase proven unchanged by `push-pr-branch` keeps this round's result. Only after this
round has completed its own initial full claim audit may a narrow repair compare patch series with
`git range-diff <previous-base>..<previous-reviewed-SHA> <current-base>..HEAD`; a round-one repair
made before that audit does not narrow round two. Do not use a tree diff between rebased head SHAs.
Restart the full claim audit when the repair expands scope or changes a contract beyond the finding.

**Round one asked whether the code does what the description says. Round two asks whether the
description is true.** That is a different question, and it is the reason the rounds are not
"review twice" (`docs/adr/0060`). Run it before saying the PR is ready — not after.

The measurement that pinned this: on two PRs that had passed round one and were CI-green, round
two found in each **claims written in the PR's own javadoc, docs or description that were false** —
"it reaches SQL unchanged as an `IllegalStateException`" (`FactoryUtil` wraps everything a factory
throws), "the only shape that works" (it forced the storage, not the instrument), "the
repository's first main-code static" (it was the second), "scoped to the job's class loader" (true
for a job jar, false for the `lib/` deployment the docs recommend). **None of those is reachable
from a lens aimed at the diff.**

## Step 1: extract the claims

Freeze this round's coverage inventory before extracting claims: the changed or published surfaces
and the behavior, test, public-contract, and factual-claim invariants they owe. Record which entries
the round checks. Resolve the current merge base against the pull request's actual base branch once
and use its literal full SHA throughout the round: `origin/main` for an ordinary pull request and
the parent branch for a stacked one. Do not leave either mutable ref in review commands. A bounded
repair starts from the prior inventory and adds every entry touched by the `range-diff`; verify its
previous objects with `git cat-file -e <previous-base>^{commit}` and
`git cat-file -e <previous-reviewed-SHA>^{commit}`, and run the full audit if either is unavailable.

Before looking at any code, list every factual assertion the change makes, from all five places it
hides:

1. The PR description — especially "the only way", "always", "never", "measured", "cannot".
2. Javadoc and comments **added or edited by this diff**.
3. The docs pages the diff touches.
4. The commit message.
5. **The issue's premise, wherever the change restates it.** A description written from an issue
   inherits that issue's factual claims, and they arrived unverified: an issue is a report, not a
   measurement.

A claim is anything a reader could act on and be wrong. Write the list out; a claim you did not
name is a claim you did not check.

The fifth is the one a careful round two still misses, because a restated premise does not read
like a claim being made — it reads like context being cited. [#352] said the dual-licensed
artifact's shipped licence text "carries no GPL-2.0 text at all"; it carries the full GPL v2, and
the PR description and commit message had repeated the sentence verbatim. **Restating an issue is
asserting it**, so check the premise against the artifact rather than against the issue — and when
it fails, correct the record in the PR, because the issue is what the next reader will find.

[#352]: https://github.com/flink-gcp/flink-connector-gcp/issues/352

## Step 2: the outward lenses

Point each away from the diff:

| Lens | The question it asks |
|---|---|
| **The user meeting the error** | They typed the value, got the message — does it name what they typed and what to do? Does it arrive on the client or on a TaskManager? Is it what the docs told them to type? |
| **The operator reading the dashboard** | The failure is happening now: which metric or log line shows it? Is a counter incremented somewhere nothing scrapes? Would the first visible symptom name this connector at all? |
| **The blast radius of a move** | Who else calls this? What does a session cluster, a `lib/` deployment, a restart or a second job in the same JVM do to the claim? What does the *other* Flink version do? |
| **The adversary defeating the invariant** | Take the guarantee the description states and try to break it: a value at the boundary, a serialized object that never ran the builder, a concurrent close, an empty or a huge input |
| **The reader following the docs** | Read the page as a user with no context, top to bottom, and do what it says. This has been the highest-yield lens in this repository |

## Step 3: pay the deferred measurements

**A deferred measurement is a claim.** Anything round one flagged as reasoned-but-unmeasured gets
measured now — the flag is not a substitute. If the claim is about a vendor library, read the
pinned version's source; if it is about the service, measure against the service; if it is about
Flink, check both supported minors.

**And a measurement you are about to record has a base.** "21 checks pass" is true when written and
stops being true when `main` moves under it, without anything in the diff changing and without any
lens here pointing at it. Before writing a verification result into a PR comment, ask GitHub whether
the PR can still merge:

```bash
gh pr view <n> --json mergeable,mergeStateStatus     # MERGEABLE, or the result you are about to
                                                    # record is measured against a stale base
```

`UNKNOWN` means ask again, not pass. `CONFLICTING` means rebase and re-run before recording, because
a conflicting PR runs no further checks and the green you are quoting is the last base's. Measured on
#1014, where the stale claim sat in this round's own comment (issue #1020).

## Step 4: sweep the prose the diff did not touch

A change that makes a sentence false usually leaves that sentence alone. `grep` the whole page —
and the sibling pages — for the term you changed, not just the paragraph you edited. Three
untouched doc sentences have survived a round one this way.

A page is more than its Markdown. A comment inside a tagged region a page renders is published
prose, and no checker reads what it asserts, so sweep the backing sources under
`flink-connector-gcp-docs-validation` too. Caught in draft on #717: a corrected page still rendered
the superseded claim from a snippet comment a hundred lines below the correction, with every check
green.

Closing or re-scoping an issue includes rewording its rendered mentions in the same change, and a
status word ("planned", "under investigation") may only appear beside the issue link that lets a
reader check it.

## Scope

The cost is real, so the *full* round is for changes whose description makes claims about
framework behaviour, deployment, timing, or "this is the only way" — not for a typo fix. For a
small change, steps 1 and 4 alone still apply: list the claims, grep the page.

## Record it

A second PR comment: the full reviewed HEAD and base SHAs, the inventory entries and claims checked,
which were false and what replaced them, the measurements paid, and the deferrals with their
reasons. For a fix-up, name the previous reviewed HEAD and base and why the bounded `range-diff`
did or did not expand the claim inventory. If round two changed the design, say so in the
description too.

Routing stays the user's (`docs/adr/0061`): fold in, file, or drop, and never `gh issue create` on
your own initiative.

## Done when

- [ ] Every claim in description, javadoc, docs, commit message **and the issue's premise** listed
- [ ] Coverage inventory entries checked are recorded
- [ ] Each checked against a source of truth, not against the diff
- [ ] Deferred measurements paid
- [ ] The whole page grepped for the changed term, not just the edited paragraph
- [ ] Mutation batch re-run if this round changed code
- [ ] A PR comment recording the round
- [ ] That PR comment names the full reviewed HEAD and base SHAs
- [ ] `independent-review` run next, and only then is the PR ready
