---
name: self-review-round-two
description: Run round two of this repository's mandatory two-round self-review — is the pull request description *true*? Use after `self-review` and before telling anyone the PR is ready, or when about to claim a PR is finished. Its lenses point outward rather than at the diff: the user meeting the error, the operator reading the dashboard, the blast radius of a move, an adversary defeating the invariant, and a reader following the docs. Also pays any measurement round one deferred.
---

# Self-review, round two

Where this sits in the development flow:

```text
plan → approval → implement → draft PR → self-review → [self-review-round-two] → ask for review
```

The trigger is round one finishing, or any turn about to call the PR ready. It runs in the
session, on the draft PR, before the draft is offered to anyone — deliberately not in CI, where a
push-triggered review is noise on every commit and a ready-for-review trigger would force a
draft/undraft cycle through the same workflow.

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

Before looking at any code, list every factual assertion the change makes, from all four places it
hides:

1. The PR description — especially "the only way", "always", "never", "measured", "cannot".
2. Javadoc and comments **added or edited by this diff**.
3. The docs pages the diff touches.
4. The commit message.

A claim is anything a reader could act on and be wrong. Write the list out; a claim you did not
name is a claim you did not check.

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

## Step 4: sweep the prose the diff did not touch

A change that makes a sentence false usually leaves that sentence alone. `grep` the whole page —
and the sibling pages — for the term you changed, not just the paragraph you edited. Three
untouched doc sentences have survived a round one this way.

Closing or re-scoping an issue includes rewording its rendered mentions in the same change, and a
status word ("planned", "under investigation") may only appear beside the issue link that lets a
reader check it.

## Step 5: re-close what this round opened

Round two changes designs; the measurement above is what says so. **Its own fixes are then the
least-reviewed code in the pull request**, and what they break is usually not behaviour but the
places that *state* the design.

Measured on PR #373, where this round moved a default (a bound from one times a limit to twice
it) and its fixes then contradicted the code they described in four places across three documents:
the **public builder javadoc** still carried the old default *and* reinstated verbatim the premise
the ADR had just withdrawn; the ADR's Decision paragraph disagreed with its own bullet two lines
below; the module `CLAUDE.md` carried the old default; and the ADR and `CLAUDE.md` both described
a call shape the fix had replaced.
Green mutants and a green build say nothing about a javadoc that disagrees with its own ADR.

So after acting, grep for the **old claim** — not for the new term — and visit every place that
*states* the design rather than describing the behaviour:

- **the public javadoc of whatever the change configures.** A builder setter is read far more often
  than a docs page, and a stale default there does the most damage;
- **the ADR's Decision paragraph**, not only the bullet you edited;
- **the module `CLAUDE.md`**;
- **the reference and SQL option tables**, where a default is a value rather than prose.

Trust the grep over the edits you remember making. A mechanical rename needs the same sweep: the
same pull request left a parameter and two test method names carrying the old word.

This is step 4 pointed at your own fixes: step 4 catches prose the *diff* made false, this catches
prose this *round* made false.

## Scope

The cost is real, so the *full* round is for changes whose description makes claims about
framework behaviour, deployment, timing, or "this is the only way" — not for a typo fix. For a
small change, steps 1 and 4 alone still apply: list the claims, grep the page. Step 5 applies
whenever this round changed anything, however small the round was.

## Record it

A second PR comment: the claims checked, which were false and what replaced them, the
measurements paid, and the deferrals with their reasons. If round two changed the design — it has
— say so in the description too, because the description is what round two just audited.

Routing stays the user's (`docs/adr/0061`): fold in, file, or drop, and never `gh issue create` on
your own initiative.

## Done when

- [ ] Every claim in description, javadoc, docs and commit message listed
- [ ] Each checked against a source of truth, not against the diff
- [ ] Deferred measurements paid
- [ ] The whole page grepped for the changed term, not just the edited paragraph
- [ ] What this round's own fixes made stale swept — the **old claim** grepped, and the javadoc,
      ADR Decision paragraph, module `CLAUDE.md` and option tables visited
- [ ] Mutation batch re-run if this round changed code
- [ ] A PR comment recording the round
- [ ] Only now is the PR ready
