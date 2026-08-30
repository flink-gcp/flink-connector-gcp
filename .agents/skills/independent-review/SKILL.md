---
name: independent-review
description: "Run the third review round of this repository's pre-review flow — a second model reads the initial pushed diff without the framing that wrote it. Use after `self-review-round-two` and before Ready; after a narrow repair, use bounded patch-series `range-diff` instead of reopening untouched work. A conflict-only base refresh follows `push-pr-branch`. Covers invocation, coverage evidence, findings, and disagreements."
---

# Independent review

Where this sits in the development flow:

```text
plan → approval → implement → draft PR → self-review → self-review-round-two →
  [independent-review] → ask for review
```

The initial trigger is round two finishing or a turn about to call the PR ready. After a narrow
repair, review `git range-diff <previous-base>..<previous-reviewed-SHA> <current-base>..<sha>` and
trace only the invariants affected by its changed hunks. A tree diff between rebased head SHAs also
contains intervening `main` changes and is not the repair. Restart a full-diff review only when the
repair expands scope or changes a contract beyond the finding. A proven base refresh retains the
round.

**Both self-review rounds are the same model reviewing its own work.** Subagents widen the reach,
but each is briefed by the model that wrote the change, from a description that model also wrote.
This round is the only one that is not. On PR [#1008] every self-review pass that saw one class
stopped at making its fields `volatile`; an independent pass named the check-then-create
interleaving they had missed. `docs/adr/0130` records that measurement and the
decision; this file is how to run it.

[#1008]: https://github.com/flink-gcp/flink-connector-gcp/pull/1008

## Prerequisite

The `codex` Claude Code plugin, which is a local install this repository does not provision —
`mise.toml` does not supply it and nothing in the build depends on it. `/codex:setup` reports
whether it is ready. If it is not, that is one of the recorded skip reasons below, not a blocker.

**The second model must not be the one that wrote the change, and nothing enforces that — it is
yours to check.** From a Codex session this round is Claude Code's, or a human's. The trap is
subtler than the obvious case: if any part of the change, or any fix-up, was delegated to
`/codex:rescue`, then Codex reviewing it is the same model in a fresh thread, and the round is
recorded as independent while not being it. When that has happened, say so and ask a human, or use
a model that wrote none of it.

## Run it

Issue it **from the main thread**, with the `Agent` tool and `subagent_type: "codex:codex-rescue"`,
or by typing the command. Do **not** reach for `Skill(codex:rescue)`: that re-enters the command and
hangs the session, and a forked general-purpose subagent has no `Agent` tool to forward with.

```text
--background --fresh Independent adversarial code review of the single commit <sha> in worktree
<path> (branch <b>, on top of <base>). Review only: do not modify any file, do not run builds or
tests, do not commit or push, and do not run any git or gh command that writes. Read the change as
the diff `git diff <base>..<sha>`, not through `git show`, and review it against the repository's
own conventions only: do not read the commit message, do not look up the pull request, its
description or its comments, and do not read the agent memory stores. Report defects with
file:line and a concrete failure scenario.
```

Before launching, copy `<sha>` from `git rev-parse HEAD` and resolve `<base>` once with
`git merge-base HEAD origin/main` for an ordinary branch, or against the parent branch for a
stacked one. Substitute both literal full SHAs in the prompt; never pass the mutable ref. Using
`origin/main` for a stacked PR also makes the reviewer include the parent's work and label it as
this change's defect.

For a narrow repair after a completed full review, use the same isolation rules but ask for
`git range-diff <previous-base>..<previous-reviewed-SHA> <current-base>..<sha>`. Keep the previous
detached review worktree until Ready; if either old object is unavailable, bounded mode refuses and
the full prompt runs. Require the reviewer to trace each changed hunk into affected callers, tests,
public contracts, and factual claims and report those surfaces. Do not reopen unrelated hunks. If
that trace shows expanded scope or a broader contract change, stop bounded mode and run the full
prompt above.

**Collecting it.** `--background` answers with a job id, and `/codex:status` and `/codex:result`
both carry `disable-model-invocation: true` — the same flag that disqualifies `/codex:review` below
— so an agent cannot read its own review through them. The companion script is the route that works:

```bash
node "<plugin>/scripts/codex-companion.mjs" status --json     # find the id, and whether it finished
node "<plugin>/scripts/codex-companion.mjs" result <job-id>   # the review itself
```

A round whose output was never read is not a round. Do not record "it found nothing" from a job you
did not collect.

Two of those are flags:

- **`--fresh`.** Without it the command asks whether to resume a prior *Codex rescue thread from
  this session*. That is narrower than it sounds, and worth being precise about: `--fresh` isolates
  the reviewer from earlier rescue threads, and from nothing else. It does not undo a
  `/codex:transfer`, which hands the Claude session over wholesale — if one has been run, this round
  is not independent and saying so is the only honest option.
- **`--background`.** The command defaults to the foreground and this takes minutes.

The rest is prose, and each sentence is doing work:

- **No pull-request number.** Measured on this skill's own first use: given `PR #N`, the reviewer
  resolved it and fetched the body and the self-review comments before reading the diff, and said
  so. "Do not trust the description" is not a mechanism; a number is a handle.
- **No agent memory.** `$project-memory` exists to bridge in-progress context between agents, and
  the session that wrote the change may have recorded its conclusions there.
- **No writes, git included.** The sandbox is chosen by a flag the forwarding layer sets from the
  wording — it defaults to write-capable — so the wording is the only lever you have, and it is not
  a guarantee. Also measured here: a review subagent told "read-only" edited the worktree, committed
  and force-pushed over the branch. **Verify afterwards** rather than trusting: the tree must be as
  you left it.
- **Name the worktree at the pushed commit.** Squash and push first, and do not edit that tree while
  the review runs. A detached `git worktree add` at the reviewed commit is safer, and the honest
  limit is that the command exposes no `--cwd`, so the path in the prose is a request rather than a
  sandbox boundary.
- **The diff, not `git show`.** The commit message carries the same WHAT/WHY as the description —
  `push-pr-branch` requires exactly that — and `git show <sha>` prints it before the first hunk, so
  naming the commit without this line hands the framing over anyway. Found by this round's own
  first run, which reported the leak against the brief that produced it.
- **Ask for `file:line` and a failure scenario**, because a finding without one cannot be verified
  and must not be acted on.

None of this makes the description or the commit message unreachable. What it buys is that the
framing was not handed over, and that reaching for it takes a deliberate step.

**Why `rescue` and not `/codex:review` or `/codex:adversarial-review`**, which are purpose-built for
this: both carry `disable-model-invocation: true`, so only the user can start them. An obligation on
every draft PR cannot rest on a command the agent cannot invoke — the same reason `docs/adr/0060`
gives for not resting self-review on `/code-review`. If the user starts one of those instead, that
satisfies this round; record which was used.

## What its findings are, and are not

A finding from a second model is **a hypothesis, exactly like a subagent's**, and it has no extra
standing for coming from outside. Read the code it names before changing anything.

- **Reading is not measuring, and this round is designed so the reviewer can only read.** The
  no-builds, no-tests instruction is what keeps it safe, and it means every finding it makes rests
  on control flow it read rather than on a run. When such a finding lands in a docs page or an ADR —
  where this repository's standard is that a premise is measured — the author owes the measurement
  before it merges. Measured on #1014: an independent finding about which configurations a change
  now refuses was accepted, written into an ADR and merged on two careful readings and no probe. A
  throwaway probe afterwards confirmed the paragraph, which is the good outcome and not the point:
  the same shape with the reading slightly wrong would have merged just as easily.
- **A finding you judge wrong is recorded with the reason**, like any deferral. A round that
  exercised judgement and a round that found nothing look identical in the record otherwise.
- **A defect that predates the change** is a different thing from a regression, and establishing
  which is part of the review. Routing it is the user's (`docs/adr/0061`): present the evidence and
  the cost, do not fold it in silently, and do not `gh issue create` on your own initiative.
- **A disagreement with a decision the PR argues for** is resolved on evidence. Its verdict is input
  to the user, not a gate.
- **The plugin forbids the reviewer applying its own fixes** — `codex-result-handling` is explicit
  that findings are presented and the user chooses. That binds the reviewer, not the author: fixing
  a verified regression in your own change is this round's normal outcome, and the rule it must not
  break is the one above about routing what is not yours.

**If this round changes code, rounds one and two run again over that change before this round is
re-run.** They ask questions this one does not, and a fix that lands with neither is the shape
`docs/adr/0130`'s own evidence is built on. The loop ends when a round produces no change.

## No small-change carve-out

Round two has one; this round does not. Its cost is mostly wall-clock rather than attention — the
non-final runs overlap with other work. Be honest that the last one does not: it blocks the PR being
called ready, and you may not edit the reviewed tree while it runs.

## When no second model is available

Say so in the PR comment, with the reason: unauthenticated, offline, the plugin missing. A step
skipped in silence reads as a step that passed. The PR may still be called ready; what may not
happen is the omission going unrecorded.

## Record it

A PR comment carrying, at minimum:

- **The full reviewed HEAD and base SHAs** — the literal pair resolved before launch and passed to
  the prompt. Verify that the detached worktree still names that HEAD after collecting the review;
  a bounded repair also names the previous reviewed HEAD and base.
- **Coverage** — for the initial review, the changed surfaces and invariants checked; for a bounded
  repair, the delta and every affected invariant traced from it.
- **How it was run** — the invocation, the job id, which model, and how long. Every "Done when" box
  below is otherwise self-attested and never reaches the artifact, so a reader cannot tell a clean
  round from one that handed over a PR number.
- **What it found**, which findings survived being checked, and — with the reason for each — which
  did not, and which were routed elsewhere. A rejected finding with no stated reason is the silent
  deferral wearing a disguise, which is `self-review`'s rule and applies here unchanged.
- **What was repaired here.**
- **That the PR can still merge**, checked rather than assumed, for the same reason round two checks
  it: any verification quoted in this comment is measured against a base that may have moved while
  the review ran, and this round takes minutes.

If it found nothing, say that. A review that ran and found nothing is evidence, and it is the only
way a reader can tell it from one that never ran.

## Done when

- [ ] Both self-review rounds finished, and the branch squashed and pushed first
- [ ] Invoked from the main thread with `--fresh`, `--background`, an explicit no-writes
      instruction, the right base, no pull-request number, and no agent memory
- [ ] No part of the change under review was authored by the model now reviewing it, and no
      `/codex:transfer` handed this session over to it
- [ ] **The job was collected and read**, not merely started
- [ ] After it finished: the reviewed tree is where you left it — HEAD unmoved and no change you did
      not make
- [ ] Every finding read against the code before it was acted on, and every rejected one written
      down with its reason
- [ ] Pre-existing defects distinguished from regressions, and their routing left to the user
- [ ] If this round caused a narrow repair: affected tests re-run, rounds one and two checked the
      repair delta, and this round reviewed that same delta; a scope-expanding repair restarted the
      full review
- [ ] A PR comment recording how it was run as well as what it found — including "it found nothing"
      or "it could not run"
- [ ] That PR comment names the full reviewed HEAD and base SHAs
