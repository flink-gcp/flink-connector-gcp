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

# ADR-0130: An independent review follows the two self-review rounds

- Status: Accepted
- Date: 2026-08-22 (measured on
  [#1008](https://github.com/flink-gcp/flink-connector-gcp/pull/1008))
- Issues: [#1017](https://github.com/flink-gcp/flink-connector-gcp/issues/1017)
- Modules: all (workflow)
- Current behavior: root `AGENTS.md` § GitHub workflow, and the `independent-review` skill

## Context

`docs/adr/0060` established two self-review rounds that ask different questions: does the code do
what the description says, and is the description true. Both are the same model reviewing its own
work. Subagents improve the reach, but every one of them is briefed by the model that wrote the
change, from a description that model also wrote.

## Decision

A draft pull request is reviewed by a second model after both self-review rounds, after each fix-up
push, and before it is called ready.

- **It is given the worktree at the pushed commit and nothing else** — no pull-request number, and
  an instruction not to look the pull request up or to read the agent memory stores. "Do not trust
  the description" was tried first and is not enough: measured on this decision's own first use, a
  reviewer handed `PR #N` resolved it and fetched the body and the review comments before reading
  the diff. A number is a handle, and `$project-memory` exists precisely to carry in-progress
  context between agents. None of this makes the description unreachable; it makes reaching for it a
  deliberate step rather than the default frame.
- **It is told to write nothing, git included**, because the tooling defaults to a write-capable run
  and because a reviewer sharing the author's worktree can otherwise commit and push over it — both
  measured here.
- **The second model is not the one that wrote the change, and nothing enforces that.** From a Codex
  session this round is Claude Code's, or a human's — and if any part of the change or a fix-up was
  delegated to Codex, a Codex round is the same model in a fresh thread. The check is the author's
  to make and to state.
- **Its findings are hypotheses**, read against the code before anything changes, exactly as a
  subagent's are. Coming from outside earns no extra standing.
- **The reviewer reads; it does not measure**, because the instruction that keeps it safe forbids
  running anything. A finding of its that will end up in a docs page or an ADR therefore carries an
  unpaid measurement, and paying it is the author's before the change lands — the repository's
  standard for an ADR premise does not soften because the reading was careful.
- **A defect it finds that predates the change is routed by the user** (`docs/adr/0061`), with the
  evidence and the cost presented rather than folded in silently.
- **Its verdict is input, not a gate.** A disagreement about a decision the PR argues for is
  resolved on evidence.
- **A round that could not run is recorded with its reason.** A step skipped in silence reads as a
  step that passed, which is the failure mode this flow exists to prevent.
- **The record carries how the round was run, not only what it found** — the invocation, the job id,
  the model, the duration — because every other condition above is self-attested and would otherwise
  never reach the artifact.
- **A change this round produces goes back through rounds one and two** before this round runs
  again. They ask questions this one does not, and a fix that lands with neither is the shape the
  evidence below is built on. The loop ends when a round produces no change.

## Evidence

Measured on [#1008](https://github.com/flink-gcp/flink-connector-gcp/pull/1008), 2026-08-22. Sample
size is one pull request; the argument below rests on what the passes converged on rather than on
frequency.

The class in question, `DefaultChangeStreamCoordinatorClient`, entered that pull request *after* both
self-review rounds had run, so three of its five subagent review passes reviewed a diff that did not
contain it and are not evidence either way. What is evidence:

| what ran over the diff containing the class | what it concluded |
| --- | --- |
| a re-run of round one, two subagents over the whole diff | the fields are touched from two threads |
| the author, acting on that | made the fields `volatile`, with a javadoc paragraph around it |
| an independent Codex pass, 16 m 48 s, given the pushed commit and told not to trust the description | the accessors are a **check-then-create**, so `volatile` does not help |

Both self-review passes reached the class and both stopped at `volatile`. What the independent pass
added: a teardown landing between the check and the assignment closes nothing, and the client the
worker then assigns is owned by nobody — a JobManager-side gRPC channel and executor leak, reaching
the service as the process's application default credentials because `close()` nulls the credentials
too. The javadoc written around the insufficient fix described a narrower failure than the code
allowed, which is the shape this flow exists to catch.

That pass also established that the code predated the change and was on `main`. That is the
distinction
between a regression and a defect the change's subject matter reaches, and it is the check a
reviewer who inherited the change's framing has least reason to make.

## Alternatives declined

**Leaving the collection route unstated.** The background job answers with an id, and the plugin's
`status` and `result` commands both carry `disable-model-invocation: true` — so an agent cannot read
its own review through them, and a round whose output is never collected is indistinguishable in the
record from one that found nothing. The skill names the companion script instead. Declining to
mention this was the first version's largest defect and was found by the round reviewing itself.

**A CI check that the third PR comment exists.** It is the cheapest checkable artifact in the flow,
which is exactly why it would be gamed: a comment is trivially produced without a review behind it,
so the check would assert the artifact and not the round. This repository has settled that shape
before — `push-pr-branch` says why it is a skill and not a checker, and `docs/adr/0060` says why
self-review is not push-triggered in CI. The same reasoning applies here, and it is why the flow
rests on the skills being read rather than on a gate.

**Refining `docs/adr/0060` in place rather than a new record.** `docs/adr/README.md` asks for
granularity by decision cluster, and the pre-review flow is arguably one cluster. It is a separate
record because 0060's subject is *self*-review — one model, two questions — and this decision's
subject is a reviewer that is not the author. 0060's title and Decision stay true, which is why this
is neither a refinement of them nor a supersession; 0060 gains a pointer.

**Asking a human instead.** That is what the flow already ends in. This round is one more reader
before one is asked for, not a substitute for one.

**Running it before round two, or instead of round one.** Both self-review rounds ask questions this
one does not, and on #1008 they found things it did not report. Ordering it last is what lets it
review the description the earlier rounds have already corrected.

## Consequences

The flow becomes `self-review` → `self-review-round-two` → `independent-review` → ask for review,
and the third round has its own PR comment for the same reason the first two do: a reader cannot
otherwise tell a review that found nothing from one that never ran.

It costs wall-clock rather than attention — #1008's pass took 16 m 48 s in the background — which is
why there is no small-change carve-out of the kind round two has.

This does not weaken `docs/adr/0060`. This round does not answer either self-review round's
*question*, and those rounds found things it did not report; what it added on #1008 was a defect
round one's first lens was looking for and had not found.

Nothing here makes a second model's review a substitute for a human's.
