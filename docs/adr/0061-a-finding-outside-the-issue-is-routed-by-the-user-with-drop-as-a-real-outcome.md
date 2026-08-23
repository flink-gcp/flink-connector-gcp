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

# ADR-0061: A finding outside the issue is routed by the user, with drop as a real outcome

- Status: Accepted
- Date: 2026-08-07 (the four constraints each violated in one sitting on PR
  [#339](https://github.com/flink-gcp/flink-connector-gcp/pull/339); the
  leave-it-in-a-note half first corrected on PR
  [#322](https://github.com/flink-gcp/flink-connector-gcp/pull/322))
- Issues: —
- Modules: all (workflow)
- Current behavior: root `AGENTS.md` § Workflow rules (the imperative form)

## Decision

A finding outside the issue being worked has **three outcomes, not two**: folded into the
current change, filed as an issue, or **dropped**. Dropping has to stay on the list, because
a rule offering only "fold or file" makes filing the safe default, and a tracker keeps what
is filed forever. The routing is the user's: ask in the session that found it, file only what
the user has said is not being folded in — and `gh issue create` is never how the routing
happens on its own. An issue for work about to happen in the open pull request is noise.

Four constraints, each of them a mistake PR [#339](https://github.com/flink-gcp/flink-connector-gcp/pull/339) made in one sitting:

- **Verify the finding before routing it.** A review subagent's example is not evidence. PR [#339](https://github.com/flink-gcp/flink-connector-gcp/pull/339)
  filed a hole in the licensing gate on a synthesised `THIRD-PARTY.txt` line that
  license-maven-plugin did not in fact produce for the dual-licensed artifact the tree carried
  then (`javax.annotation-api`, since excluded by [#352](https://github.com/flink-gcp/flink-connector-gcp/issues/352)) — so the issue described a
  defect that does not exist. An unverified finding wastes the
  user's decision, not just the tracker.
- **Route every finding, not the ones that look like they need a decision.** PR [#339](https://github.com/flink-gcp/flink-connector-gcp/pull/339) asked about
  three, got three answers, and filed four more unasked. Having asked is not a licence for
  the rest.
- **A decision the user has already given is not reopenable by the note.** Told to fold the
  pom consolidation into PR [#339](https://github.com/flink-gcp/flink-connector-gcp/pull/339), it filed an issue instead. That is the same error pointed the
  other way, and it is worse: it overrides an answer rather than skipping a question.
- **Never batch-file at the end of a review.** Findings arrive together; the decisions are
  one per finding, and a batch is how the ones that should have been dropped ride along with
  the ones that should not.

**A deferral left in a PR comment or a `CLAUDE.md` "known gap" line is the silent deferral
wearing a disguise**: the reason is recorded, the work is not tracked, and the next reader
meets a claim with no anchor. A filed issue states the **grounded** reason the work is not
being done now — a measured cost, a blocker in the code, scope the user has not approved —
never "not planned" or "out of scope", which describe an intention rather than the item. It
carries the better approach when one can be named, plus a **measure-first** step when the
cost or benefit is asserted rather than measured. Filing with *no* known answer is still
right when the first task is finding out whether the problem is real; the issue says so.

## Evidence

[#323], [#324] and [#325] came out of PR [#322](https://github.com/flink-gcp/flink-connector-gcp/pull/322) this way, after the same mistake had been made
there first — three findings written up in a self-review comment and a module `CLAUDE.md`,
and left there.

[#323]: https://github.com/flink-gcp/flink-connector-gcp/issues/323
[#324]: https://github.com/flink-gcp/flink-connector-gcp/issues/324
[#325]: https://github.com/flink-gcp/flink-connector-gcp/issues/325
