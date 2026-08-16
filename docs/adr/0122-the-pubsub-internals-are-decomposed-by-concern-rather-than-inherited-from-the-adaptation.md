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

# ADR-0122: The Pub/Sub internals are decomposed by concern, rather than inherited from the adaptation

- Status: Accepted
- Date: 2026-08-16
- Issues: [#755], [#17], [#31]
- Modules: pubsub (`source.streamingpull`, `sink.writer`)
- Current behavior: unchanged by this record; see
  `docs/content/docs/connectors/datastream/pubsub.md`

## Context

The Pub/Sub sink was adapted from the Flink connector in [GoogleCloudPlatform/pubsub][upstream]
([#17]), and the source reader core was vendored from the same place ([#31], [#79]). The
*implementations* have since been rewritten incrementally — [#297], [#325], [#348], [#351],
[#357], [#358] and [#377] between them replaced every algorithm — and [#755] measured what survives
as names and signatures rather than logic. What was inherited and never revisited is the **class
decomposition**: a structure chosen for a connector an order of magnitude smaller now carries flow
control, acknowledgement extension, park and unpark, metrics, repair and failure routing.

[#755] asked whether that structure costs anything today, and pre-registered that "if nobody can
name one, that is an answer". Nobody could — see Evidence.

The redesign proceeds anyway, and this record says on what basis: **house-style parity**. Every
other connector in this repository was designed from scratch against the same seam conventions —
a noun interface for the client, a `Default*` implementation, single-concern collaborators, one
metrics class — and this module is the only one whose internals were adapted into shape. That is a
maintainability investment made with open eyes, not a fix.

## Decision

**The Pub/Sub internals are decomposed by concern, in six pull requests, before the `1.0.0` tag.
The measured algorithms are behavior-preserved rather than re-derived.**

The public surface does not move. Every class named here is `@Internal`; the `@PublicEvolving`
builders, options, destinations, policies and serialization schemas are untouched, which is why
this can land at all without gating on a compatibility argument.

### Scope criterion

Every moved body is behavior-preserving. Where a body depends today on a shared monitor,
outer-class state, a callback field or caller-owned flush placement, literal motion is impossible;
each such adaptation is listed in this record with the invariant it must keep, is pinned by a named
test, and is covered by a mutant in its pull request's batch. Everything else is literal motion.

A blank-page rewrite of the algorithms is **declined**. They are the accumulated answer to eight
issues' worth of measurement, and re-deriving them would spend that evidence for structure that can
be had without touching them.

### Source reader stack

| Class | Fate | Responsibility |
|---|---|---|
| `AckConfirmationWait` | new | the confirmation round trip, the response check and the four failure mappings, held outside the tracker's monitor |
| `PubSubAckTracker` | one monitor, kept | the four-state lifecycle stays under a single lock; see the declined alternatives |
| `PubSubSplitReader` | orchestrator | fetch, drain, await, then the three guards literal and in order |
| `DataAvailabilitySignal` | new | the level-triggered cross-thread wake |
| `SubscriberSlot` | new | one split's lifecycle, owning both paused and parked |
| `SubscriberRoster` | new | the ordered slot collection and the park, pause and close policies over it |
| `PullSubscriberOpener` / `DefaultPullSubscriberOpener` | new | the house-style top-level opener seam, promoted from a package-private inner interface |
| `PullSubscriber` / `StreamingPullSubscriber` | renamed | the pull bridge; the notification is the opener's parameter, not a property of the type |
| `SubscriberTeardown` | new | the three-exit classification of ADR-0012, the residue counters, and the teardown warnings whose wording the docs quote ([#359]) |

### Sink writer

| Class | Fate | Responsibility |
|---|---|---|
| `PubSubWriter` | kept, reduced | the writer lifecycle and every verdict: what a record, a mail and a failure mean |
| `InFlightTracker` | new | the in-flight ledger, the capacity gate and ADR-0052's progress-bounded waits |
| `DestinationState` | inner class promoted | one topic's publisher and repair debt, including the publish-sequence ordering of parked messages |
| `TopicRepairer` | new | the repair loop, the isolation pass and the recovery budget, behind a three-method context (`republish`, `drainInFlight`, `releaseParked`) — the fourth method this record first planned, a batched-send flush, stayed with the writer because its only repair-side caller, `repairPendingTopics`, never moved |

### Invariants that change house without changing behavior

Each is pinned by a named test and attacked by a mutant in its pull request, **with one stated
exception, invariant 4** — see the note after the list:

1. **The guards run on the fetch thread, in order.** [#377] measured that under a full stall no
   other thread reaches a reporting path either, so a watchdog would report nothing sooner. The
   order — paused-split failure check, then park, then checkpoint detector — is load-bearing
   because parking closes, and closing absorbs the failure. They stay three literal statements in
   `PubSubSplitReader.fetch()`; a guard list would make the order data.
2. **Parked implies paused.** `SubscriberSlot` owns both flags, so the invariant holds by
   construction rather than by convention between a set and a map, and a failed reopen still
   leaves the split paused.
3. **Every shutdown starts before any close.** The park and the reader's close each build one
   `Closers.closeAll` list, which is what [#297] bought and what a per-split loop would spend.
4. **The acknowledgement tracker keeps one monitor.** `addPendingAck` runs on client-library
   callback threads while the rest runs on the task and fetcher threads, and settling happens
   outside the lock.
5. **The teardown reads the already-reported latch live.** ADR-0012 rejected a snapshot taken
   before shutdown, because shutdown can produce a new failure between the phases;
   `SubscriberTeardown` therefore takes a predicate that reads under the subscriber's monitor.
6. **The capacity wait flushes once; the drains do not.** `awaitCapacity` asks the publishers for
   what is still batched after its first empty yield, whereas the drains are pre-flushed by their
   callers. The extracted tracker keeps that asymmetry, and the idle hook belongs to the capacity
   wait alone.
7. **The solo verdict crosses the repair seam explicitly.** Whether a rejection came from a batch
   or from a message published alone is what makes `INVALID_ARGUMENT` routable, so it is a
   parameter of the republish call rather than an assumption inside it.
8. **The extracted waits log under the writer's category.** An operator's log filter and the tests
   that pin the warning both key off it.

**Invariant 4 is the exception, and it is stated rather than tested.** No test in this repository
fails if the acknowledgement loop and the confirmation wait are moved back inside `synchronized
(this)`: the symptom is a callback thread stalled behind a server round trip, which a
single-threaded unit test cannot observe and which an integration test would only reach as a
timing-dependent slowdown. Writing a test that reliably distinguishes the two would mean blocking
a settle handle from a second thread and asserting a *non*-event, which is the shape flaky tests
are made of. What guards it instead is the `@GuardedBy` annotations, the scope of the
`synchronized` block being visibly narrower than the method, and this record. A later change that
widens it has to argue with this paragraph.

## Evidence

Measured 2026-08-16 over `flink-connector-gcp-pubsub/src/main`, whose whole history at that point
runs 2026-07-19 to 2026-08-16 — about four weeks, and the repository's entire life. That short
span is itself part of the reading: it is the reason a co-change count is weak evidence of a
structural cost, and the reason this record does not claim the decomposition has been *proven*
harmless. It has only failed to be caught costing anything yet.

| Fact | Value | Sample |
|---|---|---|
| Commits touching the module's main tree | 103 (including 4 merges) | 2026-07-19 to 2026-08-16 |
| Main-tree files per commit | mean 4.2, median 3 | those 103 commits |
| `PubSubSplitReader` commits also touching the subscriber | 8 of 13 | non-merge commits touching that file |
| …of which fall outside the two waves (initial build-out 2026-07-25, [#357] on 2026-08-07/08) | 0 | the same 8 |
| Steady-state fix breadth | 2–4 main files | the commits closing [#348], [#351], [#358], [#377] |

Two qualifications a later reader should not have to re-derive. The mean is sensitive to which
commits count: excluding the four merges, and excluding the 67-file organisation-header sweep
`f67b5345`, the same measurement gives 4.37. And the one bug that was genuinely hard to place,
[#440], turned out to live in *test* infrastructure — its fix touched no main-tree file — while
[#297]'s cost was pattern duplication across three sibling classes, which was already paid down by
extracting `Closers` and `BoundedShutdown` into the base module rather than by redecomposing
anything.

## Alternatives declined

- **Leaving the decomposition alone.** The honest reading of the evidence, and what a
  cost-benefit argument alone would conclude. Declined because the cost being unmeasurable is not
  the same as the module being consistent with the rest of the repository, and consistency is
  worth paying for once, before the first published version, rather than never.
- **A blank-page rewrite of the algorithms.** See the scope criterion.
- **Splitting `PubSubAckTracker` into per-split ledgers.** Pending state is per split, but staged
  and checkpoint-bound state is cross-split and swept by checkpoint id. Per-split objects would
  either duplicate the sweep or share one monitor between them, which is a worse concurrency
  contract than the single class.
- **A separately synchronized message buffer.** The subscriber's receive path performs its closed
  check, its acknowledgement registration, its buffer insertion and its byte accounting under one
  monitor, and shutdown clears under that same monitor. A buffer with its own lock would let a
  callback admitted before shutdown insert after it.
- **A guard pipeline in the split reader.** See invariant 1.
- **A destination registry, an ordering-key gate, and a flush coordinator separate from the
  in-flight ledger.** The map carries no policy; the ordering-key guard is eight lines plus data
  that belongs to the destination; and the wait predicates re-read the ledger's counters every
  pass, so separating them would be a getter-chatter seam with no independent test surface.
- **Promoting the wait machinery to the base module, or sharing it with the Bigtable writer.**
  The contracts differ: ADR-0052 bounds a stalled Pub/Sub wait, ADR-0078 reports a stalled Bigtable
  one. A shared class would have to carry both policies as a flag on day one.
- **`AsyncSinkBase`.** ADR-0004 stands and is not re-proposed here.

## Consequences

- Six pull requests, each independently green; the five that move production code each carry
  their own mutation batch, and the sixth closes this record. The integration tests against the
  emulator and real GCP are the decomposition-agnostic behavioral net: no production-change pull
  request edits their behavior or assertions — [#765] retargeted renamed types in three of them,
  nothing more.
- The renaming pull requests additionally build against Flink 1.20.4, which selects a different
  source root, and build the downstream `flink-sql-connector-gcp-pubsub`, which a module-scoped
  verification does not reach.
- **This record decides nothing about attribution.** As [#755] pre-registered, the Apache-2.0
  headers, the `NOTICE` entry and the module README's provenance section are not moved by a
  restructuring, and a diff percentage is not a reason to touch any of them. Genuinely new files
  carry this repository's header; the README's provenance list is corrected for renames in the
  same pull request that performs them, so it keeps naming files that exist.

  Whether a given file has stopped carrying anything of the original is a **separate question,
  settled separately on an audit against upstream** rather than as a by-product of this
  decomposition — ADR-0123 is that audit. A later reader should not mistake this record for that
  determination, in either direction.

## Outcome

The series completed on 2026-08-16. Five pull requests moved the code — [#761], [#764], [#765],
[#767] and [#768] — and the sixth closed this record with the two tables below. (The notice
retirement [#763] and the agent-guidance scoping [#766] landed inside the series but moved no
production code; [#763]'s determination belongs to ADR-0123.) The structure the Decision tables
plan is the structure that landed.

The name map, for a reader holding an old stack trace, test name or link against the new tree:

| Before | After | Pull request |
|---|---|---|
| `PubSubAckTracker` (confirmation wait inline) | + `AckConfirmationWait` | [#761] |
| `PubSubWriter` (ledger and waits inline) | + `InFlightTracker` | [#764] |
| `NotifyingPullSubscriber` | `PullSubscriber` | [#765] |
| `PubSubNotifyingPullSubscriber` | `StreamingPullSubscriber` + `SubscriberTeardown` | [#765] |
| `PubSubWriter.DestinationState` (inner) | `DestinationState` (top-level) | [#767] |
| `PubSubWriter.repairDestination` | `TopicRepairer.repair` | [#767] |
| `PubSubSplitReader.AssignedSplit` (inner) | `SubscriberSlot` | [#768] |
| `PubSubSplitReader.SubscriberOpener` (inner) | `PullSubscriberOpener` + `DefaultPullSubscriberOpener` | [#768] |
| `PubSubSplitReader` signal fields and methods (`signalDataAvailable` et al.) | `DataAvailabilitySignal` | [#768] |
| `PubSubSplitReader` roster policy | `SubscriberRoster` | [#768] |

And the promised list of every place literal motion was impossible, with the invariant each
adaptation keeps and the test that pins it:

| Adaptation | Invariant kept | Pinning test |
|---|---|---|
| [#764]: the tracker logs through the writer's injected `Logger`, not its own | 8 — the extracted waits log under the writer's category | `InFlightTrackerTest.aStalledWaitWarnsUnderTheWritersLogCategory` |
| [#764]: `sendWhatIsStillBatched` is injected as an idle hook used by `awaitCapacity` alone; the drains stay pre-flushed by their callers | 6 — the capacity wait flushes once; the drains do not | `InFlightTrackerTest.onlyTheCapacityWaitAsksForWhatIsStillBatched` |
| [#764]: `checkAsyncError` is wired into the tracker as an injected failure check rather than moved | ADR-0052's waits abort on a failure captured off-thread | `InFlightTrackerTest.bothWaitsSurfaceACapturedFailureEvenWithNothingInFlight` |
| [#764]: the writer's `maxConsecutiveRejections` precondition stays ahead of the tracker's construction, so a doubly invalid configuration still reports the writer's message first | construction-time validation, unchanged in which message wins | none — deliberately unpinned, recorded in [#764]'s review record: a test sees only the one exception that fires |
| [#765]: the teardown takes a live `isAlreadyReported` predicate that reads the latch under the subscriber's monitor at classification time, never a construction-time snapshot | 5 — the teardown reads the already-reported latch live | `SubscriberTeardownTest.absorbsTheClientsReportOfTheFailureItAlreadyDelivered` |
| [#767]: `RepairContext` is three methods, not the four first planned — the batched-send flush stayed with the writer because its only repair-side caller, `repairPendingTopics`, never moved | 6's asymmetry — the flush placement stays caller-owned | `PubSubWriterProgressTimeoutTest.theRepairsOwnDrainAlsoSendsWhatIsStillBatched`, which cannot complete unless `repairPendingTopics` flushes before its leading drain |
| [#767]: the solo/batched verdict crosses `RepairContext.republish` as an explicit parameter rather than being re-derived inside the repairer | 7 — the isolation verdict is data at the seam | `TopicRepairerTest.theIsolationPassPublishesSoloAndDrainsOncePerMessage` |
| [#767] and [#768]: the repairer and the roster log through the injected writer and reader `Logger` | 8, applied to both halves — operator-facing lines keep their categories | the roster's half by `PubSubSplitReaderTest.aPausedSplitOutgrowingItsMessageBoundHasItsSubscriberStopped` (a `LogCapture` against the reader's category); the repairer's half has no category oracle — its message text is pinned by `TopicRepairerTest`'s exhaustion tests and the category was verified in [#767]'s review |
| [#768]: the park's `pausedSplits.isEmpty()` fast path becomes lazy list allocation — the set no longer exists; the allocation half of the base property is kept, and the per-fetch O(assigned splits) walk is stated in the roster comment rather than claimed neutral | 3 — one `Closers.closeAll` list per park | `PubSubSplitReaderTest.parkingSeveralSplitsAtOnceShutsThemAllDownBeforeWaitingOnAny` |
| [#768]: the [#348] guard's iteration-order comment is reworded — it argued from `HashSet` vs `LinkedHashMap`, structures that no longer both exist | the reproducibility contract and the `SplitFetcherManager` belt-and-braces citation are kept, comment-only | none — a comment rewording with nothing behavioral to pin: the iteration source, the assignment-ordered slot map, is the same map the base iterated |
| [#768]: `pauseOrResume` drops a pause for an unassigned split instead of recording its id — a slot can only pause an assigned split | unreachable in production (the `SplitFetcherManager` filtering argument), so dropped and stored must be indistinguishable there | `SubscriberRosterTest.aPauseForAnUnassignedSplitIsDroppedNotStored`, which assigns the split *after* the stray pause |
| [#768]: javadoc cross-references retargeted where the classes split | prose only; no behavior | none — nothing to pin |
| [#768], added in review: `SubscriberSlot.resume()` carries its own precondition, so parked-implies-paused is enforced at both ends rather than by caller ordering | 2 — parked implies paused, by construction | `SubscriberSlotTest.resumingWhileStillParkedThrows` |

[#17]: https://github.com/flink-gcp/flink-connector-gcp/issues/17
[#31]: https://github.com/flink-gcp/flink-connector-gcp/issues/31
[#79]: https://github.com/flink-gcp/flink-connector-gcp/issues/79
[#297]: https://github.com/flink-gcp/flink-connector-gcp/issues/297
[#325]: https://github.com/flink-gcp/flink-connector-gcp/issues/325
[#348]: https://github.com/flink-gcp/flink-connector-gcp/issues/348
[#351]: https://github.com/flink-gcp/flink-connector-gcp/issues/351
[#357]: https://github.com/flink-gcp/flink-connector-gcp/issues/357
[#358]: https://github.com/flink-gcp/flink-connector-gcp/issues/358
[#359]: https://github.com/flink-gcp/flink-connector-gcp/issues/359
[#377]: https://github.com/flink-gcp/flink-connector-gcp/issues/377
[#440]: https://github.com/flink-gcp/flink-connector-gcp/issues/440
[#755]: https://github.com/flink-gcp/flink-connector-gcp/issues/755
[#761]: https://github.com/flink-gcp/flink-connector-gcp/pull/761
[#763]: https://github.com/flink-gcp/flink-connector-gcp/pull/763
[#764]: https://github.com/flink-gcp/flink-connector-gcp/pull/764
[#765]: https://github.com/flink-gcp/flink-connector-gcp/pull/765
[#766]: https://github.com/flink-gcp/flink-connector-gcp/pull/766
[#767]: https://github.com/flink-gcp/flink-connector-gcp/pull/767
[#768]: https://github.com/flink-gcp/flink-connector-gcp/pull/768
[upstream]: https://github.com/GoogleCloudPlatform/pubsub
