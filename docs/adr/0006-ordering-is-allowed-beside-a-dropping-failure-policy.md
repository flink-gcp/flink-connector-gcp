<!--
Copyright 2026 laughingman7743

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

# ADR-0006: Message ordering is allowed beside a dropping failure policy

- Status: Accepted
- Date: 2026-08-03
- Issues: [#215] (lifting the `build()` precondition [#206] shipped)
- Modules: pubsub
- Current behavior: `docs/content/docs/connectors/datastream/pubsub.md` § Ordering and a
  dropping policy

## Context

[#206] shipped a `build()` precondition rejecting `enableMessageOrdering` beside a dropping
handler, because a dropped keyed message pauses its ordering key in the SDK and nothing would
resume it. [#215] lifted it: allowed, with the gap documented rather than mechanised.

## Evidence

The SDK facts the design turns on — read from `google-cloud-pubsub` 1.152.0 sources, not
assumed:

- `SequentialExecutorService.cancelQueuedTasks` adds the ordering key to `keysWithErrors`
  **unconditionally**, taking a bare `Throwable` it never inspects — so an `INVALID_ARGUMENT`
  poisons a key exactly as a `NOT_FOUND` does.
- Nothing auto-resumes: `keysWithErrors.remove` has one caller, the public `resumePublish`.
- A later `publish()` on a paused key **returns an already-failed future** carrying the shared
  static `CancellationException` rather than throwing — which is what makes the "leave it
  paused" design below work at all.

A naive lift would have let one dropped keyed message kill its key for the writer's lifetime.

## Decision

Three changes, each with a reason not to re-litigate:

- **(a) Parking a cascade no longer depends on the create disposition** — a cascade's root may
  be a dropped message, which `CREATE_NEVER` needs repaired too. The tempting narrower form
  (park only when a drop is recorded for that key) **is [#78]'s bug rebuilt**: the drop mail and
  the cascade mail arrive in either order, so a cascade observed first would find nothing
  recorded and become `asyncError`. Unconditional parking also makes a root's error message win
  over a cascade's under `CREATE_NEVER`, which used to depend on mail order.
- **(b) The repair carries a reason** — `DestinationState.topicMissing`, set only where a
  `TOPIC_NOT_FOUND` is parked, so only that repair creates a topic. This is what preserves
  "`CREATE_NEVER` creates nothing" once (a) removed the disposition guard from the parking
  branches, and the invariant is asserted directly rather than through "nothing is parked".
  Creation is decided **per attempt but performed at most once per repair**: a batch parked for
  another reason can turn out to need it (its republish being the first publish to meet the
  missing topic), while the retry loop itself is for metadata propagating over a topic that by
  then exists.
- **(c) A dropped keyed message registers its key** — `keysToResume`, drained by
  `resumeOrderingKeys` — and **the resume is deliberately not in `routeFailedMessage`**:
  `write()` tests `repairNeeded` *before* `awaitCapacity()`, and mails run inside it, so a key
  resumed from the failure mail could be published to by the rest of that same `write()` while
  its cascades were still parked, putting a newer message ahead of older ones. Left paused, that
  racing publish comes back cancelled, is parked, and is republished in sequence order with the
  rest. Without (c) the writer is still correct — the next message for the key fails and is
  repaired — but (c) is what makes `flush()`'s `while (repairNeeded)` loop mean *no checkpoint
  completes with a key paused*.

## Consequences

- The `return` after a throwing handler is **belt and braces, and measured as such**: a mutant
  deleting it survives, because `asyncError` gates every path into a repair anyway, and the test
  says so rather than claiming a discrimination it does not have.
- The fake publisher models the paused-key state itself since [#277] — a failed keyed publish
  pauses its key, a publish on a paused key comes back cancelled without being published, and
  only `resumePublish` clears it — so the racing-publish reordering above is pinned by test
  (`aKeyPausedByADropStaysPausedUntilTheRepairResumesIt`) rather than verified only by reading
  the SDK source.
- The one finding from that SDK reading that was not a writer decision — a `shutdown()` that
  never returns — is ADR-0007's subject ([#265]).

[#78]: https://github.com/laughingman7743/flink-connector-gcp/issues/78
[#206]: https://github.com/laughingman7743/flink-connector-gcp/issues/206
[#215]: https://github.com/laughingman7743/flink-connector-gcp/issues/215
[#265]: https://github.com/laughingman7743/flink-connector-gcp/issues/265
[#277]: https://github.com/laughingman7743/flink-connector-gcp/issues/277
