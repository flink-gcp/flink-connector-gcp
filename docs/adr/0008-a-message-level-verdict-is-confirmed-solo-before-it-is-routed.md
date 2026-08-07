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

# ADR-0008: A `MESSAGE_LEVEL` verdict is confirmed solo before it is routed

- Status: Accepted
- Date: 2026-08-05
- Issues: [#264] (closing [#269] with it), [#303] (the gated pin)
- Modules: pubsub (Bigtable adopted the same design in [#239] — its module record)
- Current behavior: `docs/content/docs/connectors/datastream/pubsub.md` § Failed-message policy

## Context / Evidence

Measured on real Pub/Sub 2026-08-06 (record on [#264]): a `Publish` carrying one invalid message
is rejected **all-or-nothing**, the SDK sets the *same* `Throwable` instance on every co-batched
future, and nothing in the status names the offender (`details=0`, no `BadRequest`) — so routing
on the report would hand a whole batch to a dropping handler for one bad message.

## Decision

The writer parks a non-solo `INVALID_ARGUMENT` (`DestinationState.isolationNeeded`, consumed per
attempt like `topicMissing`) and the repair runs an **isolation pass**: each parked message goes
out as its own single-message request (`publishTo(..., soloVerdict=true)` + `flushOutstanding` +
`drainInFlight` per message), and only a message rejected solo reaches `routeFailedMessage`.

Decisions not to re-litigate:

- **The pass resumes a key right after a drop pauses it** (`resumeRegisteredKeys`, between
  publishes) — this does not violate ADR-0006's "resume never in `routeFailedMessage`", because
  the resume stays inside the repair, the key's remaining messages are held by the pass in
  sequence order, and drains only complete publishes — and without it one drop costs one budget
  attempt, [#269] rebuilt inside the fix.
- **A batched report is not counted** by `publishFailure` (the [#208] cascade-exclusion
  argument: one incident, not batch-size errors), so `errorClass.INVALID_ARGUMENT.errors` and
  `numRecordsSendErrors` count true rejections.
- **Client-side limit validation was declined** as the fix (it covers only the limits we
  encode) and **fail-on-batched-rejection was declined** (it defeats the dropping policy).

[#269] resolved as fallout: a poisoned key drains in one attempt however long the run, budget
semantics unchanged — what remains is the two-variant exhaustion message (`kept failing …` vs
`could not drain its parked messages within the recovery budget …`, chosen by whether this
repair handed messages to the handler), each variant pinned by test.

## Consequences

An oversized message under default batching never shared a request (the SDK sends an element
exceeding the byte threshold alone, measured), so the fan-out concerns under-threshold
violations — attribute limits and the like. The measured behaviour is pinned end-to-end by
`PubSubSinkRejectionRealGcpITCase` ([#303]) — the first sink-side gated class, extending the
source-side `AbstractPubSubRealGcpITCase` cross-package, which is the settled answer to where a
sink gated class lives — deliberately at the outcome level (survivors published, exactly the
invalid message routed, flush green), since the outcome is what the fix guarantees whatever the
service's rejection granularity.

[#208]: https://github.com/laughingman7743/flink-connector-gcp/issues/208
[#239]: https://github.com/laughingman7743/flink-connector-gcp/issues/239
[#264]: https://github.com/laughingman7743/flink-connector-gcp/issues/264
[#269]: https://github.com/laughingman7743/flink-connector-gcp/issues/269
[#303]: https://github.com/laughingman7743/flink-connector-gcp/issues/303
