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

# ADR-0045: A `ROW_LEVEL` verdict is confirmed solo before it is routed (Bigtable)

- Status: Accepted
- Date: 2026-08-07
- Issues: [#239] (adopting Pub/Sub's [#264] design — ADR-0008; [#361] is the open bound)
- Modules: bigtable
- Current behavior: `docs/content/docs/connectors/datastream/bigtable.md` § Failed-mutation
  policy

## Context

Bigtable rejects the whole `MutateRows` request rather than the entry that provoked it, gax fans
that one status out over every entry future, and the sink routed each — so under `logAndDrop`
one malformed record silently discarded its whole batch while the job stayed green.

## Decision

The writer **parks** a row-level verdict answering a batched submission and `runIsolationPass()`
re-submits each parked mutation as the only entry of its own request: a solo success was
collateral damage and is now applied, a solo rejection is what reaches the handler. The pass
runs from `flush()` *and* from `write()`, which bounds the park to one batch rather than a
checkpoint interval, and it terminates because every submission inside it is solo.

- **The discriminator is our own submission, not the exception**, and that is the decision not
  to re-argue. gax's `BatcherImpl.sendOutstanding()` swaps the open batch, so an entry added to
  an emptied accumulator and flushed at once travels alone — a property of code in this
  repository. The alternative was measured and declined:
  `MutateRowsBatchingDescriptor.splitException` gives a per-entry rejection an
  `io.grpc.StatusRuntimeException` cause while `createSyntheticErrorForRpcFailure` gives the
  request-level one the original `ApiException` as its cause, so `cause instanceof ApiException`
  discriminates at bigtable 2.80.0 / gax 2.82.0 (read 2026-08-07). Nothing documents it, nothing
  would flag a change, and a silent change re-opens a P0 in the unsafe direction. So **every**
  non-solo row-level verdict is isolated, at one extra request per genuinely bad record. What
  that optimisation would buy is **unmeasured**: of the two `INVALID_ARGUMENT` conditions the
  suite exercises, only the timestamp mismatch has ever been run with a second entry in the
  request — the empty row key was measured on a single-entry request, which cannot tell the two
  apart. A sample of one, and the round-2 review of [#239]'s own pull request is what caught the
  claim that it was two.
- **Fail-on-batched-rejection was declined** — it defeats the dropping policy the user opted
  into — as was **client-side limit validation**, which covers only the limits we encode. Both
  settled on Pub/Sub [#264], whose solo-verdict isolation republish this adopts wholesale; what
  Bigtable does *not* need is the half of [#264] that exists for ordering keys and topic
  creation — no `DestinationState`, no recovery budget, no resume between publishes.
- **The cost is real and belongs in the documentation**: while isolating, the sink spends
  roughly one request per record; `parkedMutations` reports it. Measured on PR
  [#360](https://github.com/laughingman7743/flink-connector-gcp/pull/360), and narrower than it
  first reads: under the default `failJob()` the pass issues **one** solo request before the
  handler's throw becomes `asyncError` and the pass's own drain rethrows it, so the unbounded
  case is *only* a dropping policy — where nothing ends the pass, because nothing is meant to.
  Bounding it with a configurable threshold is [#361]; the same shape probably exists in
  Pub/Sub's pass, which that issue is scoped to confirm.
- Pinned offline by `BigtableWriterTest` through a `FakeMutationBatcher` that decides outcomes
  **per request** — a request carrying a rejected row key fails every entry of that request — so
  the pass's behaviour emerges from the fake rather than being scripted; and against the service
  by `BigtableRejectionRealGcpITCase.routesOnlyTheRejectedEntryAndAppliesTheRestOfItsBatch`,
  which asserts the *outcome* rather than the rejection's granularity: the service answers per
  entry for some conditions, and the sink must behave the same either way.

[#239]: https://github.com/laughingman7743/flink-connector-gcp/issues/239
[#264]: https://github.com/laughingman7743/flink-connector-gcp/issues/264
[#361]: https://github.com/laughingman7743/flink-connector-gcp/issues/361
