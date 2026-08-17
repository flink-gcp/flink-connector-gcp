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

# ADR-0011: The Pub/Sub source is FLIP-27 streaming pull, and readers checkpoint no splits

- Status: Accepted
- Date: 2026-07-25 ([#79], [#80], [#81], [#101]); gated-suite scope 2026-08-01 ([#82])
- Issues: [#79], [#80], [#81], [#101], [#118], [#82]
- Modules: pubsub
- Current behavior: `docs/content/docs/connectors/datastream/pubsub.md` (source half, including
  § Why streaming pull rather than synchronous pull)

## Decision

FLIP-27 streaming-pull source; split = (subscription, uid), ack on checkpoint completion, nack
on close. Streaming pull over unary `Pull` is argued on the docs page (lease extension,
ordering), with the costs stated both ways.

- **The reader checkpoints no splits** — the enumerator is the only owner of split assignment,
  recomputing the deterministic plan
  (`splitCount = PER_KEY ? |subs| : max(|subs|, parallelism)`) on every start — because
  `SourceOperator` unions reader-restored splits with the enumerator's plan, so a reader that
  snapshotted its splits would leave a rescaled restore with one subscription consumed by two
  subtasks, exactly what `PER_KEY` exists to prevent (the [#79] self-review bug; pinned by
  `checkpointsNeverCarrySplits`, exercised end-to-end by `PubSubSourceRecoveryITCase`).
- Tuning lives in one `PubSubSubscriberOptions` object (nested-options pattern, same shape as
  `PubSubPublisherOptions`).
- **The subscriber shutdown mode is not exposed** (deviating from the [#80] issue text):
  `NACK_IMMEDIATELY` is fixed because `WAIT_FOR_PROCESSING` waits for acknowledgements that only
  arrive at checkpoint completion, which never happens during close; only `shutdownTimeout` is a
  knob (an SDK enum on the public API would also break the [#47] SQL mapping).
- **The "fail when running without checkpointing" guard cannot read the configuration**
  (deviating from the issue text): `SourceReaderContext.getConfiguration()` is the TaskManager
  configuration (`SourceOperatorFactory` passes `getTaskManagerInfo().getConfiguration()`),
  while `env.enableCheckpointing(...)` writes into the job configuration, so absence proves
  nothing and failing on it would break jobs that enable checkpointing programmatically while
  passing every MiniCluster test. Replaced by `MissingCheckpointDetector` (no checkpoint taken +
  messages outstanding + budget spent → fail), **evaluated from `PubSubSplitReader.fetch()`, not
  the record path** — once flow control fills, the client stops delivering and `pollNext` is
  never called again, so a record-driven check would go silent in exactly the state it exists to
  catch; the detector bounds the fetch park only while armed, so a healthy reader parks
  indefinitely as before. Its budget ([#101]) starts at the reader's **first split assignment**,
  not at `createReader` (a reader is created before the enumerator's startup check finishes); an
  unstarted detector is **not armed**, so a reader assigned no split parks indefinitely; and it
  retires at the **first checkpoint barrier** — `SourceOperator.snapshotState` is called
  unconditionally, so a barrier carrying no data counts, which bounds the guard to measuring one
  interval, once. The detector's two budget fields are deliberately plain, not volatile:
  `AddSplitsTask` runs on the fetcher thread, the same thread as `fetch()`, so both are confined to
  it. **Its third field is volatile and has to be**, which this record first stated the other way
  round: `checkpointTaken()` is called from the reader's *task* thread while the budget is read on
  the fetcher's, so the "a checkpoint was taken" latch crosses threads where the budget does not.
  The reasoning for the split lives in the class javadoc. The config-derived ack-extension check is
  a best-effort warning only.
- `parallelPullCount > 1` is rejected with `orderingMode(PER_KEY)` rather than silently forced
  to 1 (the factory still force-sets 1 so the guarantee does not rest on the SDK default).
- **The startup check** ([#81]) verifies every subscription (`GetSubscription`) before any split
  is assigned and rejects: a missing subscription without create options, an unordered
  subscription under `orderingMode(PER_KEY)`, an exactly-once-delivery subscription, and
  `deserializationFailurePolicy(NACK)` on a subscription without a dead-letter policy — the NACK
  requirement is enforced twice, in the builder for auto-created subscriptions and in the
  enumerator preflight for existing ones. The failure messages name the missing permission or
  setting on purpose; that text is the entire value of those branches.
- Subscription auto-creation is authorized by the **presence** of per-subscription
  `SubscriptionCreateOptions` — no disposition enum, because a subscription without a topic
  binding is not a subscription (ADR-0014 records how the two directions spell creation
  differently; the source never creates a topic).
- `StartPosition` seeks **once, at the first start of a job, never on a restore**: the guard is
  `PubSubEnumeratorState.startPositionApplied`, and a checkpoint with the flag still false
  contains no reader holding a split, so re-applying after such a restore is safe; a redeploy
  without state seeks again.

## Consequences

**The real-GCP gated suite ([#82]) is the only coverage of**: ordered dispatch (per-key callback
serialization is gated on a streaming-pull response field the emulator never sets), dead-letter
forwarding (performed by the Pub/Sub service agent under project-level grants in `opentofu/`,
not by the job's credentials), seek on an ordering-enabled subscription, the create-option knobs
persisting (the emulator stores but ignores them), nack-redelivery *promptness* (an
observed-behaviour bound — the [#118] settlement moved the claim there and left the emulator IT
asserting non-loss only), and the subscription admin's permission-denied message texts (via
impersonation of the zero-role `e2e-no-pubsub` account — the local-run self-grant is documented
on the docs page and deliberately not in opentofu, keeping personal identifiers out of source).
Gating is `@EnabledIfEnvironmentVariable` on `PUBSUB_IT_PROJECT` **on every concrete class,
never the abstract base** — `scripts/e2e-gated-its.sh` greps the annotation literal and then
expects a surefire report per matching file. `PubSubSubscriptionAdmin` carries a
`CredentialsProvider` constructor that the impersonation tests drive. It was `@VisibleForTesting`
and test-only when this was written; [#139] and [#546] gave it a production caller — the source's
own `serviceAccountKeyFile` reaches the admin through it — so it is an ordinary public constructor
now, and this paragraph no longer claims otherwise.

[#47]: https://github.com/laughingman7743/flink-connector-gcp/issues/47
[#79]: https://github.com/laughingman7743/flink-connector-gcp/issues/79
[#80]: https://github.com/laughingman7743/flink-connector-gcp/issues/80
[#81]: https://github.com/laughingman7743/flink-connector-gcp/issues/81
[#82]: https://github.com/laughingman7743/flink-connector-gcp/issues/82
[#101]: https://github.com/laughingman7743/flink-connector-gcp/issues/101
[#118]: https://github.com/laughingman7743/flink-connector-gcp/issues/118
[#139]: https://github.com/flink-gcp/flink-connector-gcp/issues/139
[#546]: https://github.com/flink-gcp/flink-connector-gcp/issues/546
