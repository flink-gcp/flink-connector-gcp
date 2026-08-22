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

# ADR-0128: An enumerator's closeable seam is minted per enumerator

- Status: Accepted
- Date: 2026-08-22 (measured 2026-08-22)
- Issues: [#990](https://github.com/flink-gcp/flink-connector-gcp/issues/990)
- Modules: spanner, bigtable, bigquery (rule applies to all)
- Current behavior: a global restore no longer fails the job with a closed-seam error

## Context

Each of the three bounded sources plans through one closeable seam, which the enumerator owns and
closes: Spanner's `PartitionPlanner`, Bigtable's `RowKeySampler`, BigQuery's `ReadSessionCreator`.
All three used to hold that seam as a single non-transient field on the source configuration, built
once by the builder, and all three implementations refuse further use once closed — Spanner and
BigQuery with a flag of their own, Bigtable with one inside `LazyBigtableDataClient`.

The JobManager does not build a fresh source object for each enumerator. Read against
`flink-runtime` 2.1.2, and confirmed on 1.20.4, the other supported minor, where
`RecreateOnResetOperatorCoordinator.provider`, `SourceCoordinatorProvider.source`,
`SchedulerBase.restoreState(Set, boolean)` and `notifyCoordinatorsOfEmptyGlobalRestore()` all still
exist with the same shapes: `RecreateOnResetOperatorCoordinator` holds `private final Provider provider`
and, on `resetToCheckpoint`, builds the replacement coordinator from that same provider;
`SourceCoordinatorProvider` holds `private final Source<?, SplitT, ?> source` and builds every
`SourceCoordinator` from it. Closing the old coordinator closes its enumerator, and the enumerator's
`close()` closes the seam. So the seam a second enumerator was handed had already been closed by the
first, and the job failed on that attempt and on every retry after it.

Which failures reach that path was already measured in this repository, and the answer is narrower
than "a failover". `BigQueryQueryJobIdentityITCase` records, against Flink 2.2.1 and re-run by the
weekly version matrix, that a task failure keeps the coordinator alive under the default `region`
strategy and under `full` alike; only the global-restore path resets coordinators, and it is reached
by a JobManager failover or by a coordinator-reported failure, since `SplitEnumeratorContext#failJob`
escalates to `handleGlobalFailure`. That probe drives a `NumberSequenceSource`, so it never touched a
connector seam and did not report the defect, but it is the measurement the defect rests on.

The exposure was wider than a race before the first checkpoint. `SchedulerBase.restoreState` takes
`notifyCoordinatorsOfEmptyGlobalRestore()` whenever there is no `CheckpointCoordinator`, which is the
ordinary configuration for a bounded batch read, and that resets every coordinator in the job. The
failure that most often causes such a restore is the planning call itself: the enumerator base class
throws from its completion handler, `SourceCoordinator` turns that into `context.failJob`, and the
restart that exists to recover from a transient planning error was guaranteed to fail differently
and permanently. The message the user saw named the wrong thing on every retry.

```text
Failed to plan the Spanner <read> of <database>; the source cannot start.
  caused by: The Spanner partition planner for <database> was closed before it was used.
```

Nothing in the suite could see it. Every enumerator test built a fresh source, the two tests that
came closest either cloned the source through Java serialization first or expected both calls to
throw before an enumerator existed, and all three scripted doubles counted closes without going
sticky — so a shared seam and a fresh one behaved identically in test scope.

## Decision

A seam the enumerator *closes* is minted per enumerator; a seam with no teardown may stay on the
configuration. Closing is what makes reuse unsafe, so closing is what the rule keys on.

That distinction decides the two seams this ADR leaves where they are. BigQuery's `QueryRunner` is
also a configuration-held, serialized enumerator seam, and it is deliberately not `AutoCloseable`,
because the REST client it wraps has nothing to release; its own first-use guard already documents a
second enumerator over the same object. Spanner's `StructStreamOpener` is closeable but belongs to a
reader, and a TaskManager deserializes the source afresh for each task attempt, so its identical
one-way flag starts false every time.

The shape is the same in each connector.

- The configuration carries a `Serializable` factory with one no-arg `create()`, not the seam. This
  follows `SpannerChangeStreamCoordinatorClientFactory` and `SpannerDatabaseAccessFactory`, which
  already had it.
- The shipped implementation is a named class holding the values the seam's own final fields held,
  so what a job graph carries is unchanged. A lambda would not do: `docs/adr/0125` keeps
  connector-minted serializable lambdas out of the job graph.
- The source mints in both `createEnumerator` and `restoreEnumerator` and hands the instance to the
  enumerator, which keeps it in a typed field and uses it for its planning call rather than reading
  the configuration again.
- The seam interface drops `Serializable`. Nothing serializes it once the factory carries the
  parameters, and dropping it is what makes the defect hard to reintroduce rather than merely
  absent: a field of the interface type on a configuration fails to serialize the job graph. It is
  not proof against a concrete seam that opts back into `Serializable` on its own, which is why each
  connector's tripwire asserts the property of the *interface*, where the configuration's field type
  would have to be declared.
- Between `create()` returning and the enumerator's constructor taking ownership, the source is the
  only thing that can release the seam, so it closes it on any throw in that window.

Minting stays lazy in the sense that matters: a seam builds its client on first use, so a restore
whose checkpoint already records a plan mints a seam and opens no client.

## Alternatives declined

**Clear the flag when a new enumerator arrives.** The flag also guards a teardown that overtakes an
in-flight planning call. `BatchClientPartitionPlanner` opens its batch transaction outside the
monitor on purpose and re-checks the flag afterwards, and that re-check is the only thing that
releases a transaction the teardown could not see. Clearing a shared flag lets the old thread fall
through and write its transaction over the new enumerator's field, and `close()` releases only the
current field value, so one Spanner session leaks with nothing reporting it. A loud restart loop is
the better failure of the two. The window is real rather than theoretical: `resetToCheckpoint` builds
the replacement coordinator inside the old one's closing future, and that future completes normally
after `CLOSING_TIMEOUT_MS` as well as on completion, so a close that outlasts 60 seconds leaves the
old worker thread alive beside the new enumerator.

**Move the lifetime into `PullAssignmentSplitEnumerator`.** The base class can only mint from what it
is given, and the giver is the source, so this would still need a factory on each configuration —
the decision above, plus a base-class refactor. Each of the three subclasses also uses its seam for
its own planning call, so the base would have to hand it back out through a fourth type parameter and
an accessor. It would also build every connector lane on any change to it.

**Share the source's minting helper through a common `Source` base class.** The three private
`enumerator(context, checkpoint)` methods are about eight lines each and structurally alike, which
makes this the obvious next question. There is no shared `Source` base class today — the three
implement `Source` and `ResultTypeQueryable` directly — and the bodies differ on three axes: whether
credentials are pushed before the mint (Spanner and Bigtable yes, BigQuery no, because its creator
reads its own key path), the enumerator's constructor arity, and BigQuery's extra query-runner
argument. A base class plus a hook per axis is more machinery than the duplication it removes.

**Mint inline, with no factory type.** `PubSubStreamingPullSource` already does this: a private
method builds a fresh `SubscriptionAdmin` from plain configuration values, and the configuration
holds no seam. It is simpler, and it was declined for one reason. A test double still has to reach
the enumerator somehow, and if the configuration keeps a seam instance for that, the shared-instance
path survives in test scope — which is exactly the blindness that let this ship. A named factory
makes the test double per-enumerator too.

**An epoch or lease on the seam, or a `close()` that releases but reopens.** Both keep one client and
one transaction shared across enumerators, so both inherit every question the paragraph above asks.
A reopenable `close()` additionally deletes the post-open re-check's reason to exist, and contradicts
three interface contracts that already say the seam is closed once by one owner.

## Consequences

The streaming sources already minted per enumerator, so the rule is a restoration rather than an
invention and the three bounded sources were the deviation. They are not all equally clean, and the
difference is the reason this decision asks for a factory type rather than an inline mint.
`PubSubStreamingPullSource` builds a fresh `SubscriptionAdmin` from plain configuration values and
keeps no seam on the configuration at all. `SpannerChangeStreamSource` passes the factory to its
enumerator, which mints inside its own asynchronous start, so it has no hand-over window either.

`BigtableChangeStreamSource` was the partial case, and is brought onto the rule here rather than
left as an exception. It already minted a `DefaultChangeStreamCoordinatorClient` per enumerator when
the configuration's `@Nullable` override was absent, so it never had the restart loop — but
`ChangeStreamCoordinatorClient` was `Serializable`, a test-injected client was one instance shared by
every enumerator, and the inline mint had no release between `create()` and the constructor. That is
the test-scope blindness named above, in the one source that still had it, and leaving it would have
made this decision a rule with one exception on the day it landed. Reviewing that slice surfaced a
second defect in the same class, older than this decision and not caused by it, which is repaired
here rather than left behind. `DefaultChangeStreamCoordinatorClient`'s three lazy accessors were a
check-then-create against fields its `close()` nulls, and the enumerator's reconciliation scan runs
on the `callAsync` executor while `close()` runs on the coordinator thread. A teardown landing
between a check and its assignment closed nothing and left the client the scan then assigned owned
by no one — a leaked JobManager-side channel and executor, reaching Bigtable as the process's
application default credentials because `close()` nulls the credentials too. Making the fields
`volatile` does not fix a compound operation; the accessors are now guarded by the object's monitor
behind a one-way closed flag, which is what its three sibling seams already had. Per-enumerator
ownership is what makes that flag correct.

No checkpointed or savepointed state format changes. Each configuration's field type changed, but a
source configuration is never persisted: it is Java-serialized into a job graph that is rebuilt from
user code at every submission, while the enumerator state that a restore actually reads travels
through each connector's own `SimpleVersionedSerializer`. The incompatibility is confined to
deserializing a job graph written by a different connector build, which is not a path this project
supports. The three configurations are `@Internal` and every setter that takes a seam is
package-private, so nothing outside these modules can name the changed types either.

A new connector adding an enumerator seam decides one question: does the enumerator close it. If it
does, the configuration carries a factory. If it does not, the configuration may carry the seam, and
the seam's javadoc says why it has no teardown.

Each of the three scripted doubles now refuses after its own `close()`. Without that they cannot
tell a shared seam from a fresh one, and a regression test written against them would pass on the
defect it was written for.
