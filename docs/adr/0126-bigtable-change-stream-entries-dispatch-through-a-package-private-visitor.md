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

# ADR-0126: Bigtable change-stream entries dispatch through a package-private visitor

- Status: Accepted
- Date: 2026-08-22 (measured 2026-08-22)
- Issues: [#985](https://github.com/flink-gcp/flink-connector-gcp/issues/985),
  [#915](https://github.com/flink-gcp/flink-connector-gcp/issues/915)
- Modules: bigtable
- Current behavior: `docs/content/docs/connectors/datastream/bigtable.md` § Change Streams source

## Context

`BigtableChangeStreamMutation.Entry` has five subtypes and `Value` has three, and three classes
branched over them with `instanceof` chains: the serializer's write half, the table envelope
converter, and the selected-cell classifier.
Adding a subtype meant finding all three by hand.
Every chain ended in a throw, so this was maintenance surface rather than a correctness defect.

[#915](https://github.com/flink-gcp/flink-connector-gcp/issues/915) filed it as one of three
hand-written case sets that should derive from what defines them.
[#985](https://github.com/flink-gcp/flink-connector-gcp/issues/985) carried it forward with
measurements arguing against acting, and two of those arguments assumed a shape this decision does
not use — see *Alternatives declined*.

Measured at `a40adc6d`: 51 `instanceof` checks across the four dispatching classes, of which **27**
are in `BigtableChangeStreamMutationConverter` over the *client's* `Entry`/`Value`, where the
subtypes do not even name their accessors alike (`getFamilyName()` against `getFamily()`).
No consolidation reaches those 27.

## Decision

**The connector's own entry and value hierarchies dispatch through visitors whose `accept` methods
are package-private, and callers outside the connector branch on a discriminator instead.**

- `Entry` and `Value` each declare `abstract <R, A> R accept(Visitor<R, A>, A)` with package-private
  access. `ChangeStreamMutationDispatcher` — `@Internal` — declares the two visitor contracts and
  the `dispatchEntry`/`dispatchValue` entry points the connector's other packages use.
- **Adding a subtype adds a visitor method, and every handler then fails to compile.** Measured by
  adding one probe method: all three handlers reported *"is not abstract and does not override
  abstract method visit(...)"*. A `mvn compile` without `clean` reported success on the same probe,
  so the guarantee is a clean build's, which is what CI runs.
- **The visitor does not reach the `@Public` surface; `getKind()` does.** Measured rather than
  reasoned: with this branch's parent commit installed as `1.0.0`, japicmp reports **no** difference
  for `accept` or for `ChangeStreamMutationDispatcher` — `accessModifier` is `public`, so a
  package-private method is not compared, and the excludes name `@Internal` — and fails on exactly
  one thing, `BigtableChangeStreamMutation$Entry.getKind():METHOD_ABSTRACT_ADDED_TO_CLASS`.
  That one run establishes both halves of this decision. The visitor is outside the frozen set
  before 1.0.0 and after it, so a subtype added later breaks the build while staying japicmp-clean —
  the combination this decision wanted. And a public abstract method added to a `@Public` type is a
  break japicmp stops, which is what makes the *concrete*-`accept` alternative below not a
  substitute, and why `getKind()` is worth the same run's cost only while 1.0.0 is unpublished.
  The [#783](https://github.com/flink-gcp/flink-connector-gcp/issues/783) re-tier
  ([ADR-0141](0141-a-surfaces-stability-tier-is-set-by-what-can-reshape-its-inputs-and-outputs.md))
  later moved `BigtableChangeStreamMutation` to `@PublicEvolving`, so the default gate no longer
  compares it and a public abstract addition is stopped only by `-Pjapicmp-patch`, costing a
  minor-release note rather than a major-release exclusion; the measurement above was taken while
  the type was `@Public` and stands as recorded, and the visitor's compile-time break at every
  implementor is tier-independent.
- **`Entry` gains `getKind()` and an `EntryKind` enum**, pairing with the `getType()`/`ValueType`
  that `Value` already had. The visitor is not reachable from outside the connector, and users
  receive entries rather than construct them, so without a discriminator their only branch would be
  `instanceof`. A test pairs each enum constant with exactly one subtype in both directions, since
  the compiler holds the visitor but not the enum. **This half had a 1.0.0 deadline**, which
  the visitor half does not: it is the abstract public method the japicmp run above stopped, so
  adding it after 1.0.0 would have cost more than nothing — since the
  [#783](https://github.com/flink-gcp/flink-connector-gcp/issues/783) re-tier, a
  minor-release-notes entry under `-Pjapicmp-patch` rather than the major-release exclusion the
  `@Public` tier would have demanded. Adding a
  constant later stays clean — the run reported the `EntryKind` enum itself, and each of its
  constants, as compatible additions.
- **Two things stay as they were.** The serializer's `readEntry`, `readValue`, `copyEntry` and
  `copyValue` remain `switch`es over a wire tag: they decode a byte, which is not a Java type yet.
  And `BigtableChangeStreamMutationConverter` keeps its 27 `instanceof` checks over the SDK types.
  Both are places an added subtype still has to be given by hand — as is the `tag < SET_CELL || tag
  > MERGE_TO_CELL` bound the copy path guards with, which refuses a sixth tag until it is widened.
  These refusals are loud, but they arrive in a running job rather than in a build, so the visitor
  narrows the hand-editing rather than removing it.

This is the second use of the shape ADR-0114 introduced for `DestinationResolution`. The two differ
in who supplies the values: BigQuery's hierarchy is what a *user's* resolver returns, so the
package-private constructor is what keeps unsupported variants away from the writers, whereas an
`Entry` is only ever built by this connector and the visitor is there for the compile-time
exhaustiveness alone.

## Alternatives declined

- **Leave the `instanceof` chains and add a tripwire test** pinning the subtype sets. Cheaper, and
  it does catch an added subtype, but only when the suite runs, and it points at the sites rather
  than requiring them to be updated. The compiler does both.
- **Switch over the discriminators instead.** Java 11 statement switches have no exhaustiveness
  check and need a `default`, so this is the `instanceof` chain's safety with different syntax.
  `Value.getType()` had existed unused since the hierarchy was written, which #985 read as evidence
  that the cheap fix was not wanted; the reading this decision takes instead is that the cheap fix
  did not buy anything.
- **A public `accept`, or a concrete one with a default body.** #985 priced the visitor as a
  `@Public` method and concluded it could be taken after 1.0.0 at the same cost. That is true only
  of a *concrete* `accept`, and a concrete one does not fail the build when a subtype forgets to
  override it — which is the entire benefit. An abstract public one would be a break japicmp stops:
  the run above did stop `getKind()`, an abstract public method on the same class, so this is
  measured rather than predicted. Since the
  [#783](https://github.com/flink-gcp/flink-connector-gcp/issues/783) re-tier it would need a
  minor-release-notes entry (the class is `@PublicEvolving`; only `-Pjapicmp-patch` compares it)
  rather than a major-release exclusion. The
  package-private form avoids the trade rather than resolving it.
- **Bind the envelope's `kind` strings to `EntryKind`.** Those strings are the SQL-visible output of
  the change-stream envelope; deriving them from the enum would let renaming a constant change what
  a query returns. The visitor already makes it impossible to add a subtype without naming its
  string.
- **Give `SelectedCellMutationClassifier` a per-entry verdict the loop folds.** #985 judged the
  classifier no visitor fit because its arms fold mutable state and throw mid-arm. Holding that
  state in the visitor instead — one instance per mutation — keeps each arm able to throw where it
  detects the violation, and preserves the two differently worded failures that a second delete
  raises depending on which entry type carried it.

**The serialized form does not move.** The tags, their order and every field behind them are what
they were; only the code that chooses among them changed. A checkpoint written before this change
restores after it, which is the property `BigtableChangeStreamMutationSerializerSnapshotTest` and
`BigtableChangeStreamSourceRescalingITCase` hold and the reason the read and copy paths were left
alone rather than rewritten alongside the write path.

## Consequences

Adding a subtype now costs an `EntryKind`/`ValueType` constant, a visitor method, and an arm in each
of the three handlers, none of which can be skipped silently.

The SDK-side converter is not reached by the visitor, but it is no longer only found by hand either.
`BigtableChangeStreamSdkEntrySurfaceTest` pins the two things the client would have to change to
grow an entry kind: `Value.ValueType`, which is an enum, and the method set of
`ChangeStreamRecordAdapter.ChangeStreamRecordBuilder`, which is how a `ChangeStreamMutation` is
assembled and therefore names every entry kind the client can build. That makes the marker-interface
problem — `Entry` has no methods and its implementations cannot be enumerated without scanning the
classpath — go away without a classpath scan, and it is the reader's half of the bump
`BigtableWriterMutationCaseTest` already covers for the sink. Measured: the builder carries eleven
methods at 2.20.1 and thirteen at 2.45.1, the additions being `addToCell` and `mergeToCell`, so this
assertion would have fired on the one growth that has actually happened.

One allocation per mutation appears in the classifier, which previously folded into local variables.
The serializer and the envelope converter allocate nothing new: their handlers are stateless
singletons and the state they need travels as the `accept` argument. That is why `accept` takes an
argument at all — the serializer's `duplicate()` returns `this`, so task threads share one instance
and a handler cannot hold mutable fields.
