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

# ADR-0123: A Pub/Sub file keeps its upstream notice while it still carries upstream expression

- Status: Accepted
- Date: 2026-08-16 (measured 2026-08-16)
- Issues: [#755], [#17], [#31]
- Modules: pubsub
- Current behavior: `flink-connector-gcp-pubsub/README.md` § Provenance and attribution

## Context

The Pub/Sub sink was adapted from the Flink connector in [GoogleCloudPlatform/pubsub][upstream]
([#17]) and the source reader core was vendored from the same place ([#31]). Thirteen main-tree
files carried `Copyright 2023 Google LLC`, and each carried an "Adapted from …" javadoc paragraph
enumerating how it had diverged.

The implementations were then rewritten incrementally by a year's worth of issues, and [#755]
redesigned the decomposition on top of that. Nobody checked, at any point, whether a given file
still contained anything of the original — the header simply stayed because it had always been
there. That is the question this record settles, and the reason it needed settling is that the
first published artifact will carry these files: Apache-2.0 §4(c) scopes notice retention to "the
Source form", so what reaches a user is the header in the sources jar rather than anything in the
binary, and a published version cannot be withdrawn. (That jar comes from the parent's
`docs-and-source` profile, which the publishing pipeline still has to activate — nothing in this
repository passes it today.)

## Decision

**A file keeps the upstream notice unless nothing upstream-specific survives in it at all.** Four
files lose it; nine keep it. (One of the nine was deferred until [#755]'s rewrite re-measured
it — see Evidence — and is settled as kept.)

The test is **residue, not percentage.** §4(c) requires retaining notices "excluding those notices
that do not pertain to any part of the Derivative Works", so what retires a notice is the absence
of the original — never the observation that a lot has changed. [#755] pre-registered the same rule
from the other side, and ADR-0122 restates it: a restructuring does not move an attribution.

**Where the audit left room for judgement, the notice stays.** That is the deliberate bias of this
record, and it decided four files. `AckTracker`, `NotifyingPullSubscriber` (named
`PullSubscriber` since [#765]), `PubSubSerializationSchema` and `PubSubDeserializationSchema`
retain exact upstream declarations —
`void shutdown();`, both `dataOnly(...)` signatures, `void addCheckpoint(long checkpointId);` — and
an argument that a method name and an empty parameter list carry no authorship would very probably
be right. **The project does not need to be right about that**, so it does not assert it: they keep
their notice, and no protectability question is reached.

What is retired is only where the coincidence is unmistakably generic: `@Override`, `try {`,
`@VisibleForTesting`, a field assignment in a constructor, and declarations Flink's own SPI
dictates (`splitId()`, `emitRecord(...)`, the `SplitReader` contract). Two independent
implementations against the same framework produce those whether or not either has seen the other.

**"Nothing upstream-specific" is not the same as "no line in common", and cannot be.** The class
names are themselves upstream coinages, so a rule demanding zero coincidence would retire no notice
at all — which is a coherent position, and the one this record would fall back to if the line below
were ever found to be in the wrong place.

**The §4(b) notice goes with the §4(c) one, and only with it.** The "Adapted from …" javadoc is
this repository's statement that it changed the file. §4's conditions govern distributing "the Work
or Derivative Works thereof", so a file holding no part of the Work is neither a modified file
under §4(b) nor a bearer of a notice pertaining to a Derivative Work under §4(c). Where the
paragraph also carried design rationale, that rationale is kept as ordinary javadoc, stated
directly rather than as a delta from something the reader cannot see.

## Evidence

Measured 2026-08-16 against upstream `e484b26` (2026-06-25). Upstream's `flink-connector/` still
exists at the same path and its last main-source change predates our adaptation, so `HEAD` is the
right comparison. Method: normalise both sides (drop the licence header, `package`/`import`, blank
and brace-only lines, comments), take exact common lines, then read every upstream file in full to
catch renamed or reflowed copies a line match misses.

**Notice retired — four files.** Everything they still share with upstream is either generic Java
or a declaration Flink's SPI dictates:

| File | Common lines | What they are |
|---|---|---|
| `PubSubWriter` | 16 of 512 | six `@Override`, five `try {`, four `@VisibleForTesting`, and one `this.publisher = publisher;` that in this file sits in an inner class upstream does not have. Upstream's `PubSubPublisherCache` — a JVM-wide static map with a shutdown hook — has no counterpart here |
| `PubSubSplitReader` | 17 of 236 | the `SplitReader` contract, its prescribed `RecordsBySplits.Builder`, `@Override` and `try {` |
| `PubSubRecordEmitter` | 12 of 101 | field and constructor boilerplate, and Flink's `RecordEmitter.emitRecord` signature |
| `SubscriptionSplit` | 5 of 30 | four `@Override` and `splitId()`, which is `SourceSplit`'s |

**Notice kept — nine files.** Four hold upstream text outright:

| File | What remains |
|---|---|
| `PubSubAckTracker` | `addCheckpoint`'s complete two-statement body; the `while (!checkpoints.isEmpty() && checkpoints.firstKey() <= checkpointId)` sweep; the staged and checkpoint field declarations, `SortedMap`/`TreeMap` included |
| `PubSubSourceReader` | `ackTracker.addCheckpoint(checkpointId);` and `ackTracker.notifyCheckpointComplete(checkpointId);` — upstream's entire method bodies |
| `DataOnlySerializationSchema` | the `open` delegation, and `PubsubMessage.newBuilder().setData(ByteString.copyFrom(…)).build()` |
| `DataOnlyDeserializationSchema` | the `open` and `getProducedType` delegations — two of its three bodies byte-identical |

Four more keep it because an exact upstream-specific *declaration* survives, and the record
declines to argue that a declaration carries no authorship:

| File | What remains |
|---|---|
| `AckTracker` | the interface declaration and `void addCheckpoint(long checkpointId);` verbatim, plus four of upstream's five method names in its order |
| `NotifyingPullSubscriber` (now `PullSubscriber`) | `void shutdown();` verbatim; upstream's other three methods are gone |
| `PubSubSerializationSchema` | the interface declaration and the `dataOnly(...)` factory signature |
| `PubSubDeserializationSchema` | the `dataOnly(...)` factory signature |

**Deferred — one file, since resolved as kept.** `PubSubNotifyingPullSubscriber` read as clear by
line count (27 of 193, mostly SDK idiom) but declares its message buffer exactly as upstream does,
annotation included:
`@GuardedBy("this") private final Deque<PubsubMessage> messages = new ArrayDeque<>();`, and
`receiveMessage` keeps upstream's statement sequence though every statement differs. [#755]'s
rewrite renamed it `StreamingPullSubscriber` and split its teardown out, and the re-measurement
that rewrite owed found **both residues untouched** — the teardown had no upstream counterpart, so
extracting it moved nothing upstream-derived. The notice stays, which makes the count nine kept,
four retired, none deferred.

**Nothing outside those files derives from upstream.** All 94 Java files under this module's
`src/test` were diffed against the 14 in upstream's corresponding module (18 across upstream's
whole `flink-connector/`, the rest belonging to its e2e-test module), and every hit is a call site
of this project's own API. Our documentation page shares no line over twenty characters with
upstream's same-path page, and we have no proto where upstream has `split.proto`.

**Upstream ships no `NOTICE` file** — only `LICENSE`. §4(d) is conditional on the Work having one,
so it never engaged; the repository's own `NOTICE` paragraph is a record this project wrote for
itself, and it stays because four files still contain adapted code.

## Alternatives declined

- **Driving the count to zero.** It would have required determining that a single-statement
  delegation and a method call are not protectable expression. That may well be right, and it is
  not needed: keeping the notice where the text is present costs a header line and no argument.
- **Rewriting `PubSubAckTracker`'s sweep to shed its residue.** The only residue of the four that
  *could* be expressed differently — and every alternative to a `TreeMap` swept at-or-below is a
  worse fit for "acknowledge everything up to checkpoint N". Rewriting working code to retire a
  notice is the inverted reasoning [#755] warned against by name.
- **Removing the notice from all thirteen and relying on the module-level record.** §4(c) is a
  per-file obligation for the Source form; a `NOTICE` paragraph does not discharge it.
- **Deleting the `NOTICE` paragraph and the README provenance section.** Both remain true, and the
  history is worth keeping where a reader looks for it.

## Consequences

- `PRESERVED_HOLDERS` in `scripts/check-license-headers.py` keeps `("Google LLC",)`, and the
  checker keeps doing what its comment says: stopping an unattributed third-party header from
  passing as an ordinary one. The deferred file was re-measured and kept, so the list is settled at nine.
- The README's provenance section now names the nine files that still carry the notice rather
  than all thirteen, and restates one entry that
  described the wrong thing. It said `PubSubWriter` retains upstream's "publish/flush core (async
  publish, `publishAllOutstanding`-then-await checkpoint flush)", which conflates a design with
  code. The *design* is retained: `flush()` asks each destination's publisher for what is still
  batched and then drains. The *code* is not — `publishAllOutstanding()` is no longer called from
  the writer at all but from behind the `TopicPublisher` seam, and the await is a counter drained
  against a progress budget through the mailbox rather than upstream's
  `ApiFutures.allAsList(outstandingPublishes).get()`. A per-file deviation list that cannot make
  that distinction is a second reason those paragraphs go with the notices.
- **Adding upstream code back to a cleared file means restoring its notice.** The four are not
  permanently exempt; they are files that, as measured on this date, carry nothing to attribute.
- Two identifiers-level points, recorded as considered rather than left to be raised: upstream
  coined `AckTracker`, `addPendingAck`, `stagePendingAck`, `dataOnly` and the `PubSub*Schema`
  names, which survive; and what [#755] describes as inherited is the *class decomposition*, a
  selection-and-arrangement claim rather than a line-level one, which the redesign in that issue
  replaces. Both point the same way — a file cleared here only gets further from upstream.

[#17]: https://github.com/flink-gcp/flink-connector-gcp/issues/17
[#31]: https://github.com/flink-gcp/flink-connector-gcp/issues/31
[#755]: https://github.com/flink-gcp/flink-connector-gcp/issues/755
[#765]: https://github.com/flink-gcp/flink-connector-gcp/pull/765
[upstream]: https://github.com/GoogleCloudPlatform/pubsub
