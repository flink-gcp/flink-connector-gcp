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

# ADR-0131: Spanner's two batch seams keep their own lazy handle lifecycle

- Status: Accepted
- Date: 2026-08-22 (measured 2026-08-22 at `7e1a13c4`)
- Issues: [#991](https://github.com/flink-gcp/flink-connector-gcp/issues/991)
- Modules: spanner
- Current behavior: unchanged; each batch seam owns its own `Spanner` handle and closes it

## Context

The bounded Spanner source reaches the service through two seams. `BatchClientPartitionPlanner`
plans the read on the coordinator, and `BatchClientStructStreamOpener` opens each partition's rows
on a reader. Each builds a `Spanner` handle on first use under its own monitor, holds the
credentials its owner pushed in, and refuses to build another once closed.

Bigtable folded the equivalent pair — `DataClientRowKeySampler` and `DataClientRowStreamOpener` —
into `LazyBigtableDataClient` in [#956](https://github.com/flink-gcp/flink-connector-gcp/pull/956),
and [#979](https://github.com/flink-gcp/flink-connector-gcp/issues/979) named that as the precedent
for doing the same here. The extraction was deliberately left out of
[#986](https://github.com/flink-gcp/flink-connector-gcp/pull/986) so it would not mix with a
credential decision, and #991 was filed to answer it on its own, with the instruction to measure
before consolidating.

Two things about the premise changed before the measurement was taken.

ADR-0128 minted the planner per enumerator and dropped `Serializable` from `PartitionPlanner`, so
the planner's fields lost their `transient` markers while the opener, still deserialized per task
attempt from the source configuration, kept its own. That looked at first like a new argument
against a holder — the two classes now sit on opposite sides of ADR-0128's line. It is not one.
The same change kept `LazyBigtableDataClient` spanning exactly that line, documenting its
`transient` marker as inert for the enumerator-side owner rather than splitting the holder in two.
A holder that spans the line is a shape this repository has already accepted.

What the measurement found is that the duplication is smaller than reported and the obstacle is
somewhere else. `SpannerClients.open` and `SpannerClients.settings` already factor out the
emulator-versus-credentials branch, reached from six classes. What remains identical between the
two seams is about eight lines: `useCredentials`, `settings()`, and the `database` and
`emulatorEndpoint` field declarations. The original count of 25 to 30 included the field javadoc
and the shape of the build-on-first-use block rather than shared code.

## Decision

The two batch seams keep their own copy of the lazy handle lifecycle. A `LazyBigtableDataClient`-
style holder is declined.

The reason is the planner's `closed` flag, which guards three things:

1. building the `Spanner` handle, inside `open()`'s first monitor block;
2. `databaseClient()`, which refuses to hand out a client after teardown;
3. the re-check `open()` makes after opening the batch transaction outside the monitor.

A holder absorbs the first two. `LazyBigtableDataClient.get` already refuses after its own
teardown, so a Spanner equivalent would take that check with it. The cost is small and worth
naming: a holder's accessor builds what it does not find, so `databaseClient()` would lose its
separate "did not open its service handle" failure. That failure is defensive rather than
reachable — `plan()` calls `open()` first — but it is a guard the extraction spends.

The third is what no holder can absorb. `open()` begins the transaction on the service without
holding the monitor, deliberately, so that a job being torn down does not wait on a round trip.
It then re-takes the monitor to read `closed` and write `transaction`, and `close()` takes the same
monitor to set `closed` and read `transaction`. That pairing is the whole guarantee: either
`close()` observes the transaction and releases it, or the re-check observes `closed` and releases
it before throwing. Nothing else releases a transaction the teardown could not see.

Move `closed` behind a holder's own monitor and the pairing dissolves, because the flag and the
field are then guarded by different locks:

```text
planner thread    re-check reads holder.isClosed() -> false
scheduler thread  close(): reads transaction (null), closes the holder, returns
planner thread    writes transaction = opened
```

One Spanner session outlives the enumerator, and nothing counts or logs it. ADR-0128 already
declined the flag-clearing repair for this same leak; a holder reaches it by a different route.

Bigtable has no counterpart, which is why the holder pays there. Each of its two seams holds the
holder and nothing else, and each `close()` is one delegating line. Neither owns a transaction, and
neither re-checks anything after an unguarded call.

## Alternatives declined

**Extract the holder and let the planner keep a second `closed`.** This is the only form that both
shares the client and preserves the interlock, and it does not pay. The planner sheds about nine
lines — two field declarations, two method bodies, the construction call, and `databaseClient()`'s
handle check — and gains about four: the holder field, two delegating methods, and the changed call
site. What it leaves behind is two
closed flags where there was one, on an object whose teardown ordering is already the subtle part.
Consolidation that trades one flag for two has removed a copy and added a hazard.

**Extract the holder for the opener alone.** The opener is where a holder genuinely pays: its
client-lifecycle half, about 60 of its 195 lines, would collapse to a field and three delegating
methods. But a holder with one user is a rename, and it would leave the file that motivated the
question — the planner — untouched.

**Widen the holder to the module's other `Spanner` owners.** It does not reach them.
`SpannerDatabaseRowLookup` builds in `open()`, its `if (client == null)` being an unopened-guard
that throws rather than a lazy build; `DefaultSpannerDatabaseAccessFactory` and both Change Streams
client factories build in `create()`. None caches under a monitor, so a holder aimed at the lazy
shape has a candidate population of exactly two.

## Consequences

The duplication stays, and it is now named rather than latent: the module reference carries the
count and the reason, so a future reader meets the `closed`-flag argument instead of re-deriving
it or re-filing the question.

Bigtable's holder is not a precedent for Spanner. The difference is not serialization, which is
what the question was originally framed around, but whether a seam owns a second lifecycle on top
of its client. Any connector weighing the same extraction should ask that question rather than
counting lines.

Re-propose the holder with an answer for the post-open re-check, superseding this record. A smaller
line count is not an answer, and neither is a holder that keeps the interlock by exposing its
monitor to the planner: that couples the planner's transaction bookkeeping to the client holder's
lock, which no contract on either side states.
