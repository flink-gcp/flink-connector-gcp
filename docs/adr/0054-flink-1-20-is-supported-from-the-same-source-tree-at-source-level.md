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

# ADR-0054: Flink 1.20 is supported from the same source tree, at source level

- Status: Accepted
- Date: 2026-08-01 ([#32], reversing the branch plan the issue was opened for — measured, not
  assumed); revised by [#404] (2026-08-09)
- Issues: [#32], [#29], [#39] (the `X.Y.Z-1.20` publishing suffix decided there), [#404]
- Modules: all connectors
- Current behavior: `docs/content/_index.md` supported-versions table, `README.md`
  § Supported versions

## Context

[#32] was opened to plan a Flink 1.20 (1.x LTS) support **branch** and its backport process,
the plan the root `AGENTS.md` had recorded as this repository's intention. Before committing
to a second branch to maintain, the actual API delta between 1.20 and 2.x for the surface
these connectors touch was measured.

## Decision

**Flink 1.20 is supported from this same source tree, not from a branch or per-version
modules.** The whole measured delta is two items:

- **1.20 still declares the deprecated `createWriter(Sink.InitContext)` abstract while 2.x
  removed the type.** Absorbed by the one-interface `CrossVersionSink` seam under
  `src/main/java-flink1` / `java-flink2`, selected by the `flink.compat` Maven property
  (default `flink2`; `just verify-flink 1.20.x` adds `-Dflink.compat=flink1` itself). The two
  variants share one FQCN on purpose, so every sink implements the same name whichever major
  it compiles against.
- **`CommittableMessage`'s checkpoint-id accessors swapped roles across the majors.** Absorbed
  by the same seam: `CrossVersionCheckpointId` in each compat root, one static `of` reading a
  `CommittableWithLineage`, called by BigQuery's `FileLoadsCheckpointStamper` ([#404]).

**Both deltas live in the seam, and neither leaves a deprecated call in shared source.** The
seam is therefore not one interface per module but a small set of same-FQCN files, and not
every one of them is compile-only — `CrossVersionCheckpointId.of` runs for every lineage-carrying
message the FILE_LOADS pre-commit stage maps. A compat-root file also need not sit at a module's `sink`
root: this one is package-private next to its only caller, which ADR-0055's skeleton allows for
an implementation type.

**This is source-level support.** The weekly `lts` row compiles and tests everything at
`FLINK_LTS`, a jar is compiled per major, and no cross-major binary claim is made — the
one-artifact claim of ADR-0053 spans the 2.x range only. A red `lts` row reproduces locally
with `just verify-flink <FLINK_LTS>`, the same first-move rule `binary_compat` has. A 1.20
patch bump is a hand edit to `FLINK_LTS` in `weekly.yaml`; dependabot does not see workflow
env.

## Evidence

The 1.20 bridge default is compile-only: 1.20's runtime always creates writers through
`createWriter(WriterInitContext)`, measured by the whole suite running green on 1.20.4 with
the bridge method throwing.

The checkpoint-id accessors, read off the shipped artifacts on 2026-08-09 — `javap -v` on
`flink-streaming-java:1.20.4` and on `flink-runtime` 2.3.0 and 2.4-SNAPSHOT (`-v`, because the
deprecation state is a method attribute the default output omits), and the sources jar of
`flink-runtime:2.2.1`, which the type moved to on 2.x:

| | `getCheckpointId()` | `getCheckpointIdOrEOI()` |
|---|---|---|
| 1.20.4 | `OptionalLong`, `@Deprecated`, default | `long`, `@Deprecated`, **abstract** |
| 2.2.1 / 2.3.0 / 2.4-SNAPSHOT | `long`, not deprecated, abstract | `long`, **`@Deprecated(forRemoval = true)`**, default |

1.20's `CommittableWithLineage` does not declare `getCheckpointId()` at all — it inherits the
`OptionalLong` default — so shared source has no spelling that is both non-deprecated on 2.x
and `long` on 1.20.

**Splitting the call changed no value on either major**, which is what makes it a guardrail
rather than a behaviour change: on 2.x `getCheckpointIdOrEOI()`'s entire body is `return
getCheckpointId();`, and on 1.20 it is the method `CommittableWithLineage` implements, so each
root calls what the one shared line called. Calling the 2.x-deprecated one from shared source
was the original choice;
it made a scheduled removal into a compile break that would land on the day the supported range
moves, which is a deliberate act rather than a good time to discover an unrelated fix. Flink
has not executed the removal — the sibling `EOI` constant's javadoc names Flink 2.2 as its
target and the method is still present in 2.4-SNAPSHOT — so nothing was broken when this was
changed.

**Whether a `forRemoval` call has come back is a question javac already answers**, which is why
there is no checker for it:

```sh
MAVEN_OPTS="-Duser.language=en -Duser.country=US" \
  ./mvnw -ntp clean test-compile -Dmaven.compiler.showDeprecation=true 2>&1 \
  | grep 'marked for removal'
```

The locale pin is load-bearing rather than tidiness: javac localises the message, so on a
Japanese JVM the same call site reads `削除用にマークされています` and an English `grep` finds
nothing — a silent false negative that looks exactly like a clean tree. Keep an eye on the other
`has been deprecated` lines as the control: if they vanish too, the probe stopped firing rather
than the tree becoming clean. Measured 2026-08-09 on the default (2.x) lane: two hits before this
change (`FileLoadsCheckpointStamper` and its test, both `getCheckpointIdOrEOI()`), none after,
with four unrelated `has been deprecated` lines still reported either way. The `java-flink1`
call is outside this probe's reach twice over — it is not compiled on the 2.x lane, and it
carries `@SuppressWarnings("deprecation")` — which is the intent, not a gap: 1.20 is frozen.

## Alternatives declined

- **A support branch with backports** — the plan [#32] was opened for. Reversed on the
  measurement above: a two-item delta does not justify a second branch's standing merge cost.
- **A Dataproc-style per-version module split** — buys isolation the handful of same-FQCN
  compat files, none of them longer than an interface stub, already provides.

[#29]: https://github.com/flink-gcp/flink-connector-gcp/issues/29
[#32]: https://github.com/flink-gcp/flink-connector-gcp/issues/32
[#39]: https://github.com/flink-gcp/flink-connector-gcp/issues/39
[#404]: https://github.com/flink-gcp/flink-connector-gcp/issues/404
