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

# ADR-0054: Flink 1.20 is supported from the same source tree, at source level

- Status: Accepted
- Date: 2026-08-01 ([#32], reversing the branch plan the issue was opened for — measured, not
  assumed)
- Issues: [#32], [#29], [#39] (the `X.Y.Z-1.20` publishing suffix decided there)
- Modules: all connectors
- Current behavior: `docs/content/_index.md` supported-versions table, `README.md` § Build

## Context

[#32] was opened to plan a Flink 1.20 (1.x LTS) support **branch** and its backport process —
the shape most Flink connector ecosystems assume. Before committing to a second branch to
maintain, the actual API delta between 1.20 and 2.x for the surface these connectors touch was
measured.

## Decision

**Flink 1.20 is supported from this same source tree, not from a branch or per-version
modules.** The whole measured delta is two items:

- **1.20 still declares the deprecated `createWriter(Sink.InitContext)` abstract while 2.x
  removed the type.** Absorbed by the one-interface `CrossVersionSink` seam under
  `src/main/java-flink1` / `java-flink2`, selected by the `flink.compat` Maven property
  (default `flink2`; `just verify-flink 1.20.x` adds `-Dflink.compat=flink1` itself). The two
  variants share one FQCN on purpose, so every sink implements the same name whichever major
  it compiles against.
- **`CommittableMessage.getCheckpointId()` changed its return type across the majors.** Dodged
  by calling `getCheckpointIdOrEOI()`, present in both and deprecated on 2.x; if a 2.x minor
  removes it, that call is the line to revisit.

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

## Alternatives declined

- **A support branch with backports** — the plan [#32] was opened for. Reversed on the
  measurement above: a two-item delta does not justify a second branch's standing merge cost.
- **A Dataproc-style per-version module split** — buys isolation the two ~15-line interface
  variants already provide.

[#29]: https://github.com/laughingman7743/flink-connector-gcp/issues/29
[#32]: https://github.com/laughingman7743/flink-connector-gcp/issues/32
[#39]: https://github.com/laughingman7743/flink-connector-gcp/issues/39
