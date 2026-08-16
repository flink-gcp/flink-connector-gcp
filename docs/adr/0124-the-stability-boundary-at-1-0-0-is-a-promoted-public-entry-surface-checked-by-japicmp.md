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

# ADR-0124: The stability boundary at 1.0.0 is a promoted @Public entry surface checked by japicmp

- Status: Accepted
- Date: 2026-08-16 (measured 2026-08-16)
- Issues: [#728](https://github.com/flink-gcp/flink-connector-gcp/issues/728),
  [#39](https://github.com/flink-gcp/flink-connector-gcp/issues/39),
  [#29](https://github.com/flink-gcp/flink-connector-gcp/issues/29)
- Modules: all
- Current behavior: `docs/content/_index.md` § API reference

## Context

`flink-connector-parent` configures `japicmp-maven-plugin` in `pluginManagement` only, and its
default check covers `@org.apache.flink.annotation.Public` types alone.
Before this decision the six connector modules' main trees carried zero `@Public` types
(131 files `@PublicEvolving`, 429 `@Internal`, 2 `@Experimental`; test-utils adds 23 more
`@Internal` files but is japicmp-skipped), so wiring the plugin as inherited would have run,
passed, and policed nothing.
Issue [#728](https://github.com/flink-gcp/flink-connector-gcp/issues/728) therefore required a
decision on the stability boundary before the plumbing was worth writing.

## Decision

**A subset of the API is promoted to `@Public` at 1.0.0, and japicmp checks that subset on
every `verify` against the latest published release.**
The promoted set is computed, not curated by taste, from three rules:

- **Seeds**: the Sink/Source entry classes and their builders, and the types users implement
  or construct directly (serialization and deserialization schemas, `DestinationResolver`s,
  the base failure SPI, the provided serializer implementations, the CDC sequence-number
  providers).
- **Signature closure**: every project type appearing in a promoted type's public or protected
  member signatures is promoted too, so a frozen method cannot require an unfrozen parameter
  type.
- **Subtype closure**: a `@PublicEvolving` type extending or implementing a promoted type is
  promoted too, because users receive and `instanceof`-match the concrete subtypes (the
  per-connector `Failed*` types, the `DestinationResolution` results).

The census promoted 117 of the 131 `@PublicEvolving` types; a nested type (a `Builder`, a
nested enum) moves with its outer type.
The 14 that remain `@PublicEvolving` are the table-layer option and enum classes, whose
user-facing surface is the SQL option-key strings held bidirectionally to the sources by
`check-option-docs` rather than the Java binary API.
The closure deliberately stops at three groups, which stay below the frozen line: the two
`@Experimental` dead-letter-queue types, the `@Internal` enumerator-state and split types that
surface only through the generics of Flink's `Source` interface, and the `@Internal`
`RetrySchedule` returned by the options classes' `to*Schedule()` bridge methods.
One stop sits inside a frozen signature: `FailureHandler.sendToDeadLetterQueue` takes the
`@Experimental` `DeadLetterQueue`, so renaming that type would trip japicmp through the frozen
method even though its own tier promises nothing — its experimental latitude is narrower in
practice than the annotation reads.

**The tiers now promise:** `@Public` does not break within a major version; `@PublicEvolving` may
break at a minor release with a release-notes entry, and must not break at a patch release;
`@Experimental` and `@Internal` may change in any release.
The patch half of that promise has its own switch: cutting a patch release runs
`just verify -Pjapicmp-patch` against the release being patched, which re-declares the japicmp
includes with `@PublicEvolving` added.
Nothing in CI exercises that profile, so a patch release first re-proves it fires — check that
the emitted `tools/japicmp-output/<module>/japicmp/japicmp.xml` names `@PublicEvolving` types,
or re-run the one-line rename rehearsal below — before trusting its green.

**A deliberate break is an explicit artifact, not a silent pass.**
Breaking a `@Public` type requires an entry under `<excludes combine.children="append">` in the
root pom's japicmp declaration plus a release-notes entry, and under the semver policy it is a
major-release event — the frozen promise holds unconditionally within a major version.
The release that ships the break wipes the accumulated excludes and bumps
`japicmp.referenceVersion` to itself.

**Two release-procedure rules fall out of the mechanics** (for the
[#29](https://github.com/flink-gcp/flink-connector-gcp/issues/29) checklist):

- The release build whose own version equals `japicmp.referenceVersion` passes
  `-Djapicmp.skip=true`.
  Maven resolves the reference artifact from the reactor itself, and japicmp then reads the old
  side's whole compile classpath as the old API, reporting Flink's own `@Public` classes as
  thousands of removals.
- After publishing, the same change that moves the tree off the released version bumps
  `japicmp.referenceVersion` to the release and wipes the excludes.

## Evidence

The census is mechanical: `javap -protected` over the compiled main classes, a fixpoint over
signature references restricted to the `@PublicEvolving` candidate set, then the subtype pass.
Promoted per module: base 4, bigquery 46, pubsub 18, cloudtasks 12, bigtable 18, spanner 19
(117 total); the diff of the adopting pull request is the exact type list.
The wiring was rehearsed on 2026-08-16 against a locally staged 1.0.0 (installed into a
throwaway repository, never `~/.m2`), because nothing is published yet and
`ignoreNonResolvableArtifacts=true` would otherwise let the configuration look green without
ever firing:

| Arm | Tree | Command | Result |
| --- | --- | --- | --- |
| Unchanged | as merged | `verify` vs staged 1.0.0 | pass, all six jars compared |
| `@Public` break | builder method renamed | `verify` | fails, `METHOD_REMOVED` |
| `@PublicEvolving` break | option constant renamed | `verify` | passes (minor-release behavior) |
| `@PublicEvolving` break | option constant renamed | `verify -Pjapicmp-patch` | fails, `FIELD_REMOVED` |
| No reference artifact | as merged | `verify` (empty repository) | passes with a logged resolution warning |

All arms ran with tests skipped and the break arms module-scoped
(`-pl flink-connector-gcp-pubsub -am`); japicmp is independent of both, and the full suite ran
separately on the adopting pull request.

The staging build itself produced the self-resolution failure quoted in the Decision, which is
how the release-time skip rule was found.
Verification against the real Maven Central 1.0.0 remains open on
[#728](https://github.com/flink-gcp/flink-connector-gcp/issues/728) until that version exists.

## Alternatives declined

- **Inheriting the parent configuration unchanged**: with zero `@Public` types it checks
  nothing; the green result would be indistinguishable from a working gate.
- **Making `@PublicEvolving` the checked boundary instead of promoting**: every evolving-API
  adjustment between minors would then need an exclusion entry, and the docs' "supported
  surface" wording would collapse the distinction between frozen and evolving that Flink users
  already know from the annotations. Promotion states the freeze where readers look for it, on
  the class.
- **Promoting entry classes only, without the closures**: a frozen builder whose setter takes
  an unfrozen options type promises less than it appears to; the signature closure is what
  makes the freeze coherent.
- **Promoting all 131 types**: freezes the table-layer option classes, whose Java shape no SQL
  user links against, and removes the evolving tier the CDC surface still wants.

## Consequences

- 116 main-tree files swap `@PublicEvolving` for `@Public`; the remaining 15 keep their tier.
- The root pom gains `japicmp.referenceVersion` (1.0.0), the plugin declaration with the
  `oldVersion` groupId override (the parent hardcodes `org.apache.flink`, the same class of
  workaround as the `directory-maven-plugin` `rootDir` override), and the `japicmp-patch`
  profile. The check rides `verify`, so `just verify` and every CI lane run it with no recipe
  change.
- Until 1.0.0 exists on Maven Central the check passes with a warning; the rehearsal above is
  the evidence that it fires, and the last [#728](https://github.com/flink-gcp/flink-connector-gcp/issues/728)
  checkbox re-verifies against the published artifact.
- `ignoreNonResolvableArtifacts=true` converts *any* resolution failure into that warning, not
  only "not published yet" — and the CI Maven cache deliberately excludes
  `io/github/flink-gcp`, so after publication every lane re-downloads the reference jar and a
  Central outage or 429 silently degrades the check to a pass. Closing that (a cache carve-out
  for the immutable released version, or failing on the warning once 1.0.0 exists) is part of
  the post-publication verification on [#728](https://github.com/flink-gcp/flink-connector-gcp/issues/728).
- The parent's `fast` profile does not skip this plugin (it targets the stale
  `io.github.zentol.japicmp` fork coordinates); the working switch is `-Djapicmp.skip=true`.
