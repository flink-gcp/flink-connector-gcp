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

# ADR-0143: The tier-annotated surface carries Javadoc, held by the link checker

- Status: Accepted
- Date: 2026-08-23
- Issues: [#730], [#1093]
- Modules: all
- Current behavior: `just check-javadoc-links` fails on a public or protected member of a
  `@Public`/`@PublicEvolving`/`@Experimental` type that has no Javadoc, and on a `ConfigOption`
  constant whose Javadoc differs from its `withDescription` text

## Context

The published Javadoc becomes the API reference at release, and nothing in the build detected
Javadoc that is *absent*. `maven-javadoc-plugin` runs with `failOnWarnings=true`, which catches a
reference the reader cannot follow — [ADR-0056]'s deliberate line — but not a member with no
comment at all. The configured checkstyle checks (`JavadocMethod`, `JavadocType`,
`JavadocParagraph`, `JavadocStyle`) validate the shape of Javadoc that exists; in checkstyle
10.18.2 the missing-Javadoc behavior lives in the separate `MissingJavadocMethod` /
`MissingJavadocType` checks, and neither was configured.

The [#730] audit measured the consequence. The gap was not drift: [#1082]'s re-tiering promoted
the change-stream surfaces from `@Internal` to `@PublicEvolving` and thereby carried 72
undocumented members and 3 bare nested enums into the public tiers overnight, while the `@Public`
surface measured 558/558 documented methods, constructors, and fields — `GcRule.Kind`'s four bare
enum constants were the one `@Public` exception, and this change documents them. Re-tiering is
the event class that creates this gap, and
[ADR-0141] makes re-tiering a normal, recurring act — so a one-time audit decays the next time a
tier moves.

The audit's fix series also created a second unguarded surface: [#1100] gave all 240
`ConfigOption` constants a Javadoc equal to their `withDescription` text, so an IDE hover shows
the same contract the option's runtime description carries. Nothing held the two copies together —
`OptionDescriptionAssertions` reads the runtime description, never the source Javadoc.

## Decision

`scripts/check-javadoc-links.py` — the checker that already parses every main-source Javadoc —
gains two rule families, run by the same `just check-javadoc-links` recipe and the existing
`javadoc_links` CI job:

1. **Presence.** Every type whose nearest annotated enclosing declaration carries `@Public`,
   `@PublicEvolving`, or `@Experimental` needs type-level Javadoc, and so does every public or
   protected member it declares — methods, constructors, fields, nested types, enum constants,
   and implicitly-public interface members. An implicit default or canonical constructor is made
   explicit so that it can carry the required comment. The sole exemption is an `@Override`
   member, whose documentation is the overridden member's. An `@Internal` nested type inside a
   public type is out of scope; an unannotated nested type inherits the enclosing tier. A type that
   is package-private or private anywhere in its enclosing chain is off the surface even when it
   inherits a tier — the published reference cannot reach it.
2. **`ConfigOption` equality.** A `public static final ConfigOption` constant built with string
   `withDescription` literals must carry a Javadoc equal to that text (whitespace-normalized,
   Java escapes decoded). Presence alone would let the copy [#1100] created drift silently.

There is no allowlist and no curate skill: the failure carries the repair — write the Javadoc, or
make it equal to the description — which is the same rationale the checker's link rules recorded.
Both rule families are held by synthetic fixtures with mutation probes per [ADR-0118].

### Checkstyle's `MissingJavadoc*` checks were considered and do not fit

Their scoping is visibility-only (verified against the 10.18.2 jar metadata):
`MissingJavadocMethod.allowedAnnotations` reads annotations on the member itself, and
`MissingJavadocType.skipAnnotations` skips by the type's own annotation — neither can restrict to
members of a *tier-annotated enclosing type*, and no include-by-annotation direction exists.
Unscoped at `scope=public` they land ~242 findings on `@Internal` code (measured with checkstyle
semantics during the [#730] audit at main `f1a43c4f`), which is not the published promise, and
scoping by hand means maintained suppression files — the shape this repository already replaces
with Python checkers.

### What the rules deliberately do not judge

Quality. A comment that restates its signature passes the presence rule; [#1092] repaired that
class by review, and holding "does the sentence say anything" is a judgment no string check makes
honestly. The equality rule judges only the `ConfigOption` copy, where the description is the
canonical text by construction.

## Consequences

- A future re-tiering that promotes an undocumented surface fails CI at the promotion, naming
  each bare member, instead of publishing an empty reference — the [#1082] event class is now
  caught where it arrives.
- Every future public member, including one whose honest Javadoc is short, owes a comment. The
  audit's evidence is that this is achievable and the escape valves are real: before enforcement
  the `@Public` surface was already fully documented but for four bare enum constants, and a
  member with genuinely nothing to say beyond its class doc is documented in one sentence
  pointing there ([#1092]'s kept-bare precedent applies to prose quality, not presence).
- An option edit that touches the description or the Javadoc alone fails until the two match
  again, so the hover and the option's runtime description cannot disagree.
- `$add-a-connector-option` gains the Javadoc obligation, and the root guidance names the
  presence rule beside the annotation rule it extends.

[#730]: https://github.com/flink-gcp/flink-connector-gcp/issues/730
[#1082]: https://github.com/flink-gcp/flink-connector-gcp/issues/1082
[#1092]: https://github.com/flink-gcp/flink-connector-gcp/issues/1092
[#1093]: https://github.com/flink-gcp/flink-connector-gcp/issues/1093
[#1100]: https://github.com/flink-gcp/flink-connector-gcp/pull/1100
[ADR-0056]: 0056-the-api-reference-is-unfiltered-aggregated-javadoc-at-one-unversioned-path.md
[ADR-0118]: 0118-script-checker-tests-use-a-non-package-root-uv-project-and-synthetic-fixtures.md
[ADR-0141]: 0141-a-surfaces-stability-tier-is-set-by-what-can-reshape-its-inputs-and-outputs.md
