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

# ADR-0116: Configuration option references stay hand-written and are held bidirectionally to the sources

- Status: Accepted
- Date: 2026-08-01; revised by [#328] (2026-08-08)
- Issues: [#89], [#328]
- Modules: all connectors (documentation tooling)
- Current behavior: [`check-option-docs.py`](../../scripts/check-option-docs.py),
  [`option-docs.toml`](../../scripts/config/option-docs.toml),
  [option-documentation curation](../../.agents/skills/curate-option-docs/SKILL.md)

## Context

Issue [#89] asked for one reference surface where a reader could find every connector option and its default without reading each connector's design narrative.
The implementation survey found that 131 of 135 builder setters already appeared somewhere in the documentation and that all 50 then-existing Table API keys already had rows.
The gap was findability and drift detection rather than missing option prose.

The existing tables also carried information that source generation could not preserve.
One Pub/Sub table grouped 17 setters into six rows, and defaults came from constants, field initializers, or an unset value whose effective default belonged to the client library.
A generated row per setter would split useful groups and could not fill the most useful defaults.

## Decision

The connector option references remain hand-written.
Their tables group related setters, state what an unset value means, and leave the surrounding connector pages to explain why an option exists.

`check-option-docs.py` buys back the drift property that generation would have supplied.
A table opts into the check when its first column header is exactly `Option`.
The exact header keeps metadata, type-mapping, policy and example tables outside the check, and a table inside a fenced code block earns no coverage.

The check runs in both directions:

- **Coverage**: every public builder setter and mapped Table API `ConfigOption` key appears in an opted-in table row.
- **Staleness**: every name in an opted-in table row resolves to a mapped setter or key, or to an explicit page-side exception.
- **Reach**: every public builder matching the supported source shapes is reached by a module mapping, while a builder whose filename falls outside those shapes is named under that module's `sources` list.

Mappings are module-wide rather than a list of option classes.
A new `*Options`, `*SinkBuilder`, or `*SourceBuilder` class therefore joins the check as soon as it exists, and a module declaring options without a mapping fails instead of remaining invisible.
An `@Internal` top-level type is the only source-level exemption from the public-builder reach rule.

The two allowlists point in opposite directions.
`[exempt]` forgives a source-side setter with no documentation row, while `[extra]` forgives a page-side row with no declaration in the mapped sources.
Each entry carries a reason, and an entry that never fires is itself a failure because it would otherwise preserve a claim after the condition that justified it disappeared.

The check compares option names, not values.
A changed default, constraint, or meaning must still update its row in the same change, and the curation skill keeps that review obligation explicit.

## Evidence

[PR #214](https://github.com/laughingman7743/flink-connector-gcp/pull/214) implemented the reference pages and the first bidirectional checker.
The checker found a documented `format` key that came from Flink's `FactoryUtil` rather than the connector's own `ConfigOption` class, establishing the need for the page-side direction.

The same pull request's second review found that all four initial `[exempt]` entries were dead.
Each bulk overload was already named in the same row as its singular form, so removing the entire allowlist changed no verdict.
That finding added the dead-entry failure in both directions.

[PR #252](https://github.com/laughingman7743/flink-connector-gcp/pull/252) added synthetic checker tests and applied 16 rule-level mutants across the three pre-existing checkers.
Two option-checker mutants survived the first pass because their tests could not distinguish the mutated behavior: the header control used `Property`, and the commented setter was placed where the declaration regex could never match it.
Rewriting those controls made both tests discriminate.

Issue [#328] exposed a different reach gap when `PubSubDeadLetterQueue.Builder` carried public options but matched none of the supported filename shapes.
[PR #395](https://github.com/laughingman7743/flink-connector-gcp/pull/395) measured broader globs, found that they admitted unrelated internal builders and an optionless base interface, and chose an explicit `sources` entry plus an unmapped-public-builder failure.

## Alternatives declined

- **Generate the reference pages**: generation would lose deliberate grouping and could not derive client-library defaults that exist only as prose.
- **Map each options class separately**: a newly added class would be invisible until someone also remembered to extend the mapping, recreating the drift the checker exists to catch.
- **Widen the filename globs until every builder matches**: the measured candidates admitted record-serialization builders, internal table builders and an optionless interface.
- **Run Java to compare default values**: that would turn an offline name audit into a JVM-backed semantic extractor while still leaving prose meanings outside the result.
- **Use allowlist entries for grouped overloads**: a row can name several setters directly, which is both clearer to the reader and testable without an exception.

## Consequences

Adding, renaming or deleting an option updates the hand-written row in the same change.
Changing only a value or its meaning remains a review obligation because the checker deliberately does not infer semantics.

A checker failure is resolved through the curation skill's stable ladder: correct the source or row first, use `[exempt]` or `[extra]` only for a real directional exception, and record why the entry must remain.

[#89]: https://github.com/laughingman7743/flink-connector-gcp/issues/89
[#328]: https://github.com/laughingman7743/flink-connector-gcp/issues/328
