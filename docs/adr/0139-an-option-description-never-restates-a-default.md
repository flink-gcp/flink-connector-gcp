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

# ADR-0139: An option description never restates a default

- Status: Accepted
- Date: 2026-08-23; BigQuery key examples refreshed by [#1047] (2026-08-23)
- Issues: [#1045] (carried from [#866] via the [#782] cross-module review), [#1047]
- Modules: all connectors
- Current behavior: a mapped option's default lives in the connector's
  `docs/content/docs/reference/<connector>.md` table; a table-owned option's default lives in its
  `docs/content/docs/connectors/table/<connector>.md` row

## Context

[ADR-0014](0014-the-pubsub-table-layer-maps-onto-the-datastream-builders.md) states the rule for
Pub/Sub: a mapped `ConfigOption` carries no default, because the options object it feeds is where
a default is written. [#866] added the description half — a default written into prose is the same
second copy, out of reach of `hasDefaultValue()` — and its fix deliberately stopped at the module
boundary: the guard test landed in Pub/Sub alone, and the cross-module decision was left to
[#782]. Meanwhile
[ADR-0086](0086-the-bigtable-table-layer-maps-onto-the-builders-over-an-hbase-compatible-ddl.md)
was already citing "ADR-0014's no-default-restated rule" to decide a Bigtable question, so the
rule was applied across modules while being recorded in one. [#1045] is the decision.

## Decision

The rule binds all five connectors, in both halves.

**`defaultValue()` half.** A mapped option is declared without a default; its default lives on
the builder and is applied by not calling the setter. Each module's `*ConnectorOptionsTest`
records its own exceptions: table-owned options that define a Table API default (Bigtable, Cloud
Tasks, Spanner selectors), and Spanner's three change-stream knobs whose `defaultValue()`
references the builder's own constant, so the compiler keeps the two copies in step.

**Description half.** No option description states a default — a declared one, a derived one, or
the value or behaviour absence selects ("uses application-default credentials when unset",
"Absent, partitions never expire"). The docs pages are where a default is written — the reference
page for a mapped option, the table page's option row for a table-owned one — a derived default
included, carrying both the derivation and the resolved value. Three prose classes are
deliberately outside the rule, because none of them states a value a default owns elsewhere:

- a constraint or failure that absence imposes ("Unset fails the restore", "the table must
  declare a NOT NULL writable 'url' metadata column") — absence selects no value, it refuses;
- a per-row-overridable table-owned option naming its role ("The default HTTP method. A non-null
  'http-method' metadata value overrides it per row");
- provenance of a default ("This option and its default are the HBase connector's") — whose it
  is, not what it is.

**How it is held.** Every module carries the same pair of guards in its
`*ConnectorOptionsTest`: the `hasDefaultValue()` assertion, and
`noDescriptionRestatesADefault`, which formats each description through `HtmlFormatter` and
rejects twelve case-insensitive phrases. The phrase list is a regression guard over the forms the
repository has actually produced, not a semantic parser; a new restatement form extends all five
lists in the same change. The guard formats the *joined* description rather than grepping source
because two of the violations it exists for span Java string-literal concatenation and are
invisible to a line-based scan.

## Evidence

Measured 2026-08-23 at `4be8cb7b`. The four-phrase instrument [#1045] carried named five
candidate sites; the full sweep of the four unguarded modules found **26**:

- Two matched the original four phrases only after string-literal joining — Bigtable
  `sink.cell-timestamp.truncate-to-millis` ("Disabled by" + " default") and Cloud Tasks
  `service-account-key-file` ("when" + " unset") — so the issue's grep-based count was an
  undercount of its own instrument.
- The same fact was restated in four modules under four phrasings: the ADC fallback read "When
  absent, clients use application-default credentials" (BigQuery), "Uses application-default
  credentials when unset" (Bigtable, Cloud Tasks), and "unset uses Application Default
  Credentials" (Spanner). A phrase-local fix per module cannot hold that class, which is the
  practical argument for one repository-wide rule.
- Every one of the 26 deleted statements was already present in the reference or table docs pages
  — zero documentation additions were needed. Each deletion removed a live second copy, not
  information.
- The change's own review round found two more restatements of the same derived default from the
  *supplying* side — `project`'s and `scan.query`'s descriptions each said the billing project
  falls back "unless scan.parent-project overrides it", the fact just deleted from
  `scan.parent-project` itself — in a form no phrase covers. Both deleted; the word "unless"
  was not added as a phrase because the constraint class the rule exempts legitimately uses it.
- Pub/Sub, the one module the guard already held, contained none of the twelve phrases before
  this change. Its two "Without it …" sentences (`sink.auto-create.message-retention`,
  `sink.auto-create.storage-policy.allowed-regions`) were judged and kept: the first warns of a
  consequence (a backwards seek cannot reach acknowledged messages) and the second points at the
  deciding policy without naming a value — the constraint and pointer classes, not restatements.

## Alternatives declined

- **Keeping the rule module-local.** ADR-0086 already applied it across modules, and the ADC
  measurement above shows identical restatements diverging only by per-module phrasing — a
  module-local rule deletes three of four copies of one sentence and keeps the fourth.
- **A `scripts/` checker.** Priced in the [#866] change and declined again here: per-module tests
  reuse each module's existing reflective `declaredOptions()` helper and cost about one test
  each, where a checker owes synthetic tests, a `curate-*` skill, a recipe and CI wiring — more
  machinery than the convention it holds.
- **Narrowing the rule to declared defaults, exempting absent-value behaviour statements.**
  Declined for Pub/Sub in [#778]/[#838] because the exemption set starts empty; measured here,
  every "behaviour" site (the FILE_LOADS location derivation included) was already stated in the
  docs pages, so the literal rule lost nothing.

## Consequences

A new option's default goes to its docs row only — the reference page for a mapped option, the
table page's option row for a table-owned one. A guard failure edits the description,
never the test. The three exempt prose classes above are the judgment a reviewer applies before
extending the phrase list; a phrase is added only with zero false positives across all five
modules, and to all five guards at once.

[#778]: https://github.com/flink-gcp/flink-connector-gcp/issues/778
[#782]: https://github.com/flink-gcp/flink-connector-gcp/issues/782
[#838]: https://github.com/flink-gcp/flink-connector-gcp/issues/838
[#866]: https://github.com/flink-gcp/flink-connector-gcp/issues/866
[#1045]: https://github.com/flink-gcp/flink-connector-gcp/issues/1045
[#1047]: https://github.com/flink-gcp/flink-connector-gcp/issues/1047
