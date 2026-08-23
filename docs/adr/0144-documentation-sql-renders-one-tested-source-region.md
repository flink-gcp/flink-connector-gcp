<!--
Copyright 2026 The flink-gcp authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# ADR-0144: Documentation SQL renders one tested source region

- Status: Accepted
- Date: 2026-08-24
- Issue: [#1097](https://github.com/flink-gcp/flink-connector-gcp/issues/1097)
- Modules: all (documentation tooling), spanner
- Current behavior: [SQL snippet maintenance skill](../../.agents/skills/maintain-doc-sql-snippets/SKILL.md),
  [`sql-snippet` shortcode](../layouts/_shortcodes/sql-snippet.html),
  [Flink SQL planner test](../../flink-connector-gcp-docs-validation/src/test/java/io/github/flink/gcp/connector/docs/DocumentationSqlPlanTest.java),
  [GoogleSQL emulator test](../../flink-connector-gcp-spanner/src/test/java/io/github/flink/gcp/connector/spanner/SpannerDocumentationSqlITCase.java)

## Context

An ordinary fenced SQL block is text to Hugo and can drift when a connector option, schema
requirement, or planner contract changes.
Keeping a checked SQL copy beside the Markdown would still allow the rendered and checked copies
to diverge.

Issue #1097 recorded 18 unvalidated fences when it was opened.
The implementation inventory on 2026-08-24 found 29 complete SQL examples in the examples and
quickstart pages: 27 Flink SQL regions and two GoogleSQL regions.
It also found 32 SQL fences in connector Table API reference pages.
Those reference fences mix complete connector DDL and queries, partial option or schema fragments,
and intentionally invalid examples, so they do not share one executable boundary with the examples
and quickstarts.

The repository's decision policy requires pricing a plain test before adding a custom checker.
The current Table API factories already expose the failure boundary through Flink's parser and
planner, and the Spanner test harness already exposes a GoogleSQL boundary through its pinned
emulator.

## Decision

Every code block declared with the `sql` language under `docs/content/docs/examples/` and
`docs/content/docs/quickstart/` renders one exact tagged region from a `.sql` test resource.
Unlabeled, indented, or differently labeled code is not classified as SQL by this boundary.
The marker syntax is:

```sql
-- tag::example-name[]
-- end::example-name[]
```

The `sql-snippet` shortcode applies the same argument, marker-count, ordering, non-empty-region,
indentation, and actionable-diagnostic contract as `java-snippet`.
Both shortcodes delegate that contract to one Hugo partial while retaining their own comment
syntax, asset directory, and lexer.
The generic code-block hook applies the SQL guard and delegates theme-compatible rendering to one
repository-owned partial, so guarded SQL cannot drift from other code blocks.
Synthetic Hugo fixtures exercise the SQL rendering, theme-option, and error branches, and the Java
fixture suite runs after a shared-partial change.
Hugo's generic code-block render hook rejects ordinary SQL fences in the covered
paths, so CommonMark container and nesting semantics come from the same parser that renders the
site rather than from a second Markdown parser in the validation harness.

Flink SQL resources live in the opt-in docs-validation module.
`DocumentationSqlPlanTest` parses every statement, creates the documented catalog objects, and
translates every query and modify operation through the planner against the current connector
factories.
It uses a batch environment for examples that set batch runtime mode and applies other documented
`SET` values to the test configuration.
It neither submits a Flink job nor calls GCP.

The shortcode emits an empty hidden marker beside each rendered SQL region.
After Hugo builds a clean production destination, the same test reads markers only from the
generated examples and quickstart paths and owns a two-way inventory between rendered examples and
all SQL source regions.
Each source region renders exactly once across that covered scope; a second page gets its own named
region even when the statements are intentionally identical, so a duplicate marker cannot hide an
additional rendered example from the inventory.
The `check-doc-snippets` recipe opts into that generated-site assertion; profile-only Maven builds
still run every planner scenario without requiring Hugo output.
It also requires every Flink SQL region to appear in exactly one named planning scenario.
The inventory is derived from the tree rather than pinned to the measured count, so a later region
cannot silently escape validation.
Hugo mounts Flink SQL under `flink/` and GoogleSQL under `spanner/`, and that namespace remains part
of each inventory identity so equal filenames and tags in different modules cannot shadow each
other.
CDC sink scenarios register an upsert changelog stream with update and delete rows instead of an
append-only placeholder view, so planning exercises the sink's changelog negotiation.

GoogleSQL resources live in the Spanner module.
`SpannerDocumentationSqlITCase` creates each documented schema and executes its seed statements
against the repository-pinned Spanner emulator.
Its inventory requires every GoogleSQL region to have an execution assertion.
Emulator acceptance is evidence for the documentation example's syntax and interaction with the
pinned client, not evidence for behavior of the Spanner service.

`just check-doc-sql-snippets` runs the production Hugo build and both executable boundaries.
`just test-sql-snippet-shortcode` owns isolated SQL rendering and raw-fence failure cases, while
the nested `just docs` invocation owns production Hugo integration.
The docs workflow runs all three before publishing the site.

## Evidence

On 2026-08-24, planning the previously unvalidated Spanner lookup example exposed an existing
defect: its facts table declared `region AS 'us'` as a computed key component.
Flink reduced that predicate to a source filter, and the connector rejected the remaining
incomplete lookup key.
The source-backed example now derives a prefixed demo region from the physical numeric account
field, so both sides deterministically produce the documented composite key and the complete
lookup plan succeeds before any GCP call. The rendered prose identifies that dependency as a
one-row demonstration; real event tables normally carry both key columns as physical fields.

On 2026-08-24, all 27 Flink SQL regions passed 21 named scenarios plus the two-way inventory test
against the current reactor.
Both GoogleSQL regions created their schemas against the emulator, and the accounts seed statement
inserted the row the example describes.
The SQL synthetic fixture suite also passed its valid render and independent marker-failure cases.

## Alternatives declined

**Add a static Markdown-to-test checker.**
Front matter, HTML comments, and nested code fences all affect whether a shortcode becomes a
reader-visible example.
Reading Hugo's generated marker elements uses the same parse result as the published site instead
of maintaining a second source-text interpretation in the validation harness.

**Stop at SQL parsing.**
Parsing catches syntax but not connector discovery, option validation, changelog requirements, or
lookup-key planning.
Queries and modifications therefore reach planner translation.

**Execute every Flink SQL example.**
The examples name user-owned GCP resources and many describe unbounded jobs.
Submitting them would require credentials and fixtures while adding no stable per-pull-request
boundary beyond the connector and planner checks already selected.

**Treat the Spanner emulator as service evidence.**
The module's testing policy records known emulator deviations.
The emulator is useful for executing these GoogleSQL examples but cannot establish production
service semantics.

**Cover every SQL fence in the documentation immediately.**
The 32 Table API reference fences mix complete connector DDL and queries, partial option or schema
fragments, and intentionally invalid examples.
Expanding source-backed validation to them requires a separate classification decision that assigns
an honest executable or negative-test boundary to each form.

**Keep SQL in both Markdown and test resources.**
Either copy could change while the other check remained green.
Rendering the tested region removes that second source of truth.

## Consequences

Changing executable SQL guidance in an examples or quickstart page means changing its tagged test
resource and the named scenario or emulator assertion together.
A connector Table API change that invalidates one of those examples fails the docs workflow before
the site is published.

The docs-validation module gains planner-only test dependencies but remains behind the
`docs-snippets` profile and cannot be deployed.
The GoogleSQL boundary adds a Docker-backed Spanner emulator test to the docs workflow.

The table-reference fences remain explicitly outside this decision.
A future expansion must classify their mixed forms and record how each complete, partial, or
intentionally invalid block earns an honest validation claim.
