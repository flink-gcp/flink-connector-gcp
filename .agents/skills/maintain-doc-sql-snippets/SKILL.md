---
name: maintain-doc-sql-snippets
description: "Maintain source-backed SQL examples in docs/content/docs/examples, docs/content/docs/quickstart, and docs/content/docs/connectors/table. Use when adding or editing a code block labeled sql in those directories, changing the sql-snippet shortcode or tagged .sql resources, updating connector Table API behavior used by an example, or responding to just check-doc-sql-snippets, DocumentationSqlPlanTest, SpannerDocumentationSqlITCase, or Hugo SQL marker failures."
---

# Maintain SQL documentation snippets

## Choose the executable boundary

Classify the SQL before editing it.

- Flink SQL belongs under
  `flink-connector-gcp-docs-validation/src/test/resources/sql-snippets/`. It is parsed and planned
  against the connector factories in `DocumentationSqlPlanTest`; no GCP job is submitted.
- GoogleSQL belongs under `flink-connector-gcp-spanner/src/test/resources/sql-snippets/`. Its DDL
  and seed statements execute against the pinned Spanner emulator in
  `SpannerDocumentationSqlITCase`. Emulator acceptance proves this example boundary, not service
  behavior.
- This contract covers `docs/content/docs/examples/`, `docs/content/docs/quickstart/`, and
  `docs/content/docs/connectors/table/`.
- Classify a Table connector reference region as a complete positive example, a partial fragment,
  or an intentionally invalid example. A complete example reaches its documented command, catalog
  or planner boundary. When a session command has no catalog or planner operation, parse it and
  assert the exact command-specific value it publishes. A fragment gets the smallest test-only
  enclosure that makes its displayed SQL meaningful without publishing invented context. A
  negative example must pass earlier validation and fail with the documented diagnostic.
- Hugo's generic `render-codeblock.html` hook rejects ordinary SQL fences in the covered paths. Do not
  replace that parser-backed boundary with a source-text fence parser.
- The inventory reads hidden `sql-snippet` marker elements from the built production HTML. Keep
  the clean `just docs` build before the planner test, and keep the inventory scoped to generated
  examples, quickstarts, and Table connector references; do not infer rendered shortcode usage
  from Markdown text.
- A source region renders exactly once across those paths. Give a second rendered example its own
  named region even when its SQL is identical, so the rendered inventory still counts both.
- Do not duplicate executable SQL in Markdown. The tagged resource is the source Hugo renders and
  the test validates.

## Add or update a region

Use exact marker-only lines around one complete example:

```sql
-- tag::example-name[]
CREATE TABLE example (...);
-- end::example-name[]
```

Render it by file and tag:

```text
{{< sql-snippet file="flink/ConnectorExamples.sql" tag="example-name" >}}
```

The `flink/` namespace selects the docs-validation resource mount. GoogleSQL regions use the
`spanner/` namespace. Keep the namespace in every source identity so two modules cannot silently
shadow the same filename and tag.

Keep the Apache header and maintenance comments outside the markers. Comments inside a region are
published and must be correct user guidance.

For Flink SQL, add the region to one named `Scenario` in `DocumentationSqlPlanTest`. Group regions
that form one catalog state in execution order. Supply only minimal temporary views needed to
stand in for an upstream example. Do not mock a connector factory or replace documented option
keys merely to get a plan. The inventory test requires every Flink region to have exactly one
validation boundary and every rendered shortcode in the built production site to resolve to one
region.

Use a batch `TableEnvironment` when a region sets `execution.runtime-mode` to `batch`. Other `SET`
operations are parsed and applied to the test configuration. Query and modify operations must reach
planner translation; parsing DDL alone misses connector discovery, option validation and lookup-key
requirements.

For a partial Table reference region, keep its enclosure in the validation harness rather than the
rendered resource. Validate an option fragment inside a complete connector DDL, validate standalone
expressions in a minimal query, and add a follow-up query when a source DDL otherwise stops before
connector discovery. A catalog-only enclosure is acceptable when the displayed construct is a DDL
fragment and forcing a query would exercise an unrelated planner limitation. Name the boundary in
the scenario so the narrower claim remains visible.

For an intentionally invalid region, assert both the object involved and the stable connector
diagnostic. Shape the preceding SQL so column-count, type, and catalog checks succeed first; a
failure at an earlier generic boundary does not validate the documented rejection.

For GoogleSQL, add an emulator assertion that creates the documented schema and executes any seed
statement. Extend the source-region inventory in the same test so a region cannot be rendered but
unexecuted. Strip statement terminators only at the client call boundary; retain copyable SQL in the
rendered region.

## Change the shortcode

`sql-snippet` and `java-snippet` share `docs/layouts/_partials/tagged-snippet.html`. Preserve both
contracts when changing that partial:

- require non-empty named `file` and `tag` arguments;
- resolve the source from the configured Hugo asset mounts;
- require exactly one start and end marker, in order, around a non-empty region;
- render only the region, with its common horizontal indentation removed;
- name the page position, file, tag and relevant marker count in failures.

Extend the SQL fixture site for SQL-specific and shared code-block behavior. Run both shortcode fixture suites after a
shared-partial change so a SQL repair cannot regress Java rendering.
When changing the SQL code-block render hook or its covered paths, retain fixtures for both a live
raw SQL fence that must fail and an SQL-looking fence nested inside an outer Markdown example that
must render.

## Respond to failures

- A parser error means the displayed Flink SQL is not valid for the supported version. Fix the SQL
  and prose together; do not skip that scenario.
- A connector discovery or validation error means the example has drifted from the working-tree
  Table API. Use the public option keys and required schema the user must supply.
- A planner error in a lookup or modify operation is part of the contract, not a reason to reduce
  the test to parsing.
- A Spanner emulator DDL or DML error means the GoogleSQL region is not executable. Keep emulator
  deviations distinct from claims about the real service.
- A documentation/region inventory mismatch means a shortcode or source region is orphaned. Add
  the missing counterpart or remove both intentionally; do not add an allowlist.
- A marker failure is a source identity error. Correct the marker or shortcode rather than copying
  SQL back into Markdown.

## Prove the result

Run:

```bash
just format
just check-doc-sql-snippets
just test-java-snippet-shortcode
just test-sql-snippet-shortcode
just check-skill-frontmatter
```

For a new mechanism, temporarily break one documented connector option key and require its named
planner scenario to fail. Temporarily remove one source marker and require `just docs` to name the
page, file and tag. Temporarily make one GoogleSQL statement invalid and require the named emulator
test to fail. Restore the committed baseline after every probe. Inspect rendered HTML to confirm it
contains the SQL region but excludes the licence header and marker comments.

Because the planner APIs differ across supported Flink versions, run `just verify-flink 1.20.4
-Pdocs-snippets` when the validation harness or SQL syntax changes. That profile-only build checks
the version-sensitive planner boundary; `just check-doc-sql-snippets` separately owns the
version-independent rendered-site inventory.
