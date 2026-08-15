---
name: maintain-doc-java-snippets
description: "Maintain Java examples in docs/content and module READMEs with their compiled source-backed snippets. Use when adding or editing a Java code block in documentation or a README, changing a java-snippet shortcode, readme-example marker, or tagged source in flink-connector-gcp-docs-validation, updating a connector API used by those examples, or responding to a just check-readme-examples, just check-doc-snippets, or Hugo snippet-marker failure."
---

# Maintain Java documentation snippets

## Choose the honest form

Classify the example before editing it.

- Use a source-backed snippet for new runnable, copyable builder or API guidance, or when changing
  the executable code or API meaning of existing runnable guidance. The exact region Hugo renders
  must be in a Java compilation unit under
  `flink-connector-gcp-docs-validation/src/test/java`.
- Use an ordinary fenced `java` block for an intentionally partial or pseudocode fragment. State
  what surrounding setup is omitted; never describe that block as compiled or validated.
- Existing runnable ordinary-fenced examples remain unvalidated until migrated. A prose-only
  correction or whitespace-only edit does not require migration, but changing their displayed Java
  tokens or what they tell a reader to call does. Partial or pseudocode fences remain ordinary when
  edited, with their omissions kept explicit.
- In Hugo content, do not copy the same runnable code into Markdown and Java. The shortcode is the
  only rendered reference to source-backed code.
- In a module README, keep the ordinary fence that GitHub can render and map its checked copy to
  the tagged Java source as described below.

## Add or update runnable guidance

1. Name the compilation unit after the page and heading so a compiler error locates the example.
2. Keep imports, wrapper methods and minimal support types outside the rendered region. Do not add
   fake production APIs merely to make prose compile.
3. Delimit one rendered region with exact, unique markers:

   ```java
   // tag::example-name[]
   // end::example-name[]
   ```

4. Render it with a named shortcode:

   ```text
   {{< java-snippet file="Example.java" tag="example-name" >}}
   ```

5. Keep the source on APIs common to the supported Flink range. If a real version difference
   exists, model it in the repository's compatibility seam; never skip a matrix row or duplicate
   the displayed snippet per version.

## Change the shortcode

The source-backed snippet contract is recorded in
`docs/adr/0115-runnable-documentation-snippets-render-one-exact-region-from-compiled-java-sources.md`.
Preserve these boundaries when changing `docs/layouts/_shortcodes/java-snippet.html`:

- Require non-empty named `file` and `tag` arguments and resolve the source from Hugo assets.
- Recognize marker-only lines by exact equality after trimming whitespace. Require exactly one
  start and one end marker, in that order, with a non-empty region between them.
- Render only the lines inside the markers. Keep wrapper code, imports, support types and marker
  comments out of the generated HTML.
- Remove the exact leading horizontal-whitespace prefix shared by every non-blank region line
  before highlighting. Preserve the lines' relative indentation and all other whitespace.
- After argument validation, keep failures actionable by naming the fixture page position, source
  file and tag, plus the observed marker count when it distinguishes the failure.

Extend `docs/tests/fixtures/java-snippet/` when changing rendering or a validation branch. The
fixture site must mount the repository shortcode rather than copy its parsing logic, and its pages
and Java sources must remain outside Hugo's production content and static roots. Give each failing
branch an independent page so the runner can invoke and assert that branch separately even when
Hugo aggregates multiple template errors.

Before trusting a new fixture, inject the defect it claims to catch into a committed shortcode,
require the named fixture to fail for the intended assertion, and then restore the committed
baseline. A compile failure alone is not evidence for a rendering or validation claim.

## Maintain a module README example

Every Java fence in `flink-*/README.md` chooses one form immediately above its opening fence.
Keep the marker and fence at the README top level; the checker rejects list- or quote-nested Java
fences so it cannot silently mistake their container prefixes for displayed Java.

Map runnable guidance to a compiled region by file name and tag:

````text
<!-- readme-example file="Example.java" tag="example-name" -->
```java
// The exact tagged source region is copied here for GitHub.
```
````

The displayed copy may differ from the source only by container indentation.
Run `just format` on the backing source first, then copy the formatted tagged region without its
marker comments.
One backing region may also serve a Hugo page, but it may not serve two README blocks.

Keep intentionally partial guidance outside compilation only when visible prose immediately before
the marker names the omission:

````text
Abbreviated, not compiled: application-specific schema setup is omitted.

<!-- readme-example partial="application-specific schema setup" -->
```java
call(...);
```
````

The hidden reason supports maintenance but does not replace the reader-visible explanation.

## Respond to failures

- A Java compiler error naming a docs-validation source means the public example or its minimal
  wrapper is stale. Fix the source and its surrounding prose together; do not add an allowlist.
  A failure in an upstream connector source is a connector build failure, not evidence that the
  example is stale.
- A missing, duplicate, reversed or empty marker means the file/tag contract is broken. Correct the
  shortcode or markers; do not fall back to copying code into Markdown.
- A `just check-readme-examples` failure names the README fence and its backing identity. Restore
  the exact displayed copy, add the missing classification, or make a partial example's omission
  visible; do not weaken the comparison or add an allowlist.
- When an API change breaks an existing example, update the example in the same change as the API.
- Keep `flink-connector-gcp-docs-validation` behind the `docs-snippets` profile and keep its deploy
  skip. It is never a Maven Central artifact.

## Prove the result

Run:

```bash
just format
just check-readme-examples
just check-doc-snippets
just test-java-snippet-shortcode
just docs
```

For a new mechanism or source-backed example, also make a production API member used by the
displayed region nonexistent temporarily (for example, a builder method) and require a named
compilation failure, then restore it from a committed baseline.
Probe a missing Hugo marker and require `just docs` to name the page, file and tag. Confirm the
rendered HTML contains the tagged code but not its wrapper, support types or marker comments.
For a README mapping, change one displayed token and require `just check-readme-examples` to name
the README, line, file, and tag before restoring the committed baseline.

Run `just check-skill-frontmatter` after editing this skill. Cross-version verification belongs in
the weekly `docs-snippets` matrix; run the affected supported versions locally when the example
touches a version-sensitive API.
