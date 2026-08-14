---
name: maintain-doc-java-snippets
description: "Maintain Java examples in docs/content and their compiled source-backed snippets. Use when adding or editing a Java code block in the documentation, changing a java-snippet shortcode or tagged source in flink-connector-gcp-docs-validation, updating a connector API used by those examples, or responding to a just check-doc-snippets or Hugo snippet-marker failure."
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
- Do not copy the same runnable code into Markdown and Java. The shortcode is the only rendered
  reference to source-backed code.

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

## Respond to failures

- A Java compiler error naming a docs-validation source means the public example or its minimal
  wrapper is stale. Fix the source and its surrounding prose together; do not add an allowlist.
  A failure in an upstream connector source is a connector build failure, not evidence that the
  example is stale.
- A missing, duplicate, reversed or empty marker means the file/tag contract is broken. Correct the
  shortcode or markers; do not fall back to copying code into Markdown.
- When an API change breaks an existing example, update the example in the same change as the API.
- Keep `flink-connector-gcp-docs-validation` behind the `docs-snippets` profile and keep its deploy
  skip. It is never a Maven Central artifact.

## Prove the result

Run:

```bash
just format
just check-doc-snippets
just docs
```

For a new mechanism or source-backed example, also make a production API member used by the
displayed region nonexistent temporarily (for example, a builder method) and require a named
compilation failure, then restore it from a committed baseline.
Probe a missing marker and require `just docs` to name the page, file and tag. Confirm the rendered
HTML contains the tagged code but not its wrapper, support types or marker comments.

Run `just check-skill-frontmatter` after editing this skill. Cross-version verification belongs in
the weekly `docs-snippets` matrix; run the affected supported versions locally when the example
touches a version-sensitive API.
