---
name: maintain-javadoc-examples
description: "Maintain Java examples in public Javadoc and their compiled backing regions. Use when adding or editing a <pre>{@code ...}</pre> block in a main Java source, changing a javadoc-example marker or a source under flink-connector-gcp-docs-validation/src/test/java/io/github/flink/gcp/connector/docs/javadoc, or responding to a just check-doc-snippets Javadoc classification, synchronization, or compilation failure."
---

# Maintain Javadoc examples

## Choose the honest form

Classify every `<pre>{@code ...}</pre>` block in a main Java source before editing it.

- Use a runnable source-backed example for complete Java that a reader can copy into an appropriate
  wrapper.
- Use an abbreviated example only when application-specific or deployment-specific text makes a
  compilable copy misleading.
  Introduce it with visible `<b>Abbreviated, not compiled:</b>` prose that names the omission.
- Do not exempt runnable guidance to avoid creating its backing source.
  The partial form is a reader-facing classification, not an allowlist.

## Back runnable guidance with one exact region

1. Put the backing compilation unit under
   `flink-connector-gcp-docs-validation/src/test/java/io/github/flink/gcp/connector/docs/javadoc`.
2. Keep imports, wrapper methods, and minimal user-owned support types outside the tagged region.
   Do not invent production APIs or hide a connector call behind a helper merely to make the
   example compile.
3. Give the displayed block its own region with exact markers:

   ```java
   // tag::example-name[]
   // end::example-name[]
   ```

4. Put the matching marker immediately above the Javadoc block:

   ```html
   <!-- javadoc-example file="JavadocProductExamples.java" tag="example-name" -->
   ```

5. Keep the code between the Javadoc and source markers identical after container indentation is
   removed.
   Do not share a region between blocks or leave unused regions and backing files behind.

For an abbreviated block, put this marker immediately above it and make the reason concrete:

```html
<p><b>Abbreviated, not compiled:</b> deployment-specific connector options are omitted.
<!-- javadoc-example partial="deployment-specific connector options" -->
```

## Respond to failures

- A missing or malformed `javadoc-example` marker means the public block has not made an honest
  runnable-or-abbreviated choice.
- A displayed-code difference means one copy changed without the other.
  Update both in the same edit and keep the more readable formatting when Java formatting permits
  it.
- A missing, duplicate, reversed, empty, or unused backing tag is an inventory defect.
  Repair or remove the stale region; do not add an exemption.
- A Java compiler error naming a Javadoc backing source means the public example is stale against
  the current reactor.
  Fix the example and backing source together.
- A Java compiler error in a connector source is a connector build failure, not evidence that its
  Javadoc example should become abbreviated.

## Prove the result

Run:

```bash
just format
just check-doc-snippets
just docs-javadoc
just test-scripts
just check-skill-frontmatter
```

For a new checker mechanism, temporarily change a displayed token and require
`just check-doc-snippets` to name the mismatched source and tag.
Then temporarily make a production API member used by a backing region nonexistent and require the
same command to fail compilation in the named backing source.
Restore each mutation from a committed baseline and rerun the clean command.
