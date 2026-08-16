# Development Guide for AI Coding Agents

## Project

This repository contains independent Google Cloud connectors for Apache Flink: BigQuery,
Cloud Pub/Sub, Cloud Tasks, Bigtable, and Spanner. It is not affiliated with the Apache Software
Foundation or Google. The Maven reactor is based on `flink-connector-parent`; Google Cloud
versions come from `libraries-bom`.

Before changing behavior, public API, build policy, CI, packaging, licensing, or a connector's
design, read the matching section of `.agents/references/repository-guide.md`. Before changing a
connector module, also read its `.agents/references/modules/<module>.md` file and the ADRs it
names. Those references preserve the detailed constraints formerly loaded into every Claude Code
session; this file intentionally stays small enough for Codex's project-guidance budget.

Before planning non-trivial repository work, use `$project-memory` to consult the private local
agent memories when they exist: the Claude Code project memory and the Codex memory store. Read
only task-relevant entries, verify their claims against current sources, and never copy local
memory into tracked files without an explicit privacy and staleness review. The skill resolves
both stores from a worktree and scopes the Codex store to this repository.

## Commands

The `justfile` is the command index and CI calls the same recipes. Run `just --list` when unsure.
In a shell without mise activated, use `mise x -- just <recipe>`.

- `just format`: format code; run before committing.
- `just verify`: full Maven verification with formatting, checkstyle, unit tests, integration
  tests, packaging, and apache-rat. It requires JDK 17 and Docker but no GCP credentials.
- `just verify-module <module>`: verify a module and its reactor dependencies; keep its `-am`.
- Scope local test runs to what changed: targeted `-Dtest=...` while iterating, one module's
  tests before committing. Run the full `just verify` locally only when the change touches what
  every module builds against — `flink-connector-gcp-base`, `flink-connector-gcp-test-utils`, the
  root POM, `mise.toml`, or a `justfile`/CI recipe; a `scripts/*.py` change wants `just
  test-scripts` and the affected checker, not a Maven build. For a single-module change, push and
  let the per-connector CI lane carry the full verification — it is faster than a local full build
  and starts clean. Two exceptions stay local: per-PR CI builds one Flink version, so a
  compatibility-sensitive change (cross-version shims, renames the 1.x source root sees) still
  runs `just verify-flink 1.20.4` before push; and self-review's "re-run whatever the change
  touches" means the scoped suite above, not an unconditional full verify.
- `just verify-flink <version>`: verify another supported Flink version; clean when moving between
  Flink 1.x and 2.x.
- `just check-readme-examples`: check module README Java examples against compiled source.
- `just check-doc-snippets`: check and compile source-backed Java examples rendered in the docs,
  module READMEs, and public Javadoc.
- `just test-java-snippet-shortcode`: test the Hugo shortcode against synthetic fixtures.
- `just lint`: lint scripts, workflows, rendered Markdown, and OpenTofu.
- `just test-scripts`: run the Python checker test suite.
- `just check-skill-frontmatter`: validate all repository skills and the Claude compatibility
  symlink.
- `just check-option-docs`, `just check-metric-docs`, and `just check-flink-api-tiers`: run the
  repository-specific source/documentation audits.
- `just e2e`: run credential-gated real-GCP suites. A fresh worktree first needs
  `just worktree-env` to link the main checkout's uncommitted `.env`.

Read `.agents/references/repository-guide.md` before changing a recipe, CI workflow, supported
Flink version, shaded artifact, NOTICE/licence source, documentation architecture, or OpenTofu.
Local `target/` content and project SNAPSHOTs in `~/.m2` can make a broken reactor change look
green; use the clean-state procedures in that guide for such changes.

## Documentation and design

- Current behavior and user-facing rationale belong in `docs/content/`.
- Before adding or changing Java examples in docs or a module README, or when
  `just check-readme-examples` or `just check-doc-snippets` fails, use
  `$maintain-doc-java-snippets`. New or materially changed runnable API guidance is source-backed;
  intentionally partial or pseudocode examples remain ordinary fenced blocks and must be
  described as abbreviated. Existing ordinary docs fences remain unvalidated until migrated;
  every module README Java fence is classified. When the shortcode itself changes, run its
  synthetic fixture suite.
- Before adding or changing a public Javadoc code block, or for a Javadoc-specific
  `just check-doc-snippets` failure, use `$maintain-javadoc-examples`. Runnable blocks map to exact
  compiled backing regions; abbreviated blocks are visibly classified and explain their omission.
- Option and metric inventories belong only in their reference/DataStream tables; use the matching
  `curate-*` skill when a checker fails.
- Module `README.md` files are overviews: status table, minimal sample, docs link, and provenance.
- Settled decision evidence, declined alternatives, and supersession history belong in
  `docs/adr/`. Update an ADR for a refinement; add a superseding ADR for a reversal.
- Agent guidance contains imperative development rules and routes to the durable records. Never
  leave repository-relevant facts only in private agent memory.
- Behavior or public API changes require the corresponding docs update in the same change.

## Code and tests

- Production packages use `io.github.flink.gcp.connector.<product>`; each main-tree class carries
  the appropriate Flink API annotation. `@Public` types are frozen by the japicmp gate — breaking
  or demoting one is a deliberate act with an exclusion and a release-notes entry (ADR-0124).
- Do not introduce Guava, Mockito, PowerMock, Lombok, or AutoValue. Use Flink preconditions,
  `javax.annotation`, and hand-written `Fake*` test doubles.
- A **sink** serialization schema returning `null` means skip, not failure; a wrapped Flink
  serializer returning `null` is a serialization failure. Read ADR-0001 before touching this
  contract. **Source** deserialization is `Collector`-based and returns nothing: a skip is
  collecting nothing for the message, and the deserialization failure policy governs *thrown*
  failures — neither is expressed by a `null` return.
- Forge options objects from `builder().build()`, never the process-wide `defaults()` singleton.
- Bound `Duration` values that will become nanoseconds at `Duration.ofNanos(Long.MAX_VALUE)`,
  through `OptionChecks.checkExpressibleInNanos` (ADR-0068). Whether the bound is re-checked where
  a deserialized options instance is relied on is a per-connector decision that ADR-0068 records —
  Pub/Sub re-checks, Bigtable documents why it does not — so read it before assuming either.
- Tests that call a production `createWriter` path must configure an emulator endpoint.
- `*Test` is a unit test; `*ITCase` runs in the integration-test execution. Credential-gated
  real-GCP tests also carry `@Tag("gated")` and are run only by `just e2e`.
- A test whose *duration is the instrument* — an elapsed-time observation that cannot be tuned
  down — carries `@Tag("slow")`, is excluded from ordinary builds, and runs in `weekly.yaml`'s
  lane. Ordinary `just verify` does not run it; `-Dtest.excluded.groups=gated` does.
- New `scripts/*.py` checkers require synthetic tests under `scripts/tests/`; do not make checker
  tests assert against the live repository tree.

## GitHub workflow

- Use one dedicated worktree per PR under `/tmp/worktrees/flink-connector-gcp/`; never switch the
  main checkout. All PRs are drafts and use `.github/PULL_REQUEST_TEMPLATE.md` with filled `WHAT`
  and `WHY` sections.
- Use `gh` for GitHub operations. Do not push directly to `main` or create an issue without the
  user's routing decision.
- Before every PR-branch push, use `$push-pr-branch`: fetch, rebase onto `origin/main`, squash only
  after rebasing, and inspect `git diff --diff-filter=D --name-only origin/main`. A PR closes its
  issue with an unformatted `Closes #N`, checked with `closingIssuesReferences`, never by eye.
- After creating the draft PR, run `$self-review`, apply verified findings, push fixes, and then run
  `$self-review-round-two` when its trigger applies. Record findings and reasoned deferrals on the
  PR. Re-run affected tests after review changes.
- Pin GitHub Actions with `just pin-actions` when adding a workflow or changing an action version.
- Write commit messages, PR text, issues, code comments, and javadoc in English.

## Tool routing

- Use Context7 for current third-party library/API documentation. Resolve the dependency version
  from this repository's POM/BOM first, ask a version-specific question, and treat community
  documentation as supporting evidence rather than a replacement for upstream behavior tests.
- Use Serena for Java symbol discovery, reference graphs, and symbol-aware refactoring. Activate
  the current worktree if it is not already active. Repository MCP configuration uses Serena's
  `no-memories` mode: `AGENTS.md`, references, docs, and ADRs remain the shared source of truth.
- Use `rg` and ordinary file tools for Markdown, YAML, configuration, and simple text searches.
- Do not use Serena's overlapping shell or memory features. Do not store credentials or private
  environment data in tracked MCP, agent, or Serena files.
