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
Claude Code project-memory index when it exists. Read only task-relevant entries, verify their
claims against current sources, and never copy local memory into tracked files without an explicit
privacy and staleness review. The skill resolves the main checkout's memory from a worktree.

## Commands

The `justfile` is the command index and CI calls the same recipes. Run `just --list` when unsure.
In a shell without mise activated, use `mise x -- just <recipe>`.

- `just format`: format code; run before committing.
- `just verify`: full Maven verification with formatting, checkstyle, unit tests, integration
  tests, packaging, and apache-rat. It requires JDK 17 and Docker but no GCP credentials.
- `just verify-module <module>`: verify a module and its reactor dependencies; keep its `-am`.
- `just verify-flink <version>`: verify another supported Flink version; clean when moving between
  Flink 1.x and 2.x.
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
  the appropriate Flink API annotation.
- Do not introduce Guava, Mockito, PowerMock, Lombok, or AutoValue. Use Flink preconditions,
  `javax.annotation`, and hand-written `Fake*` test doubles.
- A serializer returning `null` means skip, not failure. A wrapped Flink serializer returning
  `null` is a serialization failure. Read ADR-0001 before touching this contract.
- Forge options objects from `builder().build()`, never the process-wide `defaults()` singleton.
- Bound `Duration` values that will become nanoseconds at `Duration.ofNanos(Long.MAX_VALUE)`.
- Tests that call a production `createWriter` path must configure an emulator endpoint.
- `*Test` is a unit test; `*ITCase` runs in the integration-test execution. Credential-gated
  real-GCP tests also carry `@Tag("gated")` and are run only by `just e2e`.
- New `scripts/*.py` checkers require synthetic tests under `scripts/tests/`; do not make checker
  tests assert against the live repository tree.

## GitHub workflow

- Use one dedicated worktree per PR under `/tmp/worktrees/flink-connector-gcp/`; never switch the
  main checkout. All PRs are drafts and use `.github/PULL_REQUEST_TEMPLATE.md` with filled `WHAT`
  and `WHY` sections.
- Use `gh` for GitHub operations. Do not push directly to `main` or create an issue without the
  user's routing decision.
- Before every PR-branch push, use `$push-pr-branch`: fetch, rebase onto `origin/main`, squash only
  after rebasing, and inspect `git diff --diff-filter=D --name-only origin/main`.
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
