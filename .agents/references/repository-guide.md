# Detailed repository guide

On-demand guidance for Codex and Claude Code when working in this repository. The concise,
automatically loaded rules live in `AGENTS.md`; read the relevant sections here before changing
the build, CI, compatibility policy, packaging, documentation architecture, or infrastructure.

## Project overview

GCP connectors for Apache Flink: BigQuery, Cloud Pub/Sub, Cloud Tasks, Bigtable and Spanner.
Independent OSS project — not affiliated with the Apache Software Foundation or Google.
Maven multi-module build based on `org.apache.flink:flink-connector-parent`, with Google Cloud
dependencies managed through `com.google.cloud:libraries-bom`.

## Build

Commands live in the `justfile` and **CI calls the same recipes**, so what runs locally is what
runs in the workflows (#111). `just --list` is the index; run `mise x -- just <recipe>` in a shell
without mise activated. Add a command here rather than to a workflow `run:` block.

- `just verify` — full build (`./mvnw -ntp verify`): spotless/checkstyle (validate), unit tests,
  integration tests, apache-rat license-header check. Requires JDK 17; `mise.toml` pins java/maven
- `just format` — run before committing; CI fails on unformatted code
- `just verify-flink 2.3.0` / `just verify-module flink-connector-gcp-bigquery` — one Flink
  version, one module. A 1.x version also selects the `flink1` compat source root (see the
  version policy), which is why the recipe exists rather than passing `-Dflink.version` by hand. `just verify <maven args>` is the passthrough the weekly matrix uses, and
  passing nothing means the version pinned in the pom. **`verify-module` carries `-am`**, and it is
  load-bearing rather than tidiness: `-pl` alone resolves the `io.github.flink-gcp` siblings from
  `~/.m2`, so the recipe reports on whichever jar is installed there rather than on the working
  tree — which fails in *both* directions, a stale jar failing a green tree (`NoClassDefFoundError`
  on a test-utils class added by #323, met on #324) and a newer one passing a broken reactor
  change. Nothing in CI is affected either way, since CI builds the reactor. That is a different
  problem from the one `binary-compat` and `e2e` solve by installing first: theirs is that a
  goal-only or repeated-`-pl` invocation cannot span one reactor at all
- **A local build is only as honest as the local state**: a primed `~/.m2` (this project's own
  SNAPSHOTs from any `install`) and a stale `target/` make reactor, packaging and
  plugin-execution changes look green locally while CI — which starts clean — fails. Verify
  such changes against a cleared state first (`mv ~/.m2/repository/io/github/flink-gcp` aside,
  `clean` on the module), and after a rebase that pulls in a sibling module's change, reinstall
  the dependency modules before believing a `verify-module` result — the tell is failures
  concentrated in files the diff never touched. `just verify-flink 1.20.x` after a 2.x build
  needs a `clean` for the same reason: `target/classes` still holds the flink2
  `CrossVersionSink`, and the compile error reads exactly like a real 1.x incompatibility
- `just binary-compat 2.3.0` — the floor-build/install/fingerprint/ceiling-rerun/diff sequence
  behind the one-artifact claim (ADR-0053 records why its order and its install step are
  load-bearing). Reproducing a red weekly `binary_compat` is what it is for, and run by hand it
  primes `~/.m2` with `io.github.flink-gcp` SNAPSHOTs (the recipe comment has the cleanup line)
- `just e2e` — the ITCases gated on the `BQ_IT_*` / `PUBSUB_IT_PROJECT` / `BIGTABLE_IT_PROJECT` /
  `SPANNER_IT_PROJECT`
  variables, **and the only thing that runs them** (#245; ADR-0065 records the per-shell
  incident and the marker mechanics): each gated class also carries `@Tag("gated")`, which the
  root pom excludes from every surefire execution, and this recipe is the opt-in that clears
  it. Its pre-flight makes a missing variable an error, and a post-run assertion
  (`scripts/e2e-gated-its.sh`) checks the gated classes actually executed. Its `-pl`-scoped
  builds install the base and test-utils modules first, for the same reactor-resolution reason
  `binary-compat` installs (#27, #61). The weekly E2E workflow (`e2e.yaml`) runs this same
  recipe via WIF; locally the variables come from the uncommitted `.env`, which a fresh
  worktree does not have — run `just worktree-env` once there to symlink the main checkout's
  copy (#156; the same link also carries `just tofu`'s credentials)
- `just sweep-e2e [--dry-run]` — returns its three billed E2E fixture types to their idle state
  after a hard cancellation: stale Bigtable and Spanner instances are deleted, and the fixed Cloud
  Tasks App Engine version is stopped. It reads the owned identifiers and thresholds from source,
  treats an unreadable source or listing as an error, attempts every service independently and
  reports the worst status. The daily schedule, source-derived values, shell failure boundaries
  and billing-account boundary are recorded in ADR-0119
- `just check-notice <module>` / `just update-notice <module>` — a shaded module's
  `META-INF/NOTICE` is generated (prose from the module's `NOTICE.template`, artifact lists from
  what Maven resolves) and its `META-INF/licenses/` texts come from sha256-pinned sources in
  `scripts/config/licence-sources.toml`. A url source is either version-templated — `{version}` filled
  from the resolved bundle, so a dependency bump re-fetches the matching tag with no edit to the
  entry — or declared `version_independent = true` (#343); the toml header carries the scheme.
  `update-notice` regenerates after a dependency change; `check-notice` verifies offline in CI.
  Because the offline check never consults the recorded sources, `just check-notice-sources` —
  regenerate every shaded module with real fetches, then require no drift — runs from verify.yaml
  when the change touches a licence-source input (a pom, the pin file, a NOTICE; derived in
  `ci-maven-args.py`) and weekly otherwise, which is what catches an upstream moving under an
  unchanged repository. `check-notice` and `update-notice` take the module as an argument, which is what lets
  the two SQL uber-jars share them and what a third would reuse unchanged; verify.yaml runs
  `check-notice` over the shaded modules *in the built set* (ADR-0058). **Invoke the licence goal
  through a phase, never as a bare `license:add-third-party`**: a CLI goal invocation selects
  reactor modules without building them, so the module cannot resolve the connector it bundles —
  `-am` does not change that, and it only appears to work against a local repository some earlier
  `install` primed
- `just check-flink-api-tiers` — classifies every `org.apache.flink` type the main sources import
  by its class-level stability annotation, read from the `-sources.jar`s at the pom-pinned
  `flink.version` (never class files: their constant pool lists method-level annotations too,
  the #103 miscount). `@Internal`, `@Experimental` and unannotated types each need a reasoned
  allowlist entry in `scripts/config/flink-api-tiers.toml`; a new one — or a stale entry — fails.
  Runs as its own `verify.yaml` job, not in `lint.yaml` and not inside `just lint` (ADR-0058),
  and it downloads the sources jars (into `target/flink-api-tiers/`) while `just lint` stays
  offline — the rule that also put `check-skill-frontmatter` in `verify.yaml`
- `just check-option-docs` — holds the hand-written option reference to public builder setters and
  Table API keys in both directions. A first-column `Option` header opts a table in; module mappings
  catch new option classes, explicit `sources` reach exceptional builder names, `[exempt]` and
  `[extra]` point in opposite directions, and an entry that never fires fails. It compares names,
  not defaults or meanings, so those still move with the source change. Its own `verify.yaml` job
  is recorded in ADR-0058; the checker design is ADR-0116, and every failure routes through
  `.agents/skills/curate-option-docs/`
- `just check-metric-docs` — holds DataStream `Metric` tables to each connector's inventory in both
  directions, including registration kind, source-derived subgroup templates, guarded
  `(Flink standard)` rows and the mechanical `num`-prefix rule. It does not check Meaning cells,
  prose or the event/state naming judgment, so a rename still sweeps those by hand. Its own
  `verify.yaml` job is recorded in ADR-0058; the checker design is ADR-0117, and every failure
  routes through `.agents/skills/curate-metric-docs/`
- `just check-gated-tags` — the two markers a gated real-GCP ITCase carries have to stay together
  (#245; ADR-0065 records both failure directions): the `@EnabledIfEnvironmentVariable` the E2E
  suite is *discovered* by, and the `@Tag("gated")` that keeps the class out of every ordinary
  build. `scripts/e2e-gated-its.sh --check-tags`, deliberately **gate-agnostic** so
  `BigQueryDefaultStreamSchemaEvolutionITCase` is covered too. Its own `verify.yaml` job
  (ADR-0058; its inputs are the Java *test* sources), and it needs no JDK, no Python and no
  network. **The one checker with no `curate-*` skill**, and the exemption is argued rather than
  an oversight: those skills exist for allowlist judgment — which entry, with what reason — and
  this check has no allowlist and exactly two mechanical fixes, both named in the failure message
- `just ci-maven-args` — CI's module-selection decision (#243; ADR-0058 carries the design):
  which Maven modules does a change build? The mapping is derived from the poms, never
  configured — the script's docstring is the specification.
  `just ci-maven-args --diff origin/main` reproduces by hand what a pull request with the
  current branch's committed diff would build
- `just test-scripts` — runs `scripts/tests` through the root non-package uv project and committed
  lockfile. Checker tests build synthetic trees and pin the false-green direction with a case that
  fails when each rule is removed; `test_ci_maven_args.py`'s real-repository CLI layer is the named
  exception whose inputs appear in `lint.yaml`. A new Python checker owes tests here and a
  curate-* skill only when its failures require judgment. ADR-0118 records the project shape,
  fixture boundary and mutation evidence
- `just check-java-license-headers` — every Java source starts with a complete copyright-bearing
  or ASF Apache-2.0 header. RAT identifies the licence family from a distinctive line and remains
  the whole-tree coverage; this check closes the narrower completeness gap with no allowlist or
  judgment. It runs at the start of `just verify`, and its parser tests use synthetic trees
- `just lint` — shellcheck over `scripts/*.sh`, ruff over `scripts/` (check *and* format), actionlint
  over `.github/workflows/`, markdownlint (markdownlint-cli2, pinned via mise's npm backend) over
  the **rendered** markdown — `docs/content/`, `docs/adr/` and the READMEs, never agent
  guidance or its detailed references — at strict defaults except MD013 and MD060, both declined with reasons in
  `.markdownlint-cli2.jsonc`. MD051's in-page anchor check is the half Hugo's build does not
  cover (`relref` validates cross-page links only), and a *cross-page* link carrying a
  `#fragment` is checked by **neither** — it can point at nothing while both stay green, which
  is why #90 resolved them by hand against the built `docs/public` and deferred a checker until
  the pages actually rot. Also `tofu fmt -check` over `opentofu/` (`tofu validate` is
  deliberately absent: every PR touching `opentofu/` gets a full plan, which subsumes it).
  Deliberately does **not** run `just --fmt --check` — an unstable feature, excluded from
  just's compatibility guarantee (ADR-0057). actionlint is handed
  `-shellcheck "$(mise which shellcheck)"` rather than letting it find the runner image's own.
  It does **not** run `just check-skill-frontmatter`, which downloads and therefore has its own
  `verify.yaml` job (below) — the recipe body is the list above and nothing else
- `just check-skill-frontmatter` — is every `.agents/skills/*/SKILL.md` frontmatter strict YAML?
  **A house style, not Claude Code's requirement, and the difference is measured**: the unquoted
  `description:` carrying a `: ` that PyYAML rejects outright was loaded by Claude Code with its
  description intact, so its reader is the more tolerant of the two and this check cannot tell you
  what it would refuse (ADR-0069). What it buys is that a file every tool agrees about cannot fail
  silently — the file stays valid markdown, no build step reads it, markdownlint's globs exclude
  it, and a skill that did not load looks like Claude choosing not to use it. Parses with
  **PyYAML** rather than a hand-rolled approximation (a second, diverging parser is the failure
  this repository has paid for elsewhere), declared in the script's own **PEP 723** header, so the
  uv project takes no *runtime* dependency and this one script runs as
  `uv run --no-project scripts/…` (pyyaml is in the dev group all the same, because
  `just test-scripts` loads the script by file path). **Where the block ends is measured too**
  (#388): Claude Code closes the frontmatter at the first `---` *anywhere* after the opening line,
  not at the first `---` line — a `----` rule, a `--- text` line and a `---` inside a
  `description:` all close it, the last of them truncating the description while the skill still
  loads — so the script delimits that way and reports a close that is not alone on its line. Only
  a body rule of exactly `---` still passes for a deleted closing delimiter, and there the loader
  agrees with it. Also checks
  name-matches-directory, a non-empty description, no duplicate key and a skill directory whose
  `SKILL.md` is gone; no allowlist, so no curate-* skill. **Its own `verify.yaml` job, not part of
  `just lint`** — because it downloads, and `just lint` stays offline; that is
  `check-flink-api-tiers`'s rule applied rather than excepted
- `just docs` / `just docs-serve` / `just docs-chroma` — build the site as CI does (a deprecation,
  a broken `relref` or a missing shortcode fails the build), preview it, regenerate the chroma
  palettes. `mise.toml` pins hugo-extended and Go; hugo-book is a Hugo module pinned in
  `docs/go.mod`. These build the hand-written pages only; the generated half is
  `just docs-javadoc` (below), which the docs workflow runs first
- `just docs-javadoc` — the aggregated JavaDoc that ships as the site's API reference (#88;
  ADR-0056), into `docs/static/api/java`, which Hugo copies verbatim (gitignored, rat-excluded).
  The one correct bare goal in this repository; the exemption from the licence-goal rule above
  is argued, and measured, in the justfile
- `just pin-actions` — pin GitHub Actions to commit SHAs; when to run it is a Workflow rule
  (a workflow added, or an action version changed)
- `just tofu <args>` — OpenTofu in `opentofu/flink-gcp`, the root module holding the project's
  persistent GCP resources (#5). Local escape hatch only: plan/apply normally run in CI (see
  "Infrastructure (OpenTofu)" below). Credentials come from
  `GOOGLE_APPLICATION_CREDENTIALS` in the uncommitted `.env` — the google provider does not read
  `CLOUDSDK_CONFIG` (only the gcloud CLI does; see `opentofu/README.md`)
- Recipe bodies stay one command per line — no embedded `#!/usr/bin/env bash` blocks. A single
  compound command is fine (`binary-compat`'s final `diff … || { …; exit 1; }`); a multi-line
  script block is not. The boundary is shellcheck coverage: it reads `scripts/`, actionlint reads
  inline `run:` blocks, and nothing reads inside a recipe
- **Inside a recipe, always name the tool: `mise x <tool> -- …`, never bare `mise x -- …`** —
  the bare form activates every tool in `mise.toml` and installs what is missing, silently
  undoing a CI job's `install_args` (the PR #113 incident; ADR-0057). The bare form stays right
  for the *entrypoint* (`mise x -- just <recipe>`), which does want everything
- A top-level justfile variable assigned from a shell command runs on **every** `just` invocation,
  whichever recipe was asked for; a default *parameter* value runs only when its own recipe does.
  That is why `check-flink-release`'s ceiling is a parameter default rather than a variable

## Documentation (`docs/` vs module READMEs)

- `docs/content/docs/connectors/datastream/<connector>.md` is **the design record**: API notes,
  design decisions, delivery guarantees, error handling and the testing strategy. Behavior or
  public API changed → update the docs page, not the README
- `docs/content/docs/reference/<connector>.md` is **the option tables**, and the only place they
  live (#89): the design record states *why* a default is what it is and links here for *what* it
  is, so a knob's name and value appear exactly once. Adding or renaming an option means editing
  this page in the same change — `just check-option-docs` fails otherwise, in both directions.
  `docs/content/docs/{quickstart,examples}/<connector>.md` (#90) are the other two per-connector
  pages: one runnable job each, and the worked cases the design record describes but does not show
- The module `README.md` is an **overview only**: title, one-paragraph description, the
  feature-status table (`Implemented (#N)` / `Planned (#N)` / `Declined (#N)` — the third for a
  feature measured and rejected, whose reasoning is the ADR the docs page links), a minimal code sample, a link to the
  docs page, and the **provenance/attribution section** — provenance pairs with `NOTICE` and is a
  licensing obligation, so it stays in the repository
- Implementation status lives in the README table only; the docs page links to it instead of
  repeating it. Keep the two from drifting by adding status nowhere else
- **Issue references in module READMEs and docs pages are explicit links**, never bare `#N`.
  GitHub autolinks `#N` only in issue/PR *comments*, not in repository markdown files, and Hugo
  never does — so a bare `#N` is dead text in both places a reader actually sees. READMEs use the
  full URL; docs pages use `[#N]({{< param BookRepo >}}/issues/N)`. The `#N` link text must match
  the `/issues/N` in the URL it wraps — a copy-paste where they disagree sends the reader to the
  wrong record, and nothing renders differently to reveal it. Cross-repository references
  keep their `owner/repo#N` text and point at *that* repository — `goccy/bigquery-emulator#342`
  is the one in the tree, and a blind `#N` rewrite would have pointed it here. Agent guidance is
  deliberately exempt: it is read by coding agents, not rendered for users
- **A status claim in a rendered file carries the issue link that *is* the status.** Wording
  like "under investigation", "planned" or "not yet supported" rots silently when the tracker
  moves on, so it may appear only beside the issue link whose open/closed state lets a reader
  check it — and prefer recording the decision plus the protocol over the status word ("closed
  wait-and-see; a reproduction gets a new issue referencing #174" outlives "under
  investigation"). Closing or re-scoping an issue includes rewording its rendered mentions in
  the same change — #186, sweeping the mentions after #174 closed, is the precedent
- **Comments and javadoc state what the code cannot show** — a measured fact, a decision with
  its issue number, or the vendor behavior being worked around — never narration of the change
  that introduced them ("now", "new", "previously", "fixed in this PR"), which is stale the
  moment it merges and meaningless to a reader who wasn't in the session that wrote it, and
  never implementation status, whose single home is the README table. Wall-clock or measured
  numbers carry their date and sample size ("measured 2026-07-31, one run") so a later reader
  can weigh them, and a superseding measurement edits the original spot rather than appending a
  correction beside it
- Pages are plain markdown with front matter (`title`, `type: docs`, `weight` — spaced by 10 so a
  new connector slots in without renumbering) and the plain Apache-2.0 header as an HTML comment.
  **No Flink shortcodes and no vendored Flink layout code** — `artifact`/`tabs`/`hint` do not
  exist here, and staying clear of them is why `NOTICE` needs no entry. Hugo's own built-ins
  (`relref`, `param`) are fine; prefer `{{< param BookRepo >}}` over hardcoding the repository URL
- Syntax highlighting is class-based (`markup.highlight.noClasses = false`) with the palettes
  selected by `prefers-color-scheme` in `docs/assets/_custom.scss`, which hugo-book bundles into
  its own stylesheet. Regenerate the palettes with `just docs-chroma`, which is where the two
  `hugo gen chromastyles` style names live (verbatim output; apache-rat excludes them)
- **New or materially changed runnable Java guidance opts into one compiled source with
  `java-snippet`** (#658; ADR-0115). The source lives under
  `flink-connector-gcp-docs-validation/src/test/java`, the shortcode renders the exact tagged
  region with its common leading indentation removed, and `just check-doc-snippets` compiles it
  against the current reactor. Ordinary fenced Java blocks are deliberately outside that check:
  use them for partial or pseudocode fragments and make that abbreviation clear in the surrounding
  prose. Existing ordinary fences remain unvalidated until they are migrated. Use
  `.agents/skills/maintain-doc-java-snippets/` when adding, updating or repairing either form.
  `just test-java-snippet-shortcode` mounts the repository shortcode into a synthetic Hugo site
  under `docs/tests/fixtures/` and holds its rendering and error branches to fixture pages and Java
  sources instead of the live examples
- **Every public Javadoc `<pre>{@code ...}</pre>` block is classified and synchronized** (#694).
  Runnable examples map to one exact tagged Java region under the docs-validation module;
  intentionally partial examples carry a visible `Abbreviated, not compiled:` label and a concrete
  reason. `just check-doc-snippets` checks the two-way inventory before compiling the backing
  sources against the current reactor. Use `.agents/skills/maintain-javadoc-examples/` when adding,
  updating or repairing either form
- The site is built as a CI check only; GitHub Pages publishing waits until the repository is
  public (#6). Each module README links to its docs page by in-repo relative path — those links
  become site URLs when Pages goes live, which is a checklist item on #6
- **The API reference is the site's generated half** (#88; ADR-0056): `just docs-javadoc`
  aggregates JavaDoc across every module into `docs/static/api/java`, never committed; pages
  link to it with `{{< param ApiDocsURL >}}`, a param and not `Book*`-prefixed on purpose.
  Nothing is filtered by API tier, doclint stays off with `failOnWarnings` on and nothing
  fetches a remote link index, and there is one unversioned path tracking `main` — the first
  two measured, all three argued in the ADR
- A module `AGENTS.md` plus its `.agents/references/modules/` detail is the third document in this
  split and the only **agent-facing** one — never rendered, never linked from the site, so
  nothing user-facing belongs in it. It carries that module's design decisions and nothing else; behavior and public
  API still go to the docs page, status still goes to the README table. Being unrendered, it keeps
  bare `#N` references under the same exemption the root `AGENTS.md` already has
- **`docs/adr/` is the decision archive**, and `docs/adr/README.md` carries the whole scheme
  (ADR-0000): the three-home boundary, the write trigger, cluster granularity, the two statuses,
  the refinement lifecycle, and the index that allocates the next number. The operative rules:
  **an ADR is owed exactly where a decision record would otherwise be owed to agent guidance,
  README or docs page** — the self-review question to ask of every diff; design discussion stays
  on the issue (`Design (settled YYYY-MM-DD)`, which may just say "Settled — see ADR-NNNN"); the
  implementing PR carries the ADR in its diff; a refinement edits the ADR in place, a reversal
  adds a new ADR and flips the old one's status — "do not silently revisit" means engage the
  record, not keep it forever. And **a repository-relevant decision or measured fact is recorded
  in the repository — an ADR, a docs page, or a rule here — never only in an agent's private
  session memory**: memory keeps user preferences, session workflow and pointers; anything a
  future maintainer or session would need belongs where the repository's readers can reach it

## Workflow rules

- **One git worktree per PR** under `/tmp/worktrees/flink-connector-gcp/`; never switch branches
  in the main checkout. Remove the worktree and local branch after merge. If the branch needs the
  real-GCP ITs or `just tofu`, run `just worktree-env` once in the fresh worktree — it symlinks
  the main checkout's uncommitted `.env` (#156)
- All changes go through **draft PRs**; nothing is pushed directly to `main` after the initial
  skeleton
- **A description follows `.github/PULL_REQUEST_TEMPLATE.md`: `## WHAT` and `## WHY`, both
  filled** (#257). WHY is the half that does not survive in git — a diff shows what changed and
  never why it was worth changing — so a description that only restates the diff is incomplete
  even when it is long. The template is a floor, not a cap: everything this project already
  expects of a description goes **under** those headings — the verification list, the mutant
  table a change with new tests carries, the decisions weighed and declined. Squash before merge
  (the review fix-ups are session bookkeeping), so the merged commit message carries the same
  WHAT/WHY and the per-round detail stays in the PR comments
- **Every push of a PR branch goes through `.agents/skills/push-pr-branch/`** — the first one, each
  review round's fix-ups, and the final one. Squashing with `git reset --soft origin/main` on a
  branch that has not been rebased builds a commit whose parent is main's tip and whose tree is the
  branch's older one, silently reverting everything main gained meanwhile. **Rebase first, then
  squash**, and read `git diff --diff-filter=D --name-only origin/main` — a list, not the
  diffstat's number — before every push. Nothing downstream reports this: CI passes on a consistent
  revert, "require branches to be up to date" is satisfied by the reverting commit itself, and
  GitHub's "Update branch" cements the deletions rather than undoing them (`docs/adr/0069`)
- **After creating a draft PR, always self-review it** — applying simplification and efficiency
  findings, not only correctness ones — and push the fixes before asking for review. Record the
  findings *and the deferrals, with their reasons* as a PR comment; recording is not routing, which
  the bullet below governs. **How the round is run is `.agents/skills/self-review/`**, and round
  two is `.agents/skills/self-review-round-two/` — the two skills carry the lenses, the
  verify-before-acting rule and the recording format, so neither round depends on a command Claude
  cannot start:
  - **`/code-review` and its alias `/review` are user-invocable only.** Both are marked
    `disable-model-invocation`, which is the documented design and not a local setting: they
    cannot be scheduled either, and a scheduled task naming one reads it as plain text. Ask the
    user to run `/code-review` when the built-in's second opinion is wanted; never wait for it in
    place of the skills. (Before Claude Code v2.1.223 `/review` was a separate command that ran a
    single-pass, read-only review of a GitHub pull request, and this rule described it as one
    Claude could start — hence the correction.)
  - The lenses stay *distinct* — correctness and concurrency, public API and simplification, test
    quality and flakiness — because one pass asked for "a review" returns much less than three
    asked for different things. Run them as parallel subagents where the session permits the Agent
    tool and sequentially where it does not, say which was done, and verify each finding against
    the code before acting on it
- **Then run a second round with different lenses, and do it before saying the PR is ready** —
  `.agents/skills/self-review-round-two/`, which is where its procedure lives (ADR-0060 carries
  the measurement that pinned this). The rounds answer different questions:
  round one asks whether the code does what the description says, **round two asks whether the
  description is true**, with lenses pointed outward rather than at the diff — the user meeting
  the error, the operator reading the dashboard, the blast radius of a move, an adversary trying
  to defeat the invariant. Three rules the rounds share: **converging agents are one source**
  (verify against the *pinned* dependency version); **a deferred measurement is a claim** round
  two must pay; and **re-run the mutation batch after acting on a review**, not only before. The
  cost is real, so the full second round is for changes whose description makes claims about
  framework behaviour, deployment, or "this is the only way" — not for a typo fix.
- **`git commit` is the first line of every mutation batch** — before the first mutant, with no
  exceptions for "it's only one command" or "it's only comments": an inline `git checkout --`
  restore has destroyed uncommitted work on seven occasions, and prose-only edits make the loss
  invisible because every test and checker stays green after the revert. And a mutant that fails
  to compile, fails spotless, or runs zero tests is **not** a kill — run batches with the format
  checks skipped, verify each mutation actually landed (a substitution whose pattern missed
  exits 0), and require a *named* failing test in the output before recording a kill.
- **A finding outside the issue being worked is routed by the user, not by the note written
  about it — and `gh issue create` is never how that routing happens** (ADR-0061 carries the
  four constraints with their incidents). Three outcomes, not two: folded into the current
  change, filed as an issue, or **dropped** — and file only what the user has said is not being
  folded in. Verify a finding before routing it, route every finding, never reopen a decision
  the user has already given, never batch-file at the end of a review. A deferral left in a PR
  comment or an agent-guidance "known gap" line is the silent deferral wearing a disguise; a filed
  issue states the **grounded** reason the work is not being done now — never "not planned" or
  "out of scope" — and carries the better approach or a measure-first step where one exists.
- Pin GitHub Actions to commit SHAs with `just pin-actions` whenever a workflow is added or an
  action version changes
- Commit messages, PR titles/descriptions, code comments, javadoc and issues are written in
  English
- **An issue normally carries three things: a milestone, a `priority:` label, and a `module:` or
  `area:` label** (ADR-0062 carries the taxonomy). Milestones are releases, `v0.3.0` through
  `v1.0.0` (going public); read each milestone's description for its theme rather than guessing
  from the number. Priority is ordered by *what breaks*, not by urgency — `P0` a shipped path
  breaking silently, down to `P3` future or externally blocked — and is orthogonal to the
  milestone where one applies. Externally blocked work has two milestone exceptions. An issue
  that cannot be completed until another project changes or publishes a release retains its
  `module:` or `area:` label, carries `priority:P3` and `status:blocked-upstream`, but has no
  milestone; remove the status and assign a release milestone before implementation starts once
  upstream clears. An issue whose remaining work requires a contract, paid environment, or
  representative workload the maintainer cannot access retains its ownership labels, carries
  `priority:P3` and `help wanted`, but has no milestone when an external contributor must supply
  that access or evidence. `help wanted` alone does not qualify: name the unavailable capability
  and what the contributor must provide, then assign a release milestone once the environment
  and contributor support are available. A temporary external service failure, a resource the
  maintainer can provision, or a future scheduling choice does not qualify for either exception.
  GitHub sub-issues only where a parent genuinely decomposes; a *cluster* sharing one root cause
  is recorded as a comment on the one to work instead, so each keeps its own closure record
- PRs close their issue with `Closes #N`, written **unformatted** — a closing keyword inside a
  code span is not parsed, the issue silently survives the merge, and nothing reports it. The
  inverse also bites: GitHub parses a closing keyword out of ordinary prose, so a body sentence
  like "the follow-up PR, which will close #361" closes the issue on merge (PR #389 did exactly
  that). Name an issue a PR must *not* close without a closing verb directly before `#N`. Check
  `closingIssuesReferences` rather than by eye (`gh api graphql -f
  query='{repository(owner:"laughingman7743",name:"flink-connector-gcp"){pullRequest(number:N)
  {closingIssuesReferences(first:5){nodes{number}}}}}'`)

## Infrastructure (OpenTofu, `opentofu/`)

Migrated to ADR (`docs/adr/0063`, which carries the design, the incidents and the service-agent
facts); the rules a session needs:

- `opentofu/flink-gcp` is the single root module for the project's **persistent** GCP resources
  (#5). Fine-grained test resources are created by the tests themselves and never belong here;
  a new connector's API and E2E grants are added in the PR that first needs them, not in advance
- CI is **tfaction v2**: pull requests touching `opentofu/**` get a plan comment, the merge
  applies that reviewed plan file from GitHub Artifacts. The standing exception to the
  just-recipe rule; `just tofu <args>` is the local equivalent. **The apply workflow must set
  `TFACTION_IS_APPLY: "true"`** — without it, setup silently falls back to the read-only plan
  account, so on any tofu-CI 403 **read the auth step's log first**. **A failed apply is
  recovered by a follow-up pull request**, never by re-running the apply job (the failure makes
  the saved plan stale) — and assume nothing from the failed apply exists until measured
- **No service account keys, ever.** All CI credentials are short-lived WIF tokens, with
  per-account bindings restricting what each workflow identity can do; plan runs read-only.
  Local runs authenticate via `GOOGLE_APPLICATION_CREDENTIALS` from the uncommitted `.env` —
  the google provider does not read `CLOUDSDK_CONFIG` (only the gcloud CLI does)
- The tofu version is pinned twice on purpose: `mise.toml` (what installs) and
  `versions.tf` `required_version` (what refuses to run on a skew) — a bump edits both

## Version policy

- Releases follow full semver (`v0.1.0`, `v0.2.0`, ...). Early milestones are **tags only** — no
  artifact publishing. Publishing to Maven Central happens once all connectors are implemented,
  as `v1.0.0` (Central namespace registration, signing and the Flink 1.x/2.x publishing strategy
  are decided then; see issues #29 and #39)
- **`flink-connector-gcp-docs-validation` is never published.** It remains outside the ordinary
  module list behind the `docs-snippets` profile and configures its module-local deploy plugin to
  skip deployment; it is build-time documentation validation, not one of the connector artifacts
  released to Maven Central
- `main` supports **the current and previous Flink minor** — today **2.2 and 2.3**, with
  `flink.version` pinned to the floor (`2.2.1`) — and **one artifact covers the range**, a
  claim the weekly `binary_compat` job measures rather than assumes (ADR-0053, which also
  carries the matrix design and the per-minor-artifact fallback). A new Flink minor moves both
  ends **deliberately**, never via a dependabot minor bump (an `ignore` rule suppresses those;
  patch bumps still arrive): `scripts/check-flink-release.sh` announces the release and prints
  the edit list in its failure output — do not hand-maintain that list anywhere else — and the
  new range is not claimed until `just binary-compat <new ceiling>` has been re-run
- **Flink 1.20 (1.x LTS) is supported from this same source, at source level** (ADR-0054): the
  `flink.compat` property selects the compat source root
  (`src/main/java-flink1`/`java-flink2`), `just verify-flink 1.20.x` selects it locally, the
  weekly `lts` row verifies it, and no cross-major binary claim is made — the one-artifact
  claim spans the 2.x range only. A 1.20 patch bump is a hand edit to `FLINK_LTS` in
  `weekly.yaml` (dependabot does not see workflow env). **A cross-major API difference goes in
  the roots, and both known ones are there** — every connector's `CrossVersionSink`, and
  BigQuery's `CrossVersionCheckpointId` (#404), which is what keeps a Flink method
  `@Deprecated(forRemoval = true)` on the moving major out of shared source. Two things the
  second one shows that the first did not: a compat file need not be compile-only, and need
  not sit at a module's `sink` root. Switching `flink.compat` between local runs needs a
  `clean` for each such class, not just for `CrossVersionSink` — an incremental build reuses
  the other major's `.class`, and a *restored* source then reads green over stale bytecode
- The version matrix lives in `weekly.yaml`, not `verify.yaml` — per-PR CI stays single-version
  for latency. Rows carry a **role** (`floor` / `ceiling` / `next` / `lts`) resolved from the
  `FLINK_*` envs at the top of the file, and every matrix job checks out `github.sha`; the
  whys, including why the `floor` row passes no `-Dflink.version`, are in ADR-0053
- JUnit stays on 5.x and testcontainers on 1.x for now; their major-version dependabot PRs are
  intentionally left open/deferred
- Google Cloud library versions come only from `libraries-bom`; never pin individual
  google-cloud artifact versions

## CI architecture

Migrated to ADRs (`docs/adr/0057`–`0059`); the rules a session needs:

- **A multi-step sequence is named once, in the `justfile`, and CI calls that recipe** (#111;
  ADR-0057). CI helpers live in `scripts/` as files — rat-headed, shellchecked, runnable by
  hand, which is the first move on any red CI job that has a recipe — while `tools/` holds
  build tool *configuration* and nothing lints inside a recipe body
- **Where a tool's version lives decides how CI installs it** (ADR-0057): pin in `mise.toml`
  and install with `jdx/mise-action` + `install_args` naming the job's subset when a version
  skew could fail an unchanged pull request; otherwise `taiki-e/install-action` unversioned —
  that is `just`, whose 1.x guarantee covers **stable features only**, so nothing CI runs may
  depend on an `--unstable` one. `jdx/mise-action` beside `setup-java` needs
  `add_shims_to_path: false` — a default to disarm, not a combination to forbid — and better
  still, arrange not to need it: a tool the Java build depends on comes from Maven where it can
  (protoc does, which is why #132 touched no CI)
- **`verify.yaml` selects what a pull request builds instead of filtering whether it runs**
  (#243; ADR-0058 carries the whole design): a required check that never reports blocks a pull
  request forever, so a `changes` job derives the Maven `-pl` subset via
  `scripts/ci-maven-args.py` instead, a root-only change (`docs/**`, `scripts/**`, the root uv
  project — minus the two licence-pin files ADR-0058 deliberately keeps out of the class)
  builds `-pl .` alone, and a real `paths-ignore` survives on the **push** trigger only. `lint.yaml` is where linters Maven does not run live, a workflow of its own purely for
  latency; a push-side paths filter must list **every input to a lint, not just the linted
  files** (`mise.toml` pins the linters, so it is on every such list)
- **`ci.yaml` is the pull-request orchestrator, and branch protection requires exactly its one
  gate, `CI passed`** (#250; ADR-0059): the children run as reusable workflows, the gate
  `needs` them all and derives its verdict through `scripts/ci-gate.py` (children with a
  legitimately skippable job carry an internal verdict job with `SKIPPED_OK`), so a job or
  workflow enrolls in `ci.yaml` or its child alone and touches no repository setting — except a
  rename of `CI passed` itself, which must update branch protection in the same change. Two
  things about `ci.yaml` do reach outside it, both because the tofu plan runs as one of its jobs
  and the plan artifact therefore belongs to *its* run (#444; ADR-0063): its **file name** is
  what `tfaction-root.yaml`'s `plan_workflow_name` must hold, and its **trigger list** must stay
  `pull_request`-only, since tfaction takes the newest run on the head branch with no event
  filter. When a fresh PR shows "no checks reported", run `gh pr view <n> --json mergeable`
  **first**: a `CONFLICTING` pull request triggers zero `pull_request` runs at all

## Licensing and provenance

- Files written for this project carry the plain Apache-2.0 header
  (`Copyright 2026 laughingman7743`). Files copied from Apache projects keep their ASF header.
  Apache RAT enforces the approved licence families over the whole tree (configuration overridden
  in the root POM; new unheaderable file types need a RAT exclude there), but its matcher identifies
  a family from one distinctive line and therefore cannot prove that the surrounding notice is
  complete. `just check-java-license-headers` closes that gap for Java by requiring the full
  copyright-bearing or ASF form before Maven runs. Still not checked for non-Java files: the contents of the
  copyright line, and which of the two headers a given file carries. A third header would need a
  deliberate checker and RAT change
- When adapting Apache-2.0 code from other projects (Beam, Dataproc connector,
  google/flink-connector-gcp, java-bigquerystorage, apache/flink-connector-gcp-pubsub):
  record the provenance in the module README and the repository `NOTICE`, and keep original
  headers where applicable. Keep each module README's "no code copied" claim accurate
- **This project is Apache-2.0 with no usage restrictions, and its dependencies must be too.**
  A library under a restrictive licence — the GPL family, or the newer source-available and
  non-commercial ones (SSPL, BUSL, Commons Clause, …) — is normally rejected outright rather than
  recorded in a NOTICE; adoption of one would be a project discussion, not a licensing entry.
  `scripts/check-notice.py` enforces this for the shaded modules (decided with the user on #138),
  with **no exemption list** — a dual licence offering a permissive arm is not an exception, since
  taking that arm elects it on this project's behalf and has to be stated in the NOTICE. The one
  such artifact the bundles carried, `javax.annotation-api`, was measured to be referenced by
  nothing and excluded rather than elected (#352; ADR-0015). Note what the gate does and does not
  catch: it matches the *resolved licence name*, so the same artifact arriving under a plain
  `CDDL 1.0` spelling would pass it and be caught only structurally, by having no template
  paragraph
- Never open or reference the private in-house implementation this project supersedes; design
  references must be public OSS or official documentation only

## Package layout convention (all connector modules)

Under `io.github.flink.gcp.connector.<product>` — migrated to ADR (`docs/adr/0055`, which
carries the full skeleton, the evidence and the declined alternatives). The rules:

- Public API lives at a package's root, implementation in subpackages beneath it; test sources
  mirror the main-tree packages — the **one** exception being a helper that must declare a
  vendor's package to reach it (`docs/adr/0067`, and read it before adding a second). The `sink`
  root holds public sink API plus the `@Internal`
  types shared by every write method, and a new top-level class there needs a reason to be
  public API. The standing exceptions beside the facade: a single-family module's `@Internal`
  `Sink` class, and the `CrossVersionSink` seam in the per-major source roots (ADR-0054)
- `sink.<writepath>` is one subpackage per write-path **family** (internal stages as `.writer`,
  `.committer`, … per the FileSink precedent), and **one family, with no second one in
  prospect, means no layer** (#119) — the module goes straight to `sink` + `sink.writer`. The
  rule is a test, not a count
- A family layer is spelled the way Google spells it in code, with no `api` suffix (#121), and
  an SPI's real implementation is named after **the SDK resource its `close()` releases**
- Serializer input formats are subpackages of the SPI (`sink.serializer.<format>`, #125) and
  must not import each other — cross-format javadoc references are fully-qualified `{@link}`s
- A connector's failure type sits at the `sink` root — a one-class `sink.failure` fails the
  layer test (BigQuery's `sink.failure` is the grandfathered exception, kept for churn cost)
- The **module root** holds what belongs to the connector as a whole rather than to one
  direction: the `@Internal` `<Product>MetricNames` inventory every connector carries (#280),
  Pub/Sub's `@Internal` `PubSubShutdownResidue`, and Bigtable's `@PublicEvolving`
  `TableDestination`

## Emulators are conveniences, not authorities

An emulator is a convenience for fast feedback, never evidence about the service's behaviour.
Where the two disagree, the real service decides, and the emulator gets a documented workaround
naming the deviation. A mapping or behaviour decision may not be settled on emulator evidence
alone (#156). The record behind the rule: the goccy/bigquery-emulator divergence table — including
the one that nearly cost a correct mapping — is on #156, and the Pub/Sub emulator's blind spots
(ordered dispatch, ordered seek, dead-letter forwarding, IAM, and every create-option knob it
stores but ignores) are why the #82 real-GCP gated suite is the only coverage of those behaviours.

## Design decisions (do not silently revisit)

Per-connector design decisions live in module-scoped `AGENTS.md` files and their detailed
references, which load or are routed when an agent touches that module. **Read both before changing a module's behavior or
public API — and before answering a design question about it**, which is the case the on-demand
load does not cover: a session that never opens a file in the module never sees its decisions, and
reasoning about a module without them is how a settled decision gets re-argued. The topics below
are the trigger; they are not a summary, and none of them is safe to answer from:

- `.agents/references/modules/flink-connector-gcp-bigquery.md` — facade and serializer SPI, error handling (#13),
  FILE_LOADS (#14), its streaming form (#69) and its live-table reconciliation (#142),
  STORAGE_API_EXACTLY_ONCE (#30), per-write-method
  option scoping, JSON columns (#49/#50), geography columns (#126), Avro and JSON serializers (#66),
  column modes (#124/#145), protobuf well-known types (#147), default-stream tuning knobs
  and the connection-pool guard (#54), Table API/SQL (#57, split into #287–#290) and its shaded
  uber-jar (#290), deferred `location()` (#10). Migrated to ADRs (`docs/adr/0016`–`0035`)
- `.agents/references/modules/flink-connector-gcp-pubsub.md` — vendoring provenance (#17/#31), sink (#18), topic
  auto-creation (#19), tuning (#20) and in-flight bounds (#85), ordering×repair (#78), emulator
  (#21), source (#79/#80), Table API/SQL (#47, split into #135–#138) and the shaded uber-jar
  (#138). Migrated to ADRs (`docs/adr/0004`–`0015`); the **general** shading decisions every
  later `flink-sql-connector-gcp-*` inherits are `docs/adr/0015` — read it before adding a
  third; what is specific to a tree (an artifact kept out of the bundle, a relocation only it
  needs) belongs beside that connector, as #290's does
- `.agents/references/modules/flink-connector-gcp-cloudtasks.md` — sink design (#23) and implementation (#24).
  Migrated to ADRs (`docs/adr/0048`–`0049`)
- `.agents/references/modules/flink-connector-gcp-bigtable.md` — sink design and implementation (#33): implement rather
  than adopt or vendor, the four SDK facts the writer rests on (including the client's own blocking
  flow controller and two Google-internal annotations accepted deliberately), the row-level vs fatal
  boundary — `INVALID_ARGUMENT` alone, settled on gRPC's definition plus AIP-194 rather than left
  unmeasured — one fixed table per sink, and why the send metrics landed here rather than with #37.
  Also table and column-family auto-creation (#233): the parked-`NOT_FOUND` repair, its two-queue
  ordering rules, the schema-not-flag opt-in and the add-only ensure. Also the E2E enablement
  (#218): the ephemeral per-class instance and its sweep, what real Bigtable
  answers each rejection with, and the emulator deviation table those measurements produced.
  Migrated to ADRs (`docs/adr/0041`–`0047`, `0073`)
- `.agents/references/modules/flink-connector-gcp-spanner.md` — the Mutation-based at-least-once sink (#220): why
  `batchWriteAtLeastOnce` rather than a commit, the retry loop the connector owns because the
  client library retries this RPC not at all, index-aware mutation-cell weights read from
  `INFORMATION_SCHEMA`, which statuses are routed and which are retried, and both dialects. Also
  the bounded batch source (#221): one server-planned partition per split, a partition as the unit
  of progress, and the two option families assembled separately. Also the gated real-GCP suite
  (#224): the ephemeral 100-processing-unit `STANDARD` instance per gated class and why that
  edition, what the service turned out to answer where only the emulator had been asked, and the
  measured 100 MiB batch-write request ceiling (#441).
  Recorded in ADRs (`docs/adr/0075`–`0077`, `0085`, `0088`)
- `.agents/references/modules/flink-connector-gcp-test-utils.md` — the shared test-utils module (#27): test-support
  code only (main-code sharing belongs in `flink-connector-gcp-base`), all-provided dependencies,
  no forced unification of emulator container fixtures, and the justfile install-list coupling its
  reactor-sibling consumers create. Migrated to ADRs (`docs/adr/0050`–`0051`)
- `.agents/references/modules/flink-connector-gcp-base.md` — the shared main-code module (#61, joined by #37's
  DLQ/metrics as `base.failure`/`base.metrics`, by `base.lifecycle`/`base.rpc`, and by #452's
  `base.source`): the failure SPI and metric-name conventions (#280), retry loops and
  retryability classification stay per-connector (the evaluated-and-declined `Retries.run`
  executor is recorded there), the pull-assignment split enumerator every bounded source extends —
  including why its counters are registered by the connector rather than named to it
  (`docs/adr/0083`) — compile-scope consumers, and the shading/install-list consequences
  that scope carries. Migrated to ADRs (`docs/adr/0036`–`0040`, `0083`)

Decisions that span connectors stay here as rules — the package layout convention, the version
policy, the CI architecture, the workflow and the infrastructure — with their records in
`docs/adr/` (`0053`–`0065`); the licensing rules stay here in full, un-migrated. A new
connector gets its own module file rather than a section here.

## Cross-connector contracts (rules here; full records in `docs/adr/`)

- **A serializer returning `null` skips the record** (#230; `docs/adr/0001`): every connector
  serialization SPI is `@Nullable` on `serialize`, and a `null` means filter, only filter — the
  record is written nowhere, is not a failure, never reaches the `FailureHandler`, and
  `recordsSkipped` is the only thing that reports it. The writer's check sits immediately after
  the serializer's `catch`, ahead of any per-destination state; a combinator over one of our own
  SPIs propagates the `null` unchanged; a `null` from a wrapped Flink `SerializationSchema` is a
  serialization *failure*, never a skip. Precedents, the rejected alternative and the combinator
  bug are in the ADR.
- **A test forges an options object on `builder().build()`, never on `defaults()`** (#316;
  `docs/adr/0002`): `defaults()` hands out a JVM-wide singleton, and a reflective forge on it
  poisons every later test in the surefire fork. The forging test also asserts the singleton
  survived, placed in the class that would do the writing. The incident, the fork mechanics and
  the deterministic reproduction recipe are in the ADR.
- **A vendor client's teardown may re-report a failure the connector already consumed** (#325;
  `docs/adr/0003`): the Bigtable batcher and the Pub/Sub subscriber do, by unrelated mechanisms.
  Their wrappers absorb the re-report, each SPI's `close()` javadoc binds a third implementation
  to the same contract, and the absorb stays per-connector because the mechanisms share no type
  to catch. A client that cannot be subclassed holds its operations as functional values, or the
  absorb has no test. The nine-SPI survey, the asymmetric consequences and the #351 refinement
  (what else a wide absorb catches) are in the ADR.
- **A `Duration` knob a connector will convert to nanoseconds is bounded at the setter that
  accepts it** (#334; `docs/adr/0068`), at `Duration.ofNanos(Long.MAX_VALUE)` — past that
  `toNanos()` throws on a TaskManager, out of a teardown or a constructor, and never where the
  value was typed. The rule covers the knob documented as taking a long `Duration` to mean
  "unbounded" even where its own conversion would not throw, so one knob name has one answer;
  `BoundedShutdown` checks it again in its constructor, being shared and reachable without a
  setter. A `nanoTime` deadline stamped at that ceiling **overflows and is still correct** — the
  read subtracts and wraps back — so never "harden" one with `Math.addExact` or a clamp, which is
  what would break it. Which sites convert how, and that measurement, are in the ADR.
- **A test driving a sink's production `createWriter(WriterInitContext)` sets an emulator
  endpoint** (`docs/adr/0064`): the production path builds the connector's real client, and an
  eagerly constructed one demands ADC — green on any machine with credentials, red only in CI.
  Build the sink with `emulatorEndpoint("localhost:1")`, and say in a comment why the endpoint
  is not optional so a simplification pass does not remove it.
- **A value object a vendor SDK will not let anyone construct is minted from a test helper
  declared in the vendor's package** (#337; `docs/adr/0067`) — not by abstracting the value away
  in production code, and never by adding a mocking framework. Both halves of the bar are
  required: the type has no public constructor, factory or reachable super-constructor, *and* the
  behaviour under test genuinely reads it. `flink-connector-gcp-bigquery`'s
  `src/test/java/com/google/cloud/bigquery/TestJobs.java` is the only Java source in this
  repository whose package is outside `io.github.flink.gcp.*`; a second one is a decision to
  take, not a precedent to follow. The helper reaches as few package-private members as it can
  (a redundant overload is one more thing an SDK release can move), and its javadoc names them,
  why no other reach exists, and the SDK version the reach was verified against — a bump that
  moves a reached member then fails a test at compile time, which is the whole safety argument.
