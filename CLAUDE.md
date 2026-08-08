# CLAUDE.md

Guidance for Claude Code (claude.ai/code) when working in this repository.

## Project overview

GCP connectors for Apache Flink: BigQuery, Cloud Pub/Sub, Cloud Tasks and Bigtable (Spanner
planned). Independent OSS project — not affiliated with the Apache Software Foundation or Google.
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
- `just e2e` — the ITCases gated on the `BQ_IT_*` / `PUBSUB_IT_PROJECT` / `BIGTABLE_IT_PROJECT`
  variables, **and the only thing that runs them** (#245; ADR-0057 records the per-shell
  incident and the marker mechanics): each gated class also carries `@Tag("gated")`, which the
  root pom excludes from every surefire execution, and this recipe is the opt-in that clears
  it. Its pre-flight makes a missing variable an error, and a post-run assertion
  (`scripts/e2e-gated-its.sh`) checks the gated classes actually executed. Its `-pl`-scoped
  builds install the base and test-utils modules first, for the same reactor-resolution reason
  `binary-compat` installs (#27, #61). The weekly E2E workflow (`e2e.yaml`) runs this same
  recipe via WIF; locally the variables come from the uncommitted `.env`, which a fresh
  worktree does not have — run `just worktree-env` once there to symlink the main checkout's
  copy (#156; the same link also carries `just tofu`'s credentials)
- `just sweep-e2e [--dry-run]` — deletes Bigtable instances an E2E run abandoned (#246).
  `AbstractBigtableRealGcpITCase` sweeps at the start of a gated class, but only the weekly
  E2E workflow schedules one, so a run whose teardown never executed leaves a one-node
  instance standing (~**$109** until the next weekly run); `sweep-e2e.yaml` runs this daily,
  which is what bounds that number. The
  instance prefix and the two-hour threshold are **read out of the Java source** (a second copy
  would go stale silently), and both greps plus the gcloud listing are hard errors, because a
  sweep that matches nothing looks exactly like a sweep with nothing to do
- `just check-notice <module>` / `just update-notice <module>` — a shaded module's
  `META-INF/NOTICE` is generated (prose from the module's `NOTICE.template`, artifact lists from
  what Maven resolves) and its `META-INF/licenses/` texts come from sha256-pinned sources in
  `scripts/licence-sources.toml`. `update-notice` regenerates after a dependency change;
  `check-notice` verifies offline in CI. Both take the module as an argument, which is what lets
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
  allowlist entry in `scripts/flink-api-tiers.toml`; a new one — or a stale entry — fails.
  Runs as its own `verify.yaml` job, not in `lint.yaml` and not inside `just lint` (ADR-0058),
  and it downloads the sources jars (into `target/flink-api-tiers/`) while `just lint` stays
  offline
- `just check-option-docs` — holds `docs/content/docs/reference/` to the options the connectors
  actually take (#89), both directions: every public builder setter and every Table API
  `ConfigOption` key must be named in a table whose **first column header is exactly `Option`** —
  that header *is* the opt-in, which is what keeps the check off the metadata, type-mapping and
  policy tables the same pages carry — and every option those tables name must exist. Modules are
  mapped to pages in `scripts/option-docs.toml`, not classes, so a **new** `*Options` class is
  required to appear from the moment it exists. Two allowlists, pointing opposite ways —
  `[exempt]` is a setter with no row, `[extra]` a row with no setter — and **an entry that
  never fires fails**, as a stale one does in `flink-api-tiers.toml`. The pages are
  **hand-written, not generated**: their tables group knobs and carry defaults the sources do
  not hold. Its own `verify.yaml` job (ADR-0058). **How to respond to each failure is
  `.claude/skills/curate-option-docs/`.** What the check does *not* do: it compares the set of
  options, not their values, so a changed default has to be edited in the same commit
- `just check-metric-docs` — the same shape of check for the **metrics tables on the DataStream
  pages** (#296). Both directions, per module in `scripts/metric-docs.toml`: every name in a
  connector's `*MetricNames` inventory must appear in a table whose **first column header is
  exactly `Metric`**, with the Type column leading `counter` or `gauge` as the source registers
  it, and every name those tables carry must be registered, a `base.metrics` subgroup template
  the module wires, or **marked `(Flink standard)` in the Type cell** (the marker is guarded).
  Three inventory-integrity rules ride along, plus the mechanical half of the #280 naming rule
  (no `num`-prefixed name). Both allowlists are empty today, and an entry that never fires
  fails. Its own `verify.yaml` job (ADR-0058). **How to respond to each failure is
  `.claude/skills/curate-metric-docs/`.** What it does *not* check: Meaning cells and the prose
  around the tables, so a rename still sweeps those by hand — in the same commit
- `just check-gated-tags` — the two markers a gated real-GCP ITCase carries have to stay together
  (#245; ADR-0057 records both failure directions): the `@EnabledIfEnvironmentVariable` the E2E
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
- `just test-scripts` — pytest over `scripts/`, through the uv project at the repository root
  (`pyproject.toml` + committed `uv.lock`; uv itself pinned in `mise.toml`). Runs as
  lint.yaml's `script_tests` job. A new `scripts/*.py` checker owes its tests here, alongside
  the curate-* skill the checker rule already demands — a skill being owed for *judgment*,
  which is why `check-gated-tags` has tests here and no skill.
  **A checker's tests are synthetic — a tree built in `tmp_path` with `ROOT`/`CONFIG`/`SOURCES`
  monkeypatched onto it — never assertions against the real repository**, which is what keeps
  lint.yaml's paths filter from having to grow to every input those checkers read.
  `test_ci_maven_args.py`'s real-repo CLI layer is the exception that names its own inputs in
  that filter (the poms, the `NOTICE.template`s). The direction the tests are aimed at is a
  checker quietly finding *less* than it should — that reads exactly like a clean tree — so
  each rule is pinned by a case that fails when the rule is removed
- `just lint` — shellcheck over `scripts/*.sh`, ruff over `scripts/` (check *and* format), actionlint
  over `.github/workflows/`, markdownlint (markdownlint-cli2, pinned via mise's npm backend) over
  the **rendered** markdown — `docs/content/`, `docs/adr/` and the READMEs, never the
  `CLAUDE.md`s — at strict defaults except MD013 and MD060, both declined with reasons in
  `.markdownlint-cli2.jsonc`. MD051's in-page anchor check is the half Hugo's build does not
  cover (`relref` validates cross-page links only), and a *cross-page* link carrying a
  `#fragment` is checked by **neither** — it can point at nothing while both stay green, which
  is why #90 resolved them by hand against the built `docs/public` and deferred a checker until
  the pages actually rot. Also `tofu fmt -check` over `opentofu/` (`tofu validate` is
  deliberately absent: every PR touching `opentofu/` gets a full plan, which subsumes it).
  Deliberately does **not** run `just --fmt --check` — an unstable feature, excluded from
  just's compatibility guarantee (ADR-0057). actionlint is handed
  `-shellcheck "$(mise which shellcheck)"` rather than letting it find the runner image's own
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
  feature-status table (`Implemented (#N)` / `Planned (#N)`), a minimal code sample, a link to the
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
  is the one in the tree, and a blind `#N` rewrite would have pointed it here. `CLAUDE.md` is
  deliberately exempt: it is read by Claude, not rendered for users
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
- The site is built as a CI check only; GitHub Pages publishing waits until the repository is
  public (#6). Each module README links to its docs page by in-repo relative path — those links
  become site URLs when Pages goes live, which is a checklist item on #6
- **The API reference is the site's generated half** (#88; ADR-0056): `just docs-javadoc`
  aggregates JavaDoc across every module into `docs/static/api/java`, never committed; pages
  link to it with `{{< param ApiDocsURL >}}`, a param and not `Book*`-prefixed on purpose.
  Nothing is filtered by API tier, doclint stays off with `failOnWarnings` on and nothing
  fetches a remote link index, and there is one unversioned path tracking `main` — the first
  two measured, all three argued in the ADR
- A module `CLAUDE.md` (`flink-connector-gcp-<product>/CLAUDE.md`) is the third document in this
  split and the only **Claude-facing** one — never rendered, never linked from the site, so
  nothing user-facing belongs in it. It carries that module's design decisions and nothing else; behavior and public
  API still go to the docs page, status still goes to the README table. Being unrendered, it keeps
  bare `#N` references under the same exemption the root `CLAUDE.md` already has
- **`docs/adr/` is the decision archive**, and `docs/adr/README.md` carries the whole scheme
  (ADR-0000): the three-home boundary, the write trigger, cluster granularity, the two statuses,
  the refinement lifecycle, and the index that allocates the next number. The operative rules:
  **an ADR is owed exactly where a decision record would otherwise be owed to a `CLAUDE.md`,
  README or docs page** — the self-review question to ask of every diff; design discussion stays
  on the issue (`Design (settled YYYY-MM-DD)`, which may just say "Settled — see ADR-NNNN"); the
  implementing PR carries the ADR in its diff; a refinement edits the ADR in place, a reversal
  adds a new ADR and flips the old one's status — "do not silently revisit" means engage the
  record, not keep it forever. And **a repository-relevant decision or measured fact is recorded
  in the repository — an ADR, a docs page, or a rule here — never only in Claude's private
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
- **After creating a draft PR, always self-review it** — applying simplification and efficiency
  findings, not only correctness ones — and push the fixes before asking for review. Record the
  findings *and the deferrals, with their reasons* as a PR comment; recording is not routing, which
  the bullet below governs. Which command to use:
  - `/review <pr>` reviews a pull request and **Claude can start it itself**, so this is the one
    to reach for once the draft PR exists
  - `/code-review` reviews the working diff and is **user-invocable only** — Claude gets
    `disable-model-invocation` if it tries, so ask the user to run it rather than assuming it will
    happen
  - With neither, fall back to review subagents given *distinct* lenses (correctness and
    concurrency, public API and simplification, test quality and flakiness). One agent asked for
    "a review" returns much less than three asked for different things — and verify each finding
    against the code before acting on it
- **Then run a second round with different lenses, and do it before saying the PR is ready**
  (ADR-0060 carries the measurement that pinned this). The rounds answer different questions:
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
  comment or a `CLAUDE.md` "known gap" line is the silent deferral wearing a disguise; a filed
  issue states the **grounded** reason the work is not being done now — never "not planned" or
  "out of scope" — and carries the better approach or a measure-first step where one exists.
- Pin GitHub Actions to commit SHAs with `just pin-actions` whenever a workflow is added or an
  action version changes
- Commit messages, PR titles/descriptions, code comments, javadoc and issues are written in
  English
- **An issue carries three things: a milestone, a `priority:` label, and a `module:` or `area:`
  label** (ADR-0062 carries the taxonomy). Milestones are releases, `v0.3.0` through `v1.0.0`
  (going public); read each milestone's description for its theme rather than guessing from the
  number. Priority is ordered by *what breaks*, not by urgency — `P0` a shipped path breaking
  silently, down to `P3` future or externally blocked — and is orthogonal to the milestone on
  purpose. GitHub sub-issues only where a parent genuinely decomposes; a *cluster* sharing one
  root cause is recorded as a comment on the one to work instead, so each keeps its own closure
  record
- PRs close their issue with `Closes #N`, written **unformatted** — a closing keyword inside a
  code span is not parsed, the issue silently survives the merge, and nothing reports it. Check
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
- `main` supports **the current and previous Flink minor** — today **2.2 and 2.3**, with
  `flink.version` pinned to the floor (`2.2.1`) — and **one artifact covers the range**, a
  claim the weekly `binary_compat` job measures rather than assumes (ADR-0053, which also
  carries the matrix design and the per-minor-artifact fallback). A new Flink minor moves both
  ends **deliberately**, never via a dependabot minor bump (an `ignore` rule suppresses those;
  patch bumps still arrive): `scripts/check-flink-release.sh` announces the release and prints
  the edit list in its failure output — do not hand-maintain that list anywhere else — and the
  new range is not claimed until `just binary-compat <new ceiling>` has been re-run
- **Flink 1.20 (1.x LTS) is supported from this same source, at source level** (ADR-0054): the
  `flink.compat` property selects the `CrossVersionSink` compat source root
  (`src/main/java-flink1`/`java-flink2`), `just verify-flink 1.20.x` selects it locally, the
  weekly `lts` row verifies it, and no cross-major binary claim is made — the one-artifact
  claim spans the 2.x range only. A 1.20 patch bump is a hand edit to `FLINK_LTS` in
  `weekly.yaml` (dependabot does not see workflow env)
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
  rename of `CI passed` itself, which must update branch protection in the same change. When a
  fresh PR shows "no checks reported", run `gh pr view <n> --json mergeable` **first**: a
  `CONFLICTING` pull request triggers zero `pull_request` runs at all

## Licensing and provenance

- Files written for this project carry the plain Apache-2.0 header
  (`Copyright 2026 laughingman7743`). Files copied from Apache projects keep their ASF header.
  apache-rat enforces this (configuration overridden in the root POM; new unheaderable file
  types need a rat exclude there) — and enforces *those two headers*, not "an Apache licence is
  mentioned somewhere", since #255 turned rat's built-in matchers off. They had been approving a
  file for carrying the bare licence URL, so a header could lose its first line and still pass.
  Still **not** checked, deliberately: the contents of the copyright line, and which of the two
  headers a given file carries. A third header would need its own configured pattern — which is
  the point at which someone decides whether it belongs here at all
- When adapting Apache-2.0 code from other projects (Beam, Dataproc connector,
  google/flink-connector-gcp, java-bigquerystorage, apache/flink-connector-gcp-pubsub):
  record the provenance in the module README and the repository `NOTICE`, and keep original
  headers where applicable. Keep each module README's "no code copied" claim accurate
- **This project is Apache-2.0 with no usage restrictions, and its dependencies must be too.**
  A library under a restrictive licence — the GPL family, or the newer source-available and
  non-commercial ones (SSPL, BUSL, Commons Clause, …) — is normally rejected outright rather than
  recorded in a NOTICE; adoption of one would be a project discussion, not a licensing entry.
  `scripts/check-notice.py` enforces this for the shaded modules (decided with the user on #138;
  the one standing exemption is `javax.annotation-api`, dual-licensed and taken under CDDL with
  the classpath exception)
- Never open or reference the private in-house implementation this project supersedes; design
  references must be public OSS or official documentation only

## Package layout convention (all connector modules)

Under `io.github.flink.gcp.connector.<product>` — migrated to ADR (`docs/adr/0055`, which
carries the full skeleton, the evidence and the declined alternatives). The rules:

- Public API lives at a package's root, implementation in subpackages beneath it; test sources
  mirror the main-tree packages. The `sink` root holds public sink API plus the `@Internal`
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

Per-connector design decisions live in module-scoped `CLAUDE.md` files, which load when Claude
touches a file in that module. **Read the module file before changing a module's behavior or
public API — and before answering a design question about it**, which is the case the on-demand
load does not cover: a session that never opens a file in the module never sees its decisions, and
reasoning about a module without them is how a settled decision gets re-argued. The topics below
are the trigger; they are not a summary, and none of them is safe to answer from:

- `flink-connector-gcp-bigquery/CLAUDE.md` — facade and serializer SPI, error handling (#13),
  FILE_LOADS (#14), its streaming form (#69) and its live-table reconciliation (#142),
  STORAGE_API_EXACTLY_ONCE (#30), per-write-method
  option scoping, JSON columns (#49/#50), geography columns (#126), Avro and JSON serializers (#66),
  column modes (#124/#145), protobuf well-known types (#147), default-stream tuning knobs
  and the connection-pool guard (#54), Table API/SQL (#57, split into #287–#290) and its shaded
  uber-jar (#290), deferred `location()` (#10). Migrated to ADRs (`docs/adr/0016`–`0035`)
- `flink-connector-gcp-pubsub/CLAUDE.md` — vendoring provenance (#17/#31), sink (#18), topic
  auto-creation (#19), tuning (#20) and in-flight bounds (#85), ordering×repair (#78), emulator
  (#21), source (#79/#80), Table API/SQL (#47, split into #135–#138) and the shaded uber-jar
  (#138). Migrated to ADRs (`docs/adr/0004`–`0015`); the **general** shading decisions every
  later `flink-sql-connector-gcp-*` inherits are `docs/adr/0015` — read it before adding a
  third; what is specific to a tree (an artifact kept out of the bundle, a relocation only it
  needs) belongs beside that connector, as #290's does
- `flink-connector-gcp-cloudtasks/CLAUDE.md` — sink design (#23) and implementation (#24).
  Migrated to ADRs (`docs/adr/0048`–`0049`)
- `flink-connector-gcp-bigtable/CLAUDE.md` — sink design and implementation (#33): implement rather
  than adopt or vendor, the four SDK facts the writer rests on (including the client's own blocking
  flow controller and two Google-internal annotations accepted deliberately), the row-level vs fatal
  boundary — `INVALID_ARGUMENT` alone, settled on gRPC's definition plus AIP-194 rather than left
  unmeasured — one fixed table per sink, and why the send metrics landed here rather than with #37.
  Also the E2E enablement (#218): the ephemeral per-class instance and its sweep, what real Bigtable
  answers each rejection with, and the emulator deviation table those measurements produced.
  Migrated to ADRs (`docs/adr/0041`–`0047`)
- `flink-connector-gcp-test-utils/CLAUDE.md` — the shared test-utils module (#27): test-support
  code only (main-code sharing belongs in `flink-connector-gcp-base`), all-provided dependencies,
  no forced unification of emulator container fixtures, and the justfile install-list coupling its
  reactor-sibling consumers create. Migrated to ADRs (`docs/adr/0050`–`0051`)
- `flink-connector-gcp-base/CLAUDE.md` — the shared main-code module (#61, joined by #37's
  DLQ/metrics as `base.failure`/`base.metrics` and by `base.lifecycle`/`base.rpc`): the failure
  SPI and metric-name conventions (#280), retry loops and
  retryability classification stay per-connector (the evaluated-and-declined `Retries.run`
  executor is recorded there), compile-scope consumers, and the shading/install-list consequences
  that scope carries. Migrated to ADRs (`docs/adr/0036`–`0040`)

Decisions that span connectors stay here as rules — the package layout convention, the version
policy, the CI architecture, the workflow and the infrastructure — with their records in
`docs/adr/` (`0053`–`0063`); the licensing rules stay here in full, un-migrated. A new
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
- **A test driving a sink's production `createWriter(WriterInitContext)` sets an emulator
  endpoint** (`docs/adr/0064`): the production path builds the connector's real client, and an
  eagerly constructed one demands ADC — green on any machine with credentials, red only in CI.
  Build the sink with `emulatorEndpoint("localhost:1")`, and say in a comment why the endpoint
  is not optional so a simplification pass does not remove it.
