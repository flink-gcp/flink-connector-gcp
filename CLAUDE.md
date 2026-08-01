# CLAUDE.md

Guidance for Claude Code (claude.ai/code) when working in this repository.

## Project overview

GCP connectors for Apache Flink: BigQuery, Cloud Pub/Sub and Cloud Tasks (Bigtable and Spanner
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
  passing nothing means the version pinned in the pom
- `just binary-compat 2.3.0` — the floor-build/install/fingerprint/ceiling-rerun/diff
  sequence, whose order is load-bearing. Reproducing a red weekly `binary_compat` is what it is
  for. The install step (root pom + the Pub/Sub connector) exists because the goal-only rerun
  cannot resolve inter-module dependencies from the reactor — same mechanism as the licence-goal
  rule below, bitten via the SQL uber-jar in #181 — and it primes `~/.m2` with
  `io.github.flink-gcp` SNAPSHOTs when run by hand (the recipe comment has the cleanup line)
- `just e2e` — the ITCases gated on `BQ_IT_*` variables, which `just verify` silently skips,
  with a pre-flight that makes a missing variable an error and a post-run assertion
  (`scripts/e2e-gated-its.sh`, which derives the class list from the gating annotation) that the
  gated classes actually executed. The weekly E2E workflow (`e2e.yaml`) runs this same recipe
  via WIF; locally the variables come from `.env`, so a worktree cannot run it (#156)
- `just check-notice <module>` / `just update-notice <module>` — a shaded module's
  `META-INF/NOTICE` is generated (prose from the module's `NOTICE.template`, artifact lists from
  what Maven resolves) and its `META-INF/licenses/` texts come from sha256-pinned sources in
  `scripts/licence-sources.toml`. `update-notice` regenerates after a dependency change;
  `check-notice` verifies offline in CI. Both take the module as an argument, so the SQL uber-jars
  to come reuse them. **Invoke the licence goal
  through a phase, never as a bare `license:add-third-party`**: a CLI goal invocation selects
  reactor modules without building them, so the module cannot resolve the connector it bundles —
  `-am` does not change that, and it only appears to work against a local repository some earlier
  `install` primed
- `just lint` — shellcheck over `scripts/*.sh`, ruff over `scripts/` (check *and* format), actionlint
  over `.github/workflows/`, `tofu fmt -check` over `opentofu/` (`tofu validate` is deliberately
  absent: it needs a provider-downloading init, and every PR touching `opentofu/` gets a full plan
  from the tofu-plan workflow, which subsumes it). Deliberately
  does **not** run `just --fmt --check`: that is an unstable feature, excluded from just's
  compatibility guarantee, so with `just` installed unpinned it could fail an unchanged pull
  request. actionlint is handed `-shellcheck "$(mise which shellcheck)"` rather than letting it
  find one on `PATH` — the runner image ships its own, and it is not the pinned one
- `just docs` / `just docs-serve` / `just docs-chroma` — build the site as CI does (a deprecation,
  a broken `relref` or a missing shortcode fails the build), preview it, regenerate the chroma
  palettes. `mise.toml` pins hugo-extended and Go; hugo-book is a Hugo module pinned in
  `docs/go.mod`
- `just tofu <args>` — OpenTofu in `opentofu/flink-gcp`, the root module holding the project's
  persistent GCP resources (#5). Local escape hatch only: plan/apply normally run in CI (see
  "Infrastructure (OpenTofu)" below). Credentials come from
  `GOOGLE_APPLICATION_CREDENTIALS` in the uncommitted `.env` — the google provider does not read
  `CLOUDSDK_CONFIG` (only the gcloud CLI does; see `opentofu/README.md`)
- Recipe bodies stay one command per line — no embedded `#!/usr/bin/env bash` blocks. A single
  compound command is fine (`binary-compat`'s final `diff … || { …; exit 1; }`); a multi-line
  script block is not. The boundary is shellcheck coverage: it reads `scripts/`, actionlint reads
  inline `run:` blocks, and nothing reads inside a recipe
- **Inside a recipe, always name the tool: `mise x <tool> -- …`, never bare `mise x -- …`.** The
  bare form activates every tool in `mise.toml` and installs what is missing, silently undoing the
  `install_args` meant to limit a CI job. Caught in CI on #113: `mise x -- shellcheck` in the lint
  job pulled a JDK, Maven, Hugo, Go and a second copy of `just`, shadowing the one
  `install-action` had already put on `PATH`. The bare form stays right for the *entrypoint*
  (`mise x -- just <recipe>`), which does want everything — `just verify` needs java and maven and
  names neither
- A top-level justfile variable assigned from a shell command runs on **every** `just` invocation,
  whichever recipe was asked for; a default *parameter* value runs only when its own recipe does.
  That is why `check-flink-release`'s ceiling is a parameter default rather than a variable

## Documentation (`docs/` vs module READMEs)

- `docs/content/docs/connectors/datastream/<connector>.md` is **the design record**: API notes,
  design decisions, delivery guarantees, error handling, tuning tables and the testing strategy.
  Behavior or public API changed → update the docs page, not the README
- The module `README.md` is an **overview only**: title, one-paragraph description, the
  feature-status table (`Implemented (#N)` / `Planned (#N)`), a minimal code sample, a link to the
  docs page, and the **provenance/attribution section** — provenance pairs with `NOTICE` and is a
  licensing obligation, so it stays in the repository
- Implementation status lives in the README table only; the docs page links to it instead of
  repeating it. Keep the two from drifting by adding status nowhere else
- **Issue references in module READMEs and docs pages are explicit links**, never bare `#N`.
  GitHub autolinks `#N` only in issue/PR *comments*, not in repository markdown files, and Hugo
  never does — so a bare `#N` is dead text in both places a reader actually sees. READMEs use the
  full URL; docs pages use `[#N]({{< param BookRepo >}}/issues/N)`. Cross-repository references
  keep their `owner/repo#N` text and point at *that* repository — `goccy/bigquery-emulator#342`
  is the one in the tree, and a blind `#N` rewrite would have pointed it here. `CLAUDE.md` is
  deliberately exempt: it is read by Claude, not rendered for users
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
- A module `CLAUDE.md` (`flink-connector-gcp-<product>/CLAUDE.md`) is the third document in this
  split and the only **Claude-facing** one — never rendered, never linked from the site, so
  nothing user-facing belongs in it. It carries that module's design decisions and nothing else; behavior and public
  API still go to the docs page, status still goes to the README table. Being unrendered, it keeps
  bare `#N` references under the same exemption the root `CLAUDE.md` already has

## Workflow rules

- **One git worktree per PR** under `/tmp/worktrees/flink-connector-gcp/`; never switch branches
  in the main checkout. Remove the worktree and local branch after merge
- All changes go through **draft PRs**; nothing is pushed directly to `main` after the initial
  skeleton
- **After creating a draft PR, always self-review it** — applying simplification and efficiency
  findings, not only correctness ones — and push the fixes before asking for review. Record the
  findings *and the deferrals, with their reasons* as a PR comment. Which command to use:
  - `/review <pr>` reviews a pull request and **Claude can start it itself**, so this is the one
    to reach for once the draft PR exists
  - `/code-review` reviews the working diff and is **user-invocable only** — Claude gets
    `disable-model-invocation` if it tries, so ask the user to run it rather than assuming it will
    happen
  - With neither, fall back to review subagents given *distinct* lenses (correctness and
    concurrency, public API and simplification, test quality and flakiness). One agent asked for
    "a review" returns much less than three asked for different things — and verify each finding
    against the code before acting on it
- Pin GitHub Actions to commit SHAs with `just pin-actions` whenever a workflow is added or an
  action version changes
- Commit messages, PR titles/descriptions, code comments, javadoc and issues are written in
  English
- Issues use milestones `v0.1.0` / `v0.2.0` / `v0.3.0+` and GitHub sub-issues; PRs close their
  issue with `Closes #N`

## Infrastructure (OpenTofu, `opentofu/`)

- `opentofu/flink-gcp` is the single root module for the project's **persistent** GCP resources
  (#5): enabled APIs, the state bucket, the WIF pool/provider, three service accounts and the
  shared IT bucket/dataset. Fine-grained test resources (tables, topics, subscriptions, queues)
  are created by the tests themselves and never belong here. A new connector's API and E2E grants
  are added in the PR that first needs them (Bigtable and Spanner are the known candidates), not
  in advance
- CI is **tfaction v2** (`tfaction-root.yaml` at the root): pull requests touching `opentofu/**`
  get a plan comment (`tofu-plan.yaml`), the merge applies that reviewed plan file from GitHub
  Artifacts and comments the result (`tofu-apply.yaml`). State locking is the GCS backend's
  native locking. These two workflows are the standing exception to the #111 just-recipe rule:
  tfaction is itself the named, rerunnable sequence, and `just tofu <args>` is the local
  equivalent. They run on plain `GITHUB_TOKEN` — no GitHub App, so tfaction's push-back features
  (auto-fix commits, follow-up PRs) are unused. **The apply workflow must set
  `TFACTION_IS_APPLY: "true"`** — tfaction's job_type is "terraform" for plan and apply alike,
  and setup falls back to `terraform_plan_config` (the read-only account) without it; that
  misconfiguration shipped with #168 and hid behind no-change applies until #170's first real
  write, whose 403s were then misdiagnosed twice (a missing service agent was blamed on evidence
  that never included the authenticated principal — **read the auth step's log first**).
  **A failed apply is recovered by a follow-up pull request**, never by re-running the apply
  job: the failure bumps the state serial, making the saved plan stale, so a re-run can only
  fail again ("Saved plan is stale") — and tofu cancels unstarted operations on the first
  error, so assume nothing from the failed apply exists until measured. tfaction's native
  automation of that follow-up PR needs the GitHub App, deliberately deferred to the dedicated
  org at go-public time (#177; decided with the user on #176, where a dispatch-triggered
  fresh-apply workflow was built as an alternative and withdrawn in the App's favour). One
  service-agent fact worth keeping: enabling an API does **not** create its service agent (they
  provision lazily; `gcloud beta services identity create` is the per-service one-off, in
  `opentofu/README.md`)
- **No service account keys, ever.** All CI credentials are short-lived WIF tokens; the provider
  condition pins the immutable repository/owner IDs, and per-account bindings restrict the apply
  account to `push` on `main` and the E2E account to `push`/`schedule`/`workflow_dispatch` on
  `main`. Plan runs read-only (`roles/viewer` + `roles/iam.securityReviewer`, plus state-bucket
  writes for the lock). Local runs authenticate via `GOOGLE_APPLICATION_CREDENTIALS` from
  `.env`; the bootstrap that created the backend's own bucket is recorded
  in `opentofu/README.md`
- The tofu version is pinned twice on purpose: `mise.toml` (what installs) and
  `versions.tf` `required_version` (what refuses to run on a skew) — a bump edits both

## Version policy

- Releases follow full semver (`v0.1.0`, `v0.2.0`, ...). Early milestones are **tags only** — no
  artifact publishing. Publishing to Maven Central happens once all connectors are implemented,
  as `v1.0.0` (Central namespace registration, signing and the Flink 1.x/2.x publishing strategy
  are decided then; see issues #29 and #39)
- `main` supports **the current and previous Flink minor**, mirroring Flink's own support policy
  (decided in #102). Today that is **2.2 and 2.3**, with `flink.version` pinned to the floor
  (`2.2.1`) because compiling against the oldest and running on newer is the direction that
  works. A new Flink minor moves both ends: that is a deliberate edit to `flink.version` plus
  `.github/workflows/weekly.yaml`, never a dependabot minor bump — which is now enforced by an
  `ignore` rule (patch bumps still arrive). Closed PRs #42 and #97 are the precedent for
  rejecting minor bumps.
- **Flink 1.20 (1.x LTS) is supported from this same source, not from a branch or per-version
  modules** (decided in #32, reversing the branch plan first recorded here — measured, not
  assumed): the whole API delta between 1.20 and 2.x for the surface these connectors touch is
  (a) 1.20 still declaring the deprecated `createWriter(Sink.InitContext)` abstract while 2.x
  removed the type — absorbed by the one-interface `CrossVersionSink` seam under
  `src/main/java-flink1`/`java-flink2`, selected by the `flink.compat` Maven property (default
  `flink2`; `just verify-flink 1.20.x` adds `-Dflink.compat=flink1` itself) — and (b)
  `CommittableMessage.getCheckpointId()` changing its return type across the majors, dodged by
  calling `getCheckpointIdOrEOI()` (present in both, deprecated on 2.x; if a 2.x minor removes
  it, that call is the line to revisit). The 1.20 bridge default is compile-only: 1.20's runtime
  always creates writers through `createWriter(WriterInitContext)`, measured by the whole suite
  running green on 1.20.4 with the bridge throwing. **This is source-level support** — the
  weekly `lts` row compiles and tests everything at `FLINK_LTS`, a jar is compiled per major,
  and no cross-major binary claim is made (the one-artifact claim below spans the 2.x range
  only). A red `lts` row reproduces locally with `just verify-flink <FLINK_LTS>` — the same
  first-move rule `binary_compat` has. A Dataproc-style per-version module split was considered and declined: it buys
  isolation the two ~15-line interface variants already provide. Publishing (the kafka-style
  `X.Y.Z-1.20` suffix) is decided in #29/#39
- **One artifact covers the supported range**, so there is no per-minor artifact suffix (the
  `-2.1` suffix assumption from before #102 is dropped; #29/#39 decide publishing). Only about
  half the Flink API surface these connectors touch is `@Public` — and `@Public` guarantees
  source, not binary, compatibility across minors — so the claim rests on the `binary_compat`
  job in `weekly.yaml`: build against the floor, then re-run the whole suite with the newest
  supported Flink swapped onto the classpath and nothing recompiled. If it ever goes red, the
  fallback is per-minor artifacts as `apache/flink-connector-kafka` publishes them
  (`5.0.0-2.1` / `5.0.0-2.2` from one branch), which is also what Paimon and Iceberg do
- The version matrix lives in `weekly.yaml`, not `ci.yaml`: per-PR CI stays single-version for
  latency, matching Flink's own `push_pr.yml` / `weekly.yml` split. Every matrix job checks out
  `github.sha` rather than a branch — a merge landing mid-run once made one version look like it
  had silently skipped 60 tests. Matrix rows carry a **role** (`floor` / `ceiling` / `next` /
  `lts`), not a version, because GitHub does not expose the `env` context to `strategy` and a
  version repeated across rows is how one of them gets missed; the version is resolved in a step
  from `FLINK_CEILING` / `FLINK_NEXT_SNAPSHOT` / `FLINK_LTS` at the top of the file. A 1.20
  patch bump is an edit to `FLINK_LTS` there — dependabot does not see workflow env, the same
  accepted staleness `FLINK_CEILING` has. The `floor` row passes no
  `-Dflink.version` at all, so the pom stays the single source of truth for it, and it runs on
  JDK 21 because floor-on-17 is already covered by `ci.yaml` and by `binary_compat`. The `next`
  row is upstream early-warning and is deliberately **not** `continue-on-error`
- **Moving the supported range** (when Flink releases a new minor): `ci.yaml` needs no edit — it
  names no Flink version and no ceiling, so bumping the pom moves it. The order is
  (1) `pom.xml` `flink.version` → the old ceiling, (2) `weekly.yaml` `FLINK_CEILING` and
  `FLINK_NEXT_SNAPSHOT`, (3) `docs/content/_index.md` table, (4) `README.md` under Build,
  (5) this section. Then **re-run the binary-compatibility measurement against the new ceiling
  before claiming the range** (`just binary-compat <new ceiling>`) — the old measurement says
  nothing about the new pair. Do not hand-maintain this list: `scripts/check-flink-release.sh`
  prints it in its failure output, which is the copy that gets read
- `scripts/check-flink-release.sh` (the `new_minor_check` job) exists because suppressing the
  dependabot minor PR removed the only thing that announced a Flink release. It compares the
  ceiling passed to it against Maven Central weekly and fails until the range is moved. It is
  deliberately **not** a dependency of the other jobs: a new upstream release must not stop the
  current range from being verified
- CI helpers live in `scripts/` as files, not inline in workflow `run:` blocks, so they can be
  run by hand — reproducing a red `binary_compat` locally is the first thing to do when it goes
  red. `tools/` is not the place: it holds build tool *configuration*
  (`tools/maven/checkstyle.xml`), following Flink's layout. Two consequences: `scripts/` is
  outside the `.github/**` rat exclude, so each file carries the plain Apache-2.0 header, and
  `just lint` shellchecks them — and also runs `actionlint`, which shellchecks inline `run:`
  blocks, so a script stays linted whether it lives in `scripts/` or in a `run:` block.
  **A `justfile` recipe is neither** — nothing
  lints inside one — so a recipe body holds commands, and anything that grows into a script goes
  to `scripts/`
- **A multi-step sequence is named once, in the `justfile`, and CI calls that recipe** (#111) —
  `binary_compat` is one step invoking `just binary-compat` rather than four `run:` blocks, so
  the order the sequence depends on has a single definition and is rerunnable by hand. The cost
  was weighed and accepted: a failure names the `==>` phase inside the recipe rather than a step
  in the GitHub UI
- **`lint.yaml` is where linters Maven does not run live** (spotless and checkstyle cover the
  Java sources inside `verify`). Today that is shellcheck, actionlint and `tofu fmt -check`
  (#5 landed; `tofu validate` is subsumed by the tofu-plan workflow's plan). Separate from
  `ci.yaml` so results arrive in seconds rather than behind the integration tests — that is the
  whole reason, the mise-versus-`setup-java` one having turned out to be a disarmable default
  rather than a conflict (see below). Its `paths` filter must list **every input
  to a lint, not just the linted files** — `mise.toml` is in it because that is where the
  shellcheck version is pinned, and skipping the lint on a version bump would skip it in the one
  change that most needs it. `docs.yaml` carries `mise.toml` for the same reason since #111
- **Where a tool's version lives decides how CI installs it.** Pin in `mise.toml` and install
  with `jdx/mise-action` + `install_args` when a version skew can fail a pull request that
  changed nothing — shellcheck (0.9.0 on ubuntu-24.04, 0.11.0 on 26.04, so an `ubuntu-latest`
  migration would fail an unrelated PR) and hugo/go (`docs.yaml` moved onto this shape in #111,
  retiring its `HUGO_VERSION`-plus-"keep in sync" duplication). `install_args` matters: it names
  the subset of `mise.toml` the job needs. Otherwise install with `taiki-e/install-action` and no
  version — that is `just`, whose 1.x compatibility guarantee ("there will never be a 2.0") means
  a newer release cannot break an unchanged justfile, so `mise.toml` says `just = "1"` and CI
  says `tool: just`. **That guarantee covers stable features only**, so nothing CI runs may
  depend on a `--unstable` one; `just --fmt --check` is kept out of `just lint` for exactly this
  reason. Reach for an unstable feature and the tool needs a pin, which means an inline version
  in every install-action step — six of them today
- **`jdx/mise-action` beside `setup-java` needs `add_shims_to_path: false`.** `mise.toml` pins java
  and maven, and the action defaults **both** `add_shims_to_path` and `export_path` to `true`, so
  out of the box its shims and env paths land in front of the JDK the job just installed. That is a
  default to disarm, not a combination to forbid — this bullet said "must not run" until it was
  checked, extrapolated from a real incident that was a *different* mechanism (`mise x --` without
  a tool name, which has its own rule under Build); the two have never actually run together in any
  workflow here. When a `setup-java` job does need a mise-pinned tool: set
  `add_shims_to_path: false`, scope `install_args` to that tool, and invoke it by explicit path —
  `just lint` already does exactly that, handing actionlint `-shellcheck "$(mise which shellcheck)"`.
  **Better still, arrange not to need it.** A tool the Java build depends on should come from Maven
  where it can, because its version then lives beside the dependency it has to track and no workflow
  changes at all — protoc resolves as `com.google.protobuf:protoc:<version>:exe:<platform>`, which
  is why #132 added code generation without touching CI. None of this changes why `just` comes from
  `taiki-e/install-action` in every workflow: that rests on its own reason, one binary on `PATH` and
  no shims at all
- `docs.yaml` and `lint.yaml` both carry `paths` filters, and `ci.yaml` carries a
  `paths-ignore` for changes that cannot affect the Maven build: `opentofu/**`, the tofu
  workflows, and `**/README.md` / `**/CLAUDE.md` — the last two only because apache-rat's
  exclude list already carries exactly those patterns, so no licence-header check is lost.
  `docs/` markdown is deliberately not ignored: rat scans it and `ci.yaml` is its only
  pre-merge check. A pull request touching only ignored paths never reports these checks. Fine while they are optional — but **a
  required check that never reports blocks a pull request forever**, so making any of them
  required means dropping its filter or adding a job that reports success when the filter does
  not match
- JUnit stays on 5.x and testcontainers on 1.x for now; their major-version dependabot PRs are
  intentionally left open/deferred
- Google Cloud library versions come only from `libraries-bom`; never pin individual
  google-cloud artifact versions

## Licensing and provenance

- Files written for this project carry the plain Apache-2.0 header
  (`Copyright 2026 laughingman7743`). Files copied from Apache projects keep their ASF header.
  apache-rat enforces this (configuration overridden in the root POM; new unheaderable file
  types need a rat exclude there)
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

Under `io.github.flink.gcp.connector.<product>` (decided in #63, applied to BigQuery first;
Pub/Sub, Cloud Tasks and later modules follow the same skeleton):

- `sink` — public sink API only: the facade + builder, write-method enum, shared options/enums,
  destination types, and the `@Internal` types shared by every write method (the sink config,
  the fixed-destination resolver, `RetrySchedule` until #61 extracts a shared retry module)
- `sink.<writepath>` — one subpackage per write-path family, which may host several write
  methods (BigQuery: `sink.storage` holds the Storage Write API family — the default-stream
  at-least-once method today, and the #30 buffered-stream exactly-once method beside it,
  sharing the appender machinery; `sink.fileloads` holds FILE_LOADS). The package root holds
  the Sink classes, the family's public options objects and committable contracts; internal
  stages follow the Flink FileSink precedent with `.writer`, `.committer` and
  post-commit-topology subpackages (`.loadjob` here, FileSink's `.compactor`) as the topology
  requires — a family without 2PC simply has no `.committer` package.
  **One family, with no second one in prospect, means no layer**: the module goes straight to
  `sink` + `sink.writer` (`sink.committer`, … as the topology requires). Decided in #119, where
  Cloud Tasks' `sink.createtask` was named after the `CreateTask` RPC rather than after a design
  and no sibling can arrive at all — `BatchCreateTasks` and `BufferTask` are REST-only and absent
  from the Java client. Pub/Sub's `sink.publisher` went with it so the two single-family modules
  stay alike. Adding the layer back is what a second family costs, and it is a mechanical move —
  the two layers #119 removed held nothing public, though a family layer generally may (BigQuery's
  `BufferedStreamOptions` and `FileLoadsOptions` are `@PublicEvolving` in theirs).
  **The layer is spelled the way Google spells it in code, with no `api` suffix** (#121):
  `sink.storage` mirrors `com.google.cloud.bigquery.storage.v1` and the
  `google-cloud-bigquerystorage` artifact, as `sink.fileloads` already drops the Jobs-API word.
  The public `WriteMethod.STORAGE_API_*` constants keep the product's documented name on
  purpose — the package names an implementation family to maintainers, the enum names a feature
  to users. Inside it, an SPI's real implementation is named after **the SDK resource it owns**
  (the one its `close()` releases) — `WriteClientBufferedStreamService` over `BigQueryWriteClient`,
  as `StreamWriterRowAppenderFactory` is over `StreamWriter`. Neither `Storage*` nor the
  repository's usual `Default*` works here: the SPI is equally a Storage Write API type, so that
  prefix distinguishes nothing, and *default stream* is BigQuery's implicit always-on stream,
  named throughout this package and the opposite of a buffered one
- `sink.tables` — shared table-metadata layer consumed by every write method: the `TableAdmin`
  SPI and its REST implementation, schema snapshot/unifier, REST↔Storage schema converters
- `sink.serializer` — the record-conversion SPI (`BigQueryProtoSerializer`) alone, with
  `sink.serializer.<format>` beneath it for each input format: `.proto`, `.avro`, and `.json` when
  the #66 JSON half lands. Each format package holds its facade, its `@PublicEvolving` options
  object and the `@Internal` types behind them, mirroring how `sink.<writepath>` keeps
  `FileLoadsOptions` and `BufferedStreamOptions` inside their family packages — so this is a
  public-API layer, not merely an internals split. Decided in #125, after #123 took the package
  to ten classes with the names already doing the package's job (`Proto*` ×4, `Avro*` ×4). It
  **passes** the #119 rule rather than contradicting it: that rule is a test, not a count — *one
  family, with no second one in prospect, means no layer* — and here two formats exist today while
  the issue that introduced the second one already plans a third, the exact opposite of Cloud
  Tasks' `sink.createtask`. `BigQueryProtoSerializer` keeps its name and its place at the root:
  the split makes it *read* as the proto family's SPI when it means "the wire form is protobuf,
  whatever the input was", but its javadoc says so, and renaming would touch the sink core, the
  writers and ~20 tests for no behavioural gain — considered and declined, unlike the #121 rename.
  Every package-private coupling stays inside one format, so nothing had to widen to `public`;
  that holds only because the tests move with their format. The format packages must not import
  each other: the three Avro→proto javadoc references are **fully-qualified `{@link}`s rather than
  imports**, so the independence is a property of the import graph and not just of the call graph.
  Spotless does keep a javadoc-only import (measured, not assumed), so the short form was
  available and was declined for that reason
- `sink.failure` — row-level failure SPI (`FailedRow`, handlers, DLQ stub), kept separate so the
  cross-connector extraction planned in #37 stays cheap
- `source` / `table` — reserved for sources (#31, #34, #64) and Table API (#47, #57), with the
  same philosophy: public API at the package root, implementation subpackages beneath. The
  family rule above applies here too, and `source.streamingpull` **keeps** its layer under it:
  the sibling Cloud Tasks cannot have is real here, since a unary-`Pull` source is a live
  alternative — weighed, and rejected on trade-offs the connector documentation records as
  cutting both ways

A new top-level class in a module's `sink` root needs a reason to be public API; implementation
types belong in the subpackages. The one standing exception is a single-family module's
`@Internal` `Sink` class (`CloudTasksCreateTaskSink`, `PubSubPublisherSink`), which sits beside
its facade because there is no family package left to hold it. Every module's `sink` root also
carries the `@Internal` `CrossVersionSink` seam in the per-major source roots
(`src/main/java-flink1`/`java-flink2` — see the version policy): it must be importable by every
sink in the module, and its two variants share one FQCN on purpose. Test sources mirror the
main-tree packages.

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
  and the connection-pool guard (#54), deferred `location()` (#10)
- `flink-connector-gcp-pubsub/CLAUDE.md` — vendoring provenance (#17/#31), sink (#18), topic
  auto-creation (#19), tuning (#20) and in-flight bounds (#85), ordering×repair (#78), emulator
  (#21), source (#79/#80), Table API/SQL (#47, split into #135–#138) and the shaded uber-jar
  (#138) — which is where the repository's only shading decisions live, so read it before adding
  a second `flink-sql-connector-gcp-*`
- `flink-connector-gcp-cloudtasks/CLAUDE.md` — sink design (#23) and implementation (#24)

Decisions that span connectors stay here: the package layout convention above, the version policy
and the licensing rules. A new connector gets its own module file rather than a section here.
