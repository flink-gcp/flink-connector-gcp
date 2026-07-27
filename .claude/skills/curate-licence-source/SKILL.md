---
name: curate-licence-source
description: Curate a licence text source or licensing decision for a shaded flink-sql-connector-gcp-* module. Use when `just check-notice` / `scripts/check-notice.py` fails with "no entry in licence-sources.json covers …", "licences the template has no paragraph for", a restricted-licence (GPL/SSPL/non-commercial) error, or when the packaging IT reports a new unrelocated package root. Covers the fallback ladder for obtaining a licence text with defensible provenance, and the decisions that must go to the user.
---

# Curate a licence source for a shaded module

The NOTICE machinery is automated end to end except for the steps that are
judgment. This skill is the procedure for those steps. Everything here ends in
either a reviewable pin (URL/jar entry + sha256 + note) or a question to the
user — never in a silently chosen source. That rule exists because this
repository once shipped licence texts curl'd from repository heads chosen ad
hoc, and replacing them is where this procedure comes from.

## Failure: "no entry in licence-sources.json covers <groupId:artifactId>"

A non-Apache-2.0 artifact needs its licence text pinned. Work down this ladder
and stop at the first rung that holds. Record the outcome in
`scripts/licence-sources.json` (entry: `artifacts`, `jar` or `url`, `sha256`,
`note` explaining why this rung).

1. **The artifact's own jar.** `unzip -Z1 <jar> | grep -iE 'LICEN|COPYING|NOTICE'`
   on the jar from `target/runtime-classpath.txt`. Version-exact and
   publisher-shipped — best provenance. Use `"jar": "<entry path>"`.
2. **The publisher's repository at the tag matching the bundled version.**
   Resolve the tag with the API, never by guessing URL shapes:
   `gh api repos/<owner>/<repo>/git/matching-refs/tags/<prefix> --jq '.[].ref'`.
   Tag names are irregular — measured examples: protobuf Java `4.33.2` lives at
   tag `v33.2` (Java majors are offset by 4); re2j 1.8 at `re2j-1.8`;
   animal-sniffer 1.27 at `animal-sniffer-1.27`.
3. **The publisher's repository head, only if frozen.** Acceptable when the
   repository is archived (`gh api repos/<o>/<r> --jq .archived` → `true`) or
   provably has no version tags. The note must say which and why — e.g. gax and
   google-auth-library-java are archived with no tag for the bundled versions.
4. **There is no rung 4.** A generic template (`opensource.org`, `spdx.org`) is
   not the project's text — the copyright line is part of a BSD or MIT licence.
   If no publisher-provided text can be pinned, the question is whether to use
   the dependency at all: take it to the user.

Verify before pinning: fetch the candidate, confirm it is text (the script
rejects HTML, but look at it), confirm the copyright line names the right
project, `sha256sum` it into the entry. Then `just update-notice <module>`,
inspect the diff, run `just check-notice <module>`.

POM-declared licence URLs are a *lead*, not an answer: measured across this
bundle they point at HTML pages (`opensource.org`, `golang.org`), bare
templates (`spdx.org`), moved repositories, and wrong branches.

## Failure: "licences the template has no paragraph for: [X]"

A new licence name reached the bundle. In order:

1. Check `licenseMerges` in the root POM first — five artifacts spell
   Apache-2.0 six ways, so a "new" licence is usually a new spelling of a known
   one. Extend the merge list, not the template.
2. If it is genuinely new and permissive (MIT, BSD-n-Clause, ISC, zlib …), add
   a paragraph with a `{{X}}` placeholder to the module's `NOTICE.template`,
   then curate sources per the ladder above.
3. If it is anything else, see the next section.

## Failure: restricted licence (GPL family, SSPL, BUSL, non-commercial, …)

Do not curate, do not add a template paragraph, do not weaken the gate. This
project is Apache-2.0 with no usage restrictions, and the standing decision
(recorded in the root `CLAUDE.md`, Licensing) is that such a dependency is
normally rejected outright — usually by excluding or replacing whatever pulled
it in. Present the user with what pulled the dependency in
(`./mvnw dependency:tree -Dincludes=<ga>`) and the options; only a user
decision changes the gate, and `RESTRICTED_EXEMPT` in `scripts/check-notice.py`
is where an agreed exemption is recorded with its reasoning
(`javax.annotation-api`, dual-licensed and taken under CDDL with the classpath
exception, is the model).

## Failure: packaging IT reports an unrelocated package root

`artifactSet` is `*:*`, so a new dependency lands in the jar automatically; if
it brings a package root outside the relocation list, `everyBundledPackage…`
fails naming it. Whether to relocate is a real decision, not a default:

- relocate (add to the module POM's `<relocations>`) when the package contains
  behaving classes a user's job could also carry — the normal case;
- do **not** relocate native-library carriers (the conscrypt/netty rule: the
  library name is derived from the package, and maven-shade does not rename
  native resources — `grpc-netty-shaded` documents the rename dance required)
  or annotation-only packages (add to the IT's `UNRELOCATED_ALLOW_LIST` with
  the reason instead).

Read the relocation comments in `flink-sql-connector-gcp-pubsub/pom.xml` and
the module `CLAUDE.md` before choosing; both record the measured failure modes.
