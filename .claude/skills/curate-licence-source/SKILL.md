---
name: curate-licence-source
description: Curate a licence text source or licensing decision for a shaded flink-sql-connector-gcp-* module. Use when `just check-notice` / `scripts/check-notice.py` fails with "no entry in licence-sources.toml covers …", "licences the template has no paragraph for", "neither templates {version} nor declares version_independent", "does not start with version_strip_prefix", "content hash … does not match the pin", a restricted-licence (GPL/SSPL/non-commercial) error, or when the packaging IT reports a new unrelocated package root, or when `just check-notice-sources` — the weekly notice_sources job or the per-PR licence-pin fetch — goes red. Covers the fallback ladder for obtaining a licence text with defensible provenance, and the decisions that must go to the user.
---

# Curate a licence source for a shaded module

The NOTICE machinery is automated end to end except for the steps that are
judgment. This skill is the procedure for those steps. Everything here ends in
either a reviewable pin (URL/jar entry + sha256 + note) or a question to the
user — never in a silently chosen source. That rule exists because this
repository once shipped licence texts curl'd from repository heads chosen ad
hoc, and replacing them is where this procedure comes from.

## Failure: "no entry in licence-sources.toml covers <groupId:artifactId>"

A non-Apache-2.0 artifact needs its licence text pinned. Work down this ladder
and stop at the first rung that holds. Record the outcome in
`scripts/licence-sources.toml` (entry: `artifacts`, `jar` or `url`, `sha256`,
plus a `#` comment explaining why this rung — a `note` key would be rejected,
the entry's key set is strict).

1. **The artifact's own jar.** `unzip -Z1 <jar> | grep -iE 'LICEN|COPYING|NOTICE'`
   on the jar from `target/runtime-classpath.txt`. Version-exact and
   publisher-shipped — best provenance. Use `"jar": "<entry path>"`.
2. **The publisher's repository at the tag matching the bundled version.**
   Resolve the tag with the API, never by guessing URL shapes:
   `gh api repos/<owner>/<repo>/git/matching-refs/tags/<prefix> --jq '.[].ref'`.
   Tag names are irregular — measured examples: protobuf Java `4.33.2` lives at
   tag `v33.2` (Java majors are offset by 4); re2j 1.8 at `re2j-1.8`;
   animal-sniffer 1.27 at `animal-sniffer-1.27`. Write the url with a
   `{version}` placeholder where the tag carries the version
   (`re2j-{version}`), so a later dependency bump re-fetches at the new tag
   with no edit to the entry (issue #343); a tag scheme that transforms the
   version first is encoded with `version_strip_prefix` (protobuf:
   `v{version}` plus stripping `4.`). check-notice.py rejects a literal
   tag-pinned url outright.
3. **The publisher's repository head, only if frozen.** Acceptable when the
   repository is archived (`gh api repos/<o>/<r> --jq .archived` → `true`) or
   provably has no version tags. The note must say which and why — e.g. gax and
   google-auth-library-java are archived with no tag for the bundled versions —
   and the entry must declare `version_independent = true`: a url without a
   `{version}` template has to say it is one on purpose.
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

## Failure: "url neither templates {version} nor declares version_independent"

The entry has to say which shape it is (issue #343), and the answer comes from
rung 2/3 evidence, not preference: if the publisher tags releases (resolve
with `matching-refs` as in rung 2), template the tag with `{version}`; only a
ref that never moves — an archived repository's head — may declare
`version_independent = true`, with the note saying why. A live repository
with no version tags is rung 4 territory: take the dependency itself to the
user. The mechanical rejections beside this one (both shapes at once, a
declaration on a `jar` source, an unknown key) need no judgment — the message
names the edit.

## Failure: the fetch check goes red (sha256 mismatch, fetch error, or drift)

`just check-notice-sources` — the weekly notice_sources job, or the per-PR
step on a licence-input change — re-fetched a pinned source and something no
longer holds. This is the human-review moment the pin design deliberately
stops at; never repin blind:

- **"content hash … does not match the pin"** — fetch the URL yourself and
  read the diff against the checked-in text. A copyright-line or formatting
  change from the publisher is a legitimate repin: update `sha256`, run
  `just update-notice`, commit text and pin together, and say in the PR what
  changed. A change to the licence *terms* is a user decision, not a repin.
- **A fetch error (404: deleted or moved tag)** — re-resolve the tag with
  rung 2 and fix the entry; if the repository vanished, work the ladder from
  rung 1 again.
- **"does not start with version_strip_prefix"** — the tag scheme the entry
  encodes no longer holds (protobuf moving off the Java-major offset is the
  anticipated case). Re-derive the scheme with `matching-refs`, and update
  template, prefix and pin together.
- **A diff with every fetch green** — the checked-in NOTICE/licences are
  stale relative to the sources; `just update-notice` per module and review
  what moved.

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
(`./mvnw dependency:tree -Dincludes=<ga>`) and the options; only a user decision
changes the gate, and the gate has no exemption list to add to.

**A dual licence offering a permissive arm is not a way past this.** It reads
like one — take the CDDL arm and the GPL never applies — but a dual licence is
an *offer*, and electing an arm is a statement this project makes to its users,
which then has to be written into the NOTICE and its obligations met
(CDDL-1.0 §3.1's source-availability clause, for one). `javax.annotation-api`
(`CDDL + GPLv2 with classpath exception`) is the worked example, and its answer
was this section's ordinary one rather than a special case: nothing referenced
it, so it was excluded from the bundles (#352, ADR-0015, which carries the
measurement). **Measure before you draft prose** — the artifact that needs no
licence paragraph is the one that is not in the jar.

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
