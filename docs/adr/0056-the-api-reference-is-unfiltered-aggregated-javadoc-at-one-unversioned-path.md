<!--
Copyright 2026 laughingman7743

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# ADR-0056: The API reference is unfiltered aggregated JavaDoc at one unversioned path

- Status: Accepted
- Date: 2026-08-01 ([#88])
- Issues: [#88], [#93] (the deploy job that publishes it), [#39] (per-release references wait
  there)
- Modules: all (docs)

## Context

The API reference is the documentation site's generated half ([#88]): `just docs-javadoc`
aggregates JavaDoc across every module into `docs/static/api/java`, which Hugo copies
verbatim, so it is part of the Pages artifact the moment [#93] adds a deploy job that runs it
before uploading. It is never committed (gitignored, rat-excluded), and pages link to it with
`{{< param ApiDocsURL >}}` — a param rather than a `relref` because the output is not Hugo
*content*, and not `Book*`-prefixed because that namespace is hugo-book's.

## Decision

Three sub-decisions, the first two measured rather than assumed:

- **Nothing is filtered by API tier.** `@Internal` is `@Documented`, so the tier is a badge on
  every class page, and using an `@Internal` type is the caller's risk — a consumer can audit
  tiers mechanically exactly as `just check-flink-api-tiers` does against Flink. Filtering was
  priced and declined: package-level exclusion would still leave 32 `@Internal` files
  documented, because 12 packages mix tiers; `sourceFileExcludes` drops files from the *source
  path* and so breaks resolution of public signatures that name them; and a doclet buys
  zero-maintenance filtering at a cost this project has no reason to pay. Apache Flink
  publishes unfiltered too.
- **Doclint stays off, `failOnWarnings` is on instead.** The parent supplies `-Xdoclint:none`
  through `<additionalJOptions>` — and it turns out not to be the check worth having. JavaDoc
  resolves `{@link}` itself rather than through doclint, so an unresolvable reference is
  reported regardless (two existed when this landed, both in `JsonDocumentSerializerOptions`,
  left behind by [#125]'s fully-qualified-link rule); a reference the reader cannot follow is
  what a published reference must be free of, a missing `@param` is not. Nothing links out
  through a fetched index: no `<links>`, which is the only setting here that would probe a
  remote site, and `detectJavaApiLink` off. That last one costs no links — the JDK cross-links
  come from the doclet's own automatic platform links, so the count is identical either way;
  what the default adds is a second mechanism for them, against an element-list the plugin
  unpacks from its own jar, bundled for Java 10–15 only.
- **One unversioned path, tracking `main`.** Per-release references wait for artifact
  publishing, which is [#39]'s scope; javadoc.io serves released versions from Central for
  free once that happens.

## Consequences

- `just docs-javadoc` is **the one correct bare goal in this repository**; the exemption from
  the licence-goal-through-a-phase rule is argued, and measured, in the justfile.
- `docs.yaml`'s push-side paths filter carries the main sources and the poms since [#88] made
  the API reference part of the site, and the workflow takes java from `mise.toml` rather
  than adding a second JDK installer for the shim rule (ADR-0057) to have to disarm.

[#39]: https://github.com/laughingman7743/flink-connector-gcp/issues/39
[#88]: https://github.com/laughingman7743/flink-connector-gcp/issues/88
[#93]: https://github.com/laughingman7743/flink-connector-gcp/issues/93
[#125]: https://github.com/laughingman7743/flink-connector-gcp/issues/125
