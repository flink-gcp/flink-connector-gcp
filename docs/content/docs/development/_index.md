---
title: Development
bookCollapseSection: true
weight: 50
---

<!--
Copyright 2026 The flink-gcp authors

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

# Development

Working on the connectors themselves, rather than using them in a job. This page covers the
toolchain and the build.

| | |
|---|---|
| [Testing]({{< relref "docs/development/testing" >}}) | The three test kinds and how to run each |
| [Checks]({{< relref "docs/development/checks" >}}) | The repository-specific checkers CI runs, and the skills that answer their failures |
| [Contributing]({{< relref "docs/development/contributing" >}}) | The issue-first process, pull-request expectations, licensing, and design records |

## Toolchain

- **JDK 17 or 21.** The build targets bytecode 17; Java 11 is not supported — see
  [Supported versions]({{< relref "/" >}}#supported-versions).
- **Maven**, through the included wrapper `./mvnw`.
- **[just](https://just.systems/)** as the command entrypoint and
  **[mise](https://mise.jdx.dev/)** to install the tools the recipes need — `mise install` in
  the repository root installs everything `mise.toml` pins. In a shell without mise activated,
  `mise x -- just <recipe>` behaves the same.
- **Python 3.11 or newer**, for the repository's checker scripts that wrap the build. mise
  installs its pinned 3.12.
- **Docker**, for the emulator-backed integration tests that run inside the ordinary build.

The `justfile` is the command index, and CI calls the same recipes, so what runs locally is what
runs there. `just --list` prints every recipe with a one-line description; this section names
only the handful most changes need, so it stays true when recipes are added.

## Building

The full build — formatting and license checks, unit tests, integration tests, packaging:

```sh
just verify
```

It needs Docker but no Google Cloud credentials. The recipe is a strict header check followed
by `./mvnw verify`, and CI's build lanes run the same recipe — though CI also runs the
[checker suite]({{< relref "docs/development/checks" >}}) alongside it, so a green local build
is necessary rather than sufficient. Two narrower forms serve day-to-day work:

```sh
just verify-module flink-connector-gcp-bigquery   # one module and what it is built on
just verify-flink 1.20.4                          # the full build against another Flink version
```

One source tree supports Flink 2.x and 1.20 together: the few API differences between the
majors live in per-major source roots (`src/main/java-flink1` / `src/main/java-flink2`)
selected by the `flink.compat` Maven property, whose comment in the root `pom.xml` documents
the mechanism. `just verify-flink` selects the seam along with the version, which is the flag
most easily forgotten in a raw Maven call.
[Supported versions]({{< relref "/" >}}#supported-versions) states the user-facing contract and
how the weekly build verifies the range.

Run `just format` before committing — CI fails on unformatted code. Format under a JDK 17:
the inherited build skips Spotless on Java 21 (google-java-format does not run there), so on
a 21-only toolchain both `just format` and `just verify` pass with unformatted Java and the
JDK 17 CI lane fails it.

## Working in an IDE

Import the root `pom.xml` as a Maven project on a JDK 17. Nothing else is required: the
formatter is Spotless, applied by `just format` and enforced by the JDK 17 build, so an IDE
formatter profile is a convenience rather than a correctness requirement; Checkstyle also runs
in the build, and an IDE plugin can point at its configuration in
`tools/maven/checkstyle.xml` to surface violations while editing.
