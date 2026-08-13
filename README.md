# GCP Connectors for Apache Flink

Connectors for using Google Cloud services with [Apache Flink](https://flink.apache.org/):
BigQuery, Cloud Pub/Sub, Cloud Tasks, Bigtable and Spanner.

> **Status: early development.** Nothing is released yet; APIs and coordinates will change.

## Modules

| Module | Description |
|---|---|
| `flink-connector-gcp-bigquery` | BigQuery sink with a unified write API: Storage Write API (at-least-once / exactly-once) and GCS-staged load jobs, with dynamic per-record table destinations and native protobuf serialization |
| `flink-sql-connector-gcp-bigquery` | The BigQuery connector as a single relocated uber-jar, for dropping into Flink's `lib/` |
| `flink-connector-gcp-pubsub` | Cloud Pub/Sub sink (dynamic topic destinations) and source |
| `flink-sql-connector-gcp-pubsub` | The Pub/Sub connector as a single relocated uber-jar, for dropping into Flink's `lib/` |
| `flink-connector-gcp-cloudtasks` | Cloud Tasks sink |
| `flink-sql-connector-gcp-cloudtasks` | The Cloud Tasks connector as a single relocated uber-jar, for dropping into Flink's `lib/` |
| `flink-connector-gcp-bigtable` | Bigtable sink, bounded scan source, and Change Streams source (implemented in #35) |
| `flink-sql-connector-gcp-bigtable` | The Bigtable connector as a single relocated uber-jar, for dropping into Flink's `lib/` |
| `flink-connector-gcp-spanner` | Spanner sink and bounded source (both dialects), with Table API / SQL sink, scan, and lookup support |
| `flink-sql-connector-gcp-spanner` | The Spanner connector as a single relocated uber-jar, for dropping into Flink's `lib/` |

## Build

Requires JDK 17 or 21 and Maven (or use the included wrapper):

```sh
./mvnw verify
```

That build is also `just verify`, which is what CI runs. The commands this repository is worked
with — the build, formatting, linting, the documentation site, the binary-compatibility check —
are recipes in the `justfile`, and the workflows call the same ones. Run `just --list` for the
index; [mise](https://mise.jdx.dev/) installs the tools they need.

`main` supports **Flink 2.2 and 2.3**, mirroring Flink's own policy of supporting the current and
the previous minor release, and additionally builds against **Flink 1.20** (the 1.x LTS) from
the same source — `just verify-flink 1.20.4` builds and tests for it.
See [Supported versions](docs/content/_index.md#supported-versions) for how the range is verified
and why a single artifact covers it.

## Getting started

[Quickstart](docs/content/docs/quickstart/_index.md) installs the artifacts from this build and
sets up credentials, then has one complete job per connector;
[Examples](docs/content/docs/examples/_index.md) covers dynamic destinations, exactly-once,
auto-creation and emulator-backed local runs; and the
[configuration reference](docs/content/docs/reference/_index.md) lists every option each connector
takes, with its default.

## License

[Apache License 2.0](LICENSE)

## Disclaimer

This is an independent open-source project. It is not affiliated with, endorsed by, or
supported by the Apache Software Foundation or Google. Apache Flink, Flink, and the
Flink logo are trademarks of the Apache Software Foundation.

## Acknowledgements

- Development of this project is assisted by [Claude](https://claude.com/) under
  [Claude for OSS](https://claude.com/contact-sales/claude-for-oss), Anthropic's support program
  for open-source developers.
- Development of this project is also assisted by [Codex](https://openai.com/codex/) under
  [Codex for Open Source](https://openai.com/form/codex-for-oss/), OpenAI's support program for
  open-source maintainers.
