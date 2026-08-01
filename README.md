# GCP Connectors for Apache Flink

Connectors for using Google Cloud services with [Apache Flink](https://flink.apache.org/):
BigQuery, Cloud Pub/Sub and Cloud Tasks, with Bigtable and Spanner planned.

> **Status: early development.** Nothing is released yet; APIs and coordinates will change.

## Modules

| Module | Description |
|---|---|
| `flink-connector-gcp-bigquery` | BigQuery sink with a unified write API: Storage Write API (at-least-once / exactly-once) and GCS-staged load jobs, with dynamic per-record table destinations and native protobuf serialization |
| `flink-connector-gcp-pubsub` | Cloud Pub/Sub sink (dynamic topic destinations) and source |
| `flink-sql-connector-gcp-pubsub` | The Pub/Sub connector as a single relocated uber-jar, for dropping into Flink's `lib/` |
| `flink-connector-gcp-cloudtasks` | Cloud Tasks sink |

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

[Quickstart](docs/content/docs/quickstart.md) installs the artifacts from this build, sets up
credentials and runs one complete job per connector;
[Examples](docs/content/docs/examples.md) covers dynamic destinations, exactly-once,
auto-creation and emulator-backed local runs.

## License

[Apache License 2.0](LICENSE)

## Disclaimer

This is an independent open-source project. It is not affiliated with, endorsed by, or
supported by the Apache Software Foundation or Google. Apache Flink, Flink, and the
Flink logo are trademarks of the Apache Software Foundation.

## Acknowledgements

Development of this project is assisted by [Claude](https://claude.com/) under
[Claude for OSS](https://claude.com/contact-sales/claude-for-oss), Anthropic's support program
for open-source developers.
