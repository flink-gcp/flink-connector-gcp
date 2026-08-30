# GCP Connectors for Apache Flink

Connectors for using Google Cloud services with [Apache Flink](https://flink.apache.org/):
BigQuery, Cloud Pub/Sub, Cloud Tasks, Bigtable and Spanner.

> **Status: released.** Artifacts are on
> [Maven Central](https://central.sonatype.com/namespace/io.github.flink-gcp) under the
> `io.github.flink-gcp` namespace, in two version lines per release: `1.0.0` for the supported
> Flink 2.x range and `1.0.0-1.20` for the Flink 1.20 LTS. The SQL uber-jars are also attached
> to the [GitHub releases](https://github.com/flink-gcp/flink-connector-gcp/releases).

## Modules

| Module | Description |
|---|---|
| `flink-connector-gcp-bigquery` | BigQuery sink with a unified write API: Storage Write API (at-least-once / exactly-once) and GCS-staged load jobs, with dynamic per-record table destinations, native protobuf serialization, and Table API / SQL sink and source support |
| `flink-sql-connector-gcp-bigquery` | The BigQuery connector as a single relocated uber-jar, for dropping into Flink's `lib/` |
| `flink-connector-gcp-pubsub` | Cloud Pub/Sub sink (dynamic topic destinations) and source, with Table API / SQL sink and source support |
| `flink-sql-connector-gcp-pubsub` | The Pub/Sub connector as a single relocated uber-jar, for dropping into Flink's `lib/` |
| `flink-connector-gcp-cloudtasks` | Cloud Tasks sink, with a Table API / SQL sink |
| `flink-sql-connector-gcp-cloudtasks` | The Cloud Tasks connector as a single relocated uber-jar, for dropping into Flink's `lib/` |
| `flink-connector-gcp-bigtable` | Bigtable sink, bounded scan source, and Change Streams source, with Table API / SQL sink, scan, lookup, and change-stream CDC support |
| `flink-sql-connector-gcp-bigtable` | The Bigtable connector as a single relocated uber-jar, for dropping into Flink's `lib/` |
| `flink-connector-gcp-spanner` | Spanner sink, bounded source and Change Streams source (both dialects), with Table API / SQL sink, scan, lookup, and change-stream CDC support |
| `flink-sql-connector-gcp-spanner` | The Spanner connector as a single relocated uber-jar, for dropping into Flink's `lib/` |

## Build

Requires JDK 17 or 21 and Maven (or use the included wrapper):

```sh
./mvnw verify
```

CI runs the same build as `just verify`. The toolchain and the day-to-day recipes are covered
by [Development](https://flink-gcp.github.io/flink-connector-gcp/docs/development/) on the
documentation site.

## Supported versions

| | Supported |
|---|---|
| Apache Flink | 2.2, 2.3, and 1.20 (LTS) |
| Java | 17, 21 |

The Flink range mirrors Flink's own support policy — the current and the previous minor
release — so a new Flink minor moves both ends of the range. Moving it is a deliberate change
backed by a weekly build over every supported version, never an automatic bump. Flink 1.20 is
supported from the same source (`just verify-flink 1.20.4` builds and tests for it) and its
lane is verified on Java 17 — the Java 21 row is a 2.x claim. Java 11 is not supported even
though Flink 2.x declares it.
See [Supported versions](https://flink-gcp.github.io/flink-connector-gcp/#supported-versions)
for how the range is verified and why a single artifact covers the 2.x range.

## Getting started

[Quickstart](https://flink-gcp.github.io/flink-connector-gcp/docs/quickstart/) puts the
connectors on a job's classpath and sets up credentials, then has one complete job per connector;
[Examples](https://flink-gcp.github.io/flink-connector-gcp/docs/examples/) covers dynamic
destinations, exactly-once, auto-creation and emulator-backed local runs; and the
[configuration reference](https://flink-gcp.github.io/flink-connector-gcp/docs/reference/) lists
every option each connector takes, with its default.

## Contributing

Beyond a trivial fix, contributions start with an issue rather than a pull request — see
[CONTRIBUTING.md](CONTRIBUTING.md) and the
[Contributing](https://flink-gcp.github.io/flink-connector-gcp/docs/development/contributing/)
page on the documentation site.

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
