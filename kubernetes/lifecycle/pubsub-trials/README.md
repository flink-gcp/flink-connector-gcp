# Reviewed Pub/Sub trials

A `pubsub-recovery` dispatch names one file in this directory, without `.toml`, through the run workflow's `pubsub_trial` input.
Each file holds the [offline trial schema](../../apps/pubsub/README.md#offline-trial-proposal) as TOML beside the Apache licence header that apache-rat requires.
The approved commit must contain the file: dispatch refuses an untracked TOML file under `kubernetes/`.

No trial is committed yet.
The campaign's trials, their numbers and their stop conditions are preregistered under [#1434](https://github.com/flink-gcp/flink-connector-gcp/issues/1434).
A file here is a runnable trial once admission opens, so the example the tests use lives in `tools/tier3/tests/fixtures/pubsub-trials/` instead, where no dispatch can name it.
