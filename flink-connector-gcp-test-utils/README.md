# Flink Connectors GCP : Test Utils

Shared test-support code for the connector modules' test suites ([#27](https://github.com/laughingman7743/flink-connector-gcp/issues/27)):
no-op sink-writer contexts, a queue-backed mailbox executor, random resource-name helpers for the
real-GCP gated tests, and deadline-bounded polling/draining helpers.

Consumed by the sibling modules at `test` scope only. This module is internal test infrastructure:
it is not part of any connector's public API, carries no compatibility guarantee, and is not
intended for use outside this repository.
