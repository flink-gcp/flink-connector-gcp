# Flink Connectors GCP : Test Utils

Shared test-support code for the connector modules' test suites ([#27](https://github.com/laughingman7743/flink-connector-gcp/issues/27)):
no-op sink-writer contexts, a queue-backed mailbox executor, random resource-name helpers for the
real-GCP gated tests, deadline-bounded polling/draining helpers, collecting source and reader
outputs for driving a source reader ([#437](https://github.com/laughingman7743/flink-connector-gcp/issues/437)),
and the Pub/Sub test harness — the emulator container image and admin/publish/pull clients
parameterised over the transport (emulator channel or application-default credentials).

Consumed by the sibling modules at `test` scope only. This module is internal test infrastructure:
it is not part of any connector's public API, carries no compatibility guarantee, and is not
intended for use outside this repository.
