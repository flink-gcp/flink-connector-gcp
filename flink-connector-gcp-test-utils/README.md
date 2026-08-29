# Flink Connectors GCP : Test Utils

Shared test-support code for the connector modules' test suites ([#27](https://github.com/flink-gcp/flink-connector-gcp/issues/27)):
no-op sink-writer contexts, a queue-backed mailbox executor, random resource-name helpers for the
real-GCP gated tests, deadline-bounded polling/draining helpers, synthetic service-account key
files for credential wiring tests, collecting source and reader
outputs for driving a source reader ([#437](https://github.com/flink-gcp/flink-connector-gcp/issues/437)),
and per-service emulator fixtures that keep shared image pins in one place and, where multiple
harnesses need them, own stock clients.

Consumed by the sibling modules at `test` scope only. This module is internal test infrastructure:
it is not part of any connector's public API, carries no compatibility guarantee, and is not
intended for use outside this repository.

## Provenance and attribution

This module contains test support extracted from sibling connector modules.
It also contains shared infrastructure written directly for their test suites.
No source code from another project has been copied or adapted into this module.
Its current source files carry only this project's copyright holder, and the
repository `NOTICE` records no externally adapted code in this module.
