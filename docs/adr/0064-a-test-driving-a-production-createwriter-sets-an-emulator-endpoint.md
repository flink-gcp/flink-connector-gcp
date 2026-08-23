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

# ADR-0064: A test driving a production `createWriter` sets an emulator endpoint

- Status: Accepted
- Date: 2026-08-02 (measured on PR
  [#242](https://github.com/flink-gcp/flink-connector-gcp/pull/242), under [#209])
- Issues: [#209]
- Modules: all connectors (tests)
- Current behavior: root `AGENTS.md` § Cross-connector contracts

## Decision

**Any test that drives a sink's production `createWriter(WriterInitContext)` builds that
connector's real client, and must set an emulator endpoint** — `emulatorEndpoint("localhost:1")`
or any unused port works, since every connector's endpoint path takes `NoCredentialsProvider`
plus a plaintext channel and nothing is dialled until a record is written (BigQuery's builder
takes two endpoints, one per transport — ADR-0029). Where the client is constructed eagerly,
the bare production path demands application-default credentials, so the test passes on any
machine with ADC configured and fails in CI with *"Your default credentials were not found"* —
local `just verify` cannot catch it, only CI can. Swapping in the injecting seam instead would
defeat the test's point, which is the production path itself. Say in a comment why the endpoint
is not optional, or a later simplification pass removes it.

## Evidence

Measured on Cloud Tasks (PR
[#242](https://github.com/flink-gcp/flink-connector-gcp/pull/242), under [#209]):
`DefaultTaskCreatorFactory.create()` builds its client eagerly, so the production-path metric
test passed two local runs and then failed in CI. The Pub/Sub twin of the same test passed
without an endpoint only because that sink creates its publishers lazily — a property of that
sink, not a general rule, which is why the contract binds every connector rather than the ones
that happen to authenticate during `createWriter`.

[#209]: https://github.com/flink-gcp/flink-connector-gcp/issues/209
