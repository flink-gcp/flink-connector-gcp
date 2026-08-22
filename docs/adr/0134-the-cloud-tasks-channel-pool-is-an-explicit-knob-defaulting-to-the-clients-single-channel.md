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

# ADR-0134: The Cloud Tasks channel pool is an explicit knob defaulting to the client's single channel

- Status: Accepted
- Date: 2026-08-22
- Issues: [#1015], [#937]
- Modules: cloudtasks
- Current behavior: `docs/content/docs/connectors/datastream/cloudtasks.md` § Tuning and § Queues,
  rate limits and sink concurrency

## Context

The [#937] measurement ([ADR-0129]) found that the Cloud Tasks writer's create throughput is
capped by the transport, not by its in-flight cap. gax's `InstantiatingGrpcChannelProvider`
defaults to `ChannelPoolSettings.staticallySized(1)`, and one HTTP/2 channel carries ~100
concurrent streams — so of the writer's nominal 1,000 in-flight creates (`maxInFlightTasks`'
default), at most ~100 are on the wire and the rest queue client-side. Measured against a real
paused queue with `google-cloud-tasks` 2.95.0 on 2026-08-22 — 10,000 unnamed HTTP tasks per arm
from one process under a 1,000-task in-flight bound: one process sustained 210 creates/s
on the default transport, a rate the reasoning above says no higher cap can lift, and
1,271 creates/s with
`ChannelPoolSettings.staticallySized(8)`, with per-RPC p50 ≈ 50 ms at low concurrency. The
documentation's throughput formula ("sink parallelism × the in-flight cap") was missing the
transport term entirely.

[#1015] asked whether `DefaultTaskCreatorFactory` should configure a channel pool, and whether its
size should be derived (for example `maxInFlightTasks / 100`, the streams-per-channel quotient) or
a builder knob.

## Decision

`CloudTasksWriterOptions` gains an explicit `channelPoolSize` knob (Table API:
`sink.channel-pool-size`). Unset — the default — leaves the client's own transport configuration
alone, which is one channel; set, the factory replaces the production transport provider with the
service's default provider builder resized to `ChannelPoolSettings.staticallySized(n)`. Starting
from the service's builder preserves the one provider-level default a bare builder would lose —
the unbounded max inbound message size — while the endpoint and credentials are applied by the
client context regardless of which builder produced the provider.

The pool size is **not derived** from `maxInFlightTasks`:

- **A derived default silently changes every existing job.** At the defaults, `1000 / 100` yields
  a 10-channel pool, and the measured 8-channel figure (1,271 creates/s) already exceeds the
  queue's recommended ~1,000 TPS ceiling for creates plus dispatches. A default that silently
  ramps a subtask past a vendor recommendation — and past the documented 500/50/5 ramp rule —
  invites the delivery-latency degradation the recommendation exists to prevent. The pipeline
  this connector exists for throttles *down* to a third-party limit and may never want more than
  one channel; extra channels are idle connections it pays for.
- **The quotient is limit arithmetic over numbers this project does not own.** The
  ~100-streams-per-channel figure is transport behavior that a gRPC or gax release can move, and
  [ADR-0070] is the precedent that a default is chosen from a measured band, not computed from a
  documented limit. An explicit knob keeps the arithmetic — and the responsibility for the ramp —
  with the operator who has read the queue's numbers.
- **`maxInFlightTasks` bounds memory, not desired throughput.** The cap exists to bound sink
  state between checkpoints (ADR-0048); a job may legitimately run a high cap for burst
  absorption while wanting no more wire concurrency than one channel provides.

Two standing rules are refined, not broken, by this knob:

- "Retries are the sink's one owned loop, never gax `createTaskSettings`" (ADR-0048) is about gax
  *policy* — retry and batching behavior. Transport *sizing* carries no policy: it changes how
  many connections exist, not what is sent or retried. gax retry and batching configuration
  remain untouched.
- The emulator arm is unaffected ([ADR-0081]): it keeps its single caller-owned plaintext channel
  and `shutdownNow()` ownership model. Configuring `channelPoolSize` beside an emulator endpoint
  is **rejected** — at the sink builder's `build()` and at the table factory's validation — rather
  than silently ignored, matching the module's credential-conflict precedent: a pool tuned against
  the emulator would measure nothing and carry a false baseline into production sizing.

## Alternatives declined

- **Derived pool (`ceil(maxInFlightTasks / 100)`), capped or not** — declined for the reasons
  above; the capped variant additionally needs a cap constant this project would then own and
  re-measure.
- **No knob, documentation only** — declined because parallelism would be the only lever past
  ~210 creates/s per subtask, and subtasks are a far more expensive unit (task slots, state,
  checkpoint traffic) than channels for a purely transport-bound ceiling the service demonstrably
  sustains.

## Consequences

- Default behavior is byte-for-byte unchanged: unset options never touch the transport provider.
- The documentation's throughput formula carries the transport term — per-subtask concurrency is
  min(`maxInFlightTasks`, `channelPoolSize` × ~100 streams) — and the tuning section carries the
  ramp caution beside the knob.
- The measured figures are bound to `google-cloud-tasks` 2.95.0 and 2026-08-22 service behavior
  (ADR-0129 records the harness); the ~100-streams-per-channel quotient is gRPC transport
  behavior that can move independently. Neither invalidates the design — the knob's default
  deliberately does not encode either number.
- This is the repository's first configured `ChannelPoolSettings`; a connector wanting the same
  lever starts from this record and ADR-0081's ownership constraint.

[#937]: https://github.com/flink-gcp/flink-connector-gcp/issues/937
[#1015]: https://github.com/flink-gcp/flink-connector-gcp/issues/1015
[ADR-0070]: 0070-the-staging-roll-threshold-is-a-measured-throughput-band-not-limit-arithmetic.md
[ADR-0081]: 0081-emulator-transport-is-shared-by-channel-ownership-not-by-settings-type.md
[ADR-0129]: 0129-the-cloud-tasks-sink-keeps-one-create-rpc-per-record-and-declines-v2beta3-batchcreatetasks.md
