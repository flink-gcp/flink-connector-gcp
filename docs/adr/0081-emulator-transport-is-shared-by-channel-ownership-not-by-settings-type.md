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

# ADR-0081: Emulator transport is shared by channel ownership, not by settings type

- Status: Accepted
- Date: 2026-08-10 ([#438])
- Issues: [#438]
- Modules: base, bigquery, pubsub, cloudtasks
- Current behavior: (internal plumbing; nothing user-rendered)

## Decision

`base.rpc.EmulatorChannels` holds the three calls that point a gRPC client at an emulator, and it
is split **by who owns the channel** rather than by which settings type a caller happens to hold:

- `plaintextProvider(InstantiatingGrpcChannelProvider.Builder, EmulatorEndpoint)` — for the five
  sites whose client creates and closes its own channel. **The builder is a parameter**, not
  something the helper creates, so the API's own transport defaults survive (see Evidence). It
  applies the plaintext hook through the `@BetaApi` `setChannelConfigurator` — an internal call,
  tier-irrelevant under
  [ADR-0141](0141-a-surfaces-stability-tier-is-set-by-what-can-reshape-its-inputs-and-outputs.md),
  whose inventory records it.
- `openPlaintextChannel(EmulatorEndpoint)` + `fixedProvider(ManagedChannel)` — for the three sites
  that own the channel themselves and shut it down deliberately, one of them gracefully.

Three things stay outside the helper, each for a checked reason.

- **`setCredentialsProvider(NoCredentialsProvider.create())` stays at every call site.** The setter
  lives on `ClientSettings.Builder`, `Publisher.Builder` and `Subscriber.Builder`; `javap` on gax
  2.82.0 shows the latter two are `public final class` extending `Object`, so the three share no
  supertype a single signature could take. The class javadoc states the whole rule — plaintext
  *and* no credentials — so the knowledge is in one place even though one call is not.
- **The ownership models are not unified.** `shouldAutoClose()` is `true` for an instantiating
  provider and `false` for a fixed one, and the three caller-owned sites depend on the difference:
  `DefaultTaskCreatorFactory` and `DefaultPublisherFactory` `shutdownNow()` the channel from their
  adapters, `PubSubDeadLetterQueue` shuts it down gracefully as a later entry in its `close()`
  list, and `PubSubTestClients` shares one channel across three clients. Collapsing them onto one
  provider kind would be a lifecycle change wearing a refactor's clothes.
- **`PubSubTestClients` keeps its hand-built version.** `flink-connector-gcp-base` depends on
  `flink-connector-gcp-test-utils` at test scope, so the reverse dependency would be a cycle; and
  that class's javadoc rests on everything in it being stock `com.google.*` types, which is what
  lets the SQL module drive the emulator with it while the connector under test uses its relocated
  copies. It is the ninth site [#438] counts and the one that cannot be reached.

`base` gains `com.google.api:gax-grpc` at compile scope for `InstantiatingGrpcChannelProvider` and
`GrpcTransportChannel`. Both SQL uber-jars already bundle that artifact, so their `NOTICE` files
are unchanged — verified with `just check-notice` on both rather than assumed.

Products whose SDK offers an emulator entry point never come here: Bigtable's
`newBuilderForEmulator(host, port)` and Spanner's `setEmulatorHost` each switch the channel and the
credentials in one call, and BigQuery's REST admin takes a URL through `BigQueryOptions.setHost`.

**What this does not close.** `plaintextProvider` accepts any provider builder, so a new site can
still hand it a bare `InstantiatingGrpcChannelProvider.newBuilder()` and lose the API's defaults
exactly as the two BigQuery sites did. The helper cannot detect that — it has no way to know what
a given API's defaults are — so the guard is the rule in `flink-connector-gcp-base/CLAUDE.md`
rather than code. A checker was not written: the failure is emulator-only and the population is
small enough that a rule a session reads is proportionate.

## Context

[#438] described eight sites "each repeating the same three calls on a gax `ClientSettings.Builder`"
and proposed one helper taking that type. Both halves turned out to be wrong about the tree, which
is why the shape here is not the shape the issue asked for.

The eight sites use **four** idioms, and only two of them match the issue's description:

| Idiom | Sites | Builder's base class |
|---|---|---|
| Bare `InstantiatingGrpcChannelProvider.newBuilder()`, endpoint on the settings *and* the provider | `BigQueryReadClients`, `BigQueryWriteClients` | `ClientSettings.Builder` |
| The API's `defaultGrpcTransportProviderBuilder()`, no settings-level endpoint | `PubSubTopicAdmin`, `PubSubSubscriptionAdmin` | `ClientSettings.Builder` |
| The same, but configuring a `Subscriber.Builder` | `DefaultSubscriberFactory` | none — `final`, extends `Object` |
| Caller-owned `ManagedChannel` + `FixedTransportChannelProvider` | `DefaultTaskCreatorFactory`, `DefaultPublisherFactory`, `PubSubDeadLetterQueue` | first is `ClientSettings.Builder`; the other two, none |

A `ClientSettings.Builder<?, ?>`-typed helper therefore reaches five of the eight and misses
`DefaultSubscriberFactory` entirely — while a helper typed on the *provider builder*, as this one
is, reaches all five client-owned sites including that one. The "endpoint has to be set twice"
fact the issue leads with is stated in exactly two files, the two using a bare provider builder,
and it is a consequence of that choice rather than a property of gax.

## Evidence

**The bare provider builder was dropping the API's inbound message limit** (measured 2026-08-09,
`javap` on the jars the pom resolves — google-cloud-bigquerystorage 3.30.0, google-cloud-pubsub
1.152.0, gax 2.82.0 via libraries-bom 26.85.1; one run):

```text
BigQueryReadStubSettings.defaultGrpcTransportProviderBuilder()
  = InstantiatingGrpcChannelProvider.newBuilder().setMaxInboundMessageSize(2147483647)
BigQueryWriteStubSettings.defaultGrpcTransportProviderBuilder()   … the same
SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
  = … .setMaxInboundMessageSize(20971520)
```

Both BigQuery sites built their provider from a bare `newBuilder()`, so the emulator path ran at
gRPC's 4 MiB default instead of `Integer.MAX_VALUE` — a `RESOURCE_EXHAUSTED` waiting for the first
read batch or response above it, on the emulator only (production goes through
`BigQueryReadClient.create()`, which uses the API's defaults). Moving both to
`<Api>Settings.defaultGrpcTransportProviderBuilder()` closes that gap and removes the double
`setEndpoint` in the same edit, since the provider now carries the endpoint and gax pushes the
settings' endpoint onto a provider only when the provider has none. The limit is pinned by
`BigQueryReadClientsTest` / `BigQueryWriteClientsTest`, reading it back through the public
`InstantiatingGrpcChannelProvider.toBuilder()`.

**Dropping the settings-level endpoint at the two BigQuery sites is safe**: every BigQuery emulator
ITCase passes with the provider carrying it alone (`just verify`, 2026-08-10), which is the same
arrangement the four Pub/Sub sites have always used.

## Alternatives declined

- **One helper typed on `ClientSettings.Builder<?, ?>`, as [#438] proposed.** Reaches five of eight
  sites, misses `DefaultSubscriberFactory`, and would have to build the transport provider itself —
  which is exactly what loses the API defaults measured above.
- **Unify all eight on the instantiating provider.** Changes who closes the channel at three sites
  that shut it down on their own schedule, one of them gracefully rather than immediately, and
  removes `PubSubTestClients`' ability to share one channel across clients.
- **Fold `NoCredentialsProvider` in via an overload per builder type.** Three overloads naming
  three client-library types would put `google-cloud-pubsub` and `google-cloud-tasks` on `base`'s
  compile classpath, which is the dependency direction `base` exists to avoid.

[#438]: https://github.com/flink-gcp/flink-connector-gcp/issues/438
