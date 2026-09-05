---
title: Testing
type: docs
weight: 10
---

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

# Testing

The suites fall into three kinds, split by what they need to run: nothing beyond Docker, real
Google Cloud credentials, or wall clock. `just verify` runs the first kind and excludes the
other two, so an ordinary build is credential-free and its duration is bounded.

## Unit and integration tests

Name a unit test `*Test` and an integration test `*ITCase`. The build selects by the first
suffix alone: a class ending in `Test` runs in the unit execution, and every other test class
lands in the integration execution — an unconventional name does not escape the build, it runs
in the wrong lane. Both kinds run in `just verify`. The integration tests
talk to Docker-backed service emulators, so Docker must be running, but no Google Cloud project
is touched. An emulator is a convenience rather than an authority: the deviations that have
been measured are recorded on the connector pages, and the real service is exercised by the
gated suites below.

While iterating, scope the run to what changed rather than rebuilding the reactor — and keep
`-am`, so the sibling modules come from the working tree rather than from whatever jars an
earlier build installed. For a single-module change, the per-connector CI lane carries the full
verification:

```sh
./mvnw test -pl flink-connector-gcp-pubsub -am -Dtest=PubSubSinkBuilderTest -Dsurefire.failIfNoSpecifiedTests=false
just verify-module flink-connector-gcp-pubsub    # the module-wide build, when a broader check is wanted
```

## Emulator image updates

The `Renovate emulator images` workflow proposes a grouped draft pull request on the first day of each month at 03:23 UTC.
It updates the image constants in test-utils; Bigtable and Pub/Sub share a pin and move together.
Review the Bigtable deviation suites' verdict and, for a Spanner bump, recheck the measurements named in the pull request before merging.
A green emulator suite does not establish real-service behavior.

For an initial run or recovery, dispatch the workflow on `main`:

```sh
gh workflow run renovate.yaml --repo flink-gcp/flink-connector-gcp --ref main
gh run list --repo flink-gcp/flink-connector-gcp --workflow renovate.yaml
```

Check the run's logs for lookup failures and inspect any resulting update pull request and its CI runs.
Missing App credentials, reported Docker package lookup failures, missing or invalid repository configuration, and the `Host error` / `Git error - aborting` paths fail the job.
An up-to-date set of images can finish successfully without opening a pull request; an existing update pull request can be refreshed instead.
The monthly run does not replace reviewing and merging updates, and no separate retention checker watches a pin left unmerged.

## Credential-gated suites

A test that talks to real Google Cloud carries `@Tag("gated")` together with an
environment-variable gate naming the project it bills. Ordinary builds exclude the tag, so
`just verify` never runs these — even in a shell that holds the variables. The opt-in is:

```sh
just e2e
```

It needs an authenticated `gcloud` CLI besides the environment gates — the one tool this
page names that mise does not install; the App Engine fixture and `just sweep-e2e` both call
it. It refuses to start unless the environment gates are set, runs the gated classes its five
per-service gates select, and asserts afterwards that they actually ran, so a silently skipped
suite cannot pass as a green one. A few deliberately manual cases — slow schema-propagation
observations — sit behind gates of their own, outside `just e2e`.
These suites create and delete billed resources; each run costs real money, which is why CI
runs them on a weekly schedule (plus manual dispatch) rather than per pull request.

The Google Cloud project behind them is provisioned in two layers. The persistent,
idle-cost-free layer — service accounts, Workload Identity Federation, buckets, the dataset —
is OpenTofu under
[`opentofu/`]({{< param BookRepo >}}/tree/main/opentofu), whose README documents the layout,
the CI plan/apply flow and the security model (no service-account keys; local runs use your own
application-default credentials). The runs themselves create what they test against — tables,
topics, subscriptions and queues, but also one ephemeral Bigtable or Spanner *instance* per
gated class and a briefly started App Engine version for Cloud Tasks, which are the resources
that cost money while they stand. A completed run deletes them;
`just sweep-e2e` returns what an interrupted run left standing — stale instances, the serving
App Engine version — to its idle state. Its default spares instances younger than two hours
(on a schedule, age is what tells a leak from a run still using its instance), so cleaning up
right after an interrupted local run means `just sweep-e2e --all` — which drops that
safeguard entirely, so say it only when nothing else, scheduled or local, is using the
project. The environment gates come
from an uncommitted `.env` at the repository root; `just worktree-env` makes it reachable from
a git worktree.

## The slow lane

A test whose duration *is* the instrument — an elapsed-time observation that cannot be tuned
down without destroying what it measures — carries `@Tag("slow")`. It is excluded from
`just verify` alongside the gated tag and runs in the weekly workflow instead, so the per-change
build stays fast without the coverage silently disappearing. Running it locally means narrowing
the exclusion back to the gated tag alone:

```sh
just verify -Dtest.excluded.groups=gated
```
