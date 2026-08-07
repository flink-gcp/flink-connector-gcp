<!--
Copyright 2026 laughingman7743

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

# ADR-0013: The Pub/Sub emulator never answers the keepalive ping, so idle streams cycle

- Status: Accepted (a measured harness property, recorded so failing-run logs are read
  correctly)
- Date: 2026-08-03 (measured on `google-cloud-pubsub` 1.152.0, four runs, while investigating
  [#244])
- Issues: [#244]
- Modules: pubsub (test harness only; real Pub/Sub answers the ping)

## Evidence

`StreamingSubscriberConnection` sends an empty `StreamingPullRequest` every 30 s and closes the
stream when the last ping is unanswered for ≥15 s; against the emulator that is *every* ping, so
an idle stream logs `No response from server for 20 seconds since last ping. Closing stream.` at
~50 s after open and then roughly every 20 s (the first cycle is longer because the stream's own
opening response answers the ping sent at open).

## Consequences

- The line is **routine on an idle emulator stream, not a fault** — it says only that the stream
  received nothing, which any subscription with no messages satisfies; healthy emulator ITs
  never show it because none of them idles that long, which is also why its appearance in a
  *failing* run is worth reading.
- Simultaneous idle streams fire **together, within milliseconds** (measured: two streams, lines
  5 ms apart, at 50045/50050 ms and 50025/50028 ms across two runs), so the *spacing* of the
  lines carries information the count does not — two lines tens of seconds apart are not two
  streams idling in parallel.
- A stream reset this way still delivers normally: publishing after an idle window, messages
  arrived in 105 ms and 104 ms. That measurement is what makes prolonged silence on a
  subscription with a backlog abnormal rather than expected.

[#244]: https://github.com/laughingman7743/flink-connector-gcp/issues/244
