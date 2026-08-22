---
title: Connectors
bookCollapseSection: true
weight: 30
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

# Connectors

Google Cloud connectors for Apache Flink.

## Choosing an API

The Table connectors map onto the DataStream connectors with the same names, but the two APIs
describe jobs at different levels.

| API | Choose it when |
|-----|----------------|
| [DataStream API]({{< relref "docs/connectors/datastream" >}}) | A Java job needs connector builders, connector-specific serialization or deserialization schemas, per-record destinations, or a connector feature with no SQL option |
| [Table API and SQL]({{< relref "docs/connectors/table" >}}) | A job is expressed as relational tables and needs DDL, SQL type mappings, or metadata columns |

The Table page for a connector documents its DDL, SQL types, metadata columns, and planner-specific
restrictions.
The corresponding DataStream page documents the underlying runtime behavior and builder-only
features.

Start with [Delivery guarantees]({{< relref "docs/connectors/delivery-guarantees" >}}) when
checkpoint durability, replay behavior, or exactly-once delivery determines which sink method to
use.

The connector types named on these pages are documented in the
[Java API reference]({{< param ApiDocsURL >}}).

## What a builder checks

A setter rejects a value when doing so tells you more than the service's own refusal would:

| Rejected when you set it | Which values |
|---|---|
| Missing, or `null` | Every required option |
| Empty, or nothing but whitespace | Every configured name, id or file path. Row-key values are the exception: an empty `prefix` means "scan the whole table" |
| A `/`, or leading or trailing whitespace | A component the connector concatenates into a resource path: `project`, `dataset`, `table`, `instance`, `database`, `topic`, `subscription`, `location`, `queue`, `parentProject`, `queryResultDataset`, `tempDataset` |
| Not matching the grammar the connector will read it by | A value it parses itself: an emulator endpoint's `host:port`, a Spanner identifier's quoting, a row-range or row-key literal, a `gs://` staging path, a Cloud Tasks relative URI, an additional field's protobuf name |

The `/` rule is about addressing rather than spelling. A component with a `/` in it does not fail —
it silently names a *different* resource, and the service then answers accurately about something
you never typed. A value that genuinely *is* a full path, such as Pub/Sub's `kmsKeyName`, is
exempt for the same reason.

Two more checks exist because of where the service's own answer would land. A Cloud Tasks target URL
must be absolute, checked at the builder for a fixed URL and again per record for one an extractor
produced, so the rejection names the URL rather than the request that carried it. And a reserved App
Engine header such as `Host` is refused at the setter because it is
[owned by Cloud Tasks]({{< relref "docs/connectors/datastream/cloudtasks" >}}) and cannot take
effect, which is worth learning where you set it.

Everything else about a name is the service's answer, including whether the resource exists and
whether the name is one that service accepts. Its rejection names the resource it refused. A copy
of those naming rules kept here would go stale in the direction that hurts, refusing a name the
service would have taken.
