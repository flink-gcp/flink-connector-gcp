---
title: Reference
bookCollapseSection: true
weight: 40
---

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

# Configuration reference

Every option each connector takes, with its default, in one place per connector.

| Page | Surface |
|---|---|
| [BigQuery]({{< relref "docs/reference/bigquery" >}}) | The sink builder, the three write methods' options objects, table creation and schema updates, and the three serializers' schema options |
| [Cloud Pub/Sub]({{< relref "docs/reference/pubsub" >}}) | The sink and source builders, publisher and subscriber tuning, topic and subscription creation |
| [Cloud Tasks]({{< relref "docs/reference/cloudtasks" >}}) | The sink builder and the writer's in-flight cap and retry budgets |

**These pages answer *what*; the connector pages answer *why*.** A row here gives you the option's
name, its default and one line on what it does. The reasoning behind a default — why
`maxInflightRequests` departs from the SDK's own, why ordering is off, why the Cloud Tasks sink has
no rate knobs at all — stays on the {{< relref "docs/connectors" >}} page it belongs to, and each
section below links to it.

**Two surfaces are documented elsewhere, deliberately.** The
[Pub/Sub SQL connector]({{< relref "docs/connectors/table/pubsub" >}}) page carries the full
`WITH` option surface, because a SQL option exists only where a builder setter does and the mapping
between them is the thing worth reading. And every type named here — including the enums, the
destination types and the SPIs an option takes — is in the
[Java API reference]({{< param ApiDocsURL >}}), generated from the source.

## What a default means

Three kinds of entry appear in the Default column, and the difference matters:

| Entry | Meaning |
|---|---|
| A value | The connector's own default, applied when you do not set the option |
| **required** | The builder rejects a sink or source built without it |
| *unset ⇒ …* | The connector sets nothing, so the client library's or the service's own default applies. The named value is that default, recorded here for sizing rather than enforced by this project — it can change under a dependency bump |

The last row is why an options object's `defaults()` is equivalent to not passing one at all: it
holds no values of its own for those knobs.
