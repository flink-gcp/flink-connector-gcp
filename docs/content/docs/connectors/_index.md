---
title: Connectors
bookCollapseSection: true
weight: 30
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
