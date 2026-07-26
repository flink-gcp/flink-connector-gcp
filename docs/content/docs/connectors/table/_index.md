---
title: Table API Connectors
bookCollapseSection: true
weight: 20
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

# Table API Connectors

Connectors for the Table API and SQL.

Each of these is a mapping onto the DataStream connector of the same name rather than a separate
implementation: the programmatic API is the source of truth, and a table option exists because a
builder setter does. The corresponding
{{< relref "docs/connectors/datastream" >}} page is where the behavior behind an option is
described; the page here documents the option surface and the decisions specific to SQL.
