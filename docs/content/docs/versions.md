---
title: Documentation versions
type: docs
weight: 15
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

# Documentation versions

The site publishes the latest patch release of the current and previous connector minor series.
For example, after 1.2.1 is released, the selector offers 1.2.1 and the latest 1.1.x release.
A later 1.1.x patch updates that series without changing which series is current.
The count spans major versions: 2.0 and the last published 1.x minor occupy the two slots after 2.0 is released.
This documentation retention policy does not change the Flink versions a connector supports.

Unversioned HTML pages redirect to the latest release when that release contains the requested page.
An unreleased API or page that exists only in Development has no release counterpart, so its old unversioned URL shows the 404 page with links to the available documentation.
Versioned URLs such as `/flink-connector-gcp/1.0/` stay on that minor series and advance to its latest patch.
The Version dropdown at the bottom of the left menu shows the exact patch and switches to the same page in another series when it exists.
Otherwise, it opens that series' homepage.
On a narrow screen, open the menu to reach it.
The API reference (Javadoc) link above the dropdown opens the Java API reference for the selected series.
Javadoc displays that version and links back to its documentation homepage, where the dropdown can switch the whole documentation site to another series.
Search also belongs to the selected series.

Development follows `main` and includes changes that have not been released.
Use a released series when looking up the behavior of a deployed connector.
Each page links to the exact source commit from which it was built.

When a series leaves the retention window, its pages are removed from the hosted site.
The [GitHub Releases]({{< param BookRepo >}}/releases) archive retains release downloads and links to the tagged source, including its documentation.
Hosting an older series does not promise continued bug fixes for that connector release.
