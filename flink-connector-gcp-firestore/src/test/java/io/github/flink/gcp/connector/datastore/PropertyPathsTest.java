/*
 * Copyright 2026 The flink-gcp authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.flink.gcp.connector.datastore;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PropertyPathsTest {

    @Test
    void aNameIsOneBackquotedSegmentWithItsBackquotesAndBackslashesEscaped() {
        assertThat(PropertyPaths.segment("plain")).isEqualTo("`plain`");
        assertThat(PropertyPaths.segment("a.b")).isEqualTo("`a.b`");
        assertThat(PropertyPaths.segment("tick`")).isEqualTo("`tick\\``");
        assertThat(PropertyPaths.segment("back\\slash")).isEqualTo("`back\\\\slash`");
        // The backslash is escaped first, so the backquote's escape is not escaped again.
        assertThat(PropertyPaths.segment("\\`")).isEqualTo("`\\\\\\``");
    }
}
