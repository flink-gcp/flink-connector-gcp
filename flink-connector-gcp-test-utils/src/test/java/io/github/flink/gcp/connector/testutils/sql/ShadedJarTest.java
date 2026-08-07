/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.testutils.sql;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The two rejections {@link ShadedJar#of} owns.
 *
 * <p>Tested here rather than through a consumer, by this module's narrow bar: every shaded module
 * passes a prefix these accept, so no green build reaches either branch — and a rejection that has
 * stopped rejecting looks exactly like one that never fired. Everything else about the type is
 * exercised for real by both {@code flink-sql-connector-gcp-*} modules, against a jar, and so is
 * deliberately not repeated here.
 */
class ShadedJarTest {

    private static final String ARTIFACT = "flink-sql-connector-gcp-example";

    @Test
    void aPrefixWithoutItsTrailingSlashIsRejected() {
        // It is compared against jar-entry paths with startsWith, so "…/pubsub/shaded" would also
        // admit a hypothetical "…/pubsub/shadedX/" and, more usefully, concatenating it would
        // produce entry names with no separator at all.
        assertThatThrownBy(() -> ShadedJar.of(ARTIFACT, "io/github/example/shaded"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must end with '/'");
    }

    @ParameterizedTest
    @ValueSource(strings = {"io/github/my_example/shaded/", "_io/github/example/shaded/"})
    void aPrefixCarryingAnUnderscoreIsRejected(String prefix) {
        // Netty maps `_` to `_1` before mapping `.` to `_` when it derives a native library name
        // from its own package, so such a prefix would have to be spelled differently in the pom's
        // META-INF/native relocations than everywhere else. No prefix here needs one.
        //
        // Both positions, because the check is about the character and not about where it sits: a
        // bound written `> 0` would pass the first case and admit the second.
        assertThatThrownBy(() -> ShadedJar.of(ARTIFACT, prefix))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("underscore");
    }
}
