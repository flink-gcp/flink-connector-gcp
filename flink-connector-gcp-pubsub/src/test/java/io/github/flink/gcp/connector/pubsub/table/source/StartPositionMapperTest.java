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

package io.github.flink.gcp.connector.pubsub.table.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;

import io.github.flink.gcp.connector.pubsub.source.StartPosition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link StartPositionMapper}. */
class StartPositionMapperTest {

    private static final String MODE = "scan.startup.mode";
    private static final String TIMESTAMP = "scan.startup.timestamp-millis";

    private static StartPosition map(Map<String, String> options) {
        return StartPositionMapper.map(Configuration.fromMap(options));
    }

    private static StartPosition mapMode(String mode) {
        return map(Collections.singletonMap(MODE, mode));
    }

    @Test
    void anEmptyConfigLeavesTheBuildersOwnDefault() {
        // Not StartPosition.continueFromSubscription(): "absent" and "explicitly the default" have
        // to stay the same state, which they only do if the setter is never called.
        assertThat(StartPositionMapper.map(new Configuration())).isNull();
    }

    @Test
    void mapsEveryModeThatNeedsNoTimestamp() {
        assertThat(mapMode("continue-from-subscription"))
                .isEqualTo(StartPosition.continueFromSubscription());
        assertThat(mapMode("earliest-retained")).isEqualTo(StartPosition.earliestRetained());
        assertThat(mapMode("latest")).isEqualTo(StartPosition.latest());
    }

    @Test
    void mapsTheTimestampModeOntoTheInstantItNames() {
        Map<String, String> options = new HashMap<>();
        options.put(MODE, "timestamp");
        options.put(TIMESTAMP, "1735689600000");

        assertThat(map(options))
                .isEqualTo(StartPosition.fromTimestamp(Instant.ofEpochMilli(1_735_689_600_000L)));
    }

    @Test
    void readsTheModeCaseInsensitively() {
        // Flink matches an enum option against toString() case-insensitively, so a DDL may spell it
        // either way; the hyphens are not optional though, which ConnectorEnumOptionSpellingTest
        // covers for every constant.
        assertThat(mapMode("EARLIEST-RETAINED")).isEqualTo(StartPosition.earliestRetained());
    }

    @Test
    void rejectsTheTimestampModeWithoutATimestamp() {
        assertThatThrownBy(() -> mapMode("timestamp"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("timestamp is required");
    }

    @ParameterizedTest
    @ValueSource(strings = {"continue-from-subscription", "earliest-retained", "latest"})
    void rejectsATimestampGivenWithAModeThatCannotUseIt(String mode) {
        Map<String, String> options = new HashMap<>();
        options.put(MODE, mode);
        options.put(TIMESTAMP, "1735689600000");

        assertThatThrownBy(() -> map(options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only meaningful for start position mode TIMESTAMP");
    }

    @Test
    void rejectsATimestampGivenWithNoModeAtAll() {
        // The one rule this mapper owns: StartPosition.of is never reached here, so without this
        // the option would be read by nothing and the job would quietly start from wherever the
        // subscription happened to be.
        assertThatThrownBy(() -> map(Collections.singletonMap(TIMESTAMP, "1735689600000")))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(TIMESTAMP)
                .hasMessageContaining(MODE);
    }
}
