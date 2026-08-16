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

package io.github.flink.gcp.connector.base.source;

import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link StartPosition}. */
class StartPositionTest {

    private static final Instant INSTANT = Instant.parse("2026-08-11T12:00:00Z");
    private static final Duration DURATION = Duration.ofHours(3);

    @Test
    void factoriesDescribeTheirPositions() {
        assertThat(StartPosition.earliest()).hasToString("StartPosition{earliest}");
        assertThat(StartPosition.latest()).hasToString("StartPosition{latest}");
        assertThat(StartPosition.at(INSTANT)).hasToString("StartPosition{at=2026-08-11T12:00:00Z}");
        assertThat(StartPosition.ago(DURATION)).hasToString("StartPosition{ago=PT3H}");
    }

    @Test
    void positionsHaveValueEquality() {
        assertThat(StartPosition.earliest())
                .isEqualTo(StartPosition.earliest())
                .hasSameHashCodeAs(StartPosition.earliest())
                .isNotEqualTo(StartPosition.latest());
        assertThat(StartPosition.at(INSTANT))
                .isEqualTo(StartPosition.at(INSTANT))
                .hasSameHashCodeAs(StartPosition.at(INSTANT))
                .isNotEqualTo(StartPosition.at(INSTANT.plusSeconds(1)));
        assertThat(StartPosition.ago(DURATION))
                .isEqualTo(StartPosition.ago(DURATION))
                .hasSameHashCodeAs(StartPosition.ago(DURATION))
                .isNotEqualTo(StartPosition.ago(DURATION.plusSeconds(1)));
    }

    @Test
    void agoRequiresAPositiveDuration() {
        assertThatThrownBy(() -> StartPosition.ago(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("duration must be positive, but was PT0S");
        assertThatThrownBy(() -> StartPosition.ago(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("duration must be positive, but was PT-1S");
    }

    @Test
    void factoriesRejectNullArguments() {
        assertThatThrownBy(() -> StartPosition.at(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("instant must not be null");
        assertThatThrownBy(() -> StartPosition.ago(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("duration must not be null");
    }

    @Test
    void everyPositionHasAStableSerializableForm() throws Exception {
        for (StartPosition position :
                Arrays.asList(
                        StartPosition.earliest(),
                        StartPosition.latest(),
                        StartPosition.at(INSTANT),
                        StartPosition.ago(DURATION))) {
            StartPosition restored =
                    InstantiationUtil.deserializeObject(
                            InstantiationUtil.serializeObject(position),
                            getClass().getClassLoader());

            assertThat(restored).isEqualTo(position);
        }
    }
}
