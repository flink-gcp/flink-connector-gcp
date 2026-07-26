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

package io.github.flink.gcp.connector.pubsub.source;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link StartPosition}. */
class StartPositionTest {

    private static final Instant NOW = Instant.parse("2026-07-25T12:00:00Z");
    private static final Instant EARLIER = Instant.parse("2026-07-20T09:30:00Z");

    @Test
    void continuingFromTheSubscriptionNeedsNoSeek() {
        StartPosition position = StartPosition.continueFromSubscription();

        assertThat(position.getMode()).isEqualTo(StartPosition.Mode.CONTINUE_FROM_SUBSCRIPTION);
        assertThat(position.getTimestamp()).isNull();
        assertThat(position.requiresSeek()).isFalse();
    }

    @Test
    void everyOtherPositionSeeks() {
        assertThat(StartPosition.earliestRetained().requiresSeek()).isTrue();
        assertThat(StartPosition.latest().requiresSeek()).isTrue();
        assertThat(StartPosition.fromTimestamp(EARLIER).requiresSeek()).isTrue();
    }

    @Test
    void theModeAndTimestampPairBuildsTheSamePositionsAsTheFactories() {
        assertThat(StartPosition.of(StartPosition.Mode.CONTINUE_FROM_SUBSCRIPTION, null))
                .isEqualTo(StartPosition.continueFromSubscription());
        assertThat(StartPosition.of(StartPosition.Mode.EARLIEST_RETAINED, null))
                .isEqualTo(StartPosition.earliestRetained());
        assertThat(StartPosition.of(StartPosition.Mode.LATEST, null))
                .isEqualTo(StartPosition.latest());
        assertThat(StartPosition.of(StartPosition.Mode.TIMESTAMP, EARLIER))
                .isEqualTo(StartPosition.fromTimestamp(EARLIER));
    }

    @Test
    void theTimestampModeRequiresATimestamp() {
        assertThatThrownBy(() -> StartPosition.of(StartPosition.Mode.TIMESTAMP, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("A timestamp is required");
    }

    @Test
    void aTimestampWithAnotherModeIsRejectedRatherThanIgnored() {
        assertThatThrownBy(() -> StartPosition.of(StartPosition.Mode.LATEST, EARLIER))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only meaningful for start position mode TIMESTAMP");
    }

    @Test
    void rejectsNullArguments() {
        assertThatThrownBy(() -> StartPosition.fromTimestamp(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("timestamp must not be null");
        assertThatThrownBy(() -> StartPosition.of(null, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("mode must not be null");
    }

    @Test
    void positionsOfTheSameModeAndTimestampAreEqual() {
        assertThat(StartPosition.fromTimestamp(EARLIER))
                .isEqualTo(StartPosition.fromTimestamp(EARLIER))
                .hasSameHashCodeAs(StartPosition.fromTimestamp(EARLIER))
                .isNotEqualTo(StartPosition.fromTimestamp(NOW))
                .isNotEqualTo(StartPosition.latest());
        assertThat(StartPosition.fromTimestamp(EARLIER))
                .hasToString("StartPosition{mode=timestamp, timestamp=" + EARLIER + "}");
        assertThat(StartPosition.latest()).hasToString("StartPosition{mode=latest}");
    }
}
