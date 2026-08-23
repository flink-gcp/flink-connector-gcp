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

package io.github.flink.gcp.connector.pubsub.source;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PubSubStartPosition}. */
class PubSubStartPositionTest {

    private static final Instant NOW = Instant.parse("2026-07-25T12:00:00Z");
    private static final Instant EARLIER = Instant.parse("2026-07-20T09:30:00Z");

    @Test
    void continuingFromTheSubscriptionNeedsNoSeek() {
        PubSubStartPosition position = PubSubStartPosition.continueFromSubscription();

        assertThat(position.getMode())
                .isEqualTo(PubSubStartPosition.Mode.CONTINUE_FROM_SUBSCRIPTION);
        assertThat(position.getTimestamp()).isNull();
        assertThat(position.requiresSeek()).isFalse();
    }

    @Test
    void everyOtherPositionSeeks() {
        assertThat(PubSubStartPosition.earliestRetained().requiresSeek()).isTrue();
        assertThat(PubSubStartPosition.latest().requiresSeek()).isTrue();
        assertThat(PubSubStartPosition.fromTimestamp(EARLIER).requiresSeek()).isTrue();
    }

    @Test
    void theModeAndTimestampPairBuildsTheSamePositionsAsTheFactories() {
        assertThat(
                        PubSubStartPosition.of(
                                PubSubStartPosition.Mode.CONTINUE_FROM_SUBSCRIPTION, null))
                .isEqualTo(PubSubStartPosition.continueFromSubscription());
        assertThat(PubSubStartPosition.of(PubSubStartPosition.Mode.EARLIEST_RETAINED, null))
                .isEqualTo(PubSubStartPosition.earliestRetained());
        assertThat(PubSubStartPosition.of(PubSubStartPosition.Mode.LATEST, null))
                .isEqualTo(PubSubStartPosition.latest());
        assertThat(PubSubStartPosition.of(PubSubStartPosition.Mode.TIMESTAMP, EARLIER))
                .isEqualTo(PubSubStartPosition.fromTimestamp(EARLIER));
    }

    @Test
    void theTimestampModeRequiresATimestamp() {
        assertThatThrownBy(() -> PubSubStartPosition.of(PubSubStartPosition.Mode.TIMESTAMP, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("A timestamp is required");
    }

    @Test
    void aTimestampWithAnotherModeIsRejectedRatherThanIgnored() {
        assertThatThrownBy(() -> PubSubStartPosition.of(PubSubStartPosition.Mode.LATEST, EARLIER))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only meaningful for start position mode TIMESTAMP");
    }

    @Test
    void rejectsNullArguments() {
        assertThatThrownBy(() -> PubSubStartPosition.fromTimestamp(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("timestamp must not be null");
        assertThatThrownBy(() -> PubSubStartPosition.of(null, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("mode must not be null");
    }

    @Test
    void positionsOfTheSameModeAndTimestampAreEqual() {
        assertThat(PubSubStartPosition.fromTimestamp(EARLIER))
                .isEqualTo(PubSubStartPosition.fromTimestamp(EARLIER))
                .hasSameHashCodeAs(PubSubStartPosition.fromTimestamp(EARLIER))
                .isNotEqualTo(PubSubStartPosition.fromTimestamp(NOW))
                .isNotEqualTo(PubSubStartPosition.latest());
        assertThat(PubSubStartPosition.fromTimestamp(EARLIER))
                .hasToString("PubSubStartPosition{mode=timestamp, timestamp=" + EARLIER + "}");
        assertThat(PubSubStartPosition.latest()).hasToString("PubSubStartPosition{mode=latest}");
    }
}
