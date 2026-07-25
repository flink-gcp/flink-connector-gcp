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

package io.github.flink.gcp.connector.pubsub.source.streamingpull.reader;

import org.apache.flink.util.clock.ManualClock;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link MissingCheckpointDetector}. */
class MissingCheckpointDetectorTest {

    private static final Duration BUDGET = Duration.ofMinutes(10);

    private final ManualClock clock = new ManualClock();

    @Test
    void staysQuietWhileTheBudgetIsUnspent() {
        MissingCheckpointDetector detector = detector(BUDGET, 1);

        clock.advanceTime(BUDGET.minusSeconds(1));

        assertThatCode(detector::check).doesNotThrowAnyException();
    }

    @Test
    void failsOnceTheBudgetIsSpentWithMessagesOutstanding() {
        MissingCheckpointDetector detector = detector(BUDGET, 3);

        clock.advanceTime(BUDGET);

        assertThatThrownBy(detector::check)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No checkpoint has been taken")
                .hasMessageContaining("3 messages")
                .hasMessageContaining("execution.checkpointing.interval")
                .hasMessageContaining("firstCheckpointTimeout");
    }

    @Test
    void staysQuietWhenThereIsNothingToAcknowledge() {
        // An idle subscription must not be mistaken for a stalled one.
        MissingCheckpointDetector detector = detector(BUDGET, 0);

        clock.advanceTime(BUDGET.multipliedBy(10));

        assertThatCode(detector::check).doesNotThrowAnyException();
    }

    @Test
    void aCheckpointRetiresTheDetector() {
        MissingCheckpointDetector detector = detector(BUDGET, 3);
        detector.checkpointTaken();

        clock.advanceTime(BUDGET.multipliedBy(10));

        assertThatCode(detector::check).doesNotThrowAnyException();
    }

    @Test
    void aZeroBudgetDisablesTheDetector() {
        MissingCheckpointDetector detector = detector(Duration.ZERO, 3);

        clock.advanceTime(Duration.ofDays(1));

        assertThatCode(detector::check).doesNotThrowAnyException();
    }

    @Test
    void anArmedDetectorBoundsTheFetchPark() {
        // A quarter of the budget, so a spent budget is noticed within a quarter of it.
        assertThat(detector(Duration.ofMinutes(2), 0).parkTimeoutMillis())
                .isEqualTo(Duration.ofSeconds(30).toMillis());
    }

    @Test
    void theParkBoundIsCappedSoLongBudgetsDoNotDelayDetection() {
        assertThat(detector(Duration.ofHours(1), 0).parkTimeoutMillis())
                .isEqualTo(Duration.ofSeconds(30).toMillis());
    }

    @Test
    void aRetiredDetectorLetsTheFetchParkIndefinitely() {
        // The steady state must be exactly what it was before this class existed: no wake-ups.
        MissingCheckpointDetector detector = detector(BUDGET, 0);
        detector.checkpointTaken();

        assertThat(detector.parkTimeoutMillis()).isZero();
        assertThat(detector(Duration.ZERO, 0).parkTimeoutMillis()).isZero();
    }

    private MissingCheckpointDetector detector(Duration budget, int outstanding) {
        return new MissingCheckpointDetector(budget, () -> outstanding, clock::relativeTimeNanos);
    }
}
