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

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FirstCheckpointWatchdog}. */
class FirstCheckpointWatchdogTest {

    private static final Duration BUDGET = Duration.ofMinutes(10);

    /**
     * One clock read costs this many checks, so a test must poll at least that often to advance.
     */
    private static final int POLLS_PER_CLOCK_READ = 1_024;

    private final MovableClock clock = new MovableClock();

    @Test
    void staysQuietWhileTheBudgetIsUnspent() {
        FirstCheckpointWatchdog watchdog = watchdog(BUDGET);
        AckTracker tracker = trackerWithOutstanding(1);

        clock.advance(BUDGET.minusSeconds(1));

        assertThatCode(() -> poll(watchdog, tracker)).doesNotThrowAnyException();
    }

    @Test
    void failsOnceTheBudgetIsSpentWithMessagesOutstanding() {
        FirstCheckpointWatchdog watchdog = watchdog(BUDGET);
        AckTracker tracker = trackerWithOutstanding(3);

        clock.advance(BUDGET);

        assertThatThrownBy(() -> poll(watchdog, tracker))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No checkpoint has been taken")
                .hasMessageContaining("execution.checkpointing.interval")
                .hasMessageContaining("firstCheckpointTimeout");
    }

    @Test
    void staysQuietWhenThereIsNothingToAcknowledge() {
        // An idle subscription must not be mistaken for a stalled one.
        FirstCheckpointWatchdog watchdog = watchdog(BUDGET);
        AckTracker tracker = trackerWithOutstanding(0);

        clock.advance(BUDGET.multipliedBy(10));

        assertThatCode(() -> poll(watchdog, tracker)).doesNotThrowAnyException();
    }

    @Test
    void aCheckpointRetiresTheWatchdog() {
        FirstCheckpointWatchdog watchdog = watchdog(BUDGET);
        AckTracker tracker = trackerWithOutstanding(3);
        watchdog.checkpointTaken();

        clock.advance(BUDGET.multipliedBy(10));

        assertThatCode(() -> poll(watchdog, tracker)).doesNotThrowAnyException();
    }

    @Test
    void aZeroBudgetDisablesTheWatchdog() {
        FirstCheckpointWatchdog watchdog = watchdog(Duration.ZERO);
        AckTracker tracker = trackerWithOutstanding(3);

        clock.advance(Duration.ofDays(1));

        assertThatCode(() -> poll(watchdog, tracker)).doesNotThrowAnyException();
    }

    private FirstCheckpointWatchdog watchdog(Duration budget) {
        return new FirstCheckpointWatchdog(budget, clock::nanos);
    }

    /**
     * Polls often enough to reach a clock read, whose frequency the watchdog deliberately coarsens.
     */
    private static void poll(FirstCheckpointWatchdog watchdog, AckTracker tracker) {
        for (int i = 0; i < POLLS_PER_CLOCK_READ; i++) {
            watchdog.check(tracker);
        }
    }

    private static AckTracker trackerWithOutstanding(int outstanding) {
        PubSubAckTracker tracker = new PubSubAckTracker();
        for (int i = 0; i < outstanding; i++) {
            tracker.addPendingAck(
                    "split-0", "message-" + i, new RecordingAckReplyConsumer("message-" + i));
        }
        return tracker;
    }

    /** A clock the test moves by hand, so no test waits on wall-clock time. */
    private static final class MovableClock {

        private long nanos;

        private long nanos() {
            return nanos;
        }

        private void advance(Duration by) {
            nanos += by.toNanos();
        }
    }
}
