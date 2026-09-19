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

package io.github.flink.gcp.connector.pubsub.source.streamingpull.reader;

import com.google.api.core.ApiService;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SubscriberLimitAwaiterTest {

    @Test
    void delayedDeliveryGetsItsOwnResponseBudget() throws Exception {
        AtomicLong clock = new AtomicLong();
        run(
                clock,
                tick ->
                        observation(
                                tick >= 11 ? 2 : 0,
                                tick >= 13,
                                tick >= 14
                                        ? ApiService.State.TERMINATED
                                        : ApiService.State.RUNNING));
        assertThat(clock).hasValue(14);
    }

    @Test
    void theFirstCallbackDoesNotSpendTheResponseBudget() throws Exception {
        AtomicLong clock = new AtomicLong();
        run(
                clock,
                tick ->
                        observation(
                                tick >= 11 ? 2 : 1,
                                tick >= 13,
                                tick >= 14
                                        ? ApiService.State.TERMINATED
                                        : ApiService.State.RUNNING));
        assertThat(clock).hasValue(14);
    }

    @Test
    void oneCallbackAloneFailsAtTheReadinessDeadline() {
        AtomicLong clock = new AtomicLong();
        assertThatThrownBy(
                        () -> run(clock, tick -> observation(1, false, ApiService.State.RUNNING)))
                .isInstanceOf(AssertionError.class)
                .hasMessageContainingAll("second callback", "deliveries=1", "buffered=1");
        assertThat(clock).hasValue(12);
    }

    @Test
    void noDeliveryFailsWithReadinessDiagnostics() {
        assertThatThrownBy(
                        () ->
                                run(
                                        new AtomicLong(),
                                        tick -> observation(0, false, ApiService.State.RUNNING)))
                .isInstanceOf(AssertionError.class)
                .hasMessageContainingAll("second callback", "deliveries=0", "buffered=0");
    }

    @Test
    void missingLimitEventFailsAfterTheCrossingCallback() {
        assertThatThrownBy(
                        () ->
                                run(
                                        new AtomicLong(),
                                        tick -> observation(2, false, ApiService.State.RUNNING)))
                .isInstanceOf(AssertionError.class)
                .hasMessageContainingAll(
                        "hard-limit event and subscriber stop", "limitExceeded=false");
    }

    @Test
    void anEventWithoutAStopDoesNotPass() {
        assertThatThrownBy(
                        () ->
                                run(
                                        new AtomicLong(),
                                        tick -> observation(2, true, ApiService.State.RUNNING)))
                .isInstanceOf(AssertionError.class)
                .hasMessageContainingAll(
                        "hard-limit event and subscriber stop", "clientState=RUNNING");
    }

    @Test
    void aStopWithoutAnEventDoesNotPass() {
        assertThatThrownBy(
                        () ->
                                run(
                                        new AtomicLong(),
                                        tick -> observation(2, false, ApiService.State.TERMINATED)))
                .isInstanceOf(AssertionError.class)
                .hasMessageContainingAll(
                        "hard-limit event and subscriber stop", "limitExceeded=false");
    }

    @Test
    void stopCanBeObservedBeforeTheLimitEventIsPublished() throws Exception {
        AtomicLong clock = new AtomicLong();
        run(clock, tick -> observation(2, tick >= 2, ApiService.State.STOPPING));
        assertThat(clock).hasValue(2);
    }

    @Test
    void clientFailureKeepsItsCause() {
        RuntimeException cause = new RuntimeException("permission denied");
        assertThatThrownBy(
                        () ->
                                run(
                                        new AtomicLong(),
                                        tick ->
                                                new SubscriberLimitAwaiter.Observation(
                                                        0,
                                                        false,
                                                        ApiService.State.FAILED,
                                                        cause,
                                                        "buffered=0")))
                .isInstanceOf(AssertionError.class)
                .hasMessageContainingAll("second callback")
                .hasCause(cause);
    }

    @Test
    void deliveryAtTheReadinessDeadlineIsTooLate() {
        assertThatThrownBy(
                        () ->
                                run(
                                        new AtomicLong(),
                                        tick ->
                                                observation(
                                                        tick >= 12 ? 2 : 0,
                                                        tick >= 12,
                                                        tick >= 12
                                                                ? ApiService.State.TERMINATED
                                                                : ApiService.State.RUNNING)))
                .isInstanceOf(AssertionError.class)
                .hasMessageContainingAll("second callback");
    }

    @Test
    void responseAtItsDeadlineIsTooLate() {
        assertThatThrownBy(
                        () ->
                                run(
                                        new AtomicLong(),
                                        tick ->
                                                observation(
                                                        2,
                                                        tick >= 4,
                                                        tick >= 4
                                                                ? ApiService.State.TERMINATED
                                                                : ApiService.State.RUNNING)))
                .isInstanceOf(AssertionError.class)
                .hasMessageContainingAll("hard-limit event and subscriber stop");
    }

    @Test
    void interruptionIsPropagated() {
        assertThatThrownBy(
                        () ->
                                SubscriberLimitAwaiter.await(
                                        Duration.ofSeconds(12),
                                        Duration.ofSeconds(4),
                                        () -> observation(0, false, ApiService.State.RUNNING),
                                        () -> 0,
                                        () -> {
                                            throw new InterruptedException("cancelled");
                                        }))
                .isInstanceOf(InterruptedException.class)
                .hasMessage("cancelled");
    }

    private static void run(
            AtomicLong clock, LongFunction<SubscriberLimitAwaiter.Observation> observe)
            throws InterruptedException {
        SubscriberLimitAwaiter.await(
                Duration.ofNanos(12),
                Duration.ofNanos(4),
                () -> observe.apply(clock.get()),
                clock::get,
                clock::incrementAndGet);
    }

    private static SubscriberLimitAwaiter.Observation observation(
            long deliveries, boolean exceeded, ApiService.State state) {
        return new SubscriberLimitAwaiter.Observation(
                deliveries, exceeded, state, null, "buffered=" + Math.min(1, deliveries));
    }
}
