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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DataAvailabilitySignal}. */
@Timeout(30)
class DataAvailabilitySignalTest {

    private final DataAvailabilitySignal signal = new DataAvailabilitySignal();

    @Test
    void raiseCompletesTheArmedFuture() {
        CompletableFuture<Void> armed = signal.arm();
        assertThat(armed).isNotDone();

        signal.raise();

        assertThat(armed).isDone();
    }

    @Test
    void aRaiseWithNoWaitArmedIsRemembered() {
        // The level trigger: the fetcher checks its own wake-up flag before entering fetch() and
        // raises exactly once per event, so a raise landing between fetches must not be dropped.
        signal.raise();

        assertThat(signal.arm()).isDone();
    }

    @Test
    void armingConsumesTheRememberedRaise() {
        signal.raise();
        signal.arm();

        // One remembered raise wakes one wait; the next arm parks again.
        assertThat(signal.arm()).isNotDone();
    }

    @Test
    void awaitReturnsOnceTheParkTimeoutPasses() throws Exception {
        // Never raised: the timed wake exists only so the caller can re-evaluate its guards, and
        // it surfaces as a plain return rather than an exception.
        CompletableFuture<Void> armed = signal.arm();
        long before = System.nanoTime();

        signal.await(armed, 10);

        // The return came from the timeout: the future is still pending — a completion did not
        // sneak in — and at least the timeout elapsed, so the wait was not skipped outright.
        assertThat(armed).isNotDone();
        assertThat(System.nanoTime() - before).isGreaterThanOrEqualTo(10_000_000L);
    }

    @Test
    void awaitRestoresTheInterruptFlag() throws Exception {
        CompletableFuture<Void> armed = signal.arm();
        Thread.currentThread().interrupt();
        try {
            signal.await(armed, 0);

            assertThat(Thread.currentThread().isInterrupted())
                    .as("the interrupt is put back for the fetcher's own machinery to see")
                    .isTrue();
        } finally {
            // Clear the flag so it cannot leak into the next test on this worker thread.
            Thread.interrupted();
        }
    }
}
