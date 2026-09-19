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

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Separates callback readiness from the hard-limit response of the one-message probe. */
final class SubscriberLimitAwaiter {

    static final class Observation {
        final long deliveries;
        final boolean limitExceeded;
        @Nullable final ApiService.State clientState;
        @Nullable final Throwable failure;
        final String description;

        Observation(
                long deliveries,
                boolean limitExceeded,
                @Nullable ApiService.State clientState,
                @Nullable Throwable failure,
                String description) {
            this.deliveries = deliveries;
            this.limitExceeded = limitExceeded;
            this.clientState = clientState;
            this.failure = failure;
            this.description = description;
        }

        @Override
        public String toString() {
            return "deliveries="
                    + deliveries
                    + " limitExceeded="
                    + limitExceeded
                    + " clientState="
                    + clientState
                    + " "
                    + description;
        }
    }

    @FunctionalInterface
    interface Pause {
        void run() throws InterruptedException;
    }

    static void await(
            Duration readinessTimeout, Duration responseTimeout, Supplier<Observation> observe)
            throws InterruptedException {
        await(
                readinessTimeout,
                responseTimeout,
                observe,
                System::nanoTime,
                () -> Thread.sleep(100));
    }

    static void await(
            Duration readinessTimeout,
            Duration responseTimeout,
            Supplier<Observation> observe,
            LongSupplier clock,
            Pause pause)
            throws InterruptedException {
        long phaseStart = clock.getAsLong();
        long budget = readinessTimeout.toNanos();
        String phase = "second callback (one-message capacity)";
        boolean responseStarted = false;
        while (true) {
            Observation current = observe.get();
            if (current.failure != null || current.clientState == ApiService.State.FAILED) {
                throw new AssertionError(
                        "Subscriber failed while waiting for " + phase + ": " + current,
                        current.failure);
            }
            long now = clock.getAsLong();
            if (now - phaseStart >= budget) {
                throw new AssertionError("Timed out waiting for " + phase + ": " + current);
            }
            if (!responseStarted && current.deliveries >= 2) {
                responseStarted = true;
                phase = "hard-limit event and subscriber stop";
                phaseStart = now;
                budget = responseTimeout.toNanos();
            }
            boolean stopped =
                    current.clientState == ApiService.State.STOPPING
                            || current.clientState == ApiService.State.TERMINATED;
            if (responseStarted && current.limitExceeded && stopped) {
                return;
            }
            pause.run();
        }
    }

    private SubscriberLimitAwaiter() {}
}
