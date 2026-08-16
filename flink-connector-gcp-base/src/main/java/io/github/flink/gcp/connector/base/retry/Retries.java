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

package io.github.flink.gcp.connector.base.retry;

import org.apache.flink.annotation.Internal;

import java.io.IOException;

/**
 * Support for the connectors' hand-written retry loops. The loops themselves stay in the
 * connectors: each pairs a {@link RetrySchedule} with its own retryability classification and
 * per-site recovery actions, which is the part no shared executor replaces (issue #61 records the
 * survey).
 */
@Internal
public final class Retries {

    private Retries() {}

    /**
     * Sleeps for the given backoff, translating an interrupt into an {@link IOException} carrying
     * the given message — the writers and committers run on threads Flink interrupts to cancel a
     * task, and their contracts fail via {@code IOException}. The interrupt flag is restored. A
     * non-positive duration returns immediately.
     *
     * @param millis the sleep duration
     * @param interruptedMessage the message of the {@code IOException} thrown on interrupt; it
     *     should name what was being waited for
     */
    public static void sleep(long millis, String interruptedMessage) throws IOException {
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(interruptedMessage, e);
        }
    }
}
