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

package io.github.flink.gcp.connector.base.retry;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link Retries}. */
class RetriesTest {

    @Test
    void sleepsForTheGivenDuration() throws IOException {
        long start = System.nanoTime();
        Retries.sleep(20, "unused");
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertThat(elapsedMs).isGreaterThanOrEqualTo(20);
    }

    @Test
    void aNonPositiveDurationReturnsImmediately() throws IOException {
        Retries.sleep(0, "unused");
        Retries.sleep(-5, "unused");
    }

    @Test
    void anInterruptBecomesAnIOExceptionAndTheFlagIsRestored() {
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> Retries.sleep(60_000, "Interrupted while testing"))
                    .isInstanceOf(IOException.class)
                    .hasMessage("Interrupted while testing")
                    .hasCauseInstanceOf(InterruptedException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            // Clear the flag so it cannot leak into another test on this worker thread.
            Thread.interrupted();
        }
    }
}
