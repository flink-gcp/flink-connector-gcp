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

package io.github.flink.gcp.connector.cloudtasks.sink.createtask.writer;

import org.apache.flink.annotation.Internal;

/**
 * The writer's view of time: reading the clock and waiting out a retry backoff.
 *
 * <p>It exists so the retry tests can drive backoffs deterministically instead of sleeping through
 * them, which is the difference between a fast test suite and a flaky one.
 */
@Internal
public interface TimeSource {

    /** The real clock, sleeping the calling thread. */
    TimeSource SYSTEM =
            new TimeSource() {
                @Override
                public long currentTimeMillis() {
                    return System.currentTimeMillis();
                }

                @Override
                public void sleep(long millis) throws InterruptedException {
                    Thread.sleep(millis);
                }
            };

    /** Returns the current time in milliseconds. */
    long currentTimeMillis();

    /**
     * Waits for the given number of milliseconds.
     *
     * @param millis how long to wait, positive
     * @throws InterruptedException if the wait is interrupted
     */
    void sleep(long millis) throws InterruptedException;
}
