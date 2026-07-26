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

package io.github.flink.gcp.connector.cloudtasks.sink.writer;

/**
 * A {@link TimeSource} whose clock only moves when the writer waits out a backoff, so retry tests
 * run at full speed and are deterministic rather than timing-dependent.
 */
final class ManualTimeSource implements TimeSource {

    /** An arbitrary non-zero epoch, so a due time is never confused with an unset one. */
    private long nowMillis = 1_000_000L;

    private long sleptMillis;

    @Override
    public long currentTimeMillis() {
        return nowMillis;
    }

    @Override
    public void sleep(long millis) {
        nowMillis += millis;
        sleptMillis += millis;
    }

    /** Returns the total time the writer has waited out backoffs. */
    long getSleptMillis() {
        return sleptMillis;
    }
}
