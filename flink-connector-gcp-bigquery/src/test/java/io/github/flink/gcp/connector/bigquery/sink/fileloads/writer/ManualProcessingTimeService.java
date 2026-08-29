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

package io.github.flink.gcp.connector.bigquery.sink.fileloads.writer;

import org.apache.flink.api.common.operators.ProcessingTimeService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;

/** Processing-time service whose clock and callbacks are controlled by a test. */
public final class ManualProcessingTimeService implements ProcessingTimeService {

    private long now;
    private final List<Long> timestamps = new ArrayList<>();
    private final List<ProcessingTimeCallback> callbacks = new ArrayList<>();

    @Override
    public long getCurrentProcessingTime() {
        return now;
    }

    @Override
    public ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback callback) {
        timestamps.add(timestamp);
        callbacks.add(callback);
        return null;
    }

    /** Advances the clock and fires every timer whose deadline has passed. */
    public void advanceTo(long time) throws Exception {
        now = time;
        for (int i = 0; i < callbacks.size(); i++) {
            if (timestamps.get(i) != null && timestamps.get(i) <= now) {
                ProcessingTimeCallback callback = callbacks.get(i);
                timestamps.set(i, null);
                callback.onProcessingTime(now);
            }
        }
    }

    /** Returns the number of timers registered since construction. */
    public int registeredTimerCount() {
        return callbacks.size();
    }
}
