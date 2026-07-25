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

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.SourceOutput;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/** {@link SourceOutput} recording emitted records and their timestamps. */
final class CollectingSourceOutput<T> implements SourceOutput<T> {

    private final List<T> records = new ArrayList<>();
    private final List<Long> timestamps = new ArrayList<>();

    @Nullable private RuntimeException failure;

    /** Makes every subsequent collect throw, standing in for a failing chained operator. */
    void failOnCollect(RuntimeException failure) {
        this.failure = failure;
    }

    List<T> records() {
        return records;
    }

    List<Long> timestamps() {
        return timestamps;
    }

    @Override
    public void collect(T record) {
        collect(record, Long.MIN_VALUE);
    }

    @Override
    public void collect(T record, long timestamp) {
        if (failure != null) {
            throw failure;
        }
        records.add(record);
        timestamps.add(timestamp);
    }

    @Override
    public void emitWatermark(Watermark watermark) {}

    @Override
    public void markIdle() {}

    @Override
    public void markActive() {}
}
