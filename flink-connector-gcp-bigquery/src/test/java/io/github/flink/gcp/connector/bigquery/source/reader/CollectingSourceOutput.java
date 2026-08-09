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

package io.github.flink.gcp.connector.bigquery.source.reader;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.SourceOutput;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link SourceOutput} collecting what a record emitter produced.
 *
 * <p>Written here rather than taken from Flink because {@code flink-connector-base}'s test jar is
 * not a dependency of this module.
 */
final class CollectingSourceOutput<T> implements SourceOutput<T> {

    private final List<T> records = new ArrayList<>();
    private final List<Long> timestamps = new ArrayList<>();

    List<T> records() {
        return records;
    }

    /** Returns the timestamps of records emitted with one; a record without does not appear. */
    List<Long> timestamps() {
        return timestamps;
    }

    @Override
    public void collect(T record) {
        records.add(record);
    }

    @Override
    public void collect(T record, long timestamp) {
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
