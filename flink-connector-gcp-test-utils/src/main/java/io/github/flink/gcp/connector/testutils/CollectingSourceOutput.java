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

package io.github.flink.gcp.connector.testutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.SourceOutput;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link SourceOutput} recording emitted records and their timestamps — what a {@code
 * RecordEmitter} is handed, and what a test driving one asserts against.
 *
 * <p>Written here rather than taken from Flink because {@code flink-connector-base}'s test jar is
 * not a dependency of any module in this repository.
 *
 * <p>{@link #timestamps()} is padded rather than sparse: a record emitted without a timestamp
 * appears as {@code null} rather than not appearing at all, so an assertion on it distinguishes
 * "one record, no timestamp" from "no record".
 *
 * <p>Not thread-safe, and it does not need to be: a {@code SourceReaderBase} hands its record
 * emitter the output on the thread that called {@code pollNext}, never on a fetcher thread. A test
 * collecting from more than one thread has to synchronize itself.
 */
@Internal
public final class CollectingSourceOutput<T> implements SourceOutput<T> {

    private final List<T> records = new ArrayList<>();

    /** One entry per record; {@code null} means {@code collect(record)} was called without one. */
    private final List<Long> timestamps = new ArrayList<>();

    @Nullable private RuntimeException failure;

    /** Makes every subsequent collect throw, standing in for a failing chained operator. */
    public void failOnCollect(RuntimeException failure) {
        this.failure = failure;
    }

    public List<T> records() {
        return records;
    }

    public List<Long> timestamps() {
        return timestamps;
    }

    @Override
    public void collect(T record) {
        add(record, null);
    }

    @Override
    public void collect(T record, long timestamp) {
        add(record, timestamp);
    }

    private void add(T record, @Nullable Long timestamp) {
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
