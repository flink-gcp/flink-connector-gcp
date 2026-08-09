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
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;

import java.util.List;

/**
 * {@link ReaderOutput} for tests that drive {@code SourceReaderBase.pollNext(...)}, delegating to
 * one shared {@link CollectingSourceOutput} whatever split a record came from.
 */
final class CollectingReaderOutput<T> implements ReaderOutput<T> {

    private final CollectingSourceOutput<T> output = new CollectingSourceOutput<>();

    List<T> records() {
        return output.records();
    }

    @Override
    public void collect(T record) {
        output.collect(record);
    }

    @Override
    public void collect(T record, long timestamp) {
        output.collect(record, timestamp);
    }

    @Override
    public void emitWatermark(Watermark watermark) {
        output.emitWatermark(watermark);
    }

    @Override
    public void markIdle() {
        output.markIdle();
    }

    @Override
    public void markActive() {
        output.markActive();
    }

    @Override
    public SourceOutput<T> createOutputForSplit(String splitId) {
        return output;
    }

    @Override
    public void releaseOutputForSplit(String splitId) {}
}
