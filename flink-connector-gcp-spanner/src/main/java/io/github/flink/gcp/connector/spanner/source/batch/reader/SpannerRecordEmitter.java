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

package io.github.flink.gcp.connector.spanner.source.batch.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.util.Preconditions;

import com.google.cloud.spanner.Struct;
import io.github.flink.gcp.connector.base.source.SynchronousDeserializationCollector;
import io.github.flink.gcp.connector.spanner.source.batch.BatchReadSplitState;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerStructDeserializationSchema;

/**
 * Deserializes each row and hands its output records on.
 *
 * <p>Runs on the task thread, one row at a time, as {@code pollNext} drains the element queue.
 *
 * <p>Nothing is recorded on the split state, because a partition has no position to record: a
 * restore re-reads the partition it was given from the start. That is why this emitter can emit
 * zero or more records without advancing a resume point here.
 *
 * <p>No timestamp is assigned. A Spanner row carries no event time of its own — the read timestamp
 * belongs to the whole snapshot, not to a row — so any per-record time would be a choice this
 * connector made for the job. A job that wants one assigns a watermark strategy over the records it
 * produced, or reads a commit-timestamp column and uses that.
 *
 * @param <T> the record type produced
 */
@Internal
public class SpannerRecordEmitter<T> implements RecordEmitter<Struct, T, BatchReadSplitState> {

    private final SpannerStructDeserializationSchema<T> deserializer;
    private final SpannerSourceReaderMetrics metrics;

    /**
     * Creates the emitter.
     *
     * @param deserializer the deserializer turning rows into records
     * @param metrics the reader's metrics
     */
    public SpannerRecordEmitter(
            SpannerStructDeserializationSchema<T> deserializer,
            SpannerSourceReaderMetrics metrics) {
        this.deserializer =
                Preconditions.checkNotNull(deserializer, "deserializer must not be null");
        this.metrics = Preconditions.checkNotNull(metrics, "metrics must not be null");
    }

    @Override
    public void emitRecord(Struct row, SourceOutput<T> output, BatchReadSplitState splitState)
            throws Exception {
        long emittedCount =
                SynchronousDeserializationCollector.<T, Exception>deserialize(
                        output::collect, out -> deserializer.deserialize(row, out));
        if (emittedCount == 0) {
            metrics.recordSkipped();
        }
    }
}
