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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.bigquery.source.serializer.BigQueryRowDeserializer;
import io.github.flink.gcp.connector.bigquery.source.split.BigQueryReadStreamSplitState;
import org.apache.avro.generic.GenericRecord;

/**
 * Converts each row and advances the stream's offset by exactly one row.
 *
 * <p>The offset is what a restored split resumes at, and it counts rows <em>read from the
 * stream</em>, not records emitted downstream: a row the deserializer skips still advances it. Were
 * the increment to move inside the emitting branch, a restore after a skip would replay the skipped
 * row and every row emitted after it.
 *
 * <p>Records are emitted without a timestamp: a BigQuery row carries no event time the connector
 * could know about, so assigning one is the job's decision through a watermark strategy.
 */
@Internal
public class BigQueryRecordEmitter<T>
        implements RecordEmitter<GenericRecord, T, BigQueryReadStreamSplitState> {

    private final BigQueryRowDeserializer<T> deserializer;
    private final BigQuerySourceReaderMetrics metrics;

    /**
     * Creates the emitter.
     *
     * @param deserializer converts a row into a record
     * @param metrics the reader's metrics
     */
    public BigQueryRecordEmitter(
            BigQueryRowDeserializer<T> deserializer, BigQuerySourceReaderMetrics metrics) {
        this.deserializer =
                Preconditions.checkNotNull(deserializer, "deserializer must not be null");
        this.metrics = Preconditions.checkNotNull(metrics, "metrics must not be null");
    }

    @Override
    public void emitRecord(
            GenericRecord row, SourceOutput<T> output, BigQueryReadStreamSplitState state)
            throws Exception {
        T record = deserializer.deserialize(row);
        if (record == null) {
            metrics.recordSkipped();
        } else {
            output.collect(record);
        }
        state.recordEmitted();
    }
}
