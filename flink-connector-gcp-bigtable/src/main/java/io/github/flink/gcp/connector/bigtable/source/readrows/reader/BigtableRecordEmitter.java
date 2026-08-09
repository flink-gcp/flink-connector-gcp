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

package io.github.flink.gcp.connector.bigtable.source.readrows.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigtable.data.v2.models.Row;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRangeSplitState;
import io.github.flink.gcp.connector.bigtable.source.serializer.BigtableRowDeserializationSchema;

import javax.annotation.Nullable;

/**
 * Deserializes each row and records that the split has passed it.
 *
 * <p>Runs on the task thread, one row at a time, as {@code pollNext} drains the element queue.
 *
 * @param <T> the record type produced
 */
@Internal
public class BigtableRecordEmitter<T> implements RecordEmitter<Row, T, RowRangeSplitState> {

    private final BigtableRowDeserializationSchema<T> deserializer;
    private final BigtableSourceReaderMetrics metrics;

    /** One instance, retargeted per row: a collector per row would allocate for every row read. */
    private final CountingCollector collector = new CountingCollector();

    /**
     * Creates the emitter.
     *
     * @param deserializer the deserializer turning rows into records
     * @param metrics the reader's metrics
     */
    public BigtableRecordEmitter(
            BigtableRowDeserializationSchema<T> deserializer, BigtableSourceReaderMetrics metrics) {
        this.deserializer =
                Preconditions.checkNotNull(deserializer, "deserializer must not be null");
        this.metrics = Preconditions.checkNotNull(metrics, "metrics must not be null");
    }

    @Override
    public void emitRecord(Row row, SourceOutput<T> output, RowRangeSplitState splitState)
            throws Exception {
        collector.retarget(output);
        try {
            deserializer.deserialize(row, collector);
            if (collector.emitted == 0) {
                metrics.recordSkipped();
            }
        } finally {
            collector.retarget(null);
        }
        // Outside the branch above, and outside any success condition: the split resumes at a row
        // key, so a row that produced no record has still been passed and must still move the
        // resume point. Leaving it behind would make a restore replay it and everything after it.
        // A deserializer that *threw* is the other case, and there the emitter never gets here —
        // the failure fails the job rather than advancing past a row nobody saw.
        splitState.recordEmitted(row.getKey());
    }

    /** A collector that counts what one row produced, so a row that produced nothing is visible. */
    private final class CountingCollector implements Collector<T> {

        @Nullable private SourceOutput<T> output;

        private int emitted;

        private void retarget(@Nullable SourceOutput<T> output) {
            this.output = output;
            this.emitted = 0;
        }

        @Override
        public void collect(T record) {
            Preconditions.checkState(
                    output != null,
                    "The collector handed to a Bigtable deserializer was used outside the"
                            + " deserialize call it was handed to.");
            emitted++;
            // No timestamp: a Bigtable row has one per cell rather than one per row, so any
            // row-level event time would be a choice this connector made for the job. A job that
            // wants one assigns a watermark strategy over the records it produced.
            output.collect(record);
        }

        @Override
        public void close() {
            // Nothing to release. Declared by Flink's Collector and deliberately inert: the output
            // belongs to the reader, and a deserializer calling this must not detach it.
        }
    }
}
