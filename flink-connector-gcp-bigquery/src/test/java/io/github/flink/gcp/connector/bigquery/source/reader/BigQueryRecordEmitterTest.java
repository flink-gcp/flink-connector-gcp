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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import io.github.flink.gcp.connector.bigquery.source.TestRows;
import io.github.flink.gcp.connector.bigquery.source.serializer.BigQueryRowDeserializer;
import io.github.flink.gcp.connector.bigquery.source.split.BigQueryReadStreamSplit;
import io.github.flink.gcp.connector.bigquery.source.split.BigQueryReadStreamSplitState;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BigQueryRecordEmitterTest {

    private static final String STREAM = "projects/p/locations/l/sessions/s/streams/one";

    private TestReaderMetrics metrics;
    private CollectingSourceOutput<String> output;
    private BigQueryReadStreamSplitState state;

    @BeforeEach
    void setUp() {
        metrics = new TestReaderMetrics();
        output = new CollectingSourceOutput<>();
        state =
                new BigQueryReadStreamSplitState(
                        new BigQueryReadStreamSplit(STREAM, 0, TestRows.SCHEMA_JSON));
    }

    @Test
    void advancesTheOffsetOncePerRow() throws Exception {
        BigQueryRecordEmitter<String> emitter = emitter(row -> String.valueOf(row.get("name")));
        List<GenericRecord> rows = TestRows.rows(3);

        for (GenericRecord row : rows) {
            emitter.emitRecord(row, output, state);
        }

        assertThat(state.getOffset()).isEqualTo(3);
        assertThat(output.records()).containsExactly("row-0", "row-1", "row-2");
    }

    @Test
    void advancesTheOffsetForASkippedRowToo() throws Exception {
        // The offset counts rows read from the stream, not records emitted. Were a skip to leave it
        // where it was, a restore would replay the skipped row and everything emitted after it.
        BigQueryRecordEmitter<String> emitter =
                emitter(row -> ((Long) row.get("id")) % 2 == 0 ? null : "kept");
        List<GenericRecord> rows = TestRows.rows(4);

        for (GenericRecord row : rows) {
            emitter.emitRecord(row, output, state);
        }

        assertThat(state.getOffset()).isEqualTo(4);
        assertThat(output.records()).containsExactly("kept", "kept");
        assertThat(metrics.counter("recordsSkipped")).isEqualTo(2);
    }

    @Test
    void emitsWithoutATimestamp() throws Exception {
        // A BigQuery row carries no event time, so assigning one is the job's decision.
        emitter(row -> "x").emitRecord(TestRows.rows(1).get(0), output, state);

        assertThat(output.timestamps()).isEmpty();
    }

    @Test
    void leavesTheOffsetWhereItWasWhenTheDeserializerFails() {
        BigQueryRecordEmitter<String> emitter =
                emitter(
                        row -> {
                            throw new IOException("boom");
                        });

        assertThatThrownBy(() -> emitter.emitRecord(TestRows.rows(1).get(0), output, state))
                .isInstanceOf(IOException.class);
        assertThat(state.getOffset()).isZero();
    }

    private BigQueryRecordEmitter<String> emitter(RowFunction function) {
        return new BigQueryRecordEmitter<>(
                new BigQueryRowDeserializer<String>() {
                    private static final long serialVersionUID = 1L;

                    @Nullable
                    @Override
                    public String deserialize(GenericRecord row) throws IOException {
                        return function.apply(row);
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return Types.STRING;
                    }
                },
                metrics.metrics());
    }

    /** What a test's deserializer does with a row. */
    @FunctionalInterface
    private interface RowFunction {
        @Nullable
        String apply(GenericRecord row) throws IOException;
    }
}
