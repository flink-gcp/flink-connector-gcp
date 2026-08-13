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
import org.apache.flink.util.Collector;

import io.github.flink.gcp.connector.bigquery.source.TestRows;
import io.github.flink.gcp.connector.bigquery.source.serializer.BigQueryRowDeserializer;
import io.github.flink.gcp.connector.bigquery.source.split.BigQueryReadStreamSplit;
import io.github.flink.gcp.connector.bigquery.source.split.BigQueryReadStreamSplitState;
import io.github.flink.gcp.connector.testutils.CollectingSourceOutput;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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
                        new BigQueryReadStreamSplit(STREAM, 0, TestRows.SCHEMA_JSON, null));
    }

    @Test
    void advancesTheOffsetOnceAfterOneOutput() throws Exception {
        emitter((row, out) -> out.collect(String.valueOf(row.get("name"))))
                .emitRecord(TestRows.rows(1).get(0), output, state);

        assertThat(state.getOffset()).isEqualTo(1);
        assertThat(output.records()).containsExactly("row-0");
        assertThat(metrics.counter("recordsSkipped")).isZero();
    }

    @Test
    void advancesTheOffsetOnceAndCountsOneSkipAfterZeroOutputs() throws Exception {
        // The offset counts rows read from the stream, not records emitted. Were a skip to leave it
        // where it was, a restore would replay the skipped row and everything emitted after it.
        emitter((row, out) -> {}).emitRecord(TestRows.rows(1).get(0), output, state);

        assertThat(state.getOffset()).isEqualTo(1);
        assertThat(output.records()).isEmpty();
        assertThat(metrics.counter("recordsSkipped")).isEqualTo(1);
    }

    @Test
    void advancesTheOffsetOnceAndDoesNotCountASkipAfterSeveralOutputs() throws Exception {
        emitter(
                        (row, out) -> {
                            out.collect("first");
                            out.collect("second");
                            out.collect("third");
                        })
                .emitRecord(TestRows.rows(1).get(0), output, state);

        assertThat(state.getOffset()).isEqualTo(1);
        assertThat(output.records()).containsExactly("first", "second", "third");
        assertThat(metrics.counter("recordsSkipped")).isZero();
    }

    @Test
    void emitsWithoutATimestamp() throws Exception {
        // A BigQuery row carries no event time, so assigning one is the job's decision.
        emitter((row, out) -> out.collect("x")).emitRecord(TestRows.rows(1).get(0), output, state);

        // The null is the record's missing timestamp, not the absence of a record: an emptiness
        // assertion here would also pass if nothing had been emitted at all.
        assertThat(output.timestamps()).containsExactly((Long) null);
    }

    @Test
    void leavesTheOffsetWhereItWasWhenTheDeserializerFails() {
        BigQueryRecordEmitter<String> emitter =
                emitter(
                        (row, out) -> {
                            throw new IOException("boom");
                        });

        assertThatThrownBy(() -> emitter.emitRecord(TestRows.rows(1).get(0), output, state))
                .isInstanceOf(IOException.class);
        assertThat(state.getOffset()).isZero();
        assertThat(metrics.counter("recordsSkipped")).isZero();
    }

    @Test
    void leavesTheOffsetWhereItWasWhenDownstreamCollectionFails() {
        output.failAfterCollects(1, new IllegalStateException("downstream exploded"));

        assertThatThrownBy(
                        () ->
                                emitter(
                                                (row, out) -> {
                                                    out.collect("first");
                                                    out.collect("second");
                                                })
                                        .emitRecord(TestRows.rows(1).get(0), output, state))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("downstream exploded");
        assertThat(state.getOffset()).isZero();
        assertThat(output.records()).containsExactly("first");
        assertThat(metrics.counter("recordsSkipped")).isZero();
    }

    @Test
    void refusesACollectorUsedAfterItsDeserializeCall() throws Exception {
        AtomicReference<Collector<String>> retained = new AtomicReference<>();
        AtomicInteger calls = new AtomicInteger();
        BigQueryRecordEmitter<String> emitter =
                emitter(
                        (row, out) -> {
                            if (calls.getAndIncrement() == 0) {
                                retained.set(out);
                            } else {
                                retained.get().collect("late");
                            }
                        });
        emitter.emitRecord(TestRows.rows(2).get(0), output, state);

        assertThatThrownBy(() -> emitter.emitRecord(TestRows.rows(2).get(1), output, state))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("only during its synchronous deserialize call");
        assertThat(state.getOffset()).isEqualTo(1);
        assertThat(output.records()).isEmpty();
    }

    @Test
    void rejectsANullCollectedRecordWithoutAdvancingTheOffset() {
        assertThatThrownBy(
                        () ->
                                emitter((row, out) -> out.collect(null))
                                        .emitRecord(TestRows.rows(1).get(0), output, state))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("source deserializer must not collect null");
        assertThat(state.getOffset()).isZero();
        assertThat(metrics.counter("recordsSkipped")).isZero();
    }

    private BigQueryRecordEmitter<String> emitter(RowFunction function) {
        return new BigQueryRecordEmitter<>(
                new BigQueryRowDeserializer<String>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void deserialize(GenericRecord row, Collector<String> out)
                            throws IOException {
                        function.apply(row, out);
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
        void apply(GenericRecord row, Collector<String> out) throws IOException;
    }
}
