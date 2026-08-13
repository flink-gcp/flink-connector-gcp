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

package io.github.flink.gcp.connector.spanner.source.batch.reader;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TestPartitions;
import io.github.flink.gcp.connector.spanner.SpannerMetricNames;
import io.github.flink.gcp.connector.spanner.source.TestStructs;
import io.github.flink.gcp.connector.spanner.source.batch.PartitionSplit;
import io.github.flink.gcp.connector.spanner.source.batch.PartitionSplitState;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerStructDeserializationSchema;
import io.github.flink.gcp.connector.testutils.CollectingSourceOutput;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SpannerRecordEmitter}. */
class SpannerRecordEmitterTest {

    private final TestReaderMetrics metrics = new TestReaderMetrics();
    private final CollectingSourceOutput<Long> output = new CollectingSourceOutput<>();

    @Test
    void aRowBecomesOneRecord() throws Exception {
        emitter((row, out) -> out.collect(TestStructs.idOf(row)))
                .emitRecord(TestStructs.row(7), output, state());

        assertThat(output.records()).containsExactly(7L);
        assertThat(metrics.counter(SpannerMetricNames.RECORDS_SKIPPED)).isZero();
    }

    @Test
    void zeroOutputsSkipTheRowAndAreCountedOnce() throws Exception {
        emitter((row, out) -> {}).emitRecord(TestStructs.row(7), output, state());

        // Written nowhere, not a failure, and this counter is the only thing that reports it.
        assertThat(output.records()).isEmpty();
        assertThat(metrics.counter(SpannerMetricNames.RECORDS_SKIPPED)).isEqualTo(1);
    }

    @Test
    void oneRowMayBecomeSeveralRecordsWithoutCountingASkip() throws Exception {
        emitter(
                        (row, out) -> {
                            out.collect(7L);
                            out.collect(8L);
                            out.collect(9L);
                        })
                .emitRecord(TestStructs.row(7), output, state());

        assertThat(output.records()).containsExactly(7L, 8L, 9L);
        assertThat(metrics.counter(SpannerMetricNames.RECORDS_SKIPPED)).isZero();
    }

    @Test
    void noTimestampIsAssigned() throws Exception {
        // A Spanner row carries no event time of its own, so any timestamp here would be this
        // connector choosing one for the job.
        emitter((row, out) -> out.collect(TestStructs.idOf(row)))
                .emitRecord(TestStructs.row(7), output, state());

        assertThat(output.timestamps()).containsExactly((Long) null);
    }

    @Test
    void aThrowingDeserializerFailsRatherThanSkipping() {
        // The other half of the null contract: a row that could not be read is a failure, and a
        // deserializer that threw must not be counted as a skip.
        assertThatThrownBy(
                        () ->
                                emitter(
                                                (row, out) -> {
                                                    throw new IOException("unreadable");
                                                })
                                        .emitRecord(TestStructs.row(7), output, state()))
                .isInstanceOf(IOException.class)
                .hasMessage("unreadable");
        assertThat(metrics.counter(SpannerMetricNames.RECORDS_SKIPPED)).isZero();
    }

    @Test
    void aDownstreamCollectionFailureIsNotCountedAsASkip() {
        output.failOnCollect(new IllegalStateException("downstream exploded"));

        assertThatThrownBy(
                        () ->
                                emitter((row, out) -> out.collect(7L))
                                        .emitRecord(TestStructs.row(7), output, state()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("downstream exploded");
        assertThat(metrics.counter(SpannerMetricNames.RECORDS_SKIPPED)).isZero();
    }

    @Test
    void refusesACollectorUsedAfterItsDeserializeCall() throws Exception {
        AtomicReference<Collector<Long>> retained = new AtomicReference<>();
        AtomicInteger calls = new AtomicInteger();
        SpannerRecordEmitter<Long> emitter =
                emitter(
                        (row, out) -> {
                            if (calls.getAndIncrement() == 0) {
                                retained.set(out);
                            } else {
                                retained.get().collect(8L);
                            }
                        });
        emitter.emitRecord(TestStructs.row(7), output, state());

        assertThatThrownBy(() -> emitter.emitRecord(TestStructs.row(8), output, state()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("only during its synchronous deserialize call");
        assertThat(output.records()).isEmpty();
    }

    @Test
    void rejectsANullCollectedRecord() {
        assertThatThrownBy(
                        () ->
                                emitter((row, out) -> out.collect(null))
                                        .emitRecord(TestStructs.row(7), output, state()))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("source deserializer must not collect null");
        assertThat(metrics.counter(SpannerMetricNames.RECORDS_SKIPPED)).isZero();
    }

    private SpannerRecordEmitter<Long> emitter(Deserialize deserialize) {
        return new SpannerRecordEmitter<>(new TestDeserializer(deserialize), metrics.metrics());
    }

    private static PartitionSplitState state() {
        return new PartitionSplitState(
                new PartitionSplit(
                        "0",
                        TestPartitions.batchTransactionId(),
                        TestPartitions.queryPartition("p0", "SELECT 1")));
    }

    /** What one test's deserializer does with a row. */
    @FunctionalInterface
    private interface Deserialize {
        void apply(Struct row, Collector<Long> out) throws IOException;
    }

    /** A deserializer whose behaviour one test chooses. */
    private static final class TestDeserializer
            implements SpannerStructDeserializationSchema<Long> {

        private static final long serialVersionUID = 1L;

        private final Deserialize deserialize;

        private TestDeserializer(Deserialize deserialize) {
            this.deserialize = deserialize;
        }

        @Override
        public void deserialize(Struct row, Collector<Long> out) throws IOException {
            deserialize.apply(row, out);
        }

        @Override
        public TypeInformation<Long> getProducedType() {
            return TypeInformation.of(Long.class);
        }
    }
}
