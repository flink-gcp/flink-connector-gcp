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

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TestPartitions;
import io.github.flink.gcp.connector.spanner.SpannerMetricNames;
import io.github.flink.gcp.connector.spanner.source.TestStructs;
import io.github.flink.gcp.connector.spanner.source.batch.PartitionSplit;
import io.github.flink.gcp.connector.spanner.source.batch.PartitionSplitState;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerStructDeserializationSchema;
import io.github.flink.gcp.connector.testutils.CollectingSourceOutput;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SpannerRecordEmitter}. */
class SpannerRecordEmitterTest {

    private final TestReaderMetrics metrics = new TestReaderMetrics();
    private final CollectingSourceOutput<Long> output = new CollectingSourceOutput<>();

    @Test
    void aRowBecomesOneRecord() throws Exception {
        emitter(row -> TestStructs.idOf(row)).emitRecord(TestStructs.row(7), output, state());

        assertThat(output.records()).containsExactly(7L);
        assertThat(metrics.counter(SpannerMetricNames.RECORDS_SKIPPED)).isZero();
    }

    @Test
    void aNullFromTheDeserializerSkipsTheRowAndIsCounted() throws Exception {
        emitter(row -> null).emitRecord(TestStructs.row(7), output, state());

        // Written nowhere, not a failure, and this counter is the only thing that reports it.
        assertThat(output.records()).isEmpty();
        assertThat(metrics.counter(SpannerMetricNames.RECORDS_SKIPPED)).isEqualTo(1);
    }

    @Test
    void noTimestampIsAssigned() throws Exception {
        // A Spanner row carries no event time of its own, so any timestamp here would be this
        // connector choosing one for the job.
        emitter(row -> TestStructs.idOf(row)).emitRecord(TestStructs.row(7), output, state());

        assertThat(output.timestamps()).containsExactly((Long) null);
    }

    @Test
    void aThrowingDeserializerFailsRatherThanSkipping() {
        // The other half of the null contract: a row that could not be read is a failure, and a
        // deserializer that threw must not be counted as a skip.
        assertThatThrownBy(
                        () ->
                                emitter(
                                                row -> {
                                                    throw new IOException("unreadable");
                                                })
                                        .emitRecord(TestStructs.row(7), output, state()))
                .isInstanceOf(IOException.class)
                .hasMessage("unreadable");
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
        @Nullable
        Long apply(Struct row) throws IOException;
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
        @Nullable
        public Long deserialize(Struct row) throws IOException {
            return deserialize.apply(row);
        }

        @Override
        public TypeInformation<Long> getProducedType() {
            return TypeInformation.of(Long.class);
        }
    }
}
