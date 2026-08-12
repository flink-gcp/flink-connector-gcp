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

package io.github.flink.gcp.connector.bigtable.source.changestream.reader;

import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.apache.flink.util.Collector;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.Range.BoundType;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import io.github.flink.gcp.connector.bigtable.BigtableMetricNames;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplitState;
import io.github.flink.gcp.connector.bigtable.source.changestream.PartitionProgressEvent;
import io.github.flink.gcp.connector.bigtable.source.changestream.PartitionTransitionEvent;
import io.github.flink.gcp.connector.bigtable.source.serializer.BigtableChangeStreamDeserializationSchema;
import io.github.flink.gcp.connector.testutils.CollectingSourceOutput;
import io.github.flink.gcp.connector.testutils.FakeSourceReaderContext;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

class BigtableChangeStreamRecordEmitterTest {

    private final MetricListener listener = new MetricListener();
    private final SourceReaderMetricGroup metricGroup =
            InternalSourceReaderMetricGroup.mock(listener.getMetricGroup());
    private final FakeSourceReaderContext context = new FakeSourceReaderContext(metricGroup);
    private final BigtableChangeStreamReaderMetrics metrics =
            new BigtableChangeStreamReaderMetrics(metricGroup);

    @Test
    void mutationUsesCommitTimestampAndCheckpointsItsToken() throws Exception {
        Instant commit = Instant.parse("2026-08-11T01:02:03Z");
        Instant watermark = Instant.parse("2026-08-11T01:00:00Z");
        ChangeStreamPartitionSplitState state = state();
        CollectingSourceOutput<String> output = new CollectingSourceOutput<>();
        BigtableChangeStreamRecordEmitter<String> emitter =
                new BigtableChangeStreamRecordEmitter<>(schema(), context, metrics);

        emitter.emitRecord(
                TestChangeStreamRecords.mutation(commit, watermark, "mutation-token"),
                output,
                state);

        assertThat(output.records()).containsExactly("row");
        assertThat(output.timestamps()).containsExactly(commit.toEpochMilli());
        assertThat(state.toSplit().getContinuationTokens().get(0))
                .satisfies(
                        token -> {
                            assertThat(token.getToken()).isEqualTo("mutation-token");
                            assertThat(token.getPartition().getStartBound())
                                    .isEqualTo(BoundType.CLOSED);
                            assertThat(token.getPartition().getEndBound())
                                    .isEqualTo(BoundType.OPEN);
                        });
        assertThat(state.getLowWatermark()).isEqualTo(watermark);
        assertThat(counter(BigtableMetricNames.CHANGE_STREAM_MUTATIONS_READ)).isEqualTo(1);
        assertThat(gauge(BigtableMetricNames.PARTITION_LOW_WATERMARK_MILLIS))
                .isEqualTo(watermark.toEpochMilli());
    }

    @Test
    void heartbeatAdvancesStateWithoutEmittingAndCloseReportsTopology() throws Exception {
        Instant watermark = Instant.parse("2026-08-11T02:00:00Z");
        ChangeStreamPartitionSplitState state = state();
        CollectingSourceOutput<String> output = new CollectingSourceOutput<>();
        BigtableChangeStreamRecordEmitter<String> emitter =
                new BigtableChangeStreamRecordEmitter<>(schema(), context, metrics);

        emitter.emitRecord(
                TestChangeStreamRecords.heartbeat(watermark, "heartbeat"), output, state);
        emitter.emitRecord(TestChangeStreamRecords.close("successor"), output, state);

        assertThat(output.records()).isEmpty();
        assertThat(state.getLowWatermark()).isEqualTo(watermark);
        assertThat(context.sourceEvents()).hasSize(2);
        PartitionProgressEvent progress = (PartitionProgressEvent) context.sourceEvents().get(0);
        assertThat(progress.getLowWatermark()).isEqualTo(watermark);
        assertThat(progress.getContinuationToken().getToken()).isEqualTo("heartbeat");
        PartitionTransitionEvent event = (PartitionTransitionEvent) context.sourceEvents().get(1);
        assertThat(event.getLowWatermark()).isEqualTo(watermark);
        assertThat(event.getSuccessors()).hasSize(1);
        assertThat(event.getSuccessors().get(0).getContinuationToken().getToken())
                .isEqualTo("successor");
        assertThat(counter(BigtableMetricNames.CHANGE_STREAM_HEARTBEATS_READ)).isEqualTo(1);
        assertThat(gauge(BigtableMetricNames.PARTITION_LOW_WATERMARK_MILLIS))
                .isEqualTo(watermark.toEpochMilli());
    }

    @Test
    void sdkRejectsCloseStreamWithUnpairedSuccessorPartitions() {
        org.assertj.core.api.Assertions.assertThatThrownBy(
                        TestChangeStreamRecords::closeWithMismatchedSuccessors)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Number of continuation tokens does not match number of new partitions");
    }

    @Test
    void filteredMutationCountsAsSkipped() throws Exception {
        BigtableChangeStreamRecordEmitter<String> emitter =
                new BigtableChangeStreamRecordEmitter<>(
                        new BigtableChangeStreamDeserializationSchema<String>() {
                            @Override
                            public void deserialize(
                                    ChangeStreamMutation mutation, Collector<String> out) {}

                            @Override
                            public org.apache.flink.api.common.typeinfo.TypeInformation<String>
                                    getProducedType() {
                                return org.apache.flink.api.common.typeinfo.Types.STRING;
                            }
                        },
                        context,
                        metrics);

        emitter.emitRecord(
                TestChangeStreamRecords.mutation(
                        Instant.parse("2026-08-11T03:00:00Z"),
                        Instant.parse("2026-08-11T02:59:00Z"),
                        "filtered"),
                new CollectingSourceOutput<>(),
                state());

        assertThat(counter(BigtableMetricNames.RECORDS_SKIPPED)).isEqualTo(1);
    }

    private long counter(String name) {
        return listener.getCounter(name).orElseThrow(AssertionError::new).getCount();
    }

    private long gauge(String name) {
        Object value = listener.getGauge(name).orElseThrow(AssertionError::new).getValue();
        return ((Number) value).longValue();
    }

    private static ChangeStreamPartitionSplitState state() {
        return new ChangeStreamPartitionSplitState(
                new ChangeStreamPartitionSplit(
                        "change-stream-0",
                        ByteStringRange.unbounded(),
                        Collections.emptyList(),
                        Instant.EPOCH));
    }

    private static BigtableChangeStreamDeserializationSchema<String> schema() {
        return new BigtableChangeStreamDeserializationSchema<String>() {
            @Override
            public void deserialize(ChangeStreamMutation mutation, Collector<String> out) {
                out.collect(mutation.getRowKey().toStringUtf8());
            }

            @Override
            public org.apache.flink.api.common.typeinfo.TypeInformation<String> getProducedType() {
                return org.apache.flink.api.common.typeinfo.Types.STRING;
            }
        };
    }
}
