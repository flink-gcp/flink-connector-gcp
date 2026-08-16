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

package io.github.flink.gcp.connector.bigtable.source.changestream.reader;

import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.apache.flink.util.Collector;

import com.google.cloud.bigtable.data.v2.models.Range.BoundType;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamMutationFilter;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamMutation;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        metrics.assigned(Collections.singletonList(state.toSplit()), Collections.emptyList());

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
        assertThat(counter("changeStreamMutationsRead")).isEqualTo(1);
        assertThat(counter("recordsSkipped")).isZero();
        assertThat(gauge("partitionLowWatermarkMillis")).isEqualTo(watermark.toEpochMilli());
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
        metrics.assigned(Collections.singletonList(state.toSplit()), Collections.emptyList());

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
        assertThat(counter("changeStreamHeartbeatsRead")).isEqualTo(1);
        assertThat(counter("changeStreamMutationEntriesFiltered")).isZero();
        assertThat(counter("changeStreamRecordsSkippedWithoutChange")).isZero();
        assertThat(gauge("partitionLowWatermarkMillis")).isEqualTo(watermark.toEpochMilli());
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
    void filteredMutationCountsAsSkippedAndStillAdvancesProtocolState() throws Exception {
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
        Instant watermark = Instant.parse("2026-08-11T02:59:00Z");
        ChangeStreamPartitionSplitState state = state();
        CollectingSourceOutput<String> output = new CollectingSourceOutput<>();

        emitter.emitRecord(
                TestChangeStreamRecords.mutation(
                        Instant.parse("2026-08-11T03:00:00Z"), watermark, "filtered"),
                output,
                state);

        assertThat(output.records()).isEmpty();
        assertThat(counter("recordsSkipped")).isEqualTo(1);
        assertThat(state.toSplit().getContinuationTokens().get(0).getToken()).isEqualTo("filtered");
        assertThat(state.getLowWatermark()).isEqualTo(watermark);
    }

    @Test
    void outputFilterDeliversAnEmptyMutationByDefaultAndCountsRemovedEntries() throws Exception {
        AtomicReference<ChangeStreamMutation> delivered = new AtomicReference<>();
        BigtableChangeStreamRecordEmitter<String> emitter =
                new BigtableChangeStreamRecordEmitter<>(
                        new BigtableChangeStreamDeserializationSchema<String>() {
                            @Override
                            public void deserialize(
                                    ChangeStreamMutation mutation, Collector<String> out) {
                                delivered.set(mutation);
                                out.collect("projected");
                            }

                            @Override
                            public org.apache.flink.api.common.typeinfo.TypeInformation<String>
                                    getProducedType() {
                                return org.apache.flink.api.common.typeinfo.Types.STRING;
                            }
                        },
                        familyInclude("absent", false),
                        context,
                        metrics);
        Instant watermark = Instant.parse("2026-08-11T03:59:00Z");
        ChangeStreamPartitionSplitState state = state();
        CollectingSourceOutput<String> output = new CollectingSourceOutput<>();

        emitter.emitRecord(
                TestChangeStreamRecords.mutationWithThreeEntries(
                        Instant.parse("2026-08-11T04:00:00Z"), watermark, "empty"),
                output,
                state);

        assertThat(output.records()).containsExactly("projected");
        assertThat(delivered.get().getEntries()).isEmpty();
        assertThat(counter("changeStreamMutationEntriesFiltered")).isEqualTo(3);
        assertThat(counter("changeStreamRecordsSkippedWithoutChange")).isZero();
        assertThat(counter("changeStreamMutationsRead")).isEqualTo(1);
        assertThat(state.toSplit().getContinuationTokens().get(0).getToken()).isEqualTo("empty");
        assertThat(state.getLowWatermark()).isEqualTo(watermark);
    }

    @Test
    void partialOutputFilterCountsRemovedEntriesOnADeliveredMutation() throws Exception {
        AtomicReference<ChangeStreamMutation> delivered = new AtomicReference<>();
        BigtableChangeStreamRecordEmitter<String> emitter =
                new BigtableChangeStreamRecordEmitter<>(
                        new BigtableChangeStreamDeserializationSchema<String>() {
                            @Override
                            public void deserialize(
                                    ChangeStreamMutation mutation, Collector<String> out) {
                                delivered.set(mutation);
                                out.collect("projected");
                            }

                            @Override
                            public org.apache.flink.api.common.typeinfo.TypeInformation<String>
                                    getProducedType() {
                                return org.apache.flink.api.common.typeinfo.Types.STRING;
                            }
                        },
                        familyInclude("family-2", false),
                        context,
                        metrics);

        emitter.emitRecord(
                TestChangeStreamRecords.mutationWithThreeEntries(
                        Instant.parse("2026-08-11T04:30:00Z"),
                        Instant.parse("2026-08-11T04:29:00Z"),
                        "partial"),
                new CollectingSourceOutput<>(),
                state());

        assertThat(delivered.get().getEntries())
                .extracting(ChangeStreamMutation.Entry::getFamilyName)
                .containsExactly("family-2");
        assertThat(counter("changeStreamMutationEntriesFiltered")).isEqualTo(2);
        assertThat(counter("changeStreamRecordsSkippedWithoutChange")).isZero();
        assertThat(counter("changeStreamMutationsRead")).isEqualTo(1);
    }

    @Test
    void skipEmptyOutputFilterDoesNotInvokeDeserializerAndStillAdvancesState() throws Exception {
        BigtableChangeStreamRecordEmitter<String> emitter =
                new BigtableChangeStreamRecordEmitter<>(
                        new BigtableChangeStreamDeserializationSchema<String>() {
                            @Override
                            public void deserialize(
                                    ChangeStreamMutation mutation, Collector<String> out) {
                                throw new AssertionError("a skipped mutation must not deserialize");
                            }

                            @Override
                            public org.apache.flink.api.common.typeinfo.TypeInformation<String>
                                    getProducedType() {
                                return org.apache.flink.api.common.typeinfo.Types.STRING;
                            }
                        },
                        familyInclude("absent", true),
                        context,
                        metrics);
        Instant watermark = Instant.parse("2026-08-11T04:59:00Z");
        ChangeStreamPartitionSplitState state = state();
        CollectingSourceOutput<String> output = new CollectingSourceOutput<>();

        emitter.emitRecord(
                TestChangeStreamRecords.mutation(
                        Instant.parse("2026-08-11T05:00:00Z"), watermark, "skipped-empty"),
                output,
                state);

        assertThat(output.records()).isEmpty();
        assertThat(counter("changeStreamMutationEntriesFiltered")).isEqualTo(1);
        assertThat(counter("changeStreamRecordsSkippedWithoutChange")).isEqualTo(1);
        assertThat(counter("recordsSkipped")).isZero();
        assertThat(counter("changeStreamMutationsRead")).isEqualTo(1);
        assertThat(state.toSplit().getContinuationTokens().get(0).getToken())
                .isEqualTo("skipped-empty");
        assertThat(state.getLowWatermark()).isEqualTo(watermark);
    }

    @Test
    void downstreamFailureDoesNotAdvanceMutationStateOrMetrics() {
        Instant watermark = Instant.parse("2026-08-11T02:59:00Z");
        ChangeStreamPartitionSplitState state = state();
        CollectingSourceOutput<String> output = new CollectingSourceOutput<>();
        output.failOnCollect(new IllegalStateException("downstream"));
        BigtableChangeStreamRecordEmitter<String> emitter =
                new BigtableChangeStreamRecordEmitter<>(schema(), context, metrics);

        assertThatThrownBy(
                        () ->
                                emitter.emitRecord(
                                        TestChangeStreamRecords.mutation(
                                                Instant.parse("2026-08-11T03:00:00Z"),
                                                watermark,
                                                "failed"),
                                        output,
                                        state))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("downstream");

        assertThat(output.records()).isEmpty();
        assertThat(state.toSplit().getContinuationTokens()).isEmpty();
        assertThat(state.getLowWatermark()).isEqualTo(Instant.EPOCH);
        assertThat(counter("changeStreamMutationsRead")).isZero();
        assertThat(counter("recordsSkipped")).isZero();
    }

    @Test
    void refusesACollectorUsedAfterItsDeserializeCall() throws Exception {
        AtomicReference<Collector<String>> retained = new AtomicReference<>();
        AtomicInteger calls = new AtomicInteger();
        BigtableChangeStreamRecordEmitter<String> emitter =
                new BigtableChangeStreamRecordEmitter<>(
                        new BigtableChangeStreamDeserializationSchema<String>() {
                            @Override
                            public void deserialize(
                                    ChangeStreamMutation mutation, Collector<String> out) {
                                if (calls.getAndIncrement() == 0) {
                                    retained.set(out);
                                } else {
                                    retained.get().collect("late");
                                }
                            }

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
                        "retained"),
                new CollectingSourceOutput<>(),
                state());

        assertThatThrownBy(
                        () ->
                                emitter.emitRecord(
                                        TestChangeStreamRecords.mutation(
                                                Instant.parse("2026-08-11T03:01:00Z"),
                                                Instant.parse("2026-08-11T03:00:00Z"),
                                                "second"),
                                        new CollectingSourceOutput<>(),
                                        state()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("only during its synchronous deserialize call");
    }

    @Test
    void rejectsANullCollectedRecordWithoutAdvancingTheSplit() {
        ChangeStreamPartitionSplitState state = state();
        BigtableChangeStreamRecordEmitter<String> emitter =
                new BigtableChangeStreamRecordEmitter<>(
                        new BigtableChangeStreamDeserializationSchema<String>() {
                            @Override
                            public void deserialize(
                                    ChangeStreamMutation mutation, Collector<String> out) {
                                out.collect(null);
                            }

                            @Override
                            public org.apache.flink.api.common.typeinfo.TypeInformation<String>
                                    getProducedType() {
                                return org.apache.flink.api.common.typeinfo.Types.STRING;
                            }
                        },
                        context,
                        metrics);

        assertThatThrownBy(
                        () ->
                                emitter.emitRecord(
                                        TestChangeStreamRecords.mutation(
                                                Instant.parse("2026-08-11T03:00:00Z"),
                                                Instant.parse("2026-08-11T02:59:00Z"),
                                                "null"),
                                        new CollectingSourceOutput<>(),
                                        state))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("source deserializer must not collect null");
        assertThat(state.toSplit().getContinuationTokens()).isEmpty();
        assertThat(counter("recordsSkipped")).isZero();
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

    private static BigtableChangeStreamMutationFilter familyInclude(
            String pattern, boolean skipMessagesWithoutChange) {
        return new BigtableChangeStreamMutationFilter(
                Collections.singletonList(Pattern.compile(pattern)),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                skipMessagesWithoutChange);
    }
}
