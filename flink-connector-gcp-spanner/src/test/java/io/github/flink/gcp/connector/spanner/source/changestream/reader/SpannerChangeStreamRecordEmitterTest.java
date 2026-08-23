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

package io.github.flink.gcp.connector.spanner.source.changestream.reader;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.apache.flink.util.Collector;

import io.github.flink.gcp.connector.spanner.SpannerMetricNames;
import io.github.flink.gcp.connector.spanner.source.changestream.ChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.spanner.source.changestream.ChangeStreamPartitionSplitState;
import io.github.flink.gcp.connector.spanner.source.changestream.ChildPartitionsEvent;
import io.github.flink.gcp.connector.spanner.source.changestream.DataChangeRecord;
import io.github.flink.gcp.connector.spanner.source.changestream.Mod;
import io.github.flink.gcp.connector.spanner.source.changestream.ModType;
import io.github.flink.gcp.connector.spanner.source.changestream.PartitionLifecycleState;
import io.github.flink.gcp.connector.spanner.source.changestream.PartitionProgressEvent;
import io.github.flink.gcp.connector.spanner.source.changestream.SpannerChangeStreamRecordFilter;
import io.github.flink.gcp.connector.spanner.source.changestream.ValueCaptureType;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerChangeStreamDeserializationSchema;
import io.github.flink.gcp.connector.testutils.CollectingSourceOutput;
import io.github.flink.gcp.connector.testutils.FakeSourceReaderContext;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpannerChangeStreamRecordEmitterTest {

    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");

    private final MetricListener listener = new MetricListener();
    private final SourceReaderMetricGroup metricGroup =
            InternalSourceReaderMetricGroup.mock(listener.getMetricGroup());
    private final FakeSourceReaderContext context = new FakeSourceReaderContext(metricGroup);
    private final SpannerChangeStreamReaderMetrics metrics =
            new SpannerChangeStreamReaderMetrics(metricGroup);

    @Test
    void dataRecordEmitsAllOutputsAtCommitTimestampAndAdvancesProgress() throws Exception {
        Instant commit = START.plusSeconds(1);
        ChangeStreamPartitionSplitState state = state(split("parent"));
        CollectingSourceOutput<String> output = new CollectingSourceOutput<>();
        SpannerChangeStreamRecordEmitter<String> emitter =
                emitter(
                        (record, out) -> {
                            out.collect("before-" + record.getRecordSequence());
                            out.collect("after-" + record.getRecordSequence());
                        });

        emitter.emitRecord(new SpannerChangeStreamRecord.Data(data("7", commit)), output, state);

        assertThat(output.records()).containsExactly("before-7", "after-7");
        assertThat(output.timestamps())
                .containsExactly(commit.toEpochMilli(), commit.toEpochMilli());
        assertThat(state.getCurrentPosition()).isEqualTo(commit);
        assertThat(state.getWatermark()).isEqualTo(START);
        assertThat(context.sourceEvents()).singleElement().satisfies(this::assertProgressAtCommit);
        assertThat(counter(SpannerMetricNames.RECORDS_SKIPPED)).isZero();
    }

    @Test
    void zeroOutputsIncrementTheSkipMetricAndStillAdvanceProgress() throws Exception {
        Instant commit = START.plusSeconds(2);
        ChangeStreamPartitionSplitState state = state(split("parent"));
        SpannerChangeStreamRecordEmitter<String> emitter = emitter((record, out) -> {});

        emitter.emitRecord(
                new SpannerChangeStreamRecord.Data(data("8", commit)),
                new CollectingSourceOutput<>(),
                state);

        assertThat(state.getCurrentPosition()).isEqualTo(commit);
        assertThat(counter(SpannerMetricNames.RECORDS_SKIPPED)).isEqualTo(1);
        assertThat(context.sourceEvents())
                .singleElement()
                .isInstanceOf(PartitionProgressEvent.class);
    }

    @Test
    void inactiveFilterPassesTheOriginalRecordDirectly() throws Exception {
        AtomicReference<DataChangeRecord> delivered = new AtomicReference<>();
        SpannerChangeStreamRecordFilter rejectingFilter =
                filter("another-table", null, null, null, false);
        SpannerChangeStreamRecordEmitter<String> emitter =
                new SpannerChangeStreamRecordEmitter<>(
                        schema(
                                (record, out) -> {
                                    delivered.set(record);
                                    out.collect(record.getRecordSequence());
                                }),
                        rejectingFilter,
                        false,
                        context,
                        metrics);
        DataChangeRecord original = data("9", START.plusSeconds(3));
        CollectingSourceOutput<String> output = new CollectingSourceOutput<>();

        emitter.emitRecord(new SpannerChangeStreamRecord.Data(original), output, state(split("p")));

        assertThat(delivered.get()).isSameAs(original);
        assertThat(output.records()).containsExactly("9");
        assertThat(counter("changeStreamRecordsFilteredByTable")).isZero();
    }

    @Test
    void tableFilterSkipsDeserializationButStillAdvancesProgress() throws Exception {
        AtomicReference<DataChangeRecord> delivered = new AtomicReference<>();
        SpannerChangeStreamRecordEmitter<String> emitter =
                new SpannerChangeStreamRecordEmitter<>(
                        schema((record, out) -> delivered.set(record)),
                        filter("another-table", null, null, null, false),
                        context,
                        metrics);
        ChangeStreamPartitionSplitState state = state(split("p"));

        emitter.emitRecord(
                new SpannerChangeStreamRecord.Data(data("10", START.plusSeconds(4))),
                new CollectingSourceOutput<>(),
                state);

        assertThat(delivered.get()).isNull();
        assertThat(state.getCurrentPosition()).isEqualTo(START.plusSeconds(4));
        assertThat(counter("changeStreamRecordsFilteredByTable")).isEqualTo(1);
        assertThat(counter(SpannerMetricNames.RECORDS_SKIPPED)).isZero();
    }

    @Test
    void heartbeatAndChildrenAdvanceStateAndReportCoordinatorEventsInOrder() throws Exception {
        ChangeStreamPartitionSplit initial =
                ChangeStreamPartitionSplit.initial(START, null, 2_000)
                        .withLifecycleState(PartitionLifecycleState.RUNNING);
        ChangeStreamPartitionSplitState state = state(initial);
        SpannerChangeStreamRecordEmitter<String> emitter = emitter((record, out) -> {});
        CollectingSourceOutput<String> output = new CollectingSourceOutput<>();

        emitter.emitRecord(
                new SpannerChangeStreamRecord.Heartbeat(START.plusSeconds(5)), output, state);
        emitter.emitRecord(
                new SpannerChangeStreamRecord.Children(
                        START.plusSeconds(6),
                        Collections.singletonList(
                                new SpannerChangeStreamRecord.Child(
                                        "child", Collections.singletonList("parent"), true))),
                output,
                state);

        assertThat(output.records()).isEmpty();
        assertThat(state.getCurrentPosition()).isEqualTo(START.plusSeconds(6));
        assertThat(state.getWatermark()).isEqualTo(START.plusSeconds(5));
        assertThat(context.sourceEvents())
                .extracting(SourceEvent::getClass)
                .containsExactly(
                        PartitionProgressEvent.class,
                        ChildPartitionsEvent.class,
                        PartitionProgressEvent.class);
        ChildPartitionsEvent children = (ChildPartitionsEvent) context.sourceEvents().get(1);
        assertThat(children.getChildren().get(0).getParentPartitionIds())
                .containsExactly(
                        ChangeStreamPartitionSplit.INITIAL_PARTITION_ID,
                        ChangeStreamPartitionSplit.idForToken("parent"));
    }

    @Test
    void outputFailureDoesNotAdvanceStateOrReportProgress() {
        ChangeStreamPartitionSplit original = split("parent");
        ChangeStreamPartitionSplitState state = state(original);
        CollectingSourceOutput<String> output = new CollectingSourceOutput<>();
        output.failOnCollect(new IllegalStateException("downstream"));
        SpannerChangeStreamRecordEmitter<String> emitter =
                emitter((record, out) -> out.collect(record.getRecordSequence()));

        assertThatThrownBy(
                        () ->
                                emitter.emitRecord(
                                        new SpannerChangeStreamRecord.Data(
                                                data("11", START.plusSeconds(7))),
                                        output,
                                        state))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("downstream");

        assertThat(state.toSplit()).isEqualTo(original);
        assertThat(context.sourceEvents()).isEmpty();
        assertThat(counter(SpannerMetricNames.RECORDS_SKIPPED)).isZero();
    }

    @Test
    void deserializerFailureAfterOutputDoesNotAdvanceStateOrReportProgress() {
        ChangeStreamPartitionSplit original = split("parent");
        ChangeStreamPartitionSplitState state = state(original);
        SpannerChangeStreamRecordEmitter<String> emitter =
                emitter(
                        (record, out) -> {
                            out.collect(record.getRecordSequence());
                            throw new IOException("broken deserializer");
                        });
        CollectingSourceOutput<String> output = new CollectingSourceOutput<>();

        assertThatThrownBy(
                        () ->
                                emitter.emitRecord(
                                        new SpannerChangeStreamRecord.Data(
                                                data("12", START.plusSeconds(8))),
                                        output,
                                        state))
                .isInstanceOf(IOException.class)
                .hasMessage("broken deserializer");

        assertThat(output.records()).containsExactly("12");
        assertThat(state.toSplit()).isEqualTo(original);
        assertThat(context.sourceEvents()).isEmpty();
    }

    @Test
    void invalidChildDoesNotAdvanceStateOrReportAnyEvent() {
        ChangeStreamPartitionSplit original = split("parent");
        ChangeStreamPartitionSplitState state = state(original);
        SpannerChangeStreamRecordEmitter<String> emitter = emitter((record, out) -> {});

        assertThatThrownBy(
                        () ->
                                emitter.emitRecord(
                                        new SpannerChangeStreamRecord.Children(
                                                START.plusSeconds(9),
                                                Collections.singletonList(
                                                        new SpannerChangeStreamRecord.Child(
                                                                "child",
                                                                Collections.emptyList(),
                                                                false))),
                                        new CollectingSourceOutput<>(),
                                        state))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("parentPartitionIds must not be empty");

        assertThat(state.toSplit()).isEqualTo(original);
        assertThat(context.sourceEvents()).isEmpty();
    }

    @Test
    void rejectsAnUnknownRecordWithoutAdvancingState() {
        ChangeStreamPartitionSplit original = split("parent");
        ChangeStreamPartitionSplitState state = state(original);
        SpannerChangeStreamRecord unknown = () -> START.plusSeconds(10);

        assertThatThrownBy(
                        () ->
                                emitter((record, out) -> {})
                                        .emitRecord(unknown, new CollectingSourceOutput<>(), state))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unsupported Spanner Change Streams record.");

        assertThat(state.toSplit()).isEqualTo(original);
        assertThat(context.sourceEvents()).isEmpty();
    }

    private void assertProgressAtCommit(SourceEvent event) {
        assertThat(event).isInstanceOf(PartitionProgressEvent.class);
        PartitionProgressEvent progress = (PartitionProgressEvent) event;
        assertThat(progress.getSplitId()).isEqualTo("change-stream-token:parent");
        assertThat(progress.getCurrentPosition()).isEqualTo(START.plusSeconds(1));
        assertThat(progress.getWatermark()).isEqualTo(START);
    }

    private long counter(String name) {
        return listener.getCounter(name).orElseThrow(AssertionError::new).getCount();
    }

    private SpannerChangeStreamRecordEmitter<String> emitter(Deserialize deserialize) {
        return new SpannerChangeStreamRecordEmitter<>(
                schema(deserialize), SpannerChangeStreamRecordFilter.none(), context, metrics);
    }

    private static SpannerChangeStreamDeserializationSchema<String> schema(
            Deserialize deserialize) {
        return new SpannerChangeStreamDeserializationSchema<String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public void deserialize(DataChangeRecord record, Collector<String> out)
                    throws IOException {
                deserialize.apply(record, out);
            }

            @Override
            public TypeInformation<String> getProducedType() {
                return TypeInformation.of(String.class);
            }
        };
    }

    private static ChangeStreamPartitionSplitState state(ChangeStreamPartitionSplit split) {
        return new ChangeStreamPartitionSplitState(split);
    }

    private static ChangeStreamPartitionSplit split(String token) {
        return new ChangeStreamPartitionSplit(
                token,
                Collections.singletonList(ChangeStreamPartitionSplit.INITIAL_PARTITION_ID),
                START,
                null,
                2_000,
                START,
                PartitionLifecycleState.RUNNING,
                START);
    }

    private static DataChangeRecord data(String sequence, Instant timestamp) {
        return DataChangeRecord.builder()
                .commitTimestamp(timestamp)
                .recordSequence(sequence)
                .serverTransactionId("tx")
                .lastRecordInTransactionInPartition(true)
                .tableName("table")
                .columnTypes(
                        Collections.singletonList(
                                new DataChangeRecord.ColumnType(
                                        "id", "{\"code\":\"INT64\"}", true, 1)))
                .mods(Collections.singletonList(new Mod("{\"id\":1}", "{}", null)))
                .modType(ModType.UPDATE)
                .valueCaptureType(ValueCaptureType.NEW_VALUES)
                .numberOfRecordsInTransaction(1)
                .numberOfPartitionsInTransaction(1)
                .transactionTag("")
                .systemTransaction(false)
                .build();
    }

    private static SpannerChangeStreamRecordFilter filter(
            String tableInclude,
            String tableExclude,
            String columnInclude,
            String columnExclude,
            boolean skip) {
        return new SpannerChangeStreamRecordFilter(
                patterns(tableInclude),
                patterns(tableExclude),
                patterns(columnInclude),
                patterns(columnExclude),
                skip);
    }

    private static List<Pattern> patterns(String expression) {
        return expression == null
                ? Collections.emptyList()
                : Collections.singletonList(Pattern.compile(expression));
    }

    @FunctionalInterface
    private interface Deserialize {
        void apply(DataChangeRecord record, Collector<String> out) throws IOException;
    }
}
