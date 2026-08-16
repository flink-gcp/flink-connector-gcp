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

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.apache.flink.util.Collector;

import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.SpannerMetricNames;
import io.github.flink.gcp.connector.spanner.source.changestream.ChildPartitionsEvent;
import io.github.flink.gcp.connector.spanner.source.changestream.DataChangeRecord;
import io.github.flink.gcp.connector.spanner.source.changestream.Mod;
import io.github.flink.gcp.connector.spanner.source.changestream.ModType;
import io.github.flink.gcp.connector.spanner.source.changestream.PartitionFinishedEvent;
import io.github.flink.gcp.connector.spanner.source.changestream.PartitionLifecycleState;
import io.github.flink.gcp.connector.spanner.source.changestream.PartitionProgressEvent;
import io.github.flink.gcp.connector.spanner.source.changestream.SpannerChangeStreamInitializationEvent;
import io.github.flink.gcp.connector.spanner.source.changestream.SpannerChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.spanner.source.changestream.SpannerChangeStreamRecordFilter;
import io.github.flink.gcp.connector.spanner.source.changestream.SpannerChangeStreamWatermarkEvent;
import io.github.flink.gcp.connector.spanner.source.changestream.ValueCaptureType;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerChangeStreamDeserializationSchema;
import io.github.flink.gcp.connector.testutils.FakeSourceReaderContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpannerChangeStreamReaderTest {

    private static final SpannerDatabase DATABASE = SpannerDatabase.of("p", "i", "d");
    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");

    private final MetricListener metrics = new MetricListener();
    private final InternalSourceReaderMetricGroup metricGroup =
            InternalSourceReaderMetricGroup.mock(metrics.getMetricGroup());
    private final FakeSourceReaderContext context = new FakeSourceReaderContext(metricGroup);
    private final ScriptedClient client = new ScriptedClient();
    private SpannerChangeStreamReader<String> reader;

    @AfterEach
    void closeReader() throws Exception {
        if (reader != null) {
            reader.close();
        }
    }

    @Test
    void fillsConcurrentCapacityOneAssignmentAtATimeAndReplacesFinishedQueries() throws Exception {
        reader = reader(2, new SequenceDeserializer());
        reader.start();
        assertThat(context.splitRequests()).isEqualTo(1);

        reader.addSplits(Collections.singletonList(split("a")));
        assertThat(client.openIds()).containsExactly("change-stream-token:a");
        assertThat(context.splitRequests()).isEqualTo(2);

        reader.addSplits(Collections.singletonList(split("b")));
        assertThat(client.openIds())
                .containsExactly("change-stream-token:a", "change-stream-token:b");
        assertThat(context.splitRequests()).isEqualTo(2);
        assertThat(counter("changeStreamQueriesStarted")).isEqualTo(2);
        assertThat(gauge("activeChangeStreamQueries")).isEqualTo(2);

        client.finish("change-stream-token:a");
        assertThat(reader.pollNext(new TrackingOutput<>()))
                .isEqualTo(InputStatus.NOTHING_AVAILABLE);
        assertThat(context.sourceEvents().get(0)).isInstanceOf(PartitionFinishedEvent.class);
        assertThat(context.splitRequests()).isEqualTo(3);
    }

    @Test
    void excessRestoredSplitsStayCheckpointedAndOpenOnlyAsCapacityReturns() throws Exception {
        reader = reader(2, new SequenceDeserializer());
        reader.addSplits(java.util.Arrays.asList(split("a"), split("b"), split("c")));
        reader.start();

        assertThat(client.openIds())
                .containsExactly("change-stream-token:a", "change-stream-token:b");
        assertThat(reader.snapshotState(1))
                .extracting(SpannerChangeStreamPartitionSplit::splitId)
                .containsExactly(
                        "change-stream-token:a", "change-stream-token:b", "change-stream-token:c");
        assertThat(context.splitRequests()).isZero();

        client.finish("change-stream-token:a");
        reader.pollNext(new TrackingOutput<>());

        assertThat(client.openIds())
                .containsExactly(
                        "change-stream-token:a", "change-stream-token:b", "change-stream-token:c");
        assertThat(client.maximumOpen()).isEqualTo(2);
        assertThat(context.splitRequests()).isZero();
        assertThat(gauge("queuedChangeStreamPartitions")).isEqualTo(0);
        assertThat(gauge("activeChangeStreamQueries")).isEqualTo(2);
    }

    @Test
    void restoredSplitsWaitForCoordinatorValidation() throws Exception {
        reader =
                new SpannerChangeStreamReader<>(
                        context, DATABASE, new SequenceDeserializer(), 2, client);
        reader.addSplits(java.util.Arrays.asList(split("a"), split("b")));
        reader.start();

        assertThat(client.openIds()).isEmpty();
        assertThat(context.splitRequests()).isZero();
        assertThat(reader.snapshotState(1))
                .extracting(SpannerChangeStreamPartitionSplit::splitId)
                .containsExactly("change-stream-token:a", "change-stream-token:b");

        reader.handleSourceEvents(
                new SpannerChangeStreamInitializationEvent(false, Long.MIN_VALUE));

        assertThat(client.openIds())
                .containsExactly("change-stream-token:a", "change-stream-token:b");
    }

    @Test
    void fallbackInitializationDiscardsReaderRestoredSplitsAndRequestsReplacement()
            throws Exception {
        reader =
                new SpannerChangeStreamReader<>(
                        context, DATABASE, new SequenceDeserializer(), 2, client);
        reader.addSplits(java.util.Arrays.asList(split("a"), split("b")));
        reader.start();

        reader.handleSourceEvents(new SpannerChangeStreamInitializationEvent(true, Long.MIN_VALUE));

        assertThat(client.openIds()).isEmpty();
        assertThat(reader.snapshotState(1)).isEmpty();
        assertThat(context.splitRequests()).isEqualTo(1);
        assertThat(gauge("queuedChangeStreamPartitions")).isEqualTo(0);
    }

    @Test
    void initializationAndUpdatesEmitOneMonotonicSourceWatermarkFromTheMainOutput()
            throws Exception {
        reader =
                new SpannerChangeStreamReader<>(
                        context, DATABASE, new SequenceDeserializer(), 1, client);
        long initialWatermark = START.toEpochMilli() - 1;
        reader.handleSourceEvents(
                new SpannerChangeStreamInitializationEvent(false, initialWatermark));
        reader.start();
        TrackingOutput<String> output = new TrackingOutput<>();

        assertThat(reader.isAvailable()).isCompleted();
        assertThat(reader.pollNext(output)).isEqualTo(InputStatus.NOTHING_AVAILABLE);
        assertThat(output.watermarks).containsExactly(initialWatermark);
        assertThat(output.splitWatermarks).isEmpty();
        assertThat(output.mainIdleMarks).isZero();

        long advanced = START.plusSeconds(5).toEpochMilli() - 1;
        reader.handleSourceEvents(new SpannerChangeStreamWatermarkEvent(advanced));
        reader.handleSourceEvents(new SpannerChangeStreamWatermarkEvent(advanced));
        assertThat(reader.pollNext(output)).isEqualTo(InputStatus.NOTHING_AVAILABLE);
        assertThat(output.watermarks).containsExactly(initialWatermark, advanced);
        assertThat(output.splitWatermarks).isEmpty();
        assertThat(output.mainIdleMarks).isZero();

        assertThatThrownBy(
                        () ->
                                reader.handleSourceEvents(
                                        new SpannerChangeStreamWatermarkEvent(advanced - 1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("moved backwards");
    }

    @Test
    void rejectsAFrontierUpdateBeforeCoordinatorInitialization() throws Exception {
        reader =
                new SpannerChangeStreamReader<>(
                        context, DATABASE, new SequenceDeserializer(), 1, client);

        assertThatThrownBy(
                        () ->
                                reader.handleSourceEvents(
                                        new SpannerChangeStreamWatermarkEvent(
                                                START.toEpochMilli() - 1)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("before initialization");
    }

    @Test
    void oneRecordHandoverPausesUntilTheMailboxDrainsIt() throws Exception {
        reader = reader(1, new SequenceDeserializer());
        reader.addSplits(Collections.singletonList(split("a")));
        reader.start();
        TrackingOutput<String> output = new TrackingOutput<>();

        client.record(
                "change-stream-token:a",
                new SpannerChangeStreamRecord.Data(data("7", START.plusSeconds(1))));

        assertThat(client.resumes("change-stream-token:a")).isZero();
        assertThat(reader.pollNext(output)).isEqualTo(InputStatus.NOTHING_AVAILABLE);
        assertThat(output.records).containsExactly("7");
        assertThat(output.timestamps).containsExactly(START.plusSeconds(1).toEpochMilli());
        assertThat(output.recordSplits).containsExactly("change-stream-token:a");
        assertThat(output.mainRecords).isEmpty();
        assertThat(client.resumes("change-stream-token:a")).isEqualTo(1);
        assertThat(reader.snapshotState(1).get(0).getCurrentPosition())
                .isEqualTo(START.plusSeconds(1));
    }

    @Test
    void oneDataChangeCanEmitSeveralRecordsAtItsCommitTimestamp() throws Exception {
        reader = reader(1, new FanOutDeserializer());
        reader.addSplits(Collections.singletonList(split("a")));
        reader.start();
        TrackingOutput<String> output = new TrackingOutput<>();

        client.record(
                "change-stream-token:a",
                new SpannerChangeStreamRecord.Data(data("7", START.plusSeconds(1))));
        reader.pollNext(output);

        assertThat(output.records).containsExactly("before-7", "after-7");
        assertThat(output.timestamps)
                .containsExactly(
                        START.plusSeconds(1).toEpochMilli(), START.plusSeconds(1).toEpochMilli());
        assertThat(counter(SpannerMetricNames.RECORDS_SKIPPED)).isZero();
        assertThat(reader.snapshotState(1).get(0).getCurrentPosition())
                .isEqualTo(START.plusSeconds(1));
    }

    @Test
    void heartbeatReportsProgressBeforeTheCoordinatorAdvancesTheSourceWatermark() throws Exception {
        reader = reader(1, new SequenceDeserializer());
        SpannerChangeStreamPartitionSplit initial =
                SpannerChangeStreamPartitionSplit.initial(START, null, 2_000)
                        .withLifecycleState(PartitionLifecycleState.RUNNING);
        reader.addSplits(Collections.singletonList(initial));
        reader.start();
        TrackingOutput<String> output = new TrackingOutput<>();

        Instant heartbeat = START.plusSeconds(5);
        client.record(initial.splitId(), new SpannerChangeStreamRecord.Heartbeat(heartbeat));
        reader.pollNext(output);
        assertThat(output.watermarks).isEmpty();
        assertThat(output.splitWatermarks).isEmpty();
        assertThat(output.totalIdleMarks()).isZero();

        long sourceWatermark = heartbeat.toEpochMilli() - 1;
        reader.handleSourceEvents(new SpannerChangeStreamWatermarkEvent(sourceWatermark));
        reader.pollNext(output);
        assertThat(output.watermarks).containsExactly(sourceWatermark);
        assertThat(output.splitWatermarks).isEmpty();
        assertThat(output.totalIdleMarks()).isZero();

        client.record(
                initial.splitId(),
                new SpannerChangeStreamRecord.Children(
                        START.plusSeconds(6),
                        Collections.singletonList(
                                new SpannerChangeStreamRecord.Child(
                                        "child", Collections.singletonList("parent"), true))));
        reader.pollNext(output);
        client.finish(initial.splitId());
        reader.pollNext(output);

        assertThat(context.sourceEvents())
                .extracting(SourceEvent::getClass)
                .containsExactly(
                        PartitionProgressEvent.class,
                        ChildPartitionsEvent.class,
                        PartitionProgressEvent.class,
                        PartitionFinishedEvent.class);
        ChildPartitionsEvent childEvent = (ChildPartitionsEvent) context.sourceEvents().get(1);
        assertThat(childEvent.getChildren().get(0).getParentPartitionIds())
                .containsExactly(
                        SpannerChangeStreamPartitionSplit.INITIAL_PARTITION_ID,
                        SpannerChangeStreamPartitionSplit.idForToken("parent"));
    }

    @Test
    void emptyParentTokensOnInitialSplitNormalizeToInitialParent() throws Exception {
        reader = reader(1, new SequenceDeserializer());
        SpannerChangeStreamPartitionSplit initial =
                SpannerChangeStreamPartitionSplit.initial(START, null, 2_000)
                        .withLifecycleState(PartitionLifecycleState.RUNNING);
        reader.addSplits(Collections.singletonList(initial));
        reader.start();

        client.record(
                initial.splitId(),
                new SpannerChangeStreamRecord.Children(
                        START.plusSeconds(1),
                        Collections.singletonList(
                                new SpannerChangeStreamRecord.Child(
                                        "child", Collections.emptyList(), false))));
        reader.pollNext(new TrackingOutput<>());

        ChildPartitionsEvent childEvent = (ChildPartitionsEvent) context.sourceEvents().get(0);
        assertThat(childEvent.getChildren().get(0).getParentPartitionIds())
                .containsExactly(SpannerChangeStreamPartitionSplit.INITIAL_PARTITION_ID);
    }

    @Test
    void emptyParentTokensOnTokenSplitRemainInvalid() throws Exception {
        reader = reader(1, new SequenceDeserializer());
        SpannerChangeStreamPartitionSplit parent = split("a");
        reader.addSplits(Collections.singletonList(parent));
        reader.start();

        client.record(
                parent.splitId(),
                new SpannerChangeStreamRecord.Children(
                        START.plusSeconds(1),
                        Collections.singletonList(
                                new SpannerChangeStreamRecord.Child(
                                        "child", Collections.emptyList(), false))));

        assertThatThrownBy(() -> reader.pollNext(new TrackingOutput<>()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("parentPartitionIds must not be empty");
    }

    @Test
    void zeroEmissionsAfterOutputSkipButStillCheckpointProgress() throws Exception {
        reader = reader(1, new FirstOnlyDeserializer());
        reader.addSplits(Collections.singletonList(split("a")));
        reader.start();
        TrackingOutput<String> output = new TrackingOutput<>();

        client.record(
                "change-stream-token:a",
                new SpannerChangeStreamRecord.Data(data("1", START.plusSeconds(3))));
        reader.pollNext(output);
        client.record(
                "change-stream-token:a",
                new SpannerChangeStreamRecord.Data(data("2", START.plusSeconds(4))));
        reader.pollNext(output);

        assertThat(output.records).containsExactly("1");
        assertThat(output.timestamps).containsExactly(START.plusSeconds(3).toEpochMilli());
        assertThat(
                        metrics.getCounter(SpannerMetricNames.RECORDS_SKIPPED)
                                .orElseThrow(AssertionError::new)
                                .getCount())
                .isEqualTo(1);
        assertThat(reader.snapshotState(1).get(0).getCurrentPosition())
                .isEqualTo(START.plusSeconds(4));
    }

    @Test
    void deserializerFailureDoesNotAdvanceSplitProgress() throws Exception {
        reader = reader(1, new FailingAfterEmitDeserializer());
        reader.addSplits(Collections.singletonList(split("a")));
        reader.start();
        TrackingOutput<String> output = new TrackingOutput<>();

        client.record(
                "change-stream-token:a",
                new SpannerChangeStreamRecord.Data(data("1", START.plusSeconds(4))));
        assertThatThrownBy(() -> reader.pollNext(output))
                .isInstanceOf(IOException.class)
                .hasMessage("broken deserializer");
        assertThat(output.records).containsExactly("partial-1");
        assertThat(reader.snapshotState(1).get(0).getCurrentPosition()).isEqualTo(START);
        assertThat(context.sourceEvents())
                .noneMatch(event -> event instanceof PartitionProgressEvent);
        assertThat(counter(SpannerMetricNames.RECORDS_SKIPPED)).isZero();
    }

    @Test
    void downstreamFailureDoesNotAdvanceSplitProgress() throws Exception {
        reader = reader(1, new SequenceDeserializer());
        reader.addSplits(Collections.singletonList(split("a")));
        reader.start();
        TrackingOutput<String> output = new TrackingOutput<>();
        output.failOnCollect(new IllegalStateException("downstream"));

        client.record(
                "change-stream-token:a",
                new SpannerChangeStreamRecord.Data(data("1", START.plusSeconds(4))));
        assertThatThrownBy(() -> reader.pollNext(output))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("downstream");

        assertThat(output.records).isEmpty();
        assertThat(reader.snapshotState(1).get(0).getCurrentPosition()).isEqualTo(START);
        assertThat(context.sourceEvents())
                .noneMatch(event -> event instanceof PartitionProgressEvent);
        assertThat(counter(SpannerMetricNames.RECORDS_SKIPPED)).isZero();
    }

    @Test
    void refusesACollectorUsedAfterItsDeserializeCall() throws Exception {
        AtomicReference<Collector<String>> retained = new AtomicReference<>();
        AtomicInteger calls = new AtomicInteger();
        reader =
                reader(
                        1,
                        new SpannerChangeStreamDeserializationSchema<String>() {
                            @Override
                            public void deserialize(
                                    DataChangeRecord record, Collector<String> out) {
                                if (calls.getAndIncrement() == 0) {
                                    retained.set(out);
                                } else {
                                    retained.get().collect("late");
                                }
                            }

                            @Override
                            public TypeInformation<String> getProducedType() {
                                return TypeInformation.of(String.class);
                            }
                        });
        reader.addSplits(Collections.singletonList(split("a")));
        reader.start();
        client.record(
                "change-stream-token:a",
                new SpannerChangeStreamRecord.Data(data("1", START.plusSeconds(4))));
        reader.pollNext(new TrackingOutput<>());

        client.record(
                "change-stream-token:a",
                new SpannerChangeStreamRecord.Data(data("2", START.plusSeconds(5))));
        assertThatThrownBy(() -> reader.pollNext(new TrackingOutput<>()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("only during its synchronous deserialize call");
    }

    @Test
    void rejectsANullCollectedRecordWithoutAdvancingSplitProgress() throws Exception {
        reader =
                reader(
                        1,
                        new SpannerChangeStreamDeserializationSchema<String>() {
                            @Override
                            public void deserialize(
                                    DataChangeRecord record, Collector<String> out) {
                                out.collect(null);
                            }

                            @Override
                            public TypeInformation<String> getProducedType() {
                                return TypeInformation.of(String.class);
                            }
                        });
        reader.addSplits(Collections.singletonList(split("a")));
        reader.start();
        client.record(
                "change-stream-token:a",
                new SpannerChangeStreamRecord.Data(data("1", START.plusSeconds(4))));

        assertThatThrownBy(() -> reader.pollNext(new TrackingOutput<>()))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("source deserializer must not collect null");
        assertThat(reader.snapshotState(1).get(0).getCurrentPosition()).isEqualTo(START);
        assertThat(context.sourceEvents())
                .noneMatch(event -> event instanceof PartitionProgressEvent);
        assertThat(counter(SpannerMetricNames.RECORDS_SKIPPED)).isZero();
    }

    @Test
    void inactivePathDoesNotInvokeTheFilterAndPreservesMetricsAndProgress() throws Exception {
        RecordingDeserializer deserializer = new RecordingDeserializer();
        reader = reader(1, deserializer, filter(null, null, null, "table\\.secret", true), false);
        reader.addSplits(Collections.singletonList(split("a")));
        reader.start();
        DataChangeRecord original = dataWithSecret("1", START.plusSeconds(4));

        client.record("change-stream-token:a", new SpannerChangeStreamRecord.Data(original));
        TrackingOutput<String> output = new TrackingOutput<>();
        reader.pollNext(output);

        assertThat(output.records).containsExactly("1");
        assertThat(deserializer.records).singleElement().isSameAs(original);
        assertThat(counter("changeStreamRecordsFilteredByTable")).isZero();
        assertThat(counter("changeStreamRecordsSkippedWithoutChange")).isZero();
        assertThat(counter("changeStreamColumnOccurrencesFiltered")).isZero();
        assertThat(counter(SpannerMetricNames.RECORDS_SKIPPED)).isZero();
        assertThat(reader.snapshotState(1).get(0).getCurrentPosition())
                .isEqualTo(START.plusSeconds(4));
        assertThat(context.sourceEvents())
                .singleElement()
                .isInstanceOf(PartitionProgressEvent.class);
    }

    @Test
    void tableFilteringBypassesTheDeserializerButStillCheckpointsProgress() throws Exception {
        RecordingDeserializer deserializer = new RecordingDeserializer();
        reader = reader(1, deserializer, filter("included", null, null, null, false));
        reader.addSplits(Collections.singletonList(split("a")));
        reader.start();

        client.record(
                "change-stream-token:a",
                new SpannerChangeStreamRecord.Data(data("1", START.plusSeconds(4))));
        TrackingOutput<String> output = new TrackingOutput<>();
        reader.pollNext(output);

        assertThat(deserializer.records).isEmpty();
        assertThat(output.records).isEmpty();
        assertThat(counter("changeStreamRecordsFilteredByTable")).isEqualTo(1);
        assertThat(counter("changeStreamRecordsSkippedWithoutChange")).isZero();
        assertThat(counter("changeStreamColumnOccurrencesFiltered")).isZero();
        assertThat(reader.snapshotState(1).get(0).getCurrentPosition())
                .isEqualTo(START.plusSeconds(4));
        assertThat(context.sourceEvents())
                .singleElement()
                .isInstanceOf(PartitionProgressEvent.class);
    }

    @Test
    void columnProjectionReachesTheDeserializerAndCountsRemovedOccurrences() throws Exception {
        RecordingDeserializer deserializer = new RecordingDeserializer();
        reader = reader(1, deserializer, filter(null, null, "table\\.visible", null, false));
        reader.addSplits(Collections.singletonList(split("a")));
        reader.start();

        client.record(
                "change-stream-token:a",
                new SpannerChangeStreamRecord.Data(dataWithSecret("1", START.plusSeconds(4))));
        TrackingOutput<String> output = new TrackingOutput<>();
        reader.pollNext(output);

        assertThat(output.records).containsExactly("1");
        assertThat(deserializer.records).singleElement().satisfies(this::assertEmptyProjection);
        assertThat(counter("changeStreamColumnOccurrencesFiltered")).isEqualTo(2);
        assertThat(counter("changeStreamRecordsSkippedWithoutChange")).isZero();
        assertThat(counter(SpannerMetricNames.RECORDS_SKIPPED)).isZero();
    }

    @Test
    void optInSkipsAnEmptyProjectionWithoutUsingTheDeserializerSkipMetric() throws Exception {
        RecordingDeserializer deserializer = new RecordingDeserializer();
        reader = reader(1, deserializer, filter(null, null, "table\\.visible", null, true));
        reader.addSplits(Collections.singletonList(split("a")));
        reader.start();

        client.record(
                "change-stream-token:a",
                new SpannerChangeStreamRecord.Data(dataWithSecret("1", START.plusSeconds(4))));
        TrackingOutput<String> output = new TrackingOutput<>();
        reader.pollNext(output);

        assertThat(output.records).isEmpty();
        assertThat(deserializer.records).isEmpty();
        assertThat(counter("changeStreamRecordsSkippedWithoutChange")).isEqualTo(1);
        assertThat(counter("changeStreamColumnOccurrencesFiltered")).isZero();
        assertThat(counter(SpannerMetricNames.RECORDS_SKIPPED)).isZero();
        assertThat(reader.snapshotState(1).get(0).getCurrentPosition())
                .isEqualTo(START.plusSeconds(4));
    }

    @Test
    void restoredProgressIsUnchangedWhileTheNewReaderUsesChangedFilters() throws Exception {
        MetricListener firstMetrics = new MetricListener();
        InternalSourceReaderMetricGroup firstMetricGroup =
                InternalSourceReaderMetricGroup.mock(firstMetrics.getMetricGroup());
        FakeSourceReaderContext firstContext = new FakeSourceReaderContext(firstMetricGroup);
        ScriptedClient firstClient = new ScriptedClient();
        SpannerChangeStreamReader<String> firstReader =
                new SpannerChangeStreamReader<>(
                        firstContext,
                        DATABASE,
                        new RecordingDeserializer(),
                        filter("other", null, null, null, false),
                        1,
                        firstClient);
        firstReader.handleSourceEvents(
                new SpannerChangeStreamInitializationEvent(false, Long.MIN_VALUE));
        firstReader.addSplits(Collections.singletonList(split("a")));
        firstReader.start();
        firstClient.record(
                "change-stream-token:a",
                new SpannerChangeStreamRecord.Data(data("1", START.plusSeconds(4))));
        firstReader.pollNext(new TrackingOutput<>());
        List<SpannerChangeStreamPartitionSplit> restored = firstReader.snapshotState(1);
        firstReader.close();

        assertThat(restored)
                .singleElement()
                .satisfies(
                        split ->
                                assertThat(split.getCurrentPosition())
                                        .isEqualTo(START.plusSeconds(4)));

        RecordingDeserializer restoredDeserializer = new RecordingDeserializer();
        reader = reader(1, restoredDeserializer, filter("table", null, null, null, false));
        reader.addSplits(restored);
        reader.start();
        client.record(
                "change-stream-token:a",
                new SpannerChangeStreamRecord.Data(data("2", START.plusSeconds(5))));
        TrackingOutput<String> output = new TrackingOutput<>();
        reader.pollNext(output);

        assertThat(output.records).containsExactly("2");
        assertThat(restoredDeserializer.records)
                .extracting(DataChangeRecord::getRecordSequence)
                .containsExactly("2");
        assertThat(reader.snapshotState(2).get(0).getCurrentPosition())
                .isEqualTo(START.plusSeconds(5));
    }

    @Test
    void queryFailureFailsTheTaskWithoutFinishingOrDroppingTheSplit() throws Exception {
        reader = reader(1, new SequenceDeserializer());
        reader.addSplits(Collections.singletonList(split("a")));
        reader.start();
        client.fail("change-stream-token:a", new IOException("broken stream"));

        assertThatThrownBy(() -> reader.pollNext(new TrackingOutput<>()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("change-stream-token:a")
                .hasMessageNotContaining("change stream was created")
                .hasRootCauseMessage("broken stream");
        assertThat(reader.snapshotState(1))
                .extracting(SpannerChangeStreamPartitionSplit::splitId)
                .containsExactly("change-stream-token:a");
        assertThat(context.sourceEvents())
                .noneMatch(event -> event instanceof PartitionFinishedEvent);
        assertThat(gauge("activeChangeStreamQueries")).isEqualTo(0);
    }

    @Test
    void initialQueryFailureAddsStartPositionGuidanceWithoutClassifyingTheCause() throws Exception {
        reader = reader(1, new SequenceDeserializer());
        SpannerChangeStreamPartitionSplit initial =
                SpannerChangeStreamPartitionSplit.initial(START, null, 2_000)
                        .withLifecycleState(PartitionLifecycleState.RUNNING);
        reader.addSplits(Collections.singletonList(initial));
        reader.start();
        client.fail(initial.splitId(), new IOException("vendor wording may change"));

        assertThatThrownBy(() -> reader.pollNext(new TrackingOutput<>()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(initial.splitId())
                .hasMessageContaining(START.toString())
                .hasMessageContaining("within retention")
                .hasMessageContaining("at or after the change stream was created")
                .hasMessageContaining("StartPosition.latest()")
                .hasRootCauseMessage("vendor wording may change");
        assertThat(reader.snapshotState(1)).containsExactly(initial);
    }

    @Test
    void boundedCompletionEndsOnlyAfterEveryAssignedQueryFinishes() throws Exception {
        reader = reader(2, new SequenceDeserializer());
        reader.addSplits(java.util.Arrays.asList(split("a"), split("b")));
        reader.start();
        reader.notifyNoMoreSplits();
        TrackingOutput<String> output = new TrackingOutput<>();

        client.finish("change-stream-token:a");
        assertThat(reader.pollNext(output)).isEqualTo(InputStatus.NOTHING_AVAILABLE);
        client.finish("change-stream-token:b");
        assertThat(reader.pollNext(output)).isEqualTo(InputStatus.END_OF_INPUT);
    }

    private SpannerChangeStreamReader<String> reader(
            int maximum, SpannerChangeStreamDeserializationSchema<String> deserializer) {
        SpannerChangeStreamReader<String> created =
                new SpannerChangeStreamReader<>(context, DATABASE, deserializer, maximum, client);
        created.handleSourceEvents(
                new SpannerChangeStreamInitializationEvent(false, Long.MIN_VALUE));
        return created;
    }

    private SpannerChangeStreamReader<String> reader(
            int maximum,
            SpannerChangeStreamDeserializationSchema<String> deserializer,
            SpannerChangeStreamRecordFilter filter) {
        SpannerChangeStreamReader<String> created =
                new SpannerChangeStreamReader<>(
                        context, DATABASE, deserializer, filter, maximum, client);
        created.handleSourceEvents(
                new SpannerChangeStreamInitializationEvent(false, Long.MIN_VALUE));
        return created;
    }

    private SpannerChangeStreamReader<String> reader(
            int maximum,
            SpannerChangeStreamDeserializationSchema<String> deserializer,
            SpannerChangeStreamRecordFilter filter,
            boolean filtersActive) {
        SpannerChangeStreamReader<String> created =
                new SpannerChangeStreamReader<>(
                        context, DATABASE, deserializer, filter, filtersActive, maximum, client);
        created.handleSourceEvents(
                new SpannerChangeStreamInitializationEvent(false, Long.MIN_VALUE));
        return created;
    }

    private void assertEmptyProjection(DataChangeRecord record) {
        assertThat(record.getColumnTypes())
                .extracting(DataChangeRecord.ColumnType::getName)
                .containsExactly("id");
        assertThat(record.getMods().get(0).getKeysJson()).isEqualTo("{\"id\":1}");
        assertThat(record.getMods().get(0).getNewValuesJson()).contains("{}");
    }

    private long counter(String name) {
        return metrics.getCounter(name).orElseThrow(AssertionError::new).getCount();
    }

    private Object gauge(String name) {
        return metrics.getGauge(name).orElseThrow(AssertionError::new).getValue();
    }

    private static SpannerChangeStreamPartitionSplit split(String token) {
        return new SpannerChangeStreamPartitionSplit(
                token,
                Collections.singletonList(SpannerChangeStreamPartitionSplit.INITIAL_PARTITION_ID),
                START,
                null,
                2_000,
                START,
                PartitionLifecycleState.RUNNING,
                START);
    }

    private static DataChangeRecord data(String sequence, Instant timestamp) {
        return new DataChangeRecord(
                timestamp,
                sequence,
                "tx",
                true,
                "table",
                Collections.singletonList(
                        new DataChangeRecord.ColumnType("id", "{\"code\":\"INT64\"}", true, 1)),
                Collections.singletonList(new Mod("{\"id\":1}", "{}", null)),
                ModType.UPDATE,
                ValueCaptureType.NEW_VALUES,
                1,
                1,
                "",
                false);
    }

    private static DataChangeRecord dataWithSecret(String sequence, Instant timestamp) {
        return new DataChangeRecord(
                timestamp,
                sequence,
                "tx",
                true,
                "table",
                java.util.Arrays.asList(
                        new DataChangeRecord.ColumnType("id", "{\"code\":\"INT64\"}", true, 1),
                        new DataChangeRecord.ColumnType(
                                "secret", "{\"code\":\"STRING\"}", false, 2)),
                Collections.singletonList(new Mod("{\"id\":1}", "{\"secret\":\"hidden\"}", null)),
                ModType.UPDATE,
                ValueCaptureType.NEW_VALUES,
                1,
                1,
                "",
                false);
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

    private static final class SequenceDeserializer
            implements SpannerChangeStreamDeserializationSchema<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(DataChangeRecord record, Collector<String> out) {
            out.collect(record.getRecordSequence());
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }

    private static final class FirstOnlyDeserializer
            implements SpannerChangeStreamDeserializationSchema<String> {

        private static final long serialVersionUID = 1L;
        private boolean emitted;

        @Override
        public void deserialize(DataChangeRecord record, Collector<String> out) {
            if (!emitted) {
                out.collect(record.getRecordSequence());
                emitted = true;
            }
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }

    private static final class FanOutDeserializer
            implements SpannerChangeStreamDeserializationSchema<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(DataChangeRecord record, Collector<String> out) {
            out.collect("before-" + record.getRecordSequence());
            out.collect("after-" + record.getRecordSequence());
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }

    private static final class FailingAfterEmitDeserializer
            implements SpannerChangeStreamDeserializationSchema<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(DataChangeRecord record, Collector<String> out) throws IOException {
            out.collect("partial-" + record.getRecordSequence());
            throw new IOException("broken deserializer");
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }

    private static final class RecordingDeserializer
            implements SpannerChangeStreamDeserializationSchema<String> {

        private static final long serialVersionUID = 1L;

        private final List<DataChangeRecord> records = new ArrayList<>();

        @Override
        public void deserialize(DataChangeRecord record, Collector<String> out) {
            records.add(record);
            out.collect(record.getRecordSequence());
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }

    private static final class ScriptedClient implements SpannerChangeStreamQueryClient {

        private final Map<String, Query> queries = new LinkedHashMap<>();
        private final List<String> openIds = new ArrayList<>();
        private int open;
        private int maximumOpen;

        @Override
        public QueryHandle open(
                SpannerChangeStreamPartitionSplit split,
                SpannerChangeStreamQueryListener listener) {
            Query query = new Query(listener);
            queries.put(split.splitId(), query);
            openIds.add(split.splitId());
            open++;
            maximumOpen = Math.max(maximumOpen, open);
            return query;
        }

        List<String> openIds() {
            return openIds;
        }

        int maximumOpen() {
            return maximumOpen;
        }

        int resumes(String splitId) {
            return queries.get(splitId).resumes;
        }

        void record(String splitId, SpannerChangeStreamRecord record) {
            queries.get(splitId).listener.record(record);
        }

        void finish(String splitId) {
            queries.get(splitId).listener.finished();
        }

        void fail(String splitId, Throwable error) {
            queries.get(splitId).listener.failed(error);
        }

        @Override
        public void close() {}

        private final class Query implements QueryHandle {
            private final SpannerChangeStreamQueryListener listener;
            private int resumes;
            private boolean closed;

            private Query(SpannerChangeStreamQueryListener listener) {
                this.listener = listener;
            }

            @Override
            public void resume() {
                resumes++;
            }

            @Override
            public void cancel() {
                close();
            }

            @Override
            public void close() {
                if (!closed) {
                    closed = true;
                    open--;
                }
            }
        }
    }

    private static class TrackingOutput<T> implements ReaderOutput<T> {

        private final List<T> records = new ArrayList<>();
        private final List<Long> timestamps = new ArrayList<>();
        private final List<Long> watermarks = new ArrayList<>();
        private final List<T> mainRecords = new ArrayList<>();
        private final List<String> recordSplits = new ArrayList<>();
        private final Map<String, List<Long>> splitWatermarks = new LinkedHashMap<>();
        private final Map<String, Integer> splitIdleMarks = new LinkedHashMap<>();
        private int mainIdleMarks;
        private RuntimeException collectFailure;

        private void failOnCollect(RuntimeException failure) {
            collectFailure = failure;
        }

        @Override
        public void collect(T record) {
            mainRecords.add(record);
        }

        @Override
        public void collect(T record, long timestamp) {
            mainRecords.add(record);
        }

        private void throwCollectFailure() {
            if (collectFailure != null) {
                throw collectFailure;
            }
        }

        @Override
        public void emitWatermark(Watermark watermark) {
            watermarks.add(watermark.getTimestamp());
        }

        @Override
        public void markIdle() {
            mainIdleMarks++;
        }

        @Override
        public void markActive() {}

        @Override
        public SourceOutput<T> createOutputForSplit(String splitId) {
            return new TrackingSplitOutput(splitId);
        }

        @Override
        public void releaseOutputForSplit(String splitId) {}

        private int totalIdleMarks() {
            return mainIdleMarks
                    + splitIdleMarks.values().stream().mapToInt(Integer::intValue).sum();
        }

        private final class TrackingSplitOutput implements SourceOutput<T> {

            private final String splitId;

            private TrackingSplitOutput(String splitId) {
                this.splitId = splitId;
            }

            @Override
            public void collect(T record) {
                collect(record, Long.MIN_VALUE);
                timestamps.set(timestamps.size() - 1, null);
            }

            @Override
            public void collect(T record, long timestamp) {
                throwCollectFailure();
                records.add(record);
                timestamps.add(timestamp);
                recordSplits.add(splitId);
            }

            @Override
            public void emitWatermark(Watermark watermark) {
                splitWatermarks
                        .computeIfAbsent(splitId, ignored -> new ArrayList<>())
                        .add(watermark.getTimestamp());
            }

            @Override
            public void markIdle() {
                splitIdleMarks.merge(splitId, 1, Integer::sum);
            }

            @Override
            public void markActive() {}
        }
    }
}
