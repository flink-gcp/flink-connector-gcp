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

package io.github.flink.gcp.connector.bigtable.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.apache.flink.util.InstantiationUtil;

import com.google.api.gax.rpc.ResponseObserver;
import com.google.api.gax.rpc.StreamController;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecord;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecordAdapter.ChangeStreamRecordBuilder;
import com.google.cloud.bigtable.data.v2.models.DefaultChangeStreamRecordAdapter;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamMutation;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.bigtable.source.changestream.TestChangeStreamTokens;
import io.github.flink.gcp.connector.bigtable.source.changestream.reader.ChangeStreamOpener;
import io.github.flink.gcp.connector.bigtable.source.serializer.BigtableChangeStreamMutationDeserializationSchema;
import io.github.flink.gcp.connector.testutils.CollectingReaderOutput;
import io.github.flink.gcp.connector.testutils.FakeSourceReaderContext;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BigtableChangeStreamSourceBuilderTest {

    @Test
    void requiresTableDeserializerAndAppProfile() {
        assertThatThrownBy(
                        () ->
                                BigtableChangeStreamSource.<BigtableChangeStreamMutation>builder()
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("table(...)");
        assertThatThrownBy(
                        () ->
                                BigtableChangeStreamSource.<BigtableChangeStreamMutation>builder()
                                        .table(TableDestination.of("p", "i", "t"))
                                        .build())
                .hasMessageContaining("deserializer(...)");
    }

    @Test
    void endTimeMakesOnlyThatSourceBounded() {
        BigtableChangeStreamSource<BigtableChangeStreamMutation> continuous = minimal().build();
        BigtableChangeStreamSource<BigtableChangeStreamMutation> bounded =
                minimal().endTime(Instant.parse("2026-08-11T00:00:00Z")).build();

        assertThat(continuous.getBoundedness()).isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);
        assertThat(bounded.getBoundedness()).isEqualTo(Boundedness.BOUNDED);
        assertThat(bounded.getProducedType())
                .isEqualTo(
                        new BigtableChangeStreamMutationDeserializationSchema().getProducedType());
    }

    @Test
    void readerReplacesAnExpiredRestoredSplitBeforeItStarts() throws Exception {
        Instant fallback = Instant.parse("2026-08-11T01:00:00Z");
        ChangeStreamPartitionSplit restored =
                new ChangeStreamPartitionSplit(
                        "restored",
                        ByteStringRange.unbounded(),
                        Collections.emptyList(),
                        Instant.parse("2026-08-01T00:00:00Z"));
        BigtableChangeStreamSource<BigtableChangeStreamMutation> source =
                minimal()
                        .opener(new NoOpChangeStreamOpener())
                        .restoreResolver((split, ignored) -> split.restartAt(fallback))
                        .build();
        FakeSourceReaderContext context =
                new FakeSourceReaderContext(
                        InternalSourceReaderMetricGroup.mock(
                                new MetricListener().getMetricGroup()));
        SourceReader<BigtableChangeStreamMutation, ChangeStreamPartitionSplit> reader =
                source.createReader(context);

        reader.addSplits(Collections.singletonList(restored));

        assertThat(reader.snapshotState(1L))
                .singleElement()
                .satisfies(
                        split -> {
                            assertThat(split.getContinuationTokens()).isEmpty();
                            assertThat(split.getLowWatermark()).isEqualTo(fallback);
                        });
        reader.close();
    }

    @Test
    void sourceConfigurationSurvivesJobSubmissionSerialization() throws Exception {
        BigtableChangeStreamSource<BigtableChangeStreamMutation> source =
                minimal()
                        .serviceAccountKeyFile("/var/run/secrets/bigtable.json")
                        .maxConcurrentStreamsPerSubtask(3)
                        .familyIncludeList(Collections.singletonList("selected"))
                        .qualifierExcludeList(Collections.singletonList("selected:Yg=="))
                        .skipMessagesWithoutChange(true)
                        .build();

        byte[] serialized = InstantiationUtil.serializeObject(source);
        Object restored =
                InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());

        assertThat(restored).isInstanceOf(BigtableChangeStreamSource.class);
        BigtableChangeStreamSource<?> restoredSource = (BigtableChangeStreamSource<?>) restored;
        assertThat(restoredSource.getBoundedness()).isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);
        assertThat(restoredSource.getConfig().getServiceAccountKeyFile())
                .isEqualTo("/var/run/secrets/bigtable.json");
        assertThat(restoredSource.getConfig().getMaxConcurrentStreamsPerSubtask()).isEqualTo(3);
        io.github.flink.gcp.connector.bigtable.source.changestream
                        .BigtableChangeStreamMutationFilter
                restoredFilter = restoredSource.getConfig().getMutationFilter();
        assertThat(restoredFilter.hasEntryFilters()).isTrue();
        assertThat(restoredFilter.includesFamily("other")).isFalse();
        assertThat(restoredFilter.includesFamily("selected")).isTrue();
        assertThat(restoredFilter.includesQualifiedColumn("selected", ByteString.copyFromUtf8("a")))
                .isTrue();
        assertThat(restoredFilter.includesQualifiedColumn("selected", ByteString.copyFromUtf8("b")))
                .isFalse();
        assertThat(restoredFilter.skipsMessagesWithoutChange()).isTrue();
    }

    @Test
    void changedFilterConfigurationDoesNotAlterRestoredSplitProgress() throws Exception {
        BigtableChangeStreamSource<BigtableChangeStreamMutation> oldSource =
                minimal().familyIncludeList(Collections.singletonList("old")).build();
        CapturingChangeStreamOpener opener = new CapturingChangeStreamOpener();
        BigtableChangeStreamSource<BigtableChangeStreamMutation> newSource =
                minimal()
                        .familyIncludeList(Collections.singletonList("new"))
                        .opener(opener)
                        .restoreResolver((split, ignored) -> split)
                        .build();
        ByteStringRange partition = ByteStringRange.create("a", "z");
        Instant lowWatermark = Instant.parse("2026-08-14T01:23:45Z");
        ChangeStreamPartitionSplit checkpointed =
                new ChangeStreamPartitionSplit(
                        "restored",
                        partition,
                        Collections.singletonList(
                                TestChangeStreamTokens.token(partition, "checkpoint-token")),
                        lowWatermark);

        byte[] serialized = oldSource.getSplitSerializer().serialize(checkpointed);
        ChangeStreamPartitionSplit restored =
                newSource
                        .getSplitSerializer()
                        .deserialize(oldSource.getSplitSerializer().getVersion(), serialized);

        assertThat(restored).isEqualTo(checkpointed);
        assertThat(restored.getContinuationTokens())
                .singleElement()
                .satisfies(token -> assertThat(token.getToken()).isEqualTo("checkpoint-token"));
        assertThat(restored.getLowWatermark()).isEqualTo(lowWatermark);

        FakeSourceReaderContext context =
                new FakeSourceReaderContext(
                        InternalSourceReaderMetricGroup.mock(
                                new MetricListener().getMetricGroup()));
        SourceReader<BigtableChangeStreamMutation, ChangeStreamPartitionSplit> reader =
                newSource.createReader(context);
        try {
            reader.addSplits(Collections.singletonList(restored));
            reader.start();
            opener.deliver(mutation("old", "new"));
            CollectingReaderOutput<BigtableChangeStreamMutation> output =
                    new CollectingReaderOutput<>();

            reader.pollNext(output);

            assertThat(output.records())
                    .singleElement()
                    .satisfies(
                            delivered ->
                                    assertThat(delivered.getEntries())
                                            .containsExactly(
                                                    new BigtableChangeStreamMutation
                                                            .DeleteFamilyEntry("new")));
            assertThat(reader.snapshotState(1L))
                    .singleElement()
                    .satisfies(
                            split -> {
                                assertThat(split.getContinuationTokens())
                                        .singleElement()
                                        .satisfies(
                                                token ->
                                                        assertThat(token.getToken())
                                                                .isEqualTo("new-token"));
                                assertThat(split.getLowWatermark())
                                        .isEqualTo(lowWatermark.plusSeconds(1));
                            });
        } finally {
            reader.close();
        }
    }

    @Test
    void usesTwoConcurrentStreamsByDefaultAndRejectsNonPositiveLimits() {
        assertThat(minimal().build().getConfig().getMaxConcurrentStreamsPerSubtask()).isEqualTo(2);
        assertThatThrownBy(() -> minimal().maxConcurrentStreamsPerSubtask(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("maximum must be positive");
        assertThatThrownBy(() -> minimal().maxConcurrentStreamsPerSubtask(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("maximum must be positive");
    }

    @Test
    void rejectsNullOrBlankServiceAccountKeyFile() {
        assertThatThrownBy(
                        () ->
                                BigtableChangeStreamSource.<BigtableChangeStreamMutation>builder()
                                        .serviceAccountKeyFile(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("serviceAccountKeyFile must not be null");
        assertThatThrownBy(
                        () ->
                                BigtableChangeStreamSource.<BigtableChangeStreamMutation>builder()
                                        .serviceAccountKeyFile(" \t"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("serviceAccountKeyFile must not be blank");
    }

    @Test
    void rejectsInvalidNullAndMutuallyExclusiveFilterLists() {
        assertThatThrownBy(() -> minimal().familyIncludeList(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("familyIncludeList must not be null");
        assertThatThrownBy(() -> minimal().qualifierIncludeList(Arrays.asList("valid", null)))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("qualifierIncludeList must not contain null");
        assertThatThrownBy(() -> minimal().familyIncludeList(Collections.singletonList("[")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("familyIncludeList pattern at index 0 is invalid");
        assertThatThrownBy(
                        () ->
                                minimal()
                                        .familyIncludeList(Collections.singletonList("a"))
                                        .familyExcludeList(Collections.singletonList("b"))
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("must not both be set");
        assertThatThrownBy(
                        () ->
                                minimal()
                                        .qualifierIncludeList(Collections.singletonList("a:.*"))
                                        .qualifierExcludeList(Collections.singletonList("b:.*"))
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("must not both be set");
    }

    private static BigtableChangeStreamSourceBuilder<BigtableChangeStreamMutation> minimal() {
        return BigtableChangeStreamSource.<BigtableChangeStreamMutation>builder()
                .table(TableDestination.of("p", "i", "t"))
                .appProfileId("single-cluster")
                .deserializer(new BigtableChangeStreamMutationDeserializationSchema());
    }

    private static ChangeStreamRecord mutation(String firstFamily, String secondFamily) {
        ChangeStreamRecordBuilder<ChangeStreamRecord> builder =
                new DefaultChangeStreamRecordAdapter().createChangeStreamRecordBuilder();
        builder.startUserMutation(ByteString.copyFromUtf8("row"), "cluster", Instant.EPOCH, 0);
        builder.deleteFamily(firstFamily);
        builder.deleteFamily(secondFamily);
        return builder.finishChangeStreamMutation(
                "new-token", Instant.parse("2026-08-14T01:23:46Z"));
    }

    private static final class CapturingChangeStreamOpener implements ChangeStreamOpener {
        private ResponseObserver<ChangeStreamRecord> observer;

        @Override
        public void open(
                TableDestination table,
                ChangeStreamPartitionSplit split,
                Instant endTime,
                ResponseObserver<ChangeStreamRecord> observer) {
            this.observer = observer;
            observer.onStart(new TestStreamController());
        }

        private void deliver(ChangeStreamRecord record) {
            observer.onResponse(record);
        }

        @Override
        public void close() throws IOException {}
    }

    private static final class TestStreamController implements StreamController {
        @Override
        public void cancel() {}

        @Override
        public void disableAutoInboundFlowControl() {}

        @Override
        public void request(int count) {}
    }

    private static final class NoOpChangeStreamOpener implements ChangeStreamOpener {
        @Override
        public void open(
                TableDestination table,
                ChangeStreamPartitionSplit split,
                Instant endTime,
                ResponseObserver<ChangeStreamRecord> observer) {}

        @Override
        public void close() throws IOException {}
    }
}
