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

import com.google.api.gax.core.CredentialsProvider;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

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
    void boundedTimestampMakesOnlyThatSourceBounded() {
        BigtableChangeStreamSource<BigtableChangeStreamMutation> continuous = minimal().build();
        BigtableChangeStreamSource<BigtableChangeStreamMutation> bounded =
                minimal().boundedTimestamp(Instant.parse("2026-08-11T00:00:00Z")).build();

        assertThat(continuous.getBoundedness()).isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);
        assertThat(bounded.getBoundedness()).isEqualTo(Boundedness.BOUNDED);
        assertThat(bounded.getProducedType())
                .isEqualTo(
                        new BigtableChangeStreamMutationDeserializationSchema().getProducedType());
    }

    @Test
    void rejectsANullBoundedTimestamp() {
        assertThatThrownBy(() -> minimal().boundedTimestamp(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("boundedTimestamp must not be null");
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
                .hasMessage("maxConcurrentStreamsPerSubtask must be positive");
        assertThatThrownBy(() -> minimal().maxConcurrentStreamsPerSubtask(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("maxConcurrentStreamsPerSubtask must be positive");
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
    void rejectsABlankApplicationProfile() {
        assertThatThrownBy(() -> minimal().appProfileId("  "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("appProfileId must not be blank");
        // U+2028 is the value that tells the two idioms apart: Character.isWhitespace calls it
        // whitespace, and String.trim() leaves it alone because it sits above U+0020. Only this
        // assertion fails if appProfileId returns to trim().isEmpty(); the ASCII one above passes
        // either way. serviceAccountKeyFile, checked here beside it, has always rejected it.
        assertThatThrownBy(() -> minimal().appProfileId("\u2028"))
                .as("U+2028 is blank to isBlank() but survives trim()")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("appProfileId must not be blank");
        assertThatThrownBy(
                        () ->
                                BigtableChangeStreamSource.<BigtableChangeStreamMutation>builder()
                                        .serviceAccountKeyFile("\u2028"))
                .as("the sibling check this one was aligned with")
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

    /**
     * Every pattern setter's message names that setter.
     *
     * <p>Each one passes its own name to {@code compilePatterns} as a string literal, and nothing
     * ties that literal to the method it sits in. Here the expected name is read off the class, so
     * renaming the setter moves the expectation and the stale literal is what fails.
     *
     * <p>Measured rather than assumed, by renaming {@code familyIncludeList} to {@code
     * familyInclude} across the declaration and every caller as an IDE would, leaving both string
     * literals alone. The assertions above keep passing, because the message and the expectation
     * are the same stale literal. {@code BigtableOptionParityTest} does fail — its exemption map is
     * keyed by setter name — but it fails saying the setter is unmapped, which is answered by
     * editing that map; after that edit this was the only failure left in the module's 897 tests.
     */
    @ParameterizedTest
    @MethodSource("patternSetters")
    void everyPatternSetterMessageNamesThatSetter(String setter) throws Exception {
        Method method = BigtableChangeStreamSourceBuilder.class.getMethod(setter, Collection.class);

        Throwable thrown = catchThrowable(() -> method.invoke(minimal(), (Object) null));

        assertThat(thrown)
                .as("%s(null) must be refused through compilePatterns", setter)
                .isInstanceOf(InvocationTargetException.class);
        assertThat(thrown.getCause())
                .isInstanceOf(NullPointerException.class)
                .hasMessage(setter + " must not be null");
    }

    @Test
    void patternSetterSourceStillFindsAllFour() {
        // The source above finds setters by their Collection parameter, which is a proxy for
        // "calls compilePatterns" rather than the thing itself. Naming them, not counting them:
        // a size guard passes when one is retyped to List in the same change that adds an
        // unrelated Collection setter, which is the substitution the proxy makes possible.
        assertThat(patternSetters())
                .as("each must reach everyPatternSetterMessageNamesThatSetter")
                .containsExactly(
                        "familyExcludeList",
                        "familyIncludeList",
                        "qualifierExcludeList",
                        "qualifierIncludeList");
    }

    private static List<String> patternSetters() {
        return Arrays.stream(BigtableChangeStreamSourceBuilder.class.getDeclaredMethods())
                .filter(method -> Modifier.isPublic(method.getModifiers()))
                .filter(method -> !method.isSynthetic())
                .filter(method -> method.getParameterCount() == 1)
                .filter(method -> method.getParameterTypes()[0] == Collection.class)
                .map(Method::getName)
                .sorted()
                .collect(Collectors.toList());
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
                Instant boundedTimestamp,
                ResponseObserver<ChangeStreamRecord> observer) {
            this.observer = observer;
            observer.onStart(new TestStreamController());
        }

        private void deliver(ChangeStreamRecord record) {
            observer.onResponse(record);
        }

        /** Captures rather than streams, so there is nothing to authenticate. */
        @Override
        public void useCredentials(@Nullable CredentialsProvider credentials) {}

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
                Instant boundedTimestamp,
                ResponseObserver<ChangeStreamRecord> observer) {}

        /** Opens nothing, so there is nothing to authenticate. */
        @Override
        public void useCredentials(@Nullable CredentialsProvider credentials) {}

        @Override
        public void close() throws IOException {}
    }
}
