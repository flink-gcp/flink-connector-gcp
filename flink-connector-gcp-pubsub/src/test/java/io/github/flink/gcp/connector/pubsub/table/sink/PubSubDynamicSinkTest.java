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

package io.github.flink.gcp.connector.pubsub.table.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.types.DataType;

import io.github.flink.gcp.connector.pubsub.sink.CreateDisposition;
import io.github.flink.gcp.connector.pubsub.sink.PubSubPublisherOptions;
import io.github.flink.gcp.connector.pubsub.sink.TopicCreateOptions;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/**
 * Tests for {@link PubSubDynamicSink}.
 *
 * <p>The sink is built directly rather than through the factory (which has its own test), with one
 * shared {@link EncodingFormat} instance: a format is part of the sink's identity, and the built-in
 * formats compare by reference, so two independently discovered ones would make every equality
 * assertion here pass or fail for the wrong reason.
 */
class PubSubDynamicSinkTest {

    /** An encoding format that compares by value, so it does not confuse the equality tests. */
    private static final class ConstantEncodingFormat
            implements EncodingFormat<SerializationSchema<RowData>> {

        private final String name;

        ConstantEncodingFormat(String name) {
            this.name = name;
        }

        @Override
        public SerializationSchema<RowData> createRuntimeEncoder(
                DynamicTableSink.Context context, DataType consumedDataType) {
            return new SerializationSchema<RowData>() {

                private static final long serialVersionUID = 1L;

                @Override
                public byte[] serialize(RowData element) {
                    return "encoded".getBytes(StandardCharsets.UTF_8);
                }
            };
        }

        @Override
        public ChangelogMode getChangelogMode() {
            return ChangelogMode.insertOnly();
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof ConstantEncodingFormat
                    && name.equals(((ConstantEncodingFormat) o).name);
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }
    }

    private static final EncodingFormat<SerializationSchema<RowData>> FORMAT =
            new ConstantEncodingFormat("format-a");

    private static final DataType PHYSICAL_DATA_TYPE =
            DataTypes.ROW(DataTypes.FIELD("id", DataTypes.STRING()));

    private static final DataType CONSUMED_DATA_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("id", DataTypes.STRING()),
                    DataTypes.FIELD("k", DataTypes.STRING()));

    private static final TopicDestination TOPIC = TopicDestination.of("my-project", "my-topic");

    private static PubSubDynamicSink sink() {
        return sink(PubSubPublisherOptions.defaults());
    }

    private static PubSubDynamicSink sink(PubSubPublisherOptions publisherOptions) {
        return new PubSubDynamicSink(
                PHYSICAL_DATA_TYPE, FORMAT, TOPIC, null, null, publisherOptions, null, null, null);
    }

    private static PubSubDynamicSink orderedSink() {
        return sink(PubSubPublisherOptions.builder().enableMessageOrdering(true).build());
    }

    private static PubSubDynamicSink distributedSink(Integer parallelism) {
        return new PubSubDynamicSink(
                PHYSICAL_DATA_TYPE,
                FORMAT,
                TOPIC,
                null,
                null,
                PubSubPublisherOptions.builder().enableMessageOrdering(true).build(),
                null,
                null,
                parallelism);
    }

    private static DataStream<RowData> oneRowInput(StreamExecutionEnvironment environment) {
        return environment
                .fromSequence(0, 0)
                .map(
                        ignored ->
                                (RowData)
                                        GenericRowData.of(
                                                StringData.fromString("id"),
                                                StringData.fromString("key")))
                .returns(RowData.class);
    }

    @Test
    void listsTheMetadataItCanWriteInDeclarationOrder() {
        assertThat(sink().listWritableMetadata())
                .containsExactly(
                        entry(
                                "attributes",
                                DataTypes.MAP(
                                                DataTypes.STRING().nullable(),
                                                DataTypes.STRING().nullable())
                                        .nullable()),
                        entry("ordering-key", DataTypes.STRING().nullable()));
    }

    @Test
    void acceptsAttributesWithoutAnyOrderingSetting() {
        PubSubDynamicSink sink = sink();

        sink.applyWritableMetadata(Collections.singletonList("attributes"), CONSUMED_DATA_TYPE);

        // The applied keys are private, so they are read back through the one thing that exposes
        // them: a sink that applied a *different* key is not equal to this one.
        PubSubDynamicSink applyingTheOtherKey = orderedSink();
        applyingTheOtherKey.applyWritableMetadata(
                Collections.singletonList("ordering-key"), CONSUMED_DATA_TYPE);
        PubSubDynamicSink applyingTheSameKey = sink();
        applyingTheSameKey.applyWritableMetadata(
                Collections.singletonList("attributes"), CONSUMED_DATA_TYPE);

        assertThat(sink).isEqualTo(applyingTheSameKey).isNotEqualTo(applyingTheOtherKey);
    }

    @Test
    void buildsTheSerializerFromTheAppliedMetadata() {
        PubSubDynamicSink sink = orderedSink();
        sink.applyWritableMetadata(Collections.singletonList("ordering-key"), CONSUMED_DATA_TYPE);

        // The provider is only reachable after metadata has been applied in a real plan, so this is
        // the one place the metadata-to-serializer wiring runs outside the emulator IT.
        assertThat(sink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false)))
                .isInstanceOf(DataStreamSinkProvider.class);
    }

    @Test
    void rejectsTheOrderingKeyColumnWhenOrderingIsNotEnabled() {
        PubSubDynamicSink sink = sink();

        assertThatThrownBy(
                        () ->
                                sink.applyWritableMetadata(
                                        Collections.singletonList("ordering-key"),
                                        CONSUMED_DATA_TYPE))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("'ordering-key'")
                .hasMessageContaining("sink.message-ordering.enabled");
    }

    @Test
    void acceptsTheOrderingKeyColumnWhenOrderingIsEnabled() {
        PubSubDynamicSink sink = orderedSink();

        sink.applyWritableMetadata(Arrays.asList("attributes", "ordering-key"), CONSUMED_DATA_TYPE);

        assertThat(sink.copy()).isEqualTo(sink);
    }

    @Test
    void writesInsertsOnly() {
        assertThat(sink().getChangelogMode(ChangelogMode.all()))
                .isEqualTo(ChangelogMode.insertOnly());
    }

    @Test
    void describesItself() {
        assertThat(sink().asSummaryString()).isEqualTo("Pub/Sub table sink");
    }

    @Test
    void buildsASinkV2Provider() {
        DynamicTableSink.SinkRuntimeProvider provider =
                sink().getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));

        assertThat(provider).isInstanceOf(SinkV2Provider.class);
        assertThat(((SinkV2Provider) provider).createSink()).isNotNull();
    }

    @Test
    void anOrderingKeyColumnBuildsAKeyedDataStreamProvider() {
        PubSubDynamicSink sink = distributedSink(3);
        sink.applyWritableMetadata(Collections.singletonList("ordering-key"), CONSUMED_DATA_TYPE);

        DynamicTableSink.SinkRuntimeProvider runtimeProvider =
                sink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));

        assertThat(runtimeProvider).isInstanceOf(DataStreamSinkProvider.class);
        DataStreamSinkProvider provider = (DataStreamSinkProvider) runtimeProvider;
        assertThat(provider.getParallelism()).contains(3);

        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<RowData> input = oneRowInput(environment);
        DataStreamSink<?> attached = provider.consumeDataStream(name -> Optional.empty(), input);
        Transformation<?> inputToWriter = attached.getTransformation().getInputs().get(0);

        assertThat(attached.getTransformation().getParallelism()).isEqualTo(3);
        assertThat(inputToWriter).isInstanceOf(PartitionTransformation.class);
        assertThat(((PartitionTransformation<?>) inputToWriter).getPartitioner().toString())
                .isEqualTo("HASH");
    }

    @Test
    void aSingleWriterNeedsNoOrderingShuffle() {
        PubSubDynamicSink sink = distributedSink(1);
        sink.applyWritableMetadata(Collections.singletonList("ordering-key"), CONSUMED_DATA_TYPE);
        DataStreamSinkProvider provider =
                (DataStreamSinkProvider)
                        sink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<RowData> input = oneRowInput(environment);

        DataStreamSink<?> attached = provider.consumeDataStream(name -> Optional.empty(), input);

        assertThat(attached.getTransformation().getParallelism()).isOne();
        assertThat(attached.getTransformation().getInputs().get(0))
                .isNotInstanceOf(PartitionTransformation.class);
    }

    @Test
    void anUnsetSinkParallelismUsesTheExecutionEnvironmentParallelism() {
        PubSubDynamicSink sink = distributedSink(null);
        sink.applyWritableMetadata(Collections.singletonList("ordering-key"), CONSUMED_DATA_TYPE);
        DataStreamSinkProvider provider =
                (DataStreamSinkProvider)
                        sink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(3);
        DataStream<RowData> input = oneRowInput(environment);
        input.getTransformation().setParallelism(1);

        DataStreamSink<?> attached = provider.consumeDataStream(name -> Optional.empty(), input);

        assertThat(attached.getTransformation().getParallelism()).isEqualTo(3);
        assertThat(attached.getTransformation().getInputs().get(0))
                .isInstanceOf(PartitionTransformation.class);
    }

    @Test
    void theOrderingKeySelectorKeepsEqualNonEmptyKeysTogetherAndSpreadsUnkeyedRows()
            throws Exception {
        PubSubDynamicSink.OrderingKeySelector selector =
                new PubSubDynamicSink.OrderingKeySelector(1);

        assertThat(
                        selector.getKey(
                                GenericRowData.of(
                                        StringData.fromString("a"), StringData.fromString("key"))))
                .isEqualTo(
                        selector.getKey(
                                GenericRowData.of(
                                        StringData.fromString("b"), StringData.fromString("key"))));
        String firstOrderingKey =
                selector.getKey(
                        GenericRowData.of(
                                StringData.fromString("a"), StringData.fromString("key")));
        assertThat(
                        selector.getKey(
                                GenericRowData.of(
                                        StringData.fromString("c"),
                                        StringData.fromString("other"))))
                .isNotEqualTo(firstOrderingKey);
        Set<Integer> unkeyedWriters =
                IntStream.range(0, 128)
                        .mapToObj(
                                i -> {
                                    RowData row =
                                            i % 2 == 0
                                                    ? GenericRowData.of(
                                                            StringData.fromString("null-" + i),
                                                            null)
                                                    : GenericRowData.of(
                                                            StringData.fromString("empty-" + i),
                                                            StringData.fromString(""));
                                    try {
                                        String key = selector.getKey(row);
                                        return KeyGroupRangeAssignment.assignKeyToParallelOperator(
                                                key, 128, 4);
                                    } catch (Exception e) {
                                        throw new AssertionError(e);
                                    }
                                })
                        .collect(Collectors.toSet());

        assertThat(unkeyedWriters).containsExactlyInAnyOrder(0, 1, 2, 3);
    }

    @Test
    void copiesCarryTheAppliedMetadata() {
        PubSubDynamicSink original = orderedSink();
        original.applyWritableMetadata(
                Collections.singletonList("ordering-key"), CONSUMED_DATA_TYPE);

        DynamicTableSink copy = original.copy();

        assertThat(copy).isEqualTo(original).hasSameHashCodeAs(original);
        // The applied metadata is part of the identity: a sink that had none is a different sink.
        assertThat(copy).isNotEqualTo(orderedSink());
    }

    @Test
    void sinksBuiltFromTheSameOptionsAreEqual() {
        assertThat(sink()).isEqualTo(sink()).hasSameHashCodeAs(sink());
    }

    @Test
    void everyFieldOfTheSinkIsPartOfItsIdentity() {
        PubSubPublisherOptions defaults = PubSubPublisherOptions.defaults();
        PubSubPublisherOptions other =
                PubSubPublisherOptions.builder().maxInFlightMessages(7).build();

        assertThat(sink())
                .isNotEqualTo(
                        new PubSubDynamicSink(
                                DataTypes.ROW(DataTypes.FIELD("other", DataTypes.INT())),
                                FORMAT,
                                TOPIC,
                                null,
                                null,
                                defaults,
                                null,
                                null,
                                null))
                .isNotEqualTo(
                        new PubSubDynamicSink(
                                PHYSICAL_DATA_TYPE,
                                new ConstantEncodingFormat("format-b"),
                                TOPIC,
                                null,
                                null,
                                defaults,
                                null,
                                null,
                                null))
                .isNotEqualTo(
                        new PubSubDynamicSink(
                                PHYSICAL_DATA_TYPE,
                                FORMAT,
                                TopicDestination.of("my-project", "other-topic"),
                                null,
                                null,
                                defaults,
                                null,
                                null,
                                null))
                .isNotEqualTo(
                        new PubSubDynamicSink(
                                PHYSICAL_DATA_TYPE,
                                FORMAT,
                                TOPIC,
                                CreateDisposition.CREATE_NEVER,
                                null,
                                defaults,
                                null,
                                null,
                                null))
                // The two fields this test left null on both sides until #786: a mutant dropping
                // either one from equals passed the whole module suite. A field outside a table
                // sink's identity is one the planner may reuse a plan across.
                .isNotEqualTo(
                        new PubSubDynamicSink(
                                PHYSICAL_DATA_TYPE,
                                FORMAT,
                                TOPIC,
                                null,
                                TopicCreateOptions.builder()
                                        .messageRetention(Duration.ofDays(3))
                                        .build(),
                                defaults,
                                null,
                                null,
                                null))
                .isNotEqualTo(sink(other))
                .isNotEqualTo(
                        new PubSubDynamicSink(
                                PHYSICAL_DATA_TYPE,
                                FORMAT,
                                TOPIC,
                                null,
                                null,
                                defaults,
                                "/keys/sa.json",
                                null,
                                null))
                .isNotEqualTo(
                        new PubSubDynamicSink(
                                PHYSICAL_DATA_TYPE,
                                FORMAT,
                                TOPIC,
                                null,
                                null,
                                defaults,
                                null,
                                "localhost:8085",
                                null))
                .isNotEqualTo(
                        new PubSubDynamicSink(
                                PHYSICAL_DATA_TYPE,
                                FORMAT,
                                TOPIC,
                                null,
                                null,
                                defaults,
                                null,
                                null,
                                4));
    }

    @Test
    void aSinkIsNotEqualToNullOrToAnotherType() {
        assertThat(sink()).isNotEqualTo(null).isNotEqualTo("Pub/Sub table sink");
    }
}
