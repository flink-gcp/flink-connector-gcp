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

package io.github.flink.gcp.connector.pubsub.table.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.types.DataType;

import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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
            return o instanceof ConstantEncodingFormat;
        }

        @Override
        public int hashCode() {
            return ConstantEncodingFormat.class.hashCode();
        }
    }

    private static final EncodingFormat<SerializationSchema<RowData>> FORMAT =
            new ConstantEncodingFormat();

    private static final DataType PHYSICAL_DATA_TYPE =
            DataTypes.ROW(DataTypes.FIELD("id", DataTypes.STRING()));

    private static final DataType CONSUMED_DATA_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("id", DataTypes.STRING()),
                    DataTypes.FIELD("k", DataTypes.STRING()));

    private static PubSubDynamicSink sink(String... keysAndValues) {
        Map<String, String> options = new HashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            options.put(keysAndValues[i], keysAndValues[i + 1]);
        }
        return new PubSubDynamicSink(
                Configuration.fromMap(options),
                PHYSICAL_DATA_TYPE,
                FORMAT,
                TopicDestination.of("my-project", "my-topic"));
    }

    private static PubSubDynamicSink orderedSink() {
        return sink("sink.message-ordering.enabled", "true");
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

        assertThat(sink).isNotEqualTo(sink());
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
    void sinksDifferingOnlyInAnOptionAreNotEqual() {
        assertThat(sink())
                .isNotEqualTo(sink("sink.in-flight.max-messages", "7"))
                .isNotEqualTo(sink("sink.create-disposition", "create-never"))
                .isNotEqualTo(sink("emulator-endpoint", "localhost:8085"))
                .isNotEqualTo(sink("sink.parallelism", "4"));
    }

    @Test
    void aSinkIsNotEqualToADifferentTopic() {
        PubSubDynamicSink other =
                new PubSubDynamicSink(
                        new Configuration(),
                        PHYSICAL_DATA_TYPE,
                        FORMAT,
                        TopicDestination.of("my-project", "other-topic"));

        assertThat(sink()).isNotEqualTo(other);
    }
}
