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

package io.github.flink.gcp.connector.pubsub.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;

import io.github.flink.gcp.connector.pubsub.table.sink.PubSubDynamicSink;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PubSubDynamicTableFactory}. */
class PubSubDynamicTableFactoryTest {

    private static final ResolvedSchema SCHEMA =
            ResolvedSchema.of(
                    Column.physical("id", DataTypes.STRING()),
                    Column.physical("amount", DataTypes.INT()));

    private static Map<String, String> minimalSinkOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", PubSubDynamicTableFactory.IDENTIFIER);
        options.put("project", "my-project");
        options.put("topic", "my-topic");
        options.put("format", "json");
        return options;
    }

    @Test
    void buildsASinkFromTheMinimalOptionSet() {
        DynamicTableSink sink = FactoryMocks.createTableSink(SCHEMA, minimalSinkOptions());

        assertThat(sink).isInstanceOf(PubSubDynamicSink.class);
        assertThat(sink.asSummaryString()).isEqualTo("Pub/Sub table sink");
    }

    @ParameterizedTest(name = "format={0}")
    @ValueSource(strings = {"json", "csv"})
    void discoversAnyEncodingFormat(String format) {
        Map<String, String> options = minimalSinkOptions();
        options.put("format", format);

        assertThat(FactoryMocks.createTableSink(SCHEMA, options))
                .isInstanceOf(PubSubDynamicSink.class);
    }

    @Test
    void rejectsASinkWithoutATopic() {
        Map<String, String> options = minimalSinkOptions();
        options.remove("topic");

        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining(
                        "Option 'topic' is required to write to a 'pubsub' table.");
    }

    @Test
    void rejectsAnUnknownOption() {
        Map<String, String> options = minimalSinkOptions();
        options.put("sink.batching.element-count-thresholds", "10");

        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("sink.batching.element-count-thresholds");
    }

    @Test
    void passesTheConfiguredSinkParallelismToTheProvider() {
        Map<String, String> options = minimalSinkOptions();
        options.put("sink.parallelism", "3");

        DynamicTableSink.SinkRuntimeProvider provider =
                FactoryMocks.createTableSink(SCHEMA, options)
                        .getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));

        assertThat(provider).isInstanceOf(SinkV2Provider.class);
        assertThat(((SinkV2Provider) provider).getParallelism()).contains(3);
    }

    @Test
    void leavesTheSinkParallelismUnsetWhenTheOptionIsAbsent() {
        DynamicTableSink.SinkRuntimeProvider provider =
                FactoryMocks.createTableSink(SCHEMA, minimalSinkOptions())
                        .getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));

        assertThat(((SinkV2Provider) provider).getParallelism()).isEmpty();
    }

    @Test
    void rejectsABlankTopicThroughTheDestinationItBuilds() {
        Map<String, String> options = minimalSinkOptions();
        options.put("topic", "projects/p/topics/t");

        // The connector's own validation is the one that speaks: a TopicDestination component is a
        // bare name, never a resource path.
        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("must not contain '/'");
    }
}
