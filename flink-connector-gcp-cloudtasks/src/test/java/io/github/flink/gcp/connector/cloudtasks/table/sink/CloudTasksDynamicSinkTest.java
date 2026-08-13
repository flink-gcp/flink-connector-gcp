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

package io.github.flink.gcp.connector.cloudtasks.table.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.types.DataType;

import com.google.cloud.tasks.v2.Task;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksCreateTaskSink;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksWriterOptions;
import io.github.flink.gcp.connector.cloudtasks.sink.QueueDestination;
import io.github.flink.gcp.connector.cloudtasks.table.CloudTasksConnectorOptions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/** Tests for {@link CloudTasksDynamicSink}. */
class CloudTasksDynamicSinkTest {

    private static final DataType PHYSICAL_TYPE =
            DataTypes.ROW(DataTypes.FIELD("payload", DataTypes.STRING()));

    private static final class ConstantEncodingFormat
            implements EncodingFormat<SerializationSchema<RowData>> {

        @Override
        public SerializationSchema<RowData> createRuntimeEncoder(
                DynamicTableSink.Context context, DataType consumedDataType) {
            return element -> "encoded".getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public ChangelogMode getChangelogMode() {
            return ChangelogMode.insertOnly();
        }
    }

    private static TableHttpTarget target(String url) {
        Configuration config = new Configuration();
        if (url != null) {
            config.set(CloudTasksConnectorOptions.HTTP_URL, url);
        }
        return TableHttpTarget.from(config);
    }

    private static CloudTasksDynamicSink sink(String url) {
        return sink(url, false);
    }

    private static CloudTasksDynamicSink sink(String url, boolean urlMetadataNotNull) {
        return new CloudTasksDynamicSink(
                PHYSICAL_TYPE,
                new ConstantEncodingFormat(),
                QueueDestination.of("project", "location", "queue"),
                target(url),
                urlMetadataNotNull,
                CloudTasksWriterOptions.builder().build(),
                null,
                null,
                null);
    }

    @Test
    void listsWritableMetadataInItsStableRequestOrder() {
        assertThat(((SupportsWritingMetadata) sink("https://example.com")).listWritableMetadata())
                .containsExactly(
                        entry("url", DataTypes.STRING().nullable()),
                        entry("http-method", DataTypes.STRING().nullable()),
                        entry(
                                "headers",
                                DataTypes.MAP(
                                                DataTypes.STRING().nullable(),
                                                DataTypes.STRING().nullable())
                                        .nullable()),
                        entry("schedule-time", DataTypes.TIMESTAMP_LTZ(6).nullable()),
                        entry("task-id", DataTypes.STRING().nullable()));
    }

    @Test
    void aDynamicUrlMustBeSelectedAndDeclaredNotNull() {
        CloudTasksDynamicSink missing = sink(null);
        assertThatThrownBy(
                        () ->
                                missing.applyWritableMetadata(
                                        java.util.Collections.emptyList(), PHYSICAL_TYPE))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("url");

        CloudTasksDynamicSink nullable = sink(null);
        assertThatThrownBy(
                        () ->
                                nullable.applyWritableMetadata(
                                        java.util.Collections.singletonList("url"),
                                        DataTypes.ROW(
                                                DataTypes.FIELD("payload", DataTypes.STRING()),
                                                DataTypes.FIELD(
                                                        "target", DataTypes.STRING().nullable()))))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("STRING NOT NULL");
    }

    @Test
    void dynamicUrlAndTaskIdMetadataReachTheirSeparateRuntimeContracts() throws Exception {
        CloudTasksDynamicSink sink = sink(null, true);
        sink.applyWritableMetadata(
                Arrays.asList("url", "task-id"),
                DataTypes.ROW(
                        DataTypes.FIELD("payload", DataTypes.STRING()),
                        DataTypes.FIELD("target", DataTypes.STRING().notNull()),
                        DataTypes.FIELD("dedupe", DataTypes.STRING())));
        SinkV2Provider provider =
                (SinkV2Provider) sink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        CloudTasksCreateTaskSink<RowData> runtime =
                (CloudTasksCreateTaskSink<RowData>) provider.createSink();
        GenericRowData row =
                GenericRowData.of(
                        StringData.fromString("payload"),
                        StringData.fromString("https://row.example/tasks"),
                        StringData.fromString("event-17"));

        Task task = runtime.getConfig().getSerializer().serialize(row);

        assertThat(task.getHttpRequest().getUrl()).isEqualTo("https://row.example/tasks");
        assertThat(runtime.getConfig().getTaskIdExtractor().extractTaskId(row))
                .isEqualTo("event-17");
        assertThat(task.getName()).isEmpty();
    }

    @Test
    void theSinkAcceptsInsertsOnlyAndCopiesMetadataSelection() {
        CloudTasksDynamicSink original = sink("https://example.com");
        original.applyWritableMetadata(
                java.util.Collections.singletonList("task-id"),
                DataTypes.ROW(
                        DataTypes.FIELD("payload", DataTypes.STRING()),
                        DataTypes.FIELD("dedupe", DataTypes.STRING())));

        assertThat(original.getChangelogMode(ChangelogMode.all()))
                .isEqualTo(ChangelogMode.insertOnly());
        assertThat(original.copy()).isEqualTo(original).isNotSameAs(original);
    }
}
