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
import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.tasks.v2.Task;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksCreateTaskSink;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksWriterOptions;
import io.github.flink.gcp.connector.cloudtasks.sink.QueueDestination;
import io.github.flink.gcp.connector.cloudtasks.table.CloudTasksConnectorOptions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/** Tests for {@link CloudTasksDynamicSink}. */
class CloudTasksDynamicSinkTest {

    private static final DataType PHYSICAL_TYPE =
            DataTypes.ROW(DataTypes.FIELD("payload", DataTypes.STRING()));

    /**
     * Equal by name, as the Pub/Sub twin is. Two independently built sinks can then compare equal
     * at all — a {@code copy()} shares the format reference, and a shared reference short-circuits
     * every field comparison behind it, which is why neither target spec's {@code equals} ran
     * before this — while two differently named formats stay unequal, which is what makes the
     * format itself part of an assertable identity.
     */
    private static final class ConstantEncodingFormat
            implements EncodingFormat<SerializationSchema<RowData>> {

        private final String name;

        private ConstantEncodingFormat() {
            this("format-a");
        }

        private ConstantEncodingFormat(String name) {
            this.name = name;
        }

        @Override
        public SerializationSchema<RowData> createRuntimeEncoder(
                DynamicTableSink.Context context, DataType consumedDataType) {
            // A named type rather than a lambda so that a test asserting the sink carries no
            // SerializedLambda into the job graph measures the connector, not this double.
            return new ConstantSerializationSchema();
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

    /** The encoder {@link ConstantEncodingFormat} hands the runtime sink. */
    private static final class ConstantSerializationSchema implements SerializationSchema<RowData> {

        private static final long serialVersionUID = 1L;

        @Override
        public byte[] serialize(RowData element) {
            return "encoded".getBytes(StandardCharsets.UTF_8);
        }
    }

    private static final DataType OTHER_PHYSICAL_TYPE =
            DataTypes.ROW(DataTypes.FIELD("other", DataTypes.INT()));

    /**
     * The nine constructor arguments as named setters, so an identity case reads as "this field
     * varied" rather than as one more nine-argument call whose changed position has to be counted.
     * Test-local on purpose: the production type takes its arguments positionally.
     */
    private static final class SinkArgs {

        private DataType physicalDataType = PHYSICAL_TYPE;
        private EncodingFormat<SerializationSchema<RowData>> encodingFormat =
                new ConstantEncodingFormat();
        private QueueDestination queue = QueueDestination.of("project", "location", "queue");
        // Qualified: this class declares a target(TargetSpec) setter, which would otherwise shadow
        // the enclosing target(String) helper.
        private TargetSpec target = CloudTasksDynamicSinkTest.target("https://example.com");
        private boolean addressMetadataNotNull;
        private CloudTasksWriterOptions writerOptions = CloudTasksWriterOptions.builder().build();
        private String serviceAccountKeyFile;
        private String emulatorEndpoint;
        private Integer parallelism;

        private SinkArgs physicalDataType(DataType physicalDataType) {
            this.physicalDataType = physicalDataType;
            return this;
        }

        private SinkArgs encodingFormat(
                EncodingFormat<SerializationSchema<RowData>> encodingFormat) {
            this.encodingFormat = encodingFormat;
            return this;
        }

        private SinkArgs queue(QueueDestination queue) {
            this.queue = queue;
            return this;
        }

        private SinkArgs target(TargetSpec target) {
            this.target = target;
            return this;
        }

        private SinkArgs addressMetadataNotNull(boolean addressMetadataNotNull) {
            this.addressMetadataNotNull = addressMetadataNotNull;
            return this;
        }

        private SinkArgs writerOptions(CloudTasksWriterOptions writerOptions) {
            this.writerOptions = writerOptions;
            return this;
        }

        private SinkArgs serviceAccountKeyFile(String serviceAccountKeyFile) {
            this.serviceAccountKeyFile = serviceAccountKeyFile;
            return this;
        }

        private SinkArgs emulatorEndpoint(String emulatorEndpoint) {
            this.emulatorEndpoint = emulatorEndpoint;
            return this;
        }

        private SinkArgs parallelism(Integer parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        private CloudTasksDynamicSink build() {
            return new CloudTasksDynamicSink(
                    physicalDataType,
                    encodingFormat,
                    queue,
                    target,
                    addressMetadataNotNull,
                    writerOptions,
                    serviceAccountKeyFile,
                    emulatorEndpoint,
                    parallelism);
        }
    }

    private static CloudTasksDynamicSink variedSink(UnaryOperator<SinkArgs> variation) {
        return variation.apply(new SinkArgs()).build();
    }

    private static HttpTargetSpec target(String url) {
        Configuration config = new Configuration();
        if (url != null) {
            config.set(CloudTasksConnectorOptions.HTTP_URL, url);
        }
        return HttpTargetSpec.from(config);
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

    private static CloudTasksDynamicSink appEngineSink(
            String relativeUri, boolean relativeUriMetadataNotNull) {
        Configuration config = new Configuration();
        if (relativeUri != null) {
            config.set(CloudTasksConnectorOptions.APP_ENGINE_RELATIVE_URI, relativeUri);
        }
        return new CloudTasksDynamicSink(
                PHYSICAL_TYPE,
                new ConstantEncodingFormat(),
                QueueDestination.of("project", "location", "queue"),
                AppEngineTargetSpec.from(config, null),
                relativeUriMetadataNotNull,
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
    void listsAppEngineMetadataWithoutTheHttpUrl() {
        assertThat(
                        ((SupportsWritingMetadata) appEngineSink("/tasks", false))
                                .listWritableMetadata())
                .containsExactly(
                        entry("relative-uri", DataTypes.STRING().nullable()),
                        entry("http-method", DataTypes.STRING().nullable()),
                        entry(
                                "headers",
                                DataTypes.MAP(
                                                DataTypes.STRING().nullable(),
                                                DataTypes.STRING().nullable())
                                        .nullable()),
                        entry("app-engine-service", DataTypes.STRING().nullable()),
                        entry("app-engine-version", DataTypes.STRING().nullable()),
                        entry("app-engine-instance", DataTypes.STRING().nullable()),
                        entry("schedule-time", DataTypes.TIMESTAMP_LTZ(6).nullable()),
                        entry("task-id", DataTypes.STRING().nullable()));
    }

    @Test
    void aDynamicRelativeUriMustBeSelectedAndDeclaredNotNull() {
        CloudTasksDynamicSink missing = appEngineSink(null, false);
        assertThatThrownBy(
                        () ->
                                missing.applyWritableMetadata(
                                        java.util.Collections.emptyList(), PHYSICAL_TYPE))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("app-engine.relative-uri")
                .hasMessageContaining("relative-uri");

        CloudTasksDynamicSink nullable = appEngineSink(null, false);
        assertThatThrownBy(
                        () ->
                                nullable.applyWritableMetadata(
                                        java.util.Collections.singletonList("relative-uri"),
                                        DataTypes.ROW(
                                                DataTypes.FIELD("payload", DataTypes.STRING()),
                                                DataTypes.FIELD(
                                                        "target", DataTypes.STRING().nullable()))))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("STRING NOT NULL");
    }

    @Test
    void rejectsWritableMetadataFromTheOtherTargetFamily() {
        assertThatThrownBy(
                        () ->
                                appEngineSink("/tasks", false)
                                        .applyWritableMetadata(
                                                java.util.Collections.singletonList("url"),
                                                PHYSICAL_TYPE))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("does not belong")
                .hasMessageContaining("url");

        assertThatThrownBy(
                        () ->
                                sink("https://example.com")
                                        .applyWritableMetadata(
                                                java.util.Collections.singletonList(
                                                        "app-engine-service"),
                                                PHYSICAL_TYPE))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("does not belong")
                .hasMessageContaining("app-engine-service");
    }

    @SuppressWarnings("unchecked")
    private static CloudTasksCreateTaskSink<RowData> runtimeOf(CloudTasksDynamicSink sink) {
        SinkV2Provider provider =
                (SinkV2Provider) sink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        return (CloudTasksCreateTaskSink<RowData>) provider.createSink();
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
        CloudTasksCreateTaskSink<RowData> runtime = runtimeOf(sink);
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
    void theTaskIdExtractorCrossesTheJobGraphAsANamedType() throws Exception {
        // The table layer mints this extractor on the user's behalf and it travels in the job
        // graph, so it must not be a lambda: a lambda is restored by a synthetic-method name the
        // compiler picks, which no connector release pins.
        CloudTasksDynamicSink sink = sink("https://example.com");
        sink.applyWritableMetadata(
                java.util.Collections.singletonList("task-id"),
                DataTypes.ROW(
                        DataTypes.FIELD("payload", DataTypes.STRING()),
                        DataTypes.FIELD("dedupe", DataTypes.STRING())));
        GenericRowData row =
                GenericRowData.of(
                        StringData.fromString("payload"), StringData.fromString("event-17"));
        CloudTasksCreateTaskSink<RowData> runtime = runtimeOf(sink);

        byte[] serialized = InstantiationUtil.serializeObject(runtime);

        assertThat(new String(serialized, StandardCharsets.ISO_8859_1))
                .doesNotContain("SerializedLambda");
        CloudTasksCreateTaskSink<RowData> restored =
                InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());
        assertThat(restored.getConfig().getTaskIdExtractor().extractTaskId(row))
                .isEqualTo("event-17");
    }

    @Test
    void aNullTaskIdColumnExtractsAsANullKey() throws Exception {
        // The metadata column is nullable, so a row may carry SQL NULL there, and the extractor
        // has to read that through isNullAt rather than through the value: a heap row answers
        // getString with null while a binary one answers it with an empty string, so only the
        // null check gives the same key for the same SQL NULL. The writer then rejects a null
        // key by naming the record - CloudTasksWriterFailureHandlerTest holds that half.
        CloudTasksDynamicSink sink = sink("https://example.com");
        sink.applyWritableMetadata(
                java.util.Collections.singletonList("task-id"),
                DataTypes.ROW(
                        DataTypes.FIELD("payload", DataTypes.STRING()),
                        DataTypes.FIELD("dedupe", DataTypes.STRING())));
        GenericRowData row = GenericRowData.of(StringData.fromString("payload"), null);

        assertThat(runtimeOf(sink).getConfig().getTaskIdExtractor().extractTaskId(row)).isNull();
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

    @Test
    void sinksBuiltFromTheSameOptionsAreEqualAndDifferingOnesAreNot() {
        // A copy shares every reference, so it compares equal without any field being read. Two
        // independently built sinks are what drives the field chain - and the target spec's own,
        // which the copy comparison never reached. Flink hashes and compares the sink inside a
        // DynamicTableSinkSpec, so an identity missing a field is observable there.
        assertThat(sink("https://example.com"))
                .isEqualTo(sink("https://example.com"))
                .hasSameHashCodeAs(sink("https://example.com"))
                .isNotEqualTo(sink("https://example.com/other"))
                .isNotEqualTo(sink("https://example.com", true))
                .isNotEqualTo(appEngineSink("/tasks/default", false));
    }

    @Test
    void everyFieldOfTheSinkIsPartOfItsIdentity() {
        // One variation per constructor argument, because the equal pair above holds however many
        // fields equals forgets: Flink hashes and compares the sink inside a DynamicTableSinkSpec,
        // where two sinks the connector considers the same are one entry.
        assertThat(sink("https://example.com"))
                .isNotEqualTo(variedSink(builder -> builder.physicalDataType(OTHER_PHYSICAL_TYPE)))
                .isNotEqualTo(
                        variedSink(
                                builder ->
                                        builder.encodingFormat(
                                                new ConstantEncodingFormat("format-b"))))
                .isNotEqualTo(
                        variedSink(
                                builder ->
                                        builder.queue(
                                                QueueDestination.of(
                                                        "project", "location", "other-queue"))))
                .isNotEqualTo(
                        variedSink(builder -> builder.target(target("https://example.com/x"))))
                .isNotEqualTo(variedSink(builder -> builder.addressMetadataNotNull(true)))
                .isNotEqualTo(
                        variedSink(
                                builder ->
                                        builder.writerOptions(
                                                CloudTasksWriterOptions.builder()
                                                        .maxInFlightTasks(7)
                                                        .build())))
                .isNotEqualTo(variedSink(builder -> builder.serviceAccountKeyFile("/keys/sa.json")))
                .isNotEqualTo(variedSink(builder -> builder.emulatorEndpoint("localhost:8123")))
                .isNotEqualTo(variedSink(builder -> builder.parallelism(3)));
    }

    @Test
    void aMetadataSelectionIsPartOfTheSinkIdentity() {
        // The selection reaches the runtime converter, so two sinks differing only in it are two
        // different sinks - and applyWritableMetadata mutates in place, which is why this is
        // asserted against a sink that has not been through it.
        CloudTasksDynamicSink selected = sink("https://example.com");
        selected.applyWritableMetadata(
                java.util.Collections.singletonList("task-id"),
                DataTypes.ROW(
                        DataTypes.FIELD("payload", DataTypes.STRING()),
                        DataTypes.FIELD("dedupe", DataTypes.STRING())));

        assertThat(selected).isNotEqualTo(sink("https://example.com"));
    }
}
