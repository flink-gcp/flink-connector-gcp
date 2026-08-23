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

package io.github.flink.gcp.connector.bigquery.sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Empty;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.bigquery.sink.cdc.CdcChangeType;
import io.github.flink.gcp.connector.bigquery.sink.cdc.CdcChangeTypeProvider;
import io.github.flink.gcp.connector.bigquery.sink.cdc.CdcOptions;
import io.github.flink.gcp.connector.bigquery.sink.cdc.CdcSequenceNumberProvider;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.BigQueryFileLoadsSink;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalField;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalFieldNullPolicy;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalFieldType;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalFieldValueProvider;
import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalFields;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer;
import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryBufferedStreamSink;
import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryDefaultStreamSink;
import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamOptions;
import io.github.flink.gcp.connector.bigquery.sink.storage.DefaultStreamOptions;
import io.github.flink.gcp.connector.bigquery.sink.storage.writer.BufferedStreamWriterState;
import io.github.flink.gcp.connector.bigquery.sink.storage.writer.WriteClientBufferedStreamServiceFactory;
import io.github.flink.gcp.connector.testutils.TestContexts;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.ObjectStreamClass;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BigQuerySinkBuilder}. */
class BigQuerySinkBuilderTest {

    private static final String SERVICE_ACCOUNT_KEY_FILE = "/var/run/secrets/bigquery-key.json";

    private static final TableDestination DESTINATION =
            TableDestination.of("my-project", "my_dataset", "my_table");

    private static final SinkWriter.Context CONTEXT = TestContexts.NO_OP;

    @Test
    void sinkConfigKeepsItsLegacySerializationIdentity() {
        assertThat(ObjectStreamClass.lookup(BigQuerySinkConfig.class).getSerialVersionUID())
                .isEqualTo(1L);
    }

    @Test
    void nothingTheBuilderMintsReachesTheJobGraphAsALambda() throws Exception {
        // A serialized lambda is bound by its SerializedLambda synthetic-method name, which the
        // compiler picks and no connector version pins. Whatever the builder supplies on the
        // user's behalf must therefore be a named type; a lambda the user passes in is their own.
        BigQueryDefaultStreamSink<String> defaults =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .table(DESTINATION)
                                .serializer(new TestSerializer())
                                .build();
        BigQueryDefaultStreamSink<String> configured =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .table(DESTINATION)
                                .serializer(new TestSerializer())
                                // Named providers, so anything the assertion finds was minted here
                                // rather than handed in: a user's own lambda is their business.
                                .additionalFields(
                                        AdditionalFields.<String>builder()
                                                .field(
                                                        AdditionalField.of(
                                                                "computed",
                                                                AdditionalFieldType.STRING,
                                                                AdditionalFieldNullPolicy.REQUIRED,
                                                                new NamedValueProvider()))
                                                .build())
                                .cdcOptions(
                                        CdcOptions.<String>builder(
                                                        CdcChangeTypeProvider.upsertOnly())
                                                .sequenceNumberProvider(new NamedSequenceProvider())
                                                .build())
                                .cdcTableOptions(
                                        CdcTableOptions.builder()
                                                .primaryKeyColumns(Collections.singletonList("f"))
                                                .build())
                                .tableCreateOptions(
                                        TableCreateOptions.builder()
                                                .clusteredFields(Collections.singletonList("f"))
                                                .build())
                                .build();

        assertThat(serializedForm(defaults)).doesNotContain("SerializedLambda");
        assertThat(serializedForm(configured)).doesNotContain("SerializedLambda");

        // Absence alone would also pass on a provider that came back empty, so read them back.
        BigQuerySinkConfig<String> restored = InstantiationUtil.clone(configured).getConfig();
        assertThat(
                        restored.getCdcTableOptionsProvider()
                                .optionsFor(DESTINATION)
                                .getPrimaryKeyColumns())
                .containsExactly("f");
        assertThat(
                        restored.getTableCreateOptionsProvider()
                                .optionsFor(DESTINATION)
                                .getClusteredFields())
                .containsExactly("f");
        assertThat(restored.getCdcOptions().getChangeTypeProvider().getChangeType("any"))
                .isEqualTo(CdcChangeType.UPSERT);

        BigQuerySinkConfig<String> restoredDefaults = InstantiationUtil.clone(defaults).getConfig();
        assertThat(restoredDefaults.getCdcTableOptionsProvider().optionsFor(DESTINATION))
                .isNotNull();
        assertThat(restoredDefaults.getTableCreateOptionsProvider().optionsFor(DESTINATION))
                .isNotNull();
    }

    @Test
    void theConfigsOwnFallbackProviderIsNamedToo() throws Exception {
        // The field is null only in a config restored from a job graph written before it existed,
        // which no builder can produce and which is exactly the path this guard is about: the
        // fallback the getter mints then travels on into the writers.
        BigQuerySinkConfig<String> config =
                new BigQuerySinkConfig<>(
                        new FixedDestinationResolver(DESTINATION),
                        new TestSerializer(),
                        null,
                        null,
                        CreateDisposition.CREATE_IF_NEEDED,
                        new FixedTableCreateOptionsProvider(TableCreateOptions.defaults()),
                        null,
                        CdcTableReconciliationPolicy.VERIFY_ONLY,
                        SchemaUpdateOptions.defaults(),
                        FailureHandler.failJob(),
                        "US",
                        null,
                        null,
                        null);

        CdcTableOptionsProvider fallback = config.getCdcTableOptionsProvider();

        assertThat(serializedForm(fallback)).doesNotContain("SerializedLambda");
        assertThat(fallback.optionsFor(DESTINATION)).isEqualTo(CdcTableOptions.defaults());
    }

    private static String serializedForm(Object value) throws Exception {
        return new String(InstantiationUtil.serializeObject(value), StandardCharsets.ISO_8859_1);
    }

    /** A named additional-field provider, so the guard above measures only what the sink mints. */
    private static final class NamedValueProvider implements AdditionalFieldValueProvider<String> {
        private static final long serialVersionUID = 1L;

        @Override
        public Object getValue(String element) {
            return element;
        }
    }

    /** The sequence-number half of the same, reaching the other write-only CDC field. */
    private static final class NamedSequenceProvider implements CdcSequenceNumberProvider<String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String getSequenceNumber(String element) {
            return "01";
        }
    }

    /** A trivial serializable test serializer. */
    private static class TestSerializer extends BigQueryProtoSerializer<Object> {
        private static final long serialVersionUID = 1L;

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return TableSchema.newBuilder()
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("f")
                                    .setType(TableFieldSchema.Type.INT64)
                                    .setMode(TableFieldSchema.Mode.NULLABLE)
                                    .build())
                    .build();
        }

        @Override
        public Descriptors.Descriptor getDescriptor(TableDestination destination) {
            return Empty.getDescriptor();
        }

        @Override
        public ByteString serialize(Object element) {
            return Empty.getDefaultInstance().toByteString();
        }
    }

    @Test
    void defaultWriteMethodIsStorageApiAtLeastOnce() {
        Sink<String> sink =
                BigQuerySink.<String>builder()
                        .table(DESTINATION)
                        .serializer(new TestSerializer())
                        .build();

        assertThat(sink).isInstanceOf(BigQueryDefaultStreamSink.class);
    }

    @Test
    void omittedAdditionalFieldsAreANoOp() throws Exception {
        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .table(DESTINATION)
                                .serializer(new TestSerializer())
                                .build();

        assertThat(sink.getConfig().getTableSchema(DESTINATION).getFieldsList())
                .extracting(TableFieldSchema::getName)
                .containsExactly("f");
        assertThat(sink.getConfig().getWriteDescriptor(DESTINATION))
                .isSameAs(Empty.getDescriptor());
        assertThat(sink.getConfig().serialize("value", DESTINATION))
                .isEqualTo(Empty.getDefaultInstance().toByteString());
    }

    @Test
    void additionalFieldsReachEveryWriteMethodAndSurviveSerialization() throws Exception {
        AdditionalFields<String> options =
                AdditionalFields.<String>builder()
                        .field(
                                AdditionalField.of(
                                        "computed",
                                        AdditionalFieldType.STRING,
                                        AdditionalFieldNullPolicy.REQUIRED,
                                        value -> value))
                        .build();
        BigQueryDefaultStreamSink<String> defaultStream =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .table(DESTINATION)
                                .serializer(new TestSerializer())
                                .additionalFields(options)
                                .build();
        BigQueryBufferedStreamSink<String> bufferedStream =
                (BigQueryBufferedStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .writeMethod(WriteMethod.STORAGE_API_EXACTLY_ONCE)
                                .table(DESTINATION)
                                .serializer(new TestSerializer())
                                .bufferedStreamOptions(BufferedStreamOptions.builder().build())
                                .additionalFields(options)
                                .build();
        BigQueryFileLoadsSink<String> fileLoads =
                (BigQueryFileLoadsSink<String>)
                        BigQuerySink.<String>builder()
                                .writeMethod(WriteMethod.FILE_LOADS)
                                .table(DESTINATION)
                                .serializer(new TestSerializer())
                                .fileLoadsOptions(
                                        FileLoadsOptions.builder()
                                                .stagingPath("gs://staging-bucket")
                                                .build())
                                .additionalFields(options)
                                .build();

        assertAdditionalField(InstantiationUtil.clone(defaultStream).getConfig());
        assertAdditionalField(InstantiationUtil.clone(bufferedStream).getConfig());
        assertAdditionalField(InstantiationUtil.clone(fileLoads).getConfig());
    }

    @Test
    void physicalAndWriteOnlyFieldsShareOneDescriptor() throws Exception {
        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .table(DESTINATION)
                                .serializer(new TestSerializer())
                                .additionalFields(
                                        AdditionalFields.<String>builder()
                                                .field(
                                                        AdditionalField.of(
                                                                "computed",
                                                                AdditionalFieldType.STRING,
                                                                AdditionalFieldNullPolicy.REQUIRED,
                                                                value -> value))
                                                .build())
                                .cdcOptions(
                                        CdcOptions.<String>builder(
                                                        CdcChangeTypeProvider.upsertOnly())
                                                .build())
                                .cdcTableOptions(
                                        CdcTableOptions.builder()
                                                .primaryKeyColumns(
                                                        java.util.Collections.singletonList("f"))
                                                .build())
                                .build();

        Descriptors.Descriptor descriptor = sink.getConfig().getWriteDescriptor(DESTINATION);
        DynamicMessage row =
                DynamicMessage.parseFrom(
                        descriptor, sink.getConfig().serialize("value", DESTINATION));

        assertThat(sink.getConfig().getTableSchema(DESTINATION).getFieldsList())
                .extracting(TableFieldSchema::getName)
                .containsExactly("f", "computed");
        assertThat(descriptor.getFields())
                .extracting(Descriptors.FieldDescriptor::getName)
                .containsExactly("computed", "_change_type");
        assertThat(row.getField(descriptor.findFieldByName("computed"))).isEqualTo("value");
        assertThat(row.getField(descriptor.findFieldByName("_change_type"))).isEqualTo("UPSERT");
    }

    @Test
    void additionalFieldsRejectNull() {
        assertThatThrownBy(() -> BigQuerySink.<String>builder().additionalFields(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("additionalFields must not be null");
    }

    private static void assertAdditionalField(BigQuerySinkConfig<String> config) throws Exception {
        assertThat(config.getTableSchema(DESTINATION).getFieldsList())
                .extracting(TableFieldSchema::getName)
                .containsExactly("f", "computed");
        Descriptors.Descriptor descriptor = config.getWriteDescriptor(DESTINATION);
        assertThat(descriptor.findFieldByName("computed")).isNotNull();
        DynamicMessage row =
                DynamicMessage.parseFrom(
                        descriptor, config.serialize("provider-value", DESTINATION));
        assertThat(row.getField(descriptor.findFieldByName("computed")))
                .isEqualTo("provider-value");
    }

    @Test
    void cdcOptionsReachTheDefaultStreamAndSurviveSerialization() throws Exception {
        CdcOptions<String> options =
                CdcOptions.<String>builder(CdcChangeTypeProvider.upsertOnly())
                        .sequenceNumberProvider(element -> "A")
                        .build();
        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .table(DESTINATION)
                                .serializer(new TestSerializer())
                                .createDisposition(CreateDisposition.CREATE_NEVER)
                                .cdcOptions(options)
                                .build();

        BigQueryDefaultStreamSink<String> copy = InstantiationUtil.clone(sink);

        assertThat(copy.getConfig().getCdcOptions()).isNotNull();
        assertThat(copy.getConfig().getCdcOptions().hasSequenceNumberProvider()).isTrue();
    }

    @Test
    void cdcOptionsRejectNullAndNonDefaultStreamWriteMethods() {
        assertThatThrownBy(() -> BigQuerySink.<String>builder().cdcOptions(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("cdcOptions must not be null");

        CdcOptions<String> options =
                CdcOptions.<String>builder(CdcChangeTypeProvider.upsertOnly()).build();
        assertThatThrownBy(
                        () ->
                                BigQuerySink.<String>builder()
                                        .writeMethod(WriteMethod.STORAGE_API_EXACTLY_ONCE)
                                        .table(DESTINATION)
                                        .serializer(new TestSerializer())
                                        .bufferedStreamOptions(
                                                BufferedStreamOptions.builder().build())
                                        .cdcOptions(options)
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "cdcOptions(...) is only valid for"
                                + " WriteMethod.STORAGE_API_AT_LEAST_ONCE");
        assertThatThrownBy(
                        () ->
                                BigQuerySink.<String>builder()
                                        .writeMethod(WriteMethod.FILE_LOADS)
                                        .table(DESTINATION)
                                        .serializer(new TestSerializer())
                                        .fileLoadsOptions(
                                                FileLoadsOptions.builder()
                                                        .stagingPath("gs://staging-bucket")
                                                        .build())
                                        .cdcOptions(options)
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "cdcOptions(...) is only valid for"
                                + " WriteMethod.STORAGE_API_AT_LEAST_ONCE");
    }

    @Test
    void cdcOptionsAllowCreateIfNeededByDefault() {
        CdcOptions<String> options =
                CdcOptions.<String>builder(CdcChangeTypeProvider.upsertOnly()).build();

        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .table(DESTINATION)
                                .serializer(new TestSerializer())
                                .cdcOptions(options)
                                .cdcTableOptions(
                                        CdcTableOptions.builder()
                                                .primaryKeyColumns(
                                                        java.util.Collections.singletonList("id"))
                                                .maxStaleness(Duration.ofMinutes(10))
                                                .build())
                                .build();

        assertThat(sink.getConfig().getCreateDisposition())
                .isEqualTo(CreateDisposition.CREATE_IF_NEEDED);
        assertThat(
                        sink.getConfig()
                                .getCdcTableOptionsProvider()
                                .optionsFor(DESTINATION)
                                .getPrimaryKeyColumns())
                .containsExactly("id");
    }

    @Test
    void fixedCdcAutoCreationDefersMissingTablePrimaryKeyValidationToRuntime() {
        CdcOptions<String> cdc =
                CdcOptions.<String>builder(CdcChangeTypeProvider.upsertOnly()).build();

        assertThat(
                        BigQuerySink.<String>builder()
                                .table(DESTINATION)
                                .serializer(new TestSerializer())
                                .cdcOptions(cdc)
                                .build())
                .isInstanceOf(BigQueryDefaultStreamSink.class);

        assertThat(
                        BigQuerySink.<String>builder()
                                .table(DESTINATION)
                                .serializer(new TestSerializer())
                                .cdcOptions(cdc)
                                .cdcTableOptionsProvider(
                                        destination ->
                                                CdcTableOptions.builder()
                                                        .primaryKeyColumns(
                                                                java.util.Collections.singletonList(
                                                                        "id"))
                                                        .build())
                                .build())
                .isInstanceOf(BigQueryDefaultStreamSink.class);
    }

    @Test
    void fixedMaximumStalenessRequiresCdcAtBuildTime() {
        assertThatThrownBy(
                        () ->
                                BigQuerySink.<String>builder()
                                        .table(DESTINATION)
                                        .serializer(new TestSerializer())
                                        .cdcTableOptions(
                                                CdcTableOptions.builder()
                                                        .maxStaleness(Duration.ofMinutes(10))
                                                        .build())
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("only valid with cdcOptions(...)");
    }

    @Test
    void reconcileRequiresAnAuthoritativePrimaryKey() {
        CdcOptions<String> cdc =
                CdcOptions.<String>builder(CdcChangeTypeProvider.upsertOnly()).build();

        assertThatThrownBy(
                        () ->
                                BigQuerySink.<String>builder()
                                        .table(DESTINATION)
                                        .serializer(new TestSerializer())
                                        .cdcOptions(cdc)
                                        .cdcTableReconciliationPolicy(
                                                CdcTableReconciliationPolicy.RECONCILE)
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("CdcTableOptions.primaryKeyColumns(...)");
    }

    @Test
    void serviceAccountKeyFileDefaultsToNull() {
        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .table(DESTINATION)
                                .serializer(new TestSerializer())
                                .build();

        assertThat(sink.getConfig().getServiceAccountKeyFile()).isNull();
    }

    @Test
    void serviceAccountKeyFileReachesEveryWriteMethodAndSurvivesSerialization() throws Exception {
        BigQueryDefaultStreamSink<String> defaultStream =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .table(DESTINATION)
                                .serializer(new TestSerializer())
                                .serviceAccountKeyFile(SERVICE_ACCOUNT_KEY_FILE)
                                .build();
        BigQueryBufferedStreamSink<String> bufferedStream =
                (BigQueryBufferedStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .writeMethod(WriteMethod.STORAGE_API_EXACTLY_ONCE)
                                .table(DESTINATION)
                                .serializer(new TestSerializer())
                                .bufferedStreamOptions(BufferedStreamOptions.builder().build())
                                .serviceAccountKeyFile(SERVICE_ACCOUNT_KEY_FILE)
                                .build();
        BigQueryFileLoadsSink<String> fileLoads =
                (BigQueryFileLoadsSink<String>)
                        BigQuerySink.<String>builder()
                                .writeMethod(WriteMethod.FILE_LOADS)
                                .table(DESTINATION)
                                .serializer(new TestSerializer())
                                .fileLoadsOptions(
                                        FileLoadsOptions.builder()
                                                .stagingPath("gs://staging-bucket")
                                                .build())
                                .serviceAccountKeyFile(SERVICE_ACCOUNT_KEY_FILE)
                                .build();

        assertThat(InstantiationUtil.clone(defaultStream).getConfig().getServiceAccountKeyFile())
                .isEqualTo(SERVICE_ACCOUNT_KEY_FILE);
        assertThat(InstantiationUtil.clone(bufferedStream).getConfig().getServiceAccountKeyFile())
                .isEqualTo(SERVICE_ACCOUNT_KEY_FILE);
        assertThat(InstantiationUtil.clone(fileLoads).getConfig().getServiceAccountKeyFile())
                .isEqualTo(SERVICE_ACCOUNT_KEY_FILE);
    }

    @Test
    void serviceAccountKeyFileRejectsNullAndBlankValues() {
        assertThatThrownBy(() -> BigQuerySink.<String>builder().serviceAccountKeyFile(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("serviceAccountKeyFile must not be null");
        assertThatThrownBy(() -> BigQuerySink.<String>builder().serviceAccountKeyFile(" \t"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("serviceAccountKeyFile must not be blank");
    }

    @Test
    void serviceAccountKeyFileCannotBeCombinedWithEitherEmulatorEndpoint() {
        assertThatThrownBy(
                        () ->
                                BigQuerySink.<String>builder()
                                        .table(DESTINATION)
                                        .serializer(new TestSerializer())
                                        .serviceAccountKeyFile(SERVICE_ACCOUNT_KEY_FILE)
                                        .emulatorEndpoint("localhost:9060")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("serviceAccountKeyFile(...)");
        assertThatThrownBy(
                        () ->
                                BigQuerySink.<String>builder()
                                        .table(DESTINATION)
                                        .serializer(new TestSerializer())
                                        .serviceAccountKeyFile(SERVICE_ACCOUNT_KEY_FILE)
                                        .emulatorRestEndpoint("localhost:9050")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("serviceAccountKeyFile(...)");
    }

    /** Unlike the other write-method option objects, the default-stream one is optional. */
    @Test
    void omittedDefaultStreamOptionsBuildWithDefaults() {
        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .table(DESTINATION)
                                .serializer(new TestSerializer())
                                .build();

        assertThat(sink.getOptions()).isEqualTo(DefaultStreamOptions.builder().build());
    }

    @Test
    void configuredDefaultStreamOptionsArePropagated() {
        DefaultStreamOptions options =
                DefaultStreamOptions.builder().maxInflightRequests(50).build();

        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .table(DESTINATION)
                                .serializer(new TestSerializer())
                                .defaultStreamOptions(options)
                                .build();

        assertThat(sink.getOptions()).isEqualTo(options);
    }

    @Test
    void rejectsDefaultStreamOptionsForOtherWriteMethods() {
        assertThatThrownBy(
                        () ->
                                BigQuerySink.<String>builder()
                                        .writeMethod(WriteMethod.STORAGE_API_EXACTLY_ONCE)
                                        .table(DESTINATION)
                                        .serializer(new TestSerializer())
                                        .bufferedStreamOptions(
                                                BufferedStreamOptions.builder().build())
                                        .defaultStreamOptions(
                                                DefaultStreamOptions.builder().build())
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("only valid for WriteMethod.STORAGE_API_AT_LEAST_ONCE");
    }

    @Test
    void defaultStreamSinkIsJavaSerializable() throws Exception {
        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .table(DESTINATION)
                                .serializer(new TestSerializer())
                                .defaultStreamOptions(
                                        DefaultStreamOptions.builder()
                                                .maxInflightRequests(50)
                                                .build())
                                .build();

        BigQueryDefaultStreamSink<String> copy = InstantiationUtil.clone(sink);

        assertThat(copy.getOptions()).isEqualTo(sink.getOptions());
        assertThat(copy.getConfig().getDestinationResolver().resolve("any", CONTEXT))
                .isEqualTo(DESTINATION);
    }

    @Test
    void buildsBufferedStreamSink() {
        BigQueryBufferedStreamSink<String> sink =
                (BigQueryBufferedStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .writeMethod(WriteMethod.STORAGE_API_EXACTLY_ONCE)
                                .table(DESTINATION)
                                .serializer(new TestSerializer())
                                .bufferedStreamOptions(BufferedStreamOptions.builder().build())
                                .build();

        assertThat(sink.getOptions()).isEqualTo(BufferedStreamOptions.builder().build());
        assertThat(sink.getConfig().getDestinationResolver().resolve("any", CONTEXT))
                .isEqualTo(DESTINATION);
    }

    @Test
    void bufferedStreamSinkIsJavaSerializable() throws Exception {
        BigQueryBufferedStreamSink<String> sink =
                (BigQueryBufferedStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .writeMethod(WriteMethod.STORAGE_API_EXACTLY_ONCE)
                                .table(DESTINATION)
                                .serializer(new TestSerializer())
                                .bufferedStreamOptions(BufferedStreamOptions.builder().build())
                                .build();

        BigQueryBufferedStreamSink<String> copy = InstantiationUtil.clone(sink);

        assertThat(copy.getOptions()).isEqualTo(sink.getOptions());
        assertThat(copy.getConfig().getDestinationResolver().resolve("any", CONTEXT))
                .isEqualTo(DESTINATION);
    }

    @Test
    void exactlyOnceRequiresBufferedStreamOptions() {
        assertThatThrownBy(
                        () ->
                                BigQuerySink.<String>builder()
                                        .writeMethod(WriteMethod.STORAGE_API_EXACTLY_ONCE)
                                        .table(DESTINATION)
                                        .serializer(new TestSerializer())
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("bufferedStreamOptions");
    }

    @Test
    void rejectsBufferedStreamOptionsForOtherWriteMethods() {
        assertThatThrownBy(
                        () ->
                                BigQuerySink.<String>builder()
                                        .table(DESTINATION)
                                        .serializer(new TestSerializer())
                                        .bufferedStreamOptions(
                                                BufferedStreamOptions.builder().build())
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("only valid for WriteMethod.STORAGE_API_EXACTLY_ONCE");
    }

    @Test
    void exactlyOnceAcceptsEnabledSchemaUpdateOptions() {
        Sink<String> sink =
                BigQuerySink.<String>builder()
                        .writeMethod(WriteMethod.STORAGE_API_EXACTLY_ONCE)
                        .table(DESTINATION)
                        .serializer(new TestSerializer())
                        .schemaUpdateOptions(SchemaUpdateOptions.builder().allowNewFields().build())
                        .bufferedStreamOptions(BufferedStreamOptions.builder().build())
                        .build();

        assertThat(sink).isInstanceOf(BigQueryBufferedStreamSink.class);
    }

    @Test
    void exactlyOnceAcceptsDisabledSchemaUpdateOptions() {
        // The default is disabled, so an explicitly-passed default must not break the build.
        Sink<String> sink =
                BigQuerySink.<String>builder()
                        .writeMethod(WriteMethod.STORAGE_API_EXACTLY_ONCE)
                        .table(DESTINATION)
                        .serializer(new TestSerializer())
                        .schemaUpdateOptions(SchemaUpdateOptions.defaults())
                        .bufferedStreamOptions(BufferedStreamOptions.builder().build())
                        .build();

        assertThat(sink).isInstanceOf(BigQueryBufferedStreamSink.class);
    }

    @Test
    void exactlyOnceAcceptsDynamicDestinations() {
        Sink<String> sink =
                BigQuerySink.<String>builder()
                        .writeMethod(WriteMethod.STORAGE_API_EXACTLY_ONCE)
                        .destinationResolver((element, context) -> DESTINATION)
                        .serializer(new TestSerializer())
                        .bufferedStreamOptions(BufferedStreamOptions.builder().build())
                        .build();

        assertThat(sink).isInstanceOf(BigQueryBufferedStreamSink.class);
    }

    @Test
    void exactlyOnceSinkWiresItsFixedDestinationIntoVersionOneStateMigration() throws Exception {
        BigQueryBufferedStreamSink<String> sink =
                (BigQueryBufferedStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .writeMethod(WriteMethod.STORAGE_API_EXACTLY_ONCE)
                                .table(DESTINATION)
                                .serializer(new TestSerializer())
                                .bufferedStreamOptions(BufferedStreamOptions.builder().build())
                                .build();
        String streamName = DESTINATION.toTablePath() + "/streams/legacy";
        byte[] versionOne;
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeUTF(streamName);
            out.writeLong(13L);
            out.writeLong(5L);
            versionOne = bytes.toByteArray();
        }

        assertThat(sink.getWriterStateSerializer().deserialize(1, versionOne))
                .isEqualTo(new BufferedStreamWriterState(DESTINATION, streamName, 13L, 5L));
    }

    @Test
    void buildsFileLoadsSink() {
        BigQueryFileLoadsSink<String> sink =
                (BigQueryFileLoadsSink<String>)
                        BigQuerySink.<String>builder()
                                .writeMethod(WriteMethod.FILE_LOADS)
                                .table(DESTINATION)
                                .serializer(new TestSerializer())
                                .fileLoadsOptions(
                                        FileLoadsOptions.builder()
                                                .stagingPath("gs://staging-bucket/prefix")
                                                .build())
                                .build();

        assertThat(sink.getOptions().getStagingPath()).isEqualTo("gs://staging-bucket/prefix");
        assertThat(sink.getConfig().getDestinationResolver().resolve("any", CONTEXT))
                .isEqualTo(DESTINATION);
    }

    @Test
    void fileLoadsRequiresFileLoadsOptions() {
        assertThatThrownBy(
                        () ->
                                BigQuerySink.<String>builder()
                                        .writeMethod(WriteMethod.FILE_LOADS)
                                        .table(DESTINATION)
                                        .serializer(new TestSerializer())
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("fileLoadsOptions");
    }

    @Test
    void rejectsFileLoadsOptionsForOtherWriteMethods() {
        assertThatThrownBy(
                        () ->
                                BigQuerySink.<String>builder()
                                        .table(DESTINATION)
                                        .serializer(new TestSerializer())
                                        .fileLoadsOptions(
                                                FileLoadsOptions.builder()
                                                        .stagingPath("gs://staging-bucket")
                                                        .build())
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("only valid for WriteMethod.FILE_LOADS");
    }

    @Test
    void fileLoadsSinkIsJavaSerializable() throws Exception {
        BigQueryFileLoadsSink<String> sink =
                (BigQueryFileLoadsSink<String>)
                        BigQuerySink.<String>builder()
                                .writeMethod(WriteMethod.FILE_LOADS)
                                .table(DESTINATION)
                                .serializer(new TestSerializer())
                                .fileLoadsOptions(
                                        FileLoadsOptions.builder()
                                                .stagingPath("gs://staging-bucket")
                                                .build())
                                .build();

        BigQueryFileLoadsSink<String> copy = InstantiationUtil.clone(sink);

        assertThat(copy.getOptions()).isEqualTo(sink.getOptions());
        assertThat(copy.getConfig().getDestinationResolver().resolve("any", CONTEXT))
                .isEqualTo(DESTINATION);
    }

    @Test
    void fixedTableUsesNamedResolver() {
        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .table(DESTINATION)
                                .serializer(new TestSerializer())
                                .build();

        DestinationResolver<? super String> resolver = sink.getConfig().getDestinationResolver();
        assertThat(resolver).isInstanceOf(FixedDestinationResolver.class);
        assertThat(((FixedDestinationResolver) resolver).getDestination()).isEqualTo(DESTINATION);
        assertThat(resolver.resolve("any", CONTEXT)).isEqualTo(DESTINATION);
    }

    @Test
    void acceptsDestinationResolver() {
        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .destinationResolver(
                                        (element, context) ->
                                                TableDestination.of(
                                                        "my-project",
                                                        "my_dataset",
                                                        String.valueOf(element)))
                                .serializer(new TestSerializer())
                                .build();

        assertThat(sink.getConfig().getDestinationResolver().resolve("events", CONTEXT))
                .isEqualTo(TableDestination.of("my-project", "my_dataset", "events"));
    }

    @Test
    void lastTableOrDestinationResolverCallWins() {
        BigQueryDefaultStreamSink<String> resolverWins =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .table(DESTINATION)
                                .destinationResolver(
                                        (element, context) ->
                                                TableDestination.of("p", "d", "dynamic"))
                                .serializer(new TestSerializer())
                                .build();
        assertThat(resolverWins.getConfig().getDestinationResolver().resolve("x", CONTEXT))
                .isEqualTo(TableDestination.of("p", "d", "dynamic"));

        BigQueryDefaultStreamSink<String> tableWins =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .destinationResolver(
                                        (element, context) ->
                                                TableDestination.of("p", "d", "dynamic"))
                                .table(DESTINATION)
                                .serializer(new TestSerializer())
                                .build();
        assertThat(tableWins.getConfig().getDestinationResolver())
                .isInstanceOf(FixedDestinationResolver.class);
    }

    @Test
    void rejectsNullTable() {
        assertThatThrownBy(() -> BigQuerySink.<String>builder().table(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("table must not be null");
    }

    @Test
    void acceptsContravariantResolverAndSerializer() {
        DestinationResolver<Object> resolverForAnyType = (element, context) -> DESTINATION;

        Sink<String> sink =
                BigQuerySink.<String>builder()
                        .destinationResolver(resolverForAnyType)
                        .serializer(new TestSerializer()) // BigQueryProtoSerializer<Object>
                        .build();

        assertThat(sink).isInstanceOf(BigQueryDefaultStreamSink.class);
    }

    @Test
    void propagatesConfigurationDefaultsAndOverrides() {
        BigQueryDefaultStreamSink<String> defaults =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .table(DESTINATION)
                                .serializer(new TestSerializer())
                                .build();
        assertThat(defaults.getConfig().getCreateDisposition())
                .isEqualTo(CreateDisposition.CREATE_IF_NEEDED);
        assertThat(defaults.getConfig().getLocation()).isNull();
        assertThat(defaults.getConfig().getFailureHandler()).isEqualTo(FailureHandler.failJob());
        assertThat(defaults.getConfig().getEmulatorEndpoint()).isNull();
        assertThat(defaults.getConfig().getEmulatorRestEndpoint()).isNull();

        BigQueryDefaultStreamSink<String> overridden =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .table(DESTINATION)
                                .serializer(new TestSerializer())
                                .createDisposition(CreateDisposition.CREATE_NEVER)
                                .failureHandler(FailureHandler.logAndDrop())
                                .location("asia-northeast1")
                                .build();
        assertThat(overridden.getConfig().getCreateDisposition())
                .isEqualTo(CreateDisposition.CREATE_NEVER);
        assertThat(overridden.getConfig().getLocation()).isEqualTo("asia-northeast1");
        assertThat(overridden.getConfig().getFailureHandler())
                .isEqualTo(FailureHandler.logAndDrop());
    }

    @Test
    void carriesBothEmulatorEndpointsIntoTheSinkAndThroughSerialization() throws Exception {
        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .table(DESTINATION)
                                .serializer(new TestSerializer())
                                .emulatorEndpoint("localhost:9060")
                                .emulatorRestEndpoint("localhost:9050")
                                .build();

        // Read back off the built sink, and again off a round-tripped copy: the endpoints are
        // invisible from outside a running writer, so nothing else would notice one being dropped
        // on the way to the config or failing to travel with the job graph.
        BigQueryDefaultStreamSink<String> copy = InstantiationUtil.clone(sink);
        assertThat(copy.getConfig().getEmulatorEndpoint().getTarget()).isEqualTo("localhost:9060");
        assertThat(copy.getConfig().getEmulatorRestEndpoint().getTarget())
                .isEqualTo("localhost:9050");
    }

    @Test
    void carriesTheEmulatorEndpointIntoTheBufferedStreamServiceFactory() {
        BigQueryBufferedStreamSink<String> sink =
                (BigQueryBufferedStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .writeMethod(WriteMethod.STORAGE_API_EXACTLY_ONCE)
                                .table(DESTINATION)
                                .serializer(new TestSerializer())
                                .bufferedStreamOptions(BufferedStreamOptions.builder().build())
                                .emulatorEndpoint("localhost:9060")
                                .build();

        assertThat(
                        ((WriteClientBufferedStreamServiceFactory) sink.getServiceFactory())
                                .getEmulatorEndpoint()
                                .getTarget())
                .isEqualTo("localhost:9060");
    }

    @Test
    void rejectsAMalformedEmulatorEndpointWhereItIsWritten() {
        // Parsed in the setter, not at build(): a typo is a client-side error, not a connection
        // failure once the job has been deployed (#235). The message names the setter that was
        // called: this is the connector with two of them, and naming the wrong one sends a user
        // to a value that may be perfectly good (#895).
        assertThatThrownBy(() -> BigQuerySink.builder().emulatorEndpoint("localhost"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("emulatorEndpoint must be host:port, was 'localhost'");
        assertThatThrownBy(() -> BigQuerySink.builder().emulatorRestEndpoint("localhost:0"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("emulatorRestEndpoint must be host:port, was 'localhost:0'");
        assertThatThrownBy(() -> BigQuerySink.builder().emulatorRestEndpoint(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("emulatorRestEndpoint must not be null");
    }

    @Test
    void fileLoadsRejectsEitherEmulatorEndpoint() {
        assertThatThrownBy(() -> fileLoadsBuilder().emulatorEndpoint("localhost:9060").build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("emulatorEndpoint(...)")
                .hasMessageContaining("Cloud Storage");
        assertThatThrownBy(() -> fileLoadsBuilder().emulatorRestEndpoint("localhost:9050").build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("emulatorRestEndpoint(...)");
    }

    private static BigQuerySinkBuilder<String> fileLoadsBuilder() {
        return BigQuerySink.<String>builder()
                .writeMethod(WriteMethod.FILE_LOADS)
                .table(DESTINATION)
                .serializer(new TestSerializer())
                .fileLoadsOptions(
                        FileLoadsOptions.builder().stagingPath("gs://bucket/tmp").build());
    }

    @Test
    void rejectsNullFailureHandler() {
        assertThatThrownBy(() -> BigQuerySink.<String>builder().failureHandler(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("failureHandler");
    }

    @Test
    void failsWithoutSerializer() {
        assertThatThrownBy(() -> BigQuerySink.<String>builder().table(DESTINATION).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("serializer");
    }

    @Test
    void failsWithoutDestination() {
        assertThatThrownBy(
                        () ->
                                BigQuerySink.<String>builder()
                                        .serializer(new TestSerializer())
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("table(...)")
                .hasMessageContaining("destinationResolver(...)");
    }

    @Test
    void sinkWithFixedDestinationIsJavaSerializable() throws Exception {
        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .table(DESTINATION)
                                .serializer(new TestSerializer())
                                .build();

        BigQueryDefaultStreamSink<String> copy = InstantiationUtil.clone(sink);

        assertThat(copy.getConfig().getDestinationResolver().resolve("events", CONTEXT))
                .isEqualTo(DESTINATION);
    }

    @Test
    void sinkWithResolverLambdaIsJavaSerializable() throws Exception {
        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .destinationResolver(
                                        (element, context) ->
                                                TableDestination.of(
                                                        "my-project",
                                                        "my_dataset",
                                                        String.valueOf(element)))
                                .serializer(new TestSerializer())
                                .build();

        BigQueryDefaultStreamSink<String> copy = InstantiationUtil.clone(sink);

        assertThat(copy.getConfig().getDestinationResolver().resolve("events", CONTEXT))
                .isEqualTo(TableDestination.of("my-project", "my_dataset", "events"));
    }
}
