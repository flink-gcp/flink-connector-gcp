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

package io.github.flink.gcp.connector.bigquery.sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Empty;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.BigQueryFileLoadsSink;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer;
import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryBufferedStreamSink;
import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryDefaultStreamSink;
import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamOptions;
import io.github.flink.gcp.connector.bigquery.sink.storage.DefaultStreamOptions;
import io.github.flink.gcp.connector.bigquery.sink.storage.writer.WriteClientBufferedStreamServiceFactory;
import io.github.flink.gcp.connector.testutils.TestContexts;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BigQuerySinkBuilder}. */
class BigQuerySinkBuilderTest {

    private static final TableDestination DESTINATION =
            TableDestination.of("my-project", "my_dataset", "my_table");

    private static final SinkWriter.Context CONTEXT = TestContexts.NO_OP;

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
                        .destination(DESTINATION)
                        .serializer(new TestSerializer())
                        .build();

        assertThat(sink).isInstanceOf(BigQueryDefaultStreamSink.class);
    }

    /** Unlike the other write-method option objects, the default-stream one is optional. */
    @Test
    void omittedDefaultStreamOptionsBuildWithDefaults() {
        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .destination(DESTINATION)
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
                                .destination(DESTINATION)
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
                                        .destination(DESTINATION)
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
                                .destination(DESTINATION)
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
                                .destination(DESTINATION)
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
                                .destination(DESTINATION)
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
                                        .destination(DESTINATION)
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
                                        .destination(DESTINATION)
                                        .serializer(new TestSerializer())
                                        .bufferedStreamOptions(
                                                BufferedStreamOptions.builder().build())
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("only valid for WriteMethod.STORAGE_API_EXACTLY_ONCE");
    }

    @Test
    void exactlyOnceRejectsEnabledSchemaUpdateOptions() {
        // The buffered stream's schema is pinned at creation, so the writer never consults
        // schemaUpdateOptions — accepting them silently would promise evolution that never runs.
        assertThatThrownBy(
                        () ->
                                BigQuerySink.<String>builder()
                                        .writeMethod(WriteMethod.STORAGE_API_EXACTLY_ONCE)
                                        .destination(DESTINATION)
                                        .serializer(new TestSerializer())
                                        .schemaUpdateOptions(
                                                SchemaUpdateOptions.builder()
                                                        .allowNewFields()
                                                        .build())
                                        .bufferedStreamOptions(
                                                BufferedStreamOptions.builder().build())
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("schemaUpdateOptions");
    }

    @Test
    void exactlyOnceAcceptsDisabledSchemaUpdateOptions() {
        // The default is disabled, so an explicitly-passed default must not break the build.
        Sink<String> sink =
                BigQuerySink.<String>builder()
                        .writeMethod(WriteMethod.STORAGE_API_EXACTLY_ONCE)
                        .destination(DESTINATION)
                        .serializer(new TestSerializer())
                        .schemaUpdateOptions(SchemaUpdateOptions.defaults())
                        .bufferedStreamOptions(BufferedStreamOptions.builder().build())
                        .build();

        assertThat(sink).isInstanceOf(BigQueryBufferedStreamSink.class);
    }

    @Test
    void exactlyOnceRequiresAFixedDestination() {
        assertThatThrownBy(
                        () ->
                                BigQuerySink.<String>builder()
                                        .writeMethod(WriteMethod.STORAGE_API_EXACTLY_ONCE)
                                        .destinationResolver((element, context) -> DESTINATION)
                                        .serializer(new TestSerializer())
                                        .bufferedStreamOptions(
                                                BufferedStreamOptions.builder().build())
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("fixed destination");
    }

    @Test
    void buildsFileLoadsSink() {
        BigQueryFileLoadsSink<String> sink =
                (BigQueryFileLoadsSink<String>)
                        BigQuerySink.<String>builder()
                                .writeMethod(WriteMethod.FILE_LOADS)
                                .destination(DESTINATION)
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
                                        .destination(DESTINATION)
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
                                        .destination(DESTINATION)
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
                                .destination(DESTINATION)
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
    void fixedDestinationUsesNamedResolver() {
        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .destination(DESTINATION)
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
    void lastDestinationCallWins() {
        BigQueryDefaultStreamSink<String> resolverWins =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .destination(DESTINATION)
                                .destinationResolver(
                                        (element, context) ->
                                                TableDestination.of("p", "d", "dynamic"))
                                .serializer(new TestSerializer())
                                .build();
        assertThat(resolverWins.getConfig().getDestinationResolver().resolve("x", CONTEXT))
                .isEqualTo(TableDestination.of("p", "d", "dynamic"));

        BigQueryDefaultStreamSink<String> destinationWins =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .destinationResolver(
                                        (element, context) ->
                                                TableDestination.of("p", "d", "dynamic"))
                                .destination(DESTINATION)
                                .serializer(new TestSerializer())
                                .build();
        assertThat(destinationWins.getConfig().getDestinationResolver())
                .isInstanceOf(FixedDestinationResolver.class);
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
                                .destination(DESTINATION)
                                .serializer(new TestSerializer())
                                .build();
        assertThat(defaults.getConfig().getCreateDisposition())
                .isEqualTo(CreateDisposition.CREATE_IF_NEEDED);
        assertThat(defaults.getConfig().getLocation()).isNull();
        assertThat(defaults.getConfig().getFailedRowHandler()).isEqualTo(FailureHandler.failJob());
        assertThat(defaults.getConfig().getEmulatorEndpoint()).isNull();
        assertThat(defaults.getConfig().getEmulatorRestEndpoint()).isNull();

        BigQueryDefaultStreamSink<String> overridden =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .destination(DESTINATION)
                                .serializer(new TestSerializer())
                                .createDisposition(CreateDisposition.CREATE_NEVER)
                                .failedRowHandler(FailureHandler.logAndDrop())
                                .location("asia-northeast1")
                                .build();
        assertThat(overridden.getConfig().getCreateDisposition())
                .isEqualTo(CreateDisposition.CREATE_NEVER);
        assertThat(overridden.getConfig().getLocation()).isEqualTo("asia-northeast1");
        assertThat(overridden.getConfig().getFailedRowHandler())
                .isEqualTo(FailureHandler.logAndDrop());
    }

    @Test
    void carriesBothEmulatorEndpointsIntoTheSinkAndThroughSerialization() throws Exception {
        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .destination(DESTINATION)
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
                                .destination(DESTINATION)
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
        // failure once the job has been deployed (#235).
        assertThatThrownBy(() -> BigQuerySink.builder().emulatorEndpoint("localhost"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("localhost");
        assertThatThrownBy(() -> BigQuerySink.builder().emulatorRestEndpoint("localhost:0"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("localhost:0");
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
                .destination(DESTINATION)
                .serializer(new TestSerializer())
                .fileLoadsOptions(
                        FileLoadsOptions.builder().stagingPath("gs://bucket/tmp").build());
    }

    @Test
    void rejectsNullFailedRowHandler() {
        assertThatThrownBy(() -> BigQuerySink.<String>builder().failedRowHandler(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("failedRowHandler");
    }

    @Test
    void failsWithoutSerializer() {
        assertThatThrownBy(() -> BigQuerySink.<String>builder().destination(DESTINATION).build())
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
                .hasMessageContaining("destination");
    }

    @Test
    void sinkWithFixedDestinationIsJavaSerializable() throws Exception {
        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .destination(DESTINATION)
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
