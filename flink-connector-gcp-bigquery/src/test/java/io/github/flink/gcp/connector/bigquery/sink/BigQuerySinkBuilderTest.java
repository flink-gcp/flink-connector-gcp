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
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BigQuerySinkBuilder}. */
class BigQuerySinkBuilderTest {

    private static final TableDestination DESTINATION =
            TableDestination.of("my-project", "my_dataset", "my_table");

    private static final SinkWriter.Context CONTEXT =
            new SinkWriter.Context() {
                @Override
                public long currentWatermark() {
                    return 0;
                }

                @Override
                public Long timestamp() {
                    return null;
                }
            };

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

    @Test
    void rejectsUnimplementedWriteMethods() {
        assertThatThrownBy(
                        () ->
                                BigQuerySink.<String>builder()
                                        .writeMethod(WriteMethod.STORAGE_API_EXACTLY_ONCE)
                                        .destination(DESTINATION)
                                        .serializer(new TestSerializer())
                                        .build())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("#30");
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
        assertThat(defaults.getConfig().getFailedRowHandler())
                .isEqualTo(FailedRowHandler.failJob());

        BigQueryDefaultStreamSink<String> overridden =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .destination(DESTINATION)
                                .serializer(new TestSerializer())
                                .createDisposition(CreateDisposition.CREATE_NEVER)
                                .failedRowHandler(FailedRowHandler.logAndDrop())
                                .location("asia-northeast1")
                                .build();
        assertThat(overridden.getConfig().getCreateDisposition())
                .isEqualTo(CreateDisposition.CREATE_NEVER);
        assertThat(overridden.getConfig().getLocation()).isEqualTo("asia-northeast1");
        assertThat(overridden.getConfig().getFailedRowHandler())
                .isEqualTo(FailedRowHandler.logAndDrop());
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
