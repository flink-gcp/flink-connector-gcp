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

package io.github.flink.gcp.connector.bigquery.sink.writer;

import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.GetWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.ProtoSchemaConverter;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.github.flink.gcp.connector.bigquery.sink.BigQueryDefaultStreamSink;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for table auto-creation against the BigQuery emulator
 * (goccy/bigquery-emulator): the at-least-once writer writing to a table that does not exist,
 * end-to-end through the Storage Write API gRPC endpoint and the REST table-creation path.
 */
@Testcontainers
@Timeout(180)
class BigQueryTableAutoCreationITCase {

    private static final String PROJECT = "it-project";
    private static final String DATASET = "it_dataset";
    private static final int REST_PORT = 9050;
    private static final int GRPC_PORT = 9060;

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

    @Container
    private static final GenericContainer<?> EMULATOR =
            new GenericContainer<>("ghcr.io/goccy/bigquery-emulator:0.8.1")
                    .withCommand("--project=" + PROJECT, "--dataset=" + DATASET)
                    .withExposedPorts(REST_PORT, GRPC_PORT)
                    .waitingFor(Wait.forListeningPorts(REST_PORT, GRPC_PORT));

    private static BigQuery restClient;

    @BeforeAll
    static void createRestClient() {
        restClient =
                BigQueryOptions.newBuilder()
                        .setHost(
                                "http://"
                                        + EMULATOR.getHost()
                                        + ":"
                                        + EMULATOR.getMappedPort(REST_PORT))
                        .setProjectId(PROJECT)
                        .setCredentials(NoCredentials.getInstance())
                        .build()
                        .getService();
    }

    private static String grpcEndpoint() {
        return EMULATOR.getHost() + ":" + EMULATOR.getMappedPort(GRPC_PORT);
    }

    /** Serializer with a fixed one-column schema, writing rows via {@link DynamicMessage}. */
    private static final class NameSerializer extends BigQueryProtoSerializer<String> {
        private static final long serialVersionUID = 1L;

        private transient Descriptors.Descriptor descriptor;

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return TableSchema.newBuilder()
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("name")
                                    .setType(TableFieldSchema.Type.STRING)
                                    .setMode(TableFieldSchema.Mode.NULLABLE)
                                    .build())
                    .build();
        }

        @Override
        public Descriptors.Descriptor getDescriptor(TableDestination destination) {
            if (descriptor == null) {
                descriptor = super.getDescriptor(destination);
            }
            return descriptor;
        }

        @Override
        public ByteString serialize(String element) {
            Descriptors.Descriptor d = getDescriptor(null);
            return DynamicMessage.newBuilder(d)
                    .setField(d.findFieldByName("name"), element)
                    .build()
                    .toByteString();
        }
    }

    /**
     * Appender factory opening plaintext Storage Write API connections to the emulator.
     *
     * <p>Emulator-specific deviations from {@link StreamWriterRowAppenderFactory} (both tracked by
     * goccy/bigquery-emulator#342; the upstream fix is merged but unreleased as of 0.8.1):
     *
     * <ul>
     *   <li>the emulator only registers a table's default stream when {@code GetWriteStream} is
     *       called with the {@code .../streams/_default} name form, and {@code AppendRows} matches
     *       that exact name — so the stream is primed here and the writer uses that name form
     *   <li>a missing table surfaces from {@code GetWriteStream} as {@code UNKNOWN} instead of
     *       {@code NOT_FOUND}; translated so the create-disposition handling reacts to it
     * </ul>
     */
    private static final class EmulatorAppenderFactory implements RowAppenderFactory {
        private static final long serialVersionUID = 1L;

        @Override
        public RowAppender create(
                TableDestination destination, Descriptors.Descriptor rowDescriptor, String location)
                throws IOException {
            String streamName = destination.toTablePath() + "/streams/_default";
            BigQueryWriteClient client =
                    BigQueryWriteClient.create(
                            BigQueryWriteSettings.newBuilder()
                                    .setEndpoint(grpcEndpoint())
                                    .setCredentialsProvider(NoCredentialsProvider.create())
                                    .setTransportChannelProvider(
                                            InstantiatingGrpcChannelProvider.newBuilder()
                                                    .setEndpoint(grpcEndpoint())
                                                    .setChannelConfigurator(
                                                            ManagedChannelBuilder::usePlaintext)
                                                    .build())
                                    .build());
            StreamWriter streamWriter;
            try {
                client.getWriteStream(
                        GetWriteStreamRequest.newBuilder().setName(streamName).build());
                streamWriter =
                        StreamWriter.newBuilder(streamName, client)
                                .setWriterSchema(ProtoSchemaConverter.convert(rowDescriptor))
                                .build();
            } catch (ApiException e) {
                client.close();
                throw new NotFoundException(
                        e, GrpcStatusCode.of(io.grpc.Status.Code.NOT_FOUND), false);
            } catch (IOException | RuntimeException e) {
                client.close();
                throw e;
            }
            return new RowAppender() {
                @Override
                public com.google.api.core.ApiFuture<
                                com.google.cloud.bigquery.storage.v1.AppendRowsResponse>
                        append(com.google.cloud.bigquery.storage.v1.ProtoRows rows) {
                    return streamWriter.append(rows);
                }

                @Override
                public void close() {
                    streamWriter.close();
                    client.close();
                }
            };
        }
    }

    private static BigQueryDefaultStreamWriter<String> writer(
            TableDestination destination, CreateDisposition disposition) {
        BigQuerySinkConfig<String> config =
                ((BigQueryDefaultStreamSink<String>)
                                BigQuerySink.<String>builder()
                                        .destination(destination)
                                        .serializer(new NameSerializer())
                                        .createDisposition(disposition)
                                        .build())
                        .getConfig();
        return new BigQueryDefaultStreamWriter<>(
                config,
                new EmulatorAppenderFactory(),
                new BigQueryTableCreator(restClient),
                BigQueryDefaultStreamWriter.DEFAULT_MAX_APPEND_REQUEST_BYTES,
                100,
                1_000,
                30);
    }

    @Test
    void createIfNeededCreatesMissingTableAndWritesEndToEnd() throws Exception {
        TableDestination destination = TableDestination.of(PROJECT, DATASET, "auto_created");
        BigQueryDefaultStreamWriter<String> writer =
                writer(destination, CreateDisposition.CREATE_IF_NEEDED);
        try {
            writer.write("alice", CONTEXT);
            writer.write("bob", CONTEXT);
            writer.flush(false);
        } finally {
            writer.close();
        }

        assertThat(restClient.getTable(TableId.of(DATASET, "auto_created"))).isNotNull();
        List<String> names = new ArrayList<>();
        restClient
                .query(
                        QueryJobConfiguration.newBuilder(
                                        "SELECT name FROM `"
                                                + PROJECT
                                                + "."
                                                + DATASET
                                                + ".auto_created` ORDER BY name")
                                .build())
                .iterateAll()
                .forEach(row -> names.add(row.get(0).getStringValue()));
        assertThat(names).containsExactly("alice", "bob");
    }

    @Test
    void createNeverFailsFastOnMissingTable() throws Exception {
        TableDestination destination = TableDestination.of(PROJECT, DATASET, "never_created");
        BigQueryDefaultStreamWriter<String> writer =
                writer(destination, CreateDisposition.CREATE_NEVER);
        try {
            assertThatThrownBy(
                            () -> {
                                writer.write("alice", CONTEXT);
                                writer.flush(false);
                            })
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("CREATE_NEVER");
        } finally {
            writer.close();
        }

        assertThat(restClient.getTable(TableId.of(DATASET, "never_created"))).isNull();
    }
}
