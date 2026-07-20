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

package io.github.flink.gcp.connector.bigquery.sink.storageapi.writer;

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.GetWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.ProtoSchemaConverter;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.protobuf.Descriptors;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.grpc.ManagedChannelBuilder;

import java.io.IOException;

/**
 * Appender factory opening plaintext Storage Write API connections to the BigQuery emulator
 * (goccy/bigquery-emulator), shared by the integration tests.
 *
 * <p>Emulator-specific deviations from {@link StreamWriterRowAppenderFactory} (both tracked by
 * goccy/bigquery-emulator#342; the upstream fix is merged but unreleased as of 0.8.1):
 *
 * <ul>
 *   <li>the emulator only registers a table's default stream when {@code GetWriteStream} is called
 *       with the {@code .../streams/_default} name form, and {@code AppendRows} matches that exact
 *       name — so the stream is primed here and the writer uses that name form
 *   <li>a missing table surfaces from {@code GetWriteStream} as {@code UNKNOWN} instead of {@code
 *       NOT_FOUND}; translated so the create-disposition handling reacts to it
 * </ul>
 */
class EmulatorAppenderFactory implements RowAppenderFactory {
    private static final long serialVersionUID = 1L;

    private final String grpcEndpoint;

    EmulatorAppenderFactory(String grpcEndpoint) {
        this.grpcEndpoint = grpcEndpoint;
    }

    @Override
    public RowAppender create(
            TableDestination destination, Descriptors.Descriptor rowDescriptor, String location)
            throws IOException {
        String streamName = destination.toTablePath() + "/streams/_default";
        BigQueryWriteClient client =
                BigQueryWriteClient.create(
                        BigQueryWriteSettings.newBuilder()
                                .setEndpoint(grpcEndpoint)
                                .setCredentialsProvider(NoCredentialsProvider.create())
                                .setTransportChannelProvider(
                                        InstantiatingGrpcChannelProvider.newBuilder()
                                                .setEndpoint(grpcEndpoint)
                                                .setChannelConfigurator(
                                                        ManagedChannelBuilder::usePlaintext)
                                                .build())
                                .build());
        StreamWriter streamWriter;
        try {
            client.getWriteStream(GetWriteStreamRequest.newBuilder().setName(streamName).build());
            streamWriter =
                    StreamWriter.newBuilder(streamName, client)
                            .setWriterSchema(ProtoSchemaConverter.convert(rowDescriptor))
                            .build();
        } catch (ApiException e) {
            client.close();
            throw new NotFoundException(e, GrpcStatusCode.of(io.grpc.Status.Code.NOT_FOUND), false);
        } catch (IOException | RuntimeException e) {
            client.close();
            throw e;
        }
        return new RowAppender() {
            @Override
            public ApiFuture<AppendRowsResponse> append(ProtoRows rows) {
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
