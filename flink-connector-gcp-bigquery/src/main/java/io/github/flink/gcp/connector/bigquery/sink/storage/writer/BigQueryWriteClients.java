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

package io.github.flink.gcp.connector.bigquery.sink.storage.writer;

import org.apache.flink.annotation.Internal;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.grpc.ManagedChannelBuilder;

import java.io.IOException;

/**
 * Storage Write API clients, shared by the two write paths that open one.
 *
 * <p>Only the emulator form lives here: the production default-stream path opens no client at all
 * (its {@code StreamWriter}s come from the SDK's JVM-static connection pool) and the production
 * buffered path uses the SDK's own {@code BigQueryWriteClient.create()}.
 */
@Internal
final class BigQueryWriteClients {

    private BigQueryWriteClients() {}

    /**
     * Creates a client talking plaintext to a BigQuery emulator with no credentials.
     *
     * <p>The endpoint is set on the settings <em>and</em> on an explicit channel provider: the
     * settings' endpoint alone would still build a TLS channel, which no emulator here terminates.
     *
     * @param endpoint the emulator's gRPC endpoint
     * @return the client; the caller owns it and must close it
     * @throws IOException if the client cannot be created
     */
    static BigQueryWriteClient forEmulator(EmulatorEndpoint endpoint) throws IOException {
        String target = endpoint.getTarget();
        return BigQueryWriteClient.create(
                BigQueryWriteSettings.newBuilder()
                        .setEndpoint(target)
                        .setCredentialsProvider(NoCredentialsProvider.create())
                        .setTransportChannelProvider(
                                InstantiatingGrpcChannelProvider.newBuilder()
                                        .setEndpoint(target)
                                        .setChannelConfigurator(ManagedChannelBuilder::usePlaintext)
                                        .build())
                        .build());
    }
}
