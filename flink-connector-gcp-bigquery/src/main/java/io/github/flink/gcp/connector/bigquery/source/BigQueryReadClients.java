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

package io.github.flink.gcp.connector.bigquery.source;

import org.apache.flink.annotation.Internal;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.grpc.ManagedChannelBuilder;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Storage Read API clients, for the two places the source opens one: the enumerator creates the
 * read session, and every reader opens its assigned streams.
 *
 * <p>{@code public} because those two live in sibling packages and Java has no
 * package-tree-internal access — the same reason the connector's metric-name inventory is public.
 */
@Internal
public final class BigQueryReadClients {

    private BigQueryReadClients() {}

    /**
     * Creates a client for the given endpoint: the emulator form when one is set, and the SDK's own
     * application-default-credentials form otherwise.
     *
     * @param emulatorEndpoint the emulator's gRPC endpoint, or {@code null} for BigQuery itself
     * @return the client; the caller owns it and must close it
     * @throws IOException if the client cannot be created
     */
    public static BigQueryReadClient create(@Nullable EmulatorEndpoint emulatorEndpoint)
            throws IOException {
        return emulatorEndpoint == null
                ? BigQueryReadClient.create()
                : forEmulator(emulatorEndpoint);
    }

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
    private static BigQueryReadClient forEmulator(EmulatorEndpoint endpoint) throws IOException {
        String target = endpoint.getTarget();
        return BigQueryReadClient.create(
                BigQueryReadSettings.newBuilder()
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
