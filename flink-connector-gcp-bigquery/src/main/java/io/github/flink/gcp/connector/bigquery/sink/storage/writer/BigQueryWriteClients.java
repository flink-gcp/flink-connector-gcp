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
import org.apache.flink.annotation.VisibleForTesting;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import io.github.flink.gcp.connector.base.rpc.EmulatorChannels;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigquery.BigQueryCredentials;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Storage Write API clients, shared by the two write paths that open one.
 *
 * <p>The production form optionally replaces ADC with runtime-loaded service-account credentials;
 * the emulator form always uses no credentials.
 */
@Internal
final class BigQueryWriteClients {

    private BigQueryWriteClients() {}

    static BigQueryWriteClient forProduction(@Nullable String serviceAccountKeyFile)
            throws IOException {
        if (serviceAccountKeyFile == null) {
            return BigQueryWriteClient.create();
        }
        return BigQueryWriteClient.create(productionSettings(serviceAccountKeyFile));
    }

    /** Builds production settings carrying the configured service-account credentials. */
    @VisibleForTesting
    static BigQueryWriteSettings productionSettings(String serviceAccountKeyFile)
            throws IOException {
        GoogleCredentials credentials = BigQueryCredentials.load(serviceAccountKeyFile);
        return BigQueryWriteSettings.newBuilder()
                .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                .build();
    }

    /**
     * Creates a client talking plaintext to a BigQuery emulator with no credentials.
     *
     * @param endpoint the emulator's gRPC endpoint
     * @return the client; the caller owns it and must close it
     * @throws IOException if the client cannot be created
     */
    static BigQueryWriteClient forEmulator(EmulatorEndpoint endpoint) throws IOException {
        return BigQueryWriteClient.create(emulatorSettings(endpoint));
    }

    /**
     * Builds the settings behind {@link #forEmulator}.
     *
     * <p>The transport provider starts from {@link
     * BigQueryWriteSettings#defaultGrpcTransportProviderBuilder()} rather than from a bare one so
     * that the API's own defaults survive: it raises the maximum inbound message size to {@link
     * Integer#MAX_VALUE}, and a provider built from scratch would run the emulator path at gRPC's 4
     * MiB default instead. Nothing sets the endpoint on the settings as well: the provider carries
     * it, and gax pushes the settings' endpoint onto a provider only when the provider has none.
     *
     * @param endpoint the emulator's gRPC endpoint
     * @return the settings
     * @throws IOException if the settings cannot be built
     */
    @VisibleForTesting
    static BigQueryWriteSettings emulatorSettings(EmulatorEndpoint endpoint) throws IOException {
        return BigQueryWriteSettings.newBuilder()
                .setCredentialsProvider(NoCredentialsProvider.create())
                .setTransportChannelProvider(
                        EmulatorChannels.plaintextProvider(
                                BigQueryWriteSettings.defaultGrpcTransportProviderBuilder(),
                                endpoint))
                .build();
    }
}
