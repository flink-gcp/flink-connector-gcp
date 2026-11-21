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
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.bigquery.BigQueryCredentials;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Storage Write API clients, shared by the two write paths that open one.
 *
 * <p>The production form optionally replaces ADC with runtime-loaded service-account credentials;
 * the emulator form always uses no credentials. Both open the client with {@link
 * GrpcEventLoops#open(GrpcEventLoops.ClientFactory)}, so its channels run on the connector's own
 * event-loop group and closing the returned holder releases that group after the client.
 */
@Internal
final class BigQueryWriteClients {

    private BigQueryWriteClients() {}

    /**
     * Opens a client against the production service, with its channels on the connector's
     * event-loop group.
     *
     * @param serviceAccountKeyFile the service-account key-file path, or {@code null} for ADC
     * @return the client with its lease; the caller owns it and must close it
     * @throws IOException if the client cannot be created
     */
    static GrpcEventLoops.LeasedClient<BigQueryWriteClient> forProduction(
            @Nullable String serviceAccountKeyFile) throws IOException {
        return GrpcEventLoops.open(
                lease ->
                        BigQueryWriteClient.create(
                                productionSettings(serviceAccountKeyFile, lease)));
    }

    /**
     * Builds the production settings: the configured service-account credentials when a key file is
     * set, application-default credentials otherwise, and the lease's event-loop group.
     */
    @VisibleForTesting
    static BigQueryWriteSettings productionSettings(
            @Nullable String serviceAccountKeyFile, GrpcEventLoops.Lease lease) throws IOException {
        BigQueryWriteSettings.Builder settings = BigQueryWriteSettings.newBuilder();
        if (serviceAccountKeyFile != null) {
            GoogleCredentials credentials = BigQueryCredentials.load(serviceAccountKeyFile);
            settings.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
        }
        return lease.applyTo(settings).build();
    }

    /**
     * Opens a client talking plaintext to a BigQuery emulator with no credentials, with its
     * channels on the connector's event-loop group.
     *
     * @param endpoint the emulator's gRPC endpoint
     * @return the client with its lease; the caller owns it and must close it
     * @throws IOException if the client cannot be created
     */
    static GrpcEventLoops.LeasedClient<BigQueryWriteClient> forEmulator(EmulatorEndpoint endpoint)
            throws IOException {
        return GrpcEventLoops.open(
                lease ->
                        BigQueryWriteClient.create(
                                lease.applyTo(emulatorSettings(endpoint).toBuilder()).build()));
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
