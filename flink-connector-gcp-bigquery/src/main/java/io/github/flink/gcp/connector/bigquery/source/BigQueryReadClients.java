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

package io.github.flink.gcp.connector.bigquery.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.rpc.ServerStreamingCallSettings;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import io.github.flink.gcp.connector.base.rpc.EmulatorChannels;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.bigquery.BigQueryCredentials;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Storage Read API clients, for the two places the source opens one: the enumerator creates the
 * read session, and every reader opens its assigned streams.
 *
 * <p>Two factory methods rather than one, because only the reading side has anything to configure:
 * the {@code ReadRows} retry budget is inert on a client that only creates sessions, and a caller
 * that had to pass it would be inventing a value. Naming them after the call each serves is what
 * keeps the wrong one from being picked.
 *
 * <p>Either way the client's channels run on the connector's own event-loop group. The production
 * clients are opened with {@link GrpcEventLoops#open(GrpcEventLoops.ClientFactory)}; an emulator
 * client with {@link EmulatorChannels#openLeasedClient}, whose holder also closes the emulator
 * channel after the client.
 *
 * <p>{@code public} because those two live in sibling packages and Java has no
 * package-tree-internal access — the same reason the connector's metric-name inventory is public.
 */
@Internal
public final class BigQueryReadClients {

    private BigQueryReadClients() {}

    /**
     * Creates the client a read session is created with: the enumerator's, and a {@code bigquery}
     * table's full lookup cache load's.
     *
     * <p>{@code CreateReadSession}'s own retry settings are left as the SDK ships them — a
     * ten-minute budget over {@code DEADLINE_EXCEEDED} and {@code UNAVAILABLE}. The enumerator
     * makes that call when it plans, on a coordinator worker thread, and a full lookup cache load
     * makes it on the loading TaskManager. Either way it comes before any of the session's rows is
     * read, so a failure there surfaces before the first fetch rather than inside one.
     *
     * @param serviceAccountKeyFile the service-account key-file path, or {@code null} for ADC
     * @param emulatorEndpoint the emulator's gRPC endpoint, or {@code null} for BigQuery itself
     * @return the client with the event-loop lease covering it, and on an emulator also the channel
     *     it runs on; the caller owns it and must close it
     * @throws IOException if the client cannot be created
     */
    public static GrpcEventLoops.LeasedClient<BigQueryReadClient> createForSessions(
            @Nullable String serviceAccountKeyFile, @Nullable EmulatorEndpoint emulatorEndpoint)
            throws IOException {
        if (emulatorEndpoint != null) {
            return EmulatorChannels.openLeasedClient(
                    BigQueryReadSettings.defaultGrpcTransportProviderBuilder(),
                    emulatorEndpoint,
                    channel -> BigQueryReadClient.create(emulatorSettingsBuilder(channel).build()));
        }
        return GrpcEventLoops.open(
                lease -> BigQueryReadClient.create(sessionSettings(serviceAccountKeyFile, lease)));
    }

    /**
     * Builds the production session client's settings, with its channels on the lease's event-loop
     * group.
     */
    @VisibleForTesting
    static BigQueryReadSettings sessionSettings(
            @Nullable String serviceAccountKeyFile, GrpcEventLoops.Lease lease) throws IOException {
        return lease.applyTo(productionSettingsBuilder(serviceAccountKeyFile)).build();
    }

    /**
     * Creates the client a reader opens its assigned streams with.
     *
     * @param serviceAccountKeyFile the service-account key-file path, or {@code null} for ADC
     * @param emulatorEndpoint the emulator's gRPC endpoint, or {@code null} for BigQuery itself
     * @param retryMaxAttempts the bound put on the client's own {@code ReadRows} retry
     * @param onRetry run once per retried attempt, or {@code null} to observe none
     * @return the client with the event-loop lease covering it, and on an emulator also the channel
     *     it runs on; the caller owns it and must close it
     * @throws IOException if the client cannot be created
     */
    public static GrpcEventLoops.LeasedClient<BigQueryReadClient> createForReads(
            @Nullable String serviceAccountKeyFile,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            int retryMaxAttempts,
            @Nullable Runnable onRetry)
            throws IOException {
        if (emulatorEndpoint != null) {
            return EmulatorChannels.openLeasedClient(
                    BigQueryReadSettings.defaultGrpcTransportProviderBuilder(),
                    emulatorEndpoint,
                    channel ->
                            BigQueryReadClient.create(
                                    boundReadRetry(
                                            emulatorSettingsBuilder(channel),
                                            retryMaxAttempts,
                                            onRetry)));
        }
        return GrpcEventLoops.open(
                lease ->
                        BigQueryReadClient.create(
                                readSettings(
                                        serviceAccountKeyFile, retryMaxAttempts, onRetry, lease)));
    }

    /**
     * Builds the production reading client's settings, with its channels on the lease's event-loop
     * group and its own {@code ReadRows} retry bounded as {@link
     * #boundReadRetry(BigQueryReadSettings.Builder, int, Runnable)} describes.
     *
     * <p>Separate from client creation so a test can read what the settings carry. Building the
     * settings resolves an explicitly configured service-account key, while the ADC case remains
     * lazy until client creation.
     *
     * @param serviceAccountKeyFile the service-account key-file path, or {@code null} for ADC
     * @param retryMaxAttempts the bound put on the client's own {@code ReadRows} retry
     * @param onRetry run once per retried attempt, or {@code null} to observe none
     * @param lease the event-loop lease the client's channels run under
     * @return the settings
     * @throws IOException if the settings cannot be built
     */
    @VisibleForTesting
    static BigQueryReadSettings readSettings(
            @Nullable String serviceAccountKeyFile,
            int retryMaxAttempts,
            @Nullable Runnable onRetry,
            GrpcEventLoops.Lease lease)
            throws IOException {
        // The lease is applied to this builder, never to a toBuilder() of the built settings:
        // BigQueryReadSettings.toBuilder() copies the stub settings only and drops the retry
        // listener boundReadRetry sets (google-cloud-bigquerystorage 3.38.0).
        return boundReadRetry(
                lease.applyTo(productionSettingsBuilder(serviceAccountKeyFile)),
                retryMaxAttempts,
                onRetry);
    }

    /**
     * Bounds a reading client's own {@code ReadRows} retry and leaves everything else about it
     * alone, then builds the settings.
     *
     * <p>Left alone deliberately: the client resumes a broken {@code ReadRows} at {@code
     * originalOffset + rowsProcessed} through its own {@code ReadRowsResumptionStrategy}, and
     * classifies which failures earn a resume. What it does not do is stop — {@code maxAttempts} is
     * unset and the total budget is twenty-four hours, so a stream that will never come back holds
     * a split fetcher for a day while reporting nothing at all. That is the whole of what this
     * method changes ({@code docs/adr/0084}).
     *
     * <p>{@code maxAttempts} and not {@code totalTimeout}, and the two are not interchangeable: gax
     * resets the attempt count whenever an attempt produced a response, while carrying the first
     * attempt's start time forward. So {@code maxAttempts} counts <em>consecutive failures without
     * progress</em>, which is the thing worth bounding, and {@code totalTimeout} runs from the
     * moment the stream was opened — shortening it would cut off the retry of a stream that has
     * been healthy for hours.
     *
     * <p>The retry listener is the only report a retry makes. Its argument is a {@code Runnable}
     * rather than the SDK's own listener type so that nothing outside this class has to name a
     * status and a metadata map it does not read: the counter behind it says how much work the
     * client's retry is doing, which is what an operator cannot otherwise see — a stream that keeps
     * failing and resuming makes progress, so it never trips {@code maxAttempts} and never reports
     * anything else.
     *
     * @param settings the settings to bound, with their transport already chosen
     * @param retryMaxAttempts the bound put on the client's own {@code ReadRows} retry
     * @param onRetry run once per retried attempt, or {@code null} to observe none
     * @return the settings
     * @throws IOException if the settings cannot be built
     */
    private static BigQueryReadSettings boundReadRetry(
            BigQueryReadSettings.Builder settings, int retryMaxAttempts, @Nullable Runnable onRetry)
            throws IOException {
        ServerStreamingCallSettings.Builder<ReadRowsRequest, ReadRowsResponse> readRows =
                settings.readRowsSettings();
        readRows.setRetrySettings(
                readRows.getRetrySettings().toBuilder().setMaxAttempts(retryMaxAttempts).build());
        if (onRetry != null) {
            settings.setReadRowsRetryAttemptListener((status, metadata) -> onRetry.run());
        }
        return settings.build();
    }

    /**
     * Builds production settings: the configured service-account form when a key file is set, and
     * the SDK's own application-default-credentials form otherwise.
     *
     * @param serviceAccountKeyFile the service-account key-file path, or {@code null} for ADC
     * @return the settings builder
     * @throws IOException if the key file cannot be read
     */
    private static BigQueryReadSettings.Builder productionSettingsBuilder(
            @Nullable String serviceAccountKeyFile) throws IOException {
        BigQueryReadSettings.Builder settings = BigQueryReadSettings.newBuilder();
        if (serviceAccountKeyFile != null) {
            settings.setCredentialsProvider(
                    FixedCredentialsProvider.create(
                            BigQueryCredentials.load(serviceAccountKeyFile)));
        }
        return settings;
    }

    /**
     * Builds the settings for a client talking plaintext to a BigQuery emulator with no
     * credentials, on a channel the caller opened and closes.
     *
     * <p>That channel is built from {@link
     * BigQueryReadSettings#defaultGrpcTransportProviderBuilder()} rather than a bare provider
     * builder so that the API's own defaults survive: it raises the maximum inbound message size to
     * {@link Integer#MAX_VALUE}, and a channel built without it would run the emulator path at
     * gRPC's 4 MiB default instead — a {@code RESOURCE_EXHAUSTED} waiting for the first read batch
     * above it.
     *
     * <p>The channel is opened by {@link EmulatorChannels} rather than by gax from an instantiating
     * provider, because gax parses the target as {@code host:port}, so a {@code dns:///} endpoint
     * fails, and builds TLS credentials when a client certificate is configured, which a plaintext
     * configurator cannot override ({@code docs/adr/0208}).
     *
     * @param channel the fixed provider around the emulator channel
     * @return the settings builder
     */
    private static BigQueryReadSettings.Builder emulatorSettingsBuilder(
            TransportChannelProvider channel) {
        return BigQueryReadSettings.newBuilder()
                .setCredentialsProvider(NoCredentialsProvider.create())
                .setTransportChannelProvider(channel);
    }
}
