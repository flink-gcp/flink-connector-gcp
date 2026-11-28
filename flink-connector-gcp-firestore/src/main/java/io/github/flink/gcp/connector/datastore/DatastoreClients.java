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

package io.github.flink.gcp.connector.datastore;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.datastore.v1.DatastoreClient;
import com.google.cloud.datastore.v1.DatastoreSettings;
import io.github.flink.gcp.connector.base.rpc.EmulatorChannels;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;

/**
 * Opens the Datastore client every direction of the Datastore-mode connector uses: the generated
 * {@code v1.DatastoreClient}, on the connector's gRPC event-loop group (ADR-0208).
 *
 * <p><b>The generated client, not the library's {@code Datastore}.</b> On an emulator, {@code
 * Datastore} builds a plaintext channel of its own and ignores any transport it is given, whenever
 * its credentials are {@code NoCredentials} or its host is a loopback address ({@code
 * GrpcDatastoreRpc} and {@code DatastoreUtils.isEmulator}, google-cloud-datastore 3.11.0), so that
 * channel ran on gRPC's shared event-loop group; its host check also could not read a {@code
 * dns:///} target. Its close waits up to six minutes for its channel without cancelling what it
 * carries. The generated client is a gax {@code BackgroundResource}: {@link #open} opens it with a
 * lease, and closing the holder bounds its close. Its calls return futures, which a JobManager-side
 * owner cancels when it closes.
 *
 * <p><b>The endpoint is always chosen here</b>: the generated client's default endpoint, or an
 * emulator's caller-owned channel ({@link EmulatorChannels#openLeasedClient}), built from the
 * endpoint's gRPC target, a resolver scheme such as {@code dns:///} included and an {@code http://}
 * or {@code https://} one removed ({@link EmulatorEndpoint#withoutHttpScheme()}), with the API's
 * channel defaults, such as its unbounded inbound message size. The client never reads {@code
 * DATASTORE_EMULATOR_HOST}: this connector reaches an emulator only through the builder's {@code
 * emulatorEndpoint(...)} (ADR-0064), over plaintext with no credentials.
 *
 * <p><b>Retry settings are each caller's.</b> The sink's client makes exactly one attempt per call,
 * bounded by its request timeout ({@link #singleAttempt(Duration)}): the sink owns its retry loop.
 * The source's reads are idempotent at a fixed read time, so they keep the settings the library's
 * {@code Datastore} applied to every call ({@link #libraryRetrySettings()}). The table lookup keeps
 * the generated client's own settings for {@code Lookup}.
 */
@Internal
public final class DatastoreClients {

    private DatastoreClients() {}

    /**
     * Builds a client's settings, without its transport, which {@link #open} chooses.
     *
     * @param emulatorEndpoint the emulator to reach, or {@code null} for the real service
     * @param credentialsOverride credentials loaded by the runtime component, or {@code null} for
     *     ADC
     * @param retrySettings the retry settings every unary call takes, or {@code null} to keep the
     *     generated client's own for each method
     * @return the settings builder
     */
    public static DatastoreSettings.Builder settings(
            @Nullable EmulatorEndpoint emulatorEndpoint,
            @Nullable Credentials credentialsOverride,
            @Nullable RetrySettings retrySettings) {
        Preconditions.checkArgument(
                emulatorEndpoint == null || credentialsOverride == null,
                "credentialsOverride cannot be combined with an emulator endpoint");
        DatastoreSettings.Builder settings = DatastoreSettings.newBuilder();
        if (emulatorEndpoint != null) {
            settings.setCredentialsProvider(NoCredentialsProvider.create());
        } else if (credentialsOverride != null) {
            settings.setCredentialsProvider(
                    FixedCredentialsProvider.create(withJwtAccess(credentialsOverride)));
        }
        if (retrySettings != null) {
            settings.applyToAllUnaryMethods(
                    method -> {
                        method.setRetrySettings(retrySettings);
                        return null;
                    });
        }
        return settings;
    }

    /**
     * Returns service-account credentials that sign their own scoped JWT rather than exchange an
     * assertion for an access token, as the library's {@code Datastore} sent them ({@code
     * ServiceOptions.getScopedCredentials()} with its default {@code useJwtAccessWithScope},
     * google-cloud-core 2.81.0), so these clients reach Datastore without reaching the OAuth token
     * endpoint (the planner's HTTP splitter does not use them); other credentials as they are.
     */
    private static Credentials withJwtAccess(Credentials credentials) {
        if (credentials instanceof ServiceAccountCredentials) {
            return ((ServiceAccountCredentials) credentials).createWithUseJwtAccessWithScope(true);
        }
        return credentials;
    }

    /**
     * Opens a client on the connector's gRPC event-loop group: the service through the settings'
     * own transport provider, or an emulator through a plaintext channel the returned holder owns.
     * Closing the holder closes the client within {@link GrpcEventLoops.LeasedClient}'s bounds,
     * then any emulator channel.
     *
     * @param database the database the client reaches, for the failure message
     * @param emulatorEndpoint the emulator to reach, or {@code null} for the real service; the same
     *     one the settings were built for
     * @param settings the settings from {@link #settings(EmulatorEndpoint, Credentials,
     *     RetrySettings)}
     * @return the client with its lease, which the caller owns and must close
     * @throws IOException if the client cannot be created
     * @throws IllegalArgumentException if an emulator is given with settings that carry
     *     credentials, which a plaintext channel would send in the clear
     */
    public static GrpcEventLoops.LeasedClient<DatastoreClient> open(
            DatabaseDestination database,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            DatastoreSettings.Builder settings)
            throws IOException {
        Preconditions.checkNotNull(database, "database must not be null");
        Preconditions.checkNotNull(settings, "settings must not be null");
        Preconditions.checkArgument(
                emulatorEndpoint == null
                        || settings.getCredentialsProvider() instanceof NoCredentialsProvider,
                "settings for an emulator must carry no credentials; build them with the same"
                        + " emulator endpoint");
        try {
            if (emulatorEndpoint == null) {
                return GrpcEventLoops.open(
                        lease -> DatastoreClient.create(lease.applyTo(settings).build()));
            }
            return EmulatorChannels.openLeasedClient(
                    DatastoreSettings.defaultGrpcTransportProviderBuilder(),
                    emulatorEndpoint.withoutHttpScheme(),
                    channel ->
                            DatastoreClient.create(
                                    settings.setTransportChannelProvider(channel).build()));
        } catch (IOException | RuntimeException e) {
            throw new IOException("Failed to create the Datastore client for " + database + ".", e);
        }
    }

    /**
     * Returns the retry settings the library's {@code Datastore} gave every call by default ({@code
     * ServiceOptions.getDefaultRetrySettings()}, which {@code GrpcDatastoreRpc} applied to each
     * unary method), for the source's reads.
     *
     * @return the settings
     */
    public static RetrySettings libraryRetrySettings() {
        return ServiceOptions.getDefaultRetrySettings();
    }

    /**
     * Returns retry settings that make exactly one attempt, bounded by the timeout; the multipliers
     * are irrelevant past one attempt.
     *
     * @param callTimeout the bound on each call
     * @return the settings
     */
    public static RetrySettings singleAttempt(Duration callTimeout) {
        Preconditions.checkNotNull(callTimeout, "callTimeout must not be null");
        return RetrySettings.newBuilder()
                .setMaxAttempts(1)
                .setTotalTimeoutDuration(callTimeout)
                .setInitialRpcTimeoutDuration(callTimeout)
                .setMaxRpcTimeoutDuration(callTimeout)
                .setRpcTimeoutMultiplier(1.0)
                .setInitialRetryDelayDuration(Duration.ZERO)
                .setMaxRetryDelayDuration(Duration.ZERO)
                .setRetryDelayMultiplier(1.0)
                .build();
    }
}
