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
import com.google.cloud.NoCredentials;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.v1.DatastoreSettings;
import io.github.flink.gcp.connector.base.rpc.EmulatorChannels;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;

/**
 * Builds the Datastore client every direction of the Datastore-mode connector opens.
 *
 * <p><b>The host is always set</b>, to the emulator endpoint or to the service's own, so the client
 * library never takes its endpoint from {@code DATASTORE_EMULATOR_HOST}: this connector reaches an
 * emulator only through the builder's {@code emulatorEndpoint(...)} (ADR-0064). An emulator is
 * reached over plaintext with no credentials: the library switches to a plaintext channel when the
 * credentials are {@link NoCredentials}, whatever the host.
 *
 * <p><b>The sink's client makes exactly one attempt per call.</b> The library applies the client's
 * retry settings both around each call and to every generated call setting, so a single-attempt
 * setting turns off both layers, and the timeout given here bounds each call; the sink owns its
 * retry loop. The source's reads are idempotent at a fixed read time, so it passes no timeout and
 * keeps the library's retry settings instead.
 *
 * <p><b>The table lookup reads through the generated client</b> ({@link
 * #lookupSettings(EmulatorEndpoint, Credentials)}), the one whose {@code Lookup} returns a future:
 * the library's {@code Datastore} and its RPC object only block. The generated client keeps its own
 * retry settings for {@code Lookup}, and never reads {@code DATASTORE_EMULATOR_HOST} either.
 */
@Internal
public final class DatastoreClients {

    private DatastoreClients() {}

    /**
     * Builds client settings.
     *
     * @param database the database the client will reach
     * @param emulatorEndpoint the emulator to reach, or {@code null} for the real service
     * @param credentialsOverride credentials loaded by the runtime component, or {@code null} for
     *     ADC
     * @param callTimeout the bound on each call, which then makes exactly one attempt; or {@code
     *     null} to keep the library's retry settings
     * @return the settings
     */
    public static DatastoreOptions settings(
            DatabaseDestination database,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            @Nullable Credentials credentialsOverride,
            @Nullable Duration callTimeout) {
        Preconditions.checkArgument(
                emulatorEndpoint == null || credentialsOverride == null,
                "credentialsOverride cannot be combined with an emulator endpoint");
        DatastoreOptions.Builder settings =
                DatastoreOptions.newBuilder()
                        .setProjectId(database.getProject())
                        .setDatabaseId(database.getDatabaseId());
        if (callTimeout != null) {
            settings.setRetrySettings(singleAttempt(callTimeout));
        }
        if (emulatorEndpoint != null) {
            settings.setHost(emulatorEndpoint.getTarget())
                    .setCredentials(NoCredentials.getInstance());
        } else {
            settings.setHost(DatastoreSettings.getDefaultEndpoint());
            if (credentialsOverride != null) {
                settings.setCredentials(credentialsOverride);
            }
        }
        return settings.build();
    }

    /**
     * Opens a client from settings already assembled by the owning runtime component.
     *
     * @param database the database to reach, for the failure message
     * @param settings the settings to open the client from
     * @return the client, which the caller owns and must close
     * @throws IOException if the client cannot be created
     */
    public static Datastore open(DatabaseDestination database, DatastoreOptions settings)
            throws IOException {
        try {
            return settings.getService();
        } catch (RuntimeException e) {
            throw new IOException("Failed to create the Datastore client for " + database + ".", e);
        }
    }

    /**
     * Builds the generated client's settings for the table lookup: the service's endpoint with the
     * given or application-default credentials, or an emulator over plaintext without credentials.
     * The library's retry settings for {@code Lookup} stay.
     *
     * @param emulatorEndpoint the emulator to reach, or {@code null} for the real service
     * @param credentialsOverride credentials loaded by the runtime component, or {@code null} for
     *     ADC
     * @return the settings
     * @throws IOException if the settings cannot be built
     */
    public static DatastoreSettings lookupSettings(
            @Nullable EmulatorEndpoint emulatorEndpoint, @Nullable Credentials credentialsOverride)
            throws IOException {
        Preconditions.checkArgument(
                emulatorEndpoint == null || credentialsOverride == null,
                "credentialsOverride cannot be combined with an emulator endpoint");
        DatastoreSettings.Builder settings = DatastoreSettings.newBuilder();
        if (emulatorEndpoint != null) {
            settings.setCredentialsProvider(NoCredentialsProvider.create())
                    .setTransportChannelProvider(
                            EmulatorChannels.plaintextProvider(
                                    DatastoreSettings.defaultGrpcTransportProviderBuilder(),
                                    emulatorEndpoint));
        } else if (credentialsOverride != null) {
            settings.setCredentialsProvider(FixedCredentialsProvider.create(credentialsOverride));
        }
        return settings.build();
    }

    /** One attempt, bounded by the timeout; the multipliers are irrelevant past one attempt. */
    private static RetrySettings singleAttempt(Duration callTimeout) {
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
