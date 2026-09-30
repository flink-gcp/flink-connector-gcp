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

package io.github.flink.gcp.connector.firestore;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.Credentials;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.function.UnaryOperator;

/**
 * Builds the Firestore client every direction of this connector opens.
 *
 * <p>Firestore's own {@code setEmulatorHost} switches the channel to plaintext and the credentials
 * to the emulator's fixed token in one call, so this connector never goes through the shared
 * plaintext-channel helpers the other connectors need — the Spanner shape.
 *
 * <p><b>The client library also reads {@value #EMULATOR_HOST_VARIABLE} on its own</b>, whenever no
 * emulator host was set explicitly, and then overrides both the channel and any configured
 * credentials with the emulator's. This connector reaches an emulator only through the builder's
 * {@code emulatorEndpoint(...)} (ADR-0064) and cannot switch the library's lookup off, so a job
 * started in an environment that carries the variable would write to whatever it names. {@link
 * #settings(DatabaseDestination, EmulatorEndpoint, Credentials, RetrySettings)} warns when that
 * happens rather than letting it pass silently.
 */
@Internal
public final class FirestoreClients {

    private static final Logger LOG = LoggerFactory.getLogger(FirestoreClients.class);

    /** The environment variable the client library reads an emulator host from. */
    @VisibleForTesting static final String EMULATOR_HOST_VARIABLE = "FIRESTORE_EMULATOR_HOST";

    private FirestoreClients() {}

    /**
     * Builds client settings with an optional runtime-loaded credential override.
     *
     * @param database the database the client will reach
     * @param emulatorEndpoint the emulator to reach, or {@code null} for the real service
     * @param credentialsOverride credentials loaded by the runtime component, or {@code null} for
     *     ADC
     * @param retrySettings the transport's retry settings, or {@code null} for the library's
     * @return the settings
     */
    public static FirestoreOptions settings(
            DatabaseDestination database,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            @Nullable Credentials credentialsOverride,
            @Nullable RetrySettings retrySettings) {
        return settings(
                database, emulatorEndpoint, credentialsOverride, retrySettings, System::getenv);
    }

    @VisibleForTesting
    static FirestoreOptions settings(
            DatabaseDestination database,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            @Nullable Credentials credentialsOverride,
            @Nullable RetrySettings retrySettings,
            UnaryOperator<String> environment) {
        Preconditions.checkArgument(
                emulatorEndpoint == null || credentialsOverride == null,
                "credentialsOverride cannot be combined with an emulator endpoint");
        FirestoreOptions.Builder settings =
                FirestoreOptions.newBuilder()
                        .setProjectId(database.getProject())
                        .setDatabaseId(database.getDatabaseId());
        if (emulatorEndpoint != null) {
            settings.setEmulatorHost(emulatorEndpoint.getTarget());
        } else {
            String inherited = environment.apply(EMULATOR_HOST_VARIABLE);
            if (inherited != null) {
                LOG.warn(
                        "{} is set to '{}' in this process's environment and no emulatorEndpoint"
                                + " is configured. The Firestore client library reads that"
                                + " variable itself and will send every request for {} to it,"
                                + " over plaintext with a placeholder token instead of the job's"
                                + " credentials, rather than to the Firestore service."
                                + " Unset the variable, or configure emulatorEndpoint(...) if the"
                                + " emulator is intended.",
                        EMULATOR_HOST_VARIABLE,
                        inherited,
                        database);
            }
            if (credentialsOverride != null) {
                settings.setCredentials(credentialsOverride);
            }
        }
        if (retrySettings != null) {
            settings.setRetrySettings(retrySettings);
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
    public static Firestore open(DatabaseDestination database, FirestoreOptions settings)
            throws IOException {
        try {
            return settings.getService();
        } catch (RuntimeException e) {
            throw new IOException("Failed to create the Firestore client for " + database + ".", e);
        }
    }
}
