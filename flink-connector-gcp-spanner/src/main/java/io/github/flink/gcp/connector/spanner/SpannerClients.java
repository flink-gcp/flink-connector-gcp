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

package io.github.flink.gcp.connector.spanner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.auth.Credentials;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Builds the Spanner service handle both directions of this connector open.
 *
 * <p>One helper rather than one copy per direction, because the emulator-versus-credentials branch
 * is exactly the code that reads green on a developer machine and red in CI: a handle built without
 * the emulator host reaches for application default credentials, which a workstation has and a
 * build agent does not.
 *
 * <p>Nothing else about the connection is configured. Spanner's own {@code setEmulatorHost}
 * switches the channel to plaintext and the credentials to none in one call, so this connector
 * never goes through the shared plaintext-channel helpers the other connectors need.
 */
@Internal
public final class SpannerClients {

    private SpannerClients() {}

    /**
     * Builds the client settings for a database.
     *
     * <p>Separate from {@link #open} so that the mapping is testable without opening a channel.
     *
     * @param database the database the handle will reach
     * @param emulatorEndpoint the emulator to reach, or {@code null} for the real service
     * @return the settings
     */
    public static SpannerOptions settings(
            SpannerDatabase database, @Nullable EmulatorEndpoint emulatorEndpoint) {
        return settings(database, emulatorEndpoint, null);
    }

    /**
     * Builds client settings with an optional runtime-loaded credential override.
     *
     * @param database the database the handle will reach
     * @param emulatorEndpoint the emulator to reach, or {@code null} for the real service
     * @param credentialsOverride credentials loaded by the runtime component, or {@code null} for
     *     ADC
     * @return the settings
     */
    public static SpannerOptions settings(
            SpannerDatabase database,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            @Nullable Credentials credentialsOverride) {
        Preconditions.checkArgument(
                emulatorEndpoint == null || credentialsOverride == null,
                "credentialsOverride cannot be combined with an emulator endpoint");
        SpannerOptions.Builder settings =
                SpannerOptions.newBuilder().setProjectId(database.getProject());
        if (emulatorEndpoint != null) {
            settings.setEmulatorHost(emulatorEndpoint.getTarget());
        } else if (credentialsOverride != null) {
            settings.setCredentials(credentialsOverride);
        }
        return settings.build();
    }

    /**
     * Opens a service handle for a database.
     *
     * @param database the database to reach
     * @param emulatorEndpoint the emulator to reach, or {@code null} for the real service
     * @return the handle, which the caller owns and must close
     * @throws IOException if the handle cannot be created
     */
    public static Spanner open(
            SpannerDatabase database, @Nullable EmulatorEndpoint emulatorEndpoint)
            throws IOException {
        return open(database, emulatorEndpoint, null);
    }

    /** Opens a service handle with an optional runtime-loaded credential override. */
    public static Spanner open(
            SpannerDatabase database,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            @Nullable Credentials credentialsOverride)
            throws IOException {
        return open(database, settings(database, emulatorEndpoint, credentialsOverride));
    }

    /** Opens a handle from settings already assembled by the owning runtime component. */
    public static Spanner open(SpannerDatabase database, SpannerOptions settings)
            throws IOException {
        try {
            return settings.getService();
        } catch (RuntimeException e) {
            throw new IOException("Failed to create the Spanner client for " + database + ".", e);
        }
    }
}
