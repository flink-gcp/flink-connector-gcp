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

package io.github.flink.gcp.connector.bigtable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;

import javax.annotation.Nullable;

/**
 * How this connector points a {@code BigtableDataClient} at an instance: production over
 * application-default credentials, production over a runtime-loaded service-account provider, or an
 * emulator over a plaintext channel with no credentials.
 *
 * <p>At the module root rather than beside either direction, because every data-client boundary of
 * this connector takes it — the sink, both sources and the table layer all build the same settings
 * from the same inputs. The admin clients do not come through here: {@code BigtableTableAdmin} and
 * the Change Streams coordinator build their own settings, and each carries its own copy of the
 * emulator branch. A second copy of this branch is exactly the thing that reads green on a machine
 * with credentials and red in CI — the failure ADR-0064 exists to describe.
 *
 * <p>Only what every path shares lives here. Per-caller tuning — the sink's batch thresholds, and
 * whatever a source may one day need — is applied by the caller on top of the builder this hands
 * back, which is why the return type is the builder rather than the settings.
 *
 * <p>Retry settings are deliberately untouched, on every path. The client retries {@code
 * MutateRows} per entry, resumes a broken {@code ReadRows} stream from the last key it saw, and
 * resumes a broken {@code ReadChangeStream} from its last continuation token. Only a failure that
 * outlives the client's own retry budget reaches the connector, and on the Change Streams path the
 * reader turns that into a terminal failure so Flink restarts from the checkpointed token. So
 * nothing here owns a retry loop, and nothing has a retry knob to map.
 */
@Internal
public final class BigtableDataClients {

    private BigtableDataClients() {}

    /**
     * Builds the settings for a client that talks to a destination's instance.
     *
     * @param destination the table whose project and instance the client is bound to; the table
     *     itself is named per call, not in the settings
     * @param appProfileId the application profile to route through, or {@code null} for the
     *     instance's default
     * @param emulatorEndpoint the emulator to connect to (plaintext, no credentials), or {@code
     *     null} for production Bigtable
     * @param credentialsOverride the runtime-loaded service-account provider, or {@code null} to
     *     preserve application-default credentials
     * @return the settings builder, for the caller to tune and build
     */
    public static BigtableDataSettings.Builder settings(
            TableDestination destination,
            @Nullable String appProfileId,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            @Nullable CredentialsProvider credentialsOverride) {
        Preconditions.checkNotNull(destination, "destination must not be null");
        BigtableDataSettings.Builder settings =
                emulatorEndpoint == null
                        ? BigtableDataSettings.newBuilder()
                        : BigtableDataSettings.newBuilderForEmulator(
                                emulatorEndpoint.getHost(), emulatorEndpoint.getPort());
        settings.setProjectId(destination.getProject()).setInstanceId(destination.getInstance());
        if (credentialsOverride != null) {
            Preconditions.checkArgument(
                    emulatorEndpoint == null,
                    "credentialsOverride cannot be combined with an emulator endpoint");
            settings.setCredentialsProvider(credentialsOverride);
        }
        if (appProfileId != null) {
            settings.setAppProfileId(appProfileId);
        }
        return settings;
    }
}
