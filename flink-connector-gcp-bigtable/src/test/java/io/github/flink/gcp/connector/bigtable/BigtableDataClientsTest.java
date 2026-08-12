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

package io.github.flink.gcp.connector.bigtable;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link BigtableDataClients} — the emulator-versus-credentials branch both directions of
 * this connector build their clients through.
 *
 * <p>The mapping is otherwise invisible: an application profile that never reaches the client looks
 * exactly like one that does, and an emulator branch that presents credentials fails only where
 * there are none to present.
 */
@Timeout(30)
class BigtableDataClientsTest {

    private static final TableDestination TABLE = TableDestination.of("p", "i", "orders");

    @Test
    void carriesTheProjectAndInstanceButNotTheTable() {
        BigtableDataSettings settings =
                BigtableDataClients.settings(TABLE, null, null, null).build();

        assertThat(settings.getProjectId()).isEqualTo("p");
        assertThat(settings.getInstanceId()).isEqualTo("i");
    }

    @Test
    void carriesTheApplicationProfileWhenOneIsSet() {
        BigtableDataSettings settings =
                BigtableDataClients.settings(TABLE, "batch-profile", null, null).build();

        assertThat(settings.getAppProfileId()).isEqualTo("batch-profile");
    }

    @Test
    void leavesTheInstancesDefaultProfileInPlaceWhenNoneIsSet() {
        BigtableDataSettings settings =
                BigtableDataClients.settings(TABLE, null, null, null).build();

        // The client spells "the instance's own default" as an empty profile id.
        assertThat(settings.getAppProfileId()).isEmpty();
    }

    @Test
    void pointsAtProductionBigtableWithoutAnEmulatorEndpoint() {
        // Built with no credentials on the machine: the credentials provider is resolved when a
        // client is created, not when the settings are built, which is what lets a job graph be
        // assembled anywhere.
        BigtableDataSettings settings =
                BigtableDataClients.settings(TABLE, null, null, null).build();

        assertThat(settings.getStubSettings().getEndpoint())
                .isEqualTo("bigtable.googleapis.com:443");
    }

    @Test
    void pointsAtTheEmulatorOverAPlaintextChannelWithNoCredentials() {
        BigtableDataSettings settings =
                BigtableDataClients.settings(
                                TABLE, null, EmulatorEndpoint.parse("bigtable.example:9035"), null)
                        .build();

        assertThat(settings.getStubSettings().getEndpoint()).isEqualTo("bigtable.example:9035");
        assertThat(settings.getStubSettings().getCredentialsProvider().getClass().getSimpleName())
                .isEqualTo("NoCredentialsProvider");
    }

    @Test
    void appliesTheApplicationProfileInEmulatorModeToo() {
        BigtableDataSettings settings =
                BigtableDataClients.settings(
                                TABLE,
                                "batch-profile",
                                EmulatorEndpoint.parse("bigtable.example:9035"),
                                null)
                        .build();

        assertThat(settings.getAppProfileId()).isEqualTo("batch-profile");
    }

    @Test
    void injectsTheRuntimeCredentialProviderInProductionMode() {
        NoCredentialsProvider provider = NoCredentialsProvider.create();

        BigtableDataSettings settings =
                BigtableDataClients.settings(TABLE, null, null, provider).build();

        assertThat(settings.getStubSettings().getCredentialsProvider()).isSameAs(provider);
    }

    @Test
    void refusesCredentialsInEmulatorMode() {
        assertThatThrownBy(
                        () ->
                                BigtableDataClients.settings(
                                        TABLE,
                                        null,
                                        EmulatorEndpoint.parse("localhost:9035"),
                                        NoCredentialsProvider.create()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("credentialsOverride cannot be combined with an emulator endpoint");
    }
}
