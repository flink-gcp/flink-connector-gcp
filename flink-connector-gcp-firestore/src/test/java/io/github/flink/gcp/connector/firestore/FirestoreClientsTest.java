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

import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.FirestoreOptions;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.testutils.LogCapture;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FirestoreClientsTest {

    private static final DatabaseDestination DATABASE = DatabaseDestination.of("p", "orders");
    private static final EmulatorEndpoint EMULATOR =
            EmulatorEndpoint.parse("localhost:8080", "emulatorEndpoint");

    @Test
    void theDatabaseAndTheEmulatorReachTheSettings() {
        FirestoreOptions settings =
                FirestoreClients.settings(DATABASE, EMULATOR, null, null, variable -> null);

        assertThat(settings.getProjectId()).isEqualTo("p");
        assertThat(settings.getDatabaseId()).isEqualTo("orders");
        assertThat(
                        ((InstantiatingGrpcChannelProvider) settings.getTransportChannelProvider())
                                .getEndpoint())
                .isEqualTo("localhost:8080");
    }

    @Test
    void credentialsCannotBeCombinedWithAnEmulator() {
        GoogleCredentials credentials = GoogleCredentials.create(new AccessToken("t", null));

        assertThatThrownBy(
                        () ->
                                FirestoreClients.settings(
                                        DATABASE, EMULATOR, credentials, null, v -> null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void anInheritedEmulatorVariableIsWarnedAboutWhenNoEmulatorIsConfigured() {
        try (LogCapture capture = LogCapture.of(FirestoreClients.class)) {
            FirestoreClients.settings(
                    DATABASE,
                    null,
                    GoogleCredentials.create(new AccessToken("t", null)),
                    null,
                    variable ->
                            variable.equals(FirestoreClients.EMULATOR_HOST_VARIABLE)
                                    ? "stray:9000"
                                    : null);

            assertThat(capture.getMessages())
                    .singleElement()
                    .asString()
                    .contains(
                            "FIRESTORE_EMULATOR_HOST", "stray:9000", "projects/p/databases/orders");
        }
    }

    @Test
    void theVariableIsNotWarnedAboutBesideAConfiguredEmulator() {
        try (LogCapture capture = LogCapture.of(FirestoreClients.class)) {
            FirestoreClients.settings(DATABASE, EMULATOR, null, null, variable -> "stray:9000");

            assertThat(capture.getMessages()).isEmpty();
        }
    }
}
