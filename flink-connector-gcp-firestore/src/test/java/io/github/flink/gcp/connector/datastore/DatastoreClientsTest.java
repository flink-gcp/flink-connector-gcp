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

import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.v1.DatastoreSettings;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DatastoreClientsTest {

    private static final DatabaseDestination DATABASE = DatabaseDestination.of("p", "orders");
    private static final Duration TIMEOUT = Duration.ofSeconds(7);

    @Test
    void anEmulatorIsReachedWithoutCredentials() {
        DatastoreOptions settings =
                DatastoreClients.settings(
                        DATABASE, EmulatorEndpoint.parse("localhost:8081", "e"), null, TIMEOUT);

        assertThat(settings.getHost()).isEqualTo("localhost:8081");
        assertThat(settings.getCredentials()).isSameAs(NoCredentials.getInstance());
        assertThat(settings.getProjectId()).isEqualTo("p");
        assertThat(settings.getDatabaseId()).isEqualTo("orders");
    }

    @Test
    void theServiceHostIsSetSoTheEmulatorVariableIsNeverConsulted() {
        String previous = System.getProperty(DatastoreOptions.LOCAL_HOST_ENV_VAR);
        // The client library reads the variable as a system property first, which a test can set.
        System.setProperty(DatastoreOptions.LOCAL_HOST_ENV_VAR, "localhost:9999");
        try {
            GoogleCredentials credentials = GoogleCredentials.create(new AccessToken("t", null));
            DatastoreOptions settings =
                    DatastoreClients.settings(DATABASE, null, credentials, TIMEOUT);

            assertThat(settings.getHost()).isEqualTo(DatastoreSettings.getDefaultEndpoint());
            assertThat(settings.getCredentials()).isSameAs(credentials);
        } finally {
            if (previous == null) {
                System.clearProperty(DatastoreOptions.LOCAL_HOST_ENV_VAR);
            } else {
                System.setProperty(DatastoreOptions.LOCAL_HOST_ENV_VAR, previous);
            }
        }
    }

    @Test
    void theClientMakesOneAttemptBoundedByTheTimeout() {
        RetrySettings retry =
                DatastoreClients.settings(
                                DATABASE,
                                EmulatorEndpoint.parse("localhost:8081", "e"),
                                null,
                                TIMEOUT)
                        .getRetrySettings();

        assertThat(retry.getMaxAttempts()).isEqualTo(1);
        assertThat(retry.getTotalTimeoutDuration()).isEqualTo(TIMEOUT);
        assertThat(retry.getInitialRpcTimeoutDuration()).isEqualTo(TIMEOUT);
        assertThat(retry.getMaxRpcTimeoutDuration()).isEqualTo(TIMEOUT);
    }

    @Test
    void withoutATimeoutTheClientKeepsTheLibrarysRetries() {
        // The source's reads are idempotent at a fixed read time, so it leaves retries to the
        // library rather than owning a loop.
        EmulatorEndpoint emulator = EmulatorEndpoint.parse("localhost:8081", "e");
        RetrySettings retry =
                DatastoreClients.settings(DATABASE, emulator, null, null).getRetrySettings();

        assertThat(retry)
                .isEqualTo(
                        DatastoreOptions.newBuilder().setProjectId("p").build().getRetrySettings());
        assertThat(retry.getMaxAttempts()).isGreaterThan(1);
    }

    @Test
    void credentialsCannotBeCombinedWithAnEmulator() {
        assertThatThrownBy(
                        () ->
                                DatastoreClients.settings(
                                        DATABASE,
                                        EmulatorEndpoint.parse("localhost:8081", "e"),
                                        GoogleCredentials.create(new AccessToken("t", null)),
                                        TIMEOUT))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
