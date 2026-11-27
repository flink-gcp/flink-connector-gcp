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

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.v1.DatastoreClient;
import com.google.cloud.datastore.v1.DatastoreSettings;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.testutils.ServiceAccountKeyFiles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DatastoreClientsTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(7);
    private static final EmulatorEndpoint EMULATOR = EmulatorEndpoint.parse("localhost:8081", "e");

    @Test
    void anEmulatorIsReachedWithoutCredentials() throws Exception {
        DatastoreSettings settings = DatastoreClients.settings(EMULATOR, null, null).build();

        assertThat(settings.getCredentialsProvider()).isInstanceOf(NoCredentialsProvider.class);
    }

    @Test
    void theServiceIsReachedAtTheClientsOwnEndpointWithTheGivenCredentials() throws Exception {
        String previous = System.getProperty(DatastoreOptions.LOCAL_HOST_ENV_VAR);
        // The library's Datastore read the variable as a system property first; the generated
        // client reads neither it nor the environment variable.
        System.setProperty(DatastoreOptions.LOCAL_HOST_ENV_VAR, "localhost:9999");
        try {
            GoogleCredentials credentials = GoogleCredentials.create(new AccessToken("t", null));
            DatastoreSettings settings = DatastoreClients.settings(null, credentials, null).build();

            assertThat(settings.getEndpoint()).isEqualTo(DatastoreSettings.getDefaultEndpoint());
            assertThat(settings.getCredentialsProvider())
                    .isInstanceOf(FixedCredentialsProvider.class);
            assertThat(settings.getCredentialsProvider().getCredentials()).isSameAs(credentials);
        } finally {
            if (previous == null) {
                System.clearProperty(DatastoreOptions.LOCAL_HOST_ENV_VAR);
            } else {
                System.setProperty(DatastoreOptions.LOCAL_HOST_ENV_VAR, previous);
            }
        }
    }

    @Test
    void aSingleAttemptReachesEveryCallTheSinkMakes() throws Exception {
        DatastoreSettings settings =
                DatastoreClients.settings(EMULATOR, null, DatastoreClients.singleAttempt(TIMEOUT))
                        .build();

        for (RetrySettings retry :
                new RetrySettings[] {
                    settings.commitSettings().getRetrySettings(),
                    settings.lookupSettings().getRetrySettings(),
                    settings.allocateIdsSettings().getRetrySettings()
                }) {
            assertThat(retry.getMaxAttempts()).isEqualTo(1);
            assertThat(retry.getTotalTimeoutDuration()).isEqualTo(TIMEOUT);
            assertThat(retry.getInitialRpcTimeoutDuration()).isEqualTo(TIMEOUT);
            assertThat(retry.getMaxRpcTimeoutDuration()).isEqualTo(TIMEOUT);
        }
    }

    @Test
    void theLibraryRetrySettingsAreTheOnesItsDatastoreGaveEveryCall() throws Exception {
        // The source's reads are idempotent at a fixed read time, so they keep the retries the
        // library's Datastore applied to every unary call; this fails the build if that default
        // moves away from the restated one.
        assertThat(DatastoreClients.libraryRetrySettings())
                .isEqualTo(
                        DatastoreOptions.newBuilder().setProjectId("p").build().getRetrySettings());
        assertThat(DatastoreClients.libraryRetrySettings().getMaxAttempts()).isGreaterThan(1);

        DatastoreSettings settings =
                DatastoreClients.settings(EMULATOR, null, DatastoreClients.libraryRetrySettings())
                        .build();
        assertThat(settings.runQuerySettings().getRetrySettings())
                .isEqualTo(DatastoreClients.libraryRetrySettings());
    }

    @Test
    void withoutRetrySettingsEachCallKeepsTheGeneratedClientsOwn() throws Exception {
        DatastoreSettings settings = DatastoreClients.settings(EMULATOR, null, null).build();

        assertThat(settings.lookupSettings().getRetrySettings())
                .isEqualTo(
                        DatastoreSettings.newBuilder().build().lookupSettings().getRetrySettings());
    }

    /**
     * The service arm builds its channels from the settings' own provider, which carries the
     * connector's event-loop group while the client's lease is open. Building the client opens no
     * connection, so this needs no service.
     */
    @Test
    void theServiceClientRunsOnTheEventLoopGroupUnderALease() throws Exception {
        assertThat(GrpcEventLoops.isRunning()).as("a lease is open before the test").isFalse();
        GoogleCredentials credentials = GoogleCredentials.create(new AccessToken("t", null));
        GrpcEventLoops.LeasedClient<DatastoreClient> client =
                DatastoreClients.open(
                        DatabaseDestination.of("p"),
                        null,
                        DatastoreClients.settings(null, credentials, null));
        try {
            assertThat(GrpcEventLoops.isRunning()).as("the client's lease").isTrue();
            InstantiatingGrpcChannelProvider provider =
                    (InstantiatingGrpcChannelProvider)
                            client.client().getSettings().getTransportChannelProvider();
            assertThat(provider.toBuilder().getChannelConfigurator())
                    .as("the group's configurator on the service provider")
                    .isNotNull();
        } finally {
            client.close();
        }
        assertThat(GrpcEventLoops.isRunning()).as("the lease was released").isFalse();
    }

    @Test
    void anEmulatorIsRefusedSettingsThatCarryCredentials() {
        GoogleCredentials credentials = GoogleCredentials.create(new AccessToken("t", null));

        assertThatThrownBy(
                        () ->
                                DatastoreClients.open(
                                        DatabaseDestination.of("p"),
                                        EMULATOR,
                                        DatastoreClients.settings(null, credentials, null)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must carry no credentials");
        assertThat(GrpcEventLoops.isRunning()).as("a lease was opened").isFalse();
    }

    /**
     * A configured service-account key signs its own scoped JWT, as the library's {@code Datastore}
     * sent it, rather than exchanging an assertion at the OAuth token endpoint, which a job with
     * egress only to Datastore cannot reach.
     */
    @Test
    void aServiceAccountKeySignsItsOwnJwt(@TempDir Path directory) throws Exception {
        GoogleCredentials loaded =
                DatastoreCredentials.load(ServiceAccountKeyFiles.create(directory).toString());
        assertThat(((ServiceAccountCredentials) loaded).getUseJwtAccessWithScope())
                .as("as the key file loads")
                .isFalse();

        Credentials sent =
                DatastoreClients.settings(null, loaded, null)
                        .build()
                        .getCredentialsProvider()
                        .getCredentials();

        assertThat(sent).isInstanceOf(ServiceAccountCredentials.class);
        assertThat(((ServiceAccountCredentials) sent).getUseJwtAccessWithScope()).isTrue();
        assertThat(((ServiceAccountCredentials) sent).getScopes())
                .as("a scoped JWT, not an audience one")
                .isNotEmpty()
                .isEqualTo(((ServiceAccountCredentials) loaded).getScopes());
    }

    @Test
    void credentialsCannotBeCombinedWithAnEmulator() {
        assertThatThrownBy(
                        () ->
                                DatastoreClients.settings(
                                        EMULATOR,
                                        GoogleCredentials.create(new AccessToken("t", null)),
                                        null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
