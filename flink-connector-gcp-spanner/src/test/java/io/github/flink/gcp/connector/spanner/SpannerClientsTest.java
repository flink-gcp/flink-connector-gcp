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

package io.github.flink.gcp.connector.spanner;

import com.google.api.core.ApiFunction;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.CustomOpenTelemetryMetricsProvider;
import com.google.cloud.spanner.SpannerOptions;
import io.github.flink.gcp.connector.base.metrics.ClientMetrics;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.testutils.EventLoopObserver;
import io.github.flink.gcp.connector.testutils.GrpcEventLoopThreads;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SpannerClients}. */
class SpannerClientsTest {

    private static final DatabaseDestination DATABASE =
            DatabaseDestination.of("my-project", "my-instance", "my-db");
    private static final EmulatorEndpoint EMULATOR =
            EmulatorEndpoint.parse("localhost:9010", "emulatorEndpoint");
    private static final GoogleCredentials CREDENTIALS =
            GoogleCredentials.create(new AccessToken("token", new Date(Long.MAX_VALUE)));

    @AfterEach
    void noLeaseIsLeftOpen() {
        assertThat(GrpcEventLoops.isRunning()).as("a lease was left open").isFalse();
    }

    @Test
    void anEmulatorsChannelsRunOnTheGroupThoughBuildReplacesTheConfigurator() throws Exception {
        // build() sets its own plaintext configurator when an emulator host is set (6.127.0); the
        // builder composes the group's into it, and the client stays in its emulator mode.
        SpannerOptions settings = SpannerClients.settings(DATABASE, EMULATOR);

        assertThat(settings.isEmulatorEnabled()).isTrue();
        assertRunsOnTheGroup(settings);
    }

    @Test
    void theServicesChannelsRunOnTheGroup() throws Exception {
        assertRunsOnTheGroup(SpannerClients.settings(DATABASE, null, CREDENTIALS, null, true));
    }

    @Test
    void theTestSwitchsChannelsRunOnTheGroup() throws Exception {
        SpannerClients.bypassEmulatorModeForTesting(true);
        try {
            assertRunsOnTheGroup(SpannerClients.settings(DATABASE, EMULATOR));
        } finally {
            SpannerClients.bypassEmulatorModeForTesting(false);
        }
    }

    @Test
    void aBuilderAComponentSetsMoreOnKeepsTheGroup() throws Exception {
        SpannerOptions.Builder builder =
                SpannerClients.settingsBuilder(DATABASE, EMULATOR, null, null, true);
        builder.setNumChannels(2);

        assertRunsOnTheGroup(builder.build());
    }

    @Test
    void openRefusesARebuiltCopyWhoseBuildDroppedTheGroup() {
        // A plain toBuilder() copy rebuilt with an emulator host gets build()'s bare plaintext
        // configurator back: its channels would run on gRPC's own group.
        SpannerOptions rebuilt = SpannerClients.settings(DATABASE, EMULATOR).toBuilder().build();

        assertThatThrownBy(() -> SpannerClients.open(DATABASE, rebuilt))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("event-loop group")
                .hasMessageContaining(DATABASE.toString());
    }

    @Test
    void openRefusesSettingsWithAChannelProvider() {
        // The client uses a transport provider in place of the configurator, so its channels would
        // run on gRPC's own group.
        SpannerOptions withProvider =
                SpannerClients.settingsBuilder(DATABASE, EMULATOR, null, null, true)
                        .setChannelProvider(
                                com.google.api.gax.grpc.InstantiatingGrpcChannelProvider
                                        .newBuilder()
                                        .setEndpoint("localhost:9010")
                                        .build())
                        .build();

        assertThatThrownBy(() -> SpannerClients.open(DATABASE, withProvider))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("set no channel provider");
    }

    /**
     * Connects a channel through the settings' configurator under a lease and checks that one of
     * the group's event loops started for it.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void assertRunsOnTheGroup(SpannerOptions settings) throws Exception {
        ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder> configurator =
                settings.getChannelConfigurator();
        // A group an earlier test released may still be stopping, so a live thread would not show
        // which group the channel reached; the group's count does.
        EventLoopObserver.LoopStarts starts =
                EventLoopObserver.loopStartsFromNow(
                        GrpcEventLoops::isRunning, GrpcEventLoops::loopsStartedWhileRunning);
        GrpcEventLoops.Lease lease = GrpcEventLoops.acquire();
        try (ServerSocket server = new ServerSocket(0)) {
            ManagedChannel channel =
                    configurator
                            .apply(
                                    NettyChannelBuilder.forAddress(
                                            "127.0.0.1", server.getLocalPort()))
                            .build();
            try {
                channel.getState(true);
                assertThat(starts.awaitAnyStarted())
                        .as("a group event loop started while the channel connects")
                        .isTrue();
            } finally {
                channel.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
            }
        } finally {
            lease.close();
        }
        assertThat(GrpcEventLoopThreads.awaitNone()).isTrue();
    }

    @Test
    void anEmulatorEndpointAlsoTurnsOffCredentialsAndTransportSecurity() {
        // One call does all three, which is why nothing else here configures the emulator — and
        // why every test touching a production path can point at a closed port safely.
        SpannerOptions settings =
                SpannerClients.settings(
                        DATABASE, EmulatorEndpoint.parse("localhost:9010", "emulatorEndpoint"));

        assertThat(settings.getProjectId()).isEqualTo("my-project");
        assertThat(settings.getHost()).isEqualTo("http://localhost:9010");
        assertThat(settings.getCredentials()).isInstanceOf(NoCredentials.class);
    }

    @Test
    void withoutAnEmulatorTheProjectIsStillTheDatabasesOwn() {
        // Not asserted against the credentials: without an emulator the client reaches for
        // application default credentials, which a workstation has and a build agent does not.
        assertThat(SpannerClients.settings(DATABASE, null).getProjectId()).isEqualTo("my-project");
    }

    @Test
    void runtimeCredentialsOverrideApplicationDefaultCredentials() {
        SpannerOptions settings = SpannerClients.settings(DATABASE, null, CREDENTIALS);

        assertThat(settings.getCredentials() == CREDENTIALS).isTrue();
    }

    @Test
    void credentialsCannotBeCombinedWithAnEmulator() {
        GoogleCredentials credentials =
                GoogleCredentials.create(new AccessToken("token", new Date(Long.MAX_VALUE)));

        assertThatThrownBy(
                        () ->
                                SpannerClients.settings(
                                        DATABASE,
                                        EmulatorEndpoint.parse(
                                                "localhost:9010", "emulatorEndpoint"),
                                        credentials))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be combined");
    }

    @Test
    void anEmulatorEndpointKeepsTheClientInItsEmulatorModeWithoutTheCloudMonitoringExport() {
        // The emulator mode is what makes the client record no client metrics; the export is
        // off so that leaving the mode (the test switch below) cannot start it.
        SpannerOptions settings = SpannerClients.settings(DATABASE, EMULATOR);

        assertThat(settings.isEmulatorEnabled()).isTrue();
        assertThat(settings.isEnableBuiltInMetrics()).isFalse();
    }

    @Test
    void theTestSwitchReachesTheSameEmulatorOutsideTheEmulatorMode() {
        SpannerClients.bypassEmulatorModeForTesting(true);
        try {
            SpannerOptions settings = SpannerClients.settings(DATABASE, EMULATOR);

            assertThat(settings.isEmulatorEnabled()).isFalse();
            assertThat(settings.getHost()).isEqualTo("http://localhost:9010");
            assertThat(settings.getCredentials()).isInstanceOf(NoCredentials.class);
            assertThat(settings.isEnableBuiltInMetrics()).isFalse();
        } finally {
            SpannerClients.bypassEmulatorModeForTesting(false);
        }
        assertThat(SpannerClients.settings(DATABASE, EMULATOR).isEmulatorEnabled()).isTrue();
    }

    @Test
    void clientMetricsAreHandedToTheClientsProvider() {
        ClientMetrics metrics = ClientMetrics.create("method");

        SpannerOptions settings =
                SpannerClients.settings(DATABASE, null, CREDENTIALS, metrics, true);

        assertThat(settings.getClientMetricsProvider())
                .isInstanceOfSatisfying(
                        CustomOpenTelemetryMetricsProvider.class,
                        provider ->
                                assertThat(provider.getOpenTelemetry())
                                        .isSameAs(metrics.openTelemetry()));
    }

    @Test
    void withoutClientMetricsTheClientKeepsItsDefaultProvider() {
        SpannerOptions settings = SpannerClients.settings(DATABASE, null, CREDENTIALS, null, true);

        assertThat(settings.getClientMetricsProvider())
                .isNotInstanceOf(CustomOpenTelemetryMetricsProvider.class);
    }

    @Test
    void theCloudMonitoringExportIsTurnedOffAndNeverOn() {
        // Never on: a client whose export is already off, as SPANNER_DISABLE_BUILTIN_METRICS
        // leaves it, keeps it off when the choice is the default.
        SpannerOptions.Builder disabled =
                SpannerOptions.newBuilder()
                        .setProjectId("my-project")
                        .setCredentials(CREDENTIALS)
                        .setBuiltInMetricsEnabled(false);
        SpannerClients.applyClientMetrics(disabled, null, true);
        assertThat(disabled.build().isEnableBuiltInMetrics()).isFalse();

        SpannerOptions.Builder enabled =
                SpannerOptions.newBuilder()
                        .setProjectId("my-project")
                        .setCredentials(CREDENTIALS)
                        .setBuiltInMetricsEnabled(true);
        SpannerClients.applyClientMetrics(enabled, null, false);
        assertThat(enabled.build().isEnableBuiltInMetrics()).isFalse();
    }

    @Test
    void closesTheHandleBeforeTheClientMetrics() {
        ClientMetrics metrics = ClientMetricsProbe.create();
        assertThat(ClientMetricsProbe.isOpen(metrics)).as("the probe on open metrics").isTrue();
        ClosingSpanner spanner = new ClosingSpanner(metrics);

        SpannerClients.close(TestLeasedSpanners.leased(spanner.spanner()), metrics);

        assertThat(spanner.closes()).isOne();
        assertThat(GrpcEventLoops.isRunning()).as("the handle's lease was released").isFalse();
        assertThat(spanner.metricsOpenAtClose()).isTrue();
        assertThat(ClientMetricsProbe.isOpen(metrics)).isFalse();
    }

    @Test
    void closesTheClientMetricsWhenTheHandleCannotBeCreated() {
        ClientMetrics metrics = ClientMetricsProbe.create();
        SpannerOptions settings =
                SpannerClients.settings(DATABASE, ClosedEmulator.endpoint(), null, metrics, true);

        assertThatThrownBy(() -> SpannerClients.open(DATABASE, settings, metrics))
                .isInstanceOf(IOException.class);

        assertThat(ClientMetricsProbe.isOpen(metrics)).isFalse();
        assertThat(GrpcEventLoops.isRunning()).as("the failed open released its lease").isFalse();
    }
}
