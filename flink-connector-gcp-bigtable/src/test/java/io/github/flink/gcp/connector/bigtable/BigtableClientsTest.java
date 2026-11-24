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

import com.google.api.core.ApiFunction;
import com.google.api.core.ApiFuture;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.GoogleCredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.StubSettings;
import com.google.bigtable.admin.v2.GetAppProfileRequest;
import com.google.bigtable.admin.v2.GetTableRequest;
import com.google.cloud.bigtable.admin.v2.BaseBigtableInstanceAdminSettings;
import com.google.cloud.bigtable.admin.v2.BaseBigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminClientV2;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClientV2;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.stub.EnhancedBigtableStubSettings;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.testutils.GrpcEventLoopThreads;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link BigtableClients}. How the holder closes a client is held by base's {@code
 * LeasedClientTest}.
 */
@Timeout(60)
class BigtableClientsTest {

    /** Nothing listens here: creating a client connects nothing until it makes a call. */
    private static final EmulatorEndpoint EMULATOR =
            EmulatorEndpoint.parse("localhost:1", "emulatorEndpoint");

    private static final String PREFIX = GrpcEventLoopThreads.PREFIX;

    @BeforeEach
    void noLeaseIsOpenBeforehand() {
        assertThat(GrpcEventLoops.isRunning())
                .as("a lease is open before the test: an earlier class in this fork leaked one")
                .isFalse();
    }

    @AfterEach
    void noLeaseIsLeftOpen() {
        assertThat(GrpcEventLoops.isRunning()).as("a lease was left open").isFalse();
    }

    /**
     * The production arm: the SDK's own provider, with the lease applied, builds a channel that
     * runs on the group. The emulator arm is held by {@code BigtableEventLoopITCase}.
     */
    @Test
    void theProductionProviderBuildsItsChannelsOnTheGroup() throws Exception {
        GrpcEventLoops.Lease lease = GrpcEventLoops.acquire();
        try (ServerSocket server = new ServerSocket(0)) {
            EnhancedBigtableStubSettings.Builder stub =
                    BigtableDataSettings.newBuilder().stubSettings();
            BigtableClients.applyTo(lease, stub);
            ManagedChannel channel =
                    configurator(stub)
                            .apply(
                                    NettyChannelBuilder.forAddress(
                                                    "127.0.0.1", server.getLocalPort())
                                            .usePlaintext())
                            .build();
            try {
                // Connecting registers the channel on an event loop, which starts its thread.
                channel.getState(true);
                assertThat(GrpcEventLoopThreads.awaitAlive())
                        .as("a %s* thread while the channel connects", GrpcEventLoopThreads.PREFIX)
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
    @SuppressWarnings("rawtypes")
    void keepsTheConfiguratorAndSettingsTheProviderAlreadyCarries() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        EnhancedBigtableStubSettings.Builder stub =
                BigtableDataSettings.newBuilderForEmulator("localhost", 1).stubSettings();
        ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder> emulators = configurator(stub);
        stub.setTransportChannelProvider(
                ((InstantiatingGrpcChannelProvider) stub.getTransportChannelProvider())
                        .toBuilder()
                                .setChannelConfigurator(
                                        builder -> {
                                            calls.incrementAndGet();
                                            return emulators.apply(builder);
                                        })
                                .build());

        try (GrpcEventLoops.Lease lease = GrpcEventLoops.acquire()) {
            BigtableClients.applyTo(lease, stub);
            configurator(stub).apply(NettyChannelBuilder.forTarget("localhost:1"));
        }

        assertThat(calls).as("the emulator's plaintext configurator still runs").hasValue(1);
        // The provider is copied whole: the emulator builder's inbound message limit survives.
        assertThat(provider(stub).toBuilder().getMaxInboundMessageSize())
                .isEqualTo(256 * 1024 * 1024);
    }

    /**
     * The admin clients' production arm: the lease goes on the settings' own provider, so a channel
     * the client builds runs on the group while the holder is open, and the holder's close stops
     * the group. The settings point at a local socket that speaks no TLS, so the call never
     * completes; it only makes the channel connect. The emulator arm is held by {@code
     * BigtableEventLoopITCase}.
     */
    @Test
    void theProductionAdminClientsBuildTheirChannelsOnTheGroup() throws Exception {
        // An earlier test's released group shuts down by starting its never-run loops, whose
        // threads would otherwise pass the checks below.
        assertThat(GrpcEventLoopThreads.awaitNone())
                .as("no %s* thread beforehand", PREFIX)
                .isTrue();
        try (ServerSocket server = new ServerSocket(0)) {
            String local = "127.0.0.1:" + server.getLocalPort();
            BaseBigtableTableAdminSettings.Builder tables =
                    BigtableClients.tableAdminSettings(null, NoCredentialsProvider.create());
            tables.setEndpoint(local);
            try (GrpcEventLoops.LeasedClient<BigtableTableAdminClientV2> client =
                    BigtableClients.openTableAdmin(tables, null)) {
                ApiFuture<?> call =
                        client.client()
                                .getTableCallable()
                                .futureCall(
                                        GetTableRequest.newBuilder()
                                                .setName("projects/p/instances/i/tables/t")
                                                .build());
                try {
                    assertThat(GrpcEventLoopThreads.awaitAlive())
                            .as("a %s* thread while the table admin connects", PREFIX)
                            .isTrue();
                } finally {
                    call.cancel(true);
                }
            }
            assertThat(GrpcEventLoopThreads.awaitNone()).isTrue();

            BaseBigtableInstanceAdminSettings.Builder instances =
                    BigtableClients.instanceAdminSettings(null, NoCredentialsProvider.create());
            instances.setEndpoint(local);
            try (GrpcEventLoops.LeasedClient<BigtableInstanceAdminClientV2> client =
                    BigtableClients.openInstanceAdmin(instances, null)) {
                ApiFuture<?> call =
                        client.client()
                                .getAppProfileCallable()
                                .futureCall(
                                        GetAppProfileRequest.newBuilder()
                                                .setName("projects/p/instances/i/appProfiles/a")
                                                .build());
                try {
                    assertThat(GrpcEventLoopThreads.awaitAlive())
                            .as("a %s* thread while the instance admin connects", PREFIX)
                            .isTrue();
                } finally {
                    call.cancel(true);
                }
            }
            assertThat(GrpcEventLoopThreads.awaitNone()).isTrue();
        }
    }

    /**
     * The holder is gax's form, so its close shuts the client down rather than only its channels.
     */
    @Test
    void theAdminClientsAreHeldAsGaxClients() throws Exception {
        GrpcEventLoops.LeasedClient<BigtableTableAdminClientV2> tables =
                BigtableClients.openTableAdmin(
                        BigtableClients.tableAdminSettings(EMULATOR, null), EMULATOR);
        tables.close();
        GrpcEventLoops.LeasedClient<BigtableInstanceAdminClientV2> instances =
                BigtableClients.openInstanceAdmin(
                        BigtableClients.instanceAdminSettings(EMULATOR, null), EMULATOR);
        instances.close();

        assertThat(tables.client().isTerminated()).isTrue();
        assertThat(instances.client().isTerminated()).isTrue();
    }

    @Test
    void theAdminSettingsCarryTheGivenCredentials() {
        CredentialsProvider provider = NoCredentialsProvider.create();

        assertThat(BigtableClients.tableAdminSettings(null, provider).getCredentialsProvider())
                .isSameAs(provider);
        assertThat(BigtableClients.instanceAdminSettings(null, provider).getCredentialsProvider())
                .isSameAs(provider);
        // Unset, they keep the client library's application-default credentials.
        assertThat(BigtableClients.tableAdminSettings(null, null).getCredentialsProvider())
                .isInstanceOf(GoogleCredentialsProvider.class);
        assertThat(BigtableClients.instanceAdminSettings(null, null).getCredentialsProvider())
                .isInstanceOf(GoogleCredentialsProvider.class);
    }

    @Test
    void theEmulatorAdminSettingsCarryNoCredentials() {
        assertThat(BigtableClients.tableAdminSettings(EMULATOR, null).getCredentialsProvider())
                .isInstanceOf(NoCredentialsProvider.class);
        assertThat(BigtableClients.instanceAdminSettings(EMULATOR, null).getCredentialsProvider())
                .isInstanceOf(NoCredentialsProvider.class);
    }

    @Test
    void refusesCredentialsWithAnEmulator() {
        CredentialsProvider provider = NoCredentialsProvider.create();

        assertThatThrownBy(() -> BigtableClients.tableAdminSettings(EMULATOR, provider))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("emulator endpoint");
        assertThatThrownBy(() -> BigtableClients.instanceAdminSettings(EMULATOR, provider))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("emulator endpoint");
    }

    @Test
    void refusesAProviderThatDoesNotBuildItsOwnChannels() {
        EnhancedBigtableStubSettings.Builder stub =
                BigtableDataSettings.newBuilder().stubSettings();
        ManagedChannel channel = NettyChannelBuilder.forTarget("localhost:1").build();
        try (GrpcEventLoops.Lease lease = GrpcEventLoops.acquire()) {
            stub.setTransportChannelProvider(
                    FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel)));

            assertThatThrownBy(() -> BigtableClients.applyTo(lease, stub))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("builds its own channels");
        } finally {
            channel.shutdownNow();
        }
    }

    @SuppressWarnings("rawtypes")
    private static ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder> configurator(
            EnhancedBigtableStubSettings.Builder stub) {
        return provider(stub).toBuilder().getChannelConfigurator();
    }

    private static InstantiatingGrpcChannelProvider provider(StubSettings.Builder<?, ?> stub) {
        return (InstantiatingGrpcChannelProvider) stub.getTransportChannelProvider();
    }
}
