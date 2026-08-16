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

package io.github.flink.gcp.connector.base.rpc;

import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import io.grpc.ManagedChannel;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link EmulatorChannels}. */
class EmulatorChannelsTest {

    private static final EmulatorEndpoint ENDPOINT = EmulatorEndpoint.parse("localhost:8086");

    @Test
    void theProviderDialsTheEndpointOverPlaintext() {
        InstantiatingGrpcChannelProvider provider =
                EmulatorChannels.plaintextProvider(anApiDefaultBuilder(), ENDPOINT);

        assertThat(provider.getEndpoint()).isEqualTo("localhost:8086");
        // The configurator is what makes the channel plaintext; the endpoint alone would leave it
        // TLS, which no emulator here terminates.
        assertThat(provider.toBuilder().getChannelConfigurator()).isNotNull();
    }

    @Test
    void theApisOwnTransportDefaultsSurvive() {
        // The reason the builder is a parameter rather than something the helper creates: each
        // API's defaultGrpcTransportProviderBuilder() raises the maximum inbound message size
        // (Integer.MAX_VALUE for BigQuery Storage, 20 MiB for Pub/Sub's subscriber stub), and
        // starting from a bare newBuilder() would silently run the emulator at gRPC's 4 MiB
        // default.
        InstantiatingGrpcChannelProvider provider =
                EmulatorChannels.plaintextProvider(anApiDefaultBuilder(), ENDPOINT);

        assertThat(provider.toBuilder().getMaxInboundMessageSize()).isEqualTo(20_971_520);
    }

    @Test
    void theClientClosesAnInstantiatedChannelWithItself() {
        assertThat(
                        EmulatorChannels.plaintextProvider(anApiDefaultBuilder(), ENDPOINT)
                                .shouldAutoClose())
                .isTrue();
    }

    @Test
    void theClientDoesNotCloseACallerOwnedChannel() throws IOException {
        ManagedChannel channel = EmulatorChannels.openPlaintextChannel(ENDPOINT);
        try {
            FixedTransportChannelProvider provider = EmulatorChannels.fixedProvider(channel);

            // The whole reason a caller takes this route: it keeps the channel, so it can share one
            // across clients and decide when the shutdown happens.
            assertThat(provider.shouldAutoClose()).isFalse();
            assertThat(((GrpcTransportChannel) provider.getTransportChannel()).getChannel())
                    .isSameAs(channel);
        } finally {
            channel.shutdownNow();
        }
    }

    @Test
    void theChannelDialsTheEndpoint() {
        ManagedChannel channel = EmulatorChannels.openPlaintextChannel(ENDPOINT);
        try {
            assertThat(channel.authority()).isEqualTo("localhost:8086");
        } finally {
            channel.shutdownNow();
        }
    }

    @Test
    void rejectsNull() {
        assertThatThrownBy(() -> EmulatorChannels.plaintextProvider(anApiDefaultBuilder(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("endpoint must not be null");
        assertThatThrownBy(() -> EmulatorChannels.plaintextProvider(null, ENDPOINT))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("providerBuilder must not be null");
        assertThatThrownBy(() -> EmulatorChannels.openPlaintextChannel(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("endpoint must not be null");
        assertThatThrownBy(() -> EmulatorChannels.fixedProvider(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("channel must not be null");
    }

    /**
     * Stands in for an {@code <Api>Settings.defaultGrpcTransportProviderBuilder()}: base depends on
     * no client library, so the property under test — that whatever the caller seeded survives — is
     * pinned with a seeded builder rather than with one API's.
     */
    private static InstantiatingGrpcChannelProvider.Builder anApiDefaultBuilder() {
        return InstantiatingGrpcChannelProvider.newBuilder().setMaxInboundMessageSize(20_971_520);
    }
}
