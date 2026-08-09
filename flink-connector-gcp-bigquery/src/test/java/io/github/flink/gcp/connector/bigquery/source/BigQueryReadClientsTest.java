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

package io.github.flink.gcp.connector.bigquery.source;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BigQueryReadClients}. */
class BigQueryReadClientsTest {

    @Test
    void theEmulatorSettingsKeepTheStorageReadApisInboundMessageSize() throws IOException {
        // Storage Read responses carry whole row batches, and the API's own transport builder
        // raises the limit to Integer.MAX_VALUE for that reason. A provider built from a bare
        // InstantiatingGrpcChannelProvider.newBuilder() drops back to gRPC's 4 MiB default, which
        // fails only on the emulator and only once a batch grows past it.
        BigQueryReadSettings settings =
                BigQueryReadClients.emulatorSettings(EmulatorEndpoint.parse("localhost:9060"));

        assertThat(provider(settings).toBuilder().getMaxInboundMessageSize())
                .isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void theEmulatorSettingsDialTheEmulatorWithoutCredentials() throws IOException {
        BigQueryReadSettings settings =
                BigQueryReadClients.emulatorSettings(EmulatorEndpoint.parse("localhost:9060"));

        assertThat(provider(settings).getEndpoint()).isEqualTo("localhost:9060");
        assertThat(provider(settings).toBuilder().getChannelConfigurator()).isNotNull();
        assertThat(settings.getCredentialsProvider()).isInstanceOf(NoCredentialsProvider.class);
    }

    private static InstantiatingGrpcChannelProvider provider(BigQueryReadSettings settings) {
        // The cast is safe by construction: the settings come from EmulatorChannels, whose
        // plaintextProvider returns this type.
        return (InstantiatingGrpcChannelProvider) settings.getTransportChannelProvider();
    }
}
