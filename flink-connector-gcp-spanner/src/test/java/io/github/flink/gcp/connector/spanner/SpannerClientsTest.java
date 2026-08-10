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

import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.SpannerOptions;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SpannerClients}. */
class SpannerClientsTest {

    private static final SpannerDatabase DATABASE =
            SpannerDatabase.of("my-project", "my-instance", "my-db");

    @Test
    void anEmulatorEndpointAlsoTurnsOffCredentialsAndTransportSecurity() {
        // One call does all three, which is why nothing else here configures the emulator — and
        // why every test touching a production path can point at a closed port safely.
        SpannerOptions settings =
                SpannerClients.settings(DATABASE, EmulatorEndpoint.parse("localhost:9010"));

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
}
