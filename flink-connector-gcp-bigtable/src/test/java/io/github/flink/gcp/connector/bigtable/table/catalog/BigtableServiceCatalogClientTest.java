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

package io.github.flink.gcp.connector.bigtable.table.catalog;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** The admin settings the catalog's client opens with; no client is created. */
class BigtableServiceCatalogClientTest {

    /** Without this, the catalog's metadata requests would go out under ADC, not the key file. */
    @Test
    void injectsTheLoadedCredentialsAndNamesTheInstance() throws Exception {
        NoCredentialsProvider provider = NoCredentialsProvider.create();

        BigtableTableAdminSettings settings =
                BigtableServiceCatalogClient.settings("proj", "inst", null, provider);

        assertThat(settings.getCredentialsProvider()).isSameAs(provider);
        assertThat(settings.getProjectId()).isEqualTo("proj");
        assertThat(settings.getInstanceId()).isEqualTo("inst");
    }

    @Test
    void anEmulatorEndpointSelectsTheEmulatorChannel() throws Exception {
        BigtableTableAdminSettings settings =
                BigtableServiceCatalogClient.settings(
                        "proj",
                        "inst",
                        EmulatorEndpoint.parse("localhost:8086", "emulator-endpoint"),
                        null);

        assertThat(settings.getStubSettings().getEndpoint()).isEqualTo("localhost:8086");
        assertThat(settings.getCredentialsProvider()).isInstanceOf(NoCredentialsProvider.class);
    }
}
