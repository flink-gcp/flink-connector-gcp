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
import com.google.bigtable.admin.v2.Table;
import com.google.cloud.bigtable.admin.v2.BaseBigtableTableAdminSettings;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** The settings and requests the catalog's client works with; no client is created. */
class BigtableServiceCatalogClientTest {

    /** Without this, the catalog's metadata requests would go out under ADC, not the key file. */
    @Test
    void injectsTheLoadedCredentials() throws Exception {
        NoCredentialsProvider provider = NoCredentialsProvider.create();

        BaseBigtableTableAdminSettings settings =
                BigtableServiceCatalogClient.settings(null, provider);

        assertThat(settings.getCredentialsProvider()).isSameAs(provider);
    }

    @Test
    void anEmulatorEndpointCarriesNoCredentials() throws Exception {
        BaseBigtableTableAdminSettings settings =
                BigtableServiceCatalogClient.settings(
                        EmulatorEndpoint.parse("localhost:8086", "emulator-endpoint"), null);

        assertThat(settings.getCredentialsProvider()).isInstanceOf(NoCredentialsProvider.class);
    }

    /** The client's settings carry no project or instance, so the requests name both. */
    @Test
    void theRequestsNameTheInstanceAndTable() {
        assertThat(BigtableServiceCatalogClient.listTablesRequest("proj", "inst").getParent())
                .isEqualTo("projects/proj/instances/inst");
        assertThat(BigtableServiceCatalogClient.listTablesRequest("proj", "inst").getView())
                .isEqualTo(Table.View.NAME_ONLY);
        assertThat(BigtableServiceCatalogClient.tableRequest("proj", "inst", "orders").getName())
                .isEqualTo("projects/proj/instances/inst/tables/orders");
        assertThat(BigtableServiceCatalogClient.tableRequest("proj", "inst", "orders").getView())
                .isEqualTo(Table.View.SCHEMA_VIEW);
    }
}
