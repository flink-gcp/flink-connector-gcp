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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.rpc.NotFoundException;
import com.google.bigtable.admin.v2.GetTableRequest;
import com.google.bigtable.admin.v2.Table;
import com.google.bigtable.admin.v2.TableName;
import com.google.bigtable.admin.v2.Type;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link BigtableCatalogClient} over one instance's {@link BigtableTableAdminClient}.
 *
 * <p>A table's families are read through the protobuf client with {@code SCHEMA_VIEW}, as the
 * sink's table admin reads them, so a family whose value type the client's model classes do not
 * know still reaches the catalog's mapping rather than failing inside the client.
 */
@Internal
final class BigtableServiceCatalogClient implements BigtableCatalogClient {

    private final BigtableTableAdminClient client;
    private final String project;
    private final String instance;

    BigtableServiceCatalogClient(BigtableTableAdminClient client, String project, String instance) {
        this.client = client;
        this.project = project;
        this.instance = instance;
    }

    /**
     * Opens the admin client of one instance.
     *
     * @param project the project that owns the instance
     * @param instance the instance
     * @param emulatorEndpoint the emulator's endpoint, or {@code null}
     * @param credentials the credentials, or {@code null} for ADC
     * @return the catalog client
     * @throws IOException if the client cannot be created
     */
    static BigtableServiceCatalogClient open(
            String project,
            String instance,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            @Nullable CredentialsProvider credentials)
            throws IOException {
        return new BigtableServiceCatalogClient(
                BigtableTableAdminClient.create(
                        settings(project, instance, emulatorEndpoint, credentials)),
                project,
                instance);
    }

    @VisibleForTesting
    static BigtableTableAdminSettings settings(
            String project,
            String instance,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            @Nullable CredentialsProvider credentials)
            throws IOException {
        BigtableTableAdminSettings.Builder settings =
                emulatorEndpoint == null
                        ? BigtableTableAdminSettings.newBuilder()
                        : BigtableTableAdminSettings.newBuilderForEmulator(
                                emulatorEndpoint.getHost(), emulatorEndpoint.getPort());
        if (credentials != null) {
            settings.setCredentialsProvider(credentials);
        }
        return settings.setProjectId(project).setInstanceId(instance).build();
    }

    @Override
    public List<String> listTables() {
        return client.listTables();
    }

    @Nullable
    @Override
    public Map<String, Type> columnFamilies(String table) {
        Table found;
        try {
            found =
                    client.getBaseClient()
                            .getTable(
                                    GetTableRequest.newBuilder()
                                            .setName(
                                                    TableName.of(project, instance, table)
                                                            .toString())
                                            .setView(Table.View.SCHEMA_VIEW)
                                            .build());
        } catch (NotFoundException e) {
            return null;
        }
        Map<String, Type> families = new LinkedHashMap<>();
        found.getColumnFamiliesMap()
                .forEach((name, family) -> families.put(name, family.getValueType()));
        return families;
    }

    @Override
    public void close() {
        client.close();
    }
}
