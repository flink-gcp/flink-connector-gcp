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
import com.google.bigtable.admin.v2.InstanceName;
import com.google.bigtable.admin.v2.ListTablesRequest;
import com.google.bigtable.admin.v2.Table;
import com.google.bigtable.admin.v2.TableName;
import com.google.bigtable.admin.v2.Type;
import com.google.cloud.bigtable.admin.v2.BaseBigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClientV2;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.bigtable.BigtableClients;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link BigtableCatalogClient} over one instance's {@link BigtableTableAdminClientV2}.
 *
 * <p>The client runs on the connector's gRPC event-loop group and is held, with its lease, from the
 * catalog's first metadata request until the catalog closes (ADR-0208), as every connector catalog
 * holds its client (ADR-0168). The SQL Gateway and the SQL Client close a session's catalogs before
 * its class loader (Flink 1.20.5 and 2.2.1), so this catalog's lease is released before that loader
 * closes. In a program's own {@code TableEnvironment}, the client stays open until the catalog
 * instance is closed, whoever closes it: the program itself, or Flink when it drops the catalog or,
 * during an {@code ALTER CATALOG}, before it opens the replacement; a catalog left open keeps its
 * client for as long as the process runs.
 *
 * <p>Both requests are protobuf requests that name the instance or table in full, never the model
 * types of the {@code @ObsoleteApi} wrapper client: tables are listed with {@code NAME_ONLY}, and a
 * table's families are read with {@code SCHEMA_VIEW}, as the sink's table admin reads them, so a
 * family whose value type the SDK's model classes do not know still reaches the catalog's mapping
 * rather than failing inside the client. The client's settings carry no project or instance; the
 * requests do.
 */
@Internal
final class BigtableServiceCatalogClient implements BigtableCatalogClient {

    /** The admin client and its event-loop lease, released when the catalog closes this. */
    private final GrpcEventLoops.LeasedClient<BigtableTableAdminClientV2> client;

    private final String project;
    private final String instance;

    private BigtableServiceCatalogClient(
            GrpcEventLoops.LeasedClient<BigtableTableAdminClientV2> client,
            String project,
            String instance) {
        this.client = client;
        this.project = project;
        this.instance = instance;
    }

    /**
     * Opens the admin client of one instance, on the connector's gRPC event-loop group.
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
                BigtableClients.openTableAdmin(
                        BigtableClients.tableAdminSettings(emulatorEndpoint, credentials),
                        emulatorEndpoint),
                project,
                instance);
    }

    @VisibleForTesting
    static BaseBigtableTableAdminSettings settings(
            @Nullable EmulatorEndpoint emulatorEndpoint, @Nullable CredentialsProvider credentials)
            throws IOException {
        return BigtableClients.tableAdminSettings(emulatorEndpoint, credentials).build();
    }

    @Override
    public List<String> listTables() {
        List<String> tables = new ArrayList<>();
        for (Table table :
                client.client().listTables(listTablesRequest(project, instance)).iterateAll()) {
            tables.add(TableName.parse(table.getName()).getTable());
        }
        return tables;
    }

    /** The listing request: the instance's tables, by name only. */
    @VisibleForTesting
    static ListTablesRequest listTablesRequest(String project, String instance) {
        return ListTablesRequest.newBuilder()
                .setParent(InstanceName.of(project, instance).toString())
                .setView(Table.View.NAME_ONLY)
                .build();
    }

    @Nullable
    @Override
    public Map<String, Type> columnFamilies(String table) {
        Table found;
        try {
            found = client.client().getTable(tableRequest(project, instance, table));
        } catch (NotFoundException e) {
            return null;
        }
        Map<String, Type> families = new LinkedHashMap<>();
        found.getColumnFamiliesMap()
                .forEach((name, family) -> families.put(name, family.getValueType()));
        return families;
    }

    /** The read of one table's families: its schema view. */
    @VisibleForTesting
    static GetTableRequest tableRequest(String project, String instance, String table) {
        return GetTableRequest.newBuilder()
                .setName(TableName.of(project, instance, table).toString())
                .setView(Table.View.SCHEMA_VIEW)
                .build();
    }

    /** Closes the admin client and releases its lease on the event-loop group. */
    @Override
    public void close() {
        client.close();
    }
}
