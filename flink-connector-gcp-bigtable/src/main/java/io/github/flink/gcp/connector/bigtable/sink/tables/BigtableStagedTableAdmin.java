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

package io.github.flink.gcp.connector.bigtable.sink.tables;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import com.google.api.gax.core.CredentialsProvider;
import com.google.bigtable.admin.v2.AppProfile;
import com.google.bigtable.admin.v2.ColumnFamily;
import com.google.bigtable.admin.v2.GetAppProfileRequest;
import com.google.bigtable.admin.v2.GetTableRequest;
import com.google.bigtable.admin.v2.Table;
import com.google.cloud.bigtable.admin.v2.BaseBigtableInstanceAdminSettings;
import com.google.cloud.bigtable.admin.v2.BaseBigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminClientV2;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClientV2;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.bigtable.BigtableClients;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.ColumnFamilyType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** Short-lived metadata clients sharing the runtime's credentials; never changes remote policy. */
@Internal
public final class BigtableStagedTableAdmin implements StagedTableAdmin {
    @Nullable private final EmulatorEndpoint endpoint;
    @Nullable private final CredentialsProvider credentials;

    /** Uses the same endpoint and runtime-loaded credential provider as the conditional client. */
    public BigtableStagedTableAdmin(
            @Nullable EmulatorEndpoint endpoint, @Nullable CredentialsProvider credentials) {
        this.endpoint = endpoint;
        this.credentials = credentials;
    }

    @Override
    public void validate(
            TableDestination destination,
            String profile,
            String markerFamily,
            Map<String, ColumnFamilyType> expectedFamilies)
            throws IOException {
        // Each client holds its own lease on the event-loop group (ADR-0208), released after it
        // closes; the try closes the table client first, then the instance client.
        try (GrpcEventLoops.LeasedClient<BigtableInstanceAdminClientV2> leasedInstances =
                        BigtableClients.openInstanceAdmin(
                                BigtableClients.instanceAdminSettings(endpoint, credentials),
                                endpoint);
                GrpcEventLoops.LeasedClient<BigtableTableAdminClientV2> leasedTables =
                        BigtableClients.openTableAdmin(
                                BigtableClients.tableAdminSettings(endpoint, credentials),
                                endpoint)) {
            BigtableInstanceAdminClientV2 instances = leasedInstances.client();
            BigtableTableAdminClientV2 tables = leasedTables.client();
            String instance =
                    "projects/"
                            + destination.getProject()
                            + "/instances/"
                            + destination.getInstance();
            AppProfile actual =
                    instances.getAppProfile(
                            GetAppProfileRequest.newBuilder()
                                    .setName(instance + "/appProfiles/" + profile)
                                    .build());
            Table table =
                    tables.getTable(
                            GetTableRequest.newBuilder()
                                    .setName(instance + "/tables/" + destination.getTable())
                                    .setView(Table.View.SCHEMA_VIEW)
                                    .build());
            validateMetadata(destination, actual, table, markerFamily, expectedFamilies);
        } catch (RuntimeException failure) {
            throw new IOException(
                    "Bigtable staged metadata validation failed before target writes: "
                            + destination,
                    failure);
        }
    }

    @VisibleForTesting
    BaseBigtableInstanceAdminSettings instanceSettings() throws IOException {
        return BigtableClients.instanceAdminSettings(endpoint, credentials).build();
    }

    @VisibleForTesting
    BaseBigtableTableAdminSettings tableSettings() throws IOException {
        return BigtableClients.tableAdminSettings(endpoint, credentials).build();
    }

    static void validateMetadata(
            TableDestination destination,
            AppProfile profile,
            Table table,
            String markerFamily,
            Map<String, ColumnFamilyType> expectedFamilies)
            throws IOException {
        if (!profile.hasSingleClusterRouting()
                || !profile.getSingleClusterRouting().getAllowTransactionalWrites()
                || profile.getSingleClusterRouting().getClusterId().isEmpty()) {
            throw new IOException(
                    "Bigtable EXACTLY_ONCE requires single-cluster routing with transactional writes");
        }
        ColumnFamily marker = table.getColumnFamiliesMap().get(markerFamily);
        if (marker == null
                || !marker.getGcRule()
                        .equals(com.google.bigtable.admin.v2.GcRule.getDefaultInstance())
                || !marker.getValueType()
                        .equals(com.google.bigtable.admin.v2.Type.getDefaultInstance())) {
            throw new IOException(
                    "Bigtable EXACTLY_ONCE requires an existing raw marker family without a GC rule: "
                            + markerFamily);
        }
        Map<String, com.google.bigtable.admin.v2.Type> types = new HashMap<>();
        table.getColumnFamiliesMap()
                .forEach((name, family) -> types.put(name, family.getValueType()));
        for (String family : expectedFamilies.keySet()) {
            if (!types.containsKey(family)) {
                throw new IOException(
                        "Bigtable staged table "
                                + destination
                                + " is missing declared data family "
                                + family);
            }
        }
        // Raw declarations do not require existing application families to be untyped.
        // Aggregate declarations opt into checking the complete declared schema.
        if (expectedFamilies.values().stream().anyMatch(type -> type != ColumnFamilyType.RAW)) {
            ColumnFamilyTypes.check(destination, expectedFamilies, types, false);
        } else {
            for (String family : expectedFamilies.keySet()) {
                if (types.get(family).hasAggregateType()) {
                    throw new IOException(
                            "Bigtable non-aggregate staged Table writes cannot use aggregate data family "
                                    + family
                                    + " in "
                                    + destination);
                }
            }
        }
    }
}
