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

import com.google.api.gax.core.CredentialsProvider;
import com.google.bigtable.admin.v2.AppProfile;
import com.google.bigtable.admin.v2.ColumnFamily;
import com.google.bigtable.admin.v2.GetAppProfileRequest;
import com.google.bigtable.admin.v2.GetTableRequest;
import com.google.bigtable.admin.v2.Table;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminSettings;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.ColumnFamilyType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** Short-lived metadata clients sharing the runtime's credentials; never changes remote policy. */
@Internal
public final class BigtableStagedTableValidator implements StagedTableValidator {
    @Nullable private final EmulatorEndpoint endpoint;
    @Nullable private final CredentialsProvider credentials;

    /** Uses the same endpoint and runtime-loaded credential provider as the conditional client. */
    public BigtableStagedTableValidator(
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
        try (BigtableInstanceAdminClient instances =
                        BigtableInstanceAdminClient.create(instanceSettings(destination));
                BigtableTableAdminClient tables =
                        BigtableTableAdminClient.create(tableSettings(destination))) {
            String instance =
                    "projects/"
                            + destination.getProject()
                            + "/instances/"
                            + destination.getInstance();
            AppProfile actual =
                    instances
                            .getBaseClient()
                            .getAppProfile(
                                    GetAppProfileRequest.newBuilder()
                                            .setName(instance + "/appProfiles/" + profile)
                                            .build());
            Table table =
                    tables.getBaseClient()
                            .getTable(
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

    BigtableInstanceAdminSettings instanceSettings(TableDestination destination)
            throws IOException {
        var builder =
                endpoint == null
                        ? BigtableInstanceAdminSettings.newBuilder()
                        : BigtableInstanceAdminSettings.newBuilderForEmulator(
                                endpoint.getHost(), endpoint.getPort());
        builder.setProjectId(destination.getProject());
        if (credentials != null) {
            builder.setCredentialsProvider(credentials);
        }
        return builder.build();
    }

    BigtableTableAdminSettings tableSettings(TableDestination destination) throws IOException {
        var builder =
                endpoint == null
                        ? BigtableTableAdminSettings.newBuilder()
                        : BigtableTableAdminSettings.newBuilderForEmulator(
                                endpoint.getHost(), endpoint.getPort());
        builder.setProjectId(destination.getProject()).setInstanceId(destination.getInstance());
        if (credentials != null) {
            builder.setCredentialsProvider(credentials);
        }
        return builder.build();
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
