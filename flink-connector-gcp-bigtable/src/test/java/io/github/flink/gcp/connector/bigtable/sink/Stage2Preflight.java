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

package io.github.flink.gcp.connector.bigtable.sink;

import com.google.bigtable.admin.v2.AppProfile;
import com.google.bigtable.admin.v2.ColumnFamily;
import com.google.bigtable.admin.v2.GetAppProfileRequest;
import com.google.bigtable.admin.v2.GetTableRequest;
import com.google.bigtable.admin.v2.Table;
import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.Mutation;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import io.github.flink.gcp.connector.bigtable.TableDestination;

import java.io.IOException;

/**
 * Metadata checks for the experimental transport, including restored sends before operator open.
 */
final class Stage2Preflight {
    private Stage2Preflight() {}

    static void read(TableDestination destination, String profile) throws IOException {
        try (BigtableInstanceAdminClient instances =
                        BigtableInstanceAdminClient.create(destination.getProject());
                BigtableTableAdminClient tables =
                        BigtableTableAdminClient.create(
                                destination.getProject(), destination.getInstance())) {
            AppProfile actual =
                    instances
                            .getBaseClient()
                            .getAppProfile(
                                    GetAppProfileRequest.newBuilder()
                                            .setName(
                                                    "projects/"
                                                            + destination.getProject()
                                                            + "/instances/"
                                                            + destination.getInstance()
                                                            + "/appProfiles/"
                                                            + profile)
                                            .build());
            Table table =
                    tables.getBaseClient()
                            .getTable(
                                    GetTableRequest.newBuilder()
                                            .setName(LocalStagedHarness.tableName(destination))
                                            .setView(Table.View.SCHEMA_VIEW)
                                            .build());
            validate(actual, table);
        } catch (RuntimeException failure) {
            throw new IOException(
                    "Stage 2 metadata validation failed before target writes", failure);
        }
    }

    static void validate(AppProfile profile, Table table) throws IOException {
        if (!profile.hasSingleClusterRouting()
                || !profile.getSingleClusterRouting().getAllowTransactionalWrites()
                || profile.getSingleClusterRouting().getClusterId().isEmpty()) {
            throw new IOException(
                    "Stage 2 requires single-cluster routing with transactional writes");
        }
        ColumnFamily family =
                table.getColumnFamiliesMap().get(StagedMutationTestSink.MARKER_FAMILY);
        if (family == null
                || (family.hasGcRule()
                        && !family.getGcRule()
                                .equals(com.google.bigtable.admin.v2.GcRule.getDefaultInstance()))
                || !family.getValueType()
                        .equals(com.google.bigtable.admin.v2.Type.getDefaultInstance())) {
            throw new IOException(
                    "Stage 2 requires an existing raw marker family without a GC rule");
        }
    }

    static void envelope(CheckAndMutateRowRequest request) throws IOException {
        if (!request.getTrueMutationsList().isEmpty()
                || request.getFalseMutationsCount() < 2
                || request.getFalseMutationsCount() > 100_000
                || request.getTableName().isEmpty()
                || request.getAppProfileId().isEmpty()
                || request.getRowKey().isEmpty()) {
            throw new IOException("Invalid staged envelope structure");
        }
        Mutation marker = request.getFalseMutations(request.getFalseMutationsCount() - 1);
        String identity = marker.getSetCell().getColumnQualifier().toStringUtf8();
        if (!marker.hasSetCell()
                || !identity.matches("[0-9a-f]{32}")
                || !marker.getSetCell().getFamilyName().equals(StagedMutationTestSink.MARKER_FAMILY)
                || marker.getSetCell().getTimestampMicros() != 0
                || !marker.getSetCell().getValue().toStringUtf8().equals("1")) {
            throw new IOException("Invalid staged marker");
        }
        com.google.bigtable.v2.RowFilter expected =
                com.google.bigtable.v2.RowFilter.newBuilder()
                        .setChain(
                                com.google.bigtable.v2.RowFilter.Chain.newBuilder()
                                        .addFilters(
                                                com.google.bigtable.v2.RowFilter.newBuilder()
                                                        .setFamilyNameRegexFilter(
                                                                StagedMutationTestSink
                                                                        .MARKER_FAMILY))
                                        .addFilters(
                                                com.google.bigtable.v2.RowFilter.newBuilder()
                                                        .setColumnQualifierRegexFilter(
                                                                marker.getSetCell()
                                                                        .getColumnQualifier())))
                        .build();
        if (!request.getPredicateFilter().equals(expected)) {
            throw new IOException("Staged predicate does not protect its marker");
        }
        for (Mutation mutation :
                request.getFalseMutationsList().subList(0, request.getFalseMutationsCount() - 1)) {
            String family;
            switch (mutation.getMutationCase()) {
                case SET_CELL:
                    family = mutation.getSetCell().getFamilyName();
                    break;
                case ADD_TO_CELL:
                    family = mutation.getAddToCell().getFamilyName();
                    break;
                case MERGE_TO_CELL:
                    family = mutation.getMergeToCell().getFamilyName();
                    break;
                case DELETE_FROM_COLUMN:
                    family = mutation.getDeleteFromColumn().getFamilyName();
                    break;
                case DELETE_FROM_FAMILY:
                    family = mutation.getDeleteFromFamily().getFamilyName();
                    break;
                default:
                    throw new IOException("Staged envelope must preserve old markers");
            }
            if (family.equals(StagedMutationTestSink.MARKER_FAMILY)) {
                throw new IOException("User mutation addresses the marker family");
            }
        }
    }
}
