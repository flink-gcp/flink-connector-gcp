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

package io.github.flink.gcp.connector.bigtable.sink.singlerow;

import org.apache.flink.annotation.Internal;

import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.RowFilter;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.options.ResourceNames;
import io.github.flink.gcp.connector.bigtable.TableDestination;

import java.io.IOException;
import java.security.SecureRandom;

/** Immutable, validated row-atomic request whose marker identity survives checkpoint recovery. */
@Internal
public final class BigtableCommittable {
    private final CheckAndMutateRowRequest request;
    private final TableDestination destination;
    private final String markerFamily;

    /** Validates both the request structure and the marker-protection contract. */
    public BigtableCommittable(CheckAndMutateRowRequest request) throws IOException {
        this.request = request;
        String[] path = request.getTableName().split("/", -1);
        try {
            if (path.length != 6
                    || !path[0].equals("projects")
                    || !path[2].equals("instances")
                    || !path[4].equals("tables")) {
                throw new IllegalArgumentException("Invalid table resource");
            }
            destination = TableDestination.of(path[1], path[3], path[5]);
            ResourceNames.checkComponent(request.getAppProfileId(), "appProfileId");
        } catch (IllegalArgumentException | NullPointerException failure) {
            throw new IOException("Invalid staged destination or application profile", failure);
        }
        if (request.getRowKey().isEmpty()
                || request.getTrueMutationsCount() != 0
                || request.getFalseMutationsCount() < 2
                || request.getFalseMutationsCount() > 100_000) {
            throw new IOException("Invalid staged envelope structure");
        }
        Mutation marker = request.getFalseMutations(request.getFalseMutationsCount() - 1);
        markerFamily = marker.getSetCell().getFamilyName();
        ByteString identity = marker.getSetCell().getColumnQualifier();
        if (!marker.hasSetCell()
                || markerFamily.isBlank()
                || !identity.toStringUtf8().matches("[0-9a-f]{32}")
                || marker.getSetCell().getTimestampMicros() != 0
                || !marker.getSetCell().getValue().equals(ByteString.copyFromUtf8("1"))
                || !request.getPredicateFilter().equals(predicate(markerFamily, identity))) {
            throw new IOException("Invalid staged marker or predicate");
        }
        for (int i = 0; i < request.getFalseMutationsCount() - 1; i++) {
            validateUserMutation(request.getFalseMutations(i), markerFamily);
        }
    }

    /** Freezes a serialized input and creates its random identity exactly once. */
    public static BigtableCommittable stage(
            TableDestination destination,
            String profile,
            String family,
            MutateRowsRequest.Entry entry,
            SecureRandom random)
            throws IOException {
        if (entry.getMutationsCount() == 0 || entry.getMutationsCount() > 99_999) {
            throw new IOException("A staged record requires 1..99,999 mutations plus its marker");
        }
        for (Mutation mutation : entry.getMutationsList()) {
            validateUserMutation(mutation, family);
        }
        byte[] bytes = new byte[16];
        random.nextBytes(bytes);
        ByteString identity = ByteString.copyFromUtf8(java.util.HexFormat.of().formatHex(bytes));
        Mutation marker =
                Mutation.newBuilder()
                        .setSetCell(
                                Mutation.SetCell.newBuilder()
                                        .setFamilyName(family)
                                        .setColumnQualifier(identity)
                                        .setTimestampMicros(0)
                                        .setValue(ByteString.copyFromUtf8("1")))
                        .build();
        return new BigtableCommittable(
                CheckAndMutateRowRequest.newBuilder()
                        .setTableName(
                                "projects/"
                                        + destination.getProject()
                                        + "/instances/"
                                        + destination.getInstance()
                                        + "/tables/"
                                        + destination.getTable())
                        .setAppProfileId(profile)
                        .setRowKey(entry.getRowKey())
                        .setPredicateFilter(predicate(family, identity))
                        .addAllFalseMutations(entry.getMutationsList())
                        .addFalseMutations(marker)
                        .build());
    }

    private static RowFilter predicate(String family, ByteString identity) {
        return Filters.FILTERS
                .chain()
                .filter(Filters.FILTERS.family().exactMatch(family))
                .filter(Filters.FILTERS.qualifier().exactMatch(identity))
                .toProto();
    }

    private static void validateUserMutation(Mutation mutation, String reserved)
            throws IOException {
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
                throw new IOException(
                        "Staged mutations must preserve row markers; whole-row deletes are unsupported");
        }
        if (family.equals(reserved)) {
            throw new IOException("User mutation addresses reserved marker family " + reserved);
        }
    }

    /** Returns the immutable conditional request, including destination and original profile. */
    public CheckAndMutateRowRequest getRequest() {
        return request;
    }

    /** Returns the original resolved destination. */
    public TableDestination getDestination() {
        return destination;
    }

    /** Returns the family protected by this envelope's marker. */
    public String getMarkerFamily() {
        return markerFamily;
    }

    /** Returns the writer admission charge, not a Java heap-size measurement. */
    public long getStagedBytes() {
        return request.getSerializedSize() + 256L;
    }
}
