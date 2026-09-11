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

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.bigtable.admin.v2.AppProfile;
import com.google.bigtable.admin.v2.ColumnFamily;
import com.google.bigtable.admin.v2.GcRule;
import com.google.bigtable.admin.v2.Table;
import com.google.bigtable.admin.v2.Type;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.ColumnFamilyType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BigtableStagedTableValidatorTest {
    private static final TableDestination DESTINATION = TableDestination.of("p", "i", "t");
    private static final AppProfile PROFILE =
            AppProfile.newBuilder()
                    .setSingleClusterRouting(
                            AppProfile.SingleClusterRouting.newBuilder()
                                    .setClusterId("c")
                                    .setAllowTransactionalWrites(true))
                    .build();
    private static final Table TABLE =
            Table.newBuilder()
                    .putColumnFamilies("markers", ColumnFamily.getDefaultInstance())
                    .putColumnFamilies("data", ColumnFamily.getDefaultInstance())
                    .build();

    @Test
    void checksRawMetadataIncludingAnEmptyButExplicitGcRule() throws Exception {
        BigtableStagedTableValidator.validateMetadata(
                DESTINATION, PROFILE, TABLE, "markers", Map.of("data", ColumnFamilyType.RAW));
        for (var marker :
                List.of(
                        ColumnFamily.newBuilder()
                                .setGcRule(GcRule.newBuilder().setMaxNumVersions(1))
                                .build(),
                        ColumnFamily.newBuilder()
                                .setGcRule(
                                        GcRule.newBuilder()
                                                .setUnion(GcRule.Union.getDefaultInstance()))
                                .build(),
                        ColumnFamily.newBuilder()
                                .setValueType(
                                        Type.newBuilder()
                                                .setInt64Type(Type.Int64.getDefaultInstance()))
                                .build())) {
            assertThatThrownBy(
                            () ->
                                    BigtableStagedTableValidator.validateMetadata(
                                            DESTINATION,
                                            PROFILE,
                                            TABLE.toBuilder()
                                                    .putColumnFamilies("markers", marker)
                                                    .build(),
                                            "markers",
                                            Map.of()))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("raw marker family without a GC rule");
        }
        assertThatThrownBy(
                        () ->
                                BigtableStagedTableValidator.validateMetadata(
                                        DESTINATION,
                                        PROFILE,
                                        TABLE.toBuilder().removeColumnFamilies("markers").build(),
                                        "markers",
                                        Map.of()))
                .isInstanceOf(IOException.class);
    }

    @Test
    void rejectsMultiClusterOrNonTransactionalRoutingAndWrongDataTypes() {
        for (var profile :
                List.of(
                        AppProfile.getDefaultInstance(),
                        AppProfile.newBuilder()
                                .setMultiClusterRoutingUseAny(
                                        AppProfile.MultiClusterRoutingUseAny.getDefaultInstance())
                                .build(),
                        PROFILE.toBuilder()
                                .setSingleClusterRouting(
                                        PROFILE.getSingleClusterRouting().toBuilder()
                                                .setAllowTransactionalWrites(false))
                                .build(),
                        PROFILE.toBuilder()
                                .setSingleClusterRouting(
                                        PROFILE.getSingleClusterRouting().toBuilder()
                                                .clearClusterId())
                                .build())) {
            assertThatThrownBy(
                            () ->
                                    BigtableStagedTableValidator.validateMetadata(
                                            DESTINATION, profile, TABLE, "markers", Map.of()))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("transactional");
        }
        assertThatThrownBy(
                        () ->
                                BigtableStagedTableValidator.validateMetadata(
                                        DESTINATION,
                                        PROFILE,
                                        TABLE,
                                        "markers",
                                        Map.of("data", ColumnFamilyType.INT64_SUM)))
                .isInstanceOf(ColumnFamilyTypes.Mismatch.class);
    }

    @Test
    void metadataClientsShareRuntimeCredentialsAndUseTheEmulatorEndpoint() throws Exception {
        NoCredentialsProvider credentials = NoCredentialsProvider.create();
        var validator =
                new BigtableStagedTableValidator(
                        EmulatorEndpoint.parse("localhost:1234", "test"), credentials);
        assertThat(validator.instanceSettings(DESTINATION).getStubSettings().getEndpoint())
                .isEqualTo("localhost:1234");
        assertThat(validator.tableSettings(DESTINATION).getStubSettings().getEndpoint())
                .isEqualTo("localhost:1234");
        assertThat(validator.instanceSettings(DESTINATION).getCredentialsProvider())
                .isSameAs(credentials);
        assertThat(validator.tableSettings(DESTINATION).getCredentialsProvider())
                .isSameAs(credentials);
    }
}
