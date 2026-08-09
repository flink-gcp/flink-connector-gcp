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

package io.github.flink.gcp.connector.bigtable.sink.tables;

import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.GCRules;
import com.google.cloud.bigtable.admin.v2.models.ModifyColumnFamiliesRequest;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.GcRule;
import io.github.flink.gcp.connector.bigtable.sink.TableCreateOptions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link BigtableTableAdmin}'s translation of the sink's serializable creation settings
 * into the client's request models, compared through {@code toProto} so the assertion is on what
 * would go on the wire.
 */
class BigtableTableAdminTest {

    private static final TableDestination TABLE = TableDestination.of("p", "i", "orders");

    @Test
    void translatesEveryFamilyIntoTheCreationRequest() {
        TableCreateOptions options =
                TableCreateOptions.builder()
                        .columnFamily("plain")
                        .columnFamily("versions", GcRule.maxVersions(3))
                        .columnFamily("aged", GcRule.maxAge(Duration.ofHours(24)))
                        .build();

        CreateTableRequest expected =
                CreateTableRequest.of("orders")
                        .addFamily("plain")
                        .addFamily("versions", GCRules.GCRULES.maxVersions(3))
                        .addFamily(
                                "aged",
                                GCRules.GCRULES.maxAge(org.threeten.bp.Duration.ofHours(24)));

        assertThat(BigtableTableAdmin.toCreateTableRequest(TABLE, options).toProto("p", "i"))
                .isEqualTo(expected.toProto("p", "i"));
    }

    @Test
    void translatesNestedRulesAndSubSecondAges() {
        // Seconds-and-nanos to seconds-and-nanos: the age must arrive exact, not truncated to a
        // coarser unit on the way through the client's threeten type.
        TableCreateOptions options =
                TableCreateOptions.builder()
                        .columnFamily(
                                "kept",
                                GcRule.union(
                                        GcRule.maxVersions(1),
                                        GcRule.intersection(
                                                GcRule.maxAge(Duration.ofSeconds(5, 123)),
                                                GcRule.maxVersions(10))))
                        .build();

        CreateTableRequest expected =
                CreateTableRequest.of("orders")
                        .addFamily(
                                "kept",
                                GCRules.GCRULES
                                        .union()
                                        .rule(GCRules.GCRULES.maxVersions(1))
                                        .rule(
                                                GCRules.GCRULES
                                                        .intersection()
                                                        .rule(
                                                                GCRules.GCRULES.maxAge(
                                                                        org.threeten.bp.Duration
                                                                                .ofSeconds(5, 123)))
                                                        .rule(GCRules.GCRULES.maxVersions(10))));

        assertThat(BigtableTableAdmin.toCreateTableRequest(TABLE, options).toProto("p", "i"))
                .isEqualTo(expected.toProto("p", "i"));
    }

    @Test
    void translatesTheMissingFamiliesIntoOneModificationRequest() {
        Map<String, GcRule> missing = new LinkedHashMap<>();
        missing.put("plain", null);
        missing.put("versions", GcRule.maxVersions(2));

        ModifyColumnFamiliesRequest expected =
                ModifyColumnFamiliesRequest.of("orders")
                        .addFamily("plain")
                        .addFamily("versions", GCRules.GCRULES.maxVersions(2));

        assertThat(
                        BigtableTableAdmin.toModifyColumnFamiliesRequest(TABLE, missing)
                                .toProto("p", "i"))
                .isEqualTo(expected.toProto("p", "i"));
    }
}
