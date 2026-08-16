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

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.models.ColumnFamily;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.GCRules;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.GcRule;
import io.github.flink.gcp.connector.bigtable.sink.TableCreateOptions;
import io.github.flink.gcp.connector.testutils.bigtable.BigtableEmulatorContainers;
import io.github.flink.gcp.connector.testutils.bigtable.BigtableTestClients;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.BigtableEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link BigtableTableAdmin} against the emulator, whose table admin surface
 * — creation, family readback with garbage-collection rules intact, {@code ALREADY_EXISTS} on a
 * repeated creation and on an existing family's re-addition — was measured to behave like the
 * service's for what this class exercises (2026-08-08, against {@code
 * google-cloud-cli:441.0.0-emulators}; {@link BigtableEmulatorContainers} pins the image). The
 * emulator stays a convenience, not an authority: the gated real-GCP suite owns the service-side
 * verdicts.
 *
 * <p>Not shared with {@code AbstractBigtableEmulatorITCase}: that harness lives in the writer's
 * package with package-private access, and this class needs only a container and one verification
 * client.
 */
@Testcontainers
@Timeout(180)
class BigtableTableAdminEmulatorITCase {

    private static final String PROJECT = "it-project";
    private static final String INSTANCE = "it-instance";

    @Container
    private static final BigtableEmulatorContainer EMULATOR =
            BigtableEmulatorContainers.newContainer();

    /** Harness-owned client for preparing tables and reading the results back. */
    private static BigtableTableAdminClient verification;

    private static BigtableTableAdmin admin;

    @BeforeAll
    static void startClients() throws IOException {
        verification = BigtableTestClients.adminClient(EMULATOR, PROJECT, INSTANCE);
        admin =
                new BigtableTableAdmin(
                        EmulatorEndpoint.parse(
                                EMULATOR.getHost() + ":" + EMULATOR.getEmulatorPort()));
    }

    @AfterAll
    static void stopClients() throws Exception {
        if (verification != null) {
            verification.close();
        }
        if (admin != null) {
            admin.close();
        }
    }

    @Test
    void createsAnAbsentTableWithEveryDeclaredFamilyAndRule() throws Exception {
        TableDestination table = TableDestination.of(PROJECT, INSTANCE, "ensure-creates");

        TableAdmin.EnsureResult result =
                admin.ensureTable(
                        table,
                        TableCreateOptions.builder()
                                .columnFamily("plain")
                                .columnFamily(
                                        "kept",
                                        GcRule.union(
                                                GcRule.maxVersions(1),
                                                GcRule.maxAge(Duration.ofDays(7))))
                                .build());

        assertThat(result.tableCreated()).isTrue();
        assertThat(result.columnFamiliesAdded()).isZero();
        Map<String, com.google.bigtable.admin.v2.GcRule> families = familiesOf("ensure-creates");
        assertThat(families.keySet()).containsExactlyInAnyOrder("plain", "kept");
        assertThat(families.get("plain"))
                .isEqualTo(com.google.bigtable.admin.v2.GcRule.getDefaultInstance());
        assertThat(families.get("kept"))
                .isEqualTo(
                        GCRules.GCRULES
                                .union()
                                .rule(GCRules.GCRULES.maxVersions(1))
                                .rule(GCRules.GCRULES.maxAge(org.threeten.bp.Duration.ofDays(7)))
                                .toProto());
    }

    @Test
    void addsOnlyTheMissingFamiliesAndNeverTouchesAnExistingRule() throws Exception {
        verification.createTable(
                CreateTableRequest.of("ensure-amends")
                        .addFamily("existing", GCRules.GCRULES.maxVersions(3)));
        TableDestination table = TableDestination.of(PROJECT, INSTANCE, "ensure-amends");

        // The declared rule for the existing family disagrees on purpose: creation-only
        // semantics mean the live rule must win by never being compared or updated.
        TableAdmin.EnsureResult result =
                admin.ensureTable(
                        table,
                        TableCreateOptions.builder()
                                .columnFamily("existing", GcRule.maxVersions(9))
                                .columnFamily("added", GcRule.maxAge(Duration.ofHours(24)))
                                .build());

        assertThat(result.tableCreated()).isFalse();
        assertThat(result.columnFamiliesAdded()).isEqualTo(1);
        Map<String, com.google.bigtable.admin.v2.GcRule> families = familiesOf("ensure-amends");
        assertThat(families.get("existing")).isEqualTo(GCRules.GCRULES.maxVersions(3).toProto());
        assertThat(families.get("added"))
                .isEqualTo(GCRules.GCRULES.maxAge(org.threeten.bp.Duration.ofHours(24)).toProto());
    }

    @Test
    void isANoOpWhenTheTableAndEveryFamilyExist() throws Exception {
        // The lost-race shape: another subtask created exactly this table first, and the loser's
        // ensure must succeed silently without modifying anything.
        verification.createTable(CreateTableRequest.of("ensure-noop").addFamily("cf"));
        TableDestination table = TableDestination.of(PROJECT, INSTANCE, "ensure-noop");

        TableAdmin.EnsureResult result =
                admin.ensureTable(table, TableCreateOptions.builder().columnFamily("cf").build());

        assertThat(result.tableCreated()).isFalse();
        assertThat(result.columnFamiliesAdded()).isZero();
        assertThat(familiesOf("ensure-noop").keySet()).containsExactly("cf");
    }

    private static Map<String, com.google.bigtable.admin.v2.GcRule> familiesOf(String tableId) {
        return verification.getTable(tableId).getColumnFamilies().stream()
                .collect(
                        Collectors.toMap(
                                ColumnFamily::getId, family -> family.getGCRule().toProto()));
    }
}
