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

import com.google.bigtable.admin.v2.ColumnFamily;
import com.google.bigtable.admin.v2.CreateTableRequest;
import com.google.bigtable.admin.v2.GetTableRequest;
import com.google.bigtable.admin.v2.InstanceName;
import com.google.bigtable.admin.v2.Table;
import com.google.bigtable.admin.v2.TableName;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.BigtableAdminProtos;
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
import java.util.HashMap;
import java.util.Map;

import static io.github.flink.gcp.connector.bigtable.BigtableAdminProtos.maxVersions;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link BigtableTableAdmin} against the emulator, whose table admin surface
 * — creation, family readback with garbage-collection rules intact, {@code ALREADY_EXISTS} on a
 * repeated creation and on an existing family's re-addition — was measured to behave like the
 * service's for what this class exercises (2026-08-08 against {@code
 * google-cloud-cli:441.0.0-emulators}, unchanged when re-measured 2026-09-03 against {@code
 * google-cloud-cli:583.0.0-emulators}; {@link BigtableEmulatorContainers} pins the image). The
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
                                EMULATOR.getHost() + ":" + EMULATOR.getEmulatorPort(),
                                "emulatorEndpoint"));
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
                        com.google.bigtable.admin.v2.GcRule.newBuilder()
                                .setUnion(
                                        com.google.bigtable.admin.v2.GcRule.Union.newBuilder()
                                                .addRules(maxVersions(1))
                                                .addRules(maxAge(Duration.ofDays(7))))
                                .build());
    }

    @Test
    void addsOnlyTheMissingFamiliesAndNeverTouchesAnExistingRule() throws Exception {
        createTable("ensure-amends", "existing", BigtableAdminProtos.maxVersionsFamily(3));
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
        assertThat(families.get("existing")).isEqualTo(maxVersions(3));
        assertThat(families.get("added")).isEqualTo(maxAge(Duration.ofHours(24)));
    }

    @Test
    void isANoOpWhenTheTableAndEveryFamilyExist() throws Exception {
        // The lost-race shape: another subtask created exactly this table first, and the loser's
        // ensure must succeed silently without modifying anything.
        createTable("ensure-noop", "cf", BigtableAdminProtos.rawFamily());
        TableDestination table = TableDestination.of(PROJECT, INSTANCE, "ensure-noop");

        TableAdmin.EnsureResult result =
                admin.ensureTable(table, TableCreateOptions.builder().columnFamily("cf").build());

        assertThat(result.tableCreated()).isFalse();
        assertThat(result.columnFamiliesAdded()).isZero();
        assertThat(familiesOf("ensure-noop").keySet()).containsExactly("cf");
    }

    private static void createTable(String tableId, String family, ColumnFamily definition) {
        verification
                .getBaseClient()
                .createTable(
                        CreateTableRequest.newBuilder()
                                .setParent(InstanceName.of(PROJECT, INSTANCE).toString())
                                .setTableId(tableId)
                                .setTable(Table.newBuilder().putColumnFamilies(family, definition))
                                .build());
    }

    private static Map<String, com.google.bigtable.admin.v2.GcRule> familiesOf(String tableId) {
        Map<String, com.google.bigtable.admin.v2.GcRule> rules = new HashMap<>();
        verification
                .getBaseClient()
                .getTable(
                        GetTableRequest.newBuilder()
                                .setName(TableName.of(PROJECT, INSTANCE, tableId).toString())
                                .setView(Table.View.SCHEMA_VIEW)
                                .build())
                .getColumnFamiliesMap()
                .forEach((name, family) -> rules.put(name, family.getGcRule()));
        return rules;
    }

    private static com.google.bigtable.admin.v2.GcRule maxAge(Duration age) {
        return com.google.bigtable.admin.v2.GcRule.newBuilder()
                .setMaxAge(com.google.protobuf.Duration.newBuilder().setSeconds(age.getSeconds()))
                .build();
    }
}
