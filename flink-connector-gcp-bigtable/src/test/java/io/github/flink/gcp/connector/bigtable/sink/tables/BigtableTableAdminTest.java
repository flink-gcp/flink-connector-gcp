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

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.GCRules;
import com.google.cloud.bigtable.admin.v2.models.ModifyColumnFamiliesRequest;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.GcRule;
import io.github.flink.gcp.connector.bigtable.sink.TableCreateOptions;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link BigtableTableAdmin}'s translation of the sink's serializable creation settings
 * into the client's request models — compared through {@code toProto} so the assertion is on what
 * would go on the wire — and for the ensure flow those requests are issued by, driven through the
 * {@link BigtableTableAdmin#ensureWith} seam so the rounds a lost family race produces can be
 * scripted. What that seam leaves to the emulator (a real client behind the three operations, and
 * the family readback the middle one projects) is {@code BigtableTableAdminEmulatorITCase}'s.
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

    @Test
    void createsTheTableAndReconcilesNothingWhenTheCreationWins() {
        ScriptedAdmin admin = ScriptedAdmin.absentTable();

        TableAdmin.EnsureResult result = admin.ensure(optionsDeclaring("plain"));

        assertThat(result.tableCreated()).isTrue();
        assertThat(result.columnFamiliesAdded()).isZero();
        assertThat(admin.creations).hasSize(1);
        assertThat(admin.readTableIds).isEmpty();
        assertThat(admin.modifications).isEmpty();
    }

    @Test
    void addsOnlyTheAbsenteesWhenTheCreationLosesTheRace() {
        ScriptedAdmin admin = ScriptedAdmin.existingTable(0, List.of(Set.of("existing")));

        TableAdmin.EnsureResult result = admin.ensure(optionsDeclaring("existing", "added"));

        assertThat(result.tableCreated()).isFalse();
        assertThat(result.columnFamiliesAdded()).isEqualTo(1);
        assertThat(admin.creations).hasSize(1);
        assertThat(admin.readTableIds).containsExactly("orders");
        assertThat(admin.modifications)
                .extracting(request -> request.toProto("p", "i"))
                .containsExactly(additionOf("added"));
    }

    @Test
    void reReadsAndRetriesOnlyTheRemainderWhenAFamilyIsAddedConcurrently() {
        // Round one reads a bare table and is told, atomically, that the families it is adding
        // already exist: a parallel subtask added "a" between this call's read and its modify, and
        // one existing family fails the whole request. Round two has to re-read rather than resend
        // — and report as added only what it actually added.
        ScriptedAdmin admin = ScriptedAdmin.existingTable(1, List.of(Set.of(), Set.of("a")));

        TableAdmin.EnsureResult result = admin.ensure(optionsDeclaring("a", "b"));

        assertThat(result.tableCreated()).isFalse();
        assertThat(result.columnFamiliesAdded()).isEqualTo(1);
        assertThat(admin.readTableIds).containsExactly("orders", "orders");
        assertThat(admin.modifications)
                .extracting(request -> request.toProto("p", "i"))
                .containsExactly(additionOf("a", "b"), additionOf("b"));
    }

    @Test
    void addsNothingWhenEveryDeclaredFamilyAlreadyExists() {
        ScriptedAdmin admin = ScriptedAdmin.existingTable(0, List.of(Set.of("a", "b")));

        TableAdmin.EnsureResult result = admin.ensure(optionsDeclaring("a", "b"));

        assertThat(result.tableCreated()).isFalse();
        assertThat(result.columnFamiliesAdded()).isZero();
        assertThat(admin.modifications).isEmpty();
    }

    @Test
    void spendsEveryRoundTheDeclaredFamiliesBuyBeforeGivingUp() {
        // The worst case that is not a contradiction: each round loses the race to exactly one of
        // the two declared families, so the third round — the last the budget buys — is the one
        // that finds nothing left to add. A budget one smaller would give up on a table that is
        // already exactly right.
        ScriptedAdmin admin =
                ScriptedAdmin.existingTable(2, List.of(Set.of(), Set.of("a"), Set.of("a", "b")));

        TableAdmin.EnsureResult result = admin.ensure(optionsDeclaring("a", "b"));

        assertThat(result.columnFamiliesAdded()).isZero();
        assertThat(admin.readTableIds).hasSize(3);
        assertThat(admin.modifications).hasSize(2);
    }

    @Test
    void failsInsteadOfSpinningWhenTheDeclaredFamiliesKeepDisappearing() {
        // "a" is added concurrently, and from there "b" is reported as already existing by every
        // addition while every read still says it is absent — a churn no single concurrent
        // addition explains. The loop gives up after the rounds two declared families buy, because
        // a loop with no end holds the task thread while the failure is a retry the writer's
        // recovery schedule spends. The message names "b" and not the whole declared set: which
        // family is churning is the one thing the operator meeting this can act on.
        ScriptedAdmin admin =
                ScriptedAdmin.existingTable(3, List.of(Set.of(), Set.of("a"), Set.of("a")));

        assertThatThrownBy(() -> admin.ensure(optionsDeclaring("a", "b")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("column families [b] declared for Bigtable table p.i.orders")
                .hasMessageContaining("after 3 reconciliation rounds");
        assertThat(admin.modifications).hasSize(3);
    }

    /** Options declaring the given families, none of them carrying a rule. */
    private static TableCreateOptions optionsDeclaring(String... families) {
        TableCreateOptions.Builder options = TableCreateOptions.builder();
        for (String family : families) {
            options.columnFamily(family);
        }
        return options.build();
    }

    /** The wire form of a request adding exactly the given families to the test's table. */
    private static com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest additionOf(
            String... families) {
        ModifyColumnFamiliesRequest request = ModifyColumnFamiliesRequest.of("orders");
        for (String family : families) {
            request.addFamily(family);
        }
        return request.toProto("p", "i");
    }

    /**
     * A scripted stand-in for the three admin operations {@link BigtableTableAdmin#ensureWith}
     * drives: it records every request it is handed, answers each read from the script's next
     * entry, and rejects the first {@code modificationsThatLoseTheRace} additions the way a
     * concurrent one does.
     */
    private static final class ScriptedAdmin {

        private final boolean creationLosesTheRace;
        private final int modificationsThatLoseTheRace;
        private final List<Set<String>> familiesPerRound;

        private final List<CreateTableRequest> creations = new ArrayList<>();
        private final List<String> readTableIds = new ArrayList<>();
        private final List<ModifyColumnFamiliesRequest> modifications = new ArrayList<>();

        private ScriptedAdmin(
                boolean creationLosesTheRace,
                int modificationsThatLoseTheRace,
                List<Set<String>> familiesPerRound) {
            this.creationLosesTheRace = creationLosesTheRace;
            this.modificationsThatLoseTheRace = modificationsThatLoseTheRace;
            this.familiesPerRound = familiesPerRound;
        }

        /** An admin whose table is absent, so the creation wins and nothing is reconciled. */
        static ScriptedAdmin absentTable() {
            return new ScriptedAdmin(false, 0, List.of());
        }

        /**
         * An admin whose table already exists, so every call reconciles: one entry of {@code
         * familiesPerRound} answers each round's read, and the first {@code
         * modificationsThatLoseTheRace} additions are answered as already existing.
         */
        static ScriptedAdmin existingTable(
                int modificationsThatLoseTheRace, List<Set<String>> familiesPerRound) {
            return new ScriptedAdmin(true, modificationsThatLoseTheRace, familiesPerRound);
        }

        TableAdmin.EnsureResult ensure(TableCreateOptions options) {
            return BigtableTableAdmin.ensureWith(
                    TABLE, options, this::createTable, this::readFamilyIds, this::modifyFamilies);
        }

        private void createTable(CreateTableRequest request) {
            creations.add(request);
            if (creationLosesTheRace) {
                throw alreadyExists();
            }
        }

        private Set<String> readFamilyIds(String tableId) {
            readTableIds.add(tableId);
            assertThat(familiesPerRound)
                    .as(
                            "the reconciliation reached round %d, which the script does not cover",
                            readTableIds.size())
                    .hasSizeGreaterThanOrEqualTo(readTableIds.size());
            return familiesPerRound.get(readTableIds.size() - 1);
        }

        private void modifyFamilies(ModifyColumnFamiliesRequest request) {
            modifications.add(request);
            if (modifications.size() <= modificationsThatLoseTheRace) {
                throw alreadyExists();
            }
        }

        /** The rejection a concurrent creation of the same table or family draws. */
        private static AlreadyExistsException alreadyExists() {
            return (AlreadyExistsException)
                    ApiExceptionFactory.createException(
                            new RuntimeException("scripted ALREADY_EXISTS"),
                            GrpcStatusCode.of(Status.Code.ALREADY_EXISTS),
                            false);
        }
    }
}
