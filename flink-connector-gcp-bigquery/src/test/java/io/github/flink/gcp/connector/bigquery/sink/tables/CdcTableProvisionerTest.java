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

package io.github.flink.gcp.connector.bigquery.sink.tables;

import com.google.cloud.bigquery.storage.v1.TableSchema;
import io.github.flink.gcp.connector.bigquery.sink.CdcTableOptions;
import io.github.flink.gcp.connector.bigquery.sink.CdcTableReconciliationPolicy;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link CdcTableProvisioner}. */
class CdcTableProvisionerTest {

    private static final TableDestination DESTINATION = TableDestination.of("p", "d", "t");
    private static final TableSchema SCHEMA = TableSchema.getDefaultInstance();
    private static final Duration TEN_MINUTES = Duration.ofMinutes(10);

    @Test
    void createIfNeededCreatesAMissingTable() throws Exception {
        FakeService service = new FakeService();
        CdcTableOptions options = options(TEN_MINUTES);

        boolean requested =
                ensure(
                        service,
                        options,
                        CreateDisposition.CREATE_IF_NEEDED,
                        CdcTableReconciliationPolicy.VERIFY_ONLY);

        assertThat(requested).isTrue();
        assertThat(service.creates).isOne();
        assertThat(service.setMaxStaleness).containsExactly(TEN_MINUTES);
        assertThat(service.state.provisioningLabel()).startsWith("complete_");
    }

    @Test
    void createConflictWithAnExternalTableUsesTheExistingTablePolicy() throws Exception {
        FakeService service = new FakeService();
        service.createWins = false;
        service.stateAfterCreateConflict =
                serviceState(Arrays.asList("id", "tenant"), null, "external-etag");
        CdcTableOptions options =
                CdcTableOptions.builder().primaryKeyColumns(Arrays.asList("id", "tenant")).build();

        boolean requested =
                ensure(
                        service,
                        options,
                        CreateDisposition.CREATE_IF_NEEDED,
                        CdcTableReconciliationPolicy.VERIFY_ONLY);

        assertThat(requested).isTrue();
        assertThat(service.creates).isOne();
        assertThat(service.labelUpdates).isEmpty();
        assertThat(service.state.provisioningLabel()).isNull();
    }

    @Test
    void creationOutcomeSurvivesAFailureAfterTablesInsert() {
        FakeService service = new FakeService();
        service.setMaxStalenessFailure = new IOException("DDL failed");

        assertThatThrownBy(
                        () ->
                                ensure(
                                        service,
                                        options(TEN_MINUTES),
                                        CreateDisposition.CREATE_IF_NEEDED,
                                        CdcTableReconciliationPolicy.VERIFY_ONLY))
                .isInstanceOfSatisfying(
                        TableAdminException.class,
                        failure -> assertThat(failure.wasCreationRequested()).isTrue())
                .hasMessageContaining("DDL failed");
    }

    @Test
    void creationOutcomeSurvivesARetriableFailureAfterTablesInsert() {
        FakeService service = new FakeService();
        service.setMaxStalenessFailure =
                new RetriableTableAdminException("option not visible yet", null);

        assertThatThrownBy(
                        () ->
                                ensure(
                                        service,
                                        options(TEN_MINUTES),
                                        CreateDisposition.CREATE_IF_NEEDED,
                                        CdcTableReconciliationPolicy.VERIFY_ONLY))
                .isInstanceOfSatisfying(
                        RetriableTableAdminException.class,
                        failure -> assertThat(failure.wasCreationRequested()).isTrue())
                .hasMessageContaining("option not visible yet");
    }

    @Test
    void newlyCreatedExplicitClearIsVerifiedAndConvergedBeforeCompletion() throws Exception {
        FakeService service = new FakeService();
        service.liveMaxStaleness = TEN_MINUTES;
        CdcTableOptions clear =
                CdcTableOptions.builder()
                        .primaryKeyColumns(Arrays.asList("id", "tenant"))
                        .clearMaxStaleness()
                        .build();

        ensure(
                service,
                clear,
                CreateDisposition.CREATE_IF_NEEDED,
                CdcTableReconciliationPolicy.VERIFY_ONLY);

        assertThat(service.setMaxStaleness).containsExactly((Duration) null);
        assertThat(service.state.provisioningLabel()).isEqualTo(complete(clear));
    }

    @Test
    void createNeverRejectsAMissingTableWithoutCreatingIt() {
        FakeService service = new FakeService();

        assertThatThrownBy(
                        () ->
                                ensure(
                                        service,
                                        options(TEN_MINUTES),
                                        CreateDisposition.CREATE_NEVER,
                                        CdcTableReconciliationPolicy.RECONCILE))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("CREATE_NEVER");
        assertThat(service.creates).isZero();
    }

    @Test
    void missingTableRequiresADeclaredPrimaryKeyOnlyWhenCreationIsNeeded() {
        FakeService service = new FakeService();

        assertThatThrownBy(
                        () ->
                                ensure(
                                        service,
                                        CdcTableOptions.builder().build(),
                                        CreateDisposition.CREATE_IF_NEEDED,
                                        CdcTableReconciliationPolicy.VERIFY_ONLY))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("primaryKeyColumns");
    }

    @Test
    void verifyOnlyAdoptsTheLivePrimaryKeyWithoutChangingAnUnlabeledTable() throws Exception {
        FakeService service = existing(serviceState(Arrays.asList("id", "tenant"), null, "e1"));

        boolean requested =
                ensure(
                        service,
                        CdcTableOptions.builder().build(),
                        CreateDisposition.CREATE_NEVER,
                        CdcTableReconciliationPolicy.VERIFY_ONLY);

        assertThat(requested).isFalse();
        assertThat(service.labelUpdates).isEmpty();
        assertThat(service.setMaxStaleness).isEmpty();
    }

    @Test
    void existingTableDoesNotResolvePhysicalCreationOptions() throws Exception {
        FakeService service = existing(serviceState(Arrays.asList("id", "tenant"), null, "e1"));
        AtomicInteger providerCalls = new AtomicInteger();

        new CdcTableProvisioner(service)
                .ensure(
                        DESTINATION,
                        SCHEMA,
                        destination -> {
                            providerCalls.incrementAndGet();
                            return TableCreateOptions.builder().build();
                        },
                        CdcTableOptions.builder().build(),
                        CreateDisposition.CREATE_IF_NEEDED,
                        CdcTableReconciliationPolicy.VERIFY_ONLY);

        assertThat(providerCalls).hasValue(0);
    }

    @Test
    void verifyOnlyFailsOnConfiguredMaximumStalenessDrift() {
        FakeService service = existing(serviceState(Arrays.asList("id", "tenant"), null, "e1"));

        assertThatThrownBy(
                        () ->
                                ensure(
                                        service,
                                        options(TEN_MINUTES),
                                        CreateDisposition.CREATE_IF_NEEDED,
                                        CdcTableReconciliationPolicy.VERIFY_ONLY))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("verify-only");
        assertThat(service.labelUpdates).isEmpty();
    }

    @Test
    void reconcileAdoptsAndConvergesAnUnlabeledTable() throws Exception {
        FakeService service = existing(serviceState(Arrays.asList("id", "tenant"), null, "e1"));

        ensure(
                service,
                options(TEN_MINUTES),
                CreateDisposition.CREATE_NEVER,
                CdcTableReconciliationPolicy.RECONCILE);

        assertThat(service.labelUpdates).hasSize(2);
        assertThat(service.labelUpdates.get(0)).startsWith("null->pending_");
        assertThat(service.labelUpdates.get(1)).contains("->complete_");
        assertThat(service.setMaxStaleness).containsExactly(TEN_MINUTES);
    }

    @Test
    void reconcileAdoptsAnUnlabeledTableWithoutManagingMaximumStaleness() throws Exception {
        CdcTableOptions options =
                CdcTableOptions.builder().primaryKeyColumns(Arrays.asList("id", "tenant")).build();
        FakeService service = existing(serviceState(Arrays.asList("id", "tenant"), null, "e1"));

        ensure(
                service,
                options,
                CreateDisposition.CREATE_NEVER,
                CdcTableReconciliationPolicy.RECONCILE);

        assertThat(service.labelUpdates).containsExactly("null->" + complete(options));
        assertThat(service.maxStalenessChecks).isZero();
        assertThat(service.setMaxStaleness).isEmpty();
    }

    @Test
    void reconcileRepairsDriftBehindACompleteLabel() throws Exception {
        CdcTableOptions options = options(TEN_MINUTES);
        String complete = complete(options);
        FakeService service = existing(serviceState(Arrays.asList("id", "tenant"), complete, "e1"));

        ensure(
                service,
                options,
                CreateDisposition.CREATE_IF_NEEDED,
                CdcTableReconciliationPolicy.RECONCILE);

        assertThat(service.labelUpdates.get(0)).startsWith(complete + "->pending_");
        assertThat(service.setMaxStaleness).containsExactly(TEN_MINUTES);
        assertThat(service.state.provisioningLabel()).isEqualTo(complete);
    }

    @Test
    void reconcileMigratesACompletedOlderSpecification() throws Exception {
        CdcTableOptions oldOptions = options(Duration.ofMinutes(5));
        CdcTableOptions newOptions = options(TEN_MINUTES);
        FakeService service =
                existing(serviceState(Arrays.asList("id", "tenant"), complete(oldOptions), "e1"));
        service.liveMaxStaleness = Duration.ofMinutes(5);

        ensure(
                service,
                newOptions,
                CreateDisposition.CREATE_IF_NEEDED,
                CdcTableReconciliationPolicy.RECONCILE);

        assertThat(service.liveMaxStaleness).isEqualTo(TEN_MINUTES);
        assertThat(service.state.provisioningLabel()).isEqualTo(complete(newOptions));
    }

    @Test
    void matchingPendingStateResumesEvenUnderVerifyOnly() throws Exception {
        CdcTableOptions options = options(TEN_MINUTES);
        FakeService service =
                existing(serviceState(Arrays.asList("id", "tenant"), pending(options), "e1"));

        ensure(
                service,
                options,
                CreateDisposition.CREATE_NEVER,
                CdcTableReconciliationPolicy.VERIFY_ONLY);

        assertThat(service.setMaxStaleness).containsExactly(TEN_MINUTES);
        assertThat(service.state.provisioningLabel()).isEqualTo(complete(options));
    }

    @Test
    void differentPendingSpecificationIsNeverTakenOver() {
        CdcTableOptions options = options(TEN_MINUTES);
        FakeService service =
                existing(
                        serviceState(
                                Arrays.asList("id", "tenant"),
                                "pending_00000000000000000000000000000000",
                                "e1"));

        assertThatThrownBy(
                        () ->
                                ensure(
                                        service,
                                        options,
                                        CreateDisposition.CREATE_IF_NEEDED,
                                        CdcTableReconciliationPolicy.RECONCILE))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("in-progress");
        assertThat(service.labelUpdates).isEmpty();
    }

    @Test
    void reconcileCanExplicitlyClearMaximumStaleness() throws Exception {
        CdcTableOptions options =
                CdcTableOptions.builder()
                        .primaryKeyColumns(Arrays.asList("id", "tenant"))
                        .clearMaxStaleness()
                        .build();
        FakeService service = existing(serviceState(Arrays.asList("id", "tenant"), null, "e1"));
        service.liveMaxStaleness = TEN_MINUTES;

        ensure(
                service,
                options,
                CreateDisposition.CREATE_NEVER,
                CdcTableReconciliationPolicy.RECONCILE);

        assertThat(service.setMaxStaleness).containsExactly((Duration) null);
        assertThat(service.liveMaxStaleness).isNull();
        assertThat(service.state.provisioningLabel()).isEqualTo(complete(options));
    }

    @Test
    void reconcileRequiresAnAuthoritativePrimaryKey() {
        FakeService service = existing(serviceState(Arrays.asList("id", "tenant"), null, "e1"));

        assertThatThrownBy(
                        () ->
                                ensure(
                                        service,
                                        CdcTableOptions.builder().build(),
                                        CreateDisposition.CREATE_NEVER,
                                        CdcTableReconciliationPolicy.RECONCILE))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("primaryKeyColumns");
    }

    @Test
    void primaryKeyDriftIsNeverRepaired() {
        FakeService service = existing(serviceState(Arrays.asList("other"), null, "e1"));

        assertThatThrownBy(
                        () ->
                                ensure(
                                        service,
                                        options(TEN_MINUTES),
                                        CreateDisposition.CREATE_IF_NEEDED,
                                        CdcTableReconciliationPolicy.RECONCILE))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("primary key");
        assertThat(service.labelUpdates).isEmpty();
    }

    @Test
    void primaryKeyIsRecheckedAfterMaximumStalenessDdl() {
        CdcTableOptions options = options(TEN_MINUTES);
        FakeService service =
                existing(serviceState(Arrays.asList("id", "tenant"), pending(options), "e1"));
        service.primaryKeyAfterDdl = Arrays.asList("other");

        assertThatThrownBy(
                        () ->
                                ensure(
                                        service,
                                        options,
                                        CreateDisposition.CREATE_IF_NEEDED,
                                        CdcTableReconciliationPolicy.RECONCILE))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("primary key");
        assertThat(service.state.provisioningLabel()).isEqualTo(pending(options));
    }

    @Test
    void lostEtagRaceIsRetriableBeforeReconciliationMutatesTheOption() {
        FakeService service = existing(serviceState(Arrays.asList("id", "tenant"), null, "e1"));
        service.rejectLabelUpdates = true;

        assertThatThrownBy(
                        () ->
                                ensure(
                                        service,
                                        options(TEN_MINUTES),
                                        CreateDisposition.CREATE_NEVER,
                                        CdcTableReconciliationPolicy.RECONCILE))
                .isInstanceOf(RetriableTableAdminException.class)
                .hasMessageContaining("concurrent metadata update");
        assertThat(service.setMaxStaleness).isEmpty();
    }

    @Test
    void specificationHashIsCaseInsensitiveForPrimaryKeyColumns() {
        CdcTableOptions options = options(TEN_MINUTES);

        assertThat(CdcTableProvisioner.specificationHash(Arrays.asList("ID", "Tenant"), options))
                .isEqualTo(
                        CdcTableProvisioner.specificationHash(
                                Arrays.asList("id", "tenant"), options));
    }

    @Test
    void specificationHashDistinguishesEveryManagedContractDimension() {
        CdcTableOptions tenMinutes = options(TEN_MINUTES);
        CdcTableOptions fiveMinutes = options(Duration.ofMinutes(5));
        CdcTableOptions clear =
                CdcTableOptions.builder()
                        .primaryKeyColumns(Arrays.asList("id", "tenant"))
                        .clearMaxStaleness()
                        .build();
        CdcTableOptions unmanaged =
                CdcTableOptions.builder().primaryKeyColumns(Arrays.asList("id", "tenant")).build();

        assertThat(CdcTableProvisioner.specificationHash(Arrays.asList("id", "tenant"), tenMinutes))
                .isNotEqualTo(
                        CdcTableProvisioner.specificationHash(
                                Arrays.asList("id", "tenant"), fiveMinutes))
                .isNotEqualTo(
                        CdcTableProvisioner.specificationHash(Arrays.asList("id", "tenant"), clear))
                .isNotEqualTo(
                        CdcTableProvisioner.specificationHash(
                                Arrays.asList("id", "tenant"), unmanaged))
                .isNotEqualTo(
                        CdcTableProvisioner.specificationHash(
                                Arrays.asList("tenant", "id"), tenMinutes));
        assertThat(CdcTableProvisioner.specificationHash(Arrays.asList("id", "tenant"), clear))
                .isNotEqualTo(
                        CdcTableProvisioner.specificationHash(
                                Arrays.asList("id", "tenant"), unmanaged));
    }

    @Test
    void aCreatedTableTheTablesApiCannotSeeYetIsRetriedRatherThanFailed() {
        // tables.insert answered, tables.get has not caught up. Repeating the whole ensure is what
        // resolves that, so the failure has to say so — and it must still report that this attempt
        // requested the creation, or the caller stops treating the destination as new.
        FakeService service = new FakeService();
        service.disappearsAfterCreate = true;

        assertThatThrownBy(
                        () ->
                                ensure(
                                        service,
                                        options(TEN_MINUTES),
                                        CreateDisposition.CREATE_IF_NEEDED,
                                        CdcTableReconciliationPolicy.VERIFY_ONLY))
                .isInstanceOfSatisfying(
                        RetriableTableAdminException.class,
                        failure -> assertThat(failure.wasCreationRequested()).isTrue())
                .hasMessageContaining("not visible through the Tables API yet");
    }

    @Test
    void aTableCreatedUnderSomeoneElsesLabelIsNeverAdopted() {
        // Two subtasks racing to create the same table: this one's insert won, but the label read
        // back is not the one it wrote, so another writer is provisioning a different contract.
        FakeService service = new FakeService();
        service.labelAfterCreate = "complete_someone_elses_spec";
        CdcTableOptions unmanaged =
                CdcTableOptions.builder().primaryKeyColumns(Arrays.asList("id", "tenant")).build();

        assertThatThrownBy(
                        () ->
                                ensure(
                                        service,
                                        unmanaged,
                                        CreateDisposition.CREATE_IF_NEEDED,
                                        CdcTableReconciliationPolicy.VERIFY_ONLY))
                .isInstanceOf(IOException.class)
                .isNotInstanceOf(RetriableTableAdminException.class)
                .hasMessageContaining("unexpected provisioning label");
        assertThat(service.labelUpdates).isEmpty();
    }

    @Test
    void verifyOnlyRejectsATableLabelledForADifferentSpecification() {
        FakeService service =
                existing(serviceState(Arrays.asList("id", "tenant"), "complete_older_spec", "e1"));
        service.liveMaxStaleness = TEN_MINUTES;

        assertThatThrownBy(
                        () ->
                                ensure(
                                        service,
                                        options(TEN_MINUTES),
                                        CreateDisposition.CREATE_IF_NEEDED,
                                        CdcTableReconciliationPolicy.VERIFY_ONLY))
                .isInstanceOf(IOException.class)
                .isNotInstanceOf(RetriableTableAdminException.class)
                .hasMessageContaining("does not match the configured CDC table specification");
        assertThat(service.labelUpdates).isEmpty();
        assertThat(service.setMaxStaleness).isEmpty();
    }

    @Test
    void aClaimAnotherWriterOverwroteBeforeTheDdlIsRetriable() {
        // The claim succeeded but the label read back afterwards is not the pending one this
        // provisioner wrote, so the table is no longer ours to run DDL against.
        FakeService service = existing(serviceState(Arrays.asList("id", "tenant"), null, "e1"));
        service.labelAfterLabelUpdate = "pending_someone_elses_spec";

        assertThatThrownBy(
                        () ->
                                ensure(
                                        service,
                                        options(TEN_MINUTES),
                                        CreateDisposition.CREATE_IF_NEEDED,
                                        CdcTableReconciliationPolicy.RECONCILE))
                .isInstanceOf(RetriableTableAdminException.class)
                .hasMessageContaining(
                        "changed provisioning state after CDC reconciliation was" + " claimed");
        assertThat(service.setMaxStaleness).isEmpty();
    }

    @Test
    void aLabelChangedWhileTheDdlRanStopsTheCompletion() {
        FakeService service =
                existing(
                        serviceState(
                                Arrays.asList("id", "tenant"),
                                pending(options(TEN_MINUTES)),
                                "e1"));
        service.labelAfterDdl = "complete_someone_elses_spec";

        assertThatThrownBy(
                        () ->
                                ensure(
                                        service,
                                        options(TEN_MINUTES),
                                        CreateDisposition.CREATE_IF_NEEDED,
                                        CdcTableReconciliationPolicy.RECONCILE))
                .isInstanceOf(IOException.class)
                .isNotInstanceOf(RetriableTableAdminException.class)
                .hasMessageContaining(
                        "changed provisioning label while max_staleness was being" + " reconciled");
        assertThat(service.setMaxStaleness).containsExactly(TEN_MINUTES);
    }

    @Test
    void anAcceptedDdlInformationSchemaCannotSeeYetIsRetriable() {
        // ADR-0112: REST is never the oracle for max_staleness, INFORMATION_SCHEMA is — and it
        // lags. Completing the label on an unverified option is exactly what must not happen.
        FakeService service =
                existing(
                        serviceState(
                                Arrays.asList("id", "tenant"),
                                pending(options(TEN_MINUTES)),
                                "e1"));
        service.maxStalenessNeverConverges = true;

        assertThatThrownBy(
                        () ->
                                ensure(
                                        service,
                                        options(TEN_MINUTES),
                                        CreateDisposition.CREATE_IF_NEEDED,
                                        CdcTableReconciliationPolicy.RECONCILE))
                .isInstanceOf(RetriableTableAdminException.class)
                .hasMessageContaining("INFORMATION_SCHEMA does not expose the desired state yet");
        assertThat(service.labelUpdates).isEmpty();
    }

    @Test
    void aCompletionAnotherAttemptAlreadyWonIsNotRepeated() {
        // Two attempts resume the same pending label: this one runs the DDL and finds the label
        // already moved to complete by the other. Claiming it again would fail the expected-label
        // precondition it no longer satisfies, turning a converged table into a retry loop.
        FakeService service =
                existing(
                        serviceState(
                                Arrays.asList("id", "tenant"),
                                pending(options(TEN_MINUTES)),
                                "e1"));
        service.labelAfterDdl = complete(options(TEN_MINUTES));

        assertThatCode(
                        () ->
                                ensure(
                                        service,
                                        options(TEN_MINUTES),
                                        CreateDisposition.CREATE_IF_NEEDED,
                                        CdcTableReconciliationPolicy.RECONCILE))
                .doesNotThrowAnyException();
        assertThat(service.setMaxStaleness).containsExactly(TEN_MINUTES);
        assertThat(service.labelUpdates).isEmpty();
    }

    @Test
    void aTableThatDisappearedDuringReconciliationIsRetriable() {
        FakeService service =
                existing(
                        serviceState(
                                Arrays.asList("id", "tenant"),
                                pending(options(TEN_MINUTES)),
                                "e1"));
        service.disappearsAfterDdl = true;

        assertThatThrownBy(
                        () ->
                                ensure(
                                        service,
                                        options(TEN_MINUTES),
                                        CreateDisposition.CREATE_IF_NEEDED,
                                        CdcTableReconciliationPolicy.RECONCILE))
                .isInstanceOf(RetriableTableAdminException.class)
                .hasMessageContaining("disappeared after max_staleness was reconciled");
    }

    @Test
    void anExistingTableWithoutAPrimaryKeyIsNotAdoptedAsACdcTable() {
        // A plain table under a CDC destination: BigQuery's upsert semantics need a primary key,
        // and nothing here adds one to an existing table.
        FakeService service = existing(serviceState(Collections.emptyList(), null, "e1"));

        assertThatThrownBy(
                        () ->
                                ensure(
                                        service,
                                        options(TEN_MINUTES),
                                        CreateDisposition.CREATE_IF_NEEDED,
                                        CdcTableReconciliationPolicy.VERIFY_ONLY))
                .isInstanceOf(IOException.class)
                .isNotInstanceOf(RetriableTableAdminException.class)
                .hasMessageContaining("has no primary key");
        assertThat(service.labelUpdates).isEmpty();
    }

    // The three below pin the *false* direction of the creation-request outcome. Every other
    // assertion on it in this module reads isTrue(), so a regression that reported an ordinary
    // provisioning run as a creation would have inflated the tablesCreated metric with the whole
    // suite still green.

    @Test
    void anExistingTableAlreadyAtTheSpecificationRequestsNoCreation() throws Exception {
        CdcTableOptions options = options(TEN_MINUTES);
        FakeService service =
                existing(serviceState(Arrays.asList("id", "tenant"), complete(options), "e1"));
        service.liveMaxStaleness = TEN_MINUTES;

        boolean requested =
                ensure(
                        service,
                        options,
                        CreateDisposition.CREATE_IF_NEEDED,
                        CdcTableReconciliationPolicy.RECONCILE);

        assertThat(requested).isFalse();
        assertThat(service.creates).isZero();
    }

    @Test
    void aCreateNeverRefusalRequestsNoCreation() {
        FakeService service = new FakeService();

        assertThatThrownBy(
                        () ->
                                ensure(
                                        service,
                                        options(TEN_MINUTES),
                                        CreateDisposition.CREATE_NEVER,
                                        CdcTableReconciliationPolicy.VERIFY_ONLY))
                .isInstanceOf(IOException.class)
                .isNotInstanceOf(TableAdminException.class)
                .hasMessageContaining("CREATE_NEVER");
        assertThat(service.creates).isZero();
    }

    @Test
    void aRefusalToCreateWithoutADeclaredPrimaryKeyRequestsNoCreation() {
        FakeService service = new FakeService();

        assertThatThrownBy(
                        () ->
                                ensure(
                                        service,
                                        CdcTableOptions.builder().maxStaleness(TEN_MINUTES).build(),
                                        CreateDisposition.CREATE_IF_NEEDED,
                                        CdcTableReconciliationPolicy.VERIFY_ONLY))
                .isInstanceOf(IOException.class)
                .isNotInstanceOf(TableAdminException.class)
                .hasMessageContaining("primaryKeyColumns");
        assertThat(service.creates).isZero();
    }

    private static boolean ensure(
            FakeService service,
            CdcTableOptions options,
            CreateDisposition createDisposition,
            CdcTableReconciliationPolicy policy)
            throws IOException {
        return new CdcTableProvisioner(service)
                .ensure(
                        DESTINATION,
                        SCHEMA,
                        destination -> TableCreateOptions.defaults(),
                        options,
                        createDisposition,
                        policy);
    }

    private static CdcTableOptions options(Duration maxStaleness) {
        return CdcTableOptions.builder()
                .primaryKeyColumns(Arrays.asList("id", "tenant"))
                .maxStaleness(maxStaleness)
                .build();
    }

    private static String pending(CdcTableOptions options) {
        return "pending_"
                + CdcTableProvisioner.specificationHash(Arrays.asList("id", "tenant"), options);
    }

    private static String complete(CdcTableOptions options) {
        return "complete_"
                + CdcTableProvisioner.specificationHash(Arrays.asList("id", "tenant"), options);
    }

    private static FakeService existing(CdcTableProvisioner.TableState state) {
        FakeService service = new FakeService();
        service.state = state;
        return service;
    }

    private static CdcTableProvisioner.TableState serviceState(
            List<String> primaryKey, @Nullable String label, String etag) {
        return new CdcTableProvisioner.TableState(primaryKey, label, etag);
    }

    private static final class FakeService implements CdcTableProvisioner.Service {
        @Nullable private CdcTableProvisioner.TableState state;
        @Nullable private Duration liveMaxStaleness;
        @Nullable private List<String> primaryKeyAfterDdl;
        @Nullable private String labelAfterDdl;
        @Nullable private CdcTableProvisioner.TableState stateAfterCreateConflict;
        @Nullable private IOException setMaxStalenessFailure;
        @Nullable private String labelAfterCreate;
        @Nullable private String labelAfterLabelUpdate;
        private final List<Duration> setMaxStaleness = new ArrayList<>();
        private final List<String> labelUpdates = new ArrayList<>();
        private boolean createWins = true;
        private boolean rejectLabelUpdates;
        private boolean disappearsAfterCreate;
        private boolean disappearsAfterDdl;
        private boolean maxStalenessNeverConverges;
        private int creates;
        private int maxStalenessChecks;
        private int etag = 1;

        @Override
        public CdcTableProvisioner.TableState read(TableDestination destination) {
            return state;
        }

        @Override
        public boolean tryCreate(
                TableDestination destination,
                TableSchema schema,
                TableCreateOptions createOptions,
                CdcTableOptions cdcOptions,
                String provisioningLabel) {
            creates++;
            if (!createWins) {
                state = stateAfterCreateConflict;
                return false;
            }
            if (disappearsAfterCreate) {
                state = null;
                return true;
            }
            state =
                    serviceState(
                            cdcOptions.getPrimaryKeyColumns(),
                            labelAfterCreate == null ? provisioningLabel : labelAfterCreate,
                            nextEtag());
            return true;
        }

        @Override
        public void setMaxStaleness(TableDestination destination, @Nullable Duration maxStaleness)
                throws IOException {
            if (setMaxStalenessFailure != null) {
                throw setMaxStalenessFailure;
            }
            setMaxStaleness.add(maxStaleness);
            if (!maxStalenessNeverConverges) {
                liveMaxStaleness = maxStaleness;
            }
            if (disappearsAfterDdl) {
                state = null;
                return;
            }
            if (state != null && (primaryKeyAfterDdl != null || labelAfterDdl != null)) {
                state =
                        serviceState(
                                primaryKeyAfterDdl == null
                                        ? state.primaryKeyColumns()
                                        : primaryKeyAfterDdl,
                                labelAfterDdl == null ? state.provisioningLabel() : labelAfterDdl,
                                nextEtag());
            }
        }

        @Override
        public boolean maxStalenessMatches(
                TableDestination destination, @Nullable Duration maxStaleness) {
            maxStalenessChecks++;
            return Objects.equals(liveMaxStaleness, maxStaleness);
        }

        @Override
        public boolean updateProvisioningLabel(
                TableDestination destination,
                @Nullable String expectedLabel,
                String nextLabel,
                String verifiedEtag) {
            if (rejectLabelUpdates
                    || state == null
                    || !Objects.equals(expectedLabel, state.provisioningLabel())
                    || !verifiedEtag.equals(state.etag())) {
                return false;
            }
            labelUpdates.add(expectedLabel + "->" + nextLabel);
            state =
                    serviceState(
                            state.primaryKeyColumns(),
                            labelAfterLabelUpdate == null ? nextLabel : labelAfterLabelUpdate,
                            nextEtag());
            return true;
        }

        private String nextEtag() {
            return "e" + etag++;
        }
    }
}
