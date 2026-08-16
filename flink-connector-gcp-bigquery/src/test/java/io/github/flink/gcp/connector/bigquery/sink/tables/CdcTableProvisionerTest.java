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
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
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
        @Nullable private CdcTableProvisioner.TableState stateAfterCreateConflict;
        @Nullable private IOException setMaxStalenessFailure;
        private final List<Duration> setMaxStaleness = new ArrayList<>();
        private final List<String> labelUpdates = new ArrayList<>();
        private boolean createWins = true;
        private boolean rejectLabelUpdates;
        private int creates;
        private int maxStalenessChecks;
        private int etag = 1;

        @Override
        public CdcTableProvisioner.TableState read(TableDestination destination) {
            return state;
        }

        @Override
        public boolean create(
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
            state = serviceState(cdcOptions.getPrimaryKeyColumns(), provisioningLabel, nextEtag());
            return true;
        }

        @Override
        public void setMaxStaleness(TableDestination destination, @Nullable Duration maxStaleness)
                throws IOException {
            if (setMaxStalenessFailure != null) {
                throw setMaxStalenessFailure;
            }
            setMaxStaleness.add(maxStaleness);
            liveMaxStaleness = maxStaleness;
            if (primaryKeyAfterDdl != null && state != null) {
                state = serviceState(primaryKeyAfterDdl, state.provisioningLabel(), nextEtag());
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
            state = serviceState(state.primaryKeyColumns(), nextLabel, nextEtag());
            return true;
        }

        private String nextEtag() {
            return "e" + etag++;
        }
    }
}
