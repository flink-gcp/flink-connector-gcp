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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import com.google.cloud.bigquery.storage.v1.TableSchema;
import io.github.flink.gcp.connector.bigquery.sink.CdcTableOptions;
import io.github.flink.gcp.connector.bigquery.sink.CdcTableReconciliationPolicy;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptionsProvider;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Recovery and reconciliation protocol for a BigQuery CDC table.
 *
 * <p>Table creation remains controlled exclusively by {@link CreateDisposition}. A missing table is
 * created through {@code tables.insert} only under {@code CREATE_IF_NEEDED}; an existing table is
 * either verified or has mutable CDC properties reconciled according to {@link
 * CdcTableReconciliationPolicy}. Primary-key drift is never repaired.
 *
 * <p>The provisioning label carries a phase and a hash of the desired CDC contract. A matching
 * pending state is resumable. Adoption, migration, and drift repair first claim the table through
 * an ETag-conditioned label transition, so a conflicting metadata update loses before DDL runs.
 */
@Internal
final class CdcTableProvisioner {

    static final String PROVISIONING_LABEL = "flink_gcp_cdc";
    private static final String PENDING_PREFIX = "pending_";
    private static final String COMPLETE_PREFIX = "complete_";

    interface Service {

        @Nullable
        TableState read(TableDestination destination) throws IOException;

        /** Creates the labelled table; false means a concurrent creator won the race. */
        boolean tryCreate(
                TableDestination destination,
                TableSchema schema,
                TableCreateOptions createOptions,
                CdcTableOptions cdcOptions,
                String provisioningLabel)
                throws IOException;

        void setMaxStaleness(TableDestination destination, @Nullable Duration maxStaleness)
                throws IOException;

        boolean maxStalenessMatches(TableDestination destination, @Nullable Duration maxStaleness)
                throws IOException;

        /** Replaces the expected provisioning label; false means a concurrent update won. */
        boolean updateProvisioningLabel(
                TableDestination destination,
                @Nullable String expectedLabel,
                String nextLabel,
                String verifiedEtag)
                throws IOException;
    }

    static final class TableState {
        private final List<String> primaryKeyColumns;
        @Nullable private final String provisioningLabel;
        private final String etag;

        TableState(
                List<String> primaryKeyColumns, @Nullable String provisioningLabel, String etag) {
            this.primaryKeyColumns = new ArrayList<>(primaryKeyColumns);
            this.provisioningLabel = provisioningLabel;
            this.etag = Preconditions.checkNotNull(etag, "etag must not be null");
        }

        List<String> primaryKeyColumns() {
            return primaryKeyColumns;
        }

        @Nullable
        String provisioningLabel() {
            return provisioningLabel;
        }

        String etag() {
            return etag;
        }
    }

    private final Service service;

    CdcTableProvisioner(Service service) {
        this.service = Preconditions.checkNotNull(service, "service must not be null");
    }

    boolean ensure(
            TableDestination destination,
            TableSchema schema,
            TableCreateOptionsProvider createOptionsProvider,
            CdcTableOptions cdcOptions,
            CreateDisposition createDisposition,
            CdcTableReconciliationPolicy reconciliationPolicy)
            throws IOException {
        TableState state = service.read(destination);
        if (state != null) {
            reconcile(destination, cdcOptions, reconciliationPolicy, state, false);
            return false;
        }

        // Both refusals — CREATE_NEVER, and a creation with no declared primary key — are decided
        // before the service is asked for anything, so they propagate as themselves and are not
        // creation requests. Past this line the request has been made, and every failure after it
        // has to say so: the metric counts requests, not successes, and a table that was asked for
        // and then failed to provision is still a table this job asked BigQuery to create.
        String initialLabel = plannedInitialLabel(destination, cdcOptions, createDisposition);
        try {
            Creation creation =
                    createMissing(
                            destination, schema, createOptionsProvider, cdcOptions, initialLabel);
            reconcile(
                    destination,
                    cdcOptions,
                    reconciliationPolicy,
                    creation.state(),
                    creation.wonTheRace());
        } catch (RetriableTableAdminException e) {
            throw e.wasCreationRequested()
                    ? e
                    : new RetriableTableAdminException(e.getMessage(), e, true);
        } catch (IOException e) {
            throw e instanceof TableAdminException
                            && ((TableAdminException) e).wasCreationRequested()
                    ? e
                    : new TableAdminException(e.getMessage(), e, true);
        }
        return true;
    }

    /**
     * The provisioning label a missing table would be created with.
     *
     * <p>Split from {@link #createMissing} so that the two conditions which refuse to create run
     * before anything is requested of the service. A refusal is not a creation request, and keeping
     * the two apart is what lets the caller decide that by position rather than by inspecting a
     * flag.
     *
     * @throws IOException if the disposition forbids creation, or no primary key is declared
     */
    private String plannedInitialLabel(
            TableDestination destination,
            CdcTableOptions cdcOptions,
            CreateDisposition createDisposition)
            throws IOException {
        if (createDisposition == CreateDisposition.CREATE_NEVER) {
            throw new IOException(
                    "BigQuery CDC table "
                            + destination
                            + " does not exist and createDisposition is CREATE_NEVER");
        }
        List<String> declaredPrimaryKey = cdcOptions.getPrimaryKeyColumns();
        if (declaredPrimaryKey.isEmpty()) {
            throw new IOException(
                    "Creating missing BigQuery CDC table "
                            + destination
                            + " requires CdcTableOptions.primaryKeyColumns(...)");
        }
        String hash = specificationHash(declaredPrimaryKey, cdcOptions);
        return cdcOptions.managesMaxStaleness() ? PENDING_PREFIX + hash : COMPLETE_PREFIX + hash;
    }

    /**
     * Creates the missing table under {@code initialLabel} and reads back the state to reconcile
     * from.
     *
     * <p>A creation this attempt lost is not an error: {@code tables.insert} answering 409 means
     * another subtask created the same table, which is the ordinary parallel case. What the caller
     * needs to know is whether <em>this</em> attempt won, because only a winner may assume the
     * label it wrote is the one on the table.
     */
    private Creation createMissing(
            TableDestination destination,
            TableSchema schema,
            TableCreateOptionsProvider createOptionsProvider,
            CdcTableOptions cdcOptions,
            String initialLabel)
            throws IOException {
        TableCreateOptions createOptions =
                Preconditions.checkNotNull(
                        createOptionsProvider.optionsFor(destination),
                        "TableCreateOptionsProvider returned null for %s",
                        destination);
        boolean createWon =
                service.tryCreate(destination, schema, createOptions, cdcOptions, initialLabel);
        TableState state = service.read(destination);
        if (state == null) {
            throw new RetriableTableAdminException(
                    "BigQuery table "
                            + destination
                            + " was created but is not visible through the Tables API yet",
                    null);
        }
        return new Creation(state, createWon);
    }

    /** What creating a missing table produced. */
    private static final class Creation {

        private final TableState state;
        private final boolean wonTheRace;

        Creation(TableState state, boolean wonTheRace) {
            this.state = state;
            this.wonTheRace = wonTheRace;
        }

        TableState state() {
            return state;
        }

        boolean wonTheRace() {
            return wonTheRace;
        }
    }

    /**
     * Brings an existing table's provisioning label and mutable CDC properties to the configured
     * specification, or verifies them.
     *
     * @param createdHere whether this attempt created the table, which is the only case in which
     *     the live label is required to be the one just written
     */
    private void reconcile(
            TableDestination destination,
            CdcTableOptions cdcOptions,
            CdcTableReconciliationPolicy reconciliationPolicy,
            TableState state,
            boolean createdHere)
            throws IOException {
        List<String> effectivePrimaryKey =
                effectivePrimaryKey(destination, cdcOptions, state.primaryKeyColumns());
        if (reconciliationPolicy == CdcTableReconciliationPolicy.RECONCILE
                && cdcOptions.getPrimaryKeyColumns().isEmpty()) {
            throw new IOException(
                    "Reconciling existing BigQuery CDC table "
                            + destination
                            + " requires CdcTableOptions.primaryKeyColumns(...) to define the"
                            + " desired primary key");
        }

        String hash = specificationHash(effectivePrimaryKey, cdcOptions);
        String pending = PENDING_PREFIX + hash;
        String complete = COMPLETE_PREFIX + hash;
        String liveLabel = state.provisioningLabel();

        if (pending.equals(liveLabel)) {
            completePending(destination, cdcOptions, effectivePrimaryKey, pending, complete, state);
            return;
        }
        if (liveLabel != null && liveLabel.startsWith(PENDING_PREFIX)) {
            throw new IOException(
                    "BigQuery table "
                            + destination
                            + " carries in-progress CDC provisioning label "
                            + PROVISIONING_LABEL
                            + "="
                            + liveLabel
                            + ", which does not match the configured CDC table specification");
        }

        boolean maxStalenessMatches = maxStalenessMatches(destination, cdcOptions);
        if (createdHere) {
            if (!complete.equals(liveLabel)) {
                throw new IOException(
                        "New BigQuery CDC table "
                                + destination
                                + " carries unexpected provisioning label "
                                + liveLabel);
            }
            return;
        }

        if (reconciliationPolicy == CdcTableReconciliationPolicy.VERIFY_ONLY) {
            verifyExisting(destination, liveLabel, complete, maxStalenessMatches);
            return;
        }

        if (complete.equals(liveLabel) && maxStalenessMatches) {
            return;
        }

        if (maxStalenessMatches) {
            updateLabelOrRetry(destination, liveLabel, complete, state.etag());
            return;
        }

        updateLabelOrRetry(destination, liveLabel, pending, state.etag());
        state = requireState(destination, "after CDC reconciliation was claimed");
        if (!pending.equals(state.provisioningLabel())) {
            throw new RetriableTableAdminException(
                    "BigQuery table "
                            + destination
                            + " changed provisioning state after CDC reconciliation was claimed",
                    null);
        }
        completePending(destination, cdcOptions, effectivePrimaryKey, pending, complete, state);
    }

    private void verifyExisting(
            TableDestination destination,
            @Nullable String liveLabel,
            String complete,
            boolean maxStalenessMatches)
            throws IOException {
        if (liveLabel != null && !complete.equals(liveLabel)) {
            throw new IOException(
                    "BigQuery table "
                            + destination
                            + " carries CDC provisioning label "
                            + PROVISIONING_LABEL
                            + "="
                            + liveLabel
                            + ", which does not match the configured CDC table specification");
        }
        if (!maxStalenessMatches) {
            throw new IOException(
                    "Existing BigQuery CDC table "
                            + destination
                            + " does not match the configured max_staleness contract; policy "
                            + CdcTableReconciliationPolicy.VERIFY_ONLY
                            + " does not start drift repair on existing tables");
        }
    }

    private void completePending(
            TableDestination destination,
            CdcTableOptions options,
            List<String> expectedPrimaryKey,
            String pending,
            String complete,
            TableState state)
            throws IOException {
        verifyPrimaryKeyUnchanged(destination, options, expectedPrimaryKey, state);
        if (!maxStalenessMatches(destination, options)) {
            service.setMaxStaleness(destination, options.getMaxStaleness());
            state = requireState(destination, "after max_staleness was reconciled");
            verifyPrimaryKeyUnchanged(destination, options, expectedPrimaryKey, state);
            if (!pending.equals(state.provisioningLabel())
                    && !complete.equals(state.provisioningLabel())) {
                throw new IOException(
                        "BigQuery table "
                                + destination
                                + " changed provisioning label while max_staleness was being"
                                + " reconciled: "
                                + state.provisioningLabel());
            }
            if (!maxStalenessMatches(destination, options)) {
                throw new RetriableTableAdminException(
                        "BigQuery accepted max_staleness reconciliation for "
                                + destination
                                + " but INFORMATION_SCHEMA does not expose the desired state yet",
                        null);
            }
            if (complete.equals(state.provisioningLabel())) {
                return;
            }
        }
        updateLabelOrRetry(destination, pending, complete, state.etag());
    }

    private static void verifyPrimaryKeyUnchanged(
            TableDestination destination,
            CdcTableOptions options,
            List<String> expectedPrimaryKey,
            TableState state)
            throws IOException {
        List<String> observed =
                effectivePrimaryKey(destination, options, state.primaryKeyColumns());
        if (!normalized(expectedPrimaryKey).equals(normalized(observed))) {
            throw new IOException(
                    "BigQuery table "
                            + destination
                            + " changed primary key while its CDC table contract was being"
                            + " reconciled: expected "
                            + expectedPrimaryKey
                            + " but found "
                            + observed);
        }
    }

    private TableState requireState(TableDestination destination, String operation)
            throws IOException {
        TableState state = service.read(destination);
        if (state == null) {
            throw new RetriableTableAdminException(
                    "BigQuery table " + destination + " disappeared " + operation, null);
        }
        return state;
    }

    private void updateLabelOrRetry(
            TableDestination destination, @Nullable String expected, String next, String etag)
            throws IOException {
        if (!service.updateProvisioningLabel(destination, expected, next, etag)) {
            throw new RetriableTableAdminException(
                    "A concurrent metadata update prevented CDC reconciliation of " + destination,
                    null);
        }
    }

    private boolean maxStalenessMatches(TableDestination destination, CdcTableOptions options)
            throws IOException {
        return !options.managesMaxStaleness()
                || service.maxStalenessMatches(destination, options.getMaxStaleness());
    }

    private static List<String> effectivePrimaryKey(
            TableDestination destination, CdcTableOptions options, List<String> actual)
            throws IOException {
        if (actual.isEmpty()) {
            throw new IOException("BigQuery CDC table " + destination + " has no primary key");
        }
        List<String> expected = options.getPrimaryKeyColumns();
        if (!expected.isEmpty() && !normalized(expected).equals(normalized(actual))) {
            throw new IOException(
                    "BigQuery table "
                            + destination
                            + " has primary key "
                            + actual
                            + " but the configured CDC table contract requires "
                            + expected);
        }
        return expected.isEmpty() ? actual : expected;
    }

    private static List<String> normalized(List<String> columns) {
        List<String> normalized = new ArrayList<>(columns.size());
        for (String column : columns) {
            normalized.add(column.toLowerCase(Locale.ROOT));
        }
        return normalized;
    }

    @VisibleForTesting
    static String specificationHash(List<String> primaryKeyColumns, CdcTableOptions options) {
        StringBuilder canonical = new StringBuilder();
        for (String column : normalized(primaryKeyColumns)) {
            canonical.append(column).append('\0');
        }
        if (!options.managesMaxStaleness()) {
            canonical.append("max_staleness=unmanaged");
        } else if (options.clearsMaxStaleness()) {
            canonical.append("max_staleness=clear");
        } else {
            Duration maxStaleness =
                    Objects.requireNonNull(options.getMaxStaleness(), "managed maxStaleness");
            canonical.append("max_staleness_us=").append(maxStaleness.toNanos() / 1_000);
        }
        byte[] digest;
        try {
            digest =
                    MessageDigest.getInstance("SHA-256")
                            .digest(canonical.toString().getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
        return StringUtils.byteToHexString(digest).substring(0, 32);
    }
}
