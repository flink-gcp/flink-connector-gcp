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

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.bigquery.sink.CdcTableOptions;
import io.github.flink.gcp.connector.bigquery.sink.CdcTableReconciliationPolicy;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptionsProvider;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RetryingTableAdmin}. */
class RetryingTableAdminTest {

    private static final TableDestination DESTINATION = TableDestination.of("p", "d", "t");
    private static final CdcTableOptions CDC_OPTIONS =
            CdcTableOptions.builder().primaryKeyColumns(List.of("f")).build();

    private static final TableSchema SCHEMA =
            TableSchema.newBuilder()
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("f")
                                    .setType(TableFieldSchema.Type.STRING)
                                    .setMode(TableFieldSchema.Mode.NULLABLE)
                                    .build())
                    .build();

    /** 1 ms backoffs, no jitter, the given attempt budget. */
    private static RetrySchedule fast(int maxAttempts) {
        return new RetrySchedule(1, 1, maxAttempts, 0);
    }

    /**
     * What a subtask that lost the creation race to the per-table metadata-update quota is
     * answered, as {@link BigQueryTableAdmin} types it. Measured 2026-08-08 — see {@link
     * RetryingTableAdmin}.
     */
    private static RetriableTableAdminException rateLimited() {
        return new RetriableTableAdminException(
                "Failed to create BigQuery table " + DESTINATION,
                new BigQueryException(
                        403,
                        "Exceeded rate limits: too many table update operations for this table.",
                        new BigQueryError("rateLimitExceeded", null, "Exceeded rate limits")));
    }

    private static RetriableTableAdminException rateLimitedAfterCreation() {
        return new RetriableTableAdminException(
                "Failed after requesting creation of " + DESTINATION, rateLimited(), true);
    }

    /** Records every call and fails creations off a script, one entry per attempt. */
    private static final class ScriptedTableAdmin implements TableAdmin {

        private final List<TableDestination> creates = new ArrayList<>();
        private final Deque<IOException> creationFailures = new ArrayDeque<>();
        private final Deque<IOException> cdcFailures = new ArrayDeque<>();
        private final Deque<Boolean> cdcResults = new ArrayDeque<>();
        private int getSchemaCalls;
        private int updateSchemaCalls;

        @Override
        public void create(
                TableDestination destination, TableSchema schema, TableCreateOptions options)
                throws IOException {
            creates.add(destination);
            IOException failure = creationFailures.poll();
            if (failure != null) {
                throw failure;
            }
        }

        @Override
        public boolean ensureCdcTable(
                TableDestination destination,
                TableSchema schema,
                TableCreateOptionsProvider optionsProvider,
                CdcTableOptions cdcOptions,
                CreateDisposition createDisposition,
                CdcTableReconciliationPolicy reconciliationPolicy)
                throws IOException {
            create(destination, schema, optionsProvider.optionsFor(destination));
            IOException failure = cdcFailures.poll();
            if (failure != null) {
                throw failure;
            }
            return cdcResults.isEmpty() || cdcResults.remove();
        }

        @Override
        public TableSchemaSnapshot getSchema(TableDestination destination) {
            getSchemaCalls++;
            return null;
        }

        @Override
        public boolean updateSchema(
                TableDestination destination, TableSchemaSnapshot base, TableSchema proposed) {
            updateSchemaCalls++;
            return true;
        }
    }

    private static boolean ensureCdc(TableAdmin admin) throws IOException {
        return admin.ensureCdcTable(
                DESTINATION,
                SCHEMA,
                destination -> TableCreateOptions.defaults(),
                CDC_OPTIONS,
                CreateDisposition.CREATE_IF_NEEDED,
                CdcTableReconciliationPolicy.VERIFY_ONLY);
    }

    @Test
    void repeatsARateLimitedCreationUntilItSucceeds() throws Exception {
        ScriptedTableAdmin delegate = new ScriptedTableAdmin();
        delegate.creationFailures.add(rateLimited());
        delegate.creationFailures.add(rateLimited());

        new RetryingTableAdmin(delegate, fast(5))
                .create(DESTINATION, SCHEMA, TableCreateOptions.defaults());

        assertThat(delegate.creates).containsExactly(DESTINATION, DESTINATION, DESTINATION);
    }

    @Test
    void repeatsARetriableCdcProvisioningStep() throws Exception {
        ScriptedTableAdmin delegate = new ScriptedTableAdmin();
        delegate.creationFailures.add(rateLimited());

        boolean creationRequested = ensureCdc(new RetryingTableAdmin(delegate, fast(3)));

        assertThat(creationRequested).isTrue();
        assertThat(delegate.creates).containsExactly(DESTINATION, DESTINATION);
    }

    @Test
    void preservesACreationRequestAcrossAPostCreateRetry() throws Exception {
        ScriptedTableAdmin delegate = new ScriptedTableAdmin();
        delegate.creationFailures.add(rateLimitedAfterCreation());
        delegate.cdcResults.add(false);

        boolean creationRequested = ensureCdc(new RetryingTableAdmin(delegate, fast(3)));

        assertThat(creationRequested).isTrue();
        assertThat(delegate.creates).containsExactly(DESTINATION, DESTINATION);
    }

    @Test
    void preservesACreationRequestWhenALaterAttemptFailsTerminally() {
        ScriptedTableAdmin delegate = new ScriptedTableAdmin();
        delegate.creationFailures.add(rateLimitedAfterCreation());
        delegate.cdcFailures.add(new IOException("verification denied"));

        assertThatThrownBy(() -> ensureCdc(new RetryingTableAdmin(delegate, fast(3))))
                .isExactlyInstanceOf(TableAdminException.class)
                .hasMessage("verification denied")
                .satisfies(
                        failure ->
                                assertThat(((TableAdminException) failure).wasCreationRequested())
                                        .isTrue());
        assertThat(delegate.creates).containsExactly(DESTINATION, DESTINATION);
    }

    @Test
    void preservesACreationRequestWhenTheCdcRetryBudgetIsExhausted() {
        ScriptedTableAdmin delegate = new ScriptedTableAdmin();
        delegate.creationFailures.add(rateLimitedAfterCreation());
        delegate.creationFailures.add(rateLimited());
        delegate.creationFailures.add(rateLimited());

        assertThatThrownBy(() -> ensureCdc(new RetryingTableAdmin(delegate, fast(3))))
                .isExactlyInstanceOf(TableAdminException.class)
                .hasMessageContaining("retry budget is exhausted")
                .satisfies(
                        failure ->
                                assertThat(((TableAdminException) failure).wasCreationRequested())
                                        .isTrue());
        assertThat(delegate.creates).hasSize(3);
    }

    @Test
    void preservesACreationRequestWhenCdcRetryBackoffIsInterrupted() {
        ScriptedTableAdmin delegate = new ScriptedTableAdmin();
        delegate.creationFailures.add(rateLimitedAfterCreation());
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> ensureCdc(new RetryingTableAdmin(delegate, fast(3))))
                    .isExactlyInstanceOf(TableAdminException.class)
                    // The operation and the target, not just the prefix. The message is
                    // interpolated per operation, and asserting only the shared prefix let a
                    // mutant reverting that interpolation to a fixed "table creation" literal
                    // survive the whole suite. Independent substrings rather than the assembled
                    // sentence, so a reworded message that still names both keeps passing.
                    .hasMessageContainingAll(
                            "Interrupted", "provision", "CDC", DESTINATION.toString())
                    .satisfies(
                            failure ->
                                    assertThat(
                                                    ((TableAdminException) failure)
                                                            .wasCreationRequested())
                                            .isTrue());
        } finally {
            Thread.interrupted();
        }
        assertThat(delegate.creates).containsExactly(DESTINATION);
    }

    @Test
    void doesNotRepeatAFailureRepeatingCannotFix() {
        // The other half of the rule: a denial is not a rate limit, and repeating it would only
        // spend the budget before reporting what the first attempt already knew.
        ScriptedTableAdmin delegate = new ScriptedTableAdmin();
        delegate.creationFailures.add(new IOException("bigquery.tables.create denied"));
        RetryingTableAdmin admin = new RetryingTableAdmin(delegate, fast(5));

        assertThatThrownBy(() -> admin.create(DESTINATION, SCHEMA, TableCreateOptions.defaults()))
                .isInstanceOf(IOException.class)
                .hasMessage("bigquery.tables.create denied");
        assertThat(delegate.creates).containsExactly(DESTINATION);
    }

    @Test
    void givesUpAtTheBudgetNamingItsAttempts() {
        ScriptedTableAdmin delegate = new ScriptedTableAdmin();
        for (int i = 0; i < 10; i++) {
            delegate.creationFailures.add(rateLimited());
        }
        RetryingTableAdmin admin = new RetryingTableAdmin(delegate, fast(3));

        assertThatThrownBy(() -> admin.create(DESTINATION, SCHEMA, TableCreateOptions.defaults()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("p.d.t")
                .hasMessageContaining("the retry budget is exhausted")
                .hasMessageContaining("3 attempt(s)")
                // The last failure survives as the cause, so the reason is still reachable.
                .cause()
                .isInstanceOf(RetriableTableAdminException.class);
        // Bounded by the schedule, not by the supply of failures.
        assertThat(delegate.creates).hasSize(3);
    }

    @Test
    void aBudgetOfOneAttemptDoesNotRepeat() {
        // The unbounded loop's only termination guard when maxAttempts is at its floor.
        ScriptedTableAdmin delegate = new ScriptedTableAdmin();
        delegate.creationFailures.add(rateLimited());
        RetryingTableAdmin admin = new RetryingTableAdmin(delegate, fast(1));

        assertThatThrownBy(() -> admin.create(DESTINATION, SCHEMA, TableCreateOptions.defaults()))
                .hasMessageContaining("1 attempt(s)");
        assertThat(delegate.creates).hasSize(1);
    }

    @Test
    void schemaReadsAndUpdatesPassStraightThrough() throws Exception {
        // updateSchema reports its own lost race as false, for the caller to re-read and re-derive
        // from; repeating it here would re-submit a proposal built against a stale snapshot.
        ScriptedTableAdmin delegate = new ScriptedTableAdmin();
        RetryingTableAdmin admin = new RetryingTableAdmin(delegate, fast(5));

        assertThat(admin.getSchema(DESTINATION)).isNull();
        assertThat(admin.updateSchema(DESTINATION, null, SCHEMA)).isTrue();

        assertThat(delegate.getSchemaCalls).isEqualTo(1);
        assertThat(delegate.updateSchemaCalls).isEqualTo(1);
    }
}
