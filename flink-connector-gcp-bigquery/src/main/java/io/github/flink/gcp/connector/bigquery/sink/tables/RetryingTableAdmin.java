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

import com.google.cloud.bigquery.storage.v1.TableSchema;
import io.github.flink.gcp.connector.base.retry.Retries;
import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.bigquery.sink.CdcTableOptions;
import io.github.flink.gcp.connector.bigquery.sink.CdcTableReconciliationPolicy;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptionsProvider;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A {@link TableAdmin} that repeats a creation the service answered with a {@link
 * RetriableTableAdminException}, within a fixed budget.
 *
 * <p><b>What it is for.</b> Parallel subtasks all race to create the same missing table. A loser is
 * normally answered HTTP 409, which {@link BigQueryTableAdmin} treats as success, but past a
 * handful of concurrent creations the per-table metadata-update quota answers instead — measured
 * 2026-08-08 at sixteen concurrent creations of one table, of which five were rate-limited. That is
 * not a 409, the client library does not retry it, and before this class existed it failed the
 * write outright (#383).
 *
 * <p><b>Why a decorator rather than a loop at each creation site.</b> There are four of them across
 * the connector — three around the at-least-once writer's {@code createTable}, one in the
 * exactly-once writer's {@code createStream} — plus the FILE_LOADS committer's, which a first
 * attempt at this missed precisely because it was written as "change every site". Wrapping instead
 * moves the decision to the three places a {@code TableAdmin} is <em>constructed</em>, which are
 * enumerable, and leaves every caller holding the SPI it already held. It also makes the budget
 * structural: a site cannot pass the wrong schedule, because no site passes one.
 *
 * <p>{@link #create} and {@link #ensureCdcTable} retry. {@link #getSchema} and {@link
 * #updateSchema} pass straight through — the latter reports its own lost race as {@code false} for
 * the caller to re-read and re-derive from, which a blind repeat here would turn into a
 * stale-proposal loop.
 */
@Internal
public final class RetryingTableAdmin implements TableAdmin {

    private static final Logger LOG = LoggerFactory.getLogger(RetryingTableAdmin.class);

    private final TableAdmin delegate;
    private final RetrySchedule schedule;

    /**
     * Wraps an admin.
     *
     * @param delegate the admin doing the work
     * @param schedule the budget bounding a repeated creation
     */
    public RetryingTableAdmin(TableAdmin delegate, RetrySchedule schedule) {
        this.delegate = Preconditions.checkNotNull(delegate, "delegate must not be null");
        this.schedule = Preconditions.checkNotNull(schedule, "schedule must not be null");
    }

    @Override
    public void create(TableDestination destination, TableSchema schema, TableCreateOptions options)
            throws IOException {
        retry(destination, "create", () -> delegate.create(destination, schema, options));
    }

    @Override
    public boolean ensureCdcTable(
            TableDestination destination,
            TableSchema schema,
            TableCreateOptionsProvider createOptionsProvider,
            CdcTableOptions cdcOptions,
            CreateDisposition createDisposition,
            CdcTableReconciliationPolicy reconciliationPolicy)
            throws IOException {
        CreationOutcome outcome = new CreationOutcome();
        try {
            retry(
                    destination,
                    "provision for CDC",
                    () -> {
                        try {
                            outcome.record(
                                    delegate.ensureCdcTable(
                                            destination,
                                            schema,
                                            createOptionsProvider,
                                            cdcOptions,
                                            createDisposition,
                                            reconciliationPolicy));
                        } catch (TableAdminException e) {
                            // Records and rethrows unchanged. The accumulator outlives the
                            // exception, so nothing has to be smuggled inside it.
                            outcome.record(e.wasCreationRequested());
                            throw e;
                        }
                    });
        } catch (IOException e) {
            throw outcome.attach(e);
        }
        return outcome.requested();
    }

    /**
     * Whether any attempt asked BigQuery to create the table.
     *
     * <p>The accumulation is across <em>attempts</em>, which is why it cannot live in the failure
     * that ends the last one: an attempt may request creation and fail, and a later attempt may
     * then find the table already there and request nothing. The metric counts requests, so the
     * earlier one must survive the attempt that made it.
     */
    private static final class CreationOutcome {

        private boolean requested;

        void record(boolean creationRequested) {
            this.requested |= creationRequested;
        }

        boolean requested() {
            return requested;
        }

        /**
         * Returns the failure to throw, carrying the accumulated outcome.
         *
         * <p>The single point at which the bit meets an exception. A failure that already reports a
         * creation request is returned untouched rather than re-wrapped, so the message a caller
         * reads is the one the service produced.
         */
        IOException attach(IOException failure) {
            if (!requested) {
                return failure;
            }
            if (failure instanceof TableAdminException
                    && ((TableAdminException) failure).wasCreationRequested()) {
                return failure;
            }
            return new TableAdminException(failure.getMessage(), failure, true);
        }
    }

    private void retry(TableDestination destination, String operation, IoRunnable runnable)
            throws IOException {
        for (int attempt = 1; ; attempt++) {
            try {
                runnable.run();
                return;
            } catch (RetriableTableAdminException e) {
                if (attempt >= schedule.maxAttempts()) {
                    String message =
                            "Failed to "
                                    + operation
                                    + " BigQuery table "
                                    + destination
                                    + ", the retry budget is exhausted ("
                                    + attempt
                                    + " attempt(s))";
                    throw new IOException(message, e);
                }
                long backoffMs = schedule.backoffMs(attempt);
                // The throwable itself rather than its toString: the reason an operator needs —
                // "Exceeded rate limits", a 503 — is on the client exception underneath, and the
                // wrapper's own message carries only the table name.
                LOG.info(
                        "Trying to {} BigQuery table {} is not possible yet (attempt {}/{}),"
                                + " backing off {} ms",
                        operation,
                        destination,
                        attempt,
                        schedule.maxAttempts(),
                        backoffMs,
                        e);
                Retries.sleep(
                        backoffMs,
                        "Interrupted while waiting to "
                                + operation
                                + " BigQuery table "
                                + destination);
            }
        }
    }

    @FunctionalInterface
    private interface IoRunnable {
        void run() throws IOException;
    }

    @Override
    public TableSchemaSnapshot getSchema(TableDestination destination) throws IOException {
        return delegate.getSchema(destination);
    }

    @Override
    public boolean updateSchema(
            TableDestination destination, TableSchemaSnapshot base, TableSchema proposed)
            throws IOException {
        return delegate.updateSchema(destination, base, proposed);
    }

    /**
     * The budget this wrap was built with.
     *
     * <p>Exists so a test can assert <em>which</em> schedule a sink wired, not merely that it wired
     * one: a creation must be bounded by the caller's recovery budget and never by a longer one,
     * and with the choice moved out of the writers this seam is what is left to check it by. Same
     * argument as {@code BufferedStreamCommitter.getCreateDisposition}.
     *
     * @return the schedule
     */
    @VisibleForTesting
    public RetrySchedule getSchedule() {
        return schedule;
    }

    /** Returns the wrapped admin so construction wiring can be verified. */
    @VisibleForTesting
    public TableAdmin getDelegate() {
        return delegate;
    }
}
