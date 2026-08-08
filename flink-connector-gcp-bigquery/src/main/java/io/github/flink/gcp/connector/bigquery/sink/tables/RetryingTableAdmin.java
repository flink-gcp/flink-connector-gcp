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

package io.github.flink.gcp.connector.bigquery.sink.tables;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigquery.storage.v1.TableSchema;
import io.github.flink.gcp.connector.base.retry.Retries;
import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptions;
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
 * <p>Only {@link #create} retries. {@link #getSchema} and {@link #updateSchema} pass straight
 * through — the latter reports its own lost race as {@code false} for the caller to re-read and
 * re-derive from, which a blind repeat here would turn into a stale-proposal loop.
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
        for (int attempt = 1; ; attempt++) {
            try {
                delegate.create(destination, schema, options);
                return;
            } catch (RetriableTableAdminException e) {
                if (attempt >= schedule.maxAttempts()) {
                    throw new IOException(
                            "Failed to create BigQuery table "
                                    + destination
                                    + ", the retry budget is exhausted ("
                                    + attempt
                                    + " attempt(s))",
                            e);
                }
                long backoffMs = schedule.backoffMs(attempt);
                // The throwable itself rather than its toString: the reason an operator needs —
                // "Exceeded rate limits", a 503 — is on the client exception underneath, and the
                // wrapper's own message carries only the table name.
                LOG.info(
                        "Creating BigQuery table {} is not possible yet (attempt {}/{}),"
                                + " backing off {} ms",
                        destination,
                        attempt,
                        schedule.maxAttempts(),
                        backoffMs,
                        e);
                Retries.sleep(
                        backoffMs, "Interrupted while waiting to retry a BigQuery table creation");
            }
        }
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
}
