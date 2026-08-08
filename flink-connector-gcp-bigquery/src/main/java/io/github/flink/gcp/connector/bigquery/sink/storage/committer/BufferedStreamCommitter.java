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

package io.github.flink.gcp.connector.bigquery.sink.storage.committer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.Committer;

import io.github.flink.gcp.connector.base.retry.Retries;
import io.github.flink.gcp.connector.base.retry.RetrySchedule;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamCommittable;
import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamOptions;
import io.github.flink.gcp.connector.bigquery.sink.storage.writer.AppendErrorClassifier;
import io.github.flink.gcp.connector.bigquery.sink.storage.writer.BufferedStreamService;
import io.github.flink.gcp.connector.bigquery.sink.storage.writer.BufferedStreamServiceFactory;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;

/**
 * Committer of the buffered-stream write path: makes each completed checkpoint's rows visible by
 * flushing every committable's stream up to its offset ({@code FlushRows}).
 *
 * <p>Flushes run synchronously inside {@link #commit}: a slow flush delays the next checkpoint —
 * that is backpressure, and it keeps the committed contract honest ({@code commit()} returning
 * means the rows are visible). Transient failures are retried in place within the configured budget
 * — {@code FlushRows} is idempotent, so retrying is always safe and cheaper than a job restart.
 * Everything else deliberately uses none of the {@link CommitRequest} retry signals: the failure
 * throws, the job restarts, and the framework re-commits the restored requests (a re-flush of an
 * already-flushed offset answers {@code ALREADY_EXISTS} and is treated as success). This is
 * stricter than Beam's flush handler (which also skips {@code NOT_FOUND} / {@code
 * INVALID_ARGUMENT}): wrongly skipping would be silent data loss, wrongly failing is a visible
 * restart.
 *
 * <p>Under {@link CreateDisposition#CREATE_IF_NEEDED} the masked-existence verdict ({@link
 * AppendErrorClassifier#isExistenceMasked}, deliberately not the writers' wider missing-table one)
 * is retried alongside the transient failures: the window in which a table the writer has just
 * auto-created has not yet propagated to the Storage Write API backend reaches {@code FlushRows} as
 * well as {@code CreateWriteStream}. Measured 2026-08-08 — one end-to-end run in five answered
 * {@code PERMISSION_DENIED: Permission 'TABLES_UPDATE_DATA' denied on resource '&lt;table&gt;' (or
 * it may not exist)} to the first commit after auto-creation, naming the table rather than the
 * stream. The disposition is what gates it: under {@link CreateDisposition#CREATE_NEVER} nothing
 * here creates a table, so the code can only mean a genuine denial or a table that disappeared, and
 * failing at once is the better answer. What the allowance costs a genuinely denied caller is the
 * recovery budget before the failure surfaces — with the defaults about 55 s per committable,
 * serially — which is the trade the writer already makes.
 *
 * <p>Runs at the sink's parallelism — flushes of distinct streams are independent, so no global
 * routing is needed (unlike FILE_LOADS, whose load jobs must see all files of a table at once).
 */
@Internal
public class BufferedStreamCommitter implements Committer<BufferedStreamCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(BufferedStreamCommitter.class);

    private final BufferedStreamServiceFactory serviceFactory;
    @Nullable private final String location;
    private final RetrySchedule retrySchedule;
    private final BufferedStreamOptions options;
    private final CreateDisposition createDisposition;

    @Nullable private BufferedStreamService service;

    /**
     * Creates a committer.
     *
     * @param serviceFactory the Storage Write API service factory
     * @param location the BigQuery location routing hint, or {@code null}
     * @param options the buffered-stream options (retry schedule)
     * @param createDisposition the sink's create disposition, which decides whether a missing-table
     *     verdict is waited out as a propagation window or failed immediately
     */
    public BufferedStreamCommitter(
            BufferedStreamServiceFactory serviceFactory,
            @Nullable String location,
            BufferedStreamOptions options,
            CreateDisposition createDisposition) {
        this.serviceFactory = serviceFactory;
        this.location = location;
        this.retrySchedule = options.toRecoverySchedule();
        this.options = options;
        this.createDisposition = createDisposition;
    }

    /**
     * Returns the disposition this committer gates its propagation-window allowance on.
     *
     * <p>Exists so the sink's wiring of it can be asserted without a live BigQuery client: the
     * allowance is otherwise only observable through a flush, which needs one.
     *
     * @return the create disposition
     */
    @VisibleForTesting
    public CreateDisposition getCreateDisposition() {
        return createDisposition;
    }

    @Override
    public void commit(Collection<CommitRequest<BufferedStreamCommittable>> requests)
            throws IOException {
        for (CommitRequest<BufferedStreamCommittable> request : requests) {
            BufferedStreamCommittable committable = request.getCommittable();
            flush(committable);
            // Requests left unsignaled are treated as committed.
        }
    }

    private void flush(BufferedStreamCommittable committable) throws IOException {
        long flushed;
        int attempt = 1;
        while (true) {
            try {
                flushed =
                        service()
                                .flushRows(
                                        committable.getStreamName(), committable.getFlushOffset());
                break;
            } catch (IOException | RuntimeException e) {
                if (AppendErrorClassifier.isOffsetAlreadyExists(e)
                        || AppendErrorClassifier.hasCode(e, Status.Code.ALREADY_EXISTS)) {
                    // A re-commit after a restore: the offset was flushed before the crash.
                    LOG.info(
                            "Stream {} is already flushed up to offset {} (re-commit after"
                                    + " restore)",
                            committable.getStreamName(),
                            committable.getFlushOffset());
                    return;
                }
                // Retrying a flush in place is always safe (FlushRows is idempotent) and much
                // cheaper than the restart the throw below causes.
                boolean retriable = AppendErrorClassifier.isTransient(e) || isPropagating(e);
                if (!retriable || attempt >= retrySchedule.maxAttempts()) {
                    throw new IOException(
                            "Failed to flush BigQuery stream "
                                    + committable.getStreamName()
                                    + " up to offset "
                                    + committable.getFlushOffset()
                                    + " (subtask "
                                    + committable.getSubtaskId()
                                    // The attempt count, and never the bare word "transient": a
                                    // masked PERMISSION_DENIED that is a genuine denial exhausts
                                    // the same budget, and a reader must not take that for an
                                    // availability problem.
                                    + (retriable
                                            ? ", the retry budget is exhausted after "
                                                    + attempt
                                                    + " attempt(s)"
                                            : "")
                                    + "); the commit will be retried after a restart",
                            e);
                }
                long backoffMs = retrySchedule.backoffMs(attempt);
                // The attempt and the budget, as the writers' repair log carries them: a masked
                // denial can repeat for the whole schedule, and nine identical lines would say
                // nothing about whether the wait is progressing or merely running out.
                LOG.info(
                        "Retrying the flush of stream {} up to offset {} (attempt {}/{}), backing"
                                + " off {} ms after: {}",
                        committable.getStreamName(),
                        committable.getFlushOffset(),
                        attempt,
                        retrySchedule.maxAttempts(),
                        backoffMs,
                        e.toString());
                Retries.sleep(backoffMs, "Interrupted while waiting to retry a BigQuery flush");
                attempt++;
            }
        }
        if (flushed != committable.getFlushOffset()) {
            throw new IOException(
                    "BigQuery flushed stream "
                            + committable.getStreamName()
                            + " up to offset "
                            + flushed
                            + " although offset "
                            + committable.getFlushOffset()
                            + " was requested");
        }
        LOG.info(
                "Flushed stream {} up to offset {} (subtask {})",
                committable.getStreamName(),
                committable.getFlushOffset(),
                committable.getSubtaskId());
    }

    /**
     * Whether the failure may be a table the writer has just created that has not propagated to the
     * Storage Write API backend yet — retriable here only because the disposition says something in
     * this job creates tables.
     *
     * <p>{@link AppendErrorClassifier#isExistenceMasked} rather than the wider {@link
     * AppendErrorClassifier#isMissingTable} the writers take, and that narrowing is load-bearing:
     * {@code FlushRows} names a write <em>stream</em>, which can legitimately be gone — streams age
     * out on a seven-day TTL and a missing one is terminal mid-run — so a {@code NOT_FOUND} here
     * would spend the whole budget on a failure that cannot succeed.
     */
    private boolean isPropagating(Throwable failure) {
        return createDisposition == CreateDisposition.CREATE_IF_NEEDED
                && AppendErrorClassifier.isExistenceMasked(failure);
    }

    @Override
    public void close() {
        if (service != null) {
            service.close();
            service = null;
        }
    }

    private BufferedStreamService service() throws IOException {
        if (service == null) {
            service = serviceFactory.create(location, options);
        }
        return service;
    }
}
