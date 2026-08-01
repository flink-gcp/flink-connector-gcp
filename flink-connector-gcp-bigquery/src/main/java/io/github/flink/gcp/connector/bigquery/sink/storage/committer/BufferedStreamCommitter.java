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
import org.apache.flink.api.connector.sink2.Committer;

import io.github.flink.gcp.connector.base.retry.Retries;
import io.github.flink.gcp.connector.base.retry.RetrySchedule;
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
 * <p>Runs at the sink's parallelism — flushes of distinct streams are independent, so no global
 * routing is needed (unlike FILE_LOADS, whose load jobs must see all files of a table at once).
 */
@Internal
public class BufferedStreamCommitter implements Committer<BufferedStreamCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(BufferedStreamCommitter.class);

    private final BufferedStreamServiceFactory serviceFactory;
    @Nullable private final String location;
    private final RetrySchedule retrySchedule;

    @Nullable private BufferedStreamService service;

    /**
     * Creates a committer.
     *
     * @param serviceFactory the Storage Write API service factory
     * @param location the BigQuery location routing hint, or {@code null}
     * @param options the buffered-stream options (retry schedule)
     */
    public BufferedStreamCommitter(
            BufferedStreamServiceFactory serviceFactory,
            @Nullable String location,
            BufferedStreamOptions options) {
        this.serviceFactory = serviceFactory;
        this.location = location;
        this.retrySchedule =
                new RetrySchedule(
                        options.getRecoveryInitialBackoff().toMillis(),
                        options.getRecoveryMaxBackoff().toMillis(),
                        options.getRecoveryMaxAttempts(),
                        0);
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
                boolean transientFailure = AppendErrorClassifier.isTransient(e);
                if (!transientFailure || attempt >= retrySchedule.maxAttempts()) {
                    throw new IOException(
                            "Failed to flush BigQuery stream "
                                    + committable.getStreamName()
                                    + " up to offset "
                                    + committable.getFlushOffset()
                                    + " (subtask "
                                    + committable.getSubtaskId()
                                    + (transientFailure ? ", the retry budget is exhausted" : "")
                                    + "); the commit will be retried after a restart",
                            e);
                }
                LOG.info(
                        "Retrying the flush of stream {} up to offset {} after: {}",
                        committable.getStreamName(),
                        committable.getFlushOffset(),
                        e.toString());
                Retries.sleep(
                        retrySchedule.backoffMs(attempt),
                        "Interrupted while waiting to retry a BigQuery flush");
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

    @Override
    public void close() {
        if (service != null) {
            service.close();
            service = null;
        }
    }

    private BufferedStreamService service() throws IOException {
        if (service == null) {
            service = serviceFactory.create(location);
        }
        return service;
    }
}
