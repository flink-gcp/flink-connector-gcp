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

package io.github.flink.gcp.connector.bigquery.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.bigquery.BigQueryMetricNames;
import io.github.flink.gcp.connector.bigquery.source.BigQuerySourceConfig;
import io.github.flink.gcp.connector.bigquery.source.split.BigQueryReadStreamSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Creates the read session once and hands its streams out one at a time.
 *
 * <p>Assignment is pull-based: a reader asks for a split when it has none and when it finishes one,
 * so a stream that takes longer than its siblings does not hold up a subtask that could be reading
 * another. Elasticity therefore comes from asking BigQuery for more streams than there are subtasks
 * rather than from splitting a stream at runtime.
 *
 * <p>The enumerator keeps <em>no</em> record of which subtask holds which split. The whole of its
 * state is one queue of unassigned splits and the flag saying the session exists, and every
 * question a ledger would answer is answered instead by what this class is handed — a request, or a
 * returned split. That is deliberate: the reference implementation this design was drawn from
 * records in its own change log a "critical data loss bug in reader split handling", fixed "by
 * signaling no-more-splits per reader and removing completed readers from queue" (read 2026-08-09).
 * What that establishes is that assignment and completion is where a hand-written enumerator goes
 * wrong quietly — and the per-reader half of it is something Flink's coordinator already does.
 *
 * <p>It does: {@code SourceCoordinator} suppresses a further split request from a subtask it has
 * already told there are no more splits, and clears that only when the subtask is reset. Since a
 * reset is also what returns a failed reader's splits ({@link #addSplitsBack}), a returned split is
 * always reachable by the subtask that comes back for it — but a <em>different</em> subtask that
 * already finished will not pick it up, because its requests no longer reach here.
 *
 * <p>The metrics follow the same rule. Counting assignments and returns needs no reconciliation,
 * while a gauge of currently-assigned splits would need the ledger this class exists without; the
 * unassigned side is Flink's own gauge, reading the queue directly — a best-effort read, since the
 * reporter thread samples a queue the coordinator thread mutates.
 */
@Internal
public class BigQueryReadSplitEnumerator
        implements SplitEnumerator<BigQueryReadStreamSplit, BigQueryReadEnumeratorState> {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryReadSplitEnumerator.class);

    private final SplitEnumeratorContext<BigQueryReadStreamSplit> context;
    private final BigQuerySourceConfig<?> config;
    private final ReadSessionCreator sessionCreator;
    @Nullable private final BigQueryReadEnumeratorState restoredState;

    /** Splits no reader currently holds, in assignment order. */
    private final Deque<BigQueryReadStreamSplit> pending = new ArrayDeque<>();

    /** Subtasks that asked for a split before the session existed, in the order they asked. */
    private final Set<Integer> awaitingInitialization = new LinkedHashSet<>();

    private boolean initialized;
    @Nullable private String sessionName;
    @Nullable private Instant sessionExpireTime;

    /** Written by {@link #close()} on the scheduler thread, read by the completion handler. */
    private volatile boolean closed;

    private Counter splitsAssigned = new ThreadSafeSimpleCounter();
    private Counter splitsReturned = new ThreadSafeSimpleCounter();
    private Counter readSessionsCreated = new ThreadSafeSimpleCounter();

    /**
     * Creates the enumerator.
     *
     * @param context the enumerator context
     * @param config the source configuration
     * @param sessionCreator creates the read session; the enumerator owns it and closes it
     * @param restoredState the checkpointed state, or {@code null} on a fresh start
     */
    public BigQueryReadSplitEnumerator(
            SplitEnumeratorContext<BigQueryReadStreamSplit> context,
            BigQuerySourceConfig<?> config,
            ReadSessionCreator sessionCreator,
            @Nullable BigQueryReadEnumeratorState restoredState) {
        this.context = Preconditions.checkNotNull(context, "context must not be null");
        this.config = Preconditions.checkNotNull(config, "config must not be null");
        this.sessionCreator =
                Preconditions.checkNotNull(sessionCreator, "sessionCreator must not be null");
        this.restoredState = restoredState;
    }

    @Override
    public void start() {
        registerMetrics();
        if (restoredState != null && restoredState.isInitialized()) {
            initialized = true;
            sessionName = restoredState.getSessionName();
            sessionExpireTime = restoredState.getSessionExpireTime();
            pending.addAll(restoredState.getPendingSplits());
            if (sessionExpireTime != null && Instant.now().isAfter(sessionExpireTime)) {
                // Nothing here can recover it: creating a second session would read a second
                // snapshot of the table, so the reads simply fail. Naming the cause is what turns a
                // restart loop into a diagnosable one; reporting it as a terminal error is #391.
                LOG.warn(
                        "The restored BigQuery read session {} expired at {}, which has passed."
                                + " Reads against it will fail; a bounded read must finish within"
                                + " the session's lifetime.",
                        sessionName,
                        sessionExpireTime);
            } else {
                LOG.info(
                        "Restored the BigQuery read session {} (expires at {}) with {} unassigned"
                                + " stream(s); no new session is created.",
                        sessionName,
                        sessionExpireTime,
                        pending.size());
            }
            return;
        }
        CreateReadSessionRequest request = ReadSessionRequests.of(config);
        LOG.info(
                "Creating a BigQuery read session for {} (maxStreamCount={}, "
                        + "preferredMinStreamCount={}, parallelism={}).",
                config.getTable(),
                config.getMaxStreamCount(),
                config.getPreferredMinStreamCount(),
                context.currentParallelism());
        context.callAsync(() -> sessionCreator.create(request), this::onSessionCreated);
    }

    /**
     * Runs on the coordinator thread once session creation finishes, so it needs no
     * synchronization. Throwing is how a split enumerator fails the job from an asynchronous call.
     */
    private void onSessionCreated(@Nullable ReadSession session, @Nullable Throwable error) {
        if (closed) {
            // The job is being torn down; failing it now would turn a clean cancellation into a
            // failure whose cause is our own shutdown.
            return;
        }
        if (error != null) {
            throw new FlinkRuntimeException(
                    "Failed to create a BigQuery read session for "
                            + config.getTable()
                            + "; the source cannot start.",
                    error);
        }
        String schemaJson = session.getAvroSchema().getSchema();
        for (ReadStream stream : session.getStreamsList()) {
            pending.add(new BigQueryReadStreamSplit(stream.getName(), 0L, schemaJson));
        }
        sessionName = session.getName();
        sessionExpireTime = Instant.ofEpochSecond(session.getExpireTime().getSeconds());
        initialized = true;
        readSessionsCreated.inc();
        if (pending.size() < context.currentParallelism()) {
            LOG.warn(
                    "BigQuery returned {} stream(s) for {} at parallelism {}; the subtasks left"
                            + " without a stream finish immediately. The stream count is BigQuery's"
                            + " decision: maxStreamCount only caps it, and a small table is read by"
                            + " one stream however many are asked for.",
                    pending.size(),
                    config.getTable(),
                    context.currentParallelism());
        } else {
            LOG.info(
                    "BigQuery read session {} created with {} stream(s) at parallelism {}; expires"
                            + " at {}.",
                    sessionName,
                    pending.size(),
                    context.currentParallelism(),
                    sessionExpireTime);
        }
        List<Integer> waiting = new ArrayList<>(awaitingInitialization);
        awaitingInitialization.clear();
        for (int subtaskId : waiting) {
            serve(subtaskId);
        }
    }

    /**
     * Registers the enumerator's metrics.
     *
     * <p>The null check is defensive: {@code SplitEnumeratorContext#metricGroup()} carries no
     * nullability annotation, and a context that answered with nothing would otherwise fail the job
     * at startup over its metrics. Flink's own contexts always provide one.
     */
    private void registerMetrics() {
        SplitEnumeratorMetricGroup metricGroup = context.metricGroup();
        if (metricGroup == null) {
            return;
        }
        splitsAssigned = metricGroup.counter(BigQueryMetricNames.SPLITS_ASSIGNED, splitsAssigned);
        splitsReturned = metricGroup.counter(BigQueryMetricNames.SPLITS_RETURNED, splitsReturned);
        readSessionsCreated =
                metricGroup.counter(BigQueryMetricNames.READ_SESSIONS_CREATED, readSessionsCreated);
        metricGroup.setUnassignedSplitsGauge(() -> (long) pending.size());
    }

    @Override
    public void addReader(int subtaskId) {
        // Assignment is pull-based: a reader with no splits asks for one when it starts, and asks
        // again whenever it finishes one. Nothing to do when it merely registers.
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (!initialized) {
            LOG.info(
                    "Source subtask {} asked for a stream before the read session existed; it waits"
                            + " for the session.",
                    subtaskId);
            awaitingInitialization.add(subtaskId);
            return;
        }
        serve(subtaskId);
    }

    /** Hands a subtask the next unassigned split, or finishes it when there is none left. */
    private void serve(int subtaskId) {
        if (!context.registeredReaders().containsKey(subtaskId)) {
            // It failed while it was parked. Its splits, if it held any, come back through
            // addSplitsBack, and it asks again when it restarts.
            LOG.info("Source subtask {} is no longer registered; skipping its request.", subtaskId);
            return;
        }
        BigQueryReadStreamSplit split = pending.poll();
        if (split == null) {
            // Nothing left right now. Nothing records that this subtask was told so: if a failed
            // reader returns a split later, this subtask is served again when it next asks.
            LOG.info("No BigQuery read stream left for source subtask {}.", subtaskId);
            context.signalNoMoreSplits(subtaskId);
            return;
        }
        LOG.info("Assigning {} to source subtask {}.", split, subtaskId);
        splitsAssigned.inc();
        context.assignSplits(
                new SplitsAssignment<>(
                        Collections.singletonMap(subtaskId, Collections.singletonList(split))));
    }

    @Override
    public void addSplitsBack(List<BigQueryReadStreamSplit> splits, int subtaskId) {
        pending.addAll(splits);
        splitsReturned.inc(splits.size());
        LOG.info(
                "Source subtask {} returned {} read stream split(s); they are reassigned on the"
                        + " next request.",
                subtaskId,
                splits.size());
    }

    @Override
    public BigQueryReadEnumeratorState snapshotState(long checkpointId) {
        return new BigQueryReadEnumeratorState(
                initialized, sessionName, sessionExpireTime, new ArrayList<>(pending));
    }

    @Override
    public void close() throws IOException {
        // Runs on the scheduler thread, possibly while session creation is still in flight; the
        // flag
        // is volatile so the completion handler sees it and stays quiet.
        closed = true;
        try {
            Closers.closeAll(sessionCreator);
        } catch (Exception e) {
            throw new IOException("Failed to close the BigQuery read session creator", e);
        }
    }

    @VisibleForTesting
    int pendingSplitCount() {
        return pending.size();
    }
}
