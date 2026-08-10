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
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import io.github.flink.gcp.connector.base.source.EnumeratorCounters;
import io.github.flink.gcp.connector.base.source.PullAssignmentSplitEnumerator;
import io.github.flink.gcp.connector.bigquery.BigQueryMetricNames;
import io.github.flink.gcp.connector.bigquery.source.BigQuerySourceConfig;
import io.github.flink.gcp.connector.bigquery.source.split.BigQueryReadStreamSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Creates the read session once and hands its streams out one at a time.
 *
 * <p>The assignment protocol — pull-based, and keeping no record of which subtask holds which split
 * — is {@link PullAssignmentSplitEnumerator}'s, and the reasoning behind it lives there. What this
 * class adds is the read session: elasticity comes from asking BigQuery for more streams than there
 * are subtasks rather than from splitting a stream at runtime, so a stream that takes longer than
 * its siblings does not hold up a subtask that could be reading another.
 *
 * <p><b>A restore never creates a second session.</b> A second session would pin a second snapshot
 * of the table, so a failed-over job would read it as of two different instants. The checkpointed
 * flag is what prevents that, and it is also why nothing here can recover an expired restored
 * session: creating another one is precisely what must not happen, so the expiry is named in the
 * log and the reads fail.
 */
@Internal
public class BigQueryReadSplitEnumerator
        extends PullAssignmentSplitEnumerator<
                BigQueryReadStreamSplit, BigQueryReadEnumeratorState, ReadSession> {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryReadSplitEnumerator.class);

    private final BigQuerySourceConfig<?> config;
    private final ReadSessionCreator sessionCreator;
    @Nullable private final BigQueryReadEnumeratorState restoredState;

    @Nullable private String sessionName;
    @Nullable private Instant sessionExpireTime;

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
        super(
                context,
                sessionCreator,
                "read stream split",
                creationFailureMessage(config),
                "Failed to close the BigQuery read session creator.");
        this.config = config;
        this.sessionCreator = sessionCreator;
        this.restoredState = restoredState;
    }

    /**
     * Builds the message the job fails with when the session cannot be created.
     *
     * <p>Static because it is evaluated as a {@code super(...)} argument, which is also where the
     * configuration has to be checked: without the check the concatenation throws a {@link
     * NullPointerException} naming nothing.
     */
    private static String creationFailureMessage(BigQuerySourceConfig<?> config) {
        Preconditions.checkNotNull(config, "config must not be null");
        return "Failed to create a BigQuery read session for "
                + config.getTable()
                + "; the source cannot start.";
    }

    @Override
    protected boolean restore() {
        if (restoredState == null || !restoredState.isInitialized()) {
            return false;
        }
        sessionName = restoredState.getSessionName();
        sessionExpireTime = restoredState.getSessionExpireTime();
        addPlannedSplits(restoredState.getPendingSplits());
        if (sessionExpireTime != null && Instant.now().isAfter(sessionExpireTime)) {
            // Nothing here can recover it: creating a second session would read a second snapshot
            // of the table, so the reads simply fail. A warning and not a thrown failure,
            // deliberately — the expiry is BigQuery's to apply and this is a client clock, so
            // refusing here would fail a read the service might still have served. The failure a
            // reader does meet carries the same explanation, from the expiry its split carries.
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
                    pendingSplitCount());
        }
        return true;
    }

    @Override
    protected void onPlanningStarted() {
        LOG.info(
                "Creating a BigQuery read session for {} (maxStreamCount={}, "
                        + "preferredMinStreamCount={}, parallelism={}).",
                config.getTable(),
                config.getMaxStreamCount(),
                config.getPreferredMinStreamCount(),
                context.currentParallelism());
    }

    @Override
    protected ReadSession plan() throws Exception {
        return sessionCreator.create(ReadSessionRequests.of(config));
    }

    @Override
    protected void onPlanned(ReadSession session) {
        String schemaJson = session.getAvroSchema().getSchema();
        sessionName = session.getName();
        sessionExpireTime = Instant.ofEpochSecond(session.getExpireTime().getSeconds());
        List<BigQueryReadStreamSplit> splits = new ArrayList<>();
        for (ReadStream stream : session.getStreamsList()) {
            splits.add(
                    new BigQueryReadStreamSplit(
                            stream.getName(), 0L, schemaJson, sessionExpireTime));
        }
        addPlannedSplits(splits);
        if (splits.size() < context.currentParallelism()) {
            LOG.warn(
                    "BigQuery returned {} stream(s) for {} at parallelism {}; the subtasks left"
                            + " without a stream finish immediately. The stream count is BigQuery's"
                            + " decision: maxStreamCount only caps it, and a small table is read by"
                            + " one stream however many are asked for.",
                    splits.size(),
                    config.getTable(),
                    context.currentParallelism());
        } else {
            LOG.info(
                    "BigQuery read session {} created with {} stream(s) at parallelism {}; expires"
                            + " at {}.",
                    sessionName,
                    splits.size(),
                    context.currentParallelism(),
                    sessionExpireTime);
        }
    }

    @Override
    protected EnumeratorCounters registerCounters(SplitEnumeratorMetricGroup metricGroup) {
        return new EnumeratorCounters(
                metricGroup.counter(
                        BigQueryMetricNames.SPLITS_ASSIGNED, new ThreadSafeSimpleCounter()),
                metricGroup.counter(
                        BigQueryMetricNames.SPLITS_RETURNED, new ThreadSafeSimpleCounter()),
                metricGroup.counter(
                        BigQueryMetricNames.READ_SESSIONS_CREATED, new ThreadSafeSimpleCounter()));
    }

    @Override
    public BigQueryReadEnumeratorState snapshotState(long checkpointId) {
        return new BigQueryReadEnumeratorState(
                isPlanned(), sessionName, sessionExpireTime, pendingSplits());
    }
}
