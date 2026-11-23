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

package io.github.flink.gcp.connector.bigquery.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.bigquery.source.BigQuerySourceConfig;
import io.github.flink.gcp.connector.bigquery.source.enumerator.ReadClientSessionCreator;
import io.github.flink.gcp.connector.bigquery.source.enumerator.ReadSessionCreator;
import io.github.flink.gcp.connector.bigquery.source.enumerator.ReadSessionRequests;
import io.github.flink.gcp.connector.bigquery.source.reader.BigQuerySourceReaderMetrics;
import io.github.flink.gcp.connector.bigquery.source.reader.BigQuerySplitReader;
import io.github.flink.gcp.connector.bigquery.source.reader.RowStreamOpener;
import io.github.flink.gcp.connector.bigquery.source.split.ReadStreamSplit;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The bounded read Flink's full lookup cache loads a {@code bigquery} table through: one Storage
 * Read session per load, whose streams one split reader of the scan reads to the end.
 *
 * <p>Each load is one input split, whatever count Flink asks for. Flink runs the splits of one load
 * on several threads and gives every one of them the same cache-entry serializer, whose copy of a
 * map that is not already binary goes through reuse buffers the serializer keeps (Flink 2.2.1 and
 * 1.20.5, {@code InputFormatCacheLoader} and {@code MapDataSerializer}). This table produces {@code
 * MAP} and {@code MULTISET} values as generic maps, so a second split could corrupt them silently
 * (#2217). The configuration therefore asks for one read stream: a single split reads its streams
 * one after another, so further streams would add a {@code ReadRows} call each and read nothing
 * faster.
 *
 * <p>The session is created when the split is opened, not when it is planned. Flink plans the
 * splits on this format and opens a copy it deserializes for each split, closing only the copy
 * (Flink 2.2.1 and 1.20.5, {@code InputFormatCacheLoader} and {@code InputSplitCacheLoadTask}), so
 * a client opened while planning would have nothing to close it. The session-creating client is
 * closed as soon as the session exists, which leaves the session itself readable; the stream
 * reader's client is closed with the load. Each load therefore reads one snapshot, the table as the
 * session saw it when it was created, and the next load reads a newer one.
 *
 * <p>A stream that fails fails the load: Flink publishes no part of it, and the lookups that follow
 * fail until the job restarts, except a few right after a failed first load, which Flink answers
 * from the empty cache before it records the failure (#2225). The client library's own {@code
 * ReadRows} retry resumes a broken stream within the load, bounded as for a scan. An exception
 * releasing the load once it is read is reported as an {@link IOException}, which Flink's loader
 * logs while keeping the rows; an unchecked one escaping its {@code finally} block would discard
 * them. No client metrics reach Flink, which configures and opens its copy with no runtime context;
 * each load logs what it read instead.
 *
 * <p>Opening, fetching and closing run inside {@link GrpcEventLoops#interruptibleReleases()}: Flink
 * stops a load by interrupting it and fails the lookup's close when the load has not ended 10 s
 * later ({@code CacheLoader.close()}, Flink 1.20.5, 2.2.1 and 2.3.0), so a release of the
 * connector's last gRPC lease on an interrupted load stops waiting for the event-loop group once
 * the wait's first second has passed (#2269). The read client is created lazily by the first fetch,
 * and a creation that fails releases its lease there.
 */
@Internal
final class BigQueryFullCacheInputFormat extends GenericInputFormat<RowData> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryFullCacheInputFormat.class);

    private final BigQuerySourceConfig<RowData> config;

    @Nullable private transient BigQuerySplitReader reader;
    @Nullable private transient RowStreamOpener opener;
    @Nullable private transient BigQuerySourceReaderMetrics metrics;
    @Nullable private transient String sessionName;
    @Nullable private transient Instant sessionExpireTime;
    private transient int streamCount;
    private transient Set<String> unfinished = new HashSet<>();
    private transient ArrayDeque<RowData> pending = new ArrayDeque<>();
    @Nullable private transient Collector<RowData> pendingRows;
    private transient long rows;
    private transient boolean stoppedReading;

    BigQueryFullCacheInputFormat(BigQuerySourceConfig<RowData> config) {
        this.config = Preconditions.checkNotNull(config, "config must not be null");
        Preconditions.checkArgument(
                config.getTable() != null && config.getQuery() == null,
                "a full-cache load reads a table, not a query's result");
        Preconditions.checkArgument(
                !config.isMaterializeViewsEnabled(),
                "a full-cache load reads a table, so its configuration materializes no view");
        Preconditions.checkArgument(
                config.getSnapshotTime() == null,
                "a full-cache load reads the table as it is when the load starts, so its"
                        + " configuration carries no snapshot time");
        Preconditions.checkArgument(
                config.getMaxStreamCount() == 1 && config.getPreferredMinStreamCount() == 0,
                "a full-cache load is one split that reads its streams serially, so its"
                        + " configuration asks for one stream");
    }

    /** The scan configuration each load reads with, for the module's tests. */
    @VisibleForTesting
    BigQuerySourceConfig<RowData> getConfig() {
        return config;
    }

    /**
     * Returns the one split a load reads, whatever count is asked for. Flink's loader asks for one,
     * and a second split would create a second session and read the whole table again.
     */
    @Override
    public GenericInputSplit[] createInputSplits(int numSplits) {
        return new GenericInputSplit[] {new GenericInputSplit(0, 1)};
    }

    /**
     * Creates the load's read session and opens its streams for reading.
     *
     * @param split the split {@link #createInputSplits(int)} returned
     * @throws IOException if the session cannot be created
     */
    @Override
    public void open(GenericInputSplit split) throws IOException {
        try (GrpcEventLoops.InterruptibleReleases releases =
                GrpcEventLoops.interruptibleReleases()) {
            openLoad(split);
        }
    }

    private void openLoad(GenericInputSplit split) throws IOException {
        super.open(split);
        // Transient fields start null in the copy Flink deserializes for each load.
        unfinished = new HashSet<>();
        pending = new ArrayDeque<>();
        pendingRows = new PendingRows(pending);
        rows = 0;
        stoppedReading = false;
        streamCount = 0;
        sessionName = null;
        sessionExpireTime = null;
        if (stopIfInterrupted("before creating a read session")) {
            return;
        }
        try {
            ReadSession session;
            try {
                session = createSession();
            } catch (InterruptedIOException e) {
                // The creator cancels the call when its wait is interrupted and keeps the
                // interrupt, which is Flink closing the cache: the load ends, as below. A failure
                // releasing the client rides on the exception, so it is logged, not dropped.
                if (stopIfInterrupted("after its wait for a read session was interrupted")) {
                    for (Throwable release : e.getSuppressed()) {
                        LOG.warn(
                                "Releasing the read-session client of the interrupted full lookup"
                                        + " cache load of {} failed.",
                                config.describeInput(),
                                release);
                    }
                    return;
                }
                throw e;
            }
            sessionName = session.getName();
            sessionExpireTime =
                    Instant.ofEpochSecond(
                            session.getExpireTime().getSeconds(),
                            session.getExpireTime().getNanos());
            List<ReadStreamSplit> streams = new ArrayList<>();
            // An empty table answers with no stream, and its load reads no row.
            for (ReadStream stream : session.getStreamsList()) {
                streams.add(
                        // No expiry on the split: the split reader would explain an expired
                        // session with a scan's remedies, and reachedEnd() gives a load's.
                        new ReadStreamSplit(
                                stream.getName(), 0L, session.getAvroSchema().getSchema(), null));
                unfinished.add(stream.getName());
            }
            streamCount = streams.size();
            LOG.info(
                    "Created read session {} with {} stream(s) for a full lookup cache load of {},"
                            + " expiring at {}.",
                    sessionName,
                    streamCount,
                    config.describeInput(),
                    sessionExpireTime);
            // An interrupt can also arrive after the session's call completed, before reading.
            if (stopIfInterrupted("after creating its read session, before reading")) {
                return;
            }
            metrics = BigQuerySourceReaderMetrics.unregistered();
            opener = config.getRowStreamOpener();
            // Before the first stream opens: the client captures the listener when it is built.
            opener.setRetryListener(metrics::readRetried);
            reader =
                    new BigQuerySplitReader(
                            opener,
                            config.getMaxRecordsPerFetch(),
                            config.getMaxBytesPerFetch(),
                            config.getDeserializer().getReaderSchema(),
                            metrics);
            reader.handleSplitsChanges(new SplitsAddition<>(streams));
        } catch (Throwable e) {
            // Flink's loader also closes the format after a failed open; closing here keeps this
            // method from relying on that, and close() is idempotent.
            Closers.closeAllSuppressing(e, this::close);
            throw e;
        }
    }

    /**
     * Stops the load when Flink has interrupted the loading thread, which it does only when it
     * closes the cache; it then expects the load to end with the interrupt kept rather than fail
     * ({@code InputFormatCacheLoader}). Checked before the session is created, when its creation is
     * interrupted, after it is created, and when a read fails, so {@link #reachedEnd()} then
     * answers {@code true}.
     */
    private boolean stopIfInterrupted(String when) {
        if (!Thread.currentThread().isInterrupted()) {
            return false;
        }
        LOG.info(
                "Stopping the full lookup cache load of {} {}: the loading thread was"
                        + " interrupted.",
                config.describeInput(),
                when);
        return true;
    }

    /**
     * Creates the load's read session and releases the client that created it, keeping a JVM-fatal
     * release failure on top as {@link Closers#closeAllSuppressing(Throwable, AutoCloseable,
     * AutoCloseable...)} does. A refusal to read a view is explained for a lookup, which cannot
     * follow the creator's own advice to read through a query.
     */
    private ReadSession createSession() throws IOException {
        ReadSessionCreator creator = config.getSessionCreatorFactory().create();
        ReadSession session;
        try {
            session = creator.create(ReadSessionRequests.of(config, config.getTable()));
        } catch (Throwable e) {
            Closers.closeAllSuppressing(e, creator);
            if (ReadClientSessionCreator.refusedAsView(e)) {
                throw new IOException(
                        "BigQuery refused to read "
                                + config.describeInput()
                                + " for a full lookup cache load because it is a view. A lookup"
                                + " join reads a table's storage, and a table that sets"
                                + " 'scan.query', or 'scan.materialize-views' to true, cannot be"
                                + " looked up,"
                                + " so the advice below to read through query(...) does not apply"
                                + " here: write the view's result to a table and look that up.",
                        e);
            }
            throw e;
        }
        creator.close();
        return session;
    }

    @Override
    public boolean reachedEnd() throws IOException {
        while (pending.isEmpty() && reader != null && !unfinished.isEmpty() && !stoppedReading) {
            RecordsWithSplitIds<GenericRecord> batch;
            try (GrpcEventLoops.InterruptibleReleases releases =
                    GrpcEventLoops.interruptibleReleases()) {
                batch = reader.fetch();
            } catch (IOException e) {
                // The client library turns an interrupt of a waiting ReadRows call into a failure
                // and sets the interrupt again; Flink interrupts only to close the cache.
                if (stopIfInterrupted("after a read failed")) {
                    stoppedReading = true;
                    return true;
                }
                throw explainExpiry(e);
            }
            boolean progressed = false;
            try {
                while (batch.nextSplit() != null) {
                    GenericRecord row;
                    while ((row = batch.nextRecordFromSplit()) != null) {
                        progressed = true;
                        rows++;
                        config.getDeserializer().deserialize(row, pendingRows);
                    }
                }
                Set<String> finished = batch.finishedSplits();
                progressed |= unfinished.removeAll(finished);
            } finally {
                batch.recycle();
            }
            if (!progressed) {
                // The split reader hands over at least one row or finishes its stream on every
                // fetch that nothing woke up, and nothing wakes this one; a fetch with neither
                // means it holds no stream this load still waits for, which would loop forever.
                throw new IllegalStateException(
                        "The read of streams "
                                + unfinished
                                + " of read session "
                                + sessionName
                                + " returned neither a row nor a finished stream.");
            }
        }
        return pending.isEmpty();
    }

    /**
     * Adds a load's explanation to a read failure past the session's expiry, which the split
     * reader, given no expiry, leaves out. Whether BigQuery reports expiry as this failure is not
     * asserted: the sentence says only what is true of the session either way, as the scan's does.
     */
    private IOException explainExpiry(IOException failure) {
        Instant expireTime = sessionExpireTime;
        if (expireTime == null || !Instant.now().isAfter(expireTime)) {
            return failure;
        }
        return new IOException(
                "The full lookup cache load of "
                        + config.describeInput()
                        + " failed after its read session "
                        + sessionName
                        + " expired at "
                        + expireTime
                        + ". A read session lives six hours from its creation, so a load has to"
                        + " finish inside that window. Once Flink records a failed load, it fails"
                        + " the lookups that follow until the job restarts and loads again with a"
                        + " new session; select fewer"
                        + " of the table's columns in the join, or narrow the load with"
                        + " 'scan.row-restriction', so that a load finishes in time.",
                failure);
    }

    @Override
    @Nullable
    public RowData nextRecord(RowData reuse) throws IOException {
        return reachedEnd() ? null : pending.poll();
    }

    @Override
    public void close() throws IOException {
        try (GrpcEventLoops.InterruptibleReleases releases =
                GrpcEventLoops.interruptibleReleases()) {
            closeLoad();
        }
    }

    private void closeLoad() throws IOException {
        BigQuerySplitReader openReader = reader;
        RowStreamOpener openOpener = opener;
        BigQuerySourceReaderMetrics loadMetrics = metrics;
        String session = sessionName;
        // Complete only when a reader opened, reported every stream finished and every row was
        // taken: a session that could not be created registered no stream, which an empty set
        // would read as finished, and a cancelled load can leave rows unread.
        boolean complete =
                openReader != null
                        && unfinished != null
                        && unfinished.isEmpty()
                        && (pending == null || pending.isEmpty());
        reader = null;
        opener = null;
        metrics = null;
        sessionName = null;
        sessionExpireTime = null;
        unfinished = new HashSet<>();
        pending = new ArrayDeque<>();
        pendingRows = null;
        try {
            // The reader's open stream before the client it reads through.
            Closers.closeAll(openReader, openOpener);
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to release a BigQuery full lookup cache load.", e);
        } finally {
            if (session != null) {
                LOG.info(
                        "{} a full lookup cache load of {} from read session {} ({} stream(s))"
                                + " after reading {} row(s), {} serialized byte(s) and {} retried"
                                + " attempt(s).",
                        complete ? "Finished" : "Stopped",
                        config.describeInput(),
                        session,
                        streamCount,
                        rows,
                        loadMetrics == null ? 0 : loadMetrics.getBytesRead(),
                        loadMetrics == null ? 0 : loadMetrics.getReadRetries());
            }
        }
    }

    /** Collects what the deserializer emits for one row into the pending queue. */
    private static final class PendingRows implements Collector<RowData> {

        private final ArrayDeque<RowData> pending;

        private PendingRows(ArrayDeque<RowData> pending) {
            this.pending = pending;
        }

        @Override
        public void collect(RowData record) {
            pending.add(record);
        }

        @Override
        public void close() {}
    }
}
