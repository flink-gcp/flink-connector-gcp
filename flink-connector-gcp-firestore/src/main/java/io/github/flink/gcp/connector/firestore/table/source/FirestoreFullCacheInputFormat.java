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

package io.github.flink.gcp.connector.firestore.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.Timestamp;
import com.google.firestore.v1.RunQueryRequest;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.firestore.FirestoreCredentials;
import io.github.flink.gcp.connector.firestore.source.FirestoreSourceConfig;
import io.github.flink.gcp.connector.firestore.source.batch.FetchedDocument;
import io.github.flink.gcp.connector.firestore.source.batch.QuerySplit;
import io.github.flink.gcp.connector.firestore.source.batch.enumerator.QueryPlan;
import io.github.flink.gcp.connector.firestore.source.batch.enumerator.QueryPlanner;
import io.github.flink.gcp.connector.firestore.source.batch.reader.FirestoreSourceReaderMetrics;
import io.github.flink.gcp.connector.firestore.source.batch.reader.FirestoreSplitReader;
import io.github.flink.gcp.connector.firestore.source.batch.reader.QueryPageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The bounded read Flink's full lookup cache loads the table through: the bounded scan's plan and
 * page reads, run start to finish by one input split.
 *
 * <p>Each load plans again in {@link #createInputSplits(int)}, so it reads at the service's time of
 * that planning and the next load reads a newer snapshot. The table's {@code scan.read-time} never
 * reaches this configuration: a lookup reads documents as they are, whatever the scan's snapshot
 * is. The planner is closed before the split is returned, because a plan is only queries and a read
 * time.
 *
 * <p>One split per load, reading the planned queries one after another through one page reader, and
 * a collection group planned as one partition. Flink runs the splits of one load on several threads
 * and gives every one of them the same cache-entry serializer, whose copy of a map that is not
 * already binary goes through reuse buffers the serializer keeps (Flink 2.2.1 and 1.20.5, {@code
 * InputFormatCacheLoader} and {@code MapDataSerializer}), so two splits loading {@code MAP} values
 * at once could corrupt them silently. A split whose page fails fails the load: Flink publishes no
 * part of it, and the lookups that follow fail until the job restarts.
 *
 * <p>No client metrics are exported to Flink: Flink's full cache configures and opens its copy of
 * this format with no runtime context, so no metric group reaches it. The load logs how many
 * documents it handed on instead; the bill also counts the planner's probe reads and any page the
 * client library's retry overfilled.
 */
@Internal
final class FirestoreFullCacheInputFormat extends GenericInputFormat<RowData> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(FirestoreFullCacheInputFormat.class);

    /** One partition takes no {@code PartitionQuery} call, and the load reads serially anyway. */
    private static final int PARTITION_COUNT = 1;

    private final FirestoreSourceConfig<RowData> config;

    @Nullable private transient QueryPageReader pageReader;
    @Nullable private transient FirestoreSplitReader reader;
    @Nullable private transient LoadSplit split;
    private transient Set<String> unfinished = new HashSet<>();
    private transient ArrayDeque<RowData> pending = new ArrayDeque<>();
    @Nullable private transient Collector<RowData> pendingRows;
    private transient long documents;

    FirestoreFullCacheInputFormat(FirestoreSourceConfig<RowData> config) {
        this.config = Preconditions.checkNotNull(config, "config must not be null");
        Preconditions.checkArgument(
                config.getReadTime() == null,
                "a full-cache load reads at the time it is planned, so its configuration carries"
                        + " no read time");
        Preconditions.checkArgument(
                config.getPartitionCount() == null,
                "a full-cache load reads a collection group as one partition, so its configuration"
                        + " carries no partition count");
    }

    /** The scan configuration each load reads with, for the module's tests. */
    @VisibleForTesting
    FirestoreSourceConfig<RowData> getConfig() {
        return config;
    }

    /**
     * Plans one load: takes the service's current time and the queries to read at it.
     *
     * @param minNumSplits ignored; Flink's loader asks for one, and a second split would run on
     *     another thread beside the first
     * @return the one split of the load
     * @throws IOException if the credentials cannot be loaded or planning fails
     */
    @Override
    public LoadSplit[] createInputSplits(int minNumSplits) throws IOException {
        GoogleCredentials credentials =
                FirestoreCredentials.load(config.getServiceAccountKeyFile());
        QueryPlan plan;
        try (QueryPlanner planner = config.getPlannerFactory().create()) {
            planner.useCredentials(credentials);
            plan = planner.plan(config, PARTITION_COUNT);
        }
        LOG.info(
                "Planned a full lookup cache load of {} as {} query(ies) at read time {}.",
                config.getDatabase(),
                plan.getQueries().size(),
                plan.getReadTime());
        return new LoadSplit[] {new LoadSplit(plan.getQueries(), plan.getReadTime())};
    }

    /**
     * Opens the load's split for reading.
     *
     * @param split the split {@link #createInputSplits(int)} returned
     * @throws IOException if the credentials cannot be loaded
     * @throws IllegalArgumentException if the split was not planned by this format
     */
    @Override
    public void open(GenericInputSplit split) throws IOException {
        Preconditions.checkArgument(
                split instanceof LoadSplit, "not a split of a full-cache load: %s", split);
        super.open(split);
        LoadSplit load = (LoadSplit) split;
        this.split = load;
        // Transient fields start null in the copy Flink deserializes for each load.
        unfinished = new HashSet<>();
        pending = new ArrayDeque<>();
        pendingRows = new PendingRows(pending);
        documents = 0;
        try {
            QueryPageReader openReader = config.getPageReader();
            pageReader = openReader;
            openReader.useCredentials(FirestoreCredentials.load(config.getServiceAccountKeyFile()));
            FirestoreSplitReader splitReader =
                    new FirestoreSplitReader(
                            config.getDatabase(),
                            openReader,
                            config.getPageSize(),
                            FirestoreSourceReaderMetrics.unregistered());
            List<QuerySplit> queries = load.toQuerySplits();
            for (QuerySplit query : queries) {
                unfinished.add(query.splitId());
            }
            splitReader.handleSplitsChanges(new SplitsAddition<>(queries));
            reader = splitReader;
        } catch (Throwable e) {
            // Flink's loader also closes the format after a failed open; closing here keeps this
            // method from relying on that, and close() is idempotent.
            Closers.closeAllSuppressing(e, this::close);
            throw e;
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        while (pending.isEmpty()) {
            if (unfinished.isEmpty() || reader == null) {
                return true;
            }
            RecordsWithSplitIds<FetchedDocument> batch = reader.fetch();
            boolean progressed = false;
            try {
                while (batch.nextSplit() != null) {
                    FetchedDocument document;
                    while ((document = batch.nextRecordFromSplit()) != null) {
                        progressed = true;
                        documents++;
                        // The table's deserializer opens itself on first use, so no context is
                        // needed here, and a load has none to give.
                        config.getDeserializer().deserialize(document.getDocument(), pendingRows);
                    }
                }
                progressed |= unfinished.removeAll(batch.finishedSplits());
            } finally {
                batch.recycle();
            }
            if (!progressed) {
                // The split reader answers with nothing only when it holds no query, which the
                // queries added in open() rule out until each has finished.
                throw new IllegalStateException(
                        "The Firestore full-cache load returned no documents and finished no query"
                                + " while "
                                + unfinished.size()
                                + " remained.");
            }
        }
        return false;
    }

    @Override
    @Nullable
    public RowData nextRecord(RowData reuse) throws IOException {
        return reachedEnd() ? null : pending.poll();
    }

    @Override
    public void close() throws IOException {
        QueryPageReader openReader = pageReader;
        FirestoreSplitReader openSplitReader = reader;
        LoadSplit openSplit = split;
        boolean complete = unfinished != null && unfinished.isEmpty();
        pageReader = null;
        reader = null;
        split = null;
        unfinished = new HashSet<>();
        pending = new ArrayDeque<>();
        pendingRows = null;
        if (openSplitReader != null) {
            openSplitReader.close();
        }
        if (openSplit != null) {
            LOG.info(
                    "{} a full lookup cache load of {} at read time {} after reading {}"
                            + " document(s).",
                    complete ? "Finished" : "Stopped",
                    config.getDatabase(),
                    openSplit.getReadTime(),
                    documents);
        }
        if (openReader != null) {
            openReader.close();
        }
    }

    /** The planned queries of one load and the read time they share. */
    static final class LoadSplit extends GenericInputSplit {

        private static final long serialVersionUID = 1L;

        private final List<RunQueryRequest> queries;
        private final Timestamp readTime;

        LoadSplit(List<RunQueryRequest> queries, Timestamp readTime) {
            super(0, 1);
            Preconditions.checkArgument(!queries.isEmpty(), "a load reads at least one query");
            this.queries = new ArrayList<>(queries);
            this.readTime = Preconditions.checkNotNull(readTime, "readTime must not be null");
        }

        /** Returns the read time every query of the load reads at. */
        Timestamp getReadTime() {
            return readTime;
        }

        /** Returns how many queries the load reads. */
        int queryCount() {
            return queries.size();
        }

        private List<QuerySplit> toQuerySplits() {
            List<QuerySplit> splits = new ArrayList<>();
            for (int i = 0; i < queries.size(); i++) {
                splits.add(new QuerySplit(String.valueOf(i), queries.get(i), readTime));
            }
            return splits;
        }

        @Override
        public String toString() {
            return "LoadSplit{queries=" + queries.size() + ", readTime=" + readTime + '}';
        }
    }

    /** Collects what the deserializer emits for one document into the pending queue. */
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
