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

package io.github.flink.gcp.connector.datastore.table.source;

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
import com.google.datastore.v1.Query;
import com.google.datastore.v1.RunQueryRequest;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.datastore.DatastoreCredentials;
import io.github.flink.gcp.connector.datastore.source.DatastoreSourceConfig;
import io.github.flink.gcp.connector.datastore.source.batch.FetchedEntity;
import io.github.flink.gcp.connector.datastore.source.batch.QuerySplit;
import io.github.flink.gcp.connector.datastore.source.batch.SplittableQueries;
import io.github.flink.gcp.connector.datastore.source.batch.enumerator.QueryPlan;
import io.github.flink.gcp.connector.datastore.source.batch.enumerator.QueryPlanner;
import io.github.flink.gcp.connector.datastore.source.batch.reader.DatastoreSourceReaderMetrics;
import io.github.flink.gcp.connector.datastore.source.batch.reader.DatastoreSplitReader;
import io.github.flink.gcp.connector.datastore.source.batch.reader.QueryPageReader;
import io.github.flink.gcp.connector.datastore.source.serializer.DatastoreEntityMetadataDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;

/**
 * The bounded read Flink's full lookup cache loads the table through: the bounded scan's plan and
 * page reads, run start to finish by one input split.
 *
 * <p>Each load plans again in {@link #createInputSplits(int)}, so it reads at the service's time of
 * that planning and the next load reads a newer snapshot. The table's {@code scan.read-time} never
 * reaches this configuration: a lookup reads entities as they are, whatever the scan's snapshot is.
 * The planner is closed before the split is returned, because a plan is only a query and a read
 * time.
 *
 * <p>One split per load, reading the kind as one key range: one query, read page by page through
 * the scan's {@link DatastoreSplitReader}, so paging, cursor continuation and a page that moved its
 * cursor without returning an entity are the scan's. Flink runs the splits of one load on several
 * threads and gives every one of them the same cache-entry serializer, whose copy of a map that is
 * not already binary goes through reuse buffers the serializer keeps (Flink 2.2.1 and 1.20.5,
 * {@code InputFormatCacheLoader} and {@code MapDataSerializer}). The table maps no {@code MAP} type
 * today, but a split per key range would become exposed as soon as one is mapped, and cutting key
 * ranges that one split then reads serially would only add the planner's statistics read and {@code
 * __scatter__} sample to every load. A split whose page fails fails the load: Flink publishes no
 * part of it, and the lookups that follow fail until the job restarts, except a few right after a
 * failed first load, which Flink answers from the empty cache before it records the failure
 * (#2225).
 *
 * <p>No client metrics are exported to Flink: Flink's full cache configures and opens its copy of
 * this format with no runtime context, so no metric group reaches it. The load logs how many
 * entities it handed on instead; the bill also counts the planner's probe read.
 */
@Internal
final class DatastoreFullCacheInputFormat extends GenericInputFormat<RowData> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(DatastoreFullCacheInputFormat.class);

    /** The parallelism the planner is told; it only floors an estimated split count, never used. */
    private static final int PLANNING_PARALLELISM = 1;

    private final DatastoreSourceConfig<RowData> config;

    @Nullable private transient QueryPageReader pageReader;
    @Nullable private transient DatastoreSplitReader reader;
    @Nullable private transient LoadSplit split;
    private transient boolean finished;
    private transient ArrayDeque<RowData> pending = new ArrayDeque<>();
    @Nullable private transient Collector<RowData> pendingRows;
    private transient long entities;

    DatastoreFullCacheInputFormat(DatastoreSourceConfig<RowData> config) {
        this.config = Preconditions.checkNotNull(config, "config must not be null");
        Preconditions.checkArgument(
                config.getReadTime() == null,
                "a full-cache load reads at the time it is planned, so its configuration carries"
                        + " no read time");
        Query query = config.getQuery();
        Preconditions.checkArgument(
                query != null, "a full-cache load reads the table's query, not a GQL query");
        Preconditions.checkArgument(
                config.getSplitCount() != null
                        ? config.getSplitCount() == 1
                        : SplittableQueries.whyNotSplittable(query) != null,
                "a full-cache load reads one key range, so its configuration asks for one split"
                        + " of a query that could be cut into several");
        Preconditions.checkArgument(
                config.getDeserializer() instanceof DatastoreEntityMetadataDeserializationSchema,
                "a full-cache load fills the table's metadata columns, so its deserializer reads"
                        + " the metadata");
    }

    /** The scan configuration each load reads with, for the module's tests. */
    @VisibleForTesting
    DatastoreSourceConfig<RowData> getConfig() {
        return config;
    }

    /**
     * Plans one load: takes the service's current time and the query to read at it.
     *
     * @param minNumSplits ignored; Flink's loader asks for one, and a second split would run on
     *     another thread beside the first
     * @return the one split of the load
     * @throws IOException if the credentials cannot be loaded or planning fails
     */
    @Override
    public LoadSplit[] createInputSplits(int minNumSplits) throws IOException {
        GoogleCredentials credentials =
                DatastoreCredentials.load(config.getServiceAccountKeyFile());
        QueryPlan plan;
        try (QueryPlanner planner = config.getPlannerFactory().create()) {
            planner.useCredentials(credentials);
            plan = planner.plan(config, PLANNING_PARALLELISM);
        }
        if (plan.getRequests().size() != 1) {
            // The configuration asks for one key range, which the planner answers with one query.
            throw new IOException(
                    "Datastore planned a full lookup cache load as "
                            + plan.getRequests().size()
                            + " queries; a load reads one.");
        }
        LOG.info(
                "Planned a full lookup cache load of {} at read time {}.",
                describeTable(),
                plan.getReadTime());
        return new LoadSplit[] {new LoadSplit(plan.getRequests().get(0), plan.getReadTime())};
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
        finished = false;
        pending = new ArrayDeque<>();
        pendingRows = new PendingRows(pending);
        entities = 0;
        try {
            QueryPageReader openReader = config.getPageReader();
            pageReader = openReader;
            openReader.useCredentials(DatastoreCredentials.load(config.getServiceAccountKeyFile()));
            DatastoreSplitReader splitReader =
                    new DatastoreSplitReader(
                            config.getDatabase(),
                            openReader,
                            config.getPageSize(),
                            DatastoreSourceReaderMetrics.unregistered());
            splitReader.handleSplitsChanges(new SplitsAddition<>(List.of(load.toQuerySplit())));
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
        // A fetch reads one page. The split reader fails a page that claims more results without
        // moving its cursor, so a page with no entity is progress and this loop cannot spin.
        while (pending.isEmpty() && !finished && reader != null) {
            RecordsWithSplitIds<FetchedEntity> batch = reader.fetch();
            try {
                while (batch.nextSplit() != null) {
                    FetchedEntity entity;
                    while ((entity = batch.nextRecordFromSplit()) != null) {
                        entities++;
                        // The table's deserializer has nothing to open, so no context is needed
                        // here, and a load has none to give.
                        ((DatastoreEntityMetadataDeserializationSchema<RowData>)
                                        config.getDeserializer())
                                .deserialize(entity.getEntity(), entity.getMetadata(), pendingRows);
                    }
                }
                finished = batch.finishedSplits().contains(LoadSplit.QUERY_ID);
            } finally {
                batch.recycle();
            }
        }
        return pending.isEmpty();
    }

    @Override
    @Nullable
    public RowData nextRecord(RowData reuse) throws IOException {
        return reachedEnd() ? null : pending.poll();
    }

    @Override
    public void close() throws IOException {
        QueryPageReader openReader = pageReader;
        DatastoreSplitReader openSplitReader = reader;
        LoadSplit openSplit = split;
        boolean complete = finished;
        pageReader = null;
        reader = null;
        split = null;
        finished = false;
        pending = new ArrayDeque<>();
        pendingRows = null;
        if (openSplitReader != null) {
            openSplitReader.close();
        }
        if (openSplit != null) {
            LOG.info(
                    "{} a full lookup cache load of {} at read time {} after reading {}"
                            + " entity(ies).",
                    complete ? "Finished" : "Stopped",
                    describeTable(),
                    openSplit.getReadTime(),
                    entities);
        }
        if (openReader != null) {
            openReader.close();
        }
    }

    /** Names the loaded kind for a log line, so that two tables' loads can be told apart. */
    private String describeTable() {
        String namespace = config.getNamespace();
        return "kind "
                + SplittableQueries.kindOf(config.getQuery())
                + (namespace.isEmpty() ? "" : " in namespace " + namespace)
                + " of "
                + config.getDatabase();
    }

    /** The planned query of one load and its read time. */
    static final class LoadSplit extends GenericInputSplit {

        private static final long serialVersionUID = 1L;

        /** The id of the load's one query split in the split reader. */
        static final String QUERY_ID = "0";

        private final RunQueryRequest query;
        private final Timestamp readTime;

        LoadSplit(RunQueryRequest query, Timestamp readTime) {
            super(0, 1);
            this.query = Preconditions.checkNotNull(query, "query must not be null");
            this.readTime = Preconditions.checkNotNull(readTime, "readTime must not be null");
        }

        /** Returns the read time the load reads at. */
        Timestamp getReadTime() {
            return readTime;
        }

        /** Returns the query the load reads. */
        RunQueryRequest getQuery() {
            return query;
        }

        private QuerySplit toQuerySplit() {
            return new QuerySplit(QUERY_ID, query, readTime);
        }

        @Override
        public String toString() {
            return "LoadSplit{readTime=" + readTime + '}';
        }
    }

    /** Collects what the deserializer emits for one entity into the pending queue. */
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
