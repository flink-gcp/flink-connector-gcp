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
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.Timestamp;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.RunQueryRequest;
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
import io.github.flink.gcp.connector.firestore.common.table.FullCacheInputFormat;

import java.io.IOException;
import java.util.List;

/**
 * The bounded read Flink's full lookup cache loads the table through: the bounded scan's plan and
 * page reads, run start to finish by one input split ({@link FullCacheInputFormat}).
 *
 * <p>Each load plans again in {@link #createInputSplits(int)}, so it reads at the service's time of
 * that planning and the next load reads a newer snapshot. The table's {@code scan.read-time} never
 * reaches this configuration: a lookup reads entities as they are, whatever the scan's snapshot is.
 * The planner is closed before the split is returned, because a plan is only a query and a read
 * time.
 *
 * <p>The load reads the kind as one key range: one query, read page by page through the scan's
 * {@link DatastoreSplitReader}, so paging, cursor continuation and a page that moved its cursor
 * without returning an entity are the scan's. The table maps no {@code MAP} type today, but a split
 * per key range would expose the loader's shared serializer as soon as one is mapped, and cutting
 * key ranges that one split then reads serially would only add the planner's statistics read and
 * {@code __scatter__} sample to every load. The bill also counts the planner's probe read, which
 * the load's log leaves out.
 */
@Internal
final class DatastoreFullCacheInputFormat
        extends FullCacheInputFormat<
                FetchedEntity, QuerySplit, DatastoreFullCacheInputFormat.LoadSplit> {

    private static final long serialVersionUID = 1L;

    /** The parallelism the planner is told; it only floors an estimated split count, never used. */
    private static final int PLANNING_PARALLELISM = 1;

    private final DatastoreSourceConfig<RowData> config;

    DatastoreFullCacheInputFormat(DatastoreSourceConfig<RowData> config) {
        super(LoadSplit.class, "entities");
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
     * @return the one split of the load
     * @throws IOException if the credentials cannot be loaded or planning fails
     */
    @Override
    protected LoadSplit planLoad() throws IOException {
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
        return new LoadSplit(plan.getRequests().get(0), plan.getReadTime());
    }

    @Override
    protected SplitReader<FetchedEntity, QuerySplit> openReader() throws IOException {
        QueryPageReader openReader = closeWithLoad(config.getPageReader());
        openReader.useCredentials(DatastoreCredentials.load(config.getServiceAccountKeyFile()));
        return new DatastoreSplitReader(
                config.getDatabase(),
                openReader,
                config.getPageSize(),
                DatastoreSourceReaderMetrics.unregistered());
    }

    @Override
    protected List<QuerySplit> querySplits(LoadSplit split) {
        return List.of(new QuerySplit("0", split.getQuery(), split.getReadTime()));
    }

    /**
     * Hands one entity to the table's deserializer with its metadata, as the scan's record emitter
     * does. The deserializer has nothing to open, so no context is needed, and a load has none to
     * give.
     */
    @Override
    protected void emit(FetchedEntity entity, Collector<RowData> out) throws IOException {
        ((DatastoreEntityMetadataDeserializationSchema<RowData>) config.getDeserializer())
                .deserialize(entity.getEntity(), entity.getMetadata(), out);
    }

    /** Names the loaded kind and its namespace, if any, and database. */
    @Override
    protected String describe(LoadSplit split) {
        String namespace = config.getNamespace();
        return "kind "
                + SplittableQueries.kindOf(config.getQuery())
                + (namespace.isEmpty() ? "" : " in namespace " + namespace)
                + " of "
                + config.getDatabase();
    }

    /** The planned query of one load and its read time. */
    static final class LoadSplit extends FullCacheInputFormat.LoadSplit {

        private static final long serialVersionUID = 1L;

        private final RunQueryRequest query;

        LoadSplit(RunQueryRequest query, Timestamp readTime) {
            super(readTime);
            this.query = Preconditions.checkNotNull(query, "query must not be null");
        }

        /** Returns the query the load reads. */
        RunQueryRequest getQuery() {
            return query;
        }

        @Override
        public String toString() {
            return "LoadSplit{readTime=" + getReadTime() + '}';
        }
    }
}
