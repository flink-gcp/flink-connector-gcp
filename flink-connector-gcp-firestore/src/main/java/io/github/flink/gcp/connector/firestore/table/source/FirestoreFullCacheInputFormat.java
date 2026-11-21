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
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.Timestamp;
import com.google.firestore.v1.RunQueryRequest;
import com.google.firestore.v1.StructuredQuery;
import io.github.flink.gcp.connector.firestore.FirestoreCredentials;
import io.github.flink.gcp.connector.firestore.common.table.FullCacheInputFormat;
import io.github.flink.gcp.connector.firestore.source.FirestoreSourceConfig;
import io.github.flink.gcp.connector.firestore.source.batch.FetchedDocument;
import io.github.flink.gcp.connector.firestore.source.batch.QuerySplit;
import io.github.flink.gcp.connector.firestore.source.batch.enumerator.QueryPlan;
import io.github.flink.gcp.connector.firestore.source.batch.enumerator.QueryPlanner;
import io.github.flink.gcp.connector.firestore.source.batch.reader.FirestoreSourceReaderMetrics;
import io.github.flink.gcp.connector.firestore.source.batch.reader.FirestoreSplitReader;
import io.github.flink.gcp.connector.firestore.source.batch.reader.QueryPageReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The bounded read Flink's full lookup cache loads the table through: the bounded scan's plan and
 * page reads, run start to finish by one input split ({@link FullCacheInputFormat}).
 *
 * <p>Each load plans again in {@link #createInputSplits(int)}, so it reads at the service's time of
 * that planning and the next load reads a newer snapshot. The table's {@code scan.read-time} never
 * reaches this configuration: a lookup reads documents as they are, whatever the scan's snapshot
 * is. The planner is closed before the split is returned, because a plan is only queries and a read
 * time.
 *
 * <p>The split reads the planned queries one after another through one page reader, and a
 * collection group is planned as one partition, which takes no {@code PartitionQuery} call. The
 * bill also counts the planner's probe reads and any page the client library's retry overfilled,
 * which the load's log leaves out.
 */
@Internal
final class FirestoreFullCacheInputFormat
        extends FullCacheInputFormat<
                FetchedDocument, QuerySplit, FirestoreFullCacheInputFormat.LoadSplit> {

    private static final long serialVersionUID = 1L;

    /** One partition takes no {@code PartitionQuery} call, and the load reads serially anyway. */
    private static final int PARTITION_COUNT = 1;

    private final FirestoreSourceConfig<RowData> config;

    FirestoreFullCacheInputFormat(FirestoreSourceConfig<RowData> config) {
        super(LoadSplit.class, "documents");
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
     * @return the one split of the load
     * @throws IOException if the credentials cannot be loaded or planning fails
     */
    @Override
    protected LoadSplit planLoad() throws IOException {
        GoogleCredentials credentials =
                FirestoreCredentials.load(config.getServiceAccountKeyFile());
        QueryPlan plan;
        try (QueryPlanner planner = config.getPlannerFactory().create()) {
            planner.useCredentials(credentials);
            plan = planner.plan(config, PARTITION_COUNT);
        }
        return new LoadSplit(plan.getQueries(), plan.getReadTime());
    }

    @Override
    protected SplitReader<FetchedDocument, QuerySplit> openReader() throws IOException {
        QueryPageReader openReader = closeWithLoad(config.getPageReader());
        openReader.useCredentials(FirestoreCredentials.load(config.getServiceAccountKeyFile()));
        return new FirestoreSplitReader(
                config.getDatabase(),
                openReader,
                config.getPageSize(),
                FirestoreSourceReaderMetrics.unregistered());
    }

    @Override
    protected List<QuerySplit> querySplits(LoadSplit split) {
        return split.toQuerySplits();
    }

    /**
     * Hands one document to the table's deserializer, which opens itself on first use, so no
     * context is needed here, and a load has none to give.
     */
    @Override
    protected void emit(FetchedDocument document, Collector<RowData> out) throws IOException {
        config.getDeserializer().deserialize(document.getDocument(), out);
    }

    /** Names the loaded collection, or collection group, and its database. */
    @Override
    protected String describe(LoadSplit split) {
        StructuredQuery query = split.queries.get(0).getStructuredQuery();
        if (query.getFromCount() == 0) {
            // The planner always names one; the log line must not fail a load that has none.
            return "a query of " + config.getDatabase();
        }
        StructuredQuery.CollectionSelector from = query.getFrom(0);
        return (from.getAllDescendants() ? "collection group " : "collection ")
                + from.getCollectionId()
                + " of "
                + config.getDatabase();
    }

    /** The planned queries of one load and the read time they share. */
    static final class LoadSplit extends FullCacheInputFormat.LoadSplit {

        private static final long serialVersionUID = 1L;

        private final List<RunQueryRequest> queries;

        LoadSplit(List<RunQueryRequest> queries, Timestamp readTime) {
            super(readTime);
            Preconditions.checkArgument(!queries.isEmpty(), "a load reads at least one query");
            this.queries = new ArrayList<>(queries);
        }

        /** Returns how many queries the load reads. */
        int queryCount() {
            return queries.size();
        }

        private List<QuerySplit> toQuerySplits() {
            List<QuerySplit> splits = new ArrayList<>();
            for (int i = 0; i < queries.size(); i++) {
                splits.add(new QuerySplit(String.valueOf(i), queries.get(i), getReadTime()));
            }
            return splits;
        }

        @Override
        public String toString() {
            return "LoadSplit{queries=" + queries.size() + ", readTime=" + getReadTime() + '}';
        }
    }
}
