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

package io.github.flink.gcp.connector.firestore.source.batch.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import com.google.auth.Credentials;
import com.google.cloud.Timestamp;
import com.google.cloud.firestore.CollectionGroup;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QueryPartition;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.firestore.v1.RunQueryRequest;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.firestore.LazyFirestoreClient;
import io.github.flink.gcp.connector.firestore.source.FirestoreQueryFactory;
import io.github.flink.gcp.connector.firestore.source.FirestoreSourceConfig;
import io.github.flink.gcp.connector.firestore.source.batch.QueryCursors;
import io.github.flink.gcp.connector.firestore.source.batch.ReadTimeQueries;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Plans a read through a {@code google-cloud-firestore} client.
 *
 * <p>Planning is four steps, in this order:
 *
 * <ol>
 *   <li><b>The base query.</b> A scan is the collection group, projected; otherwise the user's
 *       factory builds the query from this planner's client, and a query addressing another
 *       database is refused.
 *   <li><b>The read time.</b> A configured one is used as it is. Otherwise a one-document probe's
 *       read time: the service's clock, not this process's, which could run ahead and ask for a
 *       snapshot in the future. It is not rounded to a whole minute, which the service requires of
 *       a read time older than an hour: rounding would hide what was written in the minute before
 *       the job started, so a read that must outlive the hour configures a whole-minute time.
 *   <li><b>A one-document probe at that read time.</b> A time outside the service's window fails
 *       here rather than on every reader, and the probe's document proves a cursor can be taken
 *       from the query's documents, which a projection that leaves out an ordered field prevents.
 *   <li><b>The splits.</b> A query's {@code offset} is resolved into a cursor at the read time, so
 *       no split carries one: the client library retries a broken page from its last document and
 *       would apply the offset a second time. A scan asks {@code PartitionQuery} for the configured
 *       number of partitions; its cursors tile the document-name space, start inclusive and end
 *       exclusive, so the partitions together hold every document once. The call cannot take a read
 *       time through the client library, although the RPC accepts one, so its boundaries describe
 *       the collection group as it stands now — which moves nothing: a document of the snapshot
 *       still falls in exactly one partition ({@code docs/adr/0173}).
 * </ol>
 */
@Internal
public class ClientQueryPlanner implements QueryPlanner {

    private final LazyFirestoreClient client;

    /**
     * Creates the planner.
     *
     * @param emulatorEndpoint the emulator endpoint, or {@code null} for the real service
     */
    public ClientQueryPlanner(@Nullable EmulatorEndpoint emulatorEndpoint) {
        this.client = new LazyFirestoreClient("query planner", emulatorEndpoint);
    }

    @Override
    public void useCredentials(@Nullable Credentials credentials) {
        client.useCredentials(credentials);
    }

    @Override
    public QueryPlan plan(FirestoreSourceConfig<?> config, int defaultPartitionCount)
            throws IOException {
        Preconditions.checkNotNull(config, "config must not be null");
        Firestore firestore = client.get(config.getDatabase());
        String collectionGroup = config.getCollectionGroup();
        Query base =
                collectionGroup != null
                        ? project(firestore.collectionGroup(collectionGroup), config)
                        : userQuery(firestore, config);
        Timestamp readTime = readTime(base, config);
        probe(base, readTime, config);
        List<RunQueryRequest> queries = new ArrayList<>();
        if (collectionGroup == null) {
            queries.add(withoutOffset(base, readTime).toProto());
        } else {
            int partitionCount =
                    config.getPartitionCount() != null
                            ? config.getPartitionCount()
                            : Math.max(1, defaultPartitionCount);
            for (QueryPartition partition :
                    partitions(firestore.collectionGroup(collectionGroup), partitionCount)) {
                queries.add(project(partition.createQuery(), config).toProto());
            }
        }
        return new QueryPlan(readTime, queries);
    }

    /**
     * Asks the service for a collection group's partitions.
     *
     * <p>A seam of its own because the emulator does not implement {@code PartitionQuery}: the
     * emulator tests replace this one call with partitions they choose, and read them for real.
     *
     * @param group the collection group
     * @param partitionCount the desired number of partitions, an upper bound the service may answer
     *     below
     * @return the partitions, in document-name order, together covering the whole group
     * @throws IOException if the call fails
     */
    @VisibleForTesting
    protected List<QueryPartition> partitions(CollectionGroup group, int partitionCount)
            throws IOException {
        return ReadTimeQueries.await(
                group.getPartitions(partitionCount),
                "Failed to partition the Firestore collection group scan.");
    }

    private static Query project(Query query, FirestoreSourceConfig<?> config) {
        List<String> fieldMask = config.getFieldMask();
        return fieldMask.isEmpty() ? query : query.select(fieldMask.toArray(new String[0]));
    }

    private static Query userQuery(Firestore firestore, FirestoreSourceConfig<?> config)
            throws IOException {
        FirestoreQueryFactory factory = config.getQueryFactory();
        Query query = factory.create(firestore);
        if (query == null) {
            throw new IOException(
                    "The Firestore query factory returned null; it must return a query.");
        }
        try {
            // Planning goes on with the query rebuilt from its wire form, which is what every
            // reader rebuilds too. The two differ for a limitToLast query, whose wire form carries
            // the reversed ordering as an ordinary limit: resolving an offset on the original would
            // skip from the other end. The rebuild is also the library's own check that the query
            // addresses this client's database.
            return Query.fromProto(firestore, query.toProto());
        } catch (IllegalArgumentException e) {
            throw new IOException(
                    "The Firestore query factory returned a query that addresses a database other"
                            + " than "
                            + config.getDatabase()
                            + "; build the query from the client the factory is given.",
                    e);
        }
    }

    private static Timestamp readTime(Query base, FirestoreSourceConfig<?> config)
            throws IOException {
        Instant configured = config.getReadTime();
        if (configured != null) {
            return Timestamp.ofTimeSecondsAndNanos(
                    configured.getEpochSecond(), configured.getNano());
        }
        Timestamp now =
                ReadTimeQueries.await(
                                base.offset(0).limit(1).get(),
                                "Failed to read the Firestore snapshot time.")
                        .getReadTime();
        if (now == null) {
            // The service answers every query, an empty one included, with a read time; a
            // response without one leaves nothing to pin the snapshot to.
            throw new IOException(
                    "Firestore answered the snapshot-time probe without a read time; the read"
                            + " cannot be pinned to one snapshot.");
        }
        return now;
    }

    private static void probe(Query base, Timestamp readTime, FirestoreSourceConfig<?> config)
            throws IOException {
        QuerySnapshot snapshot =
                ReadTimeQueries.get(
                        base.offset(0).limit(1),
                        readTime,
                        config.getReadTime() == null
                                ? "Failed to read Firestore at the snapshot time " + readTime + "."
                                : "Failed to read Firestore at the configured read time "
                                        + config.getReadTime()
                                        + ". If the cause concerns the read time: without"
                                        + " point-in-time recovery it must lie within the past"
                                        + " hour; with it, on a whole minute within the past seven"
                                        + " days.");
        if (!snapshot.isEmpty()) {
            try {
                QueryCursors.continueAfter(base, snapshot.getDocuments().get(0), 1);
            } catch (IllegalArgumentException e) {
                throw new IOException(e.getMessage(), e);
            }
        }
    }

    /**
     * Resolves a query's {@code offset} into a cursor after the last document it skips, at the read
     * time. A query the offset skips entirely becomes one with a limit of zero.
     */
    private static Query withoutOffset(Query query, Timestamp readTime) throws IOException {
        int offset = query.toProto().getStructuredQuery().getOffset();
        if (offset == 0) {
            return query;
        }
        List<? extends DocumentSnapshot> lastSkipped =
                ReadTimeQueries.get(
                                query.offset(offset - 1).limit(1),
                                readTime,
                                "Failed to resolve the Firestore query's offset.")
                        .getDocuments();
        if (lastSkipped.isEmpty()) {
            return query.offset(0).limit(0);
        }
        return QueryCursors.continueAfter(query, lastSkipped.get(0), 0);
    }

    @Override
    public void close() throws IOException {
        client.close();
    }
}
