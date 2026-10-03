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

package io.github.flink.gcp.connector.datastore.source.batch.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import com.google.api.gax.rpc.StatusCode;
import com.google.auth.Credentials;
import com.google.cloud.Timestamp;
import com.google.cloud.datastore.spi.v1.DatastoreRpc;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.GqlQuery;
import com.google.datastore.v1.PartitionId;
import com.google.datastore.v1.PropertyFilter;
import com.google.datastore.v1.PropertyOrder;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.QueryResultBatch;
import com.google.datastore.v1.ReadOptions;
import com.google.datastore.v1.RunQueryRequest;
import com.google.datastore.v1.RunQueryResponse;
import com.google.datastore.v1.Value;
import com.google.datastore.v1.client.DatastoreException;
import com.google.datastore.v1.client.DatastoreHelper;
import com.google.protobuf.Int32Value;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.rpc.StatusCodes;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import io.github.flink.gcp.connector.datastore.LazyDatastoreClient;
import io.github.flink.gcp.connector.datastore.source.DatastoreSourceBuilder;
import io.github.flink.gcp.connector.datastore.source.DatastoreSourceConfig;
import io.github.flink.gcp.connector.datastore.source.batch.SplittableQueries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Plans a read through a {@code google-cloud-datastore} client and, to cut key ranges, the client
 * library's {@code QuerySplitter}.
 *
 * <p>Planning is four steps, in this order:
 *
 * <ol>
 *   <li><b>The query.</b> A kind scan or a query is used as configured. A GQL query is parsed by
 *       the service: the planner asks for it with {@code LIMIT 0} appended, which parses it without
 *       reading an entity, and takes the query the service answers with, without that limit. A
 *       query that already ends in a {@code LIMIT} or an {@code OFFSET} clause refuses the appended
 *       clause with {@code INVALID_ARGUMENT}; it is then run as written and the answer's query, its
 *       own limit included, is taken. That reads and bills its first batch. A query the parse turns
 *       into a nearest-neighbour search is refused, as the builder refuses one given directly.
 *   <li><b>The read time.</b> A one-entity probe of the query: at the configured read time, so a
 *       time outside the service's window fails here rather than on every reader, or with no read
 *       time, and the batch's read time is taken. That is the service's clock, not this process's,
 *       which could run ahead and ask for a snapshot in the future, and it is not rounded to a
 *       whole minute ({@code docs/adr/0173} records why).
 *   <li><b>The split count.</b> A configured one is used as it is. Otherwise it is estimated from
 *       the database's statistics, Beam's way: the kind's {@code entity_bytes} in the latest
 *       statistics, one split per {@value #BYTES_PER_SPLIT} bytes, at least {@value #MIN_SPLITS}
 *       and at least the parallelism, at most {@value #MAX_SPLITS}. A kind without statistics,
 *       which every kind is until the service first computes them, gets the lower bound.
 *   <li><b>The key ranges.</b> A query that {@link SplittableQueries} accepts is cut by {@code
 *       QuerySplitter}, which samples the kind's keys through {@code __scatter__} and ANDs a key
 *       range onto the query for each split; its ranges are then laid out again in the service's
 *       key order ({@link KeyRanges}), because the splitter sorts names as Java strings. It samples
 *       at the current time, not at the read time: its read-time overload is a beta API, and
 *       nothing is lost without it, because the ranges tile the key space and every entity of the
 *       snapshot falls in exactly one of them. Any other query is one split.
 * </ol>
 */
@Internal
public class ClientQueryPlanner implements QueryPlanner {

    private static final Logger LOG = LoggerFactory.getLogger(ClientQueryPlanner.class);

    /** The least number of key ranges an estimated count asks for; Beam's floor. */
    static final int MIN_SPLITS = 12;

    /** The most key ranges an estimated count asks for; Beam's ceiling. */
    static final int MAX_SPLITS = DatastoreSourceBuilder.MAX_SPLIT_COUNT;

    /** The kind size each estimated split stands for: 64 MiB, Beam's bundle size. */
    static final long BYTES_PER_SPLIT = 64L * 1024 * 1024;

    private final LazyDatastoreClient client;
    private final SplitterClient splitterClient;

    /**
     * Creates the planner.
     *
     * @param emulatorEndpoint the emulator endpoint, or {@code null} for the real service
     */
    public ClientQueryPlanner(@Nullable EmulatorEndpoint emulatorEndpoint) {
        this.client = new LazyDatastoreClient("query planner", emulatorEndpoint);
        this.splitterClient = new SplitterClient(emulatorEndpoint);
    }

    @Override
    public void useCredentials(@Nullable Credentials credentials) {
        client.useCredentials(credentials);
        splitterClient.useCredentials(credentials);
    }

    @Override
    public QueryPlan plan(DatastoreSourceConfig<?> config, int parallelism) throws IOException {
        Preconditions.checkNotNull(config, "config must not be null");
        DatabaseDestination database = config.getDatabase();
        DatastoreRpc rpc = client.rpc(database);
        PartitionId partition =
                PartitionId.newBuilder()
                        .setProjectId(database.getProject())
                        .setDatabaseId(database.getDatabaseId())
                        .setNamespaceId(config.getNamespace())
                        .build();
        RunQueryRequest base =
                RunQueryRequest.newBuilder()
                        .setProjectId(database.getProject())
                        .setDatabaseId(database.getDatabaseId())
                        .setPartitionId(partition)
                        .build();
        Query query =
                config.getQuery() != null
                        ? config.getQuery()
                        : parseGql(rpc, base, config.getGqlQuery());
        String unreadable = SplittableQueries.whyNotReadable(query);
        if (unreadable != null) {
            // Only a GQL query reaches this; the builder refuses the other shapes.
            throw new IOException(
                    "The source cannot read the query the GQL query parsed into: "
                            + unreadable
                            + ".");
        }
        RunQueryRequest request = base.toBuilder().setQuery(query).build();
        Timestamp readTime = readTime(rpc, request, config.getReadTime());

        String reason = SplittableQueries.whyNotSplittable(query);
        if (reason != null) {
            if (config.getSplitCount() != null) {
                // Only a GQL query reaches this; the builder refuses the other shapes.
                throw new IOException(
                        "splitCount applies to a query that can be split, and the GQL query"
                                + " parsed into one that is read as one split because "
                                + reason
                                + ". Remove splitCount, or write a query that can be split.");
            }
            return new QueryPlan(readTime, List.of(request), reason);
        }
        int splitCount =
                config.getSplitCount() != null
                        ? config.getSplitCount()
                        : estimateSplitCount(
                                rpc, base, SplittableQueries.kindOf(query), parallelism);
        if (splitCount == 1) {
            return new QueryPlan(readTime, List.of(request), null);
        }
        List<RunQueryRequest> requests = new ArrayList<>();
        for (Query range : KeyRanges.retile(query, split(query, partition, splitCount))) {
            requests.add(base.toBuilder().setQuery(range).build());
        }
        return new QueryPlan(readTime, requests, null);
    }

    /**
     * Cuts a query into key ranges through the client library's {@code QuerySplitter}.
     *
     * <p>A seam of its own because the emulator answers the {@code __scatter__} sampling with no
     * keys: the emulator tests replace this one call with key ranges they choose, and read them for
     * real.
     *
     * @param query the query, which {@link SplittableQueries} accepts
     * @param partition the database and namespace the query reads
     * @param splitCount the desired number of key ranges, an upper bound the sampling may answer
     *     below
     * @return the queries, one per key range, in key order, together covering the whole query
     * @throws IOException if a call fails
     */
    @VisibleForTesting
    protected List<Query> split(Query query, PartitionId partition, int splitCount)
            throws IOException {
        try {
            return DatastoreHelper.getQuerySplitter()
                    .getSplits(
                            query,
                            partition,
                            splitCount,
                            splitterClient.get(partition.getProjectId()));
        } catch (DatastoreException e) {
            throw new IOException(
                    "Failed to cut the Datastore query into key ranges (" + e.getCode() + ").", e);
        } catch (IllegalArgumentException e) {
            throw new IOException("The client library's splitter refused the query.", e);
        }
    }

    /**
     * Has the service parse a GQL query into the query it stands for.
     *
     * @param rpc the client's RPC object
     * @param base the request naming the database and namespace, without a query
     * @param gql the GQL query
     * @return the parsed query
     * @throws IOException if the service refuses or fails to parse it
     */
    @VisibleForTesting
    static Query parseGql(DatastoreRpc rpc, RunQueryRequest base, @Nullable String gql)
            throws IOException {
        Preconditions.checkNotNull(gql, "a configuration without a query carries a GQL query");
        try {
            return rpc.runQuery(gqlRequest(base, gql + " LIMIT 0")).getQuery().toBuilder()
                    .clearLimit()
                    .build();
        } catch (RuntimeException e) {
            if (!isInvalidArgument(e)) {
                throw new IOException(gqlFailure(gql), e);
            }
            LOG.info(
                    "Datastore refused the GQL query with LIMIT 0 appended, as it does when the"
                            + " query ends in a LIMIT or an OFFSET clause; parsing it by running it"
                            + " as written.");
        }
        try {
            return rpc.runQuery(gqlRequest(base, gql)).getQuery();
        } catch (RuntimeException e) {
            throw new IOException(gqlFailure(gql), e);
        }
    }

    /** Names the query in a parse failure, cut to a length a log line holds. */
    private static String gqlFailure(String gql) {
        String shown = gql.length() <= 200 ? gql : gql.substring(0, 200) + "...";
        return "Failed to run the Datastore GQL query to parse it: " + shown;
    }

    private static RunQueryRequest gqlRequest(RunQueryRequest base, String gql) {
        return base.toBuilder()
                .setGqlQuery(GqlQuery.newBuilder().setQueryString(gql).setAllowLiterals(true))
                .build();
    }

    private static boolean isInvalidArgument(Throwable e) {
        return ExceptionUtils.findThrowable(
                        e, t -> StatusCodes.codeOf(t) == StatusCode.Code.INVALID_ARGUMENT)
                .isPresent();
    }

    /**
     * Probes the query for one entity, at the configured read time when there is one, and returns
     * the read time the plan is pinned to.
     *
     * @param rpc the client's RPC object
     * @param request the query to probe
     * @param configured the configured read time, or {@code null} to take the service's
     * @return the read time
     * @throws IOException if the probe fails or answers without a read time
     */
    @VisibleForTesting
    static Timestamp readTime(
            DatastoreRpc rpc, RunQueryRequest request, @Nullable Instant configured)
            throws IOException {
        RunQueryRequest.Builder probe = request.toBuilder();
        probe.getQueryBuilder().setOffset(0).setLimit(Int32Value.of(1));
        Timestamp readTime = null;
        if (configured != null) {
            readTime =
                    Timestamp.ofTimeSecondsAndNanos(
                            configured.getEpochSecond(), configured.getNano());
            probe.setReadOptions(ReadOptions.newBuilder().setReadTime(readTime.toProto()));
        }
        QueryResultBatch batch;
        try {
            batch = rpc.runQuery(probe.build()).getBatch();
        } catch (RuntimeException e) {
            throw new IOException(
                    configured == null
                            ? "Failed to run the Datastore query to take its snapshot time. If the"
                                    + " cause names a missing index, the query needs one the"
                                    + " database does not have."
                            : "Failed to read Datastore at the configured read time "
                                    + configured
                                    + ". If the cause concerns the read time: without"
                                    + " point-in-time recovery it must lie within the past hour;"
                                    + " with it, on a whole minute within the past seven days.",
                    e);
        }
        if (readTime != null) {
            return readTime;
        }
        if (!batch.hasReadTime()) {
            // The service answers every query batch with the time it was read at; a batch without
            // one leaves nothing to pin the snapshot to.
            throw new IOException(
                    "Datastore answered the snapshot-time probe without a read time; the read"
                            + " cannot be pinned to one snapshot.");
        }
        return Timestamp.fromProto(batch.getReadTime());
    }

    /**
     * Estimates the split count from the database's statistics.
     *
     * @param rpc the client's RPC object
     * @param base the request naming the database and namespace, without a query
     * @param kind the kind to estimate for
     * @param parallelism the source's parallelism, a lower bound on the count
     * @return the split count
     * @throws IOException if reading the statistics fails
     */
    @VisibleForTesting
    static int estimateSplitCount(
            DatastoreRpc rpc, RunQueryRequest base, @Nullable String kind, int parallelism)
            throws IOException {
        Preconditions.checkNotNull(kind, "a query that can be split names one kind");
        int floor = Math.max(MIN_SPLITS, parallelism);
        Long bytes;
        try {
            bytes = kindBytes(rpc, base, kind);
        } catch (RuntimeException e) {
            throw new IOException(
                    "Failed to read the Datastore statistics for kind '"
                            + kind
                            + "' to estimate the split count. Set splitCount to skip the"
                            + " estimate.",
                    e);
        }
        if (bytes == null) {
            LOG.info(
                    "No Datastore statistics for kind '{}' yet; asking for {} key ranges.",
                    kind,
                    Math.min(MAX_SPLITS, floor));
            return Math.min(MAX_SPLITS, floor);
        }
        long estimated = Math.round((double) bytes / BYTES_PER_SPLIT);
        int splitCount = (int) Math.min(MAX_SPLITS, Math.max(floor, estimated));
        LOG.info(
                "Datastore statistics give kind '{}' {} bytes; asking for {} key ranges.",
                kind,
                bytes,
                splitCount);
        return splitCount;
    }

    /**
     * Reads a kind's size from the latest statistics, or {@code null} when there are none.
     *
     * <p>Two queries, each served by a built-in index: the latest statistics time, then the kind's
     * entry at that time. One query ordering the kind's entries by time would need a composite
     * index. Outside the default namespace the namespace's own statistics kinds are read.
     */
    @Nullable
    private static Long kindBytes(DatastoreRpc rpc, RunQueryRequest base, String kind) {
        boolean defaultNamespace = base.getPartitionId().getNamespaceId().isEmpty();
        Query latest =
                SplittableQueries.ofKind(defaultNamespace ? "__Stat_Total__" : "__Stat_Ns_Total__")
                        .toBuilder()
                        .addOrder(
                                DatastoreHelper.makeOrder(
                                        "timestamp", PropertyOrder.Direction.DESCENDING))
                        .setLimit(Int32Value.of(1))
                        .build();
        List<Entity> totals = entities(rpc.runQuery(base.toBuilder().setQuery(latest).build()));
        if (totals.isEmpty() || !totals.get(0).containsProperties("timestamp")) {
            return null;
        }
        Value timestamp = totals.get(0).getPropertiesOrThrow("timestamp");
        Query kindStatistics =
                SplittableQueries.ofKind(defaultNamespace ? "__Stat_Kind__" : "__Stat_Ns_Kind__")
                        .toBuilder()
                        .setFilter(
                                DatastoreHelper.makeAndFilter(
                                        DatastoreHelper.makeFilter(
                                                        "kind_name",
                                                        PropertyFilter.Operator.EQUAL,
                                                        DatastoreHelper.makeValue(kind))
                                                .build(),
                                        DatastoreHelper.makeFilter(
                                                        "timestamp",
                                                        PropertyFilter.Operator.EQUAL,
                                                        timestamp)
                                                .build()))
                        .setLimit(Int32Value.of(1))
                        .build();
        List<Entity> kinds =
                entities(rpc.runQuery(base.toBuilder().setQuery(kindStatistics).build()));
        if (kinds.isEmpty() || !kinds.get(0).containsProperties("entity_bytes")) {
            return null;
        }
        return kinds.get(0).getPropertiesOrThrow("entity_bytes").getIntegerValue();
    }

    private static List<Entity> entities(RunQueryResponse response) {
        List<Entity> entities = new ArrayList<>();
        response.getBatch().getEntityResultsList().forEach(r -> entities.add(r.getEntity()));
        return entities;
    }

    @Override
    public void close() throws IOException {
        try {
            Closers.closeAll(client::close, splitterClient);
        } catch (IOException | RuntimeException | Error e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to close the Datastore query planner's clients.", e);
        }
    }
}
