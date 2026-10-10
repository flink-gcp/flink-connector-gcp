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

import com.google.cloud.Timestamp;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.EntityResult;
import com.google.datastore.v1.Filter;
import com.google.datastore.v1.FindNearest;
import com.google.datastore.v1.PartitionId;
import com.google.datastore.v1.PropertyFilter;
import com.google.datastore.v1.PropertyOrder;
import com.google.datastore.v1.PropertyReference;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.QueryResultBatch;
import com.google.datastore.v1.RunQueryRequest;
import com.google.datastore.v1.RunQueryResponse;
import com.google.datastore.v1.Value;
import com.google.datastore.v1.client.DatastoreHelper;
import com.google.protobuf.Int32Value;
import io.github.flink.gcp.connector.datastore.source.batch.SplittableQueries;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The planner's calls that the emulator cannot answer as the service does: a GQL query that refuses
 * the appended limit, a probe without a read time, and the statistics the emulator never keeps.
 * Key-range splitting itself is the emulator tests' and the gated suite's.
 */
class ClientQueryPlannerTest {

    private static final Timestamp NOW = Timestamp.ofTimeSecondsAndNanos(1_700_000_000L, 42_000);

    private static RunQueryRequest base(String namespace) {
        return RunQueryRequest.newBuilder()
                .setProjectId("p")
                .setPartitionId(
                        PartitionId.newBuilder().setProjectId("p").setNamespaceId(namespace))
                .build();
    }

    private static RunQueryResponse batch(Entity... entities) {
        QueryResultBatch.Builder batch =
                QueryResultBatch.newBuilder()
                        .setReadTime(NOW.toProto())
                        .setMoreResults(QueryResultBatch.MoreResultsType.NO_MORE_RESULTS);
        for (Entity entity : entities) {
            batch.addEntityResults(EntityResult.newBuilder().setEntity(entity));
        }
        return RunQueryResponse.newBuilder().setBatch(batch).build();
    }

    private static String kindOf(RunQueryRequest request) {
        return request.getQuery().getKind(0).getName();
    }

    @Test
    void parsesAGqlQueryWithoutReadingAnEntity() throws IOException {
        Query parsed =
                SplittableQueries.ofKind("Task").toBuilder().setLimit(Int32Value.of(0)).build();
        FakeDatastoreRpc rpc =
                new FakeDatastoreRpc(
                        request ->
                                RunQueryResponse.newBuilder()
                                        .setQuery(parsed)
                                        .setBatch(QueryResultBatch.getDefaultInstance())
                                        .build());

        Query query = ClientQueryPlanner.parseGql(rpc, base("tenant"), "SELECT * FROM Task");

        assertThat(query).isEqualTo(SplittableQueries.ofKind("Task"));
        assertThat(rpc.requests())
                .singleElement()
                .satisfies(
                        request -> {
                            assertThat(request.getGqlQuery().getQueryString())
                                    .isEqualTo("SELECT * FROM Task LIMIT 0");
                            assertThat(request.getGqlQuery().getAllowLiterals()).isTrue();
                            assertThat(request.getPartitionId().getNamespaceId())
                                    .isEqualTo("tenant");
                        });
    }

    @Test
    void parsesAGqlQueryThatRefusesTheAppendedLimitByRunningItAsWritten() throws IOException {
        Query parsed =
                SplittableQueries.ofKind("Task").toBuilder().setLimit(Int32Value.of(5)).build();
        FakeDatastoreRpc rpc =
                new FakeDatastoreRpc(
                        request -> {
                            if (request.getGqlQuery().getQueryString().endsWith("LIMIT 0")) {
                                throw Status.INVALID_ARGUMENT
                                        .withDescription("syntax error")
                                        .asRuntimeException();
                            }
                            return RunQueryResponse.newBuilder()
                                    .setQuery(parsed)
                                    .setBatch(QueryResultBatch.getDefaultInstance())
                                    .build();
                        });

        Query query = ClientQueryPlanner.parseGql(rpc, base(""), "SELECT * FROM Task LIMIT 5");

        assertThat(query).as("the query's own limit stays").isEqualTo(parsed);
        assertThat(rpc.requests())
                .extracting(request -> request.getGqlQuery().getQueryString())
                .containsExactly(
                        "SELECT * FROM Task LIMIT 5 LIMIT 0", "SELECT * FROM Task LIMIT 5");
    }

    @Test
    void refusesAGqlQueryThatParsesIntoAMetadataKind() {
        FakeDatastoreRpc rpc =
                new FakeDatastoreRpc(
                        request ->
                                RunQueryResponse.newBuilder()
                                        .setQuery(
                                                SplittableQueries.ofKind("__kind__").toBuilder()
                                                        .setLimit(Int32Value.of(0)))
                                        .setBatch(QueryResultBatch.getDefaultInstance())
                                        .build());

        assertThatThrownBy(
                        () -> ClientQueryPlanner.parseGql(rpc, base(""), "SELECT * FROM __kind__"))
                .isInstanceOf(IOException.class)
                .hasMessage(
                        "The source cannot read the query the GQL query parsed into: __kind__ is a"
                                + " metadata kind, which the source can neither page through nor"
                                + " resume after a failure; read metadata with the client library"
                                + " instead.");
        assertThat(rpc.requests())
                .as("parseGql sends the parse request and nothing after it")
                .singleElement()
                .extracting(request -> request.getGqlQuery().getQueryString())
                .isEqualTo("SELECT * FROM __kind__ LIMIT 0");
    }

    @Test
    void refusesAMetadataKindParsedByRunningTheQueryAsWritten() {
        FakeDatastoreRpc rpc =
                new FakeDatastoreRpc(
                        request -> {
                            if (request.getGqlQuery().getQueryString().endsWith("LIMIT 0")) {
                                throw Status.INVALID_ARGUMENT
                                        .withDescription("syntax error")
                                        .asRuntimeException();
                            }
                            return RunQueryResponse.newBuilder()
                                    .setQuery(
                                            SplittableQueries.ofKind("__kind__").toBuilder()
                                                    .setLimit(Int32Value.of(5)))
                                    .setBatch(QueryResultBatch.getDefaultInstance())
                                    .build();
                        });

        assertThatThrownBy(
                        () ->
                                ClientQueryPlanner.parseGql(
                                        rpc, base(""), "SELECT * FROM __kind__ LIMIT 5"))
                .isInstanceOf(IOException.class)
                .hasMessage(
                        "The source cannot read the query the GQL query parsed into: __kind__ is a"
                                + " metadata kind, which the source can neither page through nor"
                                + " resume after a failure; read metadata with the client library"
                                + " instead.");
        assertThat(rpc.requests())
                .extracting(request -> request.getGqlQuery().getQueryString())
                .containsExactly(
                        "SELECT * FROM __kind__ LIMIT 5 LIMIT 0", "SELECT * FROM __kind__ LIMIT 5");
    }

    @Test
    void refusesAGqlQueryThatParsesIntoANearestNeighbourSearch() {
        Query parsed =
                SplittableQueries.ofKind("Task").toBuilder()
                        .setLimit(Int32Value.of(0))
                        .setFindNearest(
                                FindNearest.newBuilder()
                                        .setVectorProperty(
                                                PropertyReference.newBuilder().setName("embedding"))
                                        .setDistanceMeasure(FindNearest.DistanceMeasure.EUCLIDEAN)
                                        .setLimit(Int32Value.of(3)))
                        .build();
        // The fake answers as a parse into a nearest-neighbour search would; the text only names
        // the request.
        FakeDatastoreRpc rpc =
                new FakeDatastoreRpc(
                        request ->
                                RunQueryResponse.newBuilder()
                                        .setQuery(parsed)
                                        .setBatch(QueryResultBatch.getDefaultInstance())
                                        .build());

        assertThatThrownBy(() -> ClientQueryPlanner.parseGql(rpc, base(""), "SELECT * FROM Task"))
                .isInstanceOf(IOException.class)
                .hasMessage(
                        "The source cannot read the query the GQL query parsed into: it is a"
                                + " nearest-neighbour search, whose results the source's paging"
                                + " and resume cursors would change.");
        assertThat(rpc.requests()).hasSize(1);
    }

    @Test
    void failsOtherwiseWithoutRunningTheQuery() {
        FakeDatastoreRpc rpc =
                new FakeDatastoreRpc(
                        request -> {
                            throw Status.PERMISSION_DENIED.asRuntimeException();
                        });

        assertThatThrownBy(() -> ClientQueryPlanner.parseGql(rpc, base(""), "SELECT * FROM Task"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to run the Datastore GQL query to parse it")
                .hasMessageContaining("SELECT * FROM Task");
        assertThat(rpc.requests()).hasSize(1);
    }

    @Test
    void takesTheServicesReadTimeFromAOneEntityProbe() throws IOException {
        FakeDatastoreRpc rpc = new FakeDatastoreRpc(request -> batch());
        RunQueryRequest request =
                base("").toBuilder()
                        .setQuery(
                                SplittableQueries.ofKind("Task").toBuilder()
                                        .setOffset(9)
                                        .setLimit(Int32Value.of(50)))
                        .build();

        assertThat(ClientQueryPlanner.readTime(rpc, request, null)).isEqualTo(NOW);
        assertThat(rpc.requests())
                .singleElement()
                .satisfies(
                        probe -> {
                            assertThat(probe.hasReadOptions()).isFalse();
                            assertThat(probe.getQuery().getOffset()).isZero();
                            assertThat(probe.getQuery().getLimit().getValue()).isOne();
                        });
    }

    @Test
    void probesAtAConfiguredReadTimeAndKeepsIt() throws IOException {
        FakeDatastoreRpc rpc = new FakeDatastoreRpc(request -> batch());
        Instant configured = Instant.parse("2026-10-03T00:00:00Z");

        Timestamp readTime =
                ClientQueryPlanner.readTime(
                        rpc,
                        base("").toBuilder().setQuery(SplittableQueries.ofKind("Task")).build(),
                        configured);

        assertThat(readTime.toSqlTimestamp().toInstant()).isEqualTo(configured);
        assertThat(rpc.requests().get(0).getReadOptions().getReadTime())
                .isEqualTo(readTime.toProto());
    }

    @Test
    void namesTheWindowWhenAConfiguredReadTimeIsRefused() {
        FakeDatastoreRpc rpc =
                new FakeDatastoreRpc(
                        request -> {
                            throw Status.INVALID_ARGUMENT.asRuntimeException();
                        });

        assertThatThrownBy(
                        () ->
                                ClientQueryPlanner.readTime(
                                        rpc,
                                        base("").toBuilder()
                                                .setQuery(SplittableQueries.ofKind("Task"))
                                                .build(),
                                        Instant.parse("2026-10-03T00:00:00Z")))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("configured read time 2026-10-03T00:00:00Z")
                .hasMessageContaining("point-in-time recovery");
    }

    @Test
    void refusesAProbeAnsweredWithoutAReadTime() {
        FakeDatastoreRpc rpc =
                new FakeDatastoreRpc(
                        request ->
                                RunQueryResponse.newBuilder()
                                        .setBatch(QueryResultBatch.getDefaultInstance())
                                        .build());

        assertThatThrownBy(
                        () ->
                                ClientQueryPlanner.readTime(
                                        rpc,
                                        base("").toBuilder()
                                                .setQuery(SplittableQueries.ofKind("Task"))
                                                .build(),
                                        null))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("without a read time");
    }

    @Test
    void withoutStatisticsAsksForTheFloorOrTheParallelism() throws IOException {
        FakeDatastoreRpc rpc = new FakeDatastoreRpc(request -> batch());

        assertThat(ClientQueryPlanner.estimateSplitCount(rpc, base(""), "Task", 4))
                .isEqualTo(ClientQueryPlanner.MIN_SPLITS);
        assertThat(ClientQueryPlanner.estimateSplitCount(rpc, base(""), "Task", 30)).isEqualTo(30);
        assertThat(rpc.requests())
                .extracting(ClientQueryPlannerTest::kindOf)
                .containsOnly("__Stat_Total__");
    }

    @Test
    void estimatesOneSplitPer64MibOfTheKindInTheLatestStatistics() throws IOException {
        Value latest = DatastoreHelper.makeValue(new java.util.Date(1_700_000_000_000L)).build();
        long tenGib = 10L * 1024 * 1024 * 1024;
        FakeDatastoreRpc rpc = statistics(latest, tenGib);

        assertThat(ClientQueryPlanner.estimateSplitCount(rpc, base(""), "Task", 4)).isEqualTo(160);
        RunQueryRequest kindStatistics = rpc.requests().get(1);
        assertThat(kindOf(kindStatistics)).isEqualTo("__Stat_Kind__");
        assertThat(kindStatistics.getQuery().getFilter().getCompositeFilter().getFiltersList())
                .extracting(Filter::getPropertyFilter)
                .anySatisfy(
                        filter -> {
                            assertThat(filter.getProperty().getName()).isEqualTo("kind_name");
                            assertThat(filter.getValue().getStringValue()).isEqualTo("Task");
                        });
    }

    @Test
    void theParallelismIsAFloorUnderAnEstimateFromStatistics() throws IOException {
        Value latest = DatastoreHelper.makeValue(new java.util.Date(1_700_000_000_000L)).build();
        long oneGib = 1024L * 1024 * 1024;

        // 1 GiB is 16 splits, which 64 subtasks would leave mostly idle.
        assertThat(
                        ClientQueryPlanner.estimateSplitCount(
                                statistics(latest, oneGib), base(""), "Task", 64))
                .isEqualTo(64);
        assertThat(
                        ClientQueryPlanner.estimateSplitCount(
                                statistics(latest, oneGib), base(""), "Task", 4))
                .isEqualTo(16);
    }

    @Test
    void boundsTheEstimate() throws IOException {
        Value latest = DatastoreHelper.makeValue(new java.util.Date(1_700_000_000_000L)).build();

        assertThat(ClientQueryPlanner.estimateSplitCount(statistics(latest, 1), base(""), "T", 4))
                .isEqualTo(ClientQueryPlanner.MIN_SPLITS);
        assertThat(
                        ClientQueryPlanner.estimateSplitCount(
                                statistics(latest, Long.MAX_VALUE / 2), base(""), "T", 4))
                .isEqualTo(ClientQueryPlanner.MAX_SPLITS);
    }

    @Test
    void readsANamespacesOwnStatistics() throws IOException {
        Value latest = DatastoreHelper.makeValue(new java.util.Date(1_700_000_000_000L)).build();
        FakeDatastoreRpc rpc = statistics(latest, 0);

        ClientQueryPlanner.estimateSplitCount(rpc, base("tenant"), "Task", 1);

        assertThat(rpc.requests())
                .extracting(ClientQueryPlannerTest::kindOf)
                .containsExactly("__Stat_Ns_Total__", "__Stat_Ns_Kind__");
        assertThat(rpc.requests())
                .extracting(request -> request.getPartitionId().getNamespaceId())
                .containsOnly("tenant");
    }

    @Test
    void namesTheEscapeWhenTheStatisticsCannotBeRead() {
        FakeDatastoreRpc rpc =
                new FakeDatastoreRpc(
                        request -> {
                            throw Status.UNAVAILABLE.asRuntimeException();
                        });

        assertThatThrownBy(() -> ClientQueryPlanner.estimateSplitCount(rpc, base(""), "Task", 1))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Set splitCount to skip the estimate");
    }

    /**
     * Answers the statistics queries as the service would for one kind of {@code bytes} at {@code
     * latest}, with an older entry beside it: the totals query only when it orders by time, newest
     * first, and asks for one; the kind query only when it matches the kind and that time. A
     * planner that read the wrong entry finds nothing and asks for the floor.
     */
    private static FakeDatastoreRpc statistics(Value latest, long bytes) {
        Value older = DatastoreHelper.makeValue(new java.util.Date(1_600_000_000_000L)).build();
        return new FakeDatastoreRpc(
                request -> {
                    Query query = request.getQuery();
                    if (kindOf(request).endsWith("Total__")) {
                        boolean newestFirst =
                                query.getOrderCount() == 1
                                        && query.getOrder(0)
                                                .getProperty()
                                                .getName()
                                                .equals("timestamp")
                                        && query.getOrder(0).getDirection()
                                                == PropertyOrder.Direction.DESCENDING
                                        && query.getLimit().getValue() == 1;
                        return batch(
                                Entity.newBuilder()
                                        .putProperties("timestamp", newestFirst ? latest : older)
                                        .build());
                    }
                    java.util.Map<String, Value> equalities = new java.util.HashMap<>();
                    for (Filter filter : query.getFilter().getCompositeFilter().getFiltersList()) {
                        PropertyFilter property = filter.getPropertyFilter();
                        if (property.getOp() == PropertyFilter.Operator.EQUAL) {
                            equalities.put(property.getProperty().getName(), property.getValue());
                        }
                    }
                    boolean matches =
                            latest.equals(equalities.get("timestamp"))
                                    && equalities.containsKey("kind_name");
                    return matches
                            ? batch(
                                    Entity.newBuilder()
                                            .putProperties(
                                                    "entity_bytes",
                                                    DatastoreHelper.makeValue(bytes).build())
                                            .build())
                            : batch();
                });
    }
}
