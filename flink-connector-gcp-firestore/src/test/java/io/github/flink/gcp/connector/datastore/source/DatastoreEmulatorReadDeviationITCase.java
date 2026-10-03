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

package io.github.flink.gcp.connector.datastore.source;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.datastore.Blob;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.spi.v1.DatastoreRpc;
import com.google.datastore.v1.GqlQuery;
import com.google.datastore.v1.PartitionId;
import com.google.datastore.v1.Projection;
import com.google.datastore.v1.PropertyReference;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.QueryResultBatch;
import com.google.datastore.v1.ReadOptions;
import com.google.datastore.v1.RunQueryRequest;
import com.google.datastore.v1.client.Datastore;
import com.google.datastore.v1.client.DatastoreFactory;
import com.google.datastore.v1.client.DatastoreHelper;
import com.google.datastore.v1.client.DatastoreOptions;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Timestamp;
import io.github.flink.gcp.connector.datastore.AbstractDatastoreEmulatorITCase;
import io.github.flink.gcp.connector.datastore.source.batch.SplittableQueries;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins what the emulator does with the calls the source makes, where that is not what the service
 * does. Each test here backs a row of the emulator-deviation table on the connector's docs page; a
 * failure after an emulator bump means the row is stale, not that the source broke.
 */
class DatastoreEmulatorReadDeviationITCase extends AbstractDatastoreEmulatorITCase {

    private static final PartitionId PARTITION =
            PartitionId.newBuilder().setProjectId(PROJECT).build();

    private static DatastoreRpc rpc() {
        return (DatastoreRpc) client().getOptions().getRpc();
    }

    private static QueryResultBatch run(Query query) {
        return rpc().runQuery(
                        RunQueryRequest.newBuilder()
                                .setProjectId(PROJECT)
                                .setPartitionId(PARTITION)
                                .setQuery(query)
                                .build())
                .getBatch();
    }

    private static void seed(String kind, int count) {
        List<FullEntity<?>> batch = new ArrayList<>();
        for (int n = 0; n < count; n++) {
            batch.add(
                    Entity.newBuilder(key(kind, "e" + String.format("%03d", n)))
                            .set("n", n)
                            .build());
        }
        client().put(batch.toArray(new FullEntity<?>[0]));
    }

    @Test
    void theEmulatorSamplesNoScatterKeysSoTheSplitterAnswersWithOneRange() throws Exception {
        String kind = uniqueKind();
        seed(kind, 100);
        Datastore http =
                DatastoreFactory.get()
                        .create(
                                new DatastoreOptions.Builder()
                                        .projectId(PROJECT)
                                        .localHost(emulatorEndpoint())
                                        .build());

        // The splitter's own sampling query: __scatter__ order, keys only.
        assertThat(
                        run(SplittableQueries.ofKind(kind).toBuilder()
                                        .addOrder(
                                                DatastoreHelper.makeOrder(
                                                        "__scatter__",
                                                        com.google.datastore.v1.PropertyOrder
                                                                .Direction.ASCENDING))
                                        .addProjection(
                                                com.google.datastore.v1.Projection.newBuilder()
                                                        .setProperty(
                                                                com.google.datastore.v1
                                                                        .PropertyReference
                                                                        .newBuilder()
                                                                        .setName("__key__")))
                                        .setLimit(Int32Value.of(96))
                                        .build())
                                .getEntityResultsCount())
                .isZero();
        // The splitter reaches the emulator over HTTP and, finding no keys, answers with the whole
        // query: the source's production path, minus the ranges the service would sample.
        List<Query> splits =
                DatastoreHelper.getQuerySplitter()
                        .getSplits(SplittableQueries.ofKind(kind), PARTITION, 4, http);
        assertThat(splits).containsExactly(SplittableQueries.ofKind(kind));
    }

    @Test
    void theEmulatorKeepsNoStatistics() {
        seed(uniqueKind(), 3);

        assertThat(run(SplittableQueries.ofKind("__Stat_Total__")).getEntityResultsCount())
                .isZero();
        assertThat(run(SplittableQueries.ofKind("__Stat_Kind__")).getEntityResultsCount()).isZero();
        // A namespace's own statistics kinds, which the planner reads outside the default one.
        PartitionId namespaced = PARTITION.toBuilder().setNamespaceId("tenant").build();
        for (String statistics : new String[] {"__Stat_Ns_Total__", "__Stat_Ns_Kind__"}) {
            assertThat(
                            rpc().runQuery(
                                            RunQueryRequest.newBuilder()
                                                    .setProjectId(PROJECT)
                                                    .setPartitionId(namespaced)
                                                    .setQuery(SplittableQueries.ofKind(statistics))
                                                    .build())
                                    .getBatch()
                                    .getEntityResultsCount())
                    .as(statistics)
                    .isZero();
        }
    }

    @Test
    void theEmulatorRefusesAReadTimeInTheFutureWithInvalidArgument() {
        String kind = uniqueKind();
        Timestamp now = run(SplittableQueries.ofKind(kind)).getReadTime();

        assertThatThrownBy(
                        () ->
                                readAt(
                                        kind,
                                        Timestamp.newBuilder()
                                                .setSeconds(now.getSeconds() + 3600)
                                                .build()))
                .isInstanceOf(ApiException.class)
                .satisfies(
                        e ->
                                assertThat(((ApiException) e).getStatusCode().getCode())
                                        .isEqualTo(StatusCode.Code.INVALID_ARGUMENT));
    }

    @Test
    void theEmulatorParsesAGqlQueryWithLimitZeroWithoutReadingAnEntity() {
        // The planner's way of parsing GQL; the fallback that runs the query as written covers a
        // refusal, so the IT reading a GQL query would pass either way.
        String kind = uniqueKind();
        seed(kind, 3);

        com.google.datastore.v1.RunQueryResponse parsed =
                rpc().runQuery(
                                RunQueryRequest.newBuilder()
                                        .setProjectId(PROJECT)
                                        .setPartitionId(PARTITION)
                                        .setGqlQuery(
                                                GqlQuery.newBuilder()
                                                        .setQueryString(
                                                                "SELECT * FROM `"
                                                                        + kind
                                                                        + "` LIMIT 0")
                                                        .setAllowLiterals(true))
                                        .build());

        assertThat(parsed.getBatch().getEntityResultsCount()).isZero();
        assertThat(parsed.getQuery().getKind(0).getName()).isEqualTo(kind);
        assertThat(parsed.getQuery().getLimit().getValue()).isZero();
    }

    @Test
    void theEmulatorReturnsProjectedTimestampsAndBlobsAsIndexValues() {
        String kind = uniqueKind();
        client().put(
                        Entity.newBuilder(key(kind, "a"))
                                .set("ts", com.google.cloud.Timestamp.ofTimeMicroseconds(5L))
                                .set("b", Blob.copyFrom(new byte[] {1, 2}))
                                .build());

        com.google.datastore.v1.Entity projected =
                run(SplittableQueries.ofKind(kind).toBuilder()
                                .addProjection(
                                        Projection.newBuilder()
                                                .setProperty(
                                                        PropertyReference.newBuilder()
                                                                .setName("ts")))
                                .addProjection(
                                        Projection.newBuilder()
                                                .setProperty(
                                                        PropertyReference.newBuilder()
                                                                .setName("b")))
                                .build())
                        .getEntityResults(0)
                        .getEntity();

        assertThat(projected.getPropertiesOrThrow("ts").getIntegerValue()).isEqualTo(5L);
        assertThat(projected.getPropertiesOrThrow("ts").getMeaning()).isEqualTo(18);
        assertThat(projected.getPropertiesOrThrow("b").getStringValue()).isEqualTo("\u0001\u0002");
        assertThat(projected.getPropertiesOrThrow("b").getMeaning()).isEqualTo(18);
    }

    @Test
    void theEmulatorReadsAtATimeOlderThanTheServiceKeeps() {
        String kind = uniqueKind();
        Timestamp now = run(SplittableQueries.ofKind(kind)).getReadTime();
        seed(kind, 1);
        Timestamp twoHoursAgo =
                Timestamp.newBuilder().setSeconds(now.getSeconds() - 2 * 3600).build();

        // The service refuses a read time older than an hour without point-in-time recovery; the
        // emulator answers it, and answers it as of that time: the entity is not there yet.
        assertThat(readAt(kind, twoHoursAgo).getEntityResultsCount()).isZero();
        assertThat(
                        readAt(kind, run(SplittableQueries.ofKind(kind)).getReadTime())
                                .getEntityResultsCount())
                .isOne();
    }

    @Test
    void theEmulatorAnswersAnOffsetOnlyBatchWithoutACursor() {
        String kind = uniqueKind();
        seed(kind, 10);

        // The service documents a skipped cursor whenever a batch skips results; the emulator sets
        // neither it nor the end cursor here. No page the source reads asks for a limit of
        // zero (only the GQL parse does, and it takes no cursor from the answer), and the source
        // continues a batch that returned entities from its end cursor, which the emulator sets.
        QueryResultBatch skipped =
                run(
                        SplittableQueries.ofKind(kind).toBuilder()
                                .setOffset(4)
                                .setLimit(Int32Value.of(0))
                                .build());
        assertThat(skipped.getSkippedResults()).isEqualTo(4);
        assertThat(skipped.getSkippedCursor().isEmpty()).isTrue();
        assertThat(skipped.getEndCursor().isEmpty()).isTrue();
        QueryResultBatch read =
                run(
                        SplittableQueries.ofKind(kind).toBuilder()
                                .setOffset(4)
                                .setLimit(Int32Value.of(2))
                                .build());
        assertThat(read.getEntityResultsCount()).isEqualTo(2);
        assertThat(read.getEndCursor().isEmpty()).isFalse();
    }

    private static QueryResultBatch readAt(String kind, Timestamp readTime) {
        return rpc().runQuery(
                        RunQueryRequest.newBuilder()
                                .setProjectId(PROJECT)
                                .setPartitionId(PARTITION)
                                .setReadOptions(ReadOptions.newBuilder().setReadTime(readTime))
                                .setQuery(SplittableQueries.ofKind(kind))
                                .build())
                .getBatch();
    }
}
