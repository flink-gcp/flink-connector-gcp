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
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.StringValue;
import com.google.cloud.datastore.StructuredQuery;
import com.google.datastore.v1.CompositeFilter;
import com.google.datastore.v1.EntityResult;
import com.google.datastore.v1.Filter;
import com.google.datastore.v1.GqlQuery;
import com.google.datastore.v1.LookupRequest;
import com.google.datastore.v1.LookupResponse;
import com.google.datastore.v1.Projection;
import com.google.datastore.v1.PropertyFilter;
import com.google.datastore.v1.PropertyReference;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.QueryResultBatch;
import com.google.datastore.v1.ReadOptions;
import com.google.datastore.v1.RunQueryRequest;
import com.google.datastore.v1.RunQueryResponse;
import com.google.datastore.v1.client.DatastoreHelper;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Timestamp;
import io.github.flink.gcp.connector.datastore.AbstractDatastoreRealGcpITCase;
import io.github.flink.gcp.connector.datastore.source.batch.SplittableQueries;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The service's answers to the raw requests the Datastore-mode source makes or relies on, where the
 * emulator's answer is known to differ or cannot be taken as evidence: {@code
 * DatastoreEmulatorReadDeviationITCase} pins the emulator's side of each.
 */
@EnabledIfEnvironmentVariable(named = "FIRESTORE_IT_PROJECT", matches = ".+")
@Tag("gated")
class DatastoreQueryRealGcpITCase extends AbstractDatastoreRealGcpITCase {

    private static RunQueryRequest.Builder request() {
        return RunQueryRequest.newBuilder()
                .setProjectId(PROJECT)
                .setDatabaseId(database().getDatabaseId())
                .setPartitionId(partition());
    }

    private static QueryResultBatch run(Query query) {
        return rpc().runQuery(request().setQuery(query).build()).getBatch();
    }

    private static Projection project(String property) {
        return Projection.newBuilder()
                .setProperty(PropertyReference.newBuilder().setName(property))
                .build();
    }

    @Test
    void aProjectionWithAKeyRangeNeedsACompositeIndex() {
        String kind = uniqueKind();
        seed(kind, 10);
        Query ranged =
                SplittableQueries.ofKind(kind).toBuilder()
                        .addProjection(project("n"))
                        .setFilter(
                                Filter.newBuilder()
                                        .setCompositeFilter(
                                                CompositeFilter.newBuilder()
                                                        .setOp(CompositeFilter.Operator.AND)
                                                        .addFilters(
                                                                keyFilter(
                                                                        PropertyFilter.Operator
                                                                                .GREATER_THAN_OR_EQUAL,
                                                                        key(kind, "e002")))
                                                        .addFilters(
                                                                keyFilter(
                                                                        PropertyFilter.Operator
                                                                                .LESS_THAN,
                                                                        key(kind, "e005")))))
                        .build();

        // The projection alone is served by the built-in indexes; with a key range ANDed on, as the
        // source's splits would add, the service asks for a composite index. This is why the
        // source reads a projection of properties as one split.
        assertThat(
                        run(SplittableQueries.ofKind(kind).toBuilder()
                                        .addProjection(project("n"))
                                        .build())
                                .getEntityResultsCount())
                .isEqualTo(10);
        assertThatThrownBy(() -> run(ranged))
                .isInstanceOf(ApiException.class)
                .hasMessageContaining("FAILED_PRECONDITION: no matching index found");
    }

    @Test
    void projectedTimestampsAndBlobsComeBackAsIndexValuesAndEveryResultCarriesItsKey() {
        // One property per projection: projecting both at once needs a composite index.
        String kind = uniqueKind();
        client().put(
                        Entity.newBuilder(key(kind, "a"))
                                .set("ts", com.google.cloud.Timestamp.ofTimeMicroseconds(5L))
                                .set("b", Blob.copyFrom(new byte[] {1, 2}))
                                .build());

        com.google.datastore.v1.Entity timestamp = projectOne(kind, "ts");
        com.google.datastore.v1.Entity blob = projectOne(kind, "b");

        assertThat(timestamp.hasKey()).isTrue();
        assertThat(timestamp.getPropertiesOrThrow("ts").getIntegerValue()).isEqualTo(5L);
        assertThat(timestamp.getPropertiesOrThrow("ts").getMeaning()).isEqualTo(18);
        assertThat(blob.hasKey()).isTrue();
        assertThat(blob.getPropertiesOrThrow("b").getStringValue()).isEqualTo("\u0001\u0002");
        assertThat(blob.getPropertiesOrThrow("b").getMeaning()).isEqualTo(18);
    }

    private static com.google.datastore.v1.Entity projectOne(String kind, String property) {
        List<EntityResult> results =
                run(SplittableQueries.ofKind(kind).toBuilder()
                                .addProjection(project(property))
                                .build())
                        .getEntityResultsList();
        assertThat(results).as(property).hasSize(1);
        return results.get(0).getEntity();
    }

    @Test
    void aGqlQueryWithLimitZeroIsParsedWithoutReadingAnEntity() {
        String kind = uniqueKind();
        seed(kind, 3);

        RunQueryResponse parsed = gql("SELECT * FROM `" + kind + "` LIMIT 0");

        assertThat(parsed.getBatch().getEntityResultsCount()).isZero();
        assertThat(parsed.getQuery().getKind(0).getName()).isEqualTo(kind);
        assertThat(parsed.getQuery().getLimit().getValue()).isZero();
    }

    @Test
    void limitZeroAfterALimitOrAnOffsetIsRefused() {
        // What makes the planner run such a query as written rather than parse it with LIMIT 0.
        String kind = uniqueKind();
        seed(kind, 3);

        for (String tail : new String[] {"LIMIT 2", "OFFSET 1"}) {
            assertThatThrownBy(() -> gql("SELECT * FROM `" + kind + "` " + tail + " LIMIT 0"))
                    .as(tail)
                    .isInstanceOf(ApiException.class)
                    .satisfies(
                            e ->
                                    assertThat(((ApiException) e).getStatusCode().getCode())
                                            .isEqualTo(StatusCode.Code.INVALID_ARGUMENT));
        }
    }

    @Test
    void anOffsetOnlyBatchCarriesASkippedCursor() {
        // The emulator answers this batch without one; the service documents it.
        String kind = uniqueKind();
        seed(kind, 10);

        QueryResultBatch skipped =
                run(
                        SplittableQueries.ofKind(kind).toBuilder()
                                .setOffset(4)
                                .setLimit(Int32Value.of(0))
                                .build());

        assertThat(skipped.getSkippedResults()).isEqualTo(4);
        assertThat(skipped.getSkippedCursor().isEmpty()).isFalse();
    }

    @Test
    void aBatchOfLargeEntitiesStopsShortOfItsLimitAndIsNotFinished() {
        // Measures the service's cap on one response, about 6 MiB of entities here, which is what
        // makes a short NOT_FINISHED batch something the reader has to continue from.
        String kind = uniqueKind();
        String halfMebibyte = "z".repeat(512 * 1024);
        for (int i = 0; i < 12; i++) {
            client().put(
                            Entity.newBuilder(key(kind, "big" + i))
                                    .set(
                                            "s",
                                            StringValue.newBuilder(halfMebibyte)
                                                    .setExcludeFromIndexes(true)
                                                    .build())
                                    .build());
        }

        QueryResultBatch batch =
                run(SplittableQueries.ofKind(kind).toBuilder().setLimit(Int32Value.of(12)).build());

        assertThat(batch.getEntityResultsCount()).isLessThan(12);
        assertThat(batch.getMoreResults()).isEqualTo(QueryResultBatch.MoreResultsType.NOT_FINISHED);
        assertThat(batch.getEndCursor().isEmpty()).isFalse();
    }

    @Test
    void metadataQueriesCarryCursorsAndResumeFromTheirEndCursor() {
        // The emulator answers __kind__ and __namespace__ without either cursor and __property__
        // with nothing; the source refuses the three kinds on that evidence (#1717 reconsiders).
        // Two kinds and two namespaces, so each metadata kind has more than the one result the
        // first page takes and its continuation has something to return.
        seed(uniqueKind(), 1);
        seed(uniqueKind(), 1);
        client().put(
                        Entity.newBuilder(
                                        Key.newBuilder(
                                                        PROJECT,
                                                        uniqueKind(),
                                                        "n",
                                                        database().getDatabaseId())
                                                .setNamespace("tenant-metadata")
                                                .build())
                                .set("v", 1L)
                                .build());

        for (String metadata : new String[] {"__kind__", "__namespace__", "__property__"}) {
            // All three at one read time, so a kind or property appearing between them cannot
            // change the count.
            QueryResultBatch all = run(SplittableQueries.ofKind(metadata));
            ReadOptions atAll = ReadOptions.newBuilder().setReadTime(all.getReadTime()).build();
            QueryResultBatch first =
                    runAt(
                            SplittableQueries.ofKind(metadata).toBuilder()
                                    .setLimit(Int32Value.of(1))
                                    .build(),
                            atAll);
            QueryResultBatch rest =
                    runAt(
                            SplittableQueries.ofKind(metadata).toBuilder()
                                    .setStartCursor(first.getEndCursor())
                                    .build(),
                            atAll);

            assertThat(all.getEntityResultsCount()).as(metadata).isPositive();
            assertThat(all.getEntityResultsList())
                    .as(metadata)
                    .allSatisfy(result -> assertThat(result.getCursor().isEmpty()).isFalse());
            assertThat(first.getEndCursor().isEmpty()).as(metadata).isFalse();
            assertThat(rest.getEntityResultsCount()).as(metadata).isPositive();
            List<com.google.datastore.v1.Key> paged = new ArrayList<>(keysOf(first));
            paged.addAll(keysOf(rest));
            assertThat(paged).as(metadata).containsExactlyElementsOf(keysOf(all));
        }
    }

    private static List<com.google.datastore.v1.Key> keysOf(QueryResultBatch batch) {
        List<com.google.datastore.v1.Key> keys = new ArrayList<>();
        for (EntityResult result : batch.getEntityResultsList()) {
            keys.add(result.getEntity().getKey());
        }
        return keys;
    }

    private static QueryResultBatch runAt(Query query, ReadOptions readOptions) {
        return rpc().runQuery(request().setReadOptions(readOptions).setQuery(query).build())
                .getBatch();
    }

    @Test
    void aKindQueryHonoursTheReadTimeAndTheNamespace() {
        String before = uniqueKind();
        seed(before, 1);
        Timestamp readTime = run(SplittableQueries.ofKind(before)).getReadTime();
        String after = uniqueKind();
        seed(after, 1);

        List<String> atReadTime =
                kindNames(
                        rpc().runQuery(
                                        request()
                                                .setReadOptions(
                                                        ReadOptions.newBuilder()
                                                                .setReadTime(readTime))
                                                .setQuery(SplittableQueries.ofKind("__kind__"))
                                                .build())
                                .getBatch());
        List<String> inANamespace =
                kindNames(
                        rpc().runQuery(
                                        request()
                                                .setPartitionId(
                                                        partition().toBuilder()
                                                                .setNamespaceId("tenant"))
                                                .setQuery(SplittableQueries.ofKind("__kind__"))
                                                .build())
                                .getBatch());

        assertThat(atReadTime).contains(before).doesNotContain(after);
        assertThat(inANamespace).doesNotContain(before, after);
    }

    @Test
    void aReadTimeFinerThanAMicrosecondIsRefused() {
        String kind = uniqueKind();
        seed(kind, 1);
        Timestamp now = run(SplittableQueries.ofKind(kind)).getReadTime();

        assertThat(lookupAt(key(kind, "e00000"), now).getFoundCount()).isOne();
        assertThatThrownBy(
                        () ->
                                lookupAt(
                                        key(kind, "e00000"),
                                        now.toBuilder().setNanos(now.getNanos() + 1).build()))
                .isInstanceOf(ApiException.class)
                .hasMessageContaining("timestamp cannot have more than microseconds precision");
    }

    @Test
    void versionsSitOnWholeMicroseconds() {
        // The premise behind "the truncated read time reads the same data", which no Datastore
        // documentation states.
        String kind = uniqueKind();
        seed(kind, 1);
        Key key = key(kind, "e00000");
        Timestamp written = lookupAt(key, null).getFound(0).getUpdateTime();
        Timestamp earlier =
                written.getNanos() >= 1_000
                        ? written.toBuilder().setNanos(written.getNanos() - 1_000).build()
                        : written.toBuilder()
                                .setSeconds(written.getSeconds() - 1)
                                .setNanos(999_999_000)
                                .build();

        assertThat(written.getNanos() % 1_000).isZero();
        assertThat(lookupAt(key, written).getFoundCount()).isOne();
        assertThat(lookupAt(key, earlier).getFoundCount()).isZero();
    }

    @Test
    void aTimestampValueIsFlooredToTheMicrosecondAndSoIsAFilterValue() {
        // Unlike Native mode, where an equality filter carrying the unfloored value matches
        // nothing.
        String kind = uniqueKind();
        com.google.cloud.Timestamp fine =
                com.google.cloud.Timestamp.ofTimeSecondsAndNanos(1_700_000_000L, 123_456_789);
        com.google.cloud.Timestamp floored =
                com.google.cloud.Timestamp.ofTimeSecondsAndNanos(1_700_000_000L, 123_456_000);
        client().put(Entity.newBuilder(key(kind, "a")).set("t", fine).build());

        assertThat(read(key(kind, "a")).getTimestamp("t")).isEqualTo(floored);
        assertThat(matches(kind, fine)).isOne();
        assertThat(matches(kind, floored)).isOne();
    }

    private static int matches(String kind, com.google.cloud.Timestamp value) {
        int[] count = {0};
        client().run(
                        com.google.cloud.datastore.Query.newEntityQueryBuilder()
                                .setKind(kind)
                                .setFilter(StructuredQuery.PropertyFilter.eq("t", value))
                                .build())
                .forEachRemaining(entity -> count[0]++);
        return count[0];
    }

    private static LookupResponse lookupAt(Key key, @Nullable Timestamp readTime) {
        LookupRequest.Builder lookup =
                LookupRequest.newBuilder()
                        .setProjectId(PROJECT)
                        .setDatabaseId(database().getDatabaseId())
                        .addKeys(keyProto(key));
        if (readTime != null) {
            lookup.setReadOptions(ReadOptions.newBuilder().setReadTime(readTime));
        }
        return rpc().lookup(lookup.build());
    }

    private static Filter keyFilter(PropertyFilter.Operator op, Key key) {
        return DatastoreHelper.makeFilter("__key__", op, DatastoreHelper.makeValue(keyProto(key)))
                .build();
    }

    private static RunQueryResponse gql(String query) {
        return rpc().runQuery(
                        request()
                                .setGqlQuery(
                                        GqlQuery.newBuilder()
                                                .setQueryString(query)
                                                .setAllowLiterals(true))
                                .build());
    }

    private static List<String> kindNames(QueryResultBatch batch) {
        List<String> names = new ArrayList<>();
        for (EntityResult result : batch.getEntityResultsList()) {
            names.add(result.getEntity().getKey().getPath(0).getName());
        }
        return names;
    }

    private static void seed(String kind, int count) {
        List<FullEntity<?>> batch = new ArrayList<>();
        for (int n = 0; n < count; n++) {
            batch.add(
                    Entity.newBuilder(key(kind, "e" + String.format("%05d", n)))
                            .set("n", n)
                            .build());
        }
        client().put(batch.toArray(new FullEntity<?>[0]));
    }
}
