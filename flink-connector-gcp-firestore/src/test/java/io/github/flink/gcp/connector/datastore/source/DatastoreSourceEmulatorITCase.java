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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ExceptionUtils;

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityQuery;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.StructuredQuery;
import com.google.cloud.datastore.spi.v1.DatastoreRpc;
import com.google.datastore.v1.Projection;
import com.google.datastore.v1.PropertyFilter;
import com.google.datastore.v1.PropertyOrder;
import com.google.datastore.v1.PropertyReference;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.QueryResultBatch;
import com.google.datastore.v1.RunQueryRequest;
import com.google.datastore.v1.client.DatastoreHelper;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Timestamp;
import io.github.flink.gcp.connector.datastore.AbstractDatastoreEmulatorITCase;
import io.github.flink.gcp.connector.datastore.source.batch.SplittableQueries;
import io.github.flink.gcp.connector.datastore.source.serializer.DatastoreEntityDeserializationSchema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The source against the emulator in Datastore mode, through the public builder: planning, the GQL
 * parse, the splits' wire form, paging, cursors, the offset, the snapshot time, namespaces and
 * projection all reach a service that answers them.
 *
 * <p><b>What this suite cannot prove.</b> The emulator answers the splitter's {@code __scatter__}
 * sampling with no keys and keeps no statistics, so the service's key-range splitting is never
 * exercised here: a kind is read through the client library's splitter, which then answers with one
 * range, or has its ranges chosen by the test through {@link FixedSplitsPlannerFactory}. Real split
 * counts and the statistics estimate are the gated real-service suite's (#1546).
 */
class DatastoreSourceEmulatorITCase extends AbstractDatastoreEmulatorITCase {

    @BeforeEach
    void forgetTheSplitsAskedFor() {
        FixedSplitsPlannerFactory.ASKED.clear();
    }

    private static List<String> read(UnaryOperator<DatastoreSourceBuilder<String>> customizer)
            throws Exception {
        return read(customizer, new TestSources.KeyNameDeserializer(), 2);
    }

    private static <T> List<T> read(
            UnaryOperator<DatastoreSourceBuilder<T>> customizer,
            DatastoreEntityDeserializationSchema<T> deserializer,
            int parallelism)
            throws Exception {
        Source<T, ?, ?> source =
                customizer
                        .apply(
                                DatastoreSource.<T>builder()
                                        .database(database())
                                        .deserializer(deserializer)
                                        .emulatorEndpoint(emulatorEndpoint()))
                        .build();
        return TestSources.collect(source, parallelism);
    }

    /** Writes entities named {@code e000}.., each with {@code n} and {@code k = n % 3}. */
    private static List<String> seed(String kind, int count) {
        return seed(kind, null, count);
    }

    private static List<String> seed(String kind, String namespace, int count) {
        List<String> names = new ArrayList<>();
        List<FullEntity<?>> batch = new ArrayList<>();
        for (int n = 0; n < count; n++) {
            String name = "e" + String.format("%03d", n);
            names.add(name);
            Key.Builder key = Key.newBuilder(PROJECT, kind, name);
            if (namespace != null) {
                key.setNamespace(namespace);
            }
            batch.add(Entity.newBuilder(key.build()).set("n", n).set("k", n % 3).build());
            if (batch.size() == 100) {
                client().put(batch.toArray(new FullEntity<?>[0]));
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            client().put(batch.toArray(new FullEntity<?>[0]));
        }
        return names;
    }

    /** Returns the read time the emulator answers a query with now. */
    private static Timestamp now(String kind) {
        return queryNow(kind).getReadTime();
    }

    /** Returns the update time of the kind's first entity, as the emulator answers it now. */
    private static Timestamp updateTime(String kind) {
        return queryNow(kind).getEntityResults(0).getUpdateTime();
    }

    private static QueryResultBatch queryNow(String kind) {
        DatastoreRpc rpc = (DatastoreRpc) client().getOptions().getRpc();
        return rpc.runQuery(
                        RunQueryRequest.newBuilder()
                                .setProjectId(PROJECT)
                                .setPartitionId(
                                        com.google.datastore.v1.PartitionId.newBuilder()
                                                .setProjectId(PROJECT))
                                .setQuery(SplittableQueries.ofKind(kind))
                                .build())
                .getBatch();
    }

    @Test
    void readsEveryEntityOfAKindInPagesThroughTheLibrarysSplitter() throws Exception {
        String kind = uniqueKind();
        List<String> names = seed(kind, 40);
        seed(uniqueKind(), 3);

        // No split count: the estimate finds no statistics and asks the client library's splitter,
        // over its HTTP client, for the lower bound; the emulator's sampling finds no keys, so the
        // splitter answers with the whole kind.
        assertThat(read(builder -> builder.kind(kind).pageSize(7)))
                .containsExactlyInAnyOrderElementsOf(names);
    }

    @Test
    void readsNothingFromAnEmptyKind() throws Exception {
        assertThat(read(builder -> builder.kind(uniqueKind()))).isEmpty();
    }

    @Test
    void readsEveryEntityOnceAcrossKeyRanges() throws Exception {
        String kind = uniqueKind();
        List<String> names = seed(kind, 60);
        // Boundaries at existing keys and one between two: an inclusive start and an exclusive end
        // must hand each boundary entity to exactly one range, and a boundary naming no entity,
        // which a sampled key may, must lose nothing on either side.
        List<String> boundaries = List.of(names.get(10), names.get(25), names.get(40) + "0");

        List<String> read =
                read(
                        builder ->
                                TestSources.withPlannerFactory(
                                        builder.kind(kind).pageSize(4),
                                        new FixedSplitsPlannerFactory(
                                                emulatorEndpoint(), boundaries)),
                        new TestSources.KeyNameDeserializer(),
                        3);

        assertThat(read).containsExactlyInAnyOrderElementsOf(names);
        assertThat(FixedSplitsPlannerFactory.ASKED)
                .as("without statistics, the estimate asks for the lower bound")
                .containsExactly(12);
    }

    @Test
    void readsEachEntityOnceWhenTheSplitterOrdersBoundariesAsJavaStrings() throws Exception {
        // The client library's splitter sorts sampled keys as Java strings, which puts "a！"
        // (U+FF01) after "a😀" (U+1F600); the emulator, like the service, sorts by UTF-8 bytes
        // and puts it first. Handed over in Java order, the ranges are laid out again.
        String kind = uniqueKind();
        List<String> names = List.of("a", "a！", "a！x", "a😀", "a😀x", "b");
        client().put(
                        names.stream()
                                .map(name -> Entity.newBuilder(key(kind, name)).build())
                                .toArray(FullEntity<?>[]::new));

        List<String> read =
                read(
                        builder ->
                                TestSources.withPlannerFactory(
                                        builder.kind(kind),
                                        new FixedSplitsPlannerFactory(
                                                emulatorEndpoint(), List.of("a😀", "a！"))),
                        new TestSources.KeyNameDeserializer(),
                        2);

        assertThat(read).containsExactlyInAnyOrderElementsOf(names);
    }

    @Test
    void cutsAnEqualityQueryIntoKeyRangesKeepingItsFilter() throws Exception {
        String kind = uniqueKind();
        List<String> names = seed(kind, 30);
        Query ones =
                SplittableQueries.ofKind(kind).toBuilder()
                        .setFilter(
                                DatastoreHelper.makeFilter(
                                        "k",
                                        PropertyFilter.Operator.EQUAL,
                                        DatastoreHelper.makeValue(1)))
                        .build();

        List<String> read =
                read(
                        builder ->
                                TestSources.withPlannerFactory(
                                        builder.query(ones).splitCount(3),
                                        new FixedSplitsPlannerFactory(
                                                emulatorEndpoint(),
                                                List.of(names.get(10), names.get(20)))),
                        new TestSources.KeyNameDeserializer(),
                        2);

        assertThat(read)
                .containsExactlyInAnyOrderElementsOf(
                        names.stream()
                                .filter(name -> Integer.parseInt(name.substring(1)) % 3 == 1)
                                .collect(Collectors.toList()));
        assertThat(FixedSplitsPlannerFactory.ASKED).containsExactly(3);
    }

    @Test
    void readsAnOrderedQueryWithItsLimitAndOffsetAcrossPagesAsOneSplit() throws Exception {
        String kind = uniqueKind();
        seed(kind, 30);
        Query query =
                SplittableQueries.ofKind(kind).toBuilder()
                        .setFilter(
                                DatastoreHelper.makeFilter(
                                        "n",
                                        PropertyFilter.Operator.GREATER_THAN_OR_EQUAL,
                                        DatastoreHelper.makeValue(3)))
                        .addOrder(
                                DatastoreHelper.makeOrder("n", PropertyOrder.Direction.DESCENDING))
                        .setOffset(2)
                        .setLimit(Int32Value.of(17))
                        .build();
        EntityQuery same =
                com.google.cloud.datastore.Query.newEntityQueryBuilder()
                        .setKind(kind)
                        .setFilter(StructuredQuery.PropertyFilter.ge("n", 3))
                        .setOrderBy(StructuredQuery.OrderBy.desc("n"))
                        .setOffset(2)
                        .setLimit(17)
                        .build();
        List<String> expected = new ArrayList<>();
        QueryResults<Entity> results = client().run(same);
        results.forEachRemaining(entity -> expected.add(entity.getKey().getName()));
        assertThat(expected).hasSize(17);

        // One split, read in order: pages of four cross the offset and the limit.
        assertThat(
                        read(
                                builder -> builder.query(query).pageSize(4),
                                new TestSources.KeyNameDeserializer(),
                                1))
                .containsExactlyElementsOf(expected);
    }

    @Test
    void stopsAQueryAtItsOwnEndCursorAcrossPages() throws Exception {
        String kind = uniqueKind();
        List<String> names = seed(kind, 12);
        DatastoreRpc rpc = (DatastoreRpc) client().getOptions().getRpc();
        // The service's cursor after the fifth entity in key order.
        com.google.protobuf.ByteString afterFifth =
                rpc.runQuery(
                                RunQueryRequest.newBuilder()
                                        .setProjectId(PROJECT)
                                        .setPartitionId(
                                                com.google.datastore.v1.PartitionId.newBuilder()
                                                        .setProjectId(PROJECT))
                                        .setQuery(
                                                SplittableQueries.ofKind(kind).toBuilder()
                                                        .setLimit(Int32Value.of(5)))
                                        .build())
                        .getBatch()
                        .getEndCursor();
        Query untilFifth =
                SplittableQueries.ofKind(kind).toBuilder().setEndCursor(afterFifth).build();

        // Pages of two cross the end cursor, where the service says MORE_RESULTS_AFTER_CURSOR.
        assertThat(
                        read(
                                builder -> builder.query(untilFifth).pageSize(2),
                                new TestSources.KeyNameDeserializer(),
                                1))
                .containsExactlyElementsOf(names.subList(0, 5));
    }

    @Test
    void readsAGqlQuery() throws Exception {
        String kind = uniqueKind();
        List<String> names = seed(kind, 12);

        assertThat(read(builder -> builder.gqlQuery("SELECT * FROM `" + kind + "` WHERE k = 2")))
                .containsExactlyInAnyOrder(names.get(2), names.get(5), names.get(8), names.get(11));
    }

    @Test
    void readsAGqlQueryThatEndsInALimitByRunningItToParseIt() throws Exception {
        String kind = uniqueKind();
        List<String> names = seed(kind, 12);

        // LIMIT 0 cannot follow the query's own clause, so the planner runs the query as written
        // to parse it, and reads the parsed query, its limit included.
        assertThat(
                        read(
                                builder ->
                                        builder.gqlQuery(
                                                "SELECT * FROM `"
                                                        + kind
                                                        + "` ORDER BY n LIMIT 4 OFFSET 1")))
                .containsExactly(names.get(1), names.get(2), names.get(3), names.get(4));
    }

    @Test
    void refusesASplitCountForAGqlQueryThatCannotBeSplit() {
        String kind = uniqueKind();
        seed(kind, 2);

        assertThatThrownBy(
                        () ->
                                read(
                                        builder ->
                                                builder.gqlQuery(
                                                                "SELECT * FROM `"
                                                                        + kind
                                                                        + "` ORDER BY n")
                                                        .splitCount(4)))
                .hasStackTraceContaining("Failed to plan the Datastore read")
                .hasStackTraceContaining("because it orders its results");
    }

    @ParameterizedTest
    @ValueSource(strings = {"__kind__", "__namespace__"})
    void refusesAGqlQueryOfAMetadataKindWhenItPlans(String metadataKind) {
        // Without the refusal, the reader fails on the first metadata entity, which the emulator
        // answers without a cursor: seed a kind in a namespace so that both kinds have one.
        seed(uniqueKind(), "tenant", 1);

        assertThatThrownBy(() -> read(builder -> builder.gqlQuery("SELECT * FROM " + metadataKind)))
                .hasStackTraceContaining("Failed to plan the Datastore read")
                .hasStackTraceContaining(
                        "The source cannot read the query the GQL query parsed into: "
                                + metadataKind
                                + " is a metadata kind")
                .hasStackTraceContaining("read metadata with the client library instead")
                .satisfies(
                        failure ->
                                assertThat(ExceptionUtils.stringifyException(failure))
                                        .doesNotContain("cursor must not be empty"));
    }

    @Test
    void readsOnlyTheConfiguredNamespace() throws Exception {
        String kind = uniqueKind();
        seed(kind, 5);
        List<String> namespaced = seed(kind, "tenant", 3);

        assertThat(read(builder -> builder.kind(kind).namespace("tenant")))
                .containsExactlyInAnyOrderElementsOf(namespaced);
    }

    @Test
    void readsAtTheConfiguredReadTimeOnEveryPage() throws Exception {
        String kind = uniqueKind();
        client().put(
                        Entity.newBuilder(key(kind, "a")).set("v", 1).build(),
                        Entity.newBuilder(key(kind, "b")).set("v", 1).build());
        Timestamp before = now(kind);
        client().put(
                        Entity.newBuilder(key(kind, "a")).set("v", 2).build(),
                        Entity.newBuilder(key(kind, "b")).set("v", 2).build(),
                        Entity.newBuilder(key(kind, "later")).set("v", 3).build());

        List<String> read =
                read(
                        builder ->
                                builder.kind(kind)
                                        // One entity per page: the second page must read at the
                                        // read time as well as the first.
                                        .pageSize(1)
                                        .readTime(
                                                Instant.ofEpochSecond(
                                                        before.getSeconds(), before.getNanos())),
                        new PropertyDeserializer("v"),
                        1);

        assertThat(read).containsExactlyInAnyOrder("a=1", "b=1");
    }

    @Test
    void readsAtAReadTimeFinerThanAMicrosecond() throws Exception {
        String kind = uniqueKind();
        client().put(Entity.newBuilder(key(kind, "a")).set("v", 1).build());
        Timestamp written = updateTime(kind);
        client().put(Entity.newBuilder(key(kind, "a")).set("v", 2).build());
        // The emulator refuses a read time with nanoseconds (the API documents only
        // microseconds), which an Instant.now() can carry; the builder truncates this one to the
        // microsecond of the first write.
        Instant readTime = Instant.ofEpochSecond(written.getSeconds(), written.getNanos() + 999);
        assertThat(readTime.getNano() % 1_000).isNotZero();

        List<String> read =
                read(
                        builder -> builder.kind(kind).readTime(readTime),
                        new PropertyDeserializer("v"),
                        1);

        assertThat(read).containsExactly("a=1");
    }

    @Test
    void refusesAReadTimeInTheFutureWhenItPlans() {
        String kind = uniqueKind();

        assertThatThrownBy(
                        () ->
                                read(
                                        builder ->
                                                builder.kind(kind)
                                                        .readTime(Instant.now().plusSeconds(3600))))
                .hasStackTraceContaining("Failed to plan the Datastore read")
                .hasStackTraceContaining("configured read time");
    }

    @Test
    void readsOnlyTheProjectedProperties() throws Exception {
        String kind = uniqueKind();
        client().put(Entity.newBuilder(key(kind, "a")).set("keep", "x").set("drop", "y").build());
        Query projected =
                SplittableQueries.ofKind(kind).toBuilder()
                        .addProjection(
                                Projection.newBuilder()
                                        .setProperty(
                                                PropertyReference.newBuilder().setName("keep")))
                        .build();

        assertThat(read(builder -> builder.query(projected), new PropertyNamesDeserializer(), 1))
                .containsExactly("[keep]");
    }

    @Test
    void readsProjectedTimestampsAndBlobsAsTheirTypes() throws Exception {
        // A projection returns both as index values, which the reader reads back.
        String kind = uniqueKind();
        com.google.cloud.Timestamp at =
                com.google.cloud.Timestamp.ofTimeSecondsAndNanos(1_700_000_000L, 123_456_000);
        client().put(
                        Entity.newBuilder(key(kind, "a"))
                                .set("ts", at)
                                .set(
                                        "b",
                                        com.google.cloud.datastore.Blob.copyFrom(new byte[] {1, 2}))
                                .build());
        Query projected =
                SplittableQueries.ofKind(kind).toBuilder()
                        .addProjection(
                                Projection.newBuilder()
                                        .setProperty(PropertyReference.newBuilder().setName("ts")))
                        .addProjection(
                                Projection.newBuilder()
                                        .setProperty(PropertyReference.newBuilder().setName("b")))
                        .build();

        assertThat(read(builder -> builder.query(projected), new TypedDeserializer(), 1))
                .containsExactly(at + " " + java.util.Arrays.toString(new byte[] {1, 2}));
    }

    /** Emits a projected timestamp and blob, read through their typed getters. */
    private static final class TypedDeserializer
            implements DatastoreEntityDeserializationSchema<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(Entity entity, Collector<String> out) {
            out.collect(
                    entity.getTimestamp("ts")
                            + " "
                            + java.util.Arrays.toString(entity.getBlob("b").toByteArray()));
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return Types.STRING;
        }
    }

    /** Emits {@code name=value} of one property. */
    private static final class PropertyDeserializer
            implements DatastoreEntityDeserializationSchema<String> {

        private static final long serialVersionUID = 1L;

        private final String property;

        private PropertyDeserializer(String property) {
            this.property = property;
        }

        @Override
        public void deserialize(Entity entity, Collector<String> out) {
            out.collect(entity.getKey().getName() + "=" + entity.getLong(property));
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return Types.STRING;
        }
    }

    /** Emits the sorted names of the properties an entity carries. */
    private static final class PropertyNamesDeserializer
            implements DatastoreEntityDeserializationSchema<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(Entity entity, Collector<String> out) {
            out.collect(
                    entity.getNames().stream().sorted().collect(Collectors.toList()).toString());
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return Types.STRING;
        }
    }
}
