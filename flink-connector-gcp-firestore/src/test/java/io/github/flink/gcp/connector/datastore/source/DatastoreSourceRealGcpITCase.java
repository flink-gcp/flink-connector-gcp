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

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.StructuredQuery;
import com.google.datastore.v1.Projection;
import com.google.datastore.v1.PropertyFilter;
import com.google.datastore.v1.PropertyOrder;
import com.google.datastore.v1.PropertyReference;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.client.DatastoreHelper;
import io.github.flink.gcp.connector.datastore.AbstractDatastoreRealGcpITCase;
import io.github.flink.gcp.connector.datastore.source.batch.SplittableQueries;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The Datastore-mode source against the real service, through the public builder: what {@code
 * DatastoreSourceEmulatorITCase} cannot show, because the emulator samples no {@code __scatter__}
 * keys, orders keys as one implementation of the documented order, serves no index it lacks, and
 * answers read times the service refuses.
 */
@EnabledIfEnvironmentVariable(named = "FIRESTORE_IT_PROJECT", matches = ".+")
@Tag("gated")
class DatastoreSourceRealGcpITCase extends AbstractDatastoreRealGcpITCase {

    private static final Logger LOG = LoggerFactory.getLogger(DatastoreSourceRealGcpITCase.class);

    /** Enough entities for the splitter's sampling to find several {@code __scatter__} keys. */
    private static final int LARGE_KIND = 3000;

    /** One kind of {@link #LARGE_KIND} entities, written once for the tests that split it. */
    private static String largeKind;

    private static List<String> largeNames;

    @BeforeAll
    static void seedTheLargeKind() {
        largeKind = uniqueKind();
        largeNames = seed(largeKind, LARGE_KIND);
    }

    @Test
    void aKindIsReadOnceAcrossTheRangesTheSplitterSampled() throws Exception {
        assertThat(planned(builder -> builder.kind(largeKind).splitCount(8).pageSize(500), 3))
                .containsExactlyInAnyOrderElementsOf(largeNames);
        LOG.info("{} entities answered {} of 8 splits", LARGE_KIND, RecordingPlannerFactory.SPLITS);

        // More than one, or the read was a single range and showed nothing of the service's
        // sampling; at most the count asked for.
        assertThat(RecordingPlannerFactory.SPLITS).singleElement().isIn(2, 3, 4, 5, 6, 7, 8);
    }

    @Test
    void anEqualityFilteredKindIsReadOnceAcrossKeyRanges() throws Exception {
        // An equality filter with a key range ANDed on is served by the built-in indexes.
        Query query =
                SplittableQueries.ofKind(largeKind).toBuilder()
                        .setFilter(
                                DatastoreHelper.makeFilter(
                                        "m",
                                        PropertyFilter.Operator.EQUAL,
                                        DatastoreHelper.makeValue(3L)))
                        .build();
        List<String> expected = new ArrayList<>();
        for (int n = 0; n < LARGE_KIND; n++) {
            if (n % 7 == 3) {
                expected.add(largeNames.get(n));
            }
        }

        assertThat(planned(builder -> builder.query(query).splitCount(4), 2))
                .containsExactlyInAnyOrderElementsOf(expected);
        assertThat(RecordingPlannerFactory.SPLITS).singleElement().isIn(2, 3, 4);
    }

    @Test
    void aKeysOnlyProjectionIsReadOnceAcrossKeyRanges() throws Exception {
        Query keysOnly =
                SplittableQueries.ofKind(largeKind).toBuilder()
                        .addProjection(
                                Projection.newBuilder()
                                        .setProperty(
                                                PropertyReference.newBuilder().setName("__key__")))
                        .build();

        assertThat(planned(builder -> builder.query(keysOnly).splitCount(4), 2))
                .containsExactlyInAnyOrderElementsOf(largeNames);
        assertThat(RecordingPlannerFactory.SPLITS).singleElement().isIn(2, 3, 4);
    }

    @Test
    void aProjectionOfAPropertyIsReadAsOneSplit() throws Exception {
        // Split into key ranges, it would need a composite index of __key__ and the property,
        // which the service refused (FAILED_PRECONDITION) when measured on 2026-10-11.
        Query projection =
                SplittableQueries.ofKind(largeKind).toBuilder()
                        .addProjection(
                                Projection.newBuilder()
                                        .setProperty(PropertyReference.newBuilder().setName("n")))
                        .build();

        assertThat(planned(builder -> builder.query(projection), 2))
                .containsExactlyInAnyOrderElementsOf(largeNames);
        assertThat(RecordingPlannerFactory.SPLITS).as("the splitter is never asked").isEmpty();
    }

    @Test
    void keyNamesAreOrderedByTheirUtf8Bytes() throws Exception {
        // U+FF01 sorts after U+1F600's leading surrogate in Java's string order, and before it in
        // UTF-8 byte order; KeyRanges re-sorts the splitter's boundaries on the byte order.
        String kind = uniqueKind();
        client().put(
                        Entity.newBuilder(key(kind, "a！")).set("n", 1L).build(),
                        Entity.newBuilder(key(kind, "a😀")).set("n", 2L).build());
        assertThat("a！".compareTo("a😀")).isPositive();

        List<String> ordered = new ArrayList<>();
        client().run(
                        com.google.cloud.datastore.Query.newEntityQueryBuilder()
                                .setKind(kind)
                                .setOrderBy(StructuredQuery.OrderBy.asc("__key__"))
                                .build())
                .forEachRemaining(entity -> ordered.add(entity.getKey().getName()));

        assertThat(ordered).containsExactly("a！", "a😀");
    }

    @Test
    void aQueryThatNeedsAMissingIndexFailsPlanningNamingTheIndex() throws Exception {
        // An inequality on one property ordered by another needs a composite index, which a new
        // database has none of.
        String kind = uniqueKind();
        seed(kind, 3);
        Query needsIndex =
                SplittableQueries.ofKind(kind).toBuilder()
                        .setFilter(
                                DatastoreHelper.makeFilter(
                                        "n",
                                        PropertyFilter.Operator.GREATER_THAN,
                                        DatastoreHelper.makeValue(0L)))
                        .addOrder(DatastoreHelper.makeOrder("m", PropertyOrder.Direction.ASCENDING))
                        .build();

        assertThatThrownBy(() -> read(builder -> builder.query(needsIndex), 1))
                .hasStackTraceContaining("Failed to plan the Datastore read")
                .hasStackTraceContaining("If the cause names a missing index")
                .hasStackTraceContaining(
                        "FAILED_PRECONDITION: One possible index to serve the query");
    }

    @Test
    void aReadTimeBeforeTheDatabaseExistedFailsPlanning() throws Exception {
        String kind = uniqueKind();
        seed(kind, 1);
        Instant twoHoursAgo = Instant.now().minus(Duration.ofHours(2));

        assertThatThrownBy(() -> read(builder -> builder.kind(kind).readTime(twoHoursAgo), 1))
                .hasStackTraceContaining("Failed to plan the Datastore read")
                .hasStackTraceContaining(
                        "INVALID_ARGUMENT: The requested 'read_time' cannot be before database"
                                + " creation time");
    }

    @Test
    void aReadTimeInTheFutureFailsPlanning() throws Exception {
        String kind = uniqueKind();
        seed(kind, 1);
        Instant inAnHour = Instant.now().plus(Duration.ofHours(1));

        assertThatThrownBy(() -> read(builder -> builder.kind(kind).readTime(inAnHour), 1))
                .hasStackTraceContaining("Failed to plan the Datastore read")
                .hasStackTraceContaining(
                        "INVALID_ARGUMENT: The requested 'read_time' cannot be in the future");
    }

    /** Reads through {@link RecordingPlannerFactory}, which records the splitter's answer. */
    private static List<String> planned(
            UnaryOperator<DatastoreSourceBuilder<String>> customizer, int parallelism)
            throws Exception {
        RecordingPlannerFactory.SPLITS.clear();
        return read(
                builder ->
                        TestSources.withPlannerFactory(
                                customizer.apply(builder), new RecordingPlannerFactory()),
                parallelism);
    }

    private static List<String> read(
            UnaryOperator<DatastoreSourceBuilder<String>> customizer, int parallelism)
            throws Exception {
        return TestSources.collect(
                customizer
                        .apply(
                                DatastoreSource.<String>builder()
                                        .database(database())
                                        .deserializer(new TestSources.KeyNameDeserializer()))
                        .build(),
                parallelism);
    }

    /** Writes {@code count} entities of the kind, each with {@code n}, and returns their names. */
    private static List<String> seed(String kind, int count) {
        List<String> names = new ArrayList<>();
        List<FullEntity<?>> batch = new ArrayList<>();
        for (int n = 0; n < count; n++) {
            String name = "e" + String.format("%05d", n);
            batch.add(Entity.newBuilder(key(kind, name)).set("n", n).set("m", n % 7).build());
            names.add(name);
            if (batch.size() == 500) {
                client().put(batch.toArray(new FullEntity<?>[0]));
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            client().put(batch.toArray(new FullEntity<?>[0]));
        }
        return names;
    }
}
