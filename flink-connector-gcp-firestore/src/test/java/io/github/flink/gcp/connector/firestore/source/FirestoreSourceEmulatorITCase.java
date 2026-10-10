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

package io.github.flink.gcp.connector.firestore.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.util.Collector;

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.FieldPath;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.WriteBatch;
import io.github.flink.gcp.connector.firestore.AbstractFirestoreEmulatorITCase;
import io.github.flink.gcp.connector.firestore.source.serializer.FirestoreDocumentDeserializationSchema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The source against the Firestore emulator, through the public builder: planning, the splits' wire
 * form, paging, cursors, the snapshot time and projection all reach a service that answers them.
 *
 * <p><b>What this suite cannot prove.</b> The emulator answers {@code PartitionQuery} with {@code
 * UNIMPLEMENTED}, so the service's partitioning is never exercised here: a scan either asks for one
 * partition, which the client library answers without a call, or has its boundaries chosen by the
 * test through {@link FixedPartitionsPlannerFactory}. Real partition counts are the gated
 * real-service suite's (#1546).
 */
class FirestoreSourceEmulatorITCase extends AbstractFirestoreEmulatorITCase {

    private static List<String> read(UnaryOperator<FirestoreSourceBuilder<String>> customizer)
            throws Exception {
        return read(customizer, new TestSources.DocumentPathDeserializer(), 2);
    }

    private static <T> List<T> read(
            UnaryOperator<FirestoreSourceBuilder<T>> customizer,
            FirestoreDocumentDeserializationSchema<T> deserializer,
            int parallelism)
            throws Exception {
        Source<T, ?, ?> source =
                customizer
                        .apply(
                                FirestoreSource.<T>builder()
                                        .database(database())
                                        .deserializer(deserializer)
                                        .emulatorEndpoint(emulatorEndpoint()))
                        .build();
        return TestSources.collect(source, parallelism);
    }

    /** Writes documents, each with {@code n} and {@code k = n % 3}, and returns their paths. */
    private static List<String> seed(List<String> paths) throws Exception {
        WriteBatch batch = client().batch();
        for (int n = 0; n < paths.size(); n++) {
            batch.set(client().document(paths.get(n)), Map.of("n", n, "k", n % 3));
        }
        batch.commit().get(30, TimeUnit.SECONDS);
        return paths;
    }

    private static List<String> collectionAtEveryDepth(String group, int count) {
        List<String> paths = new ArrayList<>();
        for (int n = 0; n < count; n++) {
            paths.add(
                    n % 2 == 0
                            ? group + "/d" + String.format("%03d", n)
                            : "parents/p"
                                    + (n % 5)
                                    + "/"
                                    + group
                                    + "/d"
                                    + String.format("%03d", n));
        }
        return paths;
    }

    @Test
    void scansACollectionGroupAtEveryDepth() throws Exception {
        String group = uniqueCollection();
        List<String> paths = seed(collectionAtEveryDepth(group, 40));
        seed(List.of(uniqueCollection() + "/unrelated"));

        assertThat(read(builder -> builder.collectionGroup(group).partitionCount(1).pageSize(7)))
                .containsExactlyInAnyOrderElementsOf(paths);
    }

    @Test
    void readsNothingFromAnEmptyCollectionGroup() throws Exception {
        assertThat(read(builder -> builder.collectionGroup(uniqueCollection()).partitionCount(1)))
                .isEmpty();
    }

    @Test
    void readsEveryDocumentOnceAcrossPartitions() throws Exception {
        String group = uniqueCollection();
        List<String> paths = seed(collectionAtEveryDepth(group, 60));
        List<String> sorted = paths.stream().sorted().collect(Collectors.toList());
        // Boundaries at existing documents and one between two: an inclusive start and an exclusive
        // end must hand each boundary document to exactly one partition, and a boundary naming no
        // document, which a service cursor may, must lose nothing on either side.
        List<String> boundaries = List.of(sorted.get(10), sorted.get(25), sorted.get(40) + "0");
        assertThat(paths).doesNotContain(sorted.get(40) + "0");

        List<String> read =
                read(
                        builder ->
                                TestSources.withPlannerFactory(
                                        builder.collectionGroup(group).pageSize(4),
                                        new FixedPartitionsPlannerFactory(
                                                emulatorEndpoint(), boundaries)),
                        new TestSources.DocumentPathDeserializer(),
                        3);

        assertThat(read).containsExactlyInAnyOrderElementsOf(paths);
    }

    @Test
    void readsAFilteredOrderedQueryWithItsLimitAndOffsetAcrossPages() throws Exception {
        String collection = uniqueCollection();
        List<String> paths = new ArrayList<>();
        for (int n = 0; n < 30; n++) {
            paths.add(collection + "/d" + String.format("%02d", n));
        }
        seed(paths);
        FirestoreQueryFactory factory =
                firestore ->
                        firestore
                                .collection(collection)
                                .whereGreaterThanOrEqualTo("n", 3)
                                .orderBy("k", Query.Direction.DESCENDING)
                                .offset(2)
                                .limit(17);
        List<String> expected =
                factory.create(client()).get().get(30, TimeUnit.SECONDS).getDocuments().stream()
                        .map(document -> document.getReference().getPath())
                        .collect(Collectors.toList());
        assertThat(expected).hasSize(17);

        // One split, read in order: pages of four cross the tie-breaking document name and the
        // limit several times.
        GatedPageReader.reset();
        GatedPageReader.GATE_OPEN.set(true);
        assertThat(
                        read(
                                builder ->
                                        TestSources.withPageReader(
                                                builder.query(factory).pageSize(4),
                                                new GatedPageReader(emulatorEndpoint(), 0))))
                .containsExactlyElementsOf(expected);
        // The offset was resolved into a cursor when the read was planned: the client library
        // retries a broken page from its last document with the offset still applied.
        assertThat(GatedPageReader.STARTED_SPLITS)
                .singleElement()
                .satisfies(
                        split -> {
                            assertThat(split.getStructuredQuery().getOffset()).isZero();
                            assertThat(split.getStructuredQuery().getLimit().getValue())
                                    .isEqualTo(17);
                        });
    }

    @Test
    void readsALimitToLastQueryAndItsOffsetInReverse() throws Exception {
        String collection = uniqueCollection();
        List<String> paths = new ArrayList<>();
        for (int n = 0; n < 10; n++) {
            paths.add(collection + "/d" + n);
        }
        seed(paths);
        FirestoreQueryFactory factory =
                firestore -> firestore.collection(collection).orderBy("n").limitToLast(3).offset(2);
        List<String> expected =
                factory.create(client()).get().get(30, TimeUnit.SECONDS).getDocuments().stream()
                        .map(document -> document.getReference().getPath())
                        .collect(Collectors.toList());
        assertThat(expected).hasSize(3);
        Collections.reverse(expected);

        // The same documents the client library returns, in the reverse of its order: the offset
        // counts from the end, as the service applies it to the reversed query the library sends.
        assertThat(read(builder -> builder.query(factory).pageSize(2)))
                .containsExactlyElementsOf(expected);
    }

    @Test
    void readsAtTheConfiguredReadTimeOnEveryPage() throws Exception {
        String collection = uniqueCollection();
        WriteBatch first = client().batch();
        first.set(client().document(collection + "/a"), Map.of("v", 1));
        first.set(client().document(collection + "/b"), Map.of("v", 1));
        Timestamp before = first.commit().get(30, TimeUnit.SECONDS).get(0).getUpdateTime();
        WriteBatch second = client().batch();
        second.set(client().document(collection + "/a"), Map.of("v", 2));
        second.set(client().document(collection + "/b"), Map.of("v", 2));
        second.set(client().document(collection + "/later"), Map.of("v", 3));
        second.commit().get(30, TimeUnit.SECONDS);

        List<String> read =
                read(
                        builder ->
                                builder.collectionGroup(collection)
                                        .partitionCount(1)
                                        // One document per page: the second page must read at the
                                        // read time as well as the first.
                                        .pageSize(1)
                                        .readTime(
                                                Instant.ofEpochSecond(
                                                        before.getSeconds(), before.getNanos())),
                        new FieldDeserializer("v"),
                        1);

        assertThat(read).containsExactly("a=1", "b=1");
    }

    @Test
    void readsAtAReadTimeFinerThanAMicrosecond() throws Exception {
        String collection = uniqueCollection();
        Timestamp written =
                client().document(collection + "/a")
                        .set(Map.of("v", 1))
                        .get(30, TimeUnit.SECONDS)
                        .getUpdateTime();
        client().document(collection + "/a").set(Map.of("v", 2)).get(30, TimeUnit.SECONDS);
        // The emulator refuses a read time with nanoseconds (the API documents only
        // microseconds), which an Instant.now() can carry; the builder truncates this one to the
        // microsecond of the first write.
        Instant readTime = Instant.ofEpochSecond(written.getSeconds(), written.getNanos() + 999);
        assertThat(readTime.getNano() % 1_000).isNotZero();

        List<String> read =
                read(
                        builder ->
                                builder.collectionGroup(collection)
                                        .partitionCount(1)
                                        .readTime(readTime),
                        new FieldDeserializer("v"),
                        1);

        assertThat(read).containsExactly("a=1");
    }

    @Test
    void readsOnlyTheProjectedFields() throws Exception {
        String group = uniqueCollection();
        client().document(group + "/a")
                .set(Map.of("keep", "x", "drop", "y"))
                .get(30, TimeUnit.SECONDS);

        assertThat(
                        read(
                                builder ->
                                        builder.collectionGroup(group)
                                                .partitionCount(1)
                                                .select("keep"),
                                new FieldNamesDeserializer(),
                                1))
                .containsExactly("[keep]");
    }

    @Test
    void refusesAQueryWhoseProjectionLeavesOutItsOrdering() throws Exception {
        String collection = uniqueCollection();
        seed(List.of(collection + "/a", collection + "/b"));

        assertThatThrownBy(
                        () ->
                                read(
                                        builder ->
                                                builder.query(
                                                        firestore ->
                                                                firestore
                                                                        .collection(collection)
                                                                        .orderBy("n")
                                                                        .select("k"))))
                .hasStackTraceContaining("Failed to plan the Firestore read")
                .hasStackTraceContaining("Add the field to the projection");
    }

    @Test
    void refusesAQueryBuiltForAnotherDatabase() {
        assertThatThrownBy(
                        () ->
                                read(
                                        builder ->
                                                builder.query(
                                                        firestore ->
                                                                otherDatabase()
                                                                        .collection("c")
                                                                        .orderBy(
                                                                                FieldPath
                                                                                        .documentId()))))
                .hasStackTraceContaining("addresses a database other than");
    }

    private static Firestore otherDatabase;

    private static synchronized Firestore otherDatabase() {
        if (otherDatabase == null) {
            otherDatabase =
                    FirestoreOptions.newBuilder()
                            .setProjectId(PROJECT)
                            .setDatabaseId("elsewhere")
                            .setEmulatorHost(emulatorEndpoint())
                            .build()
                            .getService();
        }
        return otherDatabase;
    }

    @AfterAll
    static void closeOtherDatabase() throws Exception {
        if (otherDatabase != null) {
            otherDatabase.close();
        }
    }

    /** Emits {@code id=value} of one field. */
    private static final class FieldDeserializer
            implements FirestoreDocumentDeserializationSchema<String> {

        private static final long serialVersionUID = 1L;

        private final String field;

        private FieldDeserializer(String field) {
            this.field = field;
        }

        @Override
        public void deserialize(DocumentSnapshot document, Collector<String> out) {
            out.collect(document.getId() + "=" + document.get(field));
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return Types.STRING;
        }
    }

    /** Emits the sorted names of the fields a document carries. */
    private static final class FieldNamesDeserializer
            implements FirestoreDocumentDeserializationSchema<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(DocumentSnapshot document, Collector<String> out) {
            out.collect(
                    document.getData().keySet().stream()
                            .sorted()
                            .collect(Collectors.toList())
                            .toString());
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return Types.STRING;
        }
    }
}
