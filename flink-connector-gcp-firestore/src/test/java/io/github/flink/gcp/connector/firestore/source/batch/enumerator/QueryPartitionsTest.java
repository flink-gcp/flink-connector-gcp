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

import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.FieldPath;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QueryPartition;
import com.google.firestore.v1.Cursor;
import com.google.firestore.v1.Value;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests {@link QueryPartitions}: the cursors' order and the partitions (#2346). */
class QueryPartitionsTest {

    private static final String ROOT = "projects/p/databases/(default)/documents";

    private static Firestore firestore;

    /**
     * A client that never dials: building documents and queries makes no call. Its credentials are
     * given, so the build does not look for Application Default Credentials.
     */
    @BeforeAll
    static void openClient() {
        firestore =
                FirestoreOptions.newBuilder()
                        .setProjectId("p")
                        .setCredentials(new FirestoreOptions.EmulatorCredentials())
                        .setEmulatorHost("127.0.0.1:1")
                        .build()
                        .getService();
    }

    @AfterAll
    static void closeClient() throws Exception {
        firestore.close();
    }

    @Test
    void theCursorsCutTheQueryInDocumentNameOrder() throws IOException {
        Query query = firestore.collectionGroup("c").orderBy(FieldPath.documentId());

        List<QueryPartition> partitions =
                QueryPartitions.partitions(
                        firestore, query, ROOT, cursors("p/2/c/x", "c/b", "c/a", "p/10/c/x"));

        assertThat(partitions)
                .extracting(partition -> bound(partition.getStartAt()))
                .containsExactly(null, "c/a", "c/b", "p/10/c/x", "p/2/c/x");
        assertThat(partitions)
                .extracting(partition -> bound(partition.getEndBefore()))
                .containsExactly("c/a", "c/b", "p/10/c/x", "p/2/c/x", null);
        assertThat(partitions)
                .allSatisfy(
                        partition ->
                                assertThat(
                                                partition
                                                        .createQuery()
                                                        .toProto()
                                                        .getStructuredQuery()
                                                        .getFrom(0)
                                                        .getCollectionId())
                                        .isEqualTo("c"));
    }

    @Test
    void noCursorIsOnePartitionOfTheWholeQuery() throws IOException {
        Query query = firestore.collectionGroup("c");

        assertThat(QueryPartitions.partitions(firestore, query, ROOT, List.of()))
                .singleElement()
                .satisfies(
                        partition -> {
                            assertThat(partition.getStartAt()).isNull();
                            assertThat(partition.getEndBefore()).isNull();
                        });
    }

    @Test
    void refusesACursorThatIsNotOneDocumentOfTheDatabase() {
        Query query = firestore.collectionGroup("c");
        Cursor otherDatabase =
                Cursor.newBuilder()
                        .addValues(
                                Value.newBuilder()
                                        .setReferenceValue(
                                                "projects/p/databases/other/documents/c/a"))
                        .build();
        Cursor notAReference =
                Cursor.newBuilder().addValues(Value.newBuilder().setStringValue("c/a")).build();
        Cursor twoValues =
                cursors("c/a", "c/b").get(0).toBuilder()
                        .addValues(Value.newBuilder().setReferenceValue(ROOT + "/c/b"))
                        .build();

        List<List<Cursor>> answers =
                List.of(
                        List.of(otherDatabase),
                        List.of(notAReference),
                        List.of(twoValues),
                        // A collection, not a document.
                        cursors("c"),
                        // A numeric id that does not parse, met when the cursors are sorted.
                        cursors("c/__idx__", "c/__id1__"));
        for (List<Cursor> answer : answers) {
            assertThatThrownBy(() -> QueryPartitions.partitions(firestore, query, ROOT, answer))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("is not one document of " + ROOT);
        }
    }

    /**
     * The library's order of a numeric id segment ({@code BasePath.compareSegments},
     * google-cloud-firestore 3.53.0): before every string segment, and in numeric order.
     */
    @Test
    void ordersANumericIdBeforeAStringAndByItsNumber() {
        List<String> names =
                new ArrayList<>(List.of("c/b", "c/__id10__", "c/a", "c/__id9__", "c/__id-1__"));

        names.sort(QueryPartitions::compareNames);

        assertThat(names).containsExactly("c/__id-1__", "c/__id9__", "c/__id10__", "c/a", "c/b");
    }

    /**
     * String segments order as their UTF-8 bytes, which the service orders document names by and
     * the library's {@code Order.compareUtf8Strings} reproduces; a shorter name that is a prefix of
     * a longer one, segment by segment, comes first. Checked against that byte order directly, over
     * random names from an alphabet that straddles the surrogate range.
     */
    @Test
    void ordersStringSegmentsByTheirUtf8Bytes() {
        // Code points below the slash too, where whole-name order and segment order part.
        int[] points = {
            '/', 0x01, ' ', '-', 'a', 'z', 0x7F, 0x80, 0x7FF, 0x800, 0xD7FF, 0xE000, 0xFFFD,
            0x10000, 0x1F600, 0x10FFFF
        };
        Random random = new Random(7);
        for (int i = 0; i < 20_000; i++) {
            String left = name(random, points);
            String right = name(random, points);
            assertThat(Integer.signum(QueryPartitions.compareNames(left, right)))
                    .as("%s vs %s", left, right)
                    .isEqualTo(Integer.signum(utf8Order(left, right)));
        }
    }

    /** A name of one to three segments, each of one to four code points other than a slash. */
    private static String name(Random random, int[] points) {
        StringBuilder name = new StringBuilder();
        int segments = 1 + random.nextInt(3);
        for (int s = 0; s < segments; s++) {
            if (s > 0) {
                name.append('/');
            }
            int length = 1 + random.nextInt(4);
            for (int c = 0; c < length; c++) {
                // Index 0 is the slash, which only separates segments.
                name.appendCodePoint(points[1 + random.nextInt(points.length - 1)]);
            }
        }
        return name.toString();
    }

    /** Segment by segment, each segment by its unsigned UTF-8 bytes, then by segment count. */
    private static int utf8Order(String left, String right) {
        String[] l = left.split("/");
        String[] r = right.split("/");
        for (int i = 0; i < Math.min(l.length, r.length); i++) {
            int segment =
                    Arrays.compareUnsigned(
                            l[i].getBytes(StandardCharsets.UTF_8),
                            r[i].getBytes(StandardCharsets.UTF_8));
            if (segment != 0) {
                return segment;
            }
        }
        return Integer.compare(l.length, r.length);
    }

    private static List<Cursor> cursors(String... documents) {
        List<Cursor> cursors = new ArrayList<>();
        for (String document : documents) {
            cursors.add(
                    Cursor.newBuilder()
                            .addValues(Value.newBuilder().setReferenceValue(ROOT + "/" + document))
                            .build());
        }
        return cursors;
    }

    private static String bound(Object[] cursor) {
        return cursor == null ? null : ((DocumentReference) cursor[0]).getPath();
    }
}
