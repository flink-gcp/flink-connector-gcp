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

import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.Blob;
import com.google.cloud.firestore.FieldPath;
import com.google.cloud.firestore.Filter;
import com.google.cloud.firestore.Firestore;
import io.github.flink.gcp.connector.firestore.TestDocuments;
import io.github.flink.gcp.connector.firestore.common.table.PredicateMatcher.Operator;
import io.github.flink.gcp.connector.firestore.table.source.FirestoreFilterPushDown.FieldFilter;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class FirestoreCollectionQueryFactoryTest {

    @Test
    void selectsEncodedPathsOrOnlyTheDocumentName() throws Exception {
        try (Firestore client = TestDocuments.offlineClient("p")) {
            assertThat(
                            new FirestoreCollectionQueryFactory(
                                            "c",
                                            new String[] {"`a.b`", "x", "r.`p.q`"},
                                            List.of(),
                                            null)
                                    .create(client))
                    .isEqualTo(
                            client.collection("c")
                                    .select(
                                            FieldPath.of("a.b"),
                                            FieldPath.of("x"),
                                            FieldPath.of("r", "p.q")));
            assertThat(
                            new FirestoreCollectionQueryFactory("c", new String[0], List.of(), null)
                                    .create(client))
                    .isEqualTo(client.collection("c").select(FieldPath.documentId()));
        }
    }

    @Test
    void addsEachEqualityOnItsLiteralFieldPathBeforeTheSelection() throws Exception {
        Timestamp time = Timestamp.ofTimeSecondsAndNanos(1, 2_000);
        Blob bytes = Blob.fromBytes(new byte[] {1});
        try (Firestore client = TestDocuments.offlineClient("p")) {
            assertThat(
                            new FirestoreCollectionQueryFactory(
                                            "c",
                                            new String[] {"x"},
                                            List.of(
                                                    FieldFilter.field(
                                                            List.of("a.b"), Operator.EQUAL, "v"),
                                                    FieldFilter.field(
                                                            List.of("r", "a.b"),
                                                            Operator.EQUAL,
                                                            1L),
                                                    FieldFilter.documentId(Operator.EQUAL, "k"),
                                                    FieldFilter.field(
                                                            List.of("n"), Operator.EQUAL, 5L),
                                                    FieldFilter.field(
                                                            List.of("t"), Operator.EQUAL, time),
                                                    FieldFilter.field(
                                                            List.of("bytes"),
                                                            Operator.EQUAL,
                                                            bytes)),
                                            null)
                                    .create(client))
                    .isEqualTo(
                            client.collection("c")
                                    .whereEqualTo(FieldPath.of("a.b"), "v")
                                    .whereEqualTo(FieldPath.of("r", "a.b"), 1L)
                                    // The client library turns an id into the document's name.
                                    .whereEqualTo(FieldPath.documentId(), "k")
                                    .whereEqualTo(FieldPath.of("n"), 5L)
                                    .whereEqualTo(FieldPath.of("t"), time)
                                    .whereEqualTo(FieldPath.of("bytes"), bytes)
                                    .select(FieldPath.of("x")));
        }
    }

    @Test
    void boundsTheQueryByTheLimitAfterTheSelection() throws Exception {
        try (Firestore client = TestDocuments.offlineClient("p")) {
            assertThat(
                            new FirestoreCollectionQueryFactory(
                                            "c", new String[] {"x"}, List.of(), 5)
                                    .create(client))
                    .isEqualTo(client.collection("c").select(FieldPath.of("x")).limit(5));
            // A collection group is read whole up to the limit, with no partition cursors.
            assertThat(
                            FirestoreCollectionQueryFactory.collectionGroup(
                                            "c", new String[] {"x"}, 5)
                                    .create(client))
                    .isEqualTo(client.collectionGroup("c").select(FieldPath.of("x")).limit(5));
            assertThat(
                            FirestoreCollectionQueryFactory.collectionGroup("c", new String[0], 0)
                                    .create(client))
                    .isEqualTo(client.collectionGroup("c").select(FieldPath.documentId()).limit(0));
        }
    }

    @Test
    void selectsTheFieldAnInequalityOrdersByAndAddsADisjunctionAsOneFilter() throws Exception {
        try (Firestore client = TestDocuments.offlineClient("p")) {
            assertThat(
                            new FirestoreCollectionQueryFactory(
                                            "c",
                                            new String[] {"x"},
                                            List.of(
                                                    FieldFilter.field(
                                                            List.of("n"),
                                                            Operator.GREATER_THAN,
                                                            5L),
                                                    FieldFilter.field(
                                                                    List.of("m"),
                                                                    Operator.NOT_EQUAL,
                                                                    1.5d)
                                                            .asRanges(),
                                                    FieldFilter.documentId(Operator.LESS_THAN, "k"),
                                                    FieldFilter.anyOf(
                                                            List.of(
                                                                    FieldFilter.field(
                                                                            List.of("s"),
                                                                            Operator.EQUAL,
                                                                            "a"),
                                                                    FieldFilter.field(
                                                                            List.of("s"),
                                                                            Operator.EQUAL,
                                                                            "b")))),
                                            null)
                                    .create(client)
                                    .toProto())
                    .isEqualTo(
                            client.collection("c")
                                    .whereGreaterThan(FieldPath.of("n"), 5L)
                                    // <> on a double: either side of the value, or NaN.
                                    .where(
                                            Filter.or(
                                                    Filter.lessThan(FieldPath.of("m"), 1.5d),
                                                    Filter.greaterThan(FieldPath.of("m"), 1.5d),
                                                    Filter.equalTo(FieldPath.of("m"), Double.NaN)))
                                    .whereLessThan(FieldPath.documentId(), "k")
                                    .where(
                                            Filter.or(
                                                    Filter.equalTo(FieldPath.of("s"), "a"),
                                                    Filter.equalTo(FieldPath.of("s"), "b")))
                                    // The cursor resuming the query needs n and m, which the
                                    // ranges order by; the document name is always its last value.
                                    .select(FieldPath.of("x"), FieldPath.of("n"), FieldPath.of("m"))
                                    .toProto());
        }
    }

    @Test
    void comparesByCollectionFieldsAndFiltersAndSurvivesJavaSerialization() throws Exception {
        List<FieldFilter> equalities =
                List.of(
                        FieldFilter.field(List.of("d"), Operator.LESS_THAN_OR_EQUAL, 1.5d),
                        FieldFilter.field(List.of("z"), Operator.NOT_EQUAL, "v").asRanges(),
                        FieldFilter.anyOf(
                                List.of(
                                        FieldFilter.field(List.of("n"), Operator.EQUAL, 1L),
                                        FieldFilter.field(List.of("n"), Operator.EQUAL, 2L))),
                        FieldFilter.field(List.of("s"), Operator.EQUAL, "v"),
                        FieldFilter.field(List.of("r", "a"), Operator.EQUAL, 1L),
                        FieldFilter.documentId(Operator.EQUAL, "k"),
                        FieldFilter.field(
                                List.of("b"), Operator.EQUAL, Blob.fromBytes(new byte[] {1})),
                        FieldFilter.field(
                                List.of("t"),
                                Operator.EQUAL,
                                Timestamp.ofTimeSecondsAndNanos(1, 0)));
        FirestoreCollectionQueryFactory factory =
                new FirestoreCollectionQueryFactory("c", new String[] {"a"}, equalities, null);

        assertThat(InstantiationUtil.clone(factory)).isEqualTo(factory).hasSameHashCodeAs(factory);
        assertThat(factory)
                .isNotEqualTo(
                        new FirestoreCollectionQueryFactory(
                                "d", new String[] {"a"}, equalities, null))
                .isNotEqualTo(
                        new FirestoreCollectionQueryFactory(
                                "c", new String[] {"b"}, equalities, null))
                .isNotEqualTo(
                        new FirestoreCollectionQueryFactory(
                                "c", new String[] {"a"}, List.of(), null))
                .isNotEqualTo(
                        new FirestoreCollectionQueryFactory("c", new String[] {"a"}, equalities, 5))
                .isNotEqualTo(
                        FirestoreCollectionQueryFactory.collectionGroup(
                                "c", new String[] {"a"}, 5));
        FirestoreCollectionQueryFactory group =
                FirestoreCollectionQueryFactory.collectionGroup("c", new String[] {"a"}, 5);
        assertThat(InstantiationUtil.clone(group)).isEqualTo(group).hasSameHashCodeAs(group);
        assertThat(group)
                .isNotEqualTo(
                        new FirestoreCollectionQueryFactory("c", new String[] {"a"}, List.of(), 5))
                .isNotEqualTo(
                        FirestoreCollectionQueryFactory.collectionGroup(
                                "c", new String[] {"a"}, 6));
    }

    @Test
    void aNotEqualIsTheRangesEitherSideAndADoubleAlsoTakesNaN() {
        FieldFilter text = FieldFilter.field(List.of("s"), Operator.NOT_EQUAL, "v");
        FieldFilter number = FieldFilter.field(List.of("d"), Operator.NOT_EQUAL, 5.0d);

        assertThat(text.isNotEqual()).isTrue();
        assertThat(text.asRanges())
                .isEqualTo(
                        FieldFilter.anyOf(
                                List.of(
                                        FieldFilter.field(List.of("s"), Operator.LESS_THAN, "v"),
                                        FieldFilter.field(
                                                List.of("s"), Operator.GREATER_THAN, "v"))));
        assertThat(text.asRanges().width()).isEqualTo(2);
        assertThat(number.asRanges().width()).isEqualTo(3);
        assertThat(number.asRanges().orderedPath()).isEqualTo(FieldPath.of("d"));
        assertThat(text.asRanges().isNotEqual()).isFalse();
    }
}
