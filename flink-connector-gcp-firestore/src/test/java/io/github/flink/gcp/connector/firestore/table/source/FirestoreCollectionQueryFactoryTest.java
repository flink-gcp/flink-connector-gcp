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
import com.google.cloud.firestore.Firestore;
import io.github.flink.gcp.connector.firestore.TestDocuments;
import io.github.flink.gcp.connector.firestore.table.source.FirestoreFilterPushDown.FieldEquality;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class FirestoreCollectionQueryFactoryTest {

    @Test
    void selectsEncodedPathsOrOnlyTheDocumentName() throws Exception {
        try (Firestore client = TestDocuments.offlineClient("p")) {
            assertThat(
                            new FirestoreCollectionQueryFactory(
                                            "c", new String[] {"`a.b`", "x", "r.`p.q`"}, List.of())
                                    .create(client))
                    .isEqualTo(
                            client.collection("c")
                                    .select(
                                            FieldPath.of("a.b"),
                                            FieldPath.of("x"),
                                            FieldPath.of("r", "p.q")));
            assertThat(
                            new FirestoreCollectionQueryFactory("c", new String[0], List.of())
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
                                                    FieldEquality.field(List.of("a.b"), "v"),
                                                    FieldEquality.field(List.of("r", "a.b"), 1L),
                                                    FieldEquality.documentId("k"),
                                                    FieldEquality.field(List.of("n"), 5L),
                                                    FieldEquality.field(List.of("t"), time),
                                                    FieldEquality.field(List.of("bytes"), bytes)))
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
    void comparesByCollectionFieldsAndEqualitiesAndSurvivesJavaSerialization() throws Exception {
        List<FieldEquality> equalities =
                List.of(
                        FieldEquality.field(List.of("s"), "v"),
                        FieldEquality.field(List.of("r", "a"), 1L),
                        FieldEquality.documentId("k"),
                        FieldEquality.field(List.of("b"), Blob.fromBytes(new byte[] {1})),
                        FieldEquality.field(List.of("t"), Timestamp.ofTimeSecondsAndNanos(1, 0)));
        FirestoreCollectionQueryFactory factory =
                new FirestoreCollectionQueryFactory("c", new String[] {"a"}, equalities);

        assertThat(InstantiationUtil.clone(factory)).isEqualTo(factory).hasSameHashCodeAs(factory);
        assertThat(factory)
                .isNotEqualTo(
                        new FirestoreCollectionQueryFactory("d", new String[] {"a"}, equalities))
                .isNotEqualTo(
                        new FirestoreCollectionQueryFactory("c", new String[] {"b"}, equalities))
                .isNotEqualTo(
                        new FirestoreCollectionQueryFactory("c", new String[] {"a"}, List.of()));
    }
}
