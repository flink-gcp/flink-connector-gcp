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

import com.google.cloud.firestore.FieldPath;
import com.google.cloud.firestore.Firestore;
import io.github.flink.gcp.connector.firestore.TestDocuments;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FirestoreCollectionQueryFactoryTest {

    @Test
    void selectsLiteralNamesOrOnlyTheDocumentName() throws Exception {
        try (Firestore client = TestDocuments.offlineClient("p")) {
            assertThat(
                            new FirestoreCollectionQueryFactory("c", new String[] {"a.b", "x"})
                                    .create(client))
                    .isEqualTo(
                            client.collection("c").select(FieldPath.of("a.b"), FieldPath.of("x")));
            assertThat(new FirestoreCollectionQueryFactory("c", new String[0]).create(client))
                    .isEqualTo(client.collection("c").select(FieldPath.documentId()));
        }
    }

    @Test
    void comparesByCollectionAndFieldsAndSurvivesJavaSerialization() throws Exception {
        FirestoreCollectionQueryFactory factory =
                new FirestoreCollectionQueryFactory("c", new String[] {"a"});

        assertThat(InstantiationUtil.clone(factory)).isEqualTo(factory).hasSameHashCodeAs(factory);
        assertThat(factory)
                .isNotEqualTo(new FirestoreCollectionQueryFactory("d", new String[] {"a"}))
                .isNotEqualTo(new FirestoreCollectionQueryFactory("c", new String[] {"b"}));
    }
}
