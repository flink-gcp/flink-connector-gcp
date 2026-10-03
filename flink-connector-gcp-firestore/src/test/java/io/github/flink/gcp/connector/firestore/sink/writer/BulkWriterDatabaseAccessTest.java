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

package io.github.flink.gcp.connector.firestore.sink.writer;

import io.github.flink.gcp.connector.firestore.sink.FirestoreDocumentReference;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Holds {@link BulkWriterDatabaseAccess#replaceReferences} to its contract offline: every reference
 * replaced, at every depth and through every kind of container, and nothing copied that holds none.
 * {@code FirestoreWriterITCase} shows the library storing the replaced values as references.
 */
class BulkWriterDatabaseAccessTest {

    private static final FirestoreDocumentReference A = FirestoreDocumentReference.of("c/a");

    @Test
    void everyReferenceIsReplacedAtEveryDepthAndThroughEveryContainer() {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("before", 1L);
        fields.put("top", A);
        fields.put("inMap", Map.of("ref", A));
        fields.put("inList", List.of("x", A));
        fields.put("inMapInList", List.of(Map.of("ref", A)));
        fields.put("inListInList", List.of(List.of(A)));
        fields.put("inListInMap", Map.of("list", List.of(1L, A)));
        fields.put("after", "s");

        Map<String, Object> replaced =
                BulkWriterDatabaseAccess.replaceReferences(fields, path -> "doc:" + path);

        Map<String, Object> expected = new LinkedHashMap<>();
        expected.put("before", 1L);
        expected.put("top", "doc:c/a");
        expected.put("inMap", Map.of("ref", "doc:c/a"));
        expected.put("inList", List.of("x", "doc:c/a"));
        expected.put("inMapInList", List.of(Map.of("ref", "doc:c/a")));
        expected.put("inListInList", List.of(List.of("doc:c/a")));
        expected.put("inListInMap", Map.of("list", List.of(1L, "doc:c/a")));
        expected.put("after", "s");
        assertThat(replaced).containsExactlyEntriesOf(expected);
        assertThat(fields.get("top")).as("the write's own fields are left alone").isSameAs(A);
    }

    @Test
    void fieldsWithoutAReferenceAreNotCopied() {
        Map<String, Object> nested = Map.of("list", List.of(1L, Map.of("k", "v")));
        Map<String, Object> fields = Map.of("a", 1L, "nested", nested, "empty", List.of());

        Map<String, Object> replaced =
                BulkWriterDatabaseAccess.replaceReferences(
                        fields,
                        path -> {
                            throw new AssertionError("no reference to replace: " + path);
                        });

        assertThat(replaced).isSameAs(fields);
    }

    @Test
    void onlyTheContainersOnAReferencesPathAreCopied() {
        Map<String, Object> untouched = Map.of("k", List.of(1L));
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("untouched", untouched);
        fields.put("ref", A);

        Map<String, Object> replaced =
                BulkWriterDatabaseAccess.replaceReferences(fields, path -> "doc:" + path);

        assertThat(replaced).isNotSameAs(fields);
        assertThat(replaced.get("untouched")).isSameAs(untouched);
    }
}
