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

import com.google.cloud.firestore.DocumentSnapshot;
import io.github.flink.gcp.connector.firestore.AbstractFirestoreEmulatorITCase;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Reads through the lookup's own client against the emulator, where the field mask is built. */
@Testcontainers
class FirestoreCollectionDocumentLookupITCase extends AbstractFirestoreEmulatorITCase {

    private static DocumentSnapshot read(FirestoreCollectionDocumentLookup lookup, String id)
            throws Exception {
        return lookup.readAsync(id).get(30, TimeUnit.SECONDS);
    }

    @Test
    void aReadAsksForEachFieldLiterallyAndNoOther() throws Exception {
        String collection = uniqueCollection();
        client().document(collection + "/d")
                .set(
                        Map.of(
                                "a.b", "literal",
                                "a", Map.of("b", "nested"),
                                "other", "unread"))
                .get();
        FirestoreCollectionDocumentLookup lookup =
                new FirestoreCollectionDocumentLookup(
                        database(), collection, new String[] {"a.b"}, emulatorEndpoint(), null);
        lookup.open();
        try {
            DocumentSnapshot snapshot = read(lookup, "d");

            assertThat(snapshot.getData()).containsOnlyKeys("a.b");
            assertThat(snapshot.getData()).containsEntry("a.b", "literal");
        } finally {
            lookup.close();
        }
    }

    @Test
    void aReadOfNoFieldReturnsTheDocumentWithoutFields() throws Exception {
        String collection = uniqueCollection();
        client().document(collection + "/d").set(Map.of("x", 1L)).get();
        FirestoreCollectionDocumentLookup lookup =
                new FirestoreCollectionDocumentLookup(
                        database(), collection, new String[0], emulatorEndpoint(), null);
        lookup.open();
        try {
            DocumentSnapshot snapshot = read(lookup, "d");

            assertThat(snapshot.exists()).isTrue();
            assertThat(snapshot.getData()).isEmpty();
            assertThat(read(lookup, "missing").exists()).isFalse();
        } finally {
            lookup.close();
        }
    }
}
