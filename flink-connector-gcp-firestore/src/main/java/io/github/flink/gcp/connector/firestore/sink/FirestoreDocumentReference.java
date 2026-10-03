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

package io.github.flink.gcp.connector.firestore.sink;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

/**
 * A field value that refers to a document, written as a Firestore reference value.
 *
 * <p>This stands in for the client library's {@code DocumentReference}, which a {@link
 * FirestoreWrite} cannot carry: that type belongs to one client instance and is not serializable.
 * The sink turns this value into a {@code DocumentReference} of its own client when it sends the
 * write, so the referenced document is always in the database the sink writes to. A reference into
 * another database cannot be expressed.
 *
 * <p>The path has the grammar of {@link FirestoreWrite}'s document path: relative to the database's
 * documents root, an even number of {@code /}-separated segments, none of them empty. The sink does
 * not check that the referenced document exists.
 *
 * <p>Reading the field back, {@code FirestoreSource} hands a deserializer the client library's
 * {@code DocumentReference}, whose {@code getPath()} is the path given here.
 *
 * <p>Instances are immutable and serializable.
 */
@PublicEvolving
public final class FirestoreDocumentReference implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String documentPath;

    private FirestoreDocumentReference(String documentPath) {
        this.documentPath = documentPath;
    }

    /**
     * Returns a reference to the document at {@code documentPath}.
     *
     * @param documentPath the document path relative to the database's documents root, for example
     *     {@code users/alice}
     * @return the reference
     * @throws IllegalArgumentException if the path is malformed
     */
    public static FirestoreDocumentReference of(String documentPath) {
        FirestoreWriteChecks.checkDocumentPath(documentPath);
        return new FirestoreDocumentReference(documentPath);
    }

    /** Returns the referenced document's path relative to the database's documents root. */
    public String getDocumentPath() {
        return documentPath;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return documentPath.equals(((FirestoreDocumentReference) o).documentPath);
    }

    @Override
    public int hashCode() {
        return documentPath.hashCode();
    }

    @Override
    public String toString() {
        return "FirestoreDocumentReference{" + documentPath + "}";
    }
}
