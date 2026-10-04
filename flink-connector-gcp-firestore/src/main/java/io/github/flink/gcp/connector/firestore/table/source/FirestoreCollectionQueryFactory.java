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

import org.apache.flink.annotation.Internal;

import com.google.cloud.firestore.FieldPath;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.Query;
import io.github.flink.gcp.connector.firestore.source.FirestoreQueryFactory;

import java.util.Arrays;
import java.util.Objects;

/**
 * The query of a {@code firestore} table that scans its one collection: every document directly in
 * it, projected to the fields of the columns read. Each field name is one literal path segment, so
 * a name containing a dot projects that field.
 */
@Internal
final class FirestoreCollectionQueryFactory implements FirestoreQueryFactory {

    private static final long serialVersionUID = 1L;

    private final String collection;
    private final String[] fields;

    /**
     * Creates the factory.
     *
     * @param collection the collection path
     * @param fields the field names to read; an empty array reads none, as for a table or a
     *     projection of only the key and metadata
     */
    FirestoreCollectionQueryFactory(String collection, String[] fields) {
        this.collection = collection;
        this.fields = fields.clone();
    }

    @Override
    public Query create(Firestore firestore) {
        Query query = firestore.collection(collection);
        FieldPath[] paths = new FieldPath[fields.length];
        for (int i = 0; i < fields.length; i++) {
            paths[i] = FieldPath.of(fields[i]);
        }
        // An empty selection reads only each document's name.
        return query.select(paths);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FirestoreCollectionQueryFactory that = (FirestoreCollectionQueryFactory) o;
        return collection.equals(that.collection) && Arrays.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hash(collection) + Arrays.hashCode(fields);
    }
}
