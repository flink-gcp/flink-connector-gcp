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
import io.github.flink.gcp.connector.firestore.table.source.FirestoreFilterPushDown.FieldEquality;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * The query of a {@code firestore} table that scans its one collection: every document directly in
 * it that the pushed equality filters admit, up to the pushed limit, projected to the fields of the
 * columns read. Each field name is one literal path segment, so a name containing a dot projects,
 * or filters on, that field.
 *
 * <p>A collection-group table's scan is this query too when the planner pushed a limit: the group,
 * unfiltered, up to the limit, read as one split instead of as partitions ({@code docs/adr/0201}).
 */
@Internal
final class FirestoreCollectionQueryFactory implements FirestoreQueryFactory {

    private static final long serialVersionUID = 1L;

    /** The collection path, or the collection id of a collection-group scan. */
    private final String collection;

    private final boolean collectionGroup;
    private final String[] fields;
    private final List<FieldEquality> equalities;
    @Nullable private final Integer limit;

    /**
     * Creates the factory.
     *
     * @param collection the collection path
     * @param fields the field paths to read, each in the client library's encoded form ({@code
     *     FieldPath.toString()}); an empty array reads none, as for a table or a projection of only
     *     the key and metadata
     * @param equalities the pushed equality filters, each a necessary condition Flink evaluates
     *     again. An equality adds no ordering, so the query stays in document-name order and its
     *     field need not be in the projection for a cursor to resume it
     * @param limit the pushed limit, or {@code null} for none. A limit beside an equality would
     *     stop the read before a document Flink's residual keeps, so the caller passes no limit
     *     with equalities
     */
    FirestoreCollectionQueryFactory(
            String collection,
            String[] fields,
            List<FieldEquality> equalities,
            @Nullable Integer limit) {
        this(collection, false, fields, equalities, limit);
    }

    private FirestoreCollectionQueryFactory(
            String collection,
            boolean collectionGroup,
            String[] fields,
            List<FieldEquality> equalities,
            @Nullable Integer limit) {
        this.collection = collection;
        this.collectionGroup = collectionGroup;
        this.fields = fields.clone();
        this.equalities = List.copyOf(equalities);
        this.limit = limit;
    }

    /**
     * Creates the factory of a limited collection-group scan: every document of every collection
     * with the id, up to the limit.
     *
     * @param collectionId the collection id
     * @param fields the field paths to read, as for the constructor
     * @param limit the pushed limit
     * @return the factory
     */
    static FirestoreCollectionQueryFactory collectionGroup(
            String collectionId, String[] fields, int limit) {
        return new FirestoreCollectionQueryFactory(collectionId, true, fields, List.of(), limit);
    }

    @Override
    public Query create(Firestore firestore) {
        Query query =
                collectionGroup
                        ? firestore.collectionGroup(collection)
                        : firestore.collection(collection);
        for (FieldEquality equality : equalities) {
            query = query.whereEqualTo(equality.path(), equality.value());
        }
        FieldPath[] paths = new FieldPath[fields.length];
        for (int i = 0; i < fields.length; i++) {
            paths[i] = FieldPath.fromServerFormat(fields[i]);
        }
        // An empty selection reads only each document's name.
        query = query.select(paths);
        return limit == null ? query : query.limit(limit);
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
        return collection.equals(that.collection)
                && collectionGroup == that.collectionGroup
                && Arrays.equals(fields, that.fields)
                && equalities.equals(that.equalities)
                && Objects.equals(limit, that.limit);
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hash(collection, collectionGroup, equalities, limit)
                + Arrays.hashCode(fields);
    }
}
