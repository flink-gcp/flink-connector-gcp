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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QueryPartition;
import com.google.firestore.v1.Cursor;
import com.google.firestore.v1.Value;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Turns the cursors of a {@code PartitionQuery} call into the client library's {@link
 * QueryPartition}s, as {@code CollectionGroup.getPartitions(long)} does; the planner pages the call
 * itself, so that a close can cancel each page (#2346, ADR-0208).
 *
 * <p>The cursors are ordered as the library orders them, since the service does not order them
 * across pages: by document name, segment by segment, a numeric id ({@code __id<n>__}) before a
 * string one and in numeric order, and strings in UTF-8 byte order ({@code BasePath.compareTo} over
 * {@code Order.compareUtf8Strings}; a document path is built only through the package-private
 * {@code ResourcePath}, and {@code Order} is package-private).
 */
@Internal
final class QueryPartitions {

    private QueryPartitions() {}

    /**
     * Returns the partitions the cursors cut the query into, each starting where the one before it
     * ends, the first with no start and the last with no end.
     *
     * @param firestore the client the query belongs to, which builds each cursor's document
     * @param partitionQuery the query the call partitioned, ordered by document name
     * @param documentsRoot the query's parent, {@code projects/<p>/databases/<d>/documents}, under
     *     which every cursor's document lies
     * @param cursors the call's cursors, in any order
     * @return the partitions, in document-name order
     * @throws IOException if a cursor is not one document reference under the root
     */
    static List<QueryPartition> partitions(
            Firestore firestore, Query partitionQuery, String documentsRoot, List<Cursor> cursors)
            throws IOException {
        String prefix = documentsRoot + "/";
        List<String> paths = new ArrayList<>(cursors.size());
        for (Cursor cursor : cursors) {
            Value value = cursor.getValuesCount() == 1 ? cursor.getValues(0) : null;
            if (value == null
                    || value.getValueTypeCase() != Value.ValueTypeCase.REFERENCE_VALUE
                    || !value.getReferenceValue().startsWith(prefix)) {
                throw new IOException(
                        "PartitionQuery answered a cursor that is not one document of "
                                + documentsRoot
                                + ": "
                                + cursor);
            }
            paths.add(value.getReferenceValue().substring(prefix.length()));
        }
        List<QueryPartition> partitions = new ArrayList<>(paths.size() + 1);
        Object[] start = null;
        try {
            paths.sort(QueryPartitions::compareNames);
            for (String path : paths) {
                Object[] end = new Object[] {firestore.document(path)};
                partitions.add(new QueryPartition(partitionQuery, start, end));
                start = end;
            }
        } catch (IllegalArgumentException e) {
            // A path that names a collection, or a numeric id that does not parse.
            throw new IOException(
                    "PartitionQuery answered a cursor that is not one document of "
                            + documentsRoot
                            + ".",
                    e);
        }
        partitions.add(new QueryPartition(partitionQuery, start, null));
        return partitions;
    }

    // compareNames, compareSegments, isNumericId, numericId and compareUtf8 are adapted from
    // google-cloud-firestore's BasePath.compareTo and Order.compareUtf8Strings
    // (https://github.com/googleapis/java-firestore, Copyright 2017 Google LLC, Apache License,
    // Version 2.0); see the module README and the repository NOTICE.

    /**
     * Compares two document paths in the order the library sorts partition cursors in.
     *
     * @param left a document path, relative to the database's documents root
     * @param right another
     * @return negative, zero or positive as {@code left} orders before, with or after {@code right}
     */
    @VisibleForTesting
    static int compareNames(String left, String right) {
        String[] leftSegments = left.split("/", -1);
        String[] rightSegments = right.split("/", -1);
        int length = Math.min(leftSegments.length, rightSegments.length);
        for (int i = 0; i < length; i++) {
            int segment = compareSegments(leftSegments[i], rightSegments[i]);
            if (segment != 0) {
                return segment;
            }
        }
        return Integer.compare(leftSegments.length, rightSegments.length);
    }

    private static int compareSegments(String left, String right) {
        boolean leftNumeric = isNumericId(left);
        boolean rightNumeric = isNumericId(right);
        if (leftNumeric != rightNumeric) {
            return leftNumeric ? -1 : 1;
        }
        if (leftNumeric) {
            return Long.compare(numericId(left), numericId(right));
        }
        return compareUtf8(left, right);
    }

    private static boolean isNumericId(String segment) {
        return segment.startsWith("__id") && segment.endsWith("__");
    }

    private static long numericId(String segment) {
        return Long.parseLong(segment.substring(4, segment.length() - 2));
    }

    /**
     * Compares two strings in the order of their UTF-8 bytes: at the first differing UTF-16 unit, a
     * surrogate, which only a code point above U+FFFF takes, orders after a unit that is not one.
     */
    private static int compareUtf8(String left, String right) {
        int length = Math.min(left.length(), right.length());
        for (int i = 0; i < length; i++) {
            char l = left.charAt(i);
            char r = right.charAt(i);
            if (l != r) {
                boolean leftSurrogate = Character.isSurrogate(l);
                if (leftSurrogate == Character.isSurrogate(r)) {
                    return Character.compare(l, r);
                }
                return leftSurrogate ? 1 : -1;
            }
        }
        return Integer.compare(left.length(), right.length());
    }
}
