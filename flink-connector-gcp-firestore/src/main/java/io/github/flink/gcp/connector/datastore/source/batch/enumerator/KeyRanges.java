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

package io.github.flink.gcp.connector.datastore.source.batch.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import com.google.datastore.v1.Filter;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.PropertyFilter;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.client.DatastoreHelper;
import com.google.protobuf.ByteString;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

/**
 * Lays the client library splitter's key ranges out again in the order the service sorts keys.
 *
 * <p>{@code QuerySplitter} picks its range boundaries from sampled keys sorted by its own key
 * comparator, which compares kinds and names as Java strings, by UTF-16 code unit. The service
 * sorts them by their UTF-8 bytes. The two orders differ when one name holds a character above
 * U+FFFF and another, at the same position, one from U+E000 to U+FFFF (measured against the
 * emulator, 2026-10-03: {@code "a！"} after {@code "a😀"} for Java, before it for the emulator). Two
 * boundaries in the wrong order make a range the service reads as empty and two ranges that
 * overlap, so the entities between them would be read twice ({@code docs/adr/0177}).
 *
 * <p>This takes every {@code __key__} bound the splitter put on its ranges, sorts the distinct ones
 * in the service's order, and builds the ranges again in the splitter's own shape: the query's
 * filter ANDed with {@code __key__ >= start} and {@code __key__ < end}. Ranges built so tile the
 * key space in the service's order, whatever order the boundaries arrived in.
 */
@Internal
final class KeyRanges {

    /** The pseudo-property a key filter names. */
    private static final String KEY_PROPERTY = "__key__";

    /** Compares two strings as the service does: by their UTF-8 bytes, unsigned. */
    private static final Comparator<ByteString> UTF8 =
            ByteString.unsignedLexicographicalComparator();

    /**
     * Orders keys of one partition as the service does: path element by path element, by kind, then
     * a numeric id before any name, ids by value and names by UTF-8 bytes; a key whose path is a
     * prefix of another's comes first.
     */
    @VisibleForTesting static final Comparator<Key> SERVICE_ORDER = KeyRanges::compare;

    private KeyRanges() {}

    /**
     * Rebuilds the splitter's ranges in the service's key order.
     *
     * @param query the query that was split
     * @param splits the splitter's ranges, in the order it returned them
     * @return the ranges, in the service's key order, tiling the key space; the splitter's answer
     *     unchanged when it put no key bound on it
     */
    static List<Query> retile(Query query, List<Query> splits) {
        TreeSet<Key> boundaries = new TreeSet<>(SERVICE_ORDER);
        for (Query split : splits) {
            collectKeyBounds(split.getFilter(), boundaries);
        }
        if (boundaries.isEmpty()) {
            return splits;
        }
        List<Query> ranges = new ArrayList<>();
        Key start = null;
        for (Key end : boundaries) {
            ranges.add(range(query, start, end));
            start = end;
        }
        ranges.add(range(query, start, null));
        return ranges;
    }

    private static void collectKeyBounds(Filter filter, TreeSet<Key> boundaries) {
        if (filter.hasCompositeFilter()) {
            for (Filter nested : filter.getCompositeFilter().getFiltersList()) {
                collectKeyBounds(nested, boundaries);
            }
        } else if (filter.hasPropertyFilter()) {
            PropertyFilter property = filter.getPropertyFilter();
            PropertyFilter.Operator op = property.getOp();
            if (property.getProperty().getName().equals(KEY_PROPERTY)
                    && (op == PropertyFilter.Operator.GREATER_THAN_OR_EQUAL
                            || op == PropertyFilter.Operator.LESS_THAN)) {
                boundaries.add(property.getValue().getKeyValue());
            }
        }
    }

    private static Query range(Query query, @Nullable Key start, @Nullable Key end) {
        List<Filter> filters = new ArrayList<>();
        if (query.hasFilter()) {
            filters.add(query.getFilter());
        }
        if (start != null) {
            filters.add(
                    DatastoreHelper.makeFilter(
                                    KEY_PROPERTY,
                                    PropertyFilter.Operator.GREATER_THAN_OR_EQUAL,
                                    DatastoreHelper.makeValue(start))
                            .build());
        }
        if (end != null) {
            filters.add(
                    DatastoreHelper.makeFilter(
                                    KEY_PROPERTY,
                                    PropertyFilter.Operator.LESS_THAN,
                                    DatastoreHelper.makeValue(end))
                            .build());
        }
        return query.toBuilder().setFilter(DatastoreHelper.makeAndFilter(filters)).build();
    }

    private static int compare(Key a, Key b) {
        int shared = Math.min(a.getPathCount(), b.getPathCount());
        for (int i = 0; i < shared; i++) {
            int result = compare(a.getPath(i), b.getPath(i));
            if (result != 0) {
                return result;
            }
        }
        return Integer.compare(a.getPathCount(), b.getPathCount());
    }

    private static int compare(Key.PathElement a, Key.PathElement b) {
        int result =
                UTF8.compare(
                        ByteString.copyFromUtf8(a.getKind()), ByteString.copyFromUtf8(b.getKind()));
        if (result != 0) {
            return result;
        }
        boolean aId = a.getIdTypeCase() == Key.PathElement.IdTypeCase.ID;
        boolean bId = b.getIdTypeCase() == Key.PathElement.IdTypeCase.ID;
        if (aId != bId) {
            return aId ? -1 : 1;
        }
        if (aId) {
            return Long.compare(a.getId(), b.getId());
        }
        return UTF8.compare(
                ByteString.copyFromUtf8(a.getName()), ByteString.copyFromUtf8(b.getName()));
    }
}
