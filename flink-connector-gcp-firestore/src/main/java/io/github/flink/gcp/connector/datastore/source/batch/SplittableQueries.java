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

package io.github.flink.gcp.connector.datastore.source.batch;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.datastore.v1.CompositeFilter;
import com.google.datastore.v1.Filter;
import com.google.datastore.v1.KindExpression;
import com.google.datastore.v1.PropertyFilter;
import com.google.datastore.v1.Query;

import javax.annotation.Nullable;

import java.util.Set;

/**
 * Decides whether a query can be cut into key ranges, and builds the query a kind scan reads.
 *
 * <p>A split adds {@code __key__ >= a} and {@code __key__ < b} to the query, so the query must keep
 * every one of its results when the key range is ANDed on, and must not need another inequality or
 * ordering to do it. The client library's {@code QuerySplitter} refuses only some of what breaks
 * that: an ordering, an inequality filter, more or fewer than one kind, a filter of no type. It
 * accepts a {@code limit}, an {@code offset} and cursors, which all describe positions in the whole
 * result rather than in a key range, and the {@code !=}, {@code NOT_IN}, {@code IN} and {@code OR}
 * filters, which put a second inequality or a disjunction beside the key range. This class refuses
 * all of them, so a query is split only when an equality filter, an ancestor filter, or none is all
 * it has ({@code docs/adr/0177}). Everything else is read whole, as one split.
 */
@Internal
public final class SplittableQueries {

    /** The metadata kinds, refused for the reason {@link #whyKindNotReadable(String)} gives. */
    private static final Set<String> METADATA_KINDS =
            Set.of("__namespace__", "__kind__", "__property__");

    private SplittableQueries() {}

    /**
     * Returns the query a kind scan reads: every entity of the kind.
     *
     * @param kind the kind
     * @return the query
     */
    public static Query ofKind(String kind) {
        Preconditions.checkNotNull(kind, "kind must not be null");
        return Query.newBuilder().addKind(KindExpression.newBuilder().setName(kind)).build();
    }

    /**
     * Returns the one kind a query reads, or {@code null} when it names none or several.
     *
     * @param query the query
     * @return the kind, or {@code null}
     */
    @Nullable
    public static String kindOf(Query query) {
        return query.getKindCount() == 1 ? query.getKind(0).getName() : null;
    }

    /**
     * Returns why the source cannot read a query at all, or {@code null} when it can.
     *
     * <p>A nearest-neighbour search is refused: the service applies a query's cursor and limit
     * before the search, so the page limit and the resume cursor every read sets would change which
     * entities it finds, and it reports tied distances in no stable order across requests. A query
     * of a metadata kind is refused for the reason {@link #whyKindNotReadable(String)} gives.
     *
     * @param query the query
     * @return the reason, phrased to follow "the source cannot read the query because", or {@code
     *     null}
     */
    @Nullable
    public static String whyNotReadable(Query query) {
        Preconditions.checkNotNull(query, "query must not be null");
        if (query.hasFindNearest()) {
            return "it is a nearest-neighbour search, whose results the source's paging and resume"
                    + " cursors would change";
        }
        for (KindExpression kind : query.getKindList()) {
            String reason = whyKindNotReadable(kind.getName());
            if (reason != null) {
                return reason;
            }
        }
        return null;
    }

    /**
     * Returns why the source cannot read a kind, or {@code null} when it can.
     *
     * <p>The metadata kinds {@code __namespace__}, {@code __kind__} and {@code __property__} are
     * refused. Google's documentation says their entities are generated dynamically, based on the
     * database's current state, so a result need not be a snapshot at the read time that a
     * restarted read could page through or replay. On the emulator, a query of {@code __kind__} or
     * {@code __namespace__} answers without the per-entity and end cursors the source pages and
     * resumes by, and one of {@code __property__} returned nothing; the service carries both
     * cursors for all three (measured 2026-10-11), and whether the refusal should stand is #1717.
     * The statistics kinds, such as {@code __Stat_Kind__}, are not refused.
     *
     * @param kind the kind
     * @return the reason, phrased to follow "the source cannot read the kind because", or {@code
     *     null}
     */
    @Nullable
    public static String whyKindNotReadable(String kind) {
        Preconditions.checkNotNull(kind, "kind must not be null");
        return METADATA_KINDS.contains(kind)
                ? kind
                        + " is a metadata kind, which the source can neither page through nor"
                        + " resume after a failure; read metadata with the client library instead"
                : null;
    }

    /**
     * Returns why a query cannot be cut into key ranges, or {@code null} when it can.
     *
     * @param query the query
     * @return the reason, phrased to follow "the query is read as one split because", or {@code
     *     null} when the query can be split
     */
    @Nullable
    public static String whyNotSplittable(Query query) {
        Preconditions.checkNotNull(query, "query must not be null");
        if (query.getKindCount() != 1) {
            return "it does not name exactly one kind";
        }
        if (query.getOrderCount() > 0) {
            return "it orders its results";
        }
        if (query.hasLimit()) {
            return "it has a limit";
        }
        if (query.getOffset() != 0) {
            return "it has an offset";
        }
        if (!query.getStartCursor().isEmpty() || !query.getEndCursor().isEmpty()) {
            return "it starts or ends at a cursor";
        }
        if (query.getDistinctOnCount() > 0) {
            return "it is a distinct query";
        }
        if (query.getProjectionCount() > 0 && !isKeysOnly(query)) {
            // Measured 2026-10-11: the service refuses a projection of a property with a key
            // range ANDed on, asking for a composite index of __key__ and the property, although
            // the projection alone is served by the built-in indexes.
            return "it projects properties, and a key range ANDed onto such a projection needs a"
                    + " composite index the query alone does not";
        }
        return query.hasFilter() ? whyNotSplittable(query.getFilter()) : null;
    }

    private static boolean isKeysOnly(Query query) {
        return query.getProjectionCount() == 1
                && "__key__".equals(query.getProjection(0).getProperty().getName());
    }

    @Nullable
    private static String whyNotSplittable(Filter filter) {
        switch (filter.getFilterTypeCase()) {
            case COMPOSITE_FILTER:
                CompositeFilter composite = filter.getCompositeFilter();
                if (composite.getOp() != CompositeFilter.Operator.AND) {
                    return "it combines filters with " + composite.getOp();
                }
                for (Filter nested : composite.getFiltersList()) {
                    String reason = whyNotSplittable(nested);
                    if (reason != null) {
                        return reason;
                    }
                }
                return null;
            case PROPERTY_FILTER:
                PropertyFilter.Operator op = filter.getPropertyFilter().getOp();
                if (op == PropertyFilter.Operator.EQUAL
                        || op == PropertyFilter.Operator.HAS_ANCESTOR) {
                    return null;
                }
                return "it filters with the " + op + " operator";
            case FILTERTYPE_NOT_SET:
                return "it has a filter of no type";
            default:
                return "it has a filter of type " + filter.getFilterTypeCase();
        }
    }
}
