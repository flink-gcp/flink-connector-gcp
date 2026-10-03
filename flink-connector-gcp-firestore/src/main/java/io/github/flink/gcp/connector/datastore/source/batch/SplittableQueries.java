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
     * entities it finds, and it reports tied distances in no stable order across requests.
     *
     * @param query the query
     * @return the reason, phrased to follow "the source cannot read the query because", or {@code
     *     null}
     */
    @Nullable
    public static String whyNotReadable(Query query) {
        Preconditions.checkNotNull(query, "query must not be null");
        return query.hasFindNearest()
                ? "it is a nearest-neighbour search, whose results the source's paging and resume"
                        + " cursors would change"
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
        return query.hasFilter() ? whyNotSplittable(query.getFilter()) : null;
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
