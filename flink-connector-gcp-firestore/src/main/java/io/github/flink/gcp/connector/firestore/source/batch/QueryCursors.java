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

package io.github.flink.gcp.connector.firestore.source.batch;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.FieldPath;
import com.google.cloud.firestore.Query;
import com.google.firestore.v1.RunQueryRequest;
import com.google.firestore.v1.StructuredQuery;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Continues a query after a document, the one operation both the split reader's paging and a
 * checkpoint's resume point are made of.
 *
 * <p>The cursor is the client library's {@code startAfter(DocumentSnapshot)}, never hand-built: it
 * first makes the query's implicit ordering explicit — inequality-filtered fields, then the
 * document name, unless the query orders by it explicitly — and takes the cursor values from the
 * document in that order. The document name is always among the orderings, so the cursor names
 * exactly one position and a document is neither repeated nor skipped, whatever the query's own
 * ordering ({@code docs/adr/0173}).
 *
 * <p>A cursor continues the query in that ordering, so the query must be read in it from its first
 * page on. The service does not guarantee that for a query that leaves the ordering implicit: an
 * Enterprise-edition database answers one without {@code orderBy} in no particular order, and a
 * continuation would then skip every document ordered before the last one passed (#2088). {@link
 * #withCursorOrder(RunQueryRequest)} writes the ordering into the query before anything reads it.
 *
 * <p>Continuing also rewrites the two clauses that count documents rather than name them: the
 * {@code offset} has been consumed once a document has been passed, and the {@code limit} shrinks
 * by the documents passed.
 */
@Internal
public final class QueryCursors {

    /** The client library's message for a snapshot that lacks an ordered field. */
    private static final String MISSING_FIELD = "is missing in the provided DocumentSnapshot";

    private static final String DOCUMENT_NAME = FieldPath.documentId().toString();

    private QueryCursors() {}

    /**
     * Returns the query with the ordering a cursor continues it in written out explicitly.
     *
     * <p>The ordering is the one the client library's {@code startAfter(DocumentSnapshot)} derives:
     * the query's own {@code orderBy}, then each inequality-filtered field it does not order by, in
     * field-path order, then the document name, the added fields in the direction of the last
     * explicit ordering, ascending when there is none. One deliberate difference: {@code != null}
     * and {@code != NaN} count as inequality filters here, as the service counts them, while the
     * library 3.50.0 leaves their fields out and would continue such a query in another order than
     * its first page. Once the ordering is explicit, the library's cursor takes it unchanged.
     *
     * <p>Making the ordering explicit changes nothing the service answers where it already honours
     * the implicit one, the Standard edition, and needs no other index: it is the ordering the
     * service applies there anyway.
     *
     * @param request the query to read
     * @return the query with that ordering explicit; {@code request} itself when it already was
     */
    public static RunQueryRequest withCursorOrder(RunQueryRequest request) {
        Preconditions.checkNotNull(request, "request must not be null");
        StructuredQuery structured = request.getStructuredQuery();
        List<StructuredQuery.Order> orders = new ArrayList<>(structured.getOrderByList());
        Set<String> ordered = new HashSet<>();
        for (StructuredQuery.Order order : orders) {
            ordered.add(order.getField().getFieldPath());
        }
        StructuredQuery.Direction direction =
                orders.isEmpty()
                        ? StructuredQuery.Direction.ASCENDING
                        : orders.get(orders.size() - 1).getDirection();
        SortedSet<FieldPath> inequalityFields = new TreeSet<>();
        if (structured.hasWhere()) {
            collectInequalityFields(structured.getWhere(), inequalityFields);
        }
        for (FieldPath field : inequalityFields) {
            String path = field.toString();
            if (!path.equals(DOCUMENT_NAME) && ordered.add(path)) {
                orders.add(order(path, direction));
            }
        }
        if (ordered.add(DOCUMENT_NAME)) {
            orders.add(order(DOCUMENT_NAME, direction));
        }
        if (orders.size() == structured.getOrderByCount()) {
            return request;
        }
        return request.toBuilder()
                .setStructuredQuery(structured.toBuilder().clearOrderBy().addAllOrderBy(orders))
                .build();
    }

    private static void collectInequalityFields(
            StructuredQuery.Filter filter, SortedSet<FieldPath> fields) {
        switch (filter.getFilterTypeCase()) {
            case COMPOSITE_FILTER:
                for (StructuredQuery.Filter child : filter.getCompositeFilter().getFiltersList()) {
                    collectInequalityFields(child, fields);
                }
                break;
            case FIELD_FILTER:
                if (isInequality(filter.getFieldFilter().getOp())) {
                    fields.add(
                            FieldPath.fromServerFormat(
                                    filter.getFieldFilter().getField().getFieldPath()));
                }
                break;
            case UNARY_FILTER:
                StructuredQuery.UnaryFilter.Operator op = filter.getUnaryFilter().getOp();
                if (op == StructuredQuery.UnaryFilter.Operator.IS_NOT_NULL
                        || op == StructuredQuery.UnaryFilter.Operator.IS_NOT_NAN) {
                    fields.add(
                            FieldPath.fromServerFormat(
                                    filter.getUnaryFilter().getField().getFieldPath()));
                }
                break;
            default:
                break;
        }
    }

    private static boolean isInequality(StructuredQuery.FieldFilter.Operator op) {
        switch (op) {
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case NOT_EQUAL:
            case NOT_IN:
                return true;
            default:
                return false;
        }
    }

    private static StructuredQuery.Order order(
            String fieldPath, StructuredQuery.Direction direction) {
        return StructuredQuery.Order.newBuilder()
                .setField(StructuredQuery.FieldReference.newBuilder().setFieldPath(fieldPath))
                .setDirection(direction)
                .build();
    }

    /**
     * Returns the query that reads what is left after a document.
     *
     * @param base the query as the split holds it
     * @param last the last document passed, or {@code null} when none has been
     * @param passed how many documents of {@code base} have been passed, including {@code last}
     * @return the continued query
     * @throws IllegalArgumentException if the query orders or filters by a field its projection
     *     omits, so no cursor can be taken from its documents; a stored value of an ordered field
     *     the client library cannot decode fails with the library's own exception, unchanged
     */
    public static Query continueAfter(Query base, @Nullable DocumentSnapshot last, long passed) {
        Preconditions.checkNotNull(base, "base must not be null");
        Preconditions.checkArgument(passed >= 0, "passed must not be negative: %s", passed);
        if (last == null) {
            Preconditions.checkArgument(
                    passed == 0, "passed must be zero when no document was: %s", passed);
            return base;
        }
        Query after = after(base, last);
        Integer limit = limitOf(base);
        return limit == null ? after : after.limit((int) Math.max(0, limit - passed));
    }

    /**
     * Returns the query continued after a document, its {@code offset} dropped and its {@code
     * limit} left as it was: for a caller that sets the limit itself, as the split reader does for
     * every page.
     *
     * @param base the query as the split holds it
     * @param last the last document passed
     * @return the continued query
     * @throws IllegalArgumentException if the query orders or filters by a field its projection
     *     omits; a stored value of an ordered field the client library cannot decode fails with the
     *     library's own exception, unchanged
     */
    public static Query after(Query base, DocumentSnapshot last) {
        try {
            return base.startAfter(last).offset(0);
        } catch (IllegalArgumentException e) {
            // startAfter also decodes the ordered fields, and a stored value the library cannot
            // decode throws IllegalArgumentException too; only the missing field gets the hint.
            if (e.getMessage() == null || !e.getMessage().contains(MISSING_FIELD)) {
                throw e;
            }
            throw new IllegalArgumentException(
                    "Cannot continue the Firestore query after "
                            + last.getReference().getPath()
                            + ": the query orders or filters by a field its projection (select)"
                            + " leaves out, and a cursor needs that field's value. Add the field"
                            + " to the projection. The client library said: "
                            + e.getMessage(),
                    e);
        }
    }

    /**
     * Returns the query's {@code limit}, or {@code null} when it has none.
     *
     * @param query the query
     * @return the limit, or {@code null}
     */
    @Nullable
    public static Integer limitOf(Query query) {
        StructuredQuery structured = query.toProto().getStructuredQuery();
        return structured.hasLimit() ? structured.getLimit().getValue() : null;
    }
}
