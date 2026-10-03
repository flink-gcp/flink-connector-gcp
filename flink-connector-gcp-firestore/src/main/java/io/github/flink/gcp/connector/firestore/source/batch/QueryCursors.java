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
import com.google.cloud.firestore.Query;
import com.google.firestore.v1.StructuredQuery;

import javax.annotation.Nullable;

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
 * <p>Continuing also rewrites the two clauses that count documents rather than name them: the
 * {@code offset} has been consumed once a document has been passed, and the {@code limit} shrinks
 * by the documents passed.
 */
@Internal
public final class QueryCursors {

    /** The client library's message for a snapshot that lacks an ordered field. */
    private static final String MISSING_FIELD = "is missing in the provided DocumentSnapshot";

    private QueryCursors() {}

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
