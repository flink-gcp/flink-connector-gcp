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

/**
 * A document the split reader handed to the task thread, with the split's query it was read under.
 *
 * <p>The query travels with the document because a checkpoint turns the two into a resume point
 * ({@link QueryCursors#continueAfter}), and the split reader has already rebuilt the query from its
 * wire form on its client. Carrying it saves the task thread rebuilding it at every checkpoint; the
 * same query object rides on every document of one split assignment, so this costs a reference, not
 * a copy.
 */
@Internal
public final class FetchedDocument {

    private final DocumentSnapshot document;
    private final Query splitQuery;

    /**
     * Creates the pair.
     *
     * @param document the document read
     * @param splitQuery the query of the split as it was assigned, not the page query
     */
    public FetchedDocument(DocumentSnapshot document, Query splitQuery) {
        this.document = Preconditions.checkNotNull(document, "document must not be null");
        this.splitQuery = Preconditions.checkNotNull(splitQuery, "splitQuery must not be null");
    }

    /** Returns the document read. */
    public DocumentSnapshot getDocument() {
        return document;
    }

    /** Returns the query of the split as it was assigned. */
    public Query getSplitQuery() {
        return splitQuery;
    }
}
