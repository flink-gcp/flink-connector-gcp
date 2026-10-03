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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Query;

import javax.annotation.Nullable;

/**
 * How far through one {@link QuerySplit} the source has emitted.
 *
 * <p>Separate from the split for the reason the Bigtable scan's state is: the split reader works on
 * the fetcher thread, while the record emitter advances this state on the task thread. It tracks
 * the last document <em>successfully deserialized</em>, including one that produced no output,
 * which is what a checkpoint must record; the split reader keeps its own count of what it handed
 * over, which is what the next page must continue from.
 *
 * <p>Not thread-safe, and does not need to be: every method here is called from the task thread.
 */
@Internal
public final class QuerySplitState {

    private final QuerySplit split;

    @Nullable private DocumentSnapshot lastEmitted;
    @Nullable private Query splitQuery;
    private long emitted;

    /**
     * Creates the state of a freshly assigned or restored split.
     *
     * <p>It starts with no progress, whatever the split's history: a restored split already carries
     * its previous progress in its query.
     *
     * @param split the split being read
     */
    public QuerySplitState(QuerySplit split) {
        this.split = Preconditions.checkNotNull(split, "split must not be null");
    }

    /**
     * Records that a document has been successfully deserialized.
     *
     * <p>Called once per document, whatever the deserializer made of it: the split resumes at a
     * cursor, so a document that produced no record still has to move the resume point.
     *
     * @param document the document handed to the emitter, with the split's query
     */
    public void recordEmitted(FetchedDocument document) {
        Preconditions.checkNotNull(document, "document must not be null");
        this.lastEmitted = document.getDocument();
        this.splitQuery = document.getSplitQuery();
        this.emitted++;
    }

    /** Returns how many documents have been successfully deserialized since assignment. */
    @VisibleForTesting
    public long getEmitted() {
        return emitted;
    }

    /**
     * Returns the split as it should be checkpointed: the work that is left.
     *
     * @return the assigned split unchanged when no document has been successfully deserialized,
     *     otherwise a split whose query starts after the last such document
     * @throws IllegalArgumentException if no cursor can be taken from the document, which the
     *     planner's check at the start of the job rules out
     */
    public QuerySplit toSplit() {
        if (lastEmitted == null) {
            return split;
        }
        return new QuerySplit(
                split.splitId(),
                QueryCursors.continueAfter(splitQuery, lastEmitted, emitted).toProto(),
                split.getReadTime());
    }
}
