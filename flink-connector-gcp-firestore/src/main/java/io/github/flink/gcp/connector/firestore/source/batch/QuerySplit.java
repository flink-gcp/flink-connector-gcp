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
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.util.Preconditions;

import com.google.cloud.Timestamp;
import com.google.firestore.v1.RunQueryRequest;

import java.util.Objects;

/**
 * One query still to be read, at the job's snapshot time.
 *
 * <p>The query <em>is</em> the remaining work, as a row range is for the Bigtable scan: a
 * checkpoint rewrites it to start after the last document successfully deserialized, with its
 * {@code limit} reduced by the documents already passed and its {@code offset} dropped, so a
 * restored split resumes without re-reading and without skipping ({@code docs/adr/0173}). A
 * partition of a collection-group scan is a query bounded by two document-name cursors; the
 * arbitrary-query shape is the user's query as one split.
 *
 * <p>The query is held in its wire form, a {@code RunQueryRequest} whose read time is unset,
 * because the client library's {@code Query} belongs to a client instance. The read time is held
 * beside it: every split of one job carries the same one, which is what makes the splits one
 * snapshot.
 *
 * <p>A split whose {@code limit} has reached zero has nothing left in it, the normal state of a
 * split whose last permitted document was emitted before the checkpoint. The split reader finishes
 * such a split without a request.
 */
@Internal
public final class QuerySplit implements SourceSplit {

    private final String splitId;
    private final RunQueryRequest query;
    private final Timestamp readTime;

    /**
     * Creates a split.
     *
     * @param splitId the split's identity, stable for as long as the split exists
     * @param query the query still to be read, without a read time or transaction
     * @param readTime the snapshot time every split of the job reads at
     */
    public QuerySplit(String splitId, RunQueryRequest query, Timestamp readTime) {
        this.splitId = Preconditions.checkNotNull(splitId, "splitId must not be null");
        this.query = Preconditions.checkNotNull(query, "query must not be null");
        Preconditions.checkArgument(
                query.hasStructuredQuery(), "query must carry a structured query: %s", query);
        Preconditions.checkArgument(
                query.getConsistencySelectorCase()
                        == RunQueryRequest.ConsistencySelectorCase.CONSISTENCYSELECTOR_NOT_SET,
                "query must not carry its own read time or transaction; the split's read time is"
                        + " applied when it is read");
        this.readTime = Preconditions.checkNotNull(readTime, "readTime must not be null");
    }

    /** Returns the query still to be read. */
    public RunQueryRequest getQuery() {
        return query;
    }

    /** Returns the snapshot time this split reads at. */
    public Timestamp getReadTime() {
        return readTime;
    }

    /** Returns whether a {@code limit} of zero leaves nothing to read. */
    public boolean isExhausted() {
        return query.getStructuredQuery().hasLimit()
                && query.getStructuredQuery().getLimit().getValue() <= 0;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof QuerySplit)) {
            return false;
        }
        QuerySplit other = (QuerySplit) o;
        return splitId.equals(other.splitId)
                && query.equals(other.query)
                && readTime.equals(other.readTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitId, query, readTime);
    }

    @Override
    public String toString() {
        return "QuerySplit{splitId='" + splitId + "', readTime=" + readTime + '}';
    }
}
