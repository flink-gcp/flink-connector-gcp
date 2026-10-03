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
import org.apache.flink.util.Preconditions;

import com.google.cloud.Timestamp;
import com.google.datastore.v1.RunQueryRequest;

import javax.annotation.Nullable;

import java.util.List;

/**
 * What planning answers with: the snapshot time, the queries every split reads at it, and why the
 * query was not split when it was not.
 */
@Internal
public final class QueryPlan {

    private final Timestamp readTime;
    private final List<RunQueryRequest> requests;
    @Nullable private final String unsplittableReason;

    /**
     * Creates a plan.
     *
     * @param readTime the snapshot time every split reads at
     * @param requests the queries to read, one per split, in order; never empty
     * @param unsplittableReason why the query is read as one split, phrased to follow "because", or
     *     {@code null} when the query could be split, however many key ranges that gave
     */
    public QueryPlan(
            Timestamp readTime,
            List<RunQueryRequest> requests,
            @Nullable String unsplittableReason) {
        this.readTime = Preconditions.checkNotNull(readTime, "readTime must not be null");
        Preconditions.checkNotNull(requests, "requests must not be null");
        Preconditions.checkArgument(!requests.isEmpty(), "a plan holds at least one query");
        Preconditions.checkArgument(
                unsplittableReason == null || requests.size() == 1,
                "a query that cannot be split is planned as one split");
        this.requests = List.copyOf(requests);
        this.unsplittableReason = unsplittableReason;
    }

    /** Returns the snapshot time every split reads at. */
    public Timestamp getReadTime() {
        return readTime;
    }

    /** Returns the queries to read, one per split. */
    public List<RunQueryRequest> getRequests() {
        return requests;
    }

    /** Returns why the query is read as one split, or {@code null} when it could be split. */
    @Nullable
    public String getUnsplittableReason() {
        return unsplittableReason;
    }
}
