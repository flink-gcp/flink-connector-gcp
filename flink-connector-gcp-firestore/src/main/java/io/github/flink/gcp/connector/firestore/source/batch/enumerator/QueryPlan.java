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
import org.apache.flink.util.Preconditions;

import com.google.cloud.Timestamp;
import com.google.firestore.v1.RunQueryRequest;

import java.util.List;

/** What planning answers with: the snapshot time and the queries every split reads at it. */
@Internal
public final class QueryPlan {

    private final Timestamp readTime;
    private final List<RunQueryRequest> queries;

    /**
     * Creates a plan.
     *
     * @param readTime the snapshot time every split reads at
     * @param queries the queries to read, one per split, in order; never empty
     */
    public QueryPlan(Timestamp readTime, List<RunQueryRequest> queries) {
        this.readTime = Preconditions.checkNotNull(readTime, "readTime must not be null");
        Preconditions.checkNotNull(queries, "queries must not be null");
        Preconditions.checkArgument(!queries.isEmpty(), "a plan holds at least one query");
        this.queries = List.copyOf(queries);
    }

    /** Returns the snapshot time every split reads at. */
    public Timestamp getReadTime() {
        return readTime;
    }

    /** Returns the queries to read, one per split. */
    public List<RunQueryRequest> getQueries() {
        return queries;
    }
}
