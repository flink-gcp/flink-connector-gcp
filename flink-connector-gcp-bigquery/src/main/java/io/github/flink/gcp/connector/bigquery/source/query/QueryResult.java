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

package io.github.flink.gcp.connector.bigquery.source.query;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.bigquery.sink.TableDestination;

/**
 * What running the query produced: the table the result landed in, and whether it came from a
 * previous attempt's job rather than a fresh submission.
 *
 * <p>The second half exists for the metrics: {@code queryJobsSubmitted} must not count a reuse, or
 * the counter whose value above one is the signal "the query has been billed more than once" would
 * say so exactly when the reuse prevented it.
 */
@Internal
public final class QueryResult {

    private final TableDestination table;
    private final boolean reattached;

    /**
     * Creates the result.
     *
     * @param table the table the query's result landed in
     * @param reattached whether a previous attempt's job was reused instead of submitting a new one
     */
    public QueryResult(TableDestination table, boolean reattached) {
        this.table = Preconditions.checkNotNull(table, "table must not be null");
        this.reattached = reattached;
    }

    /** Returns the table the query's result landed in. */
    public TableDestination getTable() {
        return table;
    }

    /** Returns whether a previous attempt's job was reused instead of submitting a new one. */
    public boolean isReattached() {
        return reattached;
    }
}
