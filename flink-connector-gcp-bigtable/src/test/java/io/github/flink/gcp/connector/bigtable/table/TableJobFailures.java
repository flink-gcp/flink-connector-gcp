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

package io.github.flink.gcp.connector.bigtable.table;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.concurrent.ExecutionException;

/** Failure-side result handling for Table API integration tests. */
final class TableJobFailures {

    private TableJobFailures() {}

    /**
     * Runs a query and returns the cause reported when its terminal job result completes
     * exceptionally.
     *
     * <p>Failure assertions must not collect rows: Flink 1.20's result fetcher can tear down the
     * MiniCluster and replace the task failure with a lifecycle exception.
     */
    static Throwable awaitFailure(TableEnvironment table, String query) throws Exception {
        TableResult result = table.executeSql(query);
        JobClient job =
                result.getJobClient()
                        .orElseThrow(
                                () ->
                                        new AssertionError(
                                                "Query did not submit a Flink job: " + query));
        try {
            job.getJobExecutionResult().get();
        } catch (ExecutionException e) {
            return e.getCause();
        }
        throw new AssertionError("Expected query to fail: " + query);
    }
}
