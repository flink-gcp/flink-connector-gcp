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

package io.github.flink.gcp.connector.spanner.table;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Deadline-bounded reads of an unbounded SQL query's first rows, for the integration tests. */
public final class UnboundedQueryRows {

    /**
     * The first rows of an unbounded query, read within a deadline. {@code hasNext()} blocks until
     * a row arrives, and Flink's collect fetcher retries through an interrupt, so a row that never
     * comes would hang the fork rather than fail the test. Past the deadline the iterator is
     * closed, which cancels the job, and the rows read so far are what the failure shows.
     *
     * <p>Every row counts, duplicates included: none of the callers restarts its job, so a repeated
     * change record is a defect their assertions must be able to see.
     */
    public static List<Row> firstRows(
            TableEnvironment table, String sql, int count, Duration deadline) throws Exception {
        List<Row> rows = new CopyOnWriteArrayList<>();
        CloseableIterator<Row> iterator = table.executeSql(sql).collect();
        ExecutorService reader =
                Executors.newSingleThreadExecutor(
                        runnable -> {
                            Thread thread = new Thread(runnable, "it-first-rows");
                            thread.setDaemon(true);
                            return thread;
                        });
        try {
            Future<?> reading =
                    reader.submit(
                            () -> {
                                while (rows.size() < count && iterator.hasNext()) {
                                    rows.add(iterator.next());
                                }
                            });
            try {
                reading.get(deadline.toMillis(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                throw new AssertionError(
                        "Read "
                                + rows
                                + " within "
                                + deadline
                                + " where "
                                + count
                                + " rows were expected from: "
                                + sql,
                        e);
            }
            return new ArrayList<>(rows);
        } finally {
            iterator.close();
            reader.shutdownNow();
        }
    }

    private UnboundedQueryRows() {}
}
