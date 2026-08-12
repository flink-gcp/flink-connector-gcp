/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.spanner.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.MutationGroup;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.spanner.v1.BatchWriteResponse;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * The {@link SpannerDatabaseAccess} over a real Spanner service handle.
 *
 * <p>Holds the three operations it needs as functional values rather than the {@code Spanner} and
 * {@code DatabaseClient} objects they come from. Both are interfaces of twenty-odd methods, so a
 * test that wants to script a half-reported batch write would otherwise have to implement all of
 * them — which is the same reason {@code BigtableBatcherAdapter} takes its batcher operations this
 * way.
 *
 * <p>{@link #close()} releases the {@code Spanner} service handle, which is what owns the channels
 * and the session pool. There is no failure-absorbing wrapper around it: unlike the gax {@code
 * Batcher} and the Pub/Sub {@code Subscriber}, closing this handle does not re-report a failure the
 * writer already consumed.
 */
@Internal
final class SpannerServiceAdapter implements SpannerDatabaseAccess {

    private final String description;
    private final Supplier<Dialect> dialect;
    private final Function<String, ResultSet> executeQuery;
    private final Function<List<MutationGroup>, Iterable<BatchWriteResponse>> batchWrite;
    private final Runnable closeService;

    /**
     * Creates the adapter.
     *
     * @param description the database, for failure messages
     * @param dialect reads the database's dialect
     * @param executeQuery runs a read-only query
     * @param batchWrite applies mutation groups, returning the response stream
     * @param closeService releases the service handle
     */
    SpannerServiceAdapter(
            String description,
            Supplier<Dialect> dialect,
            Function<String, ResultSet> executeQuery,
            Function<List<MutationGroup>, Iterable<BatchWriteResponse>> batchWrite,
            Runnable closeService) {
        this.description = Preconditions.checkNotNull(description, "description must not be null");
        this.dialect = Preconditions.checkNotNull(dialect, "dialect must not be null");
        this.executeQuery =
                Preconditions.checkNotNull(executeQuery, "executeQuery must not be null");
        this.batchWrite = Preconditions.checkNotNull(batchWrite, "batchWrite must not be null");
        this.closeService =
                Preconditions.checkNotNull(closeService, "closeService must not be null");
    }

    @Override
    public CellWeights readCellWeights() throws IOException {
        Dialect databaseDialect;
        String sql;
        try {
            databaseDialect = dialect.get();
            sql = InformationSchemaCellWeights.queryFor(databaseDialect);
        } catch (SpannerException e) {
            throw new IOException("Failed to read the dialect of " + description + ".", e);
        }
        try (ResultSet resultSet = executeQuery.apply(sql)) {
            return InformationSchemaCellWeights.read(resultSet, databaseDialect);
        } catch (SpannerException e) {
            throw new IOException(
                    "Failed to read the mutation-cell weights of "
                            + description
                            + " from INFORMATION_SCHEMA. The sink needs to read the database's"
                            + " secondary indexes to weigh a mutation the way Spanner counts one.",
                    e);
        }
    }

    @Override
    public void batchWrite(List<MutationGroup> groups, GroupOutcomes outcomes) {
        for (BatchWriteResponse response : batchWrite.apply(groups)) {
            // One response may decide several groups, and reports them by their index in the
            // request — which is why the writer sends the groups it is still waiting on as a
            // fresh list each attempt rather than keeping the original indexes.
            for (int groupIndex : response.getIndexesList()) {
                outcomes.report(groupIndex, response.getStatus());
            }
        }
    }

    @Override
    public void close() {
        closeService.run();
    }
}
