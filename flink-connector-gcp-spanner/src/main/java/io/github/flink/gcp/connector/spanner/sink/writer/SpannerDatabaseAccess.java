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

import com.google.cloud.spanner.MutationGroup;
import com.google.rpc.Status;

import java.io.IOException;
import java.util.List;

/**
 * The narrow view of a Spanner database the writer needs: read the schema once, then apply batches.
 *
 * <p>This exists rather than the writer holding a {@code DatabaseClient} because that interface is
 * thirty-odd methods wide and its {@code batchWriteAtLeastOnce} returns a live server stream — a
 * test that wants to script a partial failure would have to fake both. Everything the writer
 * actually does is here.
 *
 * <p>Implementations are created per subtask and are not thread-safe; every method is called from
 * the task thread.
 */
@Internal
public interface SpannerDatabaseAccess extends AutoCloseable {

    /**
     * Reads how many cells each column of the database costs, counted the way Spanner counts a
     * mutation. Called once, when the writer opens.
     *
     * @return the weights
     * @throws IOException if the schema cannot be read
     */
    CellWeights readCellWeights() throws IOException;

    /**
     * Applies the groups through {@code batchWriteAtLeastOnce}, reporting each group's outcome to
     * {@code outcomes} as the service reports it.
     *
     * <p>Outcomes arrive as the server stream produces them, so a failure part-way through leaves
     * the groups already reported decided and the rest undetermined — which is the distinction the
     * writer's retry loop is built on, and why this takes a callback rather than returning a list.
     *
     * @param groups the mutation groups to apply, indexed as {@code outcomes} will report them
     * @param outcomes receives one call per group the service decided
     * @throws com.google.cloud.spanner.SpannerException if the request fails, after any outcomes
     *     the service did report
     */
    void batchWrite(List<MutationGroup> groups, GroupOutcomes outcomes);

    @Override
    void close();

    /** Receives the outcome of one mutation group of a batch write. */
    @Internal
    @FunctionalInterface
    interface GroupOutcomes {

        /**
         * Reports one group's outcome.
         *
         * @param groupIndex the group's index in the list passed to {@link #batchWrite}
         * @param status the service's verdict for that group, {@code OK} when it was applied
         */
        void report(int groupIndex, Status status);
    }
}
