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

import com.google.auth.Credentials;
import io.github.flink.gcp.connector.firestore.source.FirestoreSourceConfig;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Plans a read: picks its snapshot time and cuts it into queries.
 *
 * <p>The seam the split enumerator plans against, so that the planning protocol can be tested
 * without a client. The emulator answers {@code PartitionQuery} with {@code UNIMPLEMENTED}, so the
 * service's partitioning cannot be exercised against it.
 *
 * <p><b>Not serializable, deliberately</b>, for the reason the Bigtable sampler is not: what
 * travels in the job graph is a {@link QueryPlannerFactory}, and the source mints one planner per
 * enumerator from it ({@code docs/adr/0128}). An implementation creates its client on first use, so
 * minting one opens nothing and a restore that re-uses a checkpointed plan opens nothing either.
 */
@Internal
public interface QueryPlanner extends AutoCloseable {

    /**
     * Plans the read.
     *
     * <p>Called from the enumerator's asynchronous planning step, once per job unless the job
     * restarts before a checkpoint records the plan, which is why a blocking implementation is
     * fine.
     *
     * @param config the source configuration
     * @param defaultPartitionCount the partition count to ask for when the configuration sets none
     * @return the plan
     * @throws IOException if a call fails or the configured read cannot be planned
     */
    QueryPlan plan(FirestoreSourceConfig<?> config, int defaultPartitionCount) throws IOException;

    /**
     * Receives the credentials the owning enumerator loaded, before the first {@link #plan}.
     *
     * <p>Abstract rather than defaulted because an implementation that quietly skipped it would
     * plan as the process's application default credentials instead of the configured service
     * account — a misconfiguration nothing would report.
     *
     * @param credentials the credentials to build clients with, or {@code null} to leave the
     *     client's application default credentials in place
     */
    void useCredentials(@Nullable Credentials credentials);

    @Override
    void close() throws IOException;
}
