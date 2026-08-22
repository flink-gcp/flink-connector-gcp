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

package io.github.flink.gcp.connector.spanner.source.batch.enumerator;

import org.apache.flink.annotation.Internal;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.PartitionOptions;
import com.google.cloud.spanner.TimestampBound;
import io.github.flink.gcp.connector.spanner.SpannerRpcPriority;
import io.github.flink.gcp.connector.spanner.source.SpannerReadOperation;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Opens a batch read and asks the service to plan it into partitions.
 *
 * <p>The seam the enumerator plans through. It is the enumerator's to close, and closing it
 * releases <em>both</em> the batch transaction and the service handle it was opened on — which is
 * why the split enumerator base class takes one closeable and not two.
 *
 * <p><b>Not serializable, deliberately.</b> What travels in the job graph is a {@link
 * PartitionPlannerFactory}, and the source mints one planner per enumerator from it. The JobManager
 * holds one source object for a job's whole life, so a planner parked on the source configuration
 * would be shared by every enumerator a coordinator reset builds, and the first teardown would
 * refuse every later one. Leaving this interface unserializable is what stops that being
 * expressible (issue #990, {@code docs/adr/0128}).
 *
 * <p>It carries no service-account key-file path: the enumerator that owns it loads one set of
 * credentials and hands them over through {@link #useCredentials}, so an implementation neither
 * holds a path nor reads one.
 */
@Internal
public interface PartitionPlanner extends AutoCloseable {

    /**
     * Plans the read into partitions.
     *
     * <p>Called at most once per enumerator, off the coordinator thread.
     *
     * @param operation what to read
     * @param bound the snapshot to read at
     * @param partitionOptions the partition-count and partition-size hints; the service may ignore
     *     both
     * @param dataBoostEnabled whether to run the read on Data Boost's independent compute
     * @param rpcPriority the priority Spanner schedules the reads at, or {@code null} to leave it
     *     unset
     * @return the snapshot and its partitions
     * @throws IOException if the read cannot be planned
     */
    PartitionPlan plan(
            SpannerReadOperation operation,
            TimestampBound bound,
            PartitionOptions partitionOptions,
            boolean dataBoostEnabled,
            @Nullable SpannerRpcPriority rpcPriority)
            throws IOException;

    /**
     * Receives the credentials the owning enumerator loaded, before the one {@link
     * #plan(SpannerReadOperation, TimestampBound, PartitionOptions, boolean, SpannerRpcPriority)}
     * call.
     *
     * <p>Declared here rather than on the implementation so that the enumerator needs no cast, and
     * abstract rather than defaulted because an implementation that quietly skipped it would plan
     * as the process's application default credentials instead of the configured service account —
     * a misconfiguration nothing would report.
     *
     * @param credentials what the enumerator loaded, or {@code null} to keep application default
     *     credentials
     */
    void useCredentials(@Nullable GoogleCredentials credentials);

    /**
     * Releases the batch transaction and the client behind it.
     *
     * <p>An implementation absorbs nothing: a teardown failure is the enumerator's to report.
     *
     * @throws IOException if the release fails
     */
    @Override
    void close() throws IOException;
}
