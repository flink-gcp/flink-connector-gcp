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

package io.github.flink.gcp.connector.bigtable.source.changestream.enumerator;

import org.apache.flink.annotation.Internal;

import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;

import java.time.Duration;
import java.util.List;

/**
 * Coordinator-side Bigtable operations, separated so partition protocol tests need no service.
 *
 * <p><b>Not serializable, deliberately.</b> What travels in the job graph is a {@link
 * ChangeStreamCoordinatorClientFactory}, and the source mints one client per enumerator from it.
 * The JobManager holds one source object for a job's whole life, so a client parked on the source
 * configuration would be shared by every enumerator a coordinator reset builds ({@code
 * docs/adr/0128}).
 */
@Internal
public interface ChangeStreamCoordinatorClient extends AutoCloseable {

    void validateSingleClusterAppProfile() throws Exception;

    Duration retention() throws Exception;

    /**
     * Returns the partitions currently covering the table's keyspace.
     *
     * <p>Every returned range must be normalised — an absent bound spelled {@code UNBOUNDED} rather
     * than as an empty key, which is what {@link
     * io.github.flink.gcp.connector.bigtable.RowRanges#copyOf(ByteStringRange)} produces. {@link
     * ChangeStreamPartitionReconciler} reads bound types, so an implementation that passed the
     * service's own spelling through would make a table's last partition read as an empty range and
     * its first fail to match itself across scans, and neither failure is reported anywhere.
     *
     * @return the current partitions, normalised
     * @throws Exception if the partitions cannot be discovered
     */
    List<ByteStringRange> generateInitialPartitions() throws Exception;

    @Override
    void close() throws Exception;
}
