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

package io.github.flink.gcp.connector.spanner.source.batch.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.BatchTransactionId;
import com.google.cloud.spanner.Partition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * What one planning call answered with: a snapshot, and the partitions of the read at it.
 *
 * <p>The read timestamp is carried for the log line the enumerator writes and for nothing else. It
 * is the one thing about a batch read that a user cannot work out afterwards, and it is what makes
 * a result reproducible: the same plan replayed at the same timestamp reads the same rows.
 */
@Internal
public final class PartitionPlan {

    private final BatchTransactionId batchTransactionId;
    private final Timestamp readTimestamp;
    private final List<Partition> partitions;

    /**
     * Creates a plan.
     *
     * @param batchTransactionId the snapshot the partitions were planned at
     * @param readTimestamp the timestamp of that snapshot
     * @param partitions the partitions, in the order the service returned them
     */
    public PartitionPlan(
            BatchTransactionId batchTransactionId,
            Timestamp readTimestamp,
            List<Partition> partitions) {
        this.batchTransactionId =
                Preconditions.checkNotNull(
                        batchTransactionId, "batchTransactionId must not be null");
        this.readTimestamp =
                Preconditions.checkNotNull(readTimestamp, "readTimestamp must not be null");
        Preconditions.checkNotNull(partitions, "partitions must not be null");
        this.partitions = Collections.unmodifiableList(new ArrayList<>(partitions));
    }

    /**
     * Returns the snapshot the partitions were planned at.
     *
     * @return the batch transaction id
     */
    public BatchTransactionId getBatchTransactionId() {
        return batchTransactionId;
    }

    /**
     * Returns the timestamp of the snapshot.
     *
     * @return the read timestamp
     */
    public Timestamp getReadTimestamp() {
        return readTimestamp;
    }

    /**
     * Returns the partitions of the read.
     *
     * @return the partitions
     */
    public List<Partition> getPartitions() {
        return partitions;
    }
}
