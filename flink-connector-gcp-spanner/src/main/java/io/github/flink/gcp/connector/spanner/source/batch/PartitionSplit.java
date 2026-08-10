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

package io.github.flink.gcp.connector.spanner.source.batch;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.util.Preconditions;

import com.google.cloud.spanner.BatchTransactionId;
import com.google.cloud.spanner.Partition;

import java.util.Objects;

/**
 * One partition of a batch read, together with the snapshot it belongs to.
 *
 * <p>A reader rejoins the snapshot with {@code batchReadOnlyTransaction(batchTransactionId)} —
 * which costs no round trip, the client builds the handle from the id alone — and streams the
 * partition with {@code execute(partition)}. Both values are the service's to produce and opaque to
 * this connector: the token names a slice of the read that the service planned, and the transaction
 * id names the session and timestamp the whole plan was made at.
 *
 * <p><b>A partition is the unit of progress.</b> There is no offset inside one to resume at, so
 * this split does not shrink as it is read and a restore re-reads it from the start. What that
 * costs is bounded by one partition per in-flight subtask, and what it buys is a resume that does
 * not rest on an ordering Spanner does not promise: a partitioned query returns its rows in no
 * contractual order.
 */
@Internal
public final class PartitionSplit implements SourceSplit {

    private final String splitId;
    private final BatchTransactionId batchTransactionId;
    private final Partition partition;

    /**
     * Creates a split.
     *
     * @param splitId the split's id, unique within one plan
     * @param batchTransactionId the snapshot every split of this plan reads at
     * @param partition the partition to read
     */
    public PartitionSplit(
            String splitId, BatchTransactionId batchTransactionId, Partition partition) {
        this.splitId = Preconditions.checkNotNull(splitId, "splitId must not be null");
        this.batchTransactionId =
                Preconditions.checkNotNull(
                        batchTransactionId, "batchTransactionId must not be null");
        this.partition = Preconditions.checkNotNull(partition, "partition must not be null");
    }

    @Override
    public String splitId() {
        return splitId;
    }

    /**
     * Returns the snapshot this split reads at.
     *
     * @return the batch transaction id
     */
    public BatchTransactionId getBatchTransactionId() {
        return batchTransactionId;
    }

    /**
     * Returns the partition to read.
     *
     * @return the partition
     */
    public Partition getPartition() {
        return partition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PartitionSplit)) {
            return false;
        }
        PartitionSplit that = (PartitionSplit) o;
        return splitId.equals(that.splitId)
                && batchTransactionId.equals(that.batchTransactionId)
                && partition.equals(that.partition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitId, batchTransactionId, partition);
    }

    /**
     * Returns the split's id, which is what identifies it in a log.
     *
     * <p>Neither of the values it carries is printed. A partition token is an opaque blob of a few
     * hundred bytes that renders as base64, and the vendor's own {@code toString} prints the whole
     * query beside it — so a line naming one partition would push the assignment it was logging off
     * the screen, for a string nothing can look up anyway.
     */
    @Override
    public String toString() {
        return "partition split " + splitId;
    }
}
