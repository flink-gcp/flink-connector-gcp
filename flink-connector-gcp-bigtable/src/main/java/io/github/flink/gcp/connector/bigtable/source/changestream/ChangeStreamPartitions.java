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

package io.github.flink.gcp.connector.bigtable.source.changestream;

import org.apache.flink.annotation.Internal;

import com.google.cloud.bigtable.data.v2.models.Range.BoundType;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.protobuf.ByteString;

import javax.annotation.Nullable;

/**
 * The shape every Bigtable Change Streams partition has, and the conversion to the shape the SDK
 * models require.
 *
 * <p>A partition is always {@code [closed start, open end)}, with either side possibly unbounded.
 * That rule is enforced at two points which cannot share a throw: {@link
 * #sdkRange(ByteStringRange)}, where a violation is a programming error, and {@link
 * ChangeStreamPartitionSplitSerializer}, whose {@code SimpleVersionedSerializer} contract is {@code
 * IOException}. So the rule is stated here once, by {@code partitionShapeViolation}, which hands
 * back the reason and lets each caller raise it in its own currency — otherwise the two wordings
 * drift apart.
 *
 * <p>It sits beside the split rather than in the reader package because the serializer is here.
 */
@Internal
public final class ChangeStreamPartitions {

    private ChangeStreamPartitions() {}

    /**
     * Returns why a range cannot be a change-stream partition, or null when it can.
     *
     * @param partition the range to check
     * @return the rule the range breaks, phrased for whoever meets it, or null when it breaks none
     */
    @Nullable
    static String partitionShapeViolation(ByteStringRange partition) {
        BoundType startBound = partition.getStartBound();
        if (startBound != BoundType.CLOSED && startBound != BoundType.UNBOUNDED) {
            return "A change-stream partition must have a closed start: " + startBound;
        }
        BoundType endBound = partition.getEndBound();
        if (endBound != BoundType.OPEN && endBound != BoundType.UNBOUNDED) {
            return "A change-stream partition must have an open end: " + endBound;
        }
        return null;
    }

    /**
     * Restores the explicit {@code [start, end)} shape required by Change Streams SDK models.
     *
     * @param partition the connector's representation of the partition
     * @return the partition with both bounds explicit, an unbounded side written as the empty key
     * @throws IllegalArgumentException if the partition is not {@code [closed start, open end)}
     */
    public static ByteStringRange sdkRange(ByteStringRange partition) {
        String violation = partitionShapeViolation(partition);
        if (violation != null) {
            throw new IllegalArgumentException(violation);
        }
        ByteString start =
                partition.getStartBound() == BoundType.UNBOUNDED
                        ? ByteString.EMPTY
                        : partition.getStart();
        ByteString end =
                partition.getEndBound() == BoundType.UNBOUNDED
                        ? ByteString.EMPTY
                        : partition.getEnd();
        return ByteStringRange.create(start, end);
    }
}
