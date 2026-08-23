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

package io.github.flink.gcp.connector.spanner.source.changestream;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import java.time.Instant;
import java.util.Collection;
import java.util.OptionalLong;

/** Calculates the non-early Flink watermark for the complete Spanner partition ledger. */
@Internal
public final class SpannerChangeStreamWatermarks {

    private SpannerChangeStreamWatermarks() {}

    /** Returns the minimum unfinished-partition frontier, if the ledger is still active. */
    public static OptionalLong sourceWatermark(Collection<ChangeStreamPartitionSplit> partitions) {
        Preconditions.checkNotNull(partitions, "partitions must not be null");
        long minimum = Long.MAX_VALUE;
        boolean unfinished = false;
        for (ChangeStreamPartitionSplit partition : partitions) {
            Preconditions.checkNotNull(partition, "partitions must not contain null");
            if (partition.getLifecycleState() == PartitionLifecycleState.FINISHED) {
                continue;
            }
            unfinished = true;
            minimum = Math.min(minimum, beforeInstant(partition.getWatermark()));
        }
        return unfinished ? OptionalLong.of(minimum) : OptionalLong.empty();
    }

    /** Converts an inclusive Spanner frontier to Flink's millisecond, exclusive-future contract. */
    public static long beforeInstant(Instant instant) {
        long containingMillisecond =
                Preconditions.checkNotNull(instant, "instant must not be null").toEpochMilli();
        return containingMillisecond == Long.MIN_VALUE ? Long.MIN_VALUE : containingMillisecond - 1;
    }
}
