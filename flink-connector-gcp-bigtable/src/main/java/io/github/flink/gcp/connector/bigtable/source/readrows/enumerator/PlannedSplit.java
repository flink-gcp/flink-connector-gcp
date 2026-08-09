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

package io.github.flink.gcp.connector.bigtable.source.readrows.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.bigtable.source.readrows.RowRangeSplit;

import java.util.OptionalLong;

/**
 * A split as the planner produced it, with the size the samples suggest it holds.
 *
 * <p>The estimate exists to be logged once and dropped. It deliberately does not travel in {@link
 * RowRangeSplit}: a checkpoint truncates the split's range, after which an estimate made against
 * the original range describes nothing, and a field in the checkpointed form is a field the
 * serializer has to carry for ever.
 *
 * <p>It is absent for splits past the last sampled key, because the samples say how many bytes
 * precede each boundary and nothing at all about what lies beyond the final one. Reporting that as
 * zero would make the most likely place for a skewed table to hide look like the emptiest.
 */
@Internal
public final class PlannedSplit {

    private final RowRangeSplit split;
    private final OptionalLong estimatedBytes;

    PlannedSplit(RowRangeSplit split, OptionalLong estimatedBytes) {
        this.split = Preconditions.checkNotNull(split, "split must not be null");
        this.estimatedBytes =
                Preconditions.checkNotNull(estimatedBytes, "estimatedBytes must not be null");
    }

    /** Returns the split. */
    public RowRangeSplit getSplit() {
        return split;
    }

    /** Returns the approximate size of the split's range, when the samples cover it. */
    public OptionalLong getEstimatedBytes() {
        return estimatedBytes;
    }

    @Override
    public String toString() {
        return "PlannedSplit{split=" + split + ", estimatedBytes=" + estimatedBytes + '}';
    }
}
