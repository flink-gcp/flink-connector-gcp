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

package io.github.flink.gcp.connector.bigtable.source.changestream.reader;

import com.google.cloud.bigtable.data.v2.models.Range.BoundType;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.protobuf.ByteString;

/** Converts the connector's range-algebra representation to the shape required by the SDK. */
final class ChangeStreamPartitions {

    private ChangeStreamPartitions() {}

    /** Restores the explicit {@code [start, end)} shape required by Change Streams SDK models. */
    static ByteStringRange sdkRange(ByteStringRange partition) {
        BoundType startBound = partition.getStartBound();
        BoundType endBound = partition.getEndBound();
        if (startBound != BoundType.CLOSED && startBound != BoundType.UNBOUNDED) {
            throw new IllegalArgumentException(
                    "A change-stream partition must have a closed start: " + startBound);
        }
        if (endBound != BoundType.OPEN && endBound != BoundType.UNBOUNDED) {
            throw new IllegalArgumentException(
                    "A change-stream partition must have an open end: " + endBound);
        }
        ByteString start =
                startBound == BoundType.UNBOUNDED ? ByteString.EMPTY : partition.getStart();
        ByteString end = endBound == BoundType.UNBOUNDED ? ByteString.EMPTY : partition.getEnd();
        return ByteStringRange.create(start, end);
    }
}
