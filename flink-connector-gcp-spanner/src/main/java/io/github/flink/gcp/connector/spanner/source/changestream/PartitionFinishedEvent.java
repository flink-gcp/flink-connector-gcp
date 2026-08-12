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

package io.github.flink.gcp.connector.spanner.source.changestream;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.util.Preconditions;

import java.time.Instant;

/** Reports successful end-of-query after all child-partitions records were forwarded. */
@Internal
public final class PartitionFinishedEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private final String splitId;
    private final Instant currentPosition;
    private final Instant watermark;

    public PartitionFinishedEvent(String splitId, Instant currentPosition, Instant watermark) {
        this.splitId = Preconditions.checkNotNull(splitId, "splitId must not be null");
        this.currentPosition =
                Preconditions.checkNotNull(currentPosition, "currentPosition must not be null");
        this.watermark = Preconditions.checkNotNull(watermark, "watermark must not be null");
    }

    public String getSplitId() {
        return splitId;
    }

    public Instant getCurrentPosition() {
        return currentPosition;
    }

    public Instant getWatermark() {
        return watermark;
    }
}
