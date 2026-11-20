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
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A reader's positions and watermarks for the splits it reads, sent as one event per report rather
 * than one per record.
 *
 * <p>A <b>committed</b> report carries the positions a completed checkpoint recorded, and the
 * coordinator derives its source-wide watermark frontier from those, as the Debezium Spanner
 * connector derives its low watermark from committed offsets: a failed-over reader restarts at or
 * after them, so a frontier built from them never runs ahead of a record the restore re-reads. A
 * <b>live</b> report carries the current positions and stands in for committed ones in a job that
 * completes no checkpoint.
 */
@Internal
public final class PartitionProgressEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private final boolean committed;
    private final List<Progress> partitions;

    /** Returns a report of the positions a completed checkpoint recorded. */
    public static PartitionProgressEvent committed(List<Progress> partitions) {
        return new PartitionProgressEvent(true, partitions);
    }

    /** Returns a report of current positions. */
    public static PartitionProgressEvent live(List<Progress> partitions) {
        return new PartitionProgressEvent(false, partitions);
    }

    private PartitionProgressEvent(boolean committed, List<Progress> partitions) {
        this.committed = committed;
        Preconditions.checkNotNull(partitions, "partitions must not be null");
        List<Progress> copy = new ArrayList<>(partitions);
        Preconditions.checkArgument(!copy.contains(null), "partitions must not contain null");
        this.partitions = Collections.unmodifiableList(copy);
    }

    /** Returns whether a completed checkpoint recorded these positions. */
    public boolean isCommitted() {
        return committed;
    }

    public List<Progress> getPartitions() {
        return partitions;
    }

    @Override
    public String toString() {
        return "PartitionProgressEvent{committed="
                + committed
                + ", partitions="
                + partitions.size()
                + '}';
    }

    /** One split's position and watermark. */
    @Internal
    public static final class Progress implements Serializable {

        private static final long serialVersionUID = 1L;

        private final String splitId;
        private final Instant currentPosition;
        private final Instant watermark;

        public Progress(String splitId, Instant currentPosition, Instant watermark) {
            this.splitId = Preconditions.checkNotNull(splitId, "splitId must not be null");
            this.currentPosition =
                    Preconditions.checkNotNull(currentPosition, "currentPosition must not be null");
            this.watermark = Preconditions.checkNotNull(watermark, "watermark must not be null");
        }

        public static Progress of(ChangeStreamPartitionSplit split) {
            return new Progress(split.splitId(), split.getCurrentPosition(), split.getWatermark());
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
}
