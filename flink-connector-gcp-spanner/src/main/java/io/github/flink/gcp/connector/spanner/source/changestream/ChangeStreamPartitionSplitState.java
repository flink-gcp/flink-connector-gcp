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

/** Mutable task-thread progress for one Spanner Change Streams partition. */
@Internal
public final class ChangeStreamPartitionSplitState {

    private final ChangeStreamPartitionSplit original;
    private Instant currentPosition;
    private Instant watermark;

    public ChangeStreamPartitionSplitState(ChangeStreamPartitionSplit split) {
        this.original = Preconditions.checkNotNull(split, "split must not be null");
        this.currentPosition = split.getCurrentPosition();
        this.watermark = split.getWatermark();
    }

    public ChangeStreamPartitionSplit toSplit() {
        return original.withProgress(currentPosition, watermark);
    }

    public String splitId() {
        return original.splitId();
    }

    public boolean isInitialPartition() {
        return original.getPartitionToken() == null;
    }

    public void advance(Instant position, Instant newWatermark) {
        currentPosition = later(currentPosition, position, "position");
        watermark = later(watermark, newWatermark, "watermark");
    }

    public Instant getCurrentPosition() {
        return currentPosition;
    }

    public Instant getWatermark() {
        return watermark;
    }

    private static Instant later(Instant current, Instant candidate, String name) {
        Preconditions.checkNotNull(candidate, name + " must not be null");
        return candidate.isAfter(current) ? candidate : current;
    }
}
