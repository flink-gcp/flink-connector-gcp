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
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A reader's positions for the splits it reads, sent as one event per report rather than one per
 * heartbeat.
 *
 * <p>A reader reports the positions a checkpoint recorded once that checkpoint completes, as Flink
 * CDC's {@code IncrementalSourceReaderWithCommit} commits its offsets, so the coordinator's ledger
 * never runs ahead of what a region failover restores. Until a checkpoint completes, it also
 * reports its current positions at most once a second, so that a job that completes none still
 * keeps the ledger near its readers.
 */
@Internal
public final class PartitionProgressEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private final List<Progress> partitions;

    public PartitionProgressEvent(List<Progress> partitions) {
        Preconditions.checkNotNull(partitions, "partitions must not be null");
        this.partitions = Collections.unmodifiableList(new ArrayList<>(partitions));
    }

    public List<Progress> getPartitions() {
        return partitions;
    }

    @Override
    public String toString() {
        return "PartitionProgressEvent{partitions=" + partitions.size() + '}';
    }

    /** One split's position: its continuation tokens and low watermark. */
    @Internal
    public static final class Progress implements Serializable {

        private static final long serialVersionUID = 1L;

        private final String splitId;
        private final List<ChangeStreamContinuationToken> continuationTokens;
        private final Instant lowWatermark;

        private Progress(
                String splitId,
                List<ChangeStreamContinuationToken> continuationTokens,
                Instant lowWatermark) {
            this.splitId = Preconditions.checkNotNull(splitId, "splitId must not be null");
            this.continuationTokens =
                    Collections.unmodifiableList(
                            new ArrayList<>(
                                    Preconditions.checkNotNull(
                                            continuationTokens,
                                            "continuationTokens must not be null")));
            this.lowWatermark =
                    Preconditions.checkNotNull(lowWatermark, "lowWatermark must not be null");
        }

        /** Returns the position of an unclosed split. */
        public static Progress of(ChangeStreamPartitionSplit split) {
            Preconditions.checkArgument(
                    !split.isClosed(), "closed split %s has no position to report", split);
            return new Progress(
                    split.splitId(), split.getContinuationTokens(), split.getLowWatermark());
        }

        /** Returns the positions of unclosed splits, in order. */
        public static List<Progress> of(List<ChangeStreamPartitionSplit> splits) {
            List<Progress> progress = new ArrayList<>(splits.size());
            for (ChangeStreamPartitionSplit split : splits) {
                progress.add(of(split));
            }
            return progress;
        }

        public String getSplitId() {
            return splitId;
        }

        public List<ChangeStreamContinuationToken> getContinuationTokens() {
            return continuationTokens;
        }

        public Instant getLowWatermark() {
            return lowWatermark;
        }
    }
}
