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
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRanges;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Reports that a partition closed and names each successor range and its continuation token. */
@Internal
public final class PartitionTransitionEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private final String finishedSplitId;
    private final Instant lowWatermark;
    private final List<Successor> successors;

    public PartitionTransitionEvent(
            String finishedSplitId, Instant lowWatermark, List<Successor> successors) {
        this.finishedSplitId =
                Preconditions.checkNotNull(finishedSplitId, "finishedSplitId must not be null");
        this.lowWatermark =
                Preconditions.checkNotNull(lowWatermark, "lowWatermark must not be null");
        this.successors = Collections.unmodifiableList(new ArrayList<>(successors));
    }

    public String getFinishedSplitId() {
        return finishedSplitId;
    }

    public Instant getLowWatermark() {
        return lowWatermark;
    }

    public List<Successor> getSuccessors() {
        return successors;
    }

    /** One target range from CloseStream paired with the token that arrived from this parent. */
    @Internal
    public static final class Successor implements Serializable {

        private static final long serialVersionUID = 1L;

        private final ByteStringRange partition;
        private final ChangeStreamContinuationToken continuationToken;

        public Successor(
                ByteStringRange partition, ChangeStreamContinuationToken continuationToken) {
            this.partition =
                    RowRanges.copyOf(
                            Preconditions.checkNotNull(partition, "partition must not be null"));
            this.continuationToken =
                    Preconditions.checkNotNull(
                            continuationToken, "continuationToken must not be null");
        }

        public ByteStringRange getPartition() {
            return RowRanges.copyOf(partition);
        }

        public ChangeStreamContinuationToken getContinuationToken() {
            return continuationToken;
        }
    }
}
