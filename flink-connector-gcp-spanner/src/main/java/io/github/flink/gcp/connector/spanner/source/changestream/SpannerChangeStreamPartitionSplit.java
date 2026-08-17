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
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;

/**
 * One Spanner Change Streams partition and its checkpointed coordinator lifecycle.
 *
 * <p>Two halves that behave differently. The <b>definition</b> — token, parents, start and end
 * timestamps, heartbeat interval — is fixed when Spanner reports the partition. The <b>progress</b>
 * — position, lifecycle state, watermark — moves, and every mutation returns a new instance. {@link
 * #samePartitionDefinition} compares only the first half, which is what lets the coordinator merge
 * a returned split's progress into the ledger's entry while still refusing a split that claims a
 * different partition under the same id.
 *
 * <p><b>The id is derived, never stored.</b> A partition is identified by its token ({@code
 * change-stream-token:<token>}) or, for the one query that starts the stream, by {@link
 * #INITIAL_PARTITION_ID}. Spanner reports the same child from every parent that leads to it, so
 * deriving the id is what makes those reports converge on one ledger entry instead of several.
 *
 * <p>Parents are deduplicated and sorted on construction for the same reason: two readers may
 * report the same child with the parents in different orders, and the definition comparison above
 * has to see those as one partition.
 *
 * <p>The two shapes are mutually enforcing — the initial split has a null token and no parents, and
 * a token partition must name at least one parent — so a split cannot be built that is neither.
 *
 * <p>{@link #toString} prints whether a token is present, never the token itself: it is an opaque
 * blob nothing can look up, and printing it would push the assignment being logged off the line.
 */
@Internal
public final class SpannerChangeStreamPartitionSplit implements SourceSplit {

    public static final String INITIAL_PARTITION_ID = "change-stream-initial";

    @Nullable private final String partitionToken;
    private final List<String> parentPartitionIds;
    private final Instant startTimestamp;
    @Nullable private final Instant endTimestamp;
    private final long heartbeatMillis;
    private final Instant currentPosition;
    private final PartitionLifecycleState lifecycleState;
    private final Instant watermark;

    public SpannerChangeStreamPartitionSplit(
            @Nullable String partitionToken,
            List<String> parentPartitionIds,
            Instant startTimestamp,
            @Nullable Instant endTimestamp,
            long heartbeatMillis,
            Instant currentPosition,
            PartitionLifecycleState lifecycleState,
            Instant watermark) {
        this.partitionToken = partitionToken;
        Preconditions.checkNotNull(parentPartitionIds, "parentPartitionIds must not be null");
        Preconditions.checkArgument(
                !parentPartitionIds.contains(null), "parentPartitionIds must not contain null");
        Preconditions.checkArgument(
                !parentPartitionIds.contains(""), "parentPartitionIds must not contain empty ids");
        List<String> normalizedParents = new ArrayList<>(new LinkedHashSet<>(parentPartitionIds));
        Collections.sort(normalizedParents);
        this.parentPartitionIds = Collections.unmodifiableList(normalizedParents);
        if (partitionToken == null) {
            Preconditions.checkArgument(
                    parentPartitionIds.isEmpty(), "the initial split must not have parents");
        } else {
            Preconditions.checkArgument(
                    !partitionToken.isEmpty(), "partitionToken must not be empty");
            Preconditions.checkArgument(
                    !parentPartitionIds.isEmpty(),
                    "a token partition must have at least one parent");
        }
        this.startTimestamp =
                Preconditions.checkNotNull(startTimestamp, "startTimestamp must not be null");
        this.endTimestamp = endTimestamp;
        Preconditions.checkArgument(
                endTimestamp == null || !endTimestamp.isBefore(startTimestamp),
                "endTimestamp must not precede startTimestamp");
        Preconditions.checkArgument(
                heartbeatMillis > 0,
                "heartbeatMillis must be positive, but was %s",
                heartbeatMillis);
        this.heartbeatMillis = heartbeatMillis;
        this.currentPosition =
                Preconditions.checkNotNull(currentPosition, "currentPosition must not be null");
        this.lifecycleState =
                Preconditions.checkNotNull(lifecycleState, "lifecycleState must not be null");
        this.watermark = Preconditions.checkNotNull(watermark, "watermark must not be null");
    }

    public static SpannerChangeStreamPartitionSplit initial(
            Instant startTimestamp, @Nullable Instant endTimestamp, long heartbeatMillis) {
        return new SpannerChangeStreamPartitionSplit(
                null,
                Collections.emptyList(),
                startTimestamp,
                endTimestamp,
                heartbeatMillis,
                startTimestamp,
                PartitionLifecycleState.SCHEDULED,
                startTimestamp);
    }

    public static String idForToken(String token) {
        Preconditions.checkNotNull(token, "token must not be null");
        Preconditions.checkArgument(!token.isEmpty(), "token must not be empty");
        return "change-stream-token:" + token;
    }

    @Override
    public String splitId() {
        return partitionToken == null ? INITIAL_PARTITION_ID : idForToken(partitionToken);
    }

    @Nullable
    public String getPartitionToken() {
        return partitionToken;
    }

    public List<String> getParentPartitionIds() {
        return parentPartitionIds;
    }

    public Instant getStartTimestamp() {
        return startTimestamp;
    }

    @Nullable
    public Instant getEndTimestamp() {
        return endTimestamp;
    }

    public long getHeartbeatMillis() {
        return heartbeatMillis;
    }

    public Instant getCurrentPosition() {
        return currentPosition;
    }

    public PartitionLifecycleState getLifecycleState() {
        return lifecycleState;
    }

    public Instant getWatermark() {
        return watermark;
    }

    public SpannerChangeStreamPartitionSplit withLifecycleState(PartitionLifecycleState state) {
        return copy(currentPosition, state, watermark);
    }

    public SpannerChangeStreamPartitionSplit withProgress(
            Instant newPosition, Instant newWatermark) {
        return copy(newPosition, lifecycleState, newWatermark);
    }

    private SpannerChangeStreamPartitionSplit copy(
            Instant position, PartitionLifecycleState state, Instant newWatermark) {
        return new SpannerChangeStreamPartitionSplit(
                partitionToken,
                parentPartitionIds,
                startTimestamp,
                endTimestamp,
                heartbeatMillis,
                position,
                state,
                newWatermark);
    }

    public boolean samePartitionDefinition(SpannerChangeStreamPartitionSplit other) {
        return Objects.equals(partitionToken, other.partitionToken)
                && parentPartitionIds.equals(other.parentPartitionIds)
                && startTimestamp.equals(other.startTimestamp)
                && Objects.equals(endTimestamp, other.endTimestamp)
                && heartbeatMillis == other.heartbeatMillis;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SpannerChangeStreamPartitionSplit)) {
            return false;
        }
        SpannerChangeStreamPartitionSplit other = (SpannerChangeStreamPartitionSplit) o;
        return heartbeatMillis == other.heartbeatMillis
                && Objects.equals(partitionToken, other.partitionToken)
                && parentPartitionIds.equals(other.parentPartitionIds)
                && startTimestamp.equals(other.startTimestamp)
                && Objects.equals(endTimestamp, other.endTimestamp)
                && currentPosition.equals(other.currentPosition)
                && lifecycleState == other.lifecycleState
                && watermark.equals(other.watermark);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                partitionToken,
                parentPartitionIds,
                startTimestamp,
                endTimestamp,
                heartbeatMillis,
                currentPosition,
                lifecycleState,
                watermark);
    }

    @Override
    public String toString() {
        return "SpannerChangeStreamPartitionSplit{splitId='"
                + splitId()
                + "', token="
                + (partitionToken == null ? "<initial>" : "<present>")
                + ", parents="
                + parentPartitionIds.size()
                + ", position="
                + currentPosition
                + ", state="
                + lifecycleState
                + ", watermark="
                + watermark
                + '}';
    }
}
