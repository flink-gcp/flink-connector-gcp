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
import org.apache.flink.util.Preconditions;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;

/** Checkpointed Spanner Change Streams partition ledger. */
@Internal
public final class SpannerChangeStreamEnumeratorState {

    private final List<SpannerChangeStreamPartitionSplit> partitions;
    private final long sourceWatermark;

    public SpannerChangeStreamEnumeratorState(List<SpannerChangeStreamPartitionSplit> partitions) {
        this(partitions, inferredSourceWatermark(partitions), true);
    }

    SpannerChangeStreamEnumeratorState(
            List<SpannerChangeStreamPartitionSplit> partitions, long sourceWatermark) {
        this(partitions, sourceWatermark, true);
    }

    /** Copies a ledger whose invariants were already enforced by the coordinator mutation paths. */
    public static SpannerChangeStreamEnumeratorState snapshotOfCoordinatorLedger(
            Collection<SpannerChangeStreamPartitionSplit> partitions, long sourceWatermark) {
        return new SpannerChangeStreamEnumeratorState(partitions, sourceWatermark, false);
    }

    private SpannerChangeStreamEnumeratorState(
            Collection<SpannerChangeStreamPartitionSplit> partitions,
            long sourceWatermark,
            boolean validate) {
        Preconditions.checkNotNull(partitions, "partitions must not be null");
        if (validate) {
            validate(partitions);
            validateSourceWatermark(partitions, sourceWatermark);
        }
        this.partitions = Collections.unmodifiableList(new ArrayList<>(partitions));
        this.sourceWatermark = sourceWatermark;
    }

    private static long inferredSourceWatermark(
            Collection<SpannerChangeStreamPartitionSplit> partitions) {
        OptionalLong inferred = SpannerChangeStreamWatermarks.sourceWatermark(partitions);
        return inferred.isPresent() ? inferred.getAsLong() : Long.MIN_VALUE;
    }

    private static void validateSourceWatermark(
            Collection<SpannerChangeStreamPartitionSplit> partitions, long sourceWatermark) {
        OptionalLong current = SpannerChangeStreamWatermarks.sourceWatermark(partitions);
        Preconditions.checkArgument(
                !current.isPresent() || sourceWatermark <= current.getAsLong(),
                "source watermark %s is ahead of complete-ledger frontier %s",
                sourceWatermark,
                current.isPresent() ? current.getAsLong() : "<finished>");
    }

    private static void validate(Collection<SpannerChangeStreamPartitionSplit> partitions) {
        Preconditions.checkArgument(
                !partitions.isEmpty(), "partitions must contain the initial partition ledger");
        Map<String, SpannerChangeStreamPartitionSplit> partitionsById = new HashMap<>();
        for (SpannerChangeStreamPartitionSplit partition : partitions) {
            Preconditions.checkNotNull(partition, "partitions must not contain null");
            Preconditions.checkArgument(
                    partitionsById.put(partition.splitId(), partition) == null,
                    "partition split ids must be unique, but %s appeared twice",
                    partition.splitId());
        }
        Preconditions.checkArgument(
                partitionsById.containsKey(SpannerChangeStreamPartitionSplit.INITIAL_PARTITION_ID),
                "partition ledger must contain initial partition %s",
                SpannerChangeStreamPartitionSplit.INITIAL_PARTITION_ID);
        for (SpannerChangeStreamPartitionSplit partition : partitions) {
            validateParents(partition, partitionsById);
        }
        validateAcyclic(partitions, partitionsById);
    }

    private static void validateAcyclic(
            Collection<SpannerChangeStreamPartitionSplit> partitions,
            Map<String, SpannerChangeStreamPartitionSplit> partitionsById) {
        Map<String, Integer> remainingParents = new HashMap<>();
        Map<String, List<String>> childrenByParent = new HashMap<>();
        Deque<String> ready = new ArrayDeque<>();
        for (SpannerChangeStreamPartitionSplit partition : partitions) {
            int parentCount = partition.getParentPartitionIds().size();
            remainingParents.put(partition.splitId(), parentCount);
            if (parentCount == 0) {
                ready.addLast(partition.splitId());
            }
            for (String parentId : partition.getParentPartitionIds()) {
                childrenByParent
                        .computeIfAbsent(parentId, ignored -> new ArrayList<>())
                        .add(partition.splitId());
            }
        }
        int visited = 0;
        while (!ready.isEmpty()) {
            String partitionId = ready.removeFirst();
            visited++;
            for (String childId :
                    childrenByParent.getOrDefault(partitionId, Collections.emptyList())) {
                int remaining = remainingParents.computeIfPresent(childId, (ignored, n) -> n - 1);
                if (remaining == 0) {
                    ready.addLast(childId);
                }
            }
        }
        Preconditions.checkArgument(
                visited == partitionsById.size(),
                "partition ledger contains a parent cycle involving %s partition(s)",
                partitionsById.size() - visited);
    }

    private static void validateParents(
            SpannerChangeStreamPartitionSplit partition,
            Map<String, SpannerChangeStreamPartitionSplit> partitionsById) {
        for (String parentId : partition.getParentPartitionIds()) {
            SpannerChangeStreamPartitionSplit parent = partitionsById.get(parentId);
            Preconditions.checkArgument(
                    parent != null,
                    "partition %s names missing parent %s",
                    partition.splitId(),
                    parentId);
            if (partition.getLifecycleState() != PartitionLifecycleState.CREATED) {
                Preconditions.checkArgument(
                        parent.getLifecycleState() == PartitionLifecycleState.FINISHED,
                        "partition %s is %s before parent %s is FINISHED",
                        partition.splitId(),
                        partition.getLifecycleState(),
                        parentId);
            }
        }
    }

    public List<SpannerChangeStreamPartitionSplit> getPartitions() {
        return partitions;
    }

    public long getSourceWatermark() {
        return sourceWatermark;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SpannerChangeStreamEnumeratorState)) {
            return false;
        }
        SpannerChangeStreamEnumeratorState other = (SpannerChangeStreamEnumeratorState) o;
        return sourceWatermark == other.sourceWatermark && partitions.equals(other.partitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitions, sourceWatermark);
    }
}
