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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeSet;

/**
 * Checkpointed Spanner Change Streams partition state.
 *
 * <p>This is the whole partition-lifecycle recovery record. The partition list contains only
 * unfinished entries. A finished parent needed by a {@link PartitionLifecycleState#CREATED CREATED}
 * child is reduced to its split id in {@code finishedParentProofs}; once no created child refers to
 * that id, the proof leaves the checkpoint too.
 *
 * <p>The public constructors read the complete-ledger shape written by serializer versions 1 and 2.
 * They validate the historical ledger before compacting it, so corrupt legacy state cannot become
 * an apparently valid proof. The compact constructor validates version 3 directly: split ids and
 * proofs are disjoint, every created dependency is present as a live split or a proof, proofs have
 * a created dependent, and the live graph is acyclic.
 *
 * <p>The coordinator snapshot factory skips those graph checks because every mutation path has
 * already enforced them. Avoiding a topological sort at each checkpoint keeps snapshot cost
 * proportional to copying the current compact state.
 *
 * <p>The source frontier is stored rather than recomputed. A validating path rejects a frontier
 * ahead of any unfinished partition, but restore must replay the value readers actually saw rather
 * than a later value derived from current entries.
 */
@Internal
public final class SpannerChangeStreamEnumeratorState {

    private final List<ChangeStreamPartitionSplit> partitions;
    private final Set<String> finishedParentProofs;
    private final boolean bounded;
    private final long sourceWatermark;

    /** Reads and compacts the complete-ledger shape used by serializer version 1. */
    public SpannerChangeStreamEnumeratorState(List<ChangeStreamPartitionSplit> partitions) {
        this(compactLegacyLedger(partitions, inferredSourceWatermark(partitions)));
    }

    /** Reads and compacts the complete-ledger shape used by serializer version 2. */
    SpannerChangeStreamEnumeratorState(
            List<ChangeStreamPartitionSplit> partitions, long sourceWatermark) {
        this(compactLegacyLedger(partitions, sourceWatermark));
    }

    /** Reads the compact shape used by serializer version 3. */
    SpannerChangeStreamEnumeratorState(
            Collection<ChangeStreamPartitionSplit> partitions,
            Collection<String> finishedParentProofs,
            boolean bounded,
            long sourceWatermark) {
        this(partitions, finishedParentProofs, bounded, sourceWatermark, true);
    }

    /** Copies compact state whose invariants were enforced by coordinator mutation paths. */
    public static SpannerChangeStreamEnumeratorState snapshotOfCoordinatorLedger(
            Collection<ChangeStreamPartitionSplit> partitions,
            Collection<String> finishedParentProofs,
            boolean bounded,
            long sourceWatermark) {
        return new SpannerChangeStreamEnumeratorState(
                partitions, finishedParentProofs, bounded, sourceWatermark, false);
    }

    private SpannerChangeStreamEnumeratorState(CompactedLegacyState legacy) {
        this(
                legacy.partitions,
                legacy.finishedParentProofs,
                legacy.bounded,
                legacy.sourceWatermark,
                true);
    }

    private SpannerChangeStreamEnumeratorState(
            Collection<ChangeStreamPartitionSplit> partitions,
            Collection<String> finishedParentProofs,
            boolean bounded,
            long sourceWatermark,
            boolean validate) {
        Preconditions.checkNotNull(partitions, "partitions must not be null");
        Preconditions.checkNotNull(finishedParentProofs, "finishedParentProofs must not be null");
        if (validate) {
            validateCompact(partitions, finishedParentProofs, bounded);
            validateSourceWatermark(partitions, sourceWatermark);
        }
        this.partitions = Collections.unmodifiableList(new ArrayList<>(partitions));
        this.finishedParentProofs =
                Collections.unmodifiableSet(new LinkedHashSet<>(finishedParentProofs));
        this.bounded = bounded;
        this.sourceWatermark = sourceWatermark;
    }

    private static CompactedLegacyState compactLegacyLedger(
            Collection<ChangeStreamPartitionSplit> partitions, long sourceWatermark) {
        Preconditions.checkNotNull(partitions, "partitions must not be null");
        Map<String, ChangeStreamPartitionSplit> partitionsById = validateLegacy(partitions);
        validateSourceWatermark(partitions, sourceWatermark);

        List<ChangeStreamPartitionSplit> unfinished = new ArrayList<>();
        Set<String> finishedParentProofs = new TreeSet<>();
        boolean bounded = false;
        for (ChangeStreamPartitionSplit partition : partitions) {
            bounded |= partition.getEndTimestamp() != null;
            if (partition.getLifecycleState() != PartitionLifecycleState.FINISHED) {
                unfinished.add(partition);
            }
            if (partition.getLifecycleState() == PartitionLifecycleState.CREATED) {
                for (String parentId : partition.getParentPartitionIds()) {
                    if (partitionsById.get(parentId).getLifecycleState()
                            == PartitionLifecycleState.FINISHED) {
                        finishedParentProofs.add(parentId);
                    }
                }
            }
        }
        return new CompactedLegacyState(unfinished, finishedParentProofs, bounded, sourceWatermark);
    }

    private static long inferredSourceWatermark(Collection<ChangeStreamPartitionSplit> partitions) {
        OptionalLong inferred = SpannerChangeStreamWatermarks.sourceWatermark(partitions);
        return inferred.isPresent() ? inferred.getAsLong() : Long.MIN_VALUE;
    }

    private static void validateSourceWatermark(
            Collection<ChangeStreamPartitionSplit> partitions, long sourceWatermark) {
        OptionalLong current = SpannerChangeStreamWatermarks.sourceWatermark(partitions);
        Preconditions.checkArgument(
                !current.isPresent() || sourceWatermark <= current.getAsLong(),
                "source watermark %s is ahead of unfinished-ledger frontier %s",
                sourceWatermark,
                current.isPresent() ? current.getAsLong() : "<finished>");
    }

    private static Map<String, ChangeStreamPartitionSplit> validateLegacy(
            Collection<ChangeStreamPartitionSplit> partitions) {
        Preconditions.checkArgument(
                !partitions.isEmpty(), "partitions must contain the initial partition ledger");
        Map<String, ChangeStreamPartitionSplit> partitionsById = indexPartitions(partitions, true);
        Preconditions.checkArgument(
                partitionsById.containsKey(ChangeStreamPartitionSplit.INITIAL_PARTITION_ID),
                "partition ledger must contain initial partition %s",
                ChangeStreamPartitionSplit.INITIAL_PARTITION_ID);
        for (ChangeStreamPartitionSplit partition : partitions) {
            for (String parentId : partition.getParentPartitionIds()) {
                ChangeStreamPartitionSplit parent = partitionsById.get(parentId);
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
        validateAcyclic(partitions, partitionsById);
        return partitionsById;
    }

    private static void validateCompact(
            Collection<ChangeStreamPartitionSplit> partitions,
            Collection<String> finishedParentProofs,
            boolean bounded) {
        Preconditions.checkArgument(
                bounded || !partitions.isEmpty(),
                "unbounded compact partition ledger must contain an unfinished partition");
        Map<String, ChangeStreamPartitionSplit> partitionsById = indexPartitions(partitions, false);
        Set<String> proofs = new LinkedHashSet<>();
        for (String proof : finishedParentProofs) {
            Preconditions.checkNotNull(proof, "finishedParentProofs must not contain null");
            Preconditions.checkArgument(!proof.isEmpty(), "finishedParentProofs must not be empty");
            Preconditions.checkArgument(
                    proofs.add(proof), "finished parent proof %s appeared twice", proof);
            Preconditions.checkArgument(
                    !partitionsById.containsKey(proof),
                    "finished parent proof %s is also a live partition",
                    proof);
        }

        Set<String> referencedProofs = new LinkedHashSet<>();
        for (ChangeStreamPartitionSplit partition : partitions) {
            Preconditions.checkArgument(
                    bounded == (partition.getEndTimestamp() != null),
                    "compact partition %s boundedness does not match checkpoint flag %s",
                    partition.splitId(),
                    bounded);
            for (String parentId : partition.getParentPartitionIds()) {
                boolean liveParent = partitionsById.containsKey(parentId);
                boolean finishedProof = proofs.contains(parentId);
                if (partition.getLifecycleState() == PartitionLifecycleState.CREATED) {
                    Preconditions.checkArgument(
                            liveParent || finishedProof,
                            "created partition %s names missing parent %s",
                            partition.splitId(),
                            parentId);
                    if (finishedProof) {
                        referencedProofs.add(parentId);
                    }
                } else {
                    Preconditions.checkArgument(
                            !liveParent,
                            "partition %s is %s before live parent %s is released",
                            partition.splitId(),
                            partition.getLifecycleState(),
                            parentId);
                }
            }
        }
        Preconditions.checkArgument(
                referencedProofs.equals(proofs),
                "finished parent proofs must be referenced by CREATED partitions; stale proofs %s",
                difference(proofs, referencedProofs));
        validateAcyclic(partitions, partitionsById);
    }

    private static Map<String, ChangeStreamPartitionSplit> indexPartitions(
            Collection<ChangeStreamPartitionSplit> partitions, boolean legacy) {
        Map<String, ChangeStreamPartitionSplit> partitionsById = new HashMap<>();
        for (ChangeStreamPartitionSplit partition : partitions) {
            Preconditions.checkNotNull(partition, "partitions must not contain null");
            Preconditions.checkArgument(
                    partitionsById.put(partition.splitId(), partition) == null,
                    "partition split ids must be unique, but %s appeared twice",
                    partition.splitId());
            if (!legacy) {
                Preconditions.checkArgument(
                        partition.getLifecycleState() != PartitionLifecycleState.FINISHED,
                        "compact partition ledger must not contain FINISHED partition %s",
                        partition.splitId());
            }
        }
        return partitionsById;
    }

    private static Set<String> difference(Set<String> left, Set<String> right) {
        Set<String> result = new LinkedHashSet<>(left);
        result.removeAll(right);
        return result;
    }

    private static void validateAcyclic(
            Collection<ChangeStreamPartitionSplit> partitions,
            Map<String, ChangeStreamPartitionSplit> partitionsById) {
        Map<String, Integer> remainingParents = new HashMap<>();
        Map<String, List<String>> childrenByParent = new HashMap<>();
        Deque<String> ready = new ArrayDeque<>();
        for (ChangeStreamPartitionSplit partition : partitions) {
            int parentCount = 0;
            for (String parentId : partition.getParentPartitionIds()) {
                if (partitionsById.containsKey(parentId)) {
                    parentCount++;
                    childrenByParent
                            .computeIfAbsent(parentId, ignored -> new ArrayList<>())
                            .add(partition.splitId());
                }
            }
            remainingParents.put(partition.splitId(), parentCount);
            if (parentCount == 0) {
                ready.addLast(partition.splitId());
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

    public List<ChangeStreamPartitionSplit> getPartitions() {
        return partitions;
    }

    public Set<String> getFinishedParentProofs() {
        return finishedParentProofs;
    }

    public boolean isBounded() {
        return bounded;
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
        return bounded == other.bounded
                && sourceWatermark == other.sourceWatermark
                && partitions.equals(other.partitions)
                && finishedParentProofs.equals(other.finishedParentProofs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitions, finishedParentProofs, bounded, sourceWatermark);
    }

    private static final class CompactedLegacyState {

        private final List<ChangeStreamPartitionSplit> partitions;
        private final Set<String> finishedParentProofs;
        private final boolean bounded;
        private final long sourceWatermark;

        private CompactedLegacyState(
                List<ChangeStreamPartitionSplit> partitions,
                Set<String> finishedParentProofs,
                boolean bounded,
                long sourceWatermark) {
            this.partitions = partitions;
            this.finishedParentProofs = finishedParentProofs;
            this.bounded = bounded;
            this.sourceWatermark = sourceWatermark;
        }
    }
}
