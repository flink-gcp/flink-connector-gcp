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

package io.github.flink.gcp.connector.bigtable.source;

import org.apache.flink.annotation.PublicEvolving;

/**
 * When the Change Streams source starts reading a partition's successors after the partition
 * closes.
 *
 * <p>Bigtable closes a partition when it splits or merges and names the successors that continue
 * its row keys. A partition's last records can still be on their way downstream when it closes, in
 * a network buffer or a sink writer's batch. If another source subtask reads a successor, the
 * successor's records take a different path and can reach a keyed operator or a sink first, so one
 * row key's changes arrive out of commit order.
 *
 * @see BigtableChangeStreamSourceBuilder#partitionHandoff(PartitionHandoff)
 */
@PublicEvolving
public enum PartitionHandoff {

    /**
     * Starts a successor only after a checkpoint taken after its parent closed has completed
     * (default). Once an aligned checkpoint completes, every operator that handles its input before
     * the barrier has processed what the parent's reader emitted before it, and every sink that
     * flushes on the barrier has written it, so one row key's changes reach a keyed operator in
     * commit order. Each partition split or merge then delays the successors until the next
     * checkpoint completes: up to one checkpoint interval plus that checkpoint's own duration, and
     * longer while checkpoints fail.
     *
     * <p>An unaligned checkpoint stores records in flight without processing them, and so does an
     * operator that keeps pending elements in its state, such as asynchronous I/O or an
     * asynchronous sink's buffered requests; neither covers what follows it. Order also holds only
     * between failures: a failover replays from the last completed checkpoint, at least once. With
     * source parallelism 1 every record takes the same path, and the successors start at once.
     * Successors of a partition whose reader emitted no record also start at once, since nothing of
     * the partition is downstream to overtake.
     */
    AFTER_CHECKPOINT("after-checkpoint"),

    /**
     * Starts a successor as soon as its parent closes. One row key's changes can then reach a keyed
     * operator out of commit order when source parallelism is above 1. Use it when the consumer
     * orders each row key's changes by their commit timestamp and tie breaker, or for a job without
     * checkpointing, which would otherwise wait {@link
     * BigtableChangeStreamSourceBuilder#firstCheckpointTimeout(java.time.Duration)} at its first
     * split or merge.
     */
    IMMEDIATE("immediate");

    private final String value;

    PartitionHandoff(String value) {
        this.value = value;
    }

    /** Returns the spelling a {@code WITH} clause uses. */
    @Override
    public String toString() {
        return value;
    }
}
