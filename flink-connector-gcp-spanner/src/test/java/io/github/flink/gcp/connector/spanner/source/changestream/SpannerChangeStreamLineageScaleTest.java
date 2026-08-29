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

import org.apache.flink.core.memory.DataOutputSerializer;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("slow")
@Timeout(value = 2, unit = TimeUnit.MINUTES)
class SpannerChangeStreamLineageScaleTest {

    private static final int HISTORICAL_TRANSITIONS = 100_000;

    @Test
    void compactCheckpointStaysBoundedAfterOneHundredThousandTransitions() throws Exception {
        java.lang.management.ThreadMXBean platformThreadMemory =
                ManagementFactory.getThreadMXBean();
        assertThat(platformThreadMemory).isInstanceOf(com.sun.management.ThreadMXBean.class);
        com.sun.management.ThreadMXBean threadMemory =
                (com.sun.management.ThreadMXBean) platformThreadMemory;
        assertThat(threadMemory.isThreadAllocatedMemorySupported()).isTrue();
        threadMemory.setThreadAllocatedMemoryEnabled(true);

        List<ChangeStreamPartitionSplit> legacyLedger = legacyLedger(HISTORICAL_TRANSITIONS);
        long legacySnapshotAllocationStarted = currentThreadAllocatedBytes(threadMemory);
        byte[] legacyCheckpoint = legacyVersionTwoCheckpoint(legacyLedger);
        long legacySnapshotHeapAllocation =
                currentThreadAllocatedBytes(threadMemory) - legacySnapshotAllocationStarted;
        int legacyBytes = legacyCheckpoint.length;
        SpannerChangeStreamEnumeratorStateSerializer serializer =
                new SpannerChangeStreamEnumeratorStateSerializer();

        long migrationStarted = System.nanoTime();
        SpannerChangeStreamEnumeratorState compact = serializer.deserialize(2, legacyCheckpoint);
        long migrationMicros = elapsedMicros(migrationStarted);

        long snapshotStarted = System.nanoTime();
        long compactSnapshotAllocationStarted = currentThreadAllocatedBytes(threadMemory);
        byte[] compactCheckpoint = serializer.serialize(compact);
        long compactSnapshotHeapAllocation =
                currentThreadAllocatedBytes(threadMemory) - compactSnapshotAllocationStarted;
        long snapshotMicros = elapsedMicros(snapshotStarted);

        long restoreStarted = System.nanoTime();
        SpannerChangeStreamEnumeratorState restored =
                serializer.deserialize(serializer.getVersion(), compactCheckpoint);
        long restoreMicros = elapsedMicros(restoreStarted);

        assertThat(compact.getPartitions())
                .singleElement()
                .extracting(ChangeStreamPartitionSplit::getPartitionToken)
                .isEqualTo("partition-" + HISTORICAL_TRANSITIONS);
        assertThat(compact.getFinishedParentProofs()).isEmpty();
        assertThat(compactCheckpoint.length).isLessThan(legacyBytes / 1_000);
        assertThat(restored).isEqualTo(compact);

        System.out.printf(
                Locale.ROOT,
                "Spanner lineage scale: transitions=%d, live_entries=%d, proofs=%d,"
                        + " legacy_bytes=%d, compact_bytes=%d, migration_micros=%d,"
                        + " snapshot_micros=%d, restore_micros=%d,"
                        + " legacy_heap_lineage_entries=%d, compact_heap_lineage_entries=%d,"
                        + " legacy_snapshot_heap_allocation_bytes=%d,"
                        + " compact_snapshot_heap_allocation_bytes=%d%n",
                HISTORICAL_TRANSITIONS,
                compact.getPartitions().size(),
                compact.getFinishedParentProofs().size(),
                legacyBytes,
                compactCheckpoint.length,
                migrationMicros,
                snapshotMicros,
                restoreMicros,
                legacyLedger.size(),
                compact.getPartitions().size() + compact.getFinishedParentProofs().size(),
                legacySnapshotHeapAllocation,
                compactSnapshotHeapAllocation);
    }

    private static List<ChangeStreamPartitionSplit> legacyLedger(int transitions) {
        Instant start = Instant.parse("2026-01-01T00:00:00Z");
        List<ChangeStreamPartitionSplit> completeLedger = new ArrayList<>(transitions + 1);
        ChangeStreamPartitionSplit initial =
                ChangeStreamPartitionSplit.initial(start, null, 2_000)
                        .withLifecycleState(PartitionLifecycleState.FINISHED);
        completeLedger.add(initial);
        String parentId = initial.splitId();
        ChangeStreamPartitionSplit current = initial;
        for (int transition = 1; transition <= transitions; transition++) {
            Instant position = start.plusSeconds(transition);
            current =
                    new ChangeStreamPartitionSplit(
                            "partition-" + transition,
                            Collections.singletonList(parentId),
                            position,
                            null,
                            2_000,
                            position,
                            transition == transitions
                                    ? PartitionLifecycleState.RUNNING
                                    : PartitionLifecycleState.FINISHED,
                            position);
            completeLedger.add(current);
            parentId = current.splitId();
        }
        return completeLedger;
    }

    private static byte[] legacyVersionTwoCheckpoint(
            List<ChangeStreamPartitionSplit> completeLedger) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(4096);
        out.writeInt(completeLedger.size());
        for (ChangeStreamPartitionSplit partition : completeLedger) {
            ChangeStreamPartitionSplitSerializer.writeSplit(out, partition);
        }
        ChangeStreamPartitionSplit current = completeLedger.get(completeLedger.size() - 1);
        out.writeLong(current.getWatermark().toEpochMilli() - 1);
        return out.getCopyOfBuffer();
    }

    private static long elapsedMicros(long startedNanos) {
        return TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startedNanos);
    }

    private static long currentThreadAllocatedBytes(com.sun.management.ThreadMXBean threadMemory) {
        return threadMemory.getThreadAllocatedBytes(Thread.currentThread().getId());
    }
}
