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

import org.apache.flink.core.memory.DataOutputSerializer;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class BigtableChangeStreamEnumeratorStateSerializerTest {

    @Test
    void roundTripsEveryCoordinatorLedger() throws IOException {
        Instant watermark = Instant.parse("2026-08-11T00:00:00Z");
        ChangeStreamPartitionSplit unassigned =
                new ChangeStreamPartitionSplit(
                        "change-stream-2",
                        ByteStringRange.unbounded().endOpen("m"),
                        Collections.emptyList(),
                        watermark);
        ChangeStreamPartitionSplit assigned =
                new ChangeStreamPartitionSplit(
                        "change-stream-3",
                        ByteStringRange.unbounded().startClosed("m"),
                        Collections.singletonList(
                                TestChangeStreamTokens.token(
                                        ByteStringRange.unbounded().startClosed("m"), "token")),
                        watermark.plusSeconds(1));
        PendingMerge merge =
                new PendingMerge(
                        ByteStringRange.unbounded(),
                        assigned.getContinuationTokens(),
                        watermark.minusSeconds(1));
        BigtableChangeStreamEnumeratorState state =
                new BigtableChangeStreamEnumeratorState(
                        true,
                        watermark,
                        4,
                        Collections.singletonList(unassigned),
                        Collections.singletonList(assigned),
                        Collections.singletonList(merge),
                        Collections.singletonList(
                                new MissingPartition(
                                        ByteStringRange.create("c", "d"),
                                        watermark.minusSeconds(120),
                                        watermark.minusSeconds(10))),
                        Arrays.asList(
                                ByteStringRange.unbounded().endOpen("c"),
                                ByteStringRange.create("d", "m")));
        BigtableChangeStreamEnumeratorStateSerializer serializer =
                new BigtableChangeStreamEnumeratorStateSerializer();

        BigtableChangeStreamEnumeratorState restored =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(state));

        assertThat(restored).isEqualTo(state);
        assertThat(restored.getCompletedPartitions())
                .containsExactly(
                        ByteStringRange.unbounded().endOpen("c"), ByteStringRange.create("d", "m"));
    }

    /**
     * A bounded run that was checkpointed before version 3 has no record of what it already
     * finished. Reading its state as an empty completed list is the only honest answer, and it puts
     * the restored run back where #951 found it for one grace period rather than corrupting it.
     */
    @Test
    void restoresVersionTwoStateWithNoCompletedPartitions() throws IOException {
        Instant start = Instant.parse("2026-08-11T00:00:00Z");
        MissingPartition missing =
                new MissingPartition(
                        ByteStringRange.create("a", "m"), start.minusSeconds(60), start);
        BigtableChangeStreamEnumeratorState legacy =
                new BigtableChangeStreamEnumeratorState(
                        true,
                        start,
                        2,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.singletonList(missing));
        BigtableChangeStreamEnumeratorStateSerializer serializer =
                new BigtableChangeStreamEnumeratorStateSerializer();

        BigtableChangeStreamEnumeratorState restored =
                serializer.deserialize(2, serializeVersionTwo(legacy));

        assertThat(restored.getMissingPartitions()).containsExactly(missing);
        assertThat(restored.getCompletedPartitions()).isEmpty();
    }

    @Test
    void restoresVersionOneStateWithNoMissingPartitionTimers() throws IOException {
        Instant start = Instant.parse("2026-08-11T00:00:00Z");
        BigtableChangeStreamEnumeratorState legacy =
                new BigtableChangeStreamEnumeratorState(
                        true,
                        start,
                        1,
                        Collections.singletonList(
                                new ChangeStreamPartitionSplit(
                                        "change-stream-0",
                                        ByteStringRange.unbounded(),
                                        Collections.emptyList(),
                                        start)),
                        Collections.singletonList(
                                new ChangeStreamPartitionSplit(
                                        "change-stream-assigned",
                                        ByteStringRange.create("a", "z"),
                                        Collections.singletonList(
                                                TestChangeStreamTokens.token(
                                                        ByteStringRange.create("a", "z"),
                                                        "assigned")),
                                        start.plusSeconds(1))),
                        Collections.singletonList(
                                new PendingMerge(
                                        ByteStringRange.unbounded(),
                                        Collections.singletonList(
                                                TestChangeStreamTokens.token(
                                                        ByteStringRange.unbounded(), "merge")),
                                        start.minusSeconds(1))));
        BigtableChangeStreamEnumeratorStateSerializer serializer =
                new BigtableChangeStreamEnumeratorStateSerializer();
        byte[] versionOne = serializeVersionOne(legacy);

        BigtableChangeStreamEnumeratorState restored = serializer.deserialize(1, versionOne);

        assertThat(restored.getUnassignedSplits()).isEqualTo(legacy.getUnassignedSplits());
        assertThat(restored.getAssignedSplits()).isEqualTo(legacy.getAssignedSplits());
        assertThat(restored.getPendingMerges()).isEqualTo(legacy.getPendingMerges());
        assertThat(restored.getStartTime()).isEqualTo(start);
        assertThat(restored.getNextSplitId()).isEqualTo(1);
        assertThat(restored.getMissingPartitions()).isEmpty();
    }

    private static byte[] serializeVersionOne(BigtableChangeStreamEnumeratorState state)
            throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(1024);
        out.writeBoolean(state.isInitialized());
        ChangeStreamPartitionSplitSerializer.writeInstant(out, state.getStartTime());
        out.writeLong(state.getNextSplitId());
        writeSplits(out, state.getUnassignedSplits());
        writeSplits(out, state.getAssignedSplits());
        out.writeInt(state.getPendingMerges().size());
        for (PendingMerge merge : state.getPendingMerges()) {
            ChangeStreamPartitionSplitSerializer.writePartition(out, merge.getPartition());
            out.writeInt(merge.getContinuationTokens().size());
            for (ChangeStreamContinuationToken token : merge.getContinuationTokens()) {
                byte[] bytes = token.toByteString().toByteArray();
                out.writeInt(bytes.length);
                out.write(bytes);
            }
            ChangeStreamPartitionSplitSerializer.writeInstant(out, merge.getLowWatermark());
        }
        return out.getCopyOfBuffer();
    }

    private static byte[] serializeVersionTwo(BigtableChangeStreamEnumeratorState state)
            throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(1024);
        out.write(serializeVersionOne(state));
        out.writeInt(state.getMissingPartitions().size());
        for (MissingPartition missing : state.getMissingPartitions()) {
            ChangeStreamPartitionSplitSerializer.writePartition(out, missing.getPartition());
            ChangeStreamPartitionSplitSerializer.writeInstant(out, missing.getFirstObserved());
            ChangeStreamPartitionSplitSerializer.writeInstant(out, missing.getLowWatermark());
        }
        return out.getCopyOfBuffer();
    }

    private static void writeSplits(
            DataOutputSerializer out, List<ChangeStreamPartitionSplit> splits) throws IOException {
        out.writeInt(splits.size());
        for (ChangeStreamPartitionSplit split : splits) {
            ChangeStreamPartitionSplitSerializer.writeSplit(out, split);
        }
    }
}
