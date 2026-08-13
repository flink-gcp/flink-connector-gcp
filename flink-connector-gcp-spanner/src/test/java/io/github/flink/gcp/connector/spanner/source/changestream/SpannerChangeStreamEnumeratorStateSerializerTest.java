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

import org.apache.flink.core.memory.DataOutputSerializer;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpannerChangeStreamEnumeratorStateSerializerTest {

    @Test
    void roundTripsTheCompletePartitionLedger() throws Exception {
        Instant start = Instant.parse("2026-08-12T00:00:00Z");
        SpannerChangeStreamPartitionSplit initial =
                SpannerChangeStreamPartitionSplit.initial(start, null, 2_000)
                        .withLifecycleState(PartitionLifecycleState.FINISHED);
        SpannerChangeStreamPartitionSplit child =
                new SpannerChangeStreamPartitionSplit(
                        "child",
                        Collections.singletonList(initial.splitId()),
                        start.plusSeconds(1),
                        null,
                        2_000,
                        start.plusSeconds(2),
                        PartitionLifecycleState.RUNNING,
                        start.plusSeconds(2));
        SpannerChangeStreamEnumeratorState state =
                new SpannerChangeStreamEnumeratorState(
                        Arrays.asList(initial, child), start.plusSeconds(1).toEpochMilli());
        SpannerChangeStreamEnumeratorStateSerializer serializer =
                new SpannerChangeStreamEnumeratorStateSerializer();

        assertThat(serializer.deserialize(serializer.getVersion(), serializer.serialize(state)))
                .isEqualTo(state);
    }

    @Test
    void rejectsUnknownVersionsEmptyLedgersAndDuplicateSplitIds() throws Exception {
        SpannerChangeStreamEnumeratorStateSerializer serializer =
                new SpannerChangeStreamEnumeratorStateSerializer();
        SpannerChangeStreamPartitionSplit initial =
                SpannerChangeStreamPartitionSplit.initial(Instant.EPOCH, null, 2_000);
        byte[] bytes =
                serializer.serialize(
                        new SpannerChangeStreamEnumeratorState(Collections.singletonList(initial)));

        assertThatThrownBy(() -> serializer.deserialize(3, bytes))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("version 3");

        DataOutputSerializer empty = new DataOutputSerializer(4);
        empty.writeInt(0);
        empty.writeLong(Long.MIN_VALUE);
        assertThatThrownBy(
                        () ->
                                serializer.deserialize(
                                        serializer.getVersion(), empty.getCopyOfBuffer()))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("Corrupt")
                .hasRootCauseMessage("partitions must contain the initial partition ledger");

        DataOutputSerializer negative = new DataOutputSerializer(4);
        negative.writeInt(-1);
        assertThatThrownBy(
                        () ->
                                serializer.deserialize(
                                        serializer.getVersion(), negative.getCopyOfBuffer()))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("negative partition count -1");

        DataOutputSerializer corrupt = new DataOutputSerializer(256);
        corrupt.writeInt(2);
        SpannerChangeStreamPartitionSplitSerializer.writeSplit(corrupt, initial);
        SpannerChangeStreamPartitionSplitSerializer.writeSplit(corrupt, initial);
        corrupt.writeLong(Long.MIN_VALUE);
        assertThatThrownBy(
                        () ->
                                serializer.deserialize(
                                        serializer.getVersion(), corrupt.getCopyOfBuffer()))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("Corrupt")
                .hasRootCauseMessage(
                        "partition split ids must be unique, but change-stream-initial appeared twice");

        assertThatThrownBy(
                        () ->
                                new SpannerChangeStreamEnumeratorState(
                                        Arrays.asList(initial, initial)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("appeared twice");
    }

    @Test
    void rejectsAChildWhoseParentIsMissingOrUnfinished() {
        SpannerChangeStreamPartitionSplit initial =
                SpannerChangeStreamPartitionSplit.initial(Instant.EPOCH, null, 2_000)
                        .withLifecycleState(PartitionLifecycleState.RUNNING);
        SpannerChangeStreamPartitionSplit child =
                new SpannerChangeStreamPartitionSplit(
                        "child",
                        Collections.singletonList(initial.splitId()),
                        Instant.EPOCH,
                        null,
                        2_000,
                        Instant.EPOCH,
                        PartitionLifecycleState.SCHEDULED,
                        Instant.EPOCH);
        SpannerChangeStreamPartitionSplit orphan =
                new SpannerChangeStreamPartitionSplit(
                        "orphan",
                        Collections.singletonList("missing-parent"),
                        Instant.EPOCH,
                        null,
                        2_000,
                        Instant.EPOCH,
                        PartitionLifecycleState.CREATED,
                        Instant.EPOCH);

        assertThatThrownBy(
                        () ->
                                new SpannerChangeStreamEnumeratorState(
                                        Collections.singletonList(child)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must contain initial partition");
        assertThatThrownBy(
                        () ->
                                new SpannerChangeStreamEnumeratorState(
                                        Arrays.asList(initial, orphan)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("names missing parent");
        assertThatThrownBy(
                        () -> new SpannerChangeStreamEnumeratorState(Arrays.asList(initial, child)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("before parent")
                .hasMessageContaining("FINISHED");
    }

    @Test
    void readsVersionOneStateWithTheCompleteLedgerMinimum() throws Exception {
        Instant start = Instant.parse("2026-08-12T00:00:00Z");
        SpannerChangeStreamPartitionSplit initial =
                SpannerChangeStreamPartitionSplit.initial(start, null, 2_000)
                        .withLifecycleState(PartitionLifecycleState.FINISHED);
        SpannerChangeStreamPartitionSplit child =
                new SpannerChangeStreamPartitionSplit(
                        "child",
                        Collections.singletonList(initial.splitId()),
                        start.plusSeconds(1),
                        null,
                        2_000,
                        start.plusSeconds(2),
                        PartitionLifecycleState.RUNNING,
                        start.plusSeconds(2));
        DataOutputSerializer versionOne = new DataOutputSerializer(512);
        versionOne.writeInt(2);
        SpannerChangeStreamPartitionSplitSerializer.writeSplit(versionOne, initial);
        SpannerChangeStreamPartitionSplitSerializer.writeSplit(versionOne, child);

        SpannerChangeStreamEnumeratorState restored =
                new SpannerChangeStreamEnumeratorStateSerializer()
                        .deserialize(1, versionOne.getCopyOfBuffer());

        assertThat(restored.getSourceWatermark())
                .isEqualTo(child.getWatermark().toEpochMilli() - 1);
    }

    @Test
    void rejectsACheckpointedWatermarkAheadOfAnUnfinishedPartition() throws Exception {
        Instant start = Instant.parse("2026-08-12T00:00:00Z");
        SpannerChangeStreamPartitionSplit initial =
                SpannerChangeStreamPartitionSplit.initial(start, null, 2_000);
        DataOutputSerializer corrupt = new DataOutputSerializer(512);
        corrupt.writeInt(1);
        SpannerChangeStreamPartitionSplitSerializer.writeSplit(corrupt, initial);
        corrupt.writeLong(start.toEpochMilli());

        assertThatThrownBy(
                        () ->
                                new SpannerChangeStreamEnumeratorStateSerializer()
                                        .deserialize(2, corrupt.getCopyOfBuffer()))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("Corrupt")
                .hasRootCauseMessage(
                        "source watermark "
                                + start.toEpochMilli()
                                + " is ahead of complete-ledger frontier "
                                + (start.toEpochMilli() - 1));
    }

    @Test
    void rejectsCreatedPartitionsWhoseParentsFormACycle() {
        SpannerChangeStreamPartitionSplit initial =
                SpannerChangeStreamPartitionSplit.initial(Instant.EPOCH, null, 2_000)
                        .withLifecycleState(PartitionLifecycleState.FINISHED);
        SpannerChangeStreamPartitionSplit left = created("left", "change-stream-token:right");
        SpannerChangeStreamPartitionSplit right = created("right", left.splitId());

        assertThatThrownBy(
                        () ->
                                new SpannerChangeStreamEnumeratorState(
                                        Arrays.asList(initial, left, right)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("parent cycle")
                .hasMessageContaining("2 partition(s)");
    }

    private static SpannerChangeStreamPartitionSplit created(String token, String parentId) {
        return new SpannerChangeStreamPartitionSplit(
                token,
                Collections.singletonList(parentId),
                Instant.EPOCH,
                null,
                2_000,
                Instant.EPOCH,
                PartitionLifecycleState.CREATED,
                Instant.EPOCH);
    }
}
