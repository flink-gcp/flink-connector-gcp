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

import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.spanner.source.changestream.enumerator.SpannerChangeStreamCoordinatorClient;
import io.github.flink.gcp.connector.spanner.source.changestream.enumerator.SpannerChangeStreamSplitEnumerator;
import io.github.flink.gcp.connector.testutils.FakeSplitEnumeratorContext;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpannerChangeStreamEnumeratorStateSerializerTest {

    private static final int SERIALIZER_VERSION = 3;

    @Test
    void roundTripsCompactLineageAndFinishedParentProofs() throws Exception {
        Instant start = Instant.parse("2026-08-12T00:00:00Z");
        ChangeStreamPartitionSplit runningParent =
                child("running-parent", start, "released-ancestor")
                        .withLifecycleState(PartitionLifecycleState.RUNNING);
        ChangeStreamPartitionSplit merged =
                new ChangeStreamPartitionSplit(
                        "merged",
                        Arrays.asList("finished-parent", runningParent.splitId()),
                        start.plusSeconds(1),
                        null,
                        2_000,
                        start.plusSeconds(1),
                        PartitionLifecycleState.CREATED,
                        start.plusSeconds(1));
        SpannerChangeStreamEnumeratorState state =
                new SpannerChangeStreamEnumeratorState(
                        Arrays.asList(runningParent, merged),
                        Collections.singletonList("finished-parent"),
                        false,
                        start.toEpochMilli() - 1);
        SpannerChangeStreamEnumeratorStateSerializer serializer =
                new SpannerChangeStreamEnumeratorStateSerializer();

        SpannerChangeStreamEnumeratorState restored =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(state));

        assertThat(restored).isEqualTo(state);
        assertThat(restored.getPartitions())
                .extracting(ChangeStreamPartitionSplit::splitId)
                .containsExactly(runningParent.splitId(), merged.splitId());
        assertThat(restored.getFinishedParentProofs()).containsExactly("finished-parent");
    }

    @Test
    void roundTripsAnEmptyBoundedLedger() throws Exception {
        SpannerChangeStreamEnumeratorState state =
                new SpannerChangeStreamEnumeratorState(
                        Collections.emptyList(), Collections.emptySet(), true, 123L);
        SpannerChangeStreamEnumeratorStateSerializer serializer =
                new SpannerChangeStreamEnumeratorStateSerializer();

        SpannerChangeStreamEnumeratorState restored =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(state));

        assertThat(restored.getPartitions()).isEmpty();
        assertThat(restored.getFinishedParentProofs()).isEmpty();
        assertThat(restored.isBounded()).isTrue();
        assertThat(restored.getSourceWatermark()).isEqualTo(123L);
    }

    @Test
    void rejectsAnEmptyUnboundedLedger() {
        assertThatThrownBy(
                        () ->
                                new SpannerChangeStreamEnumeratorState(
                                        Collections.emptyList(),
                                        Collections.emptySet(),
                                        false,
                                        Long.MIN_VALUE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "unbounded compact partition ledger must contain an unfinished partition");
    }

    @Test
    void rejectsCompactStateWhoseBoundedFlagDisagreesWithItsPartitions() {
        ChangeStreamPartitionSplit bounded =
                ChangeStreamPartitionSplit.initial(
                        Instant.EPOCH, Instant.EPOCH.plusSeconds(1), 2_000);
        ChangeStreamPartitionSplit unbounded =
                ChangeStreamPartitionSplit.initial(Instant.EPOCH, null, 2_000);

        assertThatThrownBy(
                        () ->
                                new SpannerChangeStreamEnumeratorState(
                                        Collections.singletonList(bounded),
                                        Collections.emptySet(),
                                        false,
                                        Long.MIN_VALUE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("boundedness does not match checkpoint flag false");
        assertThatThrownBy(
                        () ->
                                new SpannerChangeStreamEnumeratorState(
                                        Collections.singletonList(unbounded),
                                        Collections.emptySet(),
                                        true,
                                        Long.MIN_VALUE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("boundedness does not match checkpoint flag true");
    }

    @Test
    void rejectsUnknownVersionsNegativeCountsAndDuplicateSplitIds() throws Exception {
        SpannerChangeStreamEnumeratorStateSerializer serializer =
                new SpannerChangeStreamEnumeratorStateSerializer();
        ChangeStreamPartitionSplit initial =
                ChangeStreamPartitionSplit.initial(Instant.EPOCH, null, 2_000);
        byte[] bytes =
                serializer.serialize(
                        new SpannerChangeStreamEnumeratorState(Collections.singletonList(initial)));

        assertThatThrownBy(() -> serializer.deserialize(SERIALIZER_VERSION + 1, bytes))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("version " + (SERIALIZER_VERSION + 1));

        DataOutputSerializer negative = new DataOutputSerializer(8);
        negative.writeBoolean(false);
        negative.writeInt(-1);
        assertThatThrownBy(
                        () ->
                                serializer.deserialize(
                                        serializer.getVersion(), negative.getCopyOfBuffer()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("negative partition count -1");

        DataOutputSerializer corrupt = versionThreeOutput(false, 2);
        ChangeStreamPartitionSplitSerializer.writeSplit(corrupt, initial);
        ChangeStreamPartitionSplitSerializer.writeSplit(corrupt, initial);
        corrupt.writeInt(0);
        corrupt.writeLong(Long.MIN_VALUE);
        assertThatThrownBy(
                        () ->
                                serializer.deserialize(
                                        serializer.getVersion(), corrupt.getCopyOfBuffer()))
                .isInstanceOf(IOException.class)
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
    void rejectsMissingDuplicateStaleAndLiveFinishedParentProofs() throws Exception {
        Instant start = Instant.parse("2026-08-12T00:00:00Z");
        ChangeStreamPartitionSplit created = child("child", start, "finished-parent");
        SpannerChangeStreamEnumeratorStateSerializer serializer =
                new SpannerChangeStreamEnumeratorStateSerializer();

        assertThatThrownBy(
                        () ->
                                new SpannerChangeStreamEnumeratorState(
                                        Collections.singletonList(created),
                                        Collections.emptySet(),
                                        false,
                                        start.toEpochMilli() - 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("names missing parent finished-parent");

        assertThatThrownBy(
                        () ->
                                new SpannerChangeStreamEnumeratorState(
                                        Collections.singletonList(created),
                                        Arrays.asList("finished-parent", "stale-parent"),
                                        false,
                                        start.toEpochMilli() - 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("stale proofs [stale-parent]");

        ChangeStreamPartitionSplit live =
                child("finished-parent", start, "released-ancestor")
                        .withLifecycleState(PartitionLifecycleState.RUNNING);
        assertThatThrownBy(
                        () ->
                                new SpannerChangeStreamEnumeratorState(
                                        Arrays.asList(live, created),
                                        Collections.singletonList(live.splitId()),
                                        false,
                                        start.toEpochMilli() - 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("is also a live partition");

        DataOutputSerializer duplicateProof = versionThreeOutput(false, 1);
        ChangeStreamPartitionSplitSerializer.writeSplit(duplicateProof, created);
        duplicateProof.writeInt(2);
        duplicateProof.writeUTF("finished-parent");
        duplicateProof.writeUTF("finished-parent");
        duplicateProof.writeLong(start.toEpochMilli() - 1);
        assertThatThrownBy(
                        () ->
                                serializer.deserialize(
                                        serializer.getVersion(), duplicateProof.getCopyOfBuffer()))
                .isInstanceOf(IOException.class)
                .hasRootCauseMessage("finished parent proof finished-parent appeared twice");
    }

    @Test
    void rejectsACompactLedgerContainingFinishedEntries() {
        ChangeStreamPartitionSplit finished =
                ChangeStreamPartitionSplit.initial(Instant.EPOCH, null, 2_000)
                        .withLifecycleState(PartitionLifecycleState.FINISHED);

        assertThatThrownBy(
                        () ->
                                new SpannerChangeStreamEnumeratorState(
                                        Collections.singletonList(finished),
                                        Collections.emptySet(),
                                        false,
                                        Long.MIN_VALUE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not contain FINISHED partition");
    }

    @Test
    void rejectsAChildWhoseLegacyParentIsMissingOrUnfinished() {
        ChangeStreamPartitionSplit initial =
                ChangeStreamPartitionSplit.initial(Instant.EPOCH, null, 2_000)
                        .withLifecycleState(PartitionLifecycleState.RUNNING);
        ChangeStreamPartitionSplit child =
                child("child", Instant.EPOCH, initial.splitId())
                        .withLifecycleState(PartitionLifecycleState.SCHEDULED);
        ChangeStreamPartitionSplit orphan = child("orphan", Instant.EPOCH, "missing-parent");

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
    void readsVersionOneStateAndDropsReleasedFinishedAncestors() throws Exception {
        Instant start = Instant.parse("2026-08-12T00:00:00Z");
        ChangeStreamPartitionSplit initial =
                ChangeStreamPartitionSplit.initial(start, null, 2_000)
                        .withLifecycleState(PartitionLifecycleState.FINISHED);
        ChangeStreamPartitionSplit child =
                child("child", start.plusSeconds(1), initial.splitId())
                        .withLifecycleState(PartitionLifecycleState.RUNNING)
                        .withProgress(start.plusSeconds(2), start.plusSeconds(2));
        DataOutputSerializer versionOne = new DataOutputSerializer(512);
        versionOne.writeInt(2);
        ChangeStreamPartitionSplitSerializer.writeSplit(versionOne, initial);
        ChangeStreamPartitionSplitSerializer.writeSplit(versionOne, child);

        SpannerChangeStreamEnumeratorState restored =
                new SpannerChangeStreamEnumeratorStateSerializer()
                        .deserialize(1, versionOne.getCopyOfBuffer());

        assertThat(restored.getPartitions()).containsExactly(child);
        assertThat(restored.getFinishedParentProofs()).isEmpty();
        assertThat(restored.getSourceWatermark())
                .isEqualTo(child.getWatermark().toEpochMilli() - 1);
    }

    @Test
    void readsVersionTwoStateAndRetainsOnlyNeededFinishedParentProofs() throws Exception {
        Instant start = Instant.parse("2026-08-12T00:00:00Z");
        ChangeStreamPartitionSplit initial =
                ChangeStreamPartitionSplit.initial(start, null, 2_000)
                        .withLifecycleState(PartitionLifecycleState.FINISHED);
        ChangeStreamPartitionSplit waiting =
                child("waiting", start.plusSeconds(1), initial.splitId());
        DataOutputSerializer versionTwo = new DataOutputSerializer(512);
        versionTwo.writeInt(2);
        ChangeStreamPartitionSplitSerializer.writeSplit(versionTwo, initial);
        ChangeStreamPartitionSplitSerializer.writeSplit(versionTwo, waiting);
        versionTwo.writeLong(start.toEpochMilli() - 1);

        SpannerChangeStreamEnumeratorState restored =
                new SpannerChangeStreamEnumeratorStateSerializer()
                        .deserialize(2, versionTwo.getCopyOfBuffer());

        assertThat(restored.getPartitions()).containsExactly(waiting);
        assertThat(restored.getFinishedParentProofs()).containsExactly(initial.splitId());
    }

    @Test
    void readsVersionTwoStateWhenScheduledSiblingSharesARetainedProof() throws Exception {
        Instant start = Instant.parse("2026-08-12T00:00:00Z");
        ChangeStreamPartitionSplit initial =
                ChangeStreamPartitionSplit.initial(start, null, 2_000)
                        .withLifecycleState(PartitionLifecycleState.FINISHED);
        ChangeStreamPartitionSplit firstFinished =
                child("first-finished", start.plusSeconds(1), initial.splitId())
                        .withLifecycleState(PartitionLifecycleState.FINISHED);
        ChangeStreamPartitionSplit secondRunning =
                child("second-running", start.plusSeconds(1), initial.splitId())
                        .withLifecycleState(PartitionLifecycleState.RUNNING);
        ChangeStreamPartitionSplit scheduled =
                new ChangeStreamPartitionSplit(
                        "scheduled",
                        Arrays.asList(initial.splitId(), firstFinished.splitId()),
                        start.plusSeconds(2),
                        null,
                        2_000,
                        start.plusSeconds(2),
                        PartitionLifecycleState.SCHEDULED,
                        start.plusSeconds(2));
        ChangeStreamPartitionSplit waiting =
                new ChangeStreamPartitionSplit(
                        "waiting",
                        Arrays.asList(initial.splitId(), secondRunning.splitId()),
                        start.plusSeconds(2),
                        null,
                        2_000,
                        start.plusSeconds(2),
                        PartitionLifecycleState.CREATED,
                        start.plusSeconds(2));
        DataOutputSerializer versionTwo = new DataOutputSerializer(1024);
        versionTwo.writeInt(5);
        for (ChangeStreamPartitionSplit partition :
                Arrays.asList(initial, firstFinished, secondRunning, scheduled, waiting)) {
            ChangeStreamPartitionSplitSerializer.writeSplit(versionTwo, partition);
        }
        versionTwo.writeLong(Long.MIN_VALUE);
        SpannerChangeStreamEnumeratorStateSerializer serializer =
                new SpannerChangeStreamEnumeratorStateSerializer();

        SpannerChangeStreamEnumeratorState restored =
                serializer.deserialize(2, versionTwo.getCopyOfBuffer());

        assertThat(restored.getPartitions()).containsExactly(secondRunning, scheduled, waiting);
        assertThat(restored.getFinishedParentProofs()).containsExactly(initial.splitId());
        assertThat(serializer.deserialize(serializer.getVersion(), serializer.serialize(restored)))
                .isEqualTo(restored);
    }

    @Test
    void readsCompletedBoundedVersionTwoStateAsAnEmptyBoundedLedger() throws Exception {
        Instant start = Instant.parse("2026-08-12T00:00:00Z");
        ChangeStreamPartitionSplit initial =
                ChangeStreamPartitionSplit.initial(start, start.plusSeconds(10), 2_000)
                        .withLifecycleState(PartitionLifecycleState.FINISHED);
        DataOutputSerializer versionTwo = new DataOutputSerializer(256);
        versionTwo.writeInt(1);
        ChangeStreamPartitionSplitSerializer.writeSplit(versionTwo, initial);
        versionTwo.writeLong(start.plusSeconds(10).toEpochMilli());

        SpannerChangeStreamEnumeratorState restored =
                new SpannerChangeStreamEnumeratorStateSerializer()
                        .deserialize(2, versionTwo.getCopyOfBuffer());

        assertThat(restored.getPartitions()).isEmpty();
        assertThat(restored.getFinishedParentProofs()).isEmpty();
        assertThat(restored.isBounded()).isTrue();
        assertThat(restored.getSourceWatermark()).isEqualTo(start.plusSeconds(10).toEpochMilli());

        FakeSplitEnumeratorContext<ChangeStreamPartitionSplit> context =
                new FakeSplitEnumeratorContext<>(1);
        context.registerReader(0);
        SpannerChangeStreamSplitEnumerator enumerator =
                new SpannerChangeStreamSplitEnumerator(
                        context,
                        () ->
                                new SpannerChangeStreamCoordinatorClient() {
                                    @Override
                                    public Duration initialize() {
                                        return Duration.ofDays(7);
                                    }

                                    @Override
                                    public void close() {}
                                },
                        StartPosition.latest(),
                        null,
                        null,
                        2_000,
                        restored);
        enumerator.start();
        enumerator.handleSplitRequest(0, "localhost");
        context.runAsyncCalls();

        assertThat(context.events()).containsExactly("noMoreSplits:0");
        enumerator.close();
    }

    @Test
    void rejectsACheckpointedWatermarkAheadOfAnUnfinishedPartition() throws Exception {
        Instant start = Instant.parse("2026-08-12T00:00:00Z");
        ChangeStreamPartitionSplit initial = ChangeStreamPartitionSplit.initial(start, null, 2_000);
        DataOutputSerializer corrupt = new DataOutputSerializer(512);
        corrupt.writeInt(1);
        ChangeStreamPartitionSplitSerializer.writeSplit(corrupt, initial);
        corrupt.writeLong(start.toEpochMilli());

        assertThatThrownBy(
                        () ->
                                new SpannerChangeStreamEnumeratorStateSerializer()
                                        .deserialize(2, corrupt.getCopyOfBuffer()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Corrupt")
                .hasRootCauseMessage(
                        "source watermark "
                                + start.toEpochMilli()
                                + " is ahead of unfinished-ledger frontier "
                                + (start.toEpochMilli() - 1));
    }

    @Test
    void rejectsCompactCreatedPartitionsWhoseLiveParentsFormACycle() {
        ChangeStreamPartitionSplit left = created("left", "change-stream-token:right");
        ChangeStreamPartitionSplit right = created("right", left.splitId());

        assertThatThrownBy(
                        () ->
                                new SpannerChangeStreamEnumeratorState(
                                        Arrays.asList(left, right),
                                        Collections.emptySet(),
                                        false,
                                        Long.MIN_VALUE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("parent cycle")
                .hasMessageContaining("2 partition(s)");
    }

    private static DataOutputSerializer versionThreeOutput(boolean bounded, int partitionCount)
            throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(512);
        out.writeBoolean(bounded);
        out.writeInt(partitionCount);
        return out;
    }

    private static ChangeStreamPartitionSplit child(String token, Instant start, String parentId) {
        return new ChangeStreamPartitionSplit(
                token,
                Collections.singletonList(parentId),
                start,
                null,
                2_000,
                start,
                PartitionLifecycleState.CREATED,
                start);
    }

    private static ChangeStreamPartitionSplit created(String token, String parentId) {
        return child(token, Instant.EPOCH, parentId);
    }
}
