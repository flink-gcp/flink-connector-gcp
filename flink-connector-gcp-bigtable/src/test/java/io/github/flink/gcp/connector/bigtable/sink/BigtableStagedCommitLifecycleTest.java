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

package io.github.flink.gcp.connector.bigtable.sink;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessageTypeInfo;
import org.apache.flink.streaming.runtime.operators.sink.CommitterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Value;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableCommittable;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRequestOptions;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableStagedSink;
import io.github.flink.gcp.connector.testutils.StubWriterInitContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Production writer, metadata clients, SDK transport and Flink collector recovery on loopback. */
class BigtableStagedCommitLifecycleTest {
    static final String TABLE = "projects/p/instances/i/tables/t";
    private final StagedMutationTestSink.Probe probe;
    private final StagedRpcTestService service;
    private final BigtableStagedSink<StagedMutationTestSink.Input> sink;

    BigtableStagedCommitLifecycleTest() throws Exception {
        service = new StagedRpcTestService();
        probe = service.probe;
        sink = sink(100_000, 64L << 20);
    }

    private BigtableStagedSink<StagedMutationTestSink.Input> sink(int maxEntries, long maxBytes) {
        return (BigtableStagedSink<StagedMutationTestSink.Input>)
                BigtableSink.<StagedMutationTestSink.Input>builder()
                        .destinationResolver(
                                (input, context) ->
                                        TableDestination.of(
                                                "p",
                                                "i",
                                                input.table().substring(TABLE.length() - 1)))
                        .serializer(
                                (input, context) ->
                                        RowMutationEntry.createFromMutationUnsafe(
                                                input.row(),
                                                com.google.cloud.bigtable.data.v2.models.Mutation
                                                        .fromProtoUnsafe(input.mutations())))
                        .appProfileId("original-profile")
                        .emulatorEndpoint("localhost:" + service.server.getPort())
                        .deliveryGuarantee(BigtableDeliveryGuarantee.EXACTLY_ONCE)
                        .stagedOptions(
                                BigtableStagedOptions.builder()
                                        .markerFamily("flink_commit")
                                        .maxStagedEntries(maxEntries)
                                        .maxStagedBytes(maxBytes)
                                        .requestOptions(
                                                BigtableRequestOptions.builder()
                                                        .maxInFlightRequests(1)
                                                        .build())
                                        .build())
                        .build();
    }

    @AfterEach
    void cleanup() throws Exception {
        service.close();
    }

    @Test
    void completionAppliesDistinctSameRowInputsAndRepeatedCompletionDoesNothing() throws Exception {
        try (var writer = writer(1, 0);
                var committer = committer(1, 0, true)) {
            writer.open();
            committer.open();
            writer.processElement(increment("r", 2), 0);
            writer.processElement(increment("r", 3), 0);
            barrier(writer, committer, 1);
            writer.snapshot(1, 0);
            committer.snapshot(1, 0);
            assertThat(probe.sent).isEmpty();
            committer.notifyOfCompletedCheckpoint(1);
            assertThat(probe.sum(TABLE, "r")).isEqualTo(5);
            assertThat(probe.applied).isEqualTo(2);
            assertThat(probe.sent.get(0).getPredicateFilter())
                    .isNotEqualTo(probe.sent.get(1).getPredicateFilter());
            committer.notifyOfCompletedCheckpoint(1);
            assertThat(probe.sent).hasSize(2);
        }
    }

    @Test
    void responseLossMidCommitRestoresOriginalRequestsAndAppliesOnlyMissingEffects()
            throws Exception {
        OperatorSubtaskState state;
        try (var writer = writer(1, 0);
                var committer = committer(1, 0, true)) {
            writer.open();
            committer.open();
            writer.processElement(increment("r", 2), 0);
            writer.processElement(increment("r", 3), 0);
            barrier(writer, committer, 1);
            state = committer.snapshot(1, 0);
            probe.loseNextAnswer = true;
            assertThatThrownBy(() -> committer.notifyOfCompletedCheckpoint(1))
                    .isInstanceOf(IOException.class)
                    .hasStackTraceContaining("response lost");
            assertThat(probe.sum(TABLE, "r")).isEqualTo(2);
        }
        CheckAndMutateRowRequest lost = probe.sent.get(0);
        try (var restored = committer(1, 0, true)) {
            restored.setRestoredCheckpointId(1);
            restored.initializeState(state);
            assertThat(probe.sent.get(1).toByteArray()).isEqualTo(lost.toByteArray());
            assertThat(probe.sum(TABLE, "r")).isEqualTo(5);
            assertThat(probe.applied).isEqualTo(2);
            assertThat(probe.deduplicated).isEqualTo(1);
        }
        try (var restoredAgain = committer(1, 0, true)) {
            restoredAgain.setRestoredCheckpointId(1);
            restoredAgain.initializeState(state);
            assertThat(probe.sum(TABLE, "r")).isEqualTo(5);
            assertThat(probe.applied).isEqualTo(2);
            assertThat(probe.deduplicated).isEqualTo(3);
        }
    }

    @Test
    void restoreDiscardsAnUncompletedTailAndSourceReplayStagesItOnce() throws Exception {
        OperatorSubtaskState state;
        try (var writer = writer(1, 0);
                var committer = committer(1, 0, true)) {
            writer.open();
            committer.open();
            writer.processElement(increment("r", 2), 0);
            barrier(writer, committer, 1);
            state = committer.snapshot(1, 0);
            committer.notifyOfCompletedCheckpoint(1);
            writer.processElement(increment("r", 3), 0);
            barrier(writer, committer, 2);
            committer.snapshot(2, 0);
            assertThat(probe.sum(TABLE, "r")).isEqualTo(2);
        }
        try (var writer = writer(1, 0);
                var restored = committer(1, 0, true)) {
            writer.setRestoredCheckpointId(1);
            writer.open();
            restored.setRestoredCheckpointId(1);
            restored.initializeState(state);
            restored.open();
            assertThat(probe.sum(TABLE, "r")).isEqualTo(2);
            writer.processElement(increment("r", 3), 0);
            barrier(writer, restored, 3);
            restored.notifyOfCompletedCheckpoint(3);
            assertThat(probe.sum(TABLE, "r")).isEqualTo(5);
        }
    }

    @Test
    void nextCompletedCheckpointIncludesTheAbortedIntervals() throws Exception {
        try (var writer = writer(1, 0);
                var committer = committer(1, 0, true)) {
            writer.open();
            committer.open();
            writer.processElement(increment("r", 2), 0);
            barrier(writer, committer, 1);
            committer.getOperator().notifyCheckpointAborted(1);
            writer.processElement(increment("r", 3), 0);
            barrier(writer, committer, 2);
            assertThat(probe.sent).isEmpty();
            committer.notifyOfCompletedCheckpoint(2);
            assertThat(probe.sum(TABLE, "r")).isEqualTo(5);
        }
    }

    @Test
    void rescalingPreservesIdentitiesAcrossOldSubtasks() throws Exception {
        OperatorSubtaskState[] states = new OperatorSubtaskState[2];
        for (int subtask = 0; subtask < 2; subtask++) {
            try (var writer = writer(2, subtask);
                    var committer = committer(2, subtask, true)) {
                writer.open();
                committer.open();
                writer.processElement(increment("r", subtask + 2), 0);
                barrier(writer, committer, 1);
                states[subtask] = committer.snapshot(1, 0);
                committer.notifyOfCompletedCheckpoint(1);
            }
        }
        OperatorSubtaskState combined = AbstractStreamOperatorTestHarness.repackageState(states);
        for (int parallelism : new int[] {1, 3}) {
            for (int subtask = 0; subtask < parallelism; subtask++) {
                OperatorSubtaskState assigned =
                        AbstractStreamOperatorTestHarness.repartitionOperatorState(
                                combined, 128, 2, parallelism, subtask);
                try (var restored = committer(parallelism, subtask, true)) {
                    restored.setRestoredCheckpointId(1);
                    restored.initializeState(assigned);
                }
            }
            assertThat(probe.sum(TABLE, "r")).isEqualTo(5);
            assertThat(probe.applied).isEqualTo(2);
        }
        assertThat(probe.deduplicated).isEqualTo(4);
    }

    @Test
    void endInputWaitsForCompletionOnlyWithCheckpointing() throws Exception {
        for (boolean checkpointing : new boolean[] {true, false}) {
            probe.sent.clear();
            try (var writer = writer(1, 0);
                    var committer = committer(1, 0, checkpointing)) {
                writer.open();
                committer.open();
                writer.processElement(increment("r", 1), 0);
                writer.endInput();
                forward(writer, committer);
                committer.endInput();
                assertThat(probe.sent).hasSize(checkpointing ? 0 : 1);
                if (checkpointing) {
                    committer.notifyOfCompletedCheckpoint(1);
                    assertThat(probe.sent).hasSize(1);
                }
            }
        }
    }

    @Test
    void discardingOwnedStateLosesUncommittedEffects() throws Exception {
        try (var writer = writer(1, 0);
                var committer = committer(1, 0, true)) {
            writer.open();
            committer.open();
            writer.processElement(increment("r", 2), 0);
            barrier(writer, committer, 1);
            committer.snapshot(1, 0);
        }
        // Model deliberately discarding the checkpoint's collector while keeping source progress.
        try (var replacement = committer(1, 0, true)) {
            replacement.open();
            replacement.notifyOfCompletedCheckpoint(2);
            assertThat(probe.sent).isEmpty();
        }
    }

    @Test
    void markerMutationIsLastAndUnsupportedMutationsFailBeforeStaging() throws Exception {
        var writer = sink.createWriter(new StubWriterInitContext(0));
        writer.write(increment("r", 2), null);
        CheckAndMutateRowRequest request = writer.prepareCommit().iterator().next().getRequest();
        assertThat(request.getTrueMutationsList()).isEmpty();
        assertThat(request.getFalseMutationsCount()).isEqualTo(2);
        assertThat(request.getFalseMutations(0)).isEqualTo(increment("r", 2).mutations().get(0));
        Mutation.SetCell marker = request.getFalseMutations(1).getSetCell();
        assertThat(marker.getFamilyName()).isEqualTo("flink_commit");
        assertThat(marker.getColumnQualifier().toStringUtf8()).matches("[0-9a-f]{32}");
        assertThat(marker.getTimestampMicros()).isZero();
        assertThat(
                        request.getPredicateFilter()
                                .getChain()
                                .getFilters(1)
                                .getColumnQualifierRegexFilter())
                .isEqualTo(marker.getColumnQualifier());
        for (Mutation mutation :
                List.of(
                        Mutation.newBuilder()
                                .setDeleteFromRow(Mutation.DeleteFromRow.getDefaultInstance())
                                .build(),
                        Mutation.newBuilder()
                                .setDeleteFromFamily(
                                        Mutation.DeleteFromFamily.newBuilder()
                                                .setFamilyName("flink_commit"))
                                .build(),
                        Mutation.newBuilder().setSetCell(marker).build())) {
            assertThatThrownBy(
                            () ->
                                    writer.write(
                                            new StagedMutationTestSink.Input(
                                                    TABLE,
                                                    ByteString.copyFromUtf8("r"),
                                                    List.of(mutation)),
                                            null))
                    .isInstanceOf(IOException.class);
        }
        assertThat(writer.prepareCommit()).isEmpty();
    }

    @Test
    void stagingCapsFailSynchronouslyAndPrepareCommitReleasesTheBudget() throws Exception {
        for (var writer :
                List.of(
                        sink(1, Long.MAX_VALUE).createWriter(new StubWriterInitContext(0)),
                        sink(10, 800).createWriter(new StubWriterInitContext(0)))) {
            writer.write(increment("r", 2), null);
            assertThatThrownBy(() -> writer.write(increment("r", 3), null))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("maxStagedEntries=")
                    .hasMessageContaining("maxStagedBytes=");
            assertThat(writer.prepareCommit()).hasSize(1);
            writer.write(increment("r", 3), null);
            assertThat(writer.prepareCommit()).hasSize(1);
        }
    }

    @Test
    void restoringAnOlderSnapshotDoesNotRollBackAlreadyCommittedRows() throws Exception {
        OperatorSubtaskState old;
        try (var writer = writer(1, 0);
                var committer = committer(1, 0, true)) {
            writer.open();
            committer.open();
            writer.processElement(increment("r", 2), 0);
            barrier(writer, committer, 1);
            old = committer.snapshot(1, 0);
            committer.notifyOfCompletedCheckpoint(1);
            writer.processElement(increment("r", 3), 0);
            barrier(writer, committer, 2);
            committer.notifyOfCompletedCheckpoint(2);
        }
        try (var restored = committer(1, 0, true)) {
            restored.setRestoredCheckpointId(1);
            restored.initializeState(old);
            assertThat(probe.sum(TABLE, "r")).isEqualTo(5);
        }
    }

    @Test
    void replayFromBeforeANotifiedSavepointStagesNewIdentities() throws Exception {
        OperatorSubtaskState checkpoint;
        try (var writer = writer(1, 0);
                var committer = committer(1, 0, true)) {
            writer.open();
            committer.open();
            writer.processElement(increment("r", 2), 0);
            barrier(writer, committer, 1);
            checkpoint = committer.snapshot(1, 0);
            committer.notifyOfCompletedCheckpoint(1);
            writer.processElement(increment("r", 3), 0);
            barrier(writer, committer, 2);
            // Simulate a synchronous savepoint's completion notification, followed by failover
            // to checkpoint 1. This harness does not exercise the JobManager's selection rule.
            committer.notifyOfCompletedCheckpoint(2);
        }
        try (var writer = writer(1, 0);
                var restored = committer(1, 0, true)) {
            writer.setRestoredCheckpointId(1);
            writer.open();
            restored.setRestoredCheckpointId(1);
            restored.initializeState(checkpoint);
            restored.open();
            writer.processElement(increment("r", 3), 0);
            barrier(writer, restored, 3);
            restored.notifyOfCompletedCheckpoint(3);
            assertThat(probe.sum(TABLE, "r")).isEqualTo(8);
            assertThat(probe.applied).isEqualTo(3);
        }
    }

    @Test
    void stateFormatRejectsOtherVersionsAndPreservesResolvedDestinationAndTimestamp()
            throws Exception {
        var writer = sink.createWriter(new StubWriterInitContext(0));
        Mutation cell =
                Mutation.newBuilder()
                        .setSetCell(
                                Mutation.SetCell.newBuilder()
                                        .setFamilyName("data")
                                        .setColumnQualifier(ByteString.copyFromUtf8("v"))
                                        .setTimestampMicros(123_000)
                                        .setValue(ByteString.copyFromUtf8("payload")))
                        .build();
        writer.write(
                new StagedMutationTestSink.Input(
                        TABLE + "2", ByteString.copyFromUtf8("r"), List.of(cell)),
                null);
        CheckAndMutateRowRequest request = writer.prepareCommit().iterator().next().getRequest();
        var serializer = sink.getCommittableSerializer();
        byte[] bytes = serializer.serialize(new BigtableCommittable(request));
        assertThat(serializer.deserialize(1, bytes).getRequest()).isEqualTo(request);
        assertThat(request.getTableName()).isEqualTo(TABLE + "2");
        assertThat(request.getFalseMutations(0)).isEqualTo(cell);
        assertThatThrownBy(() -> serializer.deserialize(2, bytes))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unsupported Bigtable committable version");
    }

    @Test
    void restoredCollectorChecksRemotePolicyBeforeSendingDuringInitializeState() throws Exception {
        OperatorSubtaskState state;
        try (var writer = writer(1, 0);
                var committer = committer(1, 0, true)) {
            writer.open();
            committer.open();
            writer.processElement(increment("r", 2), 0);
            barrier(writer, committer, 1);
            state = committer.snapshot(1, 0);
        }
        service.invalidProfile = true;
        try (var restored = committer(1, 0, true)) {
            restored.setRestoredCheckpointId(1);
            assertThatThrownBy(() -> restored.initializeState(state))
                    .hasStackTraceContaining("transactional");
        }
        assertThat(service.metadata)
                .contains("projects/p/instances/i/appProfiles/original-profile");
        assertThat(probe.sent).isEmpty();
    }

    static StagedMutationTestSink.Input increment(String row, long value) {
        Mutation add =
                Mutation.newBuilder()
                        .setAddToCell(
                                Mutation.AddToCell.newBuilder()
                                        .setFamilyName("agg")
                                        .setColumnQualifier(
                                                Value.newBuilder()
                                                        .setRawValue(
                                                                ByteString.copyFromUtf8("count")))
                                        .setTimestamp(Value.newBuilder().setRawTimestampMicros(0))
                                        .setInput(Value.newBuilder().setIntValue(value)))
                        .build();
        return new StagedMutationTestSink.Input(TABLE, ByteString.copyFromUtf8(row), List.of(add));
    }

    OneInputStreamOperatorTestHarness<
                    StagedMutationTestSink.Input, CommittableMessage<BigtableCommittable>>
            writer(int parallelism, int index) throws Exception {
        var harness =
                new OneInputStreamOperatorTestHarness<
                        StagedMutationTestSink.Input, CommittableMessage<BigtableCommittable>>(
                        new SinkWriterOperatorFactory<>(sink), 128, parallelism, index);
        harness.setup(
                CommittableMessageTypeInfo.of(sink::getCommittableSerializer)
                        .createSerializer(new SerializerConfigImpl()));
        return harness;
    }

    OneInputStreamOperatorTestHarness<
                    CommittableMessage<BigtableCommittable>,
                    CommittableMessage<BigtableCommittable>>
            committer(int parallelism, int index, boolean checkpointing) throws Exception {
        return new OneInputStreamOperatorTestHarness<>(
                new CommitterOperatorFactory<>(sink, false, checkpointing),
                128,
                parallelism,
                index);
    }

    static void barrier(
            OneInputStreamOperatorTestHarness<
                            StagedMutationTestSink.Input, CommittableMessage<BigtableCommittable>>
                    writer,
            OneInputStreamOperatorTestHarness<
                            CommittableMessage<BigtableCommittable>,
                            CommittableMessage<BigtableCommittable>>
                    committer,
            long checkpoint)
            throws Exception {
        writer.getOperator().prepareSnapshotPreBarrier(checkpoint);
        forward(writer, committer);
    }

    static <T, C> void forward(
            OneInputStreamOperatorTestHarness<T, CommittableMessage<C>> writer,
            OneInputStreamOperatorTestHarness<CommittableMessage<C>, CommittableMessage<C>>
                    committer)
            throws Exception {
        for (var message : writer.extractOutputValues()) {
            committer.processElement(new StreamRecord<>(message));
        }
        writer.getOutput().clear();
    }
}
