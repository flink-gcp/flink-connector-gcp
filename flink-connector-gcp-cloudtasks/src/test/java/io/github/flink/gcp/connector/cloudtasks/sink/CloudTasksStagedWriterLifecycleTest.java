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

package io.github.flink.gcp.connector.cloudtasks.sink;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.SupportsWriterState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessageTypeInfo;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.runtime.operators.sink.CommitterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Modifier;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises production staging and serialization through real Flink operators. The recording
 * committer observes ownership only; RPC authorization and service recovery belong to the next
 * stage.
 */
class CloudTasksStagedWriterLifecycleTest {
    private final String runId = UUID.randomUUID().toString();
    private final StagedTaskTestSink.Probe probe = new StagedTaskTestSink.Probe();
    private final StagedTaskTestSink sink = new StagedTaskTestSink(runId);

    CloudTasksStagedWriterLifecycleTest() {
        StagedTaskTestSink.PROBES.put(runId, probe);
    }

    @AfterEach
    void removeProbe() {
        StagedTaskTestSink.PROBES.remove(runId);
    }

    @Test
    void preBarrierTransfersProductionEnvelopesAndOnlyCompletionCommits() throws Exception {
        try (var writer = writer(1, 0);
                var committer = committer(1, 0)) {
            writer.open();
            committer.open();
            writer.processElement("body", 0);
            assertThat(writer.extractOutputValues()).isEmpty();
            writer.getOperator().prepareSnapshotPreBarrier(1);
            var emitted = envelopes(writer);
            assertThat(emitted).hasSize(1);
            assertThat(emitted.get(0).getTaskBytes())
                    .isEqualTo(emitted.get(0).parseTask().toByteString());
            assertThat(writer.snapshot(1, 0).hasState()).isFalse();
            forward(writer, committer);
            assertThat(committer.snapshot(1, 0).hasState()).isTrue();
            assertThat(probe.committed).isEmpty();
            committer.notifyOfCompletedCheckpoint(1);
            assertThat(probe.committed).containsExactlyElementsOf(emitted);
            committer.notifyOfCompletedCheckpoint(1);
            assertThat(probe.committed).hasSize(1);
        }
    }

    @Test
    void restoreUsesOriginalBytesAndDeadlineWithoutRunningUserCodeOrIdentityAgain()
            throws Exception {
        OperatorSubtaskState snapshot;
        List<CloudTasksCommittable> original;
        try (var writer = writer(1, 0);
                var committer = committer(1, 0)) {
            writer.open();
            committer.open();
            writer.processElement("same", 0);
            probe.clockMillis = 2000;
            writer.processElement("same", 0);
            writer.getOperator().prepareSnapshotPreBarrier(1);
            original = envelopes(writer);
            forward(writer, committer);
            snapshot = committer.snapshot(1, 0);
            committer.notifyOfCompletedCheckpoint(1);
        }
        assertThat(probe.committed).containsExactlyElementsOf(original);
        probe.committed.clear();
        probe.clockMillis = 3000;
        try (var restored = committer(1, 0)) {
            restored.setRestoredCheckpointId(1);
            restored.initializeState(snapshot);
            // The real operator restores and commits before open().
            assertThat(probe.committed).containsExactlyElementsOf(original);
            assertThat(probe.serialized).isEqualTo(2);
            assertThat(probe.identities).isEqualTo(2);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2})
    void crashAroundTransferAcceptsReplayOnlyFromTheLastCompletedCheckpoint(int crashPoint)
            throws Exception {
        OperatorSubtaskState lastCompleted;
        try (var empty = committer(1, 0)) {
            empty.open();
            lastCompleted = empty.snapshot(1, 0);
            empty.notifyOfCompletedCheckpoint(1);
        }
        String discardedName =
                CloudTasksCommittableSerializerTest.QUEUE + "/tasks/" + "0".repeat(31) + "1";
        try (var writer = writer(1, 0);
                var doomed = committer(1, 0)) {
            writer.open();
            doomed.open();
            writer.processElement("replayed", 0);
            assertThat(probe.identities).isEqualTo(1);
            assertThat(writer.extractOutputValues()).isEmpty();
            if (crashPoint >= 1) {
                writer.getOperator().prepareSnapshotPreBarrier(2);
                assertThat(envelopes(writer).get(0).parseTask().getName()).isEqualTo(discardedName);
            }
            if (crashPoint == 2) {
                forward(writer, doomed);
                assertThat(doomed.snapshot(2, 0).hasState()).isTrue();
            }
            // Lose the writer before emission, after emission, or after an uncompleted snapshot.
        }
        assertThat(probe.committed).isEmpty();
        try (var restored = committer(1, 0);
                var replay = writer(1, 0)) {
            restored.setRestoredCheckpointId(1);
            restored.initializeState(lastCompleted);
            restored.open();
            replay.open();
            replay.processElement("replayed", 0);
            replay.getOperator().prepareSnapshotPreBarrier(2);
            var owned = envelopes(replay);
            assertThat(owned.get(0).parseTask().getName()).isNotEqualTo(discardedName);
            forward(replay, restored);
            restored.snapshot(2, 0);
            restored.notifyOfCompletedCheckpoint(2);
            assertThat(probe.committed).containsExactlyElementsOf(owned);
        }
    }

    @Test
    void failedCheckpointBatchesStayInTheCollectorUntilALaterOwnedCheckpoint() throws Exception {
        OperatorSubtaskState snapshot;
        try (var writer = writer(1, 0);
                var committer = committer(1, 0)) {
            writer.open();
            committer.open();
            for (int checkpoint = 1; checkpoint <= 3; checkpoint++) {
                writer.processElement("batch-" + checkpoint, 0);
                writer.getOperator().prepareSnapshotPreBarrier(checkpoint);
                forward(writer, committer);
                committer.snapshot(checkpoint, 0);
                assertThat(probe.committed).isEmpty();
            }
            snapshot = committer.snapshot(3, 0);
        }
        try (var restored = committer(1, 0)) {
            restored.setRestoredCheckpointId(3);
            restored.initializeState(snapshot);
            assertThat(probe.committed).hasSize(3);
            assertThat(probe.committed)
                    .extracting(e -> e.parseTask().getHttpRequest().getBody().toStringUtf8())
                    .containsExactly("batch-1", "batch-2", "batch-3");
        }
    }

    @Test
    void rescalingTwoCommittersToOneAndThreePreservesTheCompleteEnvelopeSet() throws Exception {
        OperatorSubtaskState[] states = new OperatorSubtaskState[2];
        var staged = new java.util.ArrayList<CloudTasksCommittable>();
        for (int subtask = 0; subtask < 2; subtask++) {
            try (var writer = writer(2, subtask);
                    var committer = committer(2, subtask)) {
                writer.open();
                committer.open();
                writer.processElement("task-" + subtask, 0);
                writer.getOperator().prepareSnapshotPreBarrier(1);
                staged.addAll(envelopes(writer));
                forward(writer, committer);
                states[subtask] = committer.snapshot(1, 0);
            }
        }
        var combined = AbstractStreamOperatorTestHarness.repackageState(states);
        for (int parallelism : new int[] {1, 3}) {
            probe.committed.clear();
            for (int subtask = 0; subtask < parallelism; subtask++) {
                var state =
                        AbstractStreamOperatorTestHarness.repartitionOperatorState(
                                combined, 128, 2, parallelism, subtask);
                try (var restored = committer(parallelism, subtask)) {
                    restored.setRestoredCheckpointId(1);
                    restored.initializeState(state);
                }
            }
            assertThat(probe.committed).containsExactlyInAnyOrderElementsOf(staged);
            assertThat(probe.identities).isEqualTo(2);
            assertThat(probe.serialized).isEqualTo(2);
        }
    }

    @Test
    void endInputWaitsForCompletionAndClosingUncompletedWorkCommitsNothing() throws Exception {
        try (var writer = writer(1, 0);
                var committer = committer(1, 0)) {
            writer.open();
            committer.open();
            writer.processElement("tail", 0);
            writer.endInput();
            var tail = envelopes(writer);
            forward(writer, committer);
            committer.endInput();
            assertThat(probe.committed).isEmpty();
            committer.snapshot(1, 0);
            committer.notifyOfCompletedCheckpoint(1);
            assertThat(probe.committed).containsExactlyElementsOf(tail);
        }
        probe.committed.clear();
        try (var writer = writer(1, 0);
                var committer = committer(1, 0)) {
            writer.open();
            committer.open();
            writer.processElement("canceled", 0);
            writer.getOperator().prepareSnapshotPreBarrier(2);
            forward(writer, committer);
            committer.snapshot(2, 0);
        }
        assertThat(probe.committed).isEmpty();
    }

    @Test
    void emptyCollectorRestoresAndThePublicBuilderDefaultsToTheEagerMode() throws Exception {
        OperatorSubtaskState state;
        try (var writer = writer(1, 0);
                var committer = committer(1, 0)) {
            writer.open();
            committer.open();
            writer.getOperator().prepareSnapshotPreBarrier(1);
            assertThat(envelopes(writer)).isEmpty();
            forward(writer, committer);
            state = committer.snapshot(1, 0);
        }
        try (var restored = committer(3, 2)) {
            restored.setRestoredCheckpointId(1);
            restored.initializeState(state);
            assertThat(probe.committed).isEmpty();
        }
        assertThat(Modifier.isAbstract(CloudTasksStagedCreateTaskSink.class.getModifiers()))
                .isFalse();
        assertThat(SupportsWriterState.class.isAssignableFrom(sink.getClass())).isFalse();
        var eager =
                CloudTasksSink.<String>builder()
                        .queue(QueueDestination.of("p", "l", "q"))
                        .serializer(element -> null)
                        .build();
        assertThat(eager)
                .isInstanceOf(CloudTasksCreateTaskSink.class)
                .isNotInstanceOf(SupportsCommitter.class);
    }

    private OneInputStreamOperatorTestHarness<String, CommittableMessage<CloudTasksCommittable>>
            writer(int parallelism, int subtask) throws Exception {
        // Clone the actual sink configuration like job deployment does, not its runtime objects.
        var deployed = InstantiationUtil.clone(sink);
        var harness =
                new OneInputStreamOperatorTestHarness<
                        String, CommittableMessage<CloudTasksCommittable>>(
                        new SinkWriterOperatorFactory<>(deployed), 128, parallelism, subtask);
        harness.setup(
                CommittableMessageTypeInfo.of(deployed::getCommittableSerializer)
                        .createSerializer(new SerializerConfigImpl()));
        return harness;
    }

    private OneInputStreamOperatorTestHarness<
                    CommittableMessage<CloudTasksCommittable>,
                    CommittableMessage<CloudTasksCommittable>>
            committer(int parallelism, int subtask) throws Exception {
        return new OneInputStreamOperatorTestHarness<>(
                new CommitterOperatorFactory<>(InstantiationUtil.clone(sink), false, true),
                128,
                parallelism,
                subtask);
    }

    private static List<CloudTasksCommittable> envelopes(
            OneInputStreamOperatorTestHarness<String, CommittableMessage<CloudTasksCommittable>>
                    writer) {
        return writer.extractOutputValues().stream()
                .filter(message -> message instanceof CommittableWithLineage)
                .map(
                        message ->
                                ((CommittableWithLineage<CloudTasksCommittable>) message)
                                        .getCommittable())
                .toList();
    }

    private static void forward(
            OneInputStreamOperatorTestHarness<String, CommittableMessage<CloudTasksCommittable>>
                    writer,
            OneInputStreamOperatorTestHarness<
                            CommittableMessage<CloudTasksCommittable>,
                            CommittableMessage<CloudTasksCommittable>>
                    committer)
            throws Exception {
        for (var message : writer.extractOutputValues()) {
            committer.processElement(new StreamRecord<>(message));
        }
        writer.getOutput().clear();
    }
}
