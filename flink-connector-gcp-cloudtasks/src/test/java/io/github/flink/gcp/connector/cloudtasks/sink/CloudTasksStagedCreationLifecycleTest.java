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
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessageTypeInfo;
import org.apache.flink.streaming.runtime.operators.sink.CommitterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.InstantiationUtil;

import com.google.api.core.ApiFutures;
import io.grpc.Status;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CloudTasksStagedCreationLifecycleTest {
    private final String runId = UUID.randomUUID().toString();
    private final StagedCreationTestSink.Probe probe = new StagedCreationTestSink.Probe();
    private final StagedCreationTestSink sink =
            new StagedCreationTestSink(runId, CloudTasksStagedOptions.builder().build());

    CloudTasksStagedCreationLifecycleTest() {
        StagedCreationTestSink.PROBES.put(runId, probe);
    }

    @AfterEach
    void removeProbe() {
        StagedCreationTestSink.PROBES.remove(runId);
    }

    @Test
    void noServiceEffectsUntilCompletionAndDurableAcknowledgementLossReplaysTheSameNames()
            throws Exception {
        OperatorSubtaskState state;
        try (var writer = writer(1, 0);
                var committer = committer(1, 0)) {
            writer.open();
            committer.open();
            writer.processElement("same body", 0);
            writer.processElement("same body", 0);
            writer.getOperator().prepareSnapshotPreBarrier(1);
            forward(writer, committer);
            state = committer.snapshot(1, 0);
            assertThat(probe.requests).isEmpty();
            committer.notifyOfCompletedCheckpoint(1);
            assertThat(probe.created).hasSize(2);
            assertThat(probe.requests).hasSize(2);
        }
        var original = java.util.List.copyOf(probe.requests);
        try (var restored = committer(1, 0)) {
            restored.setRestoredCheckpointId(1);
            restored.initializeState(state);
            assertThat(probe.requests.subList(2, 4)).containsExactlyElementsOf(original);
            assertThat(probe.created).hasSize(2);
            restored.open();
            restored.notifyOfCompletedCheckpoint(1);
            assertThat(probe.requests).hasSize(4);
            assertThat(probe.metrics.get(1).getNumCommittablesAlreadyCommittedCounter().getCount())
                    .isEqualTo(2);
            assertThat(probe.metrics.get(1).getNumCommittablesFailureCounter().getCount()).isZero();
        }
        assertThat(probe.identities.get()).isEqualTo(2);
        assertThat(probe.serialized.get()).isEqualTo(2);
    }

    @Test
    void partialCommitRestoresAcceptedAndRejectedTasksWithoutDuplicateCreation() throws Exception {
        probe.actions.add(probe::accept);
        probe.actions.add(
                request ->
                        ApiFutures.immediateFailedFuture(
                                Status.INVALID_ARGUMENT.asRuntimeException()));
        OperatorSubtaskState state;
        try (var writer = writer(1, 0);
                var committer = committer(1, 0)) {
            writer.open();
            committer.open();
            writer.processElement("accepted", 0);
            writer.processElement("rejected", 0);
            writer.getOperator().prepareSnapshotPreBarrier(1);
            forward(writer, committer);
            state = committer.snapshot(1, 0);
            assertThatThrownBy(() -> committer.notifyOfCompletedCheckpoint(1))
                    .hasMessageContaining("INVALID_ARGUMENT");
        }
        assertThat(probe.created).hasSize(1);
        try (var restored = committer(1, 0)) {
            restored.setRestoredCheckpointId(1);
            restored.initializeState(state);
        }
        assertThat(probe.created).hasSize(2);
        assertThat(probe.requests).hasSize(4);
        assertThat(probe.requests.get(2)).isEqualTo(probe.requests.get(0));
        assertThat(probe.requests.get(3)).isEqualTo(probe.requests.get(1));
        assertThat(probe.serialized.get()).isEqualTo(2);
    }

    @Test
    void expiredRestoredWorkFailsDuringInitializationWithoutAnotherCreate() throws Exception {
        OperatorSubtaskState state;
        try (var writer = writer(1, 0);
                var committer = committer(1, 0)) {
            writer.open();
            committer.open();
            writer.processElement("expired", 0);
            writer.getOperator().prepareSnapshotPreBarrier(1);
            forward(writer, committer);
            state = committer.snapshot(1, 0);
        }
        probe.wall.set(3_281_000);
        try (var restored = committer(1, 0)) {
            restored.setRestoredCheckpointId(1);
            assertThatThrownBy(() -> restored.initializeState(state))
                    .hasStackTraceContaining("envelope expired");
        }
        assertThat(probe.requests).isEmpty();
        assertThat(probe.serialized.get()).isEqualTo(1);
    }

    @Test
    void rescaleCreatesEachEnvelopeOnceAndRetainsItsOriginalBytes() throws Exception {
        OperatorSubtaskState[] states = new OperatorSubtaskState[2];
        for (int subtask = 0; subtask < 2; subtask++) {
            try (var writer = writer(2, subtask);
                    var committer = committer(2, subtask)) {
                writer.open();
                committer.open();
                writer.processElement("body-" + subtask, 0);
                writer.getOperator().prepareSnapshotPreBarrier(1);
                forward(writer, committer);
                states[subtask] = committer.snapshot(1, 0);
            }
        }
        var combined = AbstractStreamOperatorTestHarness.repackageState(states);
        for (int parallelism : new int[] {1, 3}) {
            for (int subtask = 0; subtask < parallelism; subtask++) {
                var repartitioned =
                        AbstractStreamOperatorTestHarness.repartitionOperatorState(
                                combined, 128, 2, parallelism, subtask);
                try (var restored = committer(parallelism, subtask)) {
                    restored.setRestoredCheckpointId(1);
                    restored.initializeState(repartitioned);
                }
            }
        }
        assertThat(probe.requests).hasSize(4);
        assertThat(probe.created).hasSize(2);
        assertThat(probe.serialized.get()).isEqualTo(2);
        assertThat(probe.identities.get()).isEqualTo(2);
    }

    private OneInputStreamOperatorTestHarness<String, CommittableMessage<CloudTasksCommittable>>
            writer(int parallelism, int subtask) throws Exception {
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
