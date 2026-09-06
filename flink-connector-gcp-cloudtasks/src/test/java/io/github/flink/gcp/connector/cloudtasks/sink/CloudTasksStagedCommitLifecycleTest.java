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
import org.apache.flink.configuration.SinkOptions;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessageTypeInfo;
import org.apache.flink.streaming.runtime.operators.sink.CommitterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Executable lifecycle evidence for ADR-0158, using Flink's operators rather than a fake runtime.
 *
 * <p>Each case pins one fact the protocol relies on: when Flink calls the writer and committer,
 * what a completed checkpoint commits, what a restore re-commits, and which checkpoint id end of
 * input emits under. None of them exercises a Cloud Tasks implementation.
 */
class CloudTasksStagedCommitLifecycleTest {
    private final String runId = UUID.randomUUID().toString();
    private final StagedCommitTestSink.Probe probe = new StagedCommitTestSink.Probe();
    private final StagedCommitTestSink sink = new StagedCommitTestSink(runId);

    CloudTasksStagedCommitLifecycleTest() {
        StagedCommitTestSink.PROBES.put(runId, probe);
    }

    @AfterEach
    void removeProbe() {
        StagedCommitTestSink.PROBES.remove(runId);
    }

    @Test
    void preBarrierTransfersCommittablesBeforeWriterSnapshotAndCompletionCreates()
            throws Exception {
        try (var writer = writer();
                var committer = committer(false, true)) {
            writer.open();
            committer.open();
            stage(writer, committer);
            writer.snapshot(1, 0);
            // The snapshot hook drains nothing: staging left the writer in prepareCommit().
            assertThat(probe.events).containsExactly("flush:false", "prepare");
            assertThat(probe.created).isEmpty();

            committer.notifyOfCompletedCheckpoint(1);
            assertThat(probe.created).containsExactly("10:one");
            assertThat(probe.callbackThreads).containsExactly(Thread.currentThread());
            committer.notifyOfCompletedCheckpoint(1);
            assertThat(probe.created).containsExactly("10:one");
        }
    }

    @Test
    void restoredCommittablesAreRetriedDuringInitializationWithOriginalBytes() throws Exception {
        OperatorSubtaskState snapshot;
        try (var writer = writer();
                var committer = committer(false, true)) {
            writer.open();
            committer.open();
            stage(writer, committer);
            snapshot = committer.snapshot(1, 0);
            assertThat(probe.created).isEmpty();
            committer.notifyOfCompletedCheckpoint(1);
            assertThat(probe.created).containsExactly("10:one");
        }
        probe.created.clear();
        try (var restored = committer(false, true)) {
            restored.setRestoredCheckpointId(1);
            restored.initializeState(snapshot);
            // open() has not run. The saved collector predates the successful first commit.
            assertThat(probe.restored).containsExactly("10:one");
            assertThat(probe.created).containsExactly("10:one");
        }
    }

    @Test
    void retryLaterUsesOneSynchronousBoundedLoopAndCannotSilentlySucceed() throws Exception {
        probe.outcome = StagedCommitTestSink.Outcome.RETRY;
        try (var writer = writer();
                var committer = committer(false, true)) {
            committer.getStreamConfig().getConfiguration().set(SinkOptions.COMMITTER_RETRIES, 2);
            writer.open();
            committer.open();
            stage(writer, committer);
            assertThatThrownBy(() -> committer.notifyOfCompletedCheckpoint(1))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Failed to commit");
            assertThat(probe.retries).containsExactly(0, 1, 2);
            assertThat(probe.callbackThreads)
                    .containsExactly(
                            Thread.currentThread(), Thread.currentThread(), Thread.currentThread());
            assertThat(probe.created).isEmpty();
        }
    }

    @Test
    void streamingEndInputWaitsForCheckpointCompletion() throws Exception {
        try (var writer = writer();
                var committer = committer(false, true)) {
            writer.open();
            committer.open();
            writer.processElement("10:one", 0);
            writer.endInput();
            forward(writer, committer);
            committer.endInput();
            assertThat(probe.events).containsExactly("flush:true", "prepare");
            assertThat(probe.created).isEmpty();
            committer.notifyOfCompletedCheckpoint(1);
            assertThat(probe.created).containsExactly("10:one");
        }
    }

    @Test
    void abortedCheckpointIsCommittedOnceByTheNextCompletedCheckpoint() throws Exception {
        try (var writer = writer();
                var committer = committer(false, true)) {
            writer.open();
            committer.open();
            // Checkpoint 1 emits "one" and is then aborted.
            stage(writer, committer);
            writer.snapshot(1, 0);
            committer.getOperator().notifyCheckpointAborted(1);
            writer.processElement("10:two", 0);
            writer.getOperator().prepareSnapshotPreBarrier(2);
            forward(writer, committer);
            assertThat(probe.created).isEmpty();

            // Completing 2 commits every pending checkpoint up to 2, including the aborted 1.
            committer.notifyOfCompletedCheckpoint(2);
            assertThat(probe.created).containsExactly("10:one", "10:two");
            committer.notifyOfCompletedCheckpoint(2);
            assertThat(probe.created).containsExactly("10:one", "10:two");
        }
    }

    @Test
    void endInputEmitsUnderTheNextCheckpointIdNotASentinel() throws Exception {
        try (var writer = writer();
                var committer = committer(false, true)) {
            writer.open();
            committer.open();
            writer.processElement("10:one", 0);
            writer.getOperator().prepareSnapshotPreBarrier(5);
            forward(writer, committer);
            writer.processElement("10:two", 0);
            writer.endInput();
            forward(writer, committer);
            committer.endInput();
            assertThat(probe.created).isEmpty();

            // "two" was emitted under 6 = lastKnownCheckpointId + 1. Completing 5 commits only
            // "one"; completing 6 commits "two". A sentinel id such as Long.MAX_VALUE would
            // never be committed by 6.
            committer.notifyOfCompletedCheckpoint(5);
            assertThat(probe.created).containsExactly("10:one");
            committer.notifyOfCompletedCheckpoint(6);
            assertThat(probe.created).containsExactly("10:one", "10:two");
        }
    }

    @Test
    void checkpointDisabledExecutionCreatesWithoutCompletion() throws Exception {
        try (var writer = writer();
                var committer = committer(false, false)) {
            writer.open();
            committer.open();
            writer.processElement("10:one", 0);
            writer.endInput();
            forward(writer, committer);
            committer.endInput();
            assertThat(probe.created).containsExactly("10:one");
        }
    }

    private OneInputStreamOperatorTestHarness<String, CommittableMessage<String>> writer()
            throws Exception {
        var harness =
                new OneInputStreamOperatorTestHarness<String, CommittableMessage<String>>(
                        new SinkWriterOperatorFactory<>(sink));
        harness.setup(
                CommittableMessageTypeInfo.of(sink::getCommittableSerializer)
                        .createSerializer(new SerializerConfigImpl()));
        return harness;
    }

    private OneInputStreamOperatorTestHarness<
                    CommittableMessage<String>, CommittableMessage<String>>
            committer(boolean batch, boolean checkpointing) throws Exception {
        return new OneInputStreamOperatorTestHarness<>(
                new CommitterOperatorFactory<>(sink, batch, checkpointing));
    }

    private static void stage(
            OneInputStreamOperatorTestHarness<String, CommittableMessage<String>> writer,
            OneInputStreamOperatorTestHarness<
                            CommittableMessage<String>, CommittableMessage<String>>
                    committer)
            throws Exception {
        writer.processElement("10:one", 0);
        writer.getOperator().prepareSnapshotPreBarrier(1);
        forward(writer, committer);
    }

    private static void forward(
            OneInputStreamOperatorTestHarness<String, CommittableMessage<String>> writer,
            OneInputStreamOperatorTestHarness<
                            CommittableMessage<String>, CommittableMessage<String>>
                    committer)
            throws Exception {
        for (CommittableMessage<String> message : writer.extractOutputValues()) {
            committer.processElement(new StreamRecord<>(message));
        }
        writer.getOutput().clear();
    }
}
