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

package io.github.flink.gcp.connector.cloudtasks.sink.acceptance;

import org.apache.flink.api.connector.sink2.Committer.CommitRequest;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.tasks.v2.CreateTaskRequest;
import com.google.cloud.tasks.v2.Task;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksCommittable;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksStagedOptions;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksWriterOptions;
import io.github.flink.gcp.connector.cloudtasks.sink.committer.CloudTasksStagedCommitter;
import io.github.flink.gcp.connector.cloudtasks.sink.writer.TaskCreator;
import io.github.flink.gcp.connector.cloudtasks.sink.writer.TimeSource;
import io.github.flink.gcp.connector.testutils.TestSinkCommitterMetricGroup;
import io.grpc.Deadline;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The accelerated retention model is an explicit assumption, never GCP evidence. */
class CloudTasksRecoveryModelTest {
    @Test
    void strictAuthorizationStopsPhysicalRecreationAtAndAfterTheBoundary() throws Exception {
        String queue = "projects/p/locations/l/queues/q";
        Task task =
                StagedRecoveryAcceptance.task("body", false, 100).toBuilder()
                        .setName(queue + "/tasks/00000000000000000000000000000001")
                        .build();
        AtomicLong clock = new AtomicLong(1_000);
        Model creator = new Model(clock);
        var options =
                CloudTasksStagedOptions.builder()
                        .nameRetention(Duration.ofMillis(1_000))
                        .clockSkewAllowance(Duration.ofMillis(100))
                        .requestTimeout(Duration.ofMillis(100))
                        .build();
        var envelope = CloudTasksCommittable.fromTask(queue, 1_000, 1_800, task);
        var writer = CloudTasksWriterOptions.builder().recoveryMaxAttempts(1).build();
        TimeSource time =
                new TimeSource() {
                    @Override
                    public long currentTimeMillis() {
                        return clock.get();
                    }

                    @Override
                    public void sleep(long millis) {
                        throw new AssertionError("The model must not wait on real time");
                    }
                };
        try (var committer =
                new CloudTasksStagedCommitter(
                        queue,
                        options,
                        writer,
                        creator,
                        time,
                        TestSinkCommitterMetricGroup.create())) {
            committer.commit(List.of(new Request(envelope)));
            assertThat(creator.generations).isEqualTo(1);
            creator.remove();
            clock.set(1_799);
            Request replay = new Request(envelope);
            committer.commit(List.of(replay));
            assertThat(replay.alreadyCommitted).isTrue();
            int calls = creator.calls;
            for (long expired : new long[] {1_800, 1_801, 2_001}) {
                clock.set(expired);
                assertThatThrownBy(() -> committer.commit(List.of(new Request(envelope))))
                        .hasMessageContaining("envelope expired");
                assertThat(creator.calls).isEqualTo(calls);
                assertThat(creator.generations).isEqualTo(1);
            }
            // Falsifying control: deliberately bypass production authorization after model expiry.
            creator.createTask(
                            CreateTaskRequest.newBuilder().setParent(queue).setTask(task).build(),
                            Deadline.after(1, java.util.concurrent.TimeUnit.SECONDS))
                    .get();
            assertThat(creator.generations).isEqualTo(2);
        }
    }

    private static final class Model implements TaskCreator {
        private final AtomicLong clock;
        private boolean live;
        private long protectedUntil;
        private int calls;
        private int generations;

        Model(AtomicLong clock) {
            this.clock = clock;
        }

        void remove() {
            live = false;
            protectedUntil = clock.get() + 1_000;
        }

        @Override
        public ApiFuture<Task> createTask(CreateTaskRequest request) {
            throw new AssertionError("Absolute deadline required");
        }

        @Override
        public ApiFuture<Task> createTask(CreateTaskRequest request, Deadline deadline) {
            calls++;
            if (live || clock.get() < protectedUntil) {
                return ApiFutures.immediateFailedFuture(Status.ALREADY_EXISTS.asRuntimeException());
            }
            live = true;
            generations++;
            return ApiFutures.immediateFuture(request.getTask());
        }

        @Override
        public void close() {}
    }

    private static final class Request implements CommitRequest<CloudTasksCommittable> {
        private final CloudTasksCommittable envelope;
        private boolean alreadyCommitted;

        Request(CloudTasksCommittable envelope) {
            this.envelope = envelope;
        }

        @Override
        public CloudTasksCommittable getCommittable() {
            return envelope;
        }

        @Override
        public int getNumberOfRetries() {
            return 0;
        }

        @Override
        public void signalAlreadyCommitted() {
            alreadyCommitted = true;
        }

        @Override
        public void retryLater() {
            throw new AssertionError("Unexpected retryLater");
        }

        @Override
        public void updateAndRetryLater(CloudTasksCommittable replacement) {
            throw new AssertionError("Unexpected envelope replacement");
        }

        @Override
        public void signalFailedWithKnownReason(Throwable reason) {
            throw new AssertionError("Unexpected finalization of failed task", reason);
        }

        @Override
        public void signalFailedWithUnknownReason(Throwable reason) {
            throw new AssertionError("Unexpected finalization of failed task", reason);
        }
    }
}
