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

import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;

import com.google.cloud.tasks.v2.Task;
import io.github.flink.gcp.connector.cloudtasks.sink.writer.DefaultTaskCreatorFactory;
import io.github.flink.gcp.connector.cloudtasks.sink.writer.TaskCreator;
import io.github.flink.gcp.connector.testutils.TestSinkCommitterMetricGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.time.Duration;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CloudTasksStagedRetentionTest {
    @ParameterizedTest
    @EnumSource(CloudTasksStagedOptions.ExpiredEnvelopePolicy.class)
    void retentionFailurePrecedesCreatorConstructionAndNeverUsesTheExpiryPolicy(
            CloudTasksStagedOptions.ExpiredEnvelopePolicy policy) {
        var factory = new RecordingFactory();
        var options = CloudTasksStagedOptions.builder().expiredEnvelopePolicy(policy).build();
        var sink = sink(options, false, factory);
        factory.retention = Duration.ofMinutes(59);
        assertThatThrownBy(() -> sink.createCommitter(context()))
                .hasMessageContaining("tombstoneTtl is shorter");
        factory.failReadback = true;
        assertThatThrownBy(() -> sink.createCommitter(context()))
                .hasMessageContaining("injected readback failure");
        assertThat(factory.reads).isEqualTo(2);
        assertThat(factory.creators).isZero();
    }

    @Test
    void sufficientReadbackCreatesAClientWhileEmulatorAndExplicitDisableSkipReadback()
            throws Exception {
        var factory = new RecordingFactory();
        try (var ignored =
                sink(CloudTasksStagedOptions.builder().build(), false, factory)
                        .createCommitter(context())) {
            assertThat(factory.reads).isEqualTo(1);
            assertThat(factory.creators).isEqualTo(1);
        }
        factory.failReadback = true;
        try (var ignored =
                        sink(CloudTasksStagedOptions.builder().build(), true, factory)
                                .createCommitter(context());
                var disabled =
                        sink(
                                        CloudTasksStagedOptions.builder()
                                                .verifyQueueRetention(false)
                                                .build(),
                                        false,
                                        factory)
                                .createCommitter(context())) {
            assertThat(factory.reads).isEqualTo(1);
            assertThat(factory.creators).isEqualTo(3);
        }
        assertThat(factory.closed).isEqualTo(3);
    }

    private static CloudTasksStagedCreateTaskSink<String> sink(
            CloudTasksStagedOptions options, boolean emulator, RecordingFactory factory) {
        var builder =
                CloudTasksSink.<String>builder()
                        .queue(QueueDestination.of("p", "l", "q"))
                        .serializer(value -> Task.getDefaultInstance());
        if (emulator) {
            builder.emulatorEndpoint("localhost:1");
        }
        var config = ((CloudTasksCreateTaskSink<String>) builder.build()).getConfig();
        return new CloudTasksStagedCreateTaskSink<>(config, options) {
            private static final long serialVersionUID = 1L;

            @Override
            protected DefaultTaskCreatorFactory taskCreatorFactory() {
                return factory;
            }
        };
    }

    private static CommitterInitContext context() {
        return new CommitterInitContext() {
            private final SinkCommitterMetricGroup metrics = TestSinkCommitterMetricGroup.create();

            @Override
            public SinkCommitterMetricGroup metricGroup() {
                return metrics;
            }

            @Override
            public OptionalLong getRestoredCheckpointId() {
                return OptionalLong.empty();
            }

            @Override
            public JobInfo getJobInfo() {
                throw new AssertionError("unused job info");
            }

            @Override
            public TaskInfo getTaskInfo() {
                throw new AssertionError("unused task info");
            }
        };
    }

    private static final class RecordingFactory extends DefaultTaskCreatorFactory {
        private static final long serialVersionUID = 1L;
        private Duration retention = Duration.ofHours(1);
        private boolean failReadback;
        private int reads;
        private int creators;
        private int closed;

        private RecordingFactory() {
            super(null, null, null);
        }

        @Override
        public Duration readQueueRetention(String queue) throws IOException {
            reads++;
            assertThat(queue).isEqualTo("projects/p/locations/l/queues/q");
            if (failReadback) {
                throw new IOException("injected readback failure");
            }
            return retention;
        }

        @Override
        public TaskCreator create() {
            creators++;
            return new TaskCreator() {
                @Override
                public com.google.api.core.ApiFuture<Task> createTask(
                        com.google.cloud.tasks.v2.CreateTaskRequest request) {
                    throw new AssertionError("no creates during setup");
                }

                @Override
                public com.google.api.core.ApiFuture<Task> createTask(
                        com.google.cloud.tasks.v2.CreateTaskRequest request,
                        io.grpc.Deadline deadline) {
                    throw new AssertionError("no creates during setup");
                }

                @Override
                public void close() {
                    closed++;
                }
            };
        }
    }
}
