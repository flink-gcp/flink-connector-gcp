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

package io.github.flink.gcp.connector.cloudtasks.sink.writer;

import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.cloudtasks.sink.QueueDestination;
import io.github.flink.gcp.connector.testutils.EventLoopObserver;
import io.github.flink.gcp.connector.testutils.GrpcEventLoopThreads;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The writer's Cloud Tasks client runs on the connector's own gRPC event-loop group and stops it
 * when the writer closes (ADR-0208, #2243).
 *
 * <p>gRPC's shared group is destroyed a second after its last channel closes, which for a job-jar
 * deployment can be after Flink has closed the job's class loader; under {@code
 * cluster.uncaught-exception-handling: FAIL} that exits the TaskManager. The emulator arm is the
 * one a test can reach; the production arm applies the same lease to the client's settings, which
 * {@code DefaultTaskCreatorFactoryTest} covers.
 */
class CloudTasksEventLoopITCase extends AbstractCloudTasksEmulatorITCase {

    @Test
    void theWritersClientRunsOnTheGroupAndStopsItOnClose() throws Exception {
        QueueDestination queue = createPausedQueue("event-loop");
        // A group left running by an earlier class in this reused fork would make the checks below
        // pass or fail for the wrong reason, so it is ruled out first.
        assertThat(
                        EventLoopObserver.awaitNoGroup(
                                GrpcEventLoops::isRunning, Duration.ofSeconds(60)))
                .as(
                        "no lease and no %s* thread before the writer opens",
                        GrpcEventLoopThreads.PREFIX)
                .isTrue();
        EventLoopObserver.LoopStarts starts =
                EventLoopObserver.loopStartsFromNow(
                        GrpcEventLoops::isRunning, GrpcEventLoops::loopsStartedWhileRunning);

        try (CloudTasksWriter<String> writer = newWriter(TestSinkConfigs.builder(queue))) {
            write(writer, "task");

            assertThat(starts.anyStarted())
                    .as("a group event loop started while the writer is open")
                    .isTrue();
            assertThat(GrpcEventLoops.isRunning()).as("the writer still holds its lease").isTrue();
        }

        assertThat(GrpcEventLoopThreads.awaitNone())
                .as("no %s* thread once the writer closed", GrpcEventLoopThreads.PREFIX)
                .isTrue();
        assertThat(listTasks(queue)).hasSize(1);
    }
}
