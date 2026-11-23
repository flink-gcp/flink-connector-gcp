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

package io.github.flink.gcp.connector.pubsub.sink.writer;

import org.apache.flink.api.common.serialization.SimpleStringSchema;

import io.github.flink.gcp.connector.pubsub.sink.CreateDisposition;
import io.github.flink.gcp.connector.pubsub.sink.PubSubPublisherOptions;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.sink.serializer.PubSubSerializationSchema;
import io.github.flink.gcp.connector.pubsub.sink.topics.TopicAdmin;
import io.github.flink.gcp.connector.testutils.FakeMailboxExecutor;
import io.github.flink.gcp.connector.testutils.GrpcEventLoopThreads;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The sink's gRPC clients run on the connector's own event-loop group and stop it when they close
 * (ADR-0208, #2263).
 *
 * <p>gRPC's shared group is destroyed a second after its last channel closes, which for a job-jar
 * deployment can be after Flink has closed the job's class loader; under {@code
 * cluster.uncaught-exception-handling: FAIL} that exits the TaskManager. The emulator arms are the
 * ones a test can reach; the production arms apply the same lease to the SDK's default provider,
 * which {@code PubSubTransportsTest} covers.
 */
class PubSubSinkEventLoopITCase extends AbstractPubSubEmulatorITCase {

    /**
     * How long a check before a test waits for a group an earlier class left running: a MiniCluster
     * job that class cancelled closes its clients after it has returned, each within its own
     * shutdown budget.
     */
    private static final Duration LEFTOVER_TEARDOWN = Duration.ofSeconds(60);

    /**
     * A group left running by an earlier class in this reused fork would make the checks below pass
     * or fail for the wrong reason, so it is ruled out first. The harness's own clients run on
     * gRPC's group, not this one.
     */
    @BeforeEach
    void noGroupIsRunning() throws InterruptedException {
        assertThat(GrpcEventLoopThreads.awaitNone(LEFTOVER_TEARDOWN))
                .as(
                        "no %s* thread before the test: an earlier class in this fork left the"
                                + " group running",
                        GrpcEventLoopThreads.PREFIX)
                .isTrue();
    }

    @Test
    void theWritersPublisherRunsOnTheGroupAndStopsItOnClose() throws Exception {
        TopicDestination destination = TopicDestination.of(PROJECT, "event-loop-topic");
        createTopic(destination);
        createSubscription(destination, "event-loop-sub");

        PubSubWriter<String> writer =
                newWriter(
                        TestSinkConfigs.forTopic(
                                destination,
                                PubSubSerializationSchema.payload(new SimpleStringSchema()),
                                CreateDisposition.CREATE_NEVER,
                                PubSubPublisherOptions.builder().build()),
                        new FakeMailboxExecutor());
        try {
            writer.write("event-loop", CONTEXT);
            writer.flush(false);

            assertThat(GrpcEventLoopThreads.alive())
                    .as("a %s* thread while the writer is open", GrpcEventLoopThreads.PREFIX)
                    .isTrue();
        } finally {
            writer.close();
        }

        assertThat(GrpcEventLoopThreads.awaitNone())
                .as("no %s* thread once the writer closed", GrpcEventLoopThreads.PREFIX)
                .isTrue();
        assertThat(pullDistinctPayloadsUntil("event-loop-sub", 1, Duration.ofSeconds(30)))
                .containsExactly("event-loop");
    }

    @Test
    void theTopicAdminClosesItsClientWhenTheCreationReturns() throws Exception {
        TopicDestination destination = TopicDestination.of(PROJECT, "event-loop-created");

        try (TopicAdmin admin = newTopicAdmin()) {
            admin.createTopic(destination, null);

            // Nothing else holds a lease, so the client's close is what stopped the group.
            assertThat(GrpcEventLoopThreads.awaitNone())
                    .as("no %s* thread once the creation returned", GrpcEventLoopThreads.PREFIX)
                    .isTrue();
        }
        assertThat(topicExists(destination)).isTrue();
    }
}
