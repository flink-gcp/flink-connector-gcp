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
import io.github.flink.gcp.connector.testutils.FakeMailboxExecutor;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for the checkpoint flush against the Pub/Sub emulator. The batching thresholds
 * are set so high that the SDK publisher never sends a batch on its own — every delivery observed
 * by these tests is forced by {@code flush} (the checkpoint's {@code prepareCommit} path), which
 * must also drain the writer's in-flight count to zero.
 */
class PubSubCheckpointFlushITCase extends AbstractPubSubEmulatorITCase {

    private static PubSubWriter<String> writer(TopicDestination destination) {
        PubSubPublisherOptions options =
                PubSubPublisherOptions.builder()
                        .batchElementCountThreshold(10_000)
                        .batchRequestByteThreshold(10_000_000)
                        .batchDelayThreshold(Duration.ofMinutes(10))
                        .build();
        return newWriter(
                TestSinkConfigs.forTopic(
                        destination,
                        PubSubSerializationSchema.dataOnly(new SimpleStringSchema()),
                        CreateDisposition.CREATE_NEVER,
                        options),
                new FakeMailboxExecutor());
    }

    @Test
    void flushDeliversBatchedMessagesAndDrainsInFlight() throws Exception {
        TopicDestination destination = TopicDestination.of(PROJECT, "flush-topic");
        createTopic(destination);
        createSubscription(destination, "flush-sub");

        PubSubWriter<String> writer = writer(destination);
        try {
            Set<String> expected = new LinkedHashSet<>();
            for (int i = 0; i < 50; i++) {
                String payload = "flush-" + i;
                expected.add(payload);
                writer.write(payload, CONTEXT);
            }
            // No batching threshold can fire, so everything is still buffered in the publisher:
            // all 50 publish futures incomplete means the server has accepted nothing yet.
            assertThat(writer.getInFlightMessages()).isEqualTo(50);

            writer.flush(false);

            assertThat(writer.getInFlightMessages()).isZero();
            assertThat(pullDistinctPayloadsUntil("flush-sub", 50, Duration.ofSeconds(30)))
                    .containsExactlyInAnyOrderElementsOf(expected);
        } finally {
            writer.close();
        }
    }

    @Test
    void repeatedWriteFlushCyclesReuseTheWriter() throws Exception {
        TopicDestination destination = TopicDestination.of(PROJECT, "flush-cycles-topic");
        createTopic(destination);
        createSubscription(destination, "flush-cycles-sub");

        PubSubWriter<String> writer = writer(destination);
        try {
            for (int cycle = 0; cycle < 3; cycle++) {
                Set<String> cycleExpected = new LinkedHashSet<>();
                for (int i = 0; i < 10; i++) {
                    String payload = "cycle-" + cycle + "-" + i;
                    cycleExpected.add(payload);
                    writer.write(payload, CONTEXT);
                }
                writer.flush(false);

                assertThat(writer.getInFlightMessages()).isZero();
                // Earlier cycles were acked away, so each cycle must deliver exactly its own
                // batch through the same reused writer and per-topic publisher.
                assertThat(
                                pullDistinctPayloadsUntil(
                                        "flush-cycles-sub",
                                        cycleExpected.size(),
                                        Duration.ofSeconds(30)))
                        .containsExactlyInAnyOrderElementsOf(cycleExpected);
            }
        } finally {
            writer.close();
        }
    }
}
