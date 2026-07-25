/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.pubsub.source;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.source.serializer.PubSubDeserializationSchema;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.PubSubEnumeratorState;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;
import io.github.flink.gcp.connector.pubsub.source.subscriptions.SubscriptionAdmin;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * MiniCluster tests of the enumerator's startup check, driven through the public builder against
 * the emulator: subscription auto-creation, the fail-fast when it is not authorised, and the start
 * position.
 */
class PubSubSourceStartupITCase extends AbstractPubSubSourceEmulatorITCase {

    @Test
    void aMissingSubscriptionIsCreatedAndConsumed() throws Exception {
        TopicDestination topic = createTopic("startup-create-topic");
        SubscriptionDestination subscription =
                SubscriptionDestination.of(PROJECT, "startup-create-sub");

        Source<String, SubscriptionSplit, PubSubEnumeratorState> source =
                sourceBuilder()
                        .subscription(
                                subscription,
                                SubscriptionCreateOptions.builder().topic(topic).build())
                        .build();

        Set<String> collected = new LinkedHashSet<>();
        try (CloseableIterator<String> records = run(source)) {
            // Nothing published before the subscription exists is retained for it, so the publish
            // has to wait for the startup check — which runs before any split is assigned.
            awaitSubscription(subscription);
            publish("startup-create-topic", "one", "two");
            while (collected.size() < 2 && records.hasNext()) {
                collected.add(records.next());
            }
        }

        assertThat(collected).containsExactlyInAnyOrder("one", "two");
    }

    @Test
    void aMissingSubscriptionWithoutCreateOptionsFailsTheJob() throws Exception {
        createTopic("startup-absent-topic");
        SubscriptionDestination subscription =
                SubscriptionDestination.of(PROJECT, "startup-absent-sub");

        Source<String, SubscriptionSplit, PubSubEnumeratorState> source =
                sourceBuilder().subscription(subscription).build();

        assertThatThrownBy(() -> collect(source, 1))
                .rootCause()
                .hasMessageContaining("does not exist")
                .hasMessageContaining("subscription(destination, SubscriptionCreateOptions)");
        assertThat(subscriptionExists(subscription)).isFalse();
    }

    @Test
    void theEarliestRetainedStartPositionReplaysAcknowledgedMessages() throws Exception {
        // Unordered: the emulator does not support seek on ordering-enabled subscriptions.
        TopicDestination topic = createTopic("startup-seek-topic");
        SubscriptionDestination subscription =
                SubscriptionDestination.of(PROJECT, "startup-seek-sub");
        try (SubscriptionAdmin admin = newSubscriptionAdmin()) {
            admin.create(
                    subscription,
                    SubscriptionCreateOptions.builder()
                            .topic(topic)
                            .retainAckedMessages(true)
                            .build());
        }
        publish("startup-seek-topic", "one", "two", "three");
        // Consumed and acknowledged by someone else, so the subscription's backlog is empty.
        assertThat(pullAndAck(subscription, 10)).hasSize(3);
        assertThat(pullAndAck(subscription, 10)).isEmpty();

        Source<String, SubscriptionSplit, PubSubEnumeratorState> source =
                sourceBuilder()
                        .subscription(subscription)
                        .startPosition(StartPosition.earliestRetained())
                        .build();

        assertThat(collect(source, 3)).containsExactlyInAnyOrder("one", "two", "three");
    }

    /** Waits for the source's startup check to create the subscription. */
    private static void awaitSubscription(SubscriptionDestination subscription)
            throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            if (subscriptionExists(subscription)) {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError(
                "The source did not create subscription " + subscription + " in time.");
    }

    private static PubSubSourceBuilder<String> sourceBuilder() {
        return PubSubSource.<String>builder()
                .deserializationSchema(
                        PubSubDeserializationSchema.dataOnly(new SimpleStringSchema()))
                .emulatorEndpoint(emulatorEndpoint());
    }

    /** Submits the source as a streaming job and returns its output. */
    private static CloseableIterator<String> run(
            Source<String, SubscriptionSplit, PubSubEnumeratorState> source) throws Exception {
        Configuration configuration = new Configuration();
        // Without this a permanent failure would be retried forever instead of failing the test.
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        // Checkpointing is what acknowledges messages; the interval is short so the run is quick.
        env.enableCheckpointing(500);
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "pubsub")
                .executeAndCollect();
    }

    /**
     * Runs the source until it has produced {@code expected} distinct records, then stops it.
     * Distinct, because the source is at-least-once and a duplicate must not crowd out an original.
     */
    private static Set<String> collect(
            Source<String, SubscriptionSplit, PubSubEnumeratorState> source, int expected)
            throws Exception {
        Set<String> collected = new LinkedHashSet<>();
        try (CloseableIterator<String> records = run(source)) {
            while (collected.size() < expected && records.hasNext()) {
                collected.add(records.next());
            }
        }
        return collected;
    }
}
