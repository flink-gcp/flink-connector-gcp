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
import io.github.flink.gcp.connector.testutils.Drains;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;

import static io.github.flink.gcp.connector.testutils.Awaits.await;
import static io.github.flink.gcp.connector.testutils.Drains.drainDistinct;
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

        List<String> collected;
        try (CloseableIterator<String> records = run(source)) {
            // Nothing published before the subscription exists is retained for it, so the publish
            // has to wait for the startup check — which runs before any split is assigned.
            awaitSubscription(subscription);
            publish("startup-create-topic", "one", "two");
            collected = drainDistinct(records, 2, COLLECT_TIMEOUT, Function.identity());
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
                .hasStackTraceContaining("does not exist")
                .hasStackTraceContaining("subscription(destination, SubscriptionCreateOptions)");
        assertThat(subscriptionExists(subscription)).isFalse();
    }

    @Test
    void anUnorderedSubscriptionFailsTheJobUnderOrderedConsumption() throws Exception {
        // The guard users are most likely to hit, and the one whose absence would be silent: the
        // job would run and quietly deliver unordered messages.
        SubscriptionDestination subscription = createTopicAndSubscription("startup-unordered", 10);

        Source<String, SubscriptionSplit, PubSubEnumeratorState> source =
                sourceBuilder()
                        .subscription(subscription)
                        .orderingMode(OrderingMode.PER_KEY)
                        .build();

        assertThatThrownBy(() -> collect(source, 1))
                .hasStackTraceContaining("orderingMode(PER_KEY)")
                .hasStackTraceContaining("message ordering");
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
        // Consumed and acknowledged by someone else, so the subscription's backlog is empty — by
        // construction: exactly three messages were published and pullAndAckUntil returns only
        // once it has acknowledged three distinct ones. Confirming that with another pull would
        // long-poll an empty subscription for about a minute to re-assert something already known
        // (issue #151), so only the replayed rows below prove the seek.
        assertThat(pullAndAckUntil(subscription, 3, Duration.ofSeconds(30))).hasSize(3);

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
        await(
                "the source to create subscription " + subscription,
                Duration.ofSeconds(60),
                () -> subscriptionExists(subscription));
    }

    private static PubSubSourceBuilder<String> sourceBuilder() {
        return PubSubSource.<String>builder()
                .deserializationSchema(
                        PubSubDeserializationSchema.payload(new SimpleStringSchema()))
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
     * Runs the source until it has produced {@code expected} distinct records or {@link
     * #COLLECT_TIMEOUT} passes, then stops it. The mechanics of the bounded drain and the reason
     * records are counted distinct live on {@link Drains#drainDistinct}.
     */
    private static List<String> collect(
            Source<String, SubscriptionSplit, PubSubEnumeratorState> source, int expected)
            throws Exception {
        try (CloseableIterator<String> records = run(source)) {
            return drainDistinct(records, expected, COLLECT_TIMEOUT, Function.identity());
        }
    }
}
