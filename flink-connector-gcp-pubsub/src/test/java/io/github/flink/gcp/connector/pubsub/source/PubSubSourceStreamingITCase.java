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

import io.github.flink.gcp.connector.pubsub.source.serializer.PubSubDeserializationSchema;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.PubSubEnumeratorState;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;
import io.github.flink.gcp.connector.testutils.Drains;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.github.flink.gcp.connector.testutils.Drains.drainDistinct;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * MiniCluster integration tests driving the source exclusively through the public builder, with the
 * emulator endpoint and real checkpointing.
 */
class PubSubSourceStreamingITCase extends AbstractPubSubSourceEmulatorITCase {

    private static final int ACK_DEADLINE_SECONDS = 60;

    @Test
    void streamingJobReceivesEveryMessageOfEverySubscription() throws Exception {
        SubscriptionDestination first =
                createTopicAndSubscription("streaming-a", ACK_DEADLINE_SECONDS);
        SubscriptionDestination second =
                createTopicAndSubscription("streaming-b", ACK_DEADLINE_SECONDS);
        List<String> fromFirst = payloads("a", 25);
        List<String> fromSecond = payloads("b", 25);
        publish("streaming-a", fromFirst.toArray(new String[0]));
        publish("streaming-b", fromSecond.toArray(new String[0]));

        List<String> collected =
                collect(source(first, second), fromFirst.size() + fromSecond.size());

        // At-least-once: a redelivery is allowed, a loss is not.
        assertThat(collected).containsAll(fromFirst).containsAll(fromSecond);
    }

    /**
     * Ordered mode assigns the single subscription to one subtask and leaves the other idle. This
     * asserts that the idle subtask does not stall the job and that no message is lost.
     *
     * <p>It deliberately does <em>not</em> assert per-ordering-key order: the emulator does not
     * drive ordered dispatch. Verified by probing the client library directly against it — the
     * {@code MessageReceiver} callbacks arrive out of order with no Flink involved, because per-key
     * callback serialization is gated on {@code subscriptionProperties.messageOrderingEnabled} in
     * the streaming-pull response, which the emulator does not set. End-to-end ordering is covered
     * by the real-GCP suite (#82).
     */
    @Test
    void orderedModeConsumesOneSubscriptionWithoutStallingOnIdleSubtasks() throws Exception {
        SubscriptionDestination subscription =
                createTopicAndOrderedSubscription("streaming-ordered", ACK_DEADLINE_SECONDS);
        List<String> published = payloads("k", 30);
        publishOrdered("streaming-ordered", "key", published.toArray(new String[0]));

        List<String> collected =
                collect(
                        PubSubSource.<String>builder()
                                .subscription(subscription)
                                .orderingMode(OrderingMode.PER_KEY)
                                .deserializationSchema(
                                        PubSubDeserializationSchema.dataOnly(
                                                new SimpleStringSchema()))
                                .emulatorEndpoint(emulatorEndpoint())
                                .build(),
                        published.size());

        assertThat(collected).containsAll(published);
    }

    private static Source<String, SubscriptionSplit, PubSubEnumeratorState> source(
            SubscriptionDestination... subscriptions) {
        return PubSubSource.<String>builder()
                .subscriptions(subscriptions)
                .deserializationSchema(
                        PubSubDeserializationSchema.dataOnly(new SimpleStringSchema()))
                .emulatorEndpoint(emulatorEndpoint())
                .build();
    }

    /**
     * Runs the source until {@code expected} <em>distinct</em> records have been collected or
     * {@link #COLLECT_TIMEOUT} passes, then cancels the job — the source is unbounded, so nothing
     * else would ever end it. The mechanics of the bounded drain and the reason records are counted
     * distinct rather than total live on {@link Drains#drainDistinct}.
     */
    private static List<String> collect(
            Source<String, SubscriptionSplit, PubSubEnumeratorState> source, int expected)
            throws Exception {
        Configuration configuration = new Configuration();
        // Without this a permanent failure would be retried forever instead of failing the test.
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(2);
        // Checkpointing is what acknowledges messages; the interval is short so the run is quick.
        env.enableCheckpointing(500);

        try (CloseableIterator<String> records =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "pubsub")
                        .executeAndCollect()) {
            return drainDistinct(records, expected, COLLECT_TIMEOUT, Function.identity());
        }
    }

    private static List<String> payloads(String prefix, int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> prefix + "-" + i)
                .collect(Collectors.toList());
    }
}
