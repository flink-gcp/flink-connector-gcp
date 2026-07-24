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

package io.github.flink.gcp.connector.pubsub.sink.publisher.writer;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.github.flink.gcp.connector.pubsub.sink.CreateDisposition;
import io.github.flink.gcp.connector.pubsub.sink.PubSubSink;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.sink.serializer.PubSubSerializationSchema;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end streaming integration test against the Pub/Sub emulator, driving the sink exclusively
 * through the public {@code PubSubSink.builder()...emulatorEndpoint(...)} path — no test seams.
 *
 * <p>Runs a MiniCluster DataStream job in streaming mode with a rate-limited source that spans
 * several 1-second checkpoints, so the checkpoint flush fires mid-stream (not only at end of
 * input), with dynamic destinations across two pre-created topics and one auto-created topic.
 */
class PubSubSinkStreamingITCase extends AbstractPubSubEmulatorITCase {

    private static final long RECORD_COUNT = 40;
    private static final double RECORDS_PER_SECOND = 10;

    /** Records rotate over these topics; the last one exists only through auto-creation. */
    private static final String[] TOPICS = {"stream-a", "stream-b", "stream-auto"};

    @Test
    void streamingJobDeliversAllRecordsAcrossDynamicDestinations() throws Exception {
        TopicDestination topicA = TopicDestination.of(PROJECT, "stream-a");
        TopicDestination topicB = TopicDestination.of(PROJECT, "stream-b");
        TopicDestination autoTopic = TopicDestination.of(PROJECT, "stream-auto");
        // Subscriptions must exist before the job publishes: the emulator retains no messages
        // published before a subscription exists. The auto-created topic can have no subscription
        // up front, so only its creation is asserted.
        createTopic(topicA);
        createTopic(topicB);
        createSubscription(topicA, "stream-a-sub");
        createSubscription(topicB, "stream-b-sub");

        // The endpoint travels into the job graph as a plain string (the container handle is not
        // serializable).
        String endpoint = emulatorEndpoint();

        // With checkpointing enabled Flink defaults to endless fixed-delay restarts; a
        // permanently failing publish would loop until the test times out instead of failing fast.
        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(1_000);
        env.setParallelism(2);

        DataGeneratorSource<String> source =
                new DataGeneratorSource<>(
                        (GeneratorFunction<Long, String>)
                                index -> TOPICS[(int) (index % TOPICS.length)] + "|" + index,
                        RECORD_COUNT,
                        RateLimiterStrategy.perSecond(RECORDS_PER_SECOND),
                        Types.STRING);

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "records")
                .sinkTo(
                        PubSubSink.<String>builder()
                                .destinationResolver(
                                        (element, context) ->
                                                TopicDestination.of(
                                                        PROJECT,
                                                        element.substring(0, element.indexOf('|'))))
                                .serializer(
                                        PubSubSerializationSchema.dataOnly(
                                                new SimpleStringSchema()))
                                .createDisposition(CreateDisposition.CREATE_IF_NEEDED)
                                .emulatorEndpoint(endpoint)
                                .build());

        env.execute("pubsub-sink-streaming-it");

        // Distinct-set equality dedupes at-least-once duplicates while proving every record
        // arrived and nothing foreign did.
        Set<String> expectedA = expectedPayloads("stream-a");
        Set<String> expectedB = expectedPayloads("stream-b");
        assertThat(
                        pullDistinctPayloadsUntil(
                                "stream-a-sub", expectedA.size(), Duration.ofSeconds(60)))
                .containsExactlyInAnyOrderElementsOf(expectedA);
        assertThat(
                        pullDistinctPayloadsUntil(
                                "stream-b-sub", expectedB.size(), Duration.ofSeconds(60)))
                .containsExactlyInAnyOrderElementsOf(expectedB);
        assertThat(topicExists(autoTopic)).isTrue();
    }

    private static Set<String> expectedPayloads(String topic) {
        Set<String> expected = new LinkedHashSet<>();
        for (long index = 0; index < RECORD_COUNT; index++) {
            if (TOPICS[(int) (index % TOPICS.length)].equals(topic)) {
                expected.add(topic + "|" + index);
            }
        }
        return expected;
    }
}
