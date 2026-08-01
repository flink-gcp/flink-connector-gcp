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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;

import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.source.serializer.PubSubDeserializationSchema;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.PubSubEnumeratorState;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.github.flink.gcp.connector.testutils.Drains.drainDistinct;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The end-to-end per-key ordering guarantee, against real Cloud Pub/Sub — the only place it can be
 * verified at all: per-key callback serialization in the client library is gated on {@code
 * subscriptionProperties.messageOrderingEnabled} in the streaming-pull response, which the emulator
 * does not set, so emulator callbacks arrive out of order with no Flink involved (see the
 * documentation page's Testing section and the correction on issue #82). The emulator ITs assert
 * only the parts that survive there: single-subtask consumption without stalling idle subtasks.
 */
@EnabledIfEnvironmentVariable(named = "PUBSUB_IT_PROJECT", matches = ".+")
class PubSubSourceOrderingRealGcpITCase extends AbstractPubSubRealGcpITCase {

    private static final int KEYS = 4;
    private static final int MESSAGES_PER_KEY = 25;

    /**
     * Publishes a numbered sequence under each of several ordering keys and asserts every key's
     * sequence arrives monotonically. Parallelism 1 with a chained collect sink, so arrival order
     * at the iterator is emission order; {@code drainDistinct} keeps first occurrences in arrival
     * order, which under ordered at-least-once delivery is exactly the property to check — a
     * redelivery replays a suffix in order, so first occurrences must already be the full sequence.
     */
    @Test
    void perKeySequencesArriveMonotonicallyUnderOrderedConsumption() throws Exception {
        TopicDestination topic = createTopic("ordering");
        SubscriptionDestination subscription =
                createSubscription(
                        topic, "ordering", builder -> builder.setEnableMessageOrdering(true));
        String[] sequence =
                IntStream.range(0, MESSAGES_PER_KEY)
                        .mapToObj(Integer::toString)
                        .toArray(String[]::new);
        for (int key = 0; key < KEYS; key++) {
            publishOrdered(topic, "k" + key, sequence);
        }

        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        env.enableCheckpointing(500);

        List<String> collected;
        try (CloseableIterator<String> records =
                env.fromSource(source(subscription), WatermarkStrategy.noWatermarks(), "pubsub")
                        .executeAndCollect()) {
            collected =
                    drainDistinct(
                            records, KEYS * MESSAGES_PER_KEY, COLLECT_TIMEOUT, Function.identity());
        }

        Map<String, List<Integer>> sequencesByKey = new LinkedHashMap<>();
        for (String record : collected) {
            int separator = record.indexOf('|');
            sequencesByKey
                    .computeIfAbsent(record.substring(0, separator), unused -> new ArrayList<>())
                    .add(Integer.parseInt(record.substring(separator + 1)));
        }
        List<Integer> expected =
                IntStream.range(0, MESSAGES_PER_KEY).boxed().collect(Collectors.toList());
        assertThat(sequencesByKey).hasSize(KEYS);
        for (Map.Entry<String, List<Integer>> entry : sequencesByKey.entrySet()) {
            assertThat(entry.getValue())
                    .as("arrival order of key %s", entry.getKey())
                    .isEqualTo(expected);
        }
    }

    private static Source<String, SubscriptionSplit, PubSubEnumeratorState> source(
            SubscriptionDestination subscription) {
        return PubSubSource.<String>builder()
                .subscription(subscription)
                .orderingMode(OrderingMode.PER_KEY)
                .deserializationSchema(new KeyTaggingSchema())
                .build();
    }

    /** Emits {@code orderingKey|payload} so the assertion can regroup arrivals by key. */
    private static class KeyTaggingSchema implements PubSubDeserializationSchema<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(
                PubsubMessage message,
                SubscriptionDestination subscription,
                Collector<String> out) {
            out.collect(message.getOrderingKey() + "|" + message.getData().toStringUtf8());
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return Types.STRING;
        }
    }
}
