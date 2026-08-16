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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.util.Collector;

import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.source.serializer.PubSubDeserializationSchema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.io.IOException;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.github.flink.gcp.connector.testutils.Awaits.await;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@code NACK} deserialization-failure policy end to end, against real Cloud Pub/Sub: a message
 * the schema cannot deserialize is nacked on every delivery until the subscription's dead-letter
 * policy forwards it to the dead-letter topic. The emulator can host the policy fields but never
 * forwards, so this path has no coverage anywhere else.
 *
 * <p>Two pieces of real-service machinery are under test at once: the source's nack (via the
 * policy) driving the delivery-attempt counter, and the forwarding itself, which is performed by
 * the <em>Pub/Sub service agent</em> — not by these credentials — and therefore depends on the
 * project-level grants provisioned in opentofu (the service agent needs {@code
 * roles/pubsub.publisher} on the dead-letter topic's project and {@code roles/pubsub.subscriber}
 * for the source subscription).
 *
 * <p>Forwarding is service-paced: attempts are counted best-effort and the forward happens some
 * time after the last nack, so the wait is generous and the class timeout wider than the base's.
 */
@Timeout(600)
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "PUBSUB_IT_PROJECT", matches = ".+")
class PubSubDeadLetterRealGcpITCase extends AbstractPubSubRealGcpITCase {

    private static final int MAX_DELIVERY_ATTEMPTS = 5;
    private static final Duration FORWARDING_TIMEOUT = Duration.ofMinutes(8);

    @Test
    void aPoisonMessageIsForwardedToTheDeadLetterTopicUnderTheNackPolicy() throws Exception {
        TopicDestination sourceTopic = createTopic("dlq-source");
        TopicDestination deadLetterTopic = createTopic("dlq-dead");
        SubscriptionDestination deadLetterObserver =
                createSubscription(deadLetterTopic, "dlq-observer");
        SubscriptionDestination subscription =
                trackSubscription(SubscriptionDestination.of(PROJECT, uniqueName("dlq-source")));

        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        env.enableCheckpointing(500);
        env.fromSource(
                        PubSubSource.<String>builder()
                                .subscription(
                                        subscription,
                                        SubscriptionCreateOptions.builder()
                                                .topic(sourceTopic)
                                                .deadLetterPolicy(
                                                        deadLetterTopic, MAX_DELIVERY_ATTEMPTS)
                                                .build())
                                .deserializationFailurePolicy(DeserializationFailurePolicy.NACK)
                                .deserializationSchema(new PoisonRejectingSchema())
                                .build(),
                        WatermarkStrategy.noWatermarks(),
                        "pubsub")
                .sinkTo(new DiscardingSink<>());

        JobClient job = env.executeAsync();
        try {
            // Nothing published before the subscription exists is retained for it, and the source
            // creates it during the startup check.
            await(
                    "the source to create the subscription with the dead-letter policy",
                    Duration.ofSeconds(60),
                    () -> describeSubscriptionOrNull(subscription) != null);
            publish(sourceTopic, "poison");

            Set<String> deadLettered = pullAndAckUntil(deadLetterObserver, 1, FORWARDING_TIMEOUT);
            assertThat(deadLettered).containsExactly("poison");
        } finally {
            // Best effort, as in PubSubSourceRecoveryITCase: a job that already ended would throw
            // here and replace the assertion error actually being reported.
            try {
                job.cancel().get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                // Best effort only.
            }
        }
    }

    private static com.google.pubsub.v1.Subscription describeSubscriptionOrNull(
            SubscriptionDestination subscription) {
        try {
            return describeSubscription(subscription);
        } catch (RuntimeException e) {
            return null;
        }
    }

    /** Rejects every message, so each delivery is nacked under the {@code NACK} policy. */
    private static class PoisonRejectingSchema implements PubSubDeserializationSchema<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(
                PubsubMessage message, SubscriptionDestination subscription, Collector<String> out)
                throws IOException {
            throw new IOException(
                    "Deliberately undeserializable: " + message.getData().toStringUtf8());
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return Types.STRING;
        }
    }
}
