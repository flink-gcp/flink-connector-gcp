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

import org.apache.flink.api.common.JobExecutionResult;
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

import com.google.api.gax.rpc.NotFoundException;
import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.source.serializer.PubSubDeserializationSchema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.io.IOException;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static io.github.flink.gcp.connector.testutils.Awaits.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

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
 *
 * <p>The source creates the subscription itself, so creation with a dead-letter policy is under
 * test as well. A subscription the service has just created can still answer {@code NOT_FOUND} to
 * the first streaming pull for a while, which fails the source (#2027); one lag of about 6 s has
 * been observed. The job therefore gets three restarts 5 s apart, so an attempt starts once the
 * subscription is served. A deterministic failure fails every attempt, and the forwarding wait
 * checks the job's result between pulls, so it stops at the next check rather than running out the
 * whole wait.
 */
@Timeout(value = 600, threadMode = ThreadMode.SEPARATE_THREAD)
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "PUBSUB_IT_PROJECT", matches = ".+")
class PubSubDeadLetterRealGcpITCase extends AbstractPubSubRealGcpITCase {

    private static final int MAX_DELIVERY_ATTEMPTS = 5;
    private static final Duration FORWARDING_TIMEOUT = Duration.ofMinutes(8);

    /** How long each pull for the forwarded message lasts before the job's state is checked. */
    private static final Duration FORWARDING_POLL = Duration.ofSeconds(10);

    @Test
    void aPoisonMessageIsForwardedToTheDeadLetterTopicUnderTheNackPolicy() throws Exception {
        TopicDestination sourceTopic = createTopic("dlq-source");
        TopicDestination deadLetterTopic = createTopic("dlq-dead");
        SubscriptionDestination deadLetterObserver =
                createSubscription(deadLetterTopic, "dlq-observer");
        SubscriptionDestination subscription =
                trackSubscription(SubscriptionDestination.of(PROJECT, uniqueName("dlq-source")));

        Configuration configuration = new Configuration();
        // Restarts absorb the propagation lag after the source creates the subscription.
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3);
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(5));
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
                                .deserializer(new PoisonRejectingSchema())
                                .build(),
                        WatermarkStrategy.noWatermarks(),
                        "pubsub")
                .sinkTo(new DiscardingSink<>());

        JobClient job = env.executeAsync();
        try (AutoCloseable cancel = () -> job.cancel().get(30, TimeUnit.SECONDS)) {
            // Nothing published before the subscription exists is retained for it, and the source
            // creates it during the startup check.
            await(
                    "the source to create the subscription with the dead-letter policy",
                    Duration.ofSeconds(60),
                    () -> describeSubscriptionOrNull(subscription) != null);
            publish(sourceTopic, "poison");

            Set<String> deadLettered = awaitForwarded(job, deadLetterObserver);
            assertThat(deadLettered).containsExactly("poison");
        }
    }

    /**
     * Pulls the dead-letter observer until the poison message arrives or the wait runs out, and
     * stops with the job's own failure at the first check after the job has failed. One pull can
     * outlast the slice, because the helper waits up to its own pull timeout.
     *
     * <p>Watches the job's result rather than its status: the per-job mini cluster shuts down once
     * the job ends, after which a status query fails instead of answering {@code FAILED}, while the
     * result future is held by the client and keeps the job's failure.
     */
    private static Set<String> awaitForwarded(
            JobClient job, SubscriptionDestination deadLetterObserver) throws Exception {
        CompletableFuture<JobExecutionResult> result = job.getJobExecutionResult();
        long deadline = System.nanoTime() + FORWARDING_TIMEOUT.toNanos();
        while (true) {
            if (result.isDone()) {
                try {
                    result.get();
                } catch (ExecutionException e) {
                    throw new AssertionError(
                            "The job failed before the poison message was forwarded.", e);
                }
                fail("The streaming job finished before the poison message was forwarded.");
            }
            long remaining = deadline - System.nanoTime();
            Duration slice =
                    Duration.ofNanos(Math.max(0, Math.min(remaining, FORWARDING_POLL.toNanos())));
            Set<String> deadLettered = pullAndAckUntil(deadLetterObserver, 1, slice);
            if (!deadLettered.isEmpty() || remaining <= 0) {
                return deadLettered;
            }
        }
    }

    private static com.google.pubsub.v1.Subscription describeSubscriptionOrNull(
            SubscriptionDestination subscription) {
        try {
            return describeSubscription(subscription);
        } catch (NotFoundException e) {
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
