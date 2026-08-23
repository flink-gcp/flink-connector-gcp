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

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.util.InstantiationUtil;

import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.source.serializer.PubSubDeserializationSchema;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.PubSubEnumeratorState;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.PubSubStreamingPullSource;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/** Tests for {@link PubSubSourceBuilder} and {@link PubSubSource}. */
class PubSubSourceBuilderTest {

    private static final SubscriptionDestination SUB_A =
            SubscriptionDestination.of("project", "sub-a");
    private static final SubscriptionDestination SUB_B =
            SubscriptionDestination.of("project", "sub-b");
    private static final TopicDestination TOPIC = TopicDestination.of("project", "topic");
    private static final String SERVICE_ACCOUNT_KEY_FILE =
            "/var/run/secrets/gcp/service-account.json";

    @Test
    void buildsAnUnboundedSourceWithTheConfiguredSettings() {
        Source<String, SubscriptionSplit, PubSubEnumeratorState> source =
                builder().subscriptions(SUB_A, SUB_B).orderingMode(OrderingMode.PER_KEY).build();

        assertThat(source.getBoundedness()).isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);
        PubSubSourceConfig<String> config = config(source);
        assertThat(config.getSubscriptions()).containsExactly(SUB_A, SUB_B);
        assertThat(config.getOrderingMode()).isEqualTo(OrderingMode.PER_KEY);
        assertThat(config.getEmulatorEndpoint()).isNull();
    }

    @Test
    void defaultsToUnorderedConsumption() {
        assertThat(config(builder().subscription(SUB_A).build()).getOrderingMode())
                .isEqualTo(OrderingMode.NONE);
    }

    @Test
    void defaultsToDefaultSubscriberOptions() {
        assertThat(config(builder().subscription(SUB_A).build()).getSubscriberOptions())
                .isEqualTo(PubSubSubscriberOptions.defaults());
    }

    @Test
    void carriesSubscriberOptionsIntoTheConfig() {
        PubSubSubscriberOptions options =
                PubSubSubscriberOptions.builder().maxRecordsPerFetch(42).build();

        assertThat(
                        config(builder().subscription(SUB_A).subscriberOptions(options).build())
                                .getSubscriberOptions())
                .isEqualTo(options);
    }

    @Test
    void rejectsAnExplicitParallelPullCountUnderOrderedConsumption() {
        PubSubSubscriberOptions options =
                PubSubSubscriberOptions.builder().parallelPullCount(4).build();

        assertThatThrownBy(
                        () ->
                                builder()
                                        .subscription(SUB_A)
                                        .orderingMode(OrderingMode.PER_KEY)
                                        .subscriberOptions(options)
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("parallelPullCount(4)")
                .hasMessageContaining("orderingMode(PER_KEY)");
    }

    @Test
    void acceptsAParallelPullCountOfOneUnderOrderedConsumption() {
        PubSubSubscriberOptions options =
                PubSubSubscriberOptions.builder().parallelPullCount(1).build();

        assertThat(
                        config(
                                        builder()
                                                .subscription(SUB_A)
                                                .orderingMode(OrderingMode.PER_KEY)
                                                .subscriberOptions(options)
                                                .build())
                                .getSubscriberOptions())
                .isEqualTo(options);
    }

    @Test
    void collectsSubscriptionsAcrossEveryOverload() {
        Source<String, SubscriptionSplit, PubSubEnumeratorState> source =
                builder().subscription(SUB_A).subscriptions(Arrays.asList(SUB_B)).build();

        assertThat(config(source).getSubscriptions()).containsExactly(SUB_A, SUB_B);
    }

    @Test
    void requiresADeserializer() {
        assertThatThrownBy(() -> PubSubSource.<String>builder().subscription(SUB_A).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("A deserializer is required: set deserializer(...).");
    }

    @Test
    void requiresAtLeastOneSubscription() {
        assertThatThrownBy(() -> PubSubSource.<String>builder().deserializer(schema()).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("At least one subscription is required");
    }

    @Test
    void rejectsDuplicateSubscriptions() {
        assertThatThrownBy(() -> builder().subscriptions(SUB_A, SUB_A).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Subscriptions must be distinct");
    }

    @Test
    void rejectsNullSubscription() {
        assertThatThrownBy(() -> PubSubSource.<String>builder().subscription(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("subscription must not be null");
    }

    @Test
    void rejectsNullDeserializer() {
        assertThatThrownBy(() -> PubSubSource.<String>builder().deserializer(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("deserializer must not be null");
    }

    @Test
    void rejectsNullOrderingMode() {
        assertThatThrownBy(() -> PubSubSource.<String>builder().orderingMode(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("orderingMode must not be null");
    }

    @Test
    void defaultsToFailingOnADeserializationFailure() {
        assertThat(config(builder().subscription(SUB_A).build()).getDeserializationFailurePolicy())
                .isEqualTo(DeserializationFailurePolicy.FAIL);
    }

    @Test
    void carriesTheDeserializationFailurePolicyIntoTheConfig() {
        assertThat(
                        config(
                                        builder()
                                                .subscription(SUB_A)
                                                .deserializationFailurePolicy(
                                                        DeserializationFailurePolicy.DROP)
                                                .build())
                                .getDeserializationFailurePolicy())
                .isEqualTo(DeserializationFailurePolicy.DROP);
    }

    @Test
    void rejectsNullDeserializationFailurePolicy() {
        assertThatThrownBy(() -> PubSubSource.<String>builder().deserializationFailurePolicy(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("deserializationFailurePolicy must not be null");
    }

    @Test
    void rejectsNullSubscriberOptions() {
        assertThatThrownBy(() -> PubSubSource.<String>builder().subscriberOptions(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("subscriberOptions must not be null");
    }

    @Test
    void serviceAccountKeyFileDefaultsToNull() {
        assertThat(config(builder().subscription(SUB_A).build()).getServiceAccountKeyFile())
                .isNull();
    }

    @Test
    @SuppressWarnings("unchecked")
    void serviceAccountKeyFilePropagatesAndSurvivesJavaSerialization() throws Exception {
        Source<String, SubscriptionSplit, PubSubEnumeratorState> source =
                builder()
                        .subscription(SUB_A)
                        .serviceAccountKeyFile(SERVICE_ACCOUNT_KEY_FILE)
                        .build();

        Object restored =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(source), getClass().getClassLoader());

        assertThat(config((PubSubStreamingPullSource<String>) restored).getServiceAccountKeyFile())
                .isEqualTo(SERVICE_ACCOUNT_KEY_FILE);
    }

    @Test
    void rejectsNullOrBlankServiceAccountKeyFile() {
        assertThatThrownBy(() -> PubSubSource.<String>builder().serviceAccountKeyFile(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("serviceAccountKeyFile must not be null");
        assertThatThrownBy(() -> PubSubSource.<String>builder().serviceAccountKeyFile(" \t"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("serviceAccountKeyFile must not be blank");
    }

    @Test
    void rejectsAServiceAccountKeyFileAlongsideAnEmulatorInEitherOrder() {
        assertThatThrownBy(
                        () ->
                                builder()
                                        .subscription(SUB_A)
                                        .serviceAccountKeyFile(SERVICE_ACCOUNT_KEY_FILE)
                                        .emulatorEndpoint("localhost:8085")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("serviceAccountKeyFile(...)")
                .hasMessageContaining("emulatorEndpoint(...)");
        assertThatThrownBy(
                        () ->
                                builder()
                                        .subscription(SUB_A)
                                        .emulatorEndpoint("localhost:8085")
                                        .serviceAccountKeyFile(SERVICE_ACCOUNT_KEY_FILE)
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("serviceAccountKeyFile(...)")
                .hasMessageContaining("emulatorEndpoint(...)");
    }

    @Test
    void rejectsNullEmulatorEndpoint() {
        assertThatThrownBy(() -> PubSubSource.<String>builder().emulatorEndpoint(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("emulatorEndpoint must not be null");
    }

    @Test
    void rejectsAMalformedEmulatorEndpoint() {
        // Parsed at the setter, so a typo fails on the client rather than at connect time on a
        // TaskManager; the full parse table is EmulatorEndpointTest's.
        assertThatThrownBy(() -> PubSubSource.<String>builder().emulatorEndpoint("localhost8085"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("emulatorEndpoint must be host:port, was 'localhost8085'");
    }

    @Test
    void builtSourceRoundTripsJavaSerialization() throws Exception {
        PubSubSubscriberOptions options = PubSubSubscriberOptionsTest.fullyPopulated();
        SubscriptionCreateOptions createOptions =
                SubscriptionCreateOptions.builder()
                        .topic(TOPIC)
                        .enableMessageOrdering(true)
                        .ackDeadline(Duration.ofSeconds(30))
                        .build();
        PubSubStartPosition startPosition =
                PubSubStartPosition.fromTimestamp(Instant.ofEpochMilli(1_000L));
        Source<String, SubscriptionSplit, PubSubEnumeratorState> source =
                builder()
                        .subscription(SUB_A, createOptions)
                        .subscription(SUB_B)
                        .orderingMode(OrderingMode.PER_KEY)
                        .subscriberOptions(options)
                        .startPosition(startPosition)
                        .emulatorEndpoint("localhost:8085")
                        .build();

        Object restored =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(source), getClass().getClassLoader());

        @SuppressWarnings("unchecked")
        PubSubSourceConfig<String> restoredConfig =
                ((PubSubStreamingPullSource<String>) restored).getConfig();
        assertThat(restoredConfig.getSubscriptions()).containsExactly(SUB_A, SUB_B);
        assertThat(restoredConfig.getOrderingMode()).isEqualTo(OrderingMode.PER_KEY);
        assertThat(restoredConfig.getSubscriberOptions()).isEqualTo(options);
        assertThat(restoredConfig.getCreateOptions()).containsExactly(entry(SUB_A, createOptions));
        assertThat(restoredConfig.getStartPosition()).isEqualTo(startPosition);
        assertThat(restoredConfig.getEmulatorEndpoint())
                .isEqualTo(EmulatorEndpoint.parse("localhost:8085", "emulatorEndpoint"));
    }

    @Test
    void defaultsToContinuingFromTheSubscription() {
        assertThat(config(builder().subscription(SUB_A).build()).getStartPosition())
                .isEqualTo(PubSubStartPosition.continueFromSubscription());
    }

    @Test
    void subscriptionsAddedWithoutCreateOptionsCarryNone() {
        assertThat(config(builder().subscriptions(SUB_A, SUB_B).build()).getCreateOptions())
                .isEmpty();
    }

    @Test
    void rejectsUnorderedCreateOptionsUnderOrderedConsumption() {
        SubscriptionCreateOptions createOptions =
                SubscriptionCreateOptions.builder().topic(TOPIC).build();

        assertThatThrownBy(
                        () ->
                                builder()
                                        .subscription(SUB_A, createOptions)
                                        .orderingMode(OrderingMode.PER_KEY)
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("orderingMode(PER_KEY)")
                .hasMessageContaining("enableMessageOrdering(true)")
                .hasMessageContaining(SUB_A.toString());
    }

    @Test
    void acceptsOrderedCreateOptionsUnderOrderedConsumption() {
        SubscriptionCreateOptions createOptions =
                SubscriptionCreateOptions.builder()
                        .topic(TOPIC)
                        .enableMessageOrdering(true)
                        .build();

        assertThat(
                        config(
                                        builder()
                                                .subscription(SUB_A, createOptions)
                                                .orderingMode(OrderingMode.PER_KEY)
                                                .build())
                                .getCreateOptions())
                .containsExactly(entry(SUB_A, createOptions));
    }

    @Test
    void rejectsCreateSettingsWithoutADeadLetterPolicyUnderANackingPolicy() {
        // Caught here rather than at startup: otherwise the source creates the subscription, then
        // refuses it, and crash-loops with an orphan accumulating a copy of the topic's stream.
        SubscriptionCreateOptions createOptions =
                SubscriptionCreateOptions.builder().topic(TOPIC).build();

        assertThatThrownBy(
                        () ->
                                builder()
                                        .subscription(SUB_A, createOptions)
                                        .deserializationFailurePolicy(
                                                DeserializationFailurePolicy.NACK)
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("deserializationFailurePolicy(NACK)")
                .hasMessageContaining("deadLetterPolicy(...)")
                .hasMessageContaining(SUB_A.toString());
    }

    @Test
    void acceptsANackingPolicyForASubscriptionItDoesNotCreate() {
        // Only auto-created subscriptions are checkable here; an existing one is checked at
        // startup.
        assertThat(
                        config(
                                        builder()
                                                .subscription(SUB_A)
                                                .deserializationFailurePolicy(
                                                        DeserializationFailurePolicy.NACK)
                                                .build())
                                .getDeserializationFailurePolicy())
                .isEqualTo(DeserializationFailurePolicy.NACK);
    }

    @Test
    void rejectsNullStartPosition() {
        assertThatThrownBy(() -> PubSubSource.<String>builder().startPosition(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("startPosition must not be null");
    }

    @Test
    void rejectsNullCreateOptions() {
        assertThatThrownBy(() -> PubSubSource.<String>builder().subscription(SUB_A, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("createOptions must not be null");
    }

    private static PubSubSourceBuilder<String> builder() {
        return PubSubSource.<String>builder().deserializer(schema());
    }

    private static PubSubDeserializationSchema<String> schema() {
        return PubSubDeserializationSchema.payload(new SimpleStringSchema());
    }

    private static PubSubSourceConfig<String> config(
            Source<String, SubscriptionSplit, PubSubEnumeratorState> source) {
        return ((PubSubStreamingPullSource<String>) source).getConfig();
    }
}
