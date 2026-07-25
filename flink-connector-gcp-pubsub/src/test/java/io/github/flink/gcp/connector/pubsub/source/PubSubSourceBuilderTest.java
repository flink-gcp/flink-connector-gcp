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

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.util.InstantiationUtil;

import io.github.flink.gcp.connector.pubsub.source.serializer.PubSubDeserializationSchema;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.PubSubEnumeratorState;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.PubSubStreamingPullSource;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PubSubSourceBuilder} and {@link PubSubSource}. */
class PubSubSourceBuilderTest {

    private static final SubscriptionDestination SUB_A =
            SubscriptionDestination.of("project", "sub-a");
    private static final SubscriptionDestination SUB_B =
            SubscriptionDestination.of("project", "sub-b");

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
    void requiresADeserializationSchema() {
        assertThatThrownBy(() -> PubSubSource.<String>builder().subscription(SUB_A).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("A deserialization schema is required.");
    }

    @Test
    void requiresAtLeastOneSubscription() {
        assertThatThrownBy(
                        () ->
                                PubSubSource.<String>builder()
                                        .deserializationSchema(schema())
                                        .build())
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
    void rejectsNullDeserializationSchema() {
        assertThatThrownBy(() -> PubSubSource.<String>builder().deserializationSchema(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("deserializationSchema must not be null");
    }

    @Test
    void rejectsNullOrderingMode() {
        assertThatThrownBy(() -> PubSubSource.<String>builder().orderingMode(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("orderingMode must not be null");
    }

    @Test
    void rejectsNullSubscriberOptions() {
        assertThatThrownBy(() -> PubSubSource.<String>builder().subscriberOptions(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("subscriberOptions must not be null");
    }

    @Test
    void rejectsNullEmulatorEndpoint() {
        assertThatThrownBy(() -> PubSubSource.<String>builder().emulatorEndpoint(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("emulatorEndpoint must not be null");
    }

    @Test
    void rejectsBlankEmulatorEndpoint() {
        assertThatThrownBy(() -> PubSubSource.<String>builder().emulatorEndpoint("  "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("emulatorEndpoint must not be blank");
    }

    @Test
    void builtSourceRoundTripsJavaSerialization() throws Exception {
        PubSubSubscriberOptions options = PubSubSubscriberOptionsTest.fullyPopulated();
        Source<String, SubscriptionSplit, PubSubEnumeratorState> source =
                builder()
                        .subscriptions(SUB_A, SUB_B)
                        .orderingMode(OrderingMode.NONE)
                        .subscriberOptions(options)
                        .emulatorEndpoint("localhost:8085")
                        .build();

        Object restored =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(source), getClass().getClassLoader());

        @SuppressWarnings("unchecked")
        PubSubSourceConfig<String> restoredConfig =
                ((PubSubStreamingPullSource<String>) restored).getConfig();
        assertThat(restoredConfig.getSubscriptions()).containsExactly(SUB_A, SUB_B);
        assertThat(restoredConfig.getOrderingMode()).isEqualTo(OrderingMode.NONE);
        assertThat(restoredConfig.getSubscriberOptions()).isEqualTo(options);
        assertThat(restoredConfig.getEmulatorEndpoint()).isEqualTo("localhost:8085");
    }

    private static PubSubSourceBuilder<String> builder() {
        return PubSubSource.<String>builder().deserializationSchema(schema());
    }

    private static PubSubDeserializationSchema<String> schema() {
        return PubSubDeserializationSchema.dataOnly(new SimpleStringSchema());
    }

    private static PubSubSourceConfig<String> config(
            Source<String, SubscriptionSplit, PubSubEnumeratorState> source) {
        return ((PubSubStreamingPullSource<String>) source).getConfig();
    }
}
